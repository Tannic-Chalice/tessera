"""TESSERA Navigator - congestion-aware routing for a simulated car (Bengaluru).

The phone page (/nav) asks for a trip; the server drives a simulated car along the route, publishes the
car's GPS to gps.raw like any other vehicle, and keeps checking the road ahead against what TESSERA
knows *right now*:
  - active geofences in the current plan (incident zones the agent created and the verifier approved)
  - live tile aggregates (H3 res 8): mean speed and share of slow reports over the last 60 s window
When the road ahead is blocked or crawling, it re-plans with A* on the road graph (edge cost = travel time
at the live tile speed, x20 inside a geofence) and tells the driver where to turn.

Road graph: the BMTC route geometry (tessera/data/bengaluru_roads.json.gz). Vertices are merged by H3 res-10
cell (~65 m), so roads shared by many bus routes become one edge and crossings become junctions."""
import heapq
import math
import threading
import time
import uuid
from collections import defaultdict

import h3

from . import config, geo, gsir

RES = 10
DEFAULT_KMH = 22.0          # road with no live data yet
MAX_KMH = 45.0              # A* heuristic must not overestimate travel time
GEOFENCE_PENALTY = 20.0
UTURN_S = 25.0              # time cost of turning round on the spot (a U-turn is not free in city traffic)
LOOKAHEAD_KM = 2.5
CRAWL_KMH, CRAWL_SLOW = 10.0, 0.55


def _km(a, b):
    return geo.haversine_m(a[0], a[1], b[0], b[1]) / 1000.0


def bearing(a, b):
    dn = b[0] - a[0]
    de = (b[1] - a[1]) * math.cos(math.radians(a[0]))
    return (math.degrees(math.atan2(de, dn)) + 360) % 360


class RoadGraph:
    def __init__(self, net):
        self.pos, self.adj = {}, defaultdict(dict)
        for r in net.routes:
            prev = None
            for lat, lon in r:
                c = h3.latlng_to_cell(lat, lon, RES)
                self.pos.setdefault(c, (lat, lon))
                if prev and prev != c:
                    d = _km(self.pos[prev], self.pos[c])
                    if d < 1.5:
                        self.adj[prev][c] = self.adj[c][prev] = min(self.adj[prev].get(c, 9e9), d)
                prev = c
        self.places = net.places or net.landmarks
        self.landmarks = net.landmarks

    def nearest(self, lat, lon):
        c0 = h3.latlng_to_cell(lat, lon, RES)
        for k in (0, 2, 5, 10, 20, 40):
            cands = [c for c in h3.grid_disk(c0, k) if c in self.adj]
            if cands:
                return min(cands, key=lambda c: _km(self.pos[c], (lat, lon)))
        return min(self.adj, key=lambda c: _km(self.pos[c], (lat, lon)))

    def place_name(self, lat, lon):
        best = min(self.places, key=lambda p: (p["lat"] - lat) ** 2 + ((p["lon"] - lon) * 0.975) ** 2)
        return best["name"]

    def route(self, src, dst, cost_fn):
        """A* over travel time. cost_fn(cell) -> (kmh, penalty). Returns list of cells or None."""
        goal = self.pos[dst]
        openq = [(0.0, 0.0, src)]
        g, came = {src: 0.0}, {}
        while openq:
            _f, gu, u = heapq.heappop(openq)
            if u == dst:
                path = [u]
                while u in came:
                    u = came[u]
                    path.append(u)
                return path[::-1]
            if gu > g.get(u, 9e9):
                continue
            for v, d in self.adj[u].items():
                kmh, pen = cost_fn(v)
                gv = gu + d / kmh * pen
                if gv < g.get(v, 9e9):
                    g[v], came[v] = gv, u
                    heapq.heappush(openq, (gv + _km(self.pos[v], goal) / MAX_KMH, gv, v))
        return None


class TrafficView:
    """What TESSERA knows now: live tiles + active geofences, read from the dashboard's live state."""

    def __init__(self, tiles, plan):
        self.tiles = {}
        for t, x in tiles.items():
            self.tiles[t] = (x.get("mean_speed", 0.0), x.get("slow_frac", 0.0), x.get("vehicles", 0))
        self.geofences = [g for g in gsir.active_geofences(plan, int(time.time() * 1000))
                          if config.city_of(g["shape"]["lat"], g["shape"]["lon"]) == "bengaluru"]
        self._cache = {}

    def at(self, cell, pos):
        r = self._cache.get(cell)
        if r is None:
            t = h3.cell_to_parent(cell, config.TILE_RES)
            mean, slow, veh = self.tiles.get(t, (None, 0.0, 0))
            kmh = DEFAULT_KMH if mean is None or veh < 3 else min(MAX_KMH, max(4.0, mean))
            fence = next((g for g in self.geofences
                          if geo.haversine_m(pos[0], pos[1], g["shape"]["lat"], g["shape"]["lon"])
                          <= g["shape"]["radius"]), None)
            crawl = veh >= 3 and mean is not None and mean < CRAWL_KMH and slow >= CRAWL_SLOW
            r = self._cache[cell] = (kmh, GEOFENCE_PENALTY if fence else 1.0, fence, crawl)
        return r


def path_km(graph, path):
    cum = [0.0]
    for a, b in zip(path, path[1:]):
        cum.append(cum[-1] + graph.adj[a].get(b, _km(graph.pos[a], graph.pos[b])))
    return cum


def eta_s(graph, path, view, start=0):
    t = 0.0
    for a, b in zip(path[start:], path[start + 1:]):
        kmh, _pen, fence, _c = view.at(b, graph.pos[b])
        t += graph.adj[a].get(b, 0.1) / (5.0 if fence else kmh) * 3600       # ~5 km/h through an incident zone
    return t


def turn_word(b_old, b_new):
    d = (b_new - b_old + 540) % 360 - 180
    if d > 25:
        return "right"
    if d < -25:
        return "left"
    return "slightly " + ("right" if d > 0 else "left") if abs(d) > 8 else "straight"


class Trip:
    def __init__(self, graph, src, dst, speedup):
        self.id = uuid.uuid4().hex[:6]
        self.vid = f"MYCAR-{self.id.upper()}"
        self.graph, self.dst, self.speedup = graph, dst, max(0.0, min(20.0, speedup))   # 0 = paused (route preview)
        self.path, self.cum = [src], [0.0]
        self.i, self.frac = 0, 0.0            # on edge path[i] -> path[i+1], fraction done
        self.speed = 0.0
        self.version = 0
        self.messages = []
        self.status = "driving"
        self.started = time.time()
        self.last_check = self.last_reroute = self.last_pub = 0.0
        self.warned = set()
        self.blocked_at = None
        self.blocked_ts = 0.0
        self.blocked_km = 0.0
        self.old_route, self.reroute_ts, self.uturn_ts, self.uturn_towards = [], 0.0, 0.0, ""
        self.hold_speed = None               # demo speed to restore once TESSERA has decided on a blocked road
        self.turn = None                     # pending manoeuvre: {idx, word, towards}
        self.story_conv = None               # agent conversation behind the incident zone we are avoiding
        self.story_src = "block"             # "block": follow the agents talking about our blocked spot; "fence": the live zone

    # ---------------------------------------------------------------- helpers
    def position(self):
        g = self.graph
        a = g.pos[self.path[self.i]]
        if self.i + 1 >= len(self.path):
            return a
        b = g.pos[self.path[self.i + 1]]
        return (a[0] + (b[0] - a[0]) * self.frac, a[1] + (b[1] - a[1]) * self.frac)

    def heading(self):
        g = self.graph
        if self.i + 1 >= len(self.path):
            return 0.0
        return bearing(g.pos[self.path[self.i]], g.pos[self.path[self.i + 1]])

    def done_km(self):
        if self.i + 1 >= len(self.path):
            return self.cum[-1]
        return self.cum[self.i] + (self.cum[self.i + 1] - self.cum[self.i]) * self.frac

    def say(self, text, kind="info", voice=True):
        self.messages.append({"n": len(self.messages), "t": time.time(), "kind": kind, "text": text,
                              "voice": voice})

    def set_path(self, path):
        self.path, self.i, self.frac = path, 0, 0.0
        self.cum = path_km(self.graph, path)
        self.version += 1


class Navigator:
    """All trips; stepped once a second by the dashboard. view_fn() -> TrafficView, publish(ev) -> Kafka."""

    def __init__(self, graph, view_fn, publish, control, story_fn=None, near_fn=None):
        self.graph, self.view_fn, self.publish, self.control = graph, view_fn, publish, control
        self.story_fn = story_fn             # conv id -> list of agent messages (multi-agent explanation)
        self.near_fn = near_fn               # (lat, lon, since_ts) -> conv id of the agents discussing that spot
        self.trips, self.lock = {}, threading.Lock()

    def start(self, src_ll, dst_ll, speedup=5.0):
        g = self.graph
        src, dst = g.nearest(*src_ll), g.nearest(*dst_ll)
        view = self.view_fn()
        path = g.route(src, dst, lambda c: view.at(c, g.pos[c])[:2])
        if not path or len(path) < 2:
            raise ValueError("no road route between these points")
        t = Trip(g, src, dst, speedup)
        t.set_path(path)
        t.say(f"Starting trip to {g.place_name(*g.pos[dst])}. {t.cum[-1]:.1f} kilometres, about "
              f"{eta_s(g, path, view) / 60:.0f} minutes in current traffic.", "start")
        with self.lock:
            self.trips = {k: v for k, v in self.trips.items() if v.status == "driving" and
                          time.time() - v.started < 4 * 3600}
            self.trips[t.id] = t
        return t

    def block_ahead(self, trip, km=1.4):
        """Demo control: report an incident on the route ~km ahead (blr-feed simulates it, TESSERA must detect it)."""
        target = trip.done_km() + km
        j = next((k for k, c in enumerate(trip.cum) if c >= target), len(trip.path) - 1)
        j = max(trip.i + 1, min(j, len(trip.path) - 2))
        lat, lon = self.graph.pos[trip.path[j]]
        self.control({"type": "inject_incident", "lat": lat, "lon": lon, "kind": "Accident"})
        trip.blocked_at = (lat, lon)
        trip.blocked_ts = time.time()
        trip.story_conv, trip.story_src = None, "block"
        trip.blocked_km = trip.cum[j]
        place = self.graph.place_name(lat, lon)
        dist = trip.cum[j] - trip.done_km()
        # The reporting driver avoids the spot straight away (like an accident report in a navigation app);
        # TESSERA's agents still have to confirm it from live traffic before OTHER cars are routed round it.
        detour = self._avoid_spot(trip, (lat, lon))
        if detour:
            turn, towards, saved = detour
            trip.say(f"You reported an accident {dist:.1f} km ahead near {place}. Rerouting. "
                     f"{turn} towards {towards}. "
                     + (f"Detour adds about {-saved:.0f} minutes. " if saved <= -1 else
                        f"Saves about {saved:.0f} minutes. " if saved >= 1 else "")
                     + "TESSERA's agents are now checking it against live traffic.", "reroute")
            return lat, lon
        slow = ""
        if trip.speedup > 1.0:
            # no road around the spot: drive in real time so TESSERA can still decide before the car gets there
            trip.hold_speed = trip.hold_speed or trip.speedup
            trip.speedup = 1.0
            slow = " The car now drives in real time until TESSERA has decided."
        trip.say(f"Demo: an accident was injected {dist:.1f} km ahead near {place}. There is no road around it "
                 f"here; TESSERA has to detect it from the traffic itself." + slow, "demo", voice=False)
        return lat, lon

    # ---------------------------------------------------------------- simulation
    def step_all(self, dt):
        with self.lock:
            trips = [t for t in self.trips.values() if t.status == "driving"]
        if not trips:
            return
        view = self.view_fn()
        for t in trips:
            try:
                self._step(t, dt, view)
            except Exception as e:  # noqa: BLE001
                t.say(f"navigation error: {e}", "error", voice=False)
                t.status = "error"

    def _step(self, t, dt, view):
        g = self.graph
        if t.speedup <= 0:                          # route preview: the car waits at the start
            return
        if t.hold_speed and (time.time() - t.blocked_ts > 300 or t.done_km() > t.blocked_km + 0.1):
            self._release(t)                        # TESSERA never confirmed it, or the car is past it
        lat, lon = t.position()
        kmh, _pen, fence, _crawl = view.at(t.path[min(t.i + 1, len(t.path) - 1)], (lat, lon))
        t.speed = 0.6 * t.speed + 0.4 * (3.0 if fence else kmh)
        move = t.speed * dt * t.speedup / 3600.0
        while move > 0 and t.i + 1 < len(t.path):
            seg = t.cum[t.i + 1] - t.cum[t.i]
            left = seg * (1 - t.frac)
            if move < left and seg > 0:
                t.frac += move / seg
                move = 0
            else:
                move -= left
                t.i, t.frac = t.i + 1, 0.0
        if t.i + 1 >= len(t.path):
            t.status = "arrived"
            t.say(f"You have arrived at {g.place_name(*g.pos[t.path[-1]])}.", "arrived")
            return
        now = time.time()
        if now - t.last_pub >= 3:                      # the car is a vehicle in the stream like any other
            lat, lon = t.position()
            self.publish({"vid": t.vid, "ts": int(now * 1000), "lat": round(lat, 6), "lon": round(lon, 6),
                          "speed": round(t.speed, 2), "heading": round(t.heading(), 1), "crs": "EPSG:4326",
                          "axis": "lat,lon", "src": "nav-car", "vtype": "car"})
            t.last_pub = now
        if now - t.last_check >= 2:
            t.last_check = now
            self._look_ahead(t, view)


    def _avoid_spot(self, t, spot):
        """Re-plan around a reported blockage (cells within r of it are closed). Tries a wide berth first."""
        g = self.graph
        view = self.view_fn()
        for r in (450, 300, 180, 90):
            closed = {c for c, p in g.pos.items()
                      if abs(p[0] - spot[0]) < 0.006 and abs(p[1] - spot[1]) < 0.006
                      and geo.haversine_m(p[0], p[1], spot[0], spot[1]) <= r}
            if t.path[t.i] in closed or t.path[min(t.i + 1, len(t.path) - 1)] in closed:
                continue
            base = lambda c: view.at(c, g.pos[c])[:2]                       # noqa: E731
            plan = self._replan(t, view, lambda c: (base(c)[0], 1e6) if c in closed else base(c))
            if plan and not any(c in closed for c in plan["new"]) and t.dst not in closed:
                t.last_reroute = time.time()
                return self._apply(t, plan)
        return None

    def _replan(self, t, view, cost):
        """Re-plan from where the car really is (on segment a -> b, `done` km past a): either finish the
        segment and route from b ("ahead"), or U-turn now and route from a ("back"). Returns the faster."""
        g = self.graph
        a, b = t.path[t.i], t.path[min(t.i + 1, len(t.path) - 1)]
        seg = max(1e-6, t.cum[min(t.i + 1, len(t.cum) - 1)] - t.cum[t.i])
        done = seg * t.frac
        here_kmh = max(4.0, view.at(b, g.pos[b])[0])
        old_rest = t.path[t.i + 1:]
        old_eta = (seg - done) / here_kmh * 3600 + eta_s(g, old_rest, view)
        opts = []
        fwd = g.route(b, t.dst, cost)
        if fwd and len(fwd) >= 2:
            opts.append(("ahead", (seg - done) / here_kmh * 3600 + eta_s(g, fwd, view), fwd))
        back = g.route(a, t.dst, cost) if a != b else None
        if back and len(back) >= 2 and back[1] != b:
            opts.append(("back", done / here_kmh * 3600 + UTURN_S + eta_s(g, back, view), back))
        if not opts:
            return None
        how, new_eta, new = min(opts, key=lambda o: o[1])
        return {"how": how, "new": new, "new_eta": new_eta, "old_eta": old_eta, "old_rest": old_rest,
                "a": a, "b": b, "done": done}

    def _apply(self, t, plan):
        """Switch the trip to a re-planned route; the car keeps its exact position. -> (turn text, towards, saved min)"""
        g = self.graph
        how, new, old_rest, a, b, done = (plan[k] for k in ("how", "new", "old_rest", "a", "b", "done"))
        if how == "back":
            new_path, frac, j, word, dist_turn = [b] + new, 1.0 - t.frac, 0, "around", 0.0
        else:
            j = 0                                    # where the new route leaves the old one
            while j + 1 < len(new) and j + 1 < len(old_rest) and new[j + 1] == old_rest[j + 1]:
                j += 1
            new_path, frac = [a] + new, t.frac
            dist_turn = path_km(g, new_path)[j + 1] - done
            if j + 1 < len(new) and new[j + 1] == (new[j - 1] if j > 0 else a):
                word = "around"                      # route doubles back at a junction
            elif j + 1 < len(old_rest) and j + 1 < len(new):
                word = turn_word(bearing(g.pos[new[j]], g.pos[old_rest[j + 1]]),
                                 bearing(g.pos[new[j]], g.pos[new[j + 1]]))
            else:
                word = "around"
        towards = g.place_name(*g.pos[new[min(len(new) - 1, j + 25)]])
        saved = (plan["old_eta"] - plan["new_eta"]) / 60
        # keep the abandoned route for the app: it is drawn grey-dashed next to the new one for a while
        here = t.position()
        rest, km = [here], 0.0
        for c0, c1 in zip(old_rest, old_rest[1:]):
            rest.append(g.pos[c0])
            km += g.adj[c0].get(c1, 0.0)
            if km > 2.0:
                break
        t.old_route, t.reroute_ts = rest, time.time()
        if how == "back":
            t.uturn_ts, t.uturn_towards = time.time(), towards
        t.set_path(new_path)
        t.frac = frac
        # after a U-turn the car already faces the new way; the card then shows the next real manoeuvre
        t.turn = None if how == "back" else {"idx": min(j + 1, len(new_path) - 1), "word": word, "towards": towards}
        if how == "back":
            turn = "Make a U-turn now"
        else:
            turn = ("Make a U-turn" if word == "around" else
                    "Continue straight" if word == "straight" else f"Turn {word}") + \
                f" in {round(max(dist_turn, 0) * 100) * 10} metres"
        return turn, towards, saved

    @staticmethod
    def _release(t):
        if t.hold_speed:
            t.speedup, t.hold_speed = t.hold_speed, None

    def _look_ahead(self, t, view):
        g = self.graph
        here = t.done_km()
        hazard = None
        for k in range(t.i + 1, len(t.path)):
            ahead = t.cum[k] - here
            if ahead > LOOKAHEAD_KM:
                break
            if ahead < 0.15:
                continue
            kmh, pen, fence, crawl = view.at(t.path[k], g.pos[t.path[k]])
            if fence or crawl:
                hazard = (k, ahead, fence, kmh)
                break
        if not hazard:
            return
        k, ahead, fence, kmh = hazard
        if fence and fence.get("provenance_id"):
            t.story_conv, t.story_src = fence["provenance_id"], "fence"
        key = fence["id"] if fence else h3.cell_to_parent(t.path[k], config.TILE_RES)
        if time.time() - t.last_reroute < 15:
            return
        where = g.place_name(*g.pos[t.path[k]])
        cause = "an incident zone" if fence else f"heavy congestion ({kmh:.0f} km/h)"
        t.last_reroute = time.time()
        plan = self._replan(t, view, lambda c: view.at(c, g.pos[c])[:2])
        if plan is None:
            return
        same = plan["how"] == "ahead" and plan["new"][:min(len(plan["new"]), 12)] == \
            plan["old_rest"][:min(len(plan["new"]), 12)]
        if same or plan["new_eta"] >= plan["old_eta"] * 0.97:
            if key not in t.warned:
                t.warned.add(key)
                t.say(f"Heads up: {cause} {round(ahead * 100) * 10} metres ahead near {where}. "
                      f"This is still the fastest route, stay on it.", "warn")
            if fence:
                self._release(t)
            return
        turn, towards, saved = self._apply(t, plan)
        t.say(f"{cause[0].upper() + cause[1:]} {round(ahead * 100) * 10} metres ahead near {where}. Rerouting. "
              f"{turn} towards {towards}. "
              + (f"Saves about {saved:.0f} minutes." if saved >= 1 else "Avoids the jam."), "reroute")
        t.warned.add(key)
        if fence:
            self._release(t)

    def state(self, t, since=0, have_version=-1):
        g = self.graph
        view = self.view_fn()
        lat, lon = t.position()
        out = {"id": t.id, "vid": t.vid, "status": t.status, "lat": lat, "lon": lon, "speed": round(t.speed, 1),
               "heading": t.heading(), "version": t.version, "speedup": t.speedup,
               "remaining_km": round(t.cum[-1] - t.done_km(), 2),
               "eta_min": round(eta_s(g, t.path, view, t.i) / 60, 1),
               "messages": t.messages[since:], "blocked_at": t.blocked_at,
               "geofences": [{"lat": f["shape"]["lat"], "lon": f["shape"]["lon"], "r": f["shape"]["radius"],
                              "id": f["id"]} for f in view.geofences]}
        if t.version != have_version:
            out["path"] = [g.pos[c] for c in t.path]
            out["path_kmh"] = [round(view.at(c, g.pos[c])[0], 1) if not view.at(c, g.pos[c])[2] else 3.0 for c in t.path]
        # structured guidance for the app's instruction card
        here = t.done_km()
        if t.turn and t.turn["idx"] > t.i:
            out["next"] = {"word": t.turn["word"], "dist_m": round(max(0.0, t.cum[t.turn["idx"]] - here) * 1000),
                           "towards": t.turn["towards"]}
        else:
            t.turn = None
            out["next"] = {"word": "straight", "dist_m": round(max(0.0, t.cum[-1] - here) * 1000),
                           "towards": g.place_name(*g.pos[t.path[-1]])}
        if time.time() - t.uturn_ts < 10:
            out["next"] = {"word": "around", "dist_m": 0, "towards": t.uturn_towards}
        if time.time() - t.reroute_ts < 30 and t.old_route:
            out["old_path"] = t.old_route
            out["reroute_id"] = int(t.reroute_ts)
        out["dest_name"] = g.place_name(*g.pos[t.path[-1]])
        if t.blocked_at and self.near_fn and t.story_src == "block":
            conv = self.near_fn(t.blocked_at[0], t.blocked_at[1], t.blocked_ts - 5)
            if conv:
                t.story_conv = conv          # follow the agents live (newest attempt) before their zone is on the route
        if t.story_conv and self.story_fn:
            try:
                out["agents"] = {"conv": t.story_conv, "messages": self.story_fn(t.story_conv)[-14:]}
            except Exception:  # noqa: BLE001
                pass
        ahead = []                                  # live tile colours along the route ahead
        for c in t.path[t.i:t.i + 400:4]:
            tl = h3.cell_to_parent(c, config.TILE_RES)
            x = view.tiles.get(tl)
            if x and x[2] >= 3:
                ahead.append({"tile": tl, "speed": round(x[0], 1), "slow": x[1]})
        seen, out["ahead"] = set(), []
        for a in ahead:
            if a["tile"] not in seen:
                seen.add(a["tile"])
                out["ahead"].append({**a, "boundary": [list(p) for p in h3.cell_to_boundary(a["tile"])]})
        return out
