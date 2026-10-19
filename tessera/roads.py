"""Bengaluru road network for the Indian scenario.

The network is the real geometry of BMTC bus routes inside a 14 km radius of central Bengaluru,
taken from the open, unofficial BMTC GTFS data set (github.com/Vonter/bmtc-gtfs, sourced from the
official Namma BMTC app, licence ODbL 1.0). Bus routes run along the city's arterial and
sub-arterial roads, so they are a good free stand-in for the road graph. Roads served by many
routes (Hosur Road, Outer Ring Road, Mysore Road ...) appear many times, so vehicles placed
uniformly along the routes concentrate on busy arterials - like real traffic.

Vehicles move along a route polyline; at shared points (junctions) they may turn onto another
route, and at the end of a route they turn onto a connecting one or U-turn."""
import bisect
import gzip
import json
import math
import os

import h3

ROADS_FILE = os.path.join(os.path.dirname(__file__), "data", "bengaluru_roads.json.gz")
JUNCTION_RES = 9          # H3 res 9 (~0.1 km2): routes sharing a cell are treated as connected
KM_PER_DEG = 111.32


def _hav_km(a, b, c, d):
    p = math.pi / 180
    x = math.sin((c - a) * p / 2) ** 2 + math.cos(a * p) * math.cos(c * p) * math.sin((d - b) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(x))


class RoadNetwork:
    def __init__(self, path=ROADS_FILE):
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            doc = json.load(fh)
        self.city = doc["city"]
        self.center = tuple(doc["center"])
        self.radius_km = doc["radius_km"]
        self.source = doc["source"]
        self.landmarks = doc["landmarks"]
        self.places = doc.get("places", [])      # named BMTC stops, used to describe incident locations
        self.routes = [[tuple(p) for p in r] for r in doc["routes"]]
        self.names = doc["route_names"]
        self.cum = []
        for r in self.routes:
            c = [0.0]
            for a, b in zip(r, r[1:]):
                c.append(c[-1] + _hav_km(a[0], a[1], b[0], b[1]))
            self.cum.append(c)
        self.length = [c[-1] for c in self.cum]
        self.total_km = sum(self.length)
        self._acc = []
        acc = 0.0
        for L in self.length:
            acc += L
            self._acc.append(acc)
        self.junctions = {}             # H3 cell -> [(route, vertex)]
        for ri, r in enumerate(self.routes):
            for vi, (lat, lon) in enumerate(r):
                self.junctions.setdefault(h3.latlng_to_cell(lat, lon, JUNCTION_RES), []).append((ri, vi))

    # ---------------------------------------------------------------- queries
    def random_position(self, rng):
        """Uniform over road length -> busy multi-route arterials get more vehicles."""
        x = rng.random() * self.total_km
        ri = min(bisect.bisect_left(self._acc, x), len(self.routes) - 1)
        return ri, rng.random() * self.length[ri]

    def point(self, ri, s):
        """(lat, lon, heading_deg) at distance s km along route ri."""
        c, r = self.cum[ri], self.routes[ri]
        s = min(max(s, 0.0), c[-1])
        i = min(max(bisect.bisect_right(c, s) - 1, 0), len(r) - 2)
        seg = c[i + 1] - c[i]
        f = 0.0 if seg <= 0 else (s - c[i]) / seg
        (a, b), (p, q) = r[i], r[i + 1]
        lat, lon = a + f * (p - a), b + f * (q - b)
        dn = (p - a) * KM_PER_DEG
        de = (q - b) * KM_PER_DEG * math.cos(math.radians(a))
        return lat, lon, (math.degrees(math.atan2(de, dn)) + 360) % 360

    def vertex_index(self, ri, s):
        return min(max(bisect.bisect_right(self.cum[ri], s) - 1, 0), len(self.routes[ri]) - 1)

    def connections(self, lat, lon, exclude=None):
        return [(r, v) for r, v in self.junctions.get(h3.latlng_to_cell(lat, lon, JUNCTION_RES), [])
                if r != exclude]

    def nearest_vertex(self, lat, lon, max_km=0.4):
        best = None
        for cell in h3.grid_disk(h3.latlng_to_cell(lat, lon, JUNCTION_RES), 3):
            for ri, vi in self.junctions.get(cell, []):
                p = self.routes[ri][vi]
                d = _hav_km(lat, lon, p[0], p[1])
                if d <= max_km and (best is None or d < best[0]):
                    best = (d, ri, vi)
        return best

    def routes_through(self, lat, lon, radius_km):
        """(route, vertex) pairs within radius_km of a point (one entry per route)."""
        out = {}
        k = max(1, int(radius_km / 0.17) + 1)
        for cell in h3.grid_disk(h3.latlng_to_cell(lat, lon, JUNCTION_RES), k):
            for ri, vi in self.junctions.get(cell, []):
                p = self.routes[ri][vi]
                if _hav_km(lat, lon, p[0], p[1]) <= radius_km and ri not in out:
                    out[ri] = vi
        return list(out.items())


class RoadMover:
    """Position of one vehicle on the network."""

    def __init__(self, net, rng, ri=None, s=None, stick_to_route=False, turn_prob=0.25):
        self.net, self.rng = net, rng
        if ri is None:
            ri, s = net.random_position(rng)
        self.ri, self.s = ri, s
        self.dir = 1 if rng.random() < 0.5 else -1
        self.stick = stick_to_route          # buses keep to their own route
        self.turn_prob = turn_prob
        self.lat, self.lon, self.heading = net.point(ri, s)
        self._last_v = net.vertex_index(ri, s)

    def place(self, ri, vi, direction):
        self.ri, self.s, self.dir = ri, self.net.cum[ri][vi], direction
        self._last_v = vi
        self.lat, self.lon, self.heading = self.net.point(ri, self.s)

    def advance(self, dist_km):
        net = self.net
        L = net.length[self.ri]
        self.s += self.dir * dist_km
        if self.s >= L or self.s <= 0:
            self.s = min(max(self.s, 0.0), L)
            lat, lon, _ = net.point(self.ri, self.s)
            options = [] if self.stick else net.connections(lat, lon, exclude=self.ri)
            if options:
                ri, vi = self.rng.choice(options)
                self.place(ri, vi, 1 if self.rng.random() < 0.5 else -1)
            else:
                self.dir = -self.dir               # end of the road: U-turn
        else:
            v = net.vertex_index(self.ri, self.s)
            if v != self._last_v:                  # passed a vertex: maybe turn at this junction
                self._last_v = v
                if not self.stick and self.rng.random() < self.turn_prob:
                    p = net.routes[self.ri][v]
                    options = net.connections(p[0], p[1], exclude=self.ri)
                    if options:
                        ri, vi = self.rng.choice(options)
                        self.place(ri, vi, 1 if self.rng.random() < 0.5 else -1)
                        return
        lat, lon, hd = net.point(self.ri, self.s)
        self.lat, self.lon = lat, lon
        if dist_km > 0:
            self.heading = hd if self.dir > 0 else (hd + 180) % 360


_LANDMARKS = None


def nearest_landmark(lat, lon, max_km=1.5):
    """Name of the closest known Bengaluru bottleneck within max_km, or None (cheap; used in agent text)."""
    global _LANDMARKS
    if _LANDMARKS is None:
        try:
            with gzip.open(ROADS_FILE, "rt", encoding="utf-8") as fh:
                _LANDMARKS = json.load(fh)["landmarks"]
        except OSError:
            _LANDMARKS = []
    best = None
    for lm in _LANDMARKS:
        d = _hav_km(lat, lon, lm["lat"], lm["lon"])
        if d <= max_km and (best is None or d < best[0]):
            best = (d, lm["name"])
    return best[1] if best else None


def overlay_edges(precision=3):
    """De-duplicated road edges (vertices rounded to ~100 m) for drawing the network on the dashboard map."""
    net = RoadNetwork()
    edges = set()
    for r in net.routes:
        q = [(round(a, precision), round(b, precision)) for a, b in r]
        for a, b in zip(q, q[1:]):
            if a != b:
                edges.add((min(a, b), max(a, b)))
    return [[list(a), list(b)] for a, b in edges]
