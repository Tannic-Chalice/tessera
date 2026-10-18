"""DATA SOURCE -> Kafka.  Kafka producer for georeferenced vehicle positions.

Modes
  synthetic (default): a live feed of Indian mixed traffic on real Bengaluru roads. It stands in for
                       the GPS feeds a city traffic control centre receives in India - AIS-140 vehicle
                       location trackers on buses, cabs and goods vehicles, and fleet / ride-hailing
                       apps on two-wheelers, autos and cars. Vehicles drive along the real road
                       geometry of BMTC bus routes; incidents (waterlogging, breakdowns, accidents,
                       Metro work ...) are injected at known Bengaluru bottlenecks with ground truth on
                       scenario.truth. Out-of-order, very late and malformed records are included on
                       purpose - everything the pipeline must cope with.
                       (CITY=beijing gives the older random-walk taxi fleet in Beijing.)
  delhi              : REAL live data - polls the Delhi Open Transit Data GTFS-Realtime feed
                       (live GPS of Delhi's DTC / cluster buses, ~every 10 s) and publishes every new
                       bus position to gps.raw. Needs a free key from https://otd.delhi.gov.in/data/realtime/
  tdrive             : replays Microsoft T-Drive taxi traces (put the .txt files in data/tdrive).

Partitioning: the record key is the coarse H3 cell (res 6) of the position, so all events for a
neighbourhood go to the same partition -> partition locality == spatial locality."""
import glob
import heapq
import math
import os
import random
import time
import uuid
import zlib

from .. import config, geo, roads
from ..kafka_utils import GracefulExit, Metrics, dumps, make_producer, setup_logging, wait_for_kafka, wait_for_topics

log = setup_logging("producer")
rng = random.Random(int(os.environ.get("SEED", "42")))

KM_PER_DEG_LAT = 111.32


def offset(lat, lon, dn_km, de_km):
    return lat + dn_km / KM_PER_DEG_LAT, lon + de_km / (KM_PER_DEG_LAT * math.cos(math.radians(lat)))


def random_point(radius_km):
    r = radius_km * math.sqrt(rng.random())
    a = rng.random() * 2 * math.pi
    return offset(config.CITY_LAT, config.CITY_LON, r * math.cos(a), r * math.sin(a))


class Incident:
    def __init__(self, sim_now):
        self.id = "inc_" + uuid.uuid4().hex[:8]
        self.lat, self.lon = random_point(config.CITY_RADIUS_KM * 0.6)
        self.radius_m = rng.uniform(600, 900)
        self.start = sim_now
        self.end = sim_now + config.INCIDENT_DURATION_S

    def contains(self, lat, lon, margin=1.0):
        return geo.haversine_m(lat, lon, self.lat, self.lon) <= self.radius_m * margin


class Vehicle:
    def __init__(self, i):
        self.vid = f"taxi-{i:04d}"
        self.lat, self.lon = random_point(config.CITY_RADIUS_KM)
        self.fleet_b = i % 10 == 0          # fleet B reports lon,lat (declared) - tests axis handling
        self.new_target()
        self.heading = 0.0
        self.speed = rng.uniform(25, 50)

    def new_target(self, near=None):
        if near is not None:
            self.tlat, self.tlon = offset(near.lat, near.lon, rng.uniform(-0.5, 0.5), rng.uniform(-0.5, 0.5))
        else:
            self.tlat, self.tlon = random_point(config.CITY_RADIUS_KM)

    def step(self, dt_s, incidents):
        base = 42.0
        # permanent slower downtown core gives the field realistic spatial structure
        d_core = geo.haversine_m(self.lat, self.lon, config.CITY_LAT, config.CITY_LON)
        if d_core < 2500:
            base = 28.0
        target = base + rng.gauss(0, 6)
        for inc in incidents:
            if inc.contains(self.lat, self.lon):
                target = rng.uniform(1.0, 7.0)
            elif inc.contains(self.lat, self.lon, margin=1.6):
                target = min(target, rng.uniform(8, 18))
        self.speed = max(0.0, 0.5 * self.speed + 0.5 * target)
        dist_km = self.speed * dt_s / 3600.0
        dn = (self.tlat - self.lat) * KM_PER_DEG_LAT
        de = (self.tlon - self.lon) * KM_PER_DEG_LAT * math.cos(math.radians(self.lat))
        d = math.hypot(dn, de)
        if d < max(dist_km, 0.05):
            self.new_target()
            return
        self.heading = (math.degrees(math.atan2(de, dn)) + 360) % 360
        self.lat, self.lon = offset(self.lat, self.lon, dn / d * dist_km, de / d * dist_km)


# ======================================================================== Bengaluru scenario
# Mixed Indian traffic on real BMTC road geometry. Calibrated against the REAL live BMTC buses in the same
# system (median ~13 km/h, ~40 % below 10 km/h at 10 pm): simulated city median ~9 km/h at 9:30 am, ~8 km/h at
# 6:30 pm, ~12 km/h at 10 pm and ~23 km/h at 2 am, worst at the chronic bottlenecks. Shares, speeds and severities below are modelling assumptions, not measurements.
VEHICLE_TYPES = [
    #  type          prefix  share  free-flow km/h (mean, sd)  jam km/h (min, max)
    ("two_wheeler", "2W", 0.50, (32.0, 7.0), (3.0, 9.0)),       # filter through queues -> still slow
    ("car", "CAR", 0.22, (30.0, 6.0), (1.0, 6.0)),
    ("auto", "AUTO", 0.12, (26.0, 5.0), (1.0, 6.0)),
    ("cab", "CAB", 0.09, (30.0, 6.0), (1.0, 6.0)),
    ("bus", "BUS", 0.03, (22.0, 4.0), (1.0, 5.0)),             # few: real BMTC buses come from the live API
    ("goods", "LCV", 0.04, (25.0, 5.0), (1.0, 5.0)),
]
INCIDENT_TYPES = ["Waterlogging after rain", "Vehicle breakdown", "Accident", "Metro / road work",
                  "Procession / VIP movement", "Tree fall", "Signal failure"]
# hour of day (IST) -> share of free-flow speed for the whole city (typical weekday profile)
DAY_PROFILE = [0.90, 0.97, 1.00, 1.00, 0.95, 0.86, 0.76, 0.63, 0.51, 0.47, 0.51, 0.59, 0.63, 0.63, 0.61, 0.57,
               0.51, 0.46, 0.42, 0.42, 0.46, 0.52, 0.60, 0.74]
# (the evening peak runs late: the live BMTC feed itself shows ~13 km/h median and ~40 % of buses below
#  10 km/h at 10 pm, so traffic only thins out after ~11 pm)
# chronic bottlenecks: how badly each junction chokes at peak (0..1); others default to 0.5
BOTTLENECK_SEVERITY = {
    "Central Silk Board": 0.9, "KR Puram / Tin Factory": 0.85, "Hebbal Flyover": 0.8,
    "Marathahalli Bridge": 0.8, "Bellandur (ORR)": 0.8, "Sarjapur Road (Ibbaluru) Junction": 0.75,
    "Electronic City": 0.7, "Goraguntepalya (Tumkur Road)": 0.75, "Hosur Road Junction": 0.7,
    "Majestic (Kempegowda Bus Station)": 0.7, "KR Market": 0.7, "Whitefield (Vydehi)": 0.65,
    "Madivala": 0.65, "Mekhri Circle": 0.6, "Nagavara Junction": 0.6, "Yeshwanthapura Circle": 0.6,
    "Koramangala Water Tank": 0.55, "Domlur Flyover": 0.55, "MG Road": 0.55,
    "Mysore Road Satellite Bus Station": 0.55, "Banashankari": 0.5, "RV College (Mysore Road)": 0.45,
}
BOTTLENECK_RADIUS_KM = 1.6
SIM_HOUR = [None]          # demo control: force a time of day (e.g. 18.0 = 6 pm peak); None = real IST clock
_NET = None


def road_network():
    global _NET
    if _NET is None:
        _NET = roads.RoadNetwork()
    return _NET


def ist_hour(ts_s):
    if SIM_HOUR[0] is not None:
        return SIM_HOUR[0]
    return ((ts_s + 5.5 * 3600) % 86400) / 3600.0


def day_factor(ts_s):
    h = ist_hour(ts_s)
    i = int(h) % 24
    f = h - int(h)
    return DAY_PROFILE[i] * (1 - f) + DAY_PROFILE[(i + 1) % 24] * f


def peak_factor(ts_s):                 # kept for compatibility
    return day_factor(ts_s)


def peakness(ts_s):
    """0 at night, 1 at the worst peak; bottlenecks never fully clear in the daytime."""
    return min(1.0, max(0.0, (1.0 - day_factor(ts_s)) / 0.55))


def congestion_at(lat, lon, ts_s):
    """Chronic congestion (0..1) from the nearest bottlenecks at this time of day."""
    pk = peakness(ts_s)
    if pk <= 0:
        return 0.0
    best = 0.0
    for lm in road_network().landmarks:
        d = KM_PER_DEG_LAT * math.hypot(lat - lm["lat"], (lon - lm["lon"]) * 0.975)
        if d < 3 * BOTTLENECK_RADIUS_KM:
            c = BOTTLENECK_SEVERITY.get(lm["name"], 0.5) * math.exp(-(d / BOTTLENECK_RADIUS_KM) ** 2)
            best = max(best, c)
    return best * (0.45 + 0.55 * pk)


def nearest_place(lat, lon):
    net = road_network()
    best = None
    for pl in getattr(net, "places", []) or net.landmarks:
        d = math.hypot(lat - pl["lat"], (lon - pl["lon"]) * 0.975)
        if best is None or d < best[0]:
            best = (d, pl["name"])
    return best[1] if best else "Bengaluru"


class BlrIncident:
    def __init__(self, sim_now, avoid=()):
        net = road_network()
        if rng.random() < 0.5:                       # at a known bottleneck ...
            free = [lm for lm in net.landmarks if lm["name"] not in avoid] or net.landmarks
            lm = rng.choice(free)
            snap = net.nearest_vertex(lm["lat"], lm["lon"], max_km=0.4)
            self.lat, self.lon = net.routes[snap[1]][snap[2]] if snap else (lm["lat"], lm["lon"])
            self.place = lm["name"]
        else:                                        # ... or anywhere on the road network
            for _ in range(50):
                ri, sd = net.random_position(rng)
                lat, lon, _h = net.point(ri, sd)
                if geo.haversine_m(lat, lon, *net.center) < 11000:
                    break
            self.lat, self.lon = lat, lon
            self.place = "near " + nearest_place(lat, lon)
        self.id = "inc_" + uuid.uuid4().hex[:8]
        self.kind = rng.choice(INCIDENT_TYPES)
        self.radius_m = rng.uniform(550, 850)
        self.start = sim_now
        self.end = sim_now + config.INCIDENT_DURATION_S

    def contains(self, lat, lon, margin=1.0):
        return geo.haversine_m(lat, lon, self.lat, self.lon) <= self.radius_m * margin


def pick_type(i):
    x = (i * 0.6180339887) % 1.0                 # deterministic, well-spread type assignment
    acc = 0.0
    for t in VEHICLE_TYPES:
        acc += t[2]
        if x < acc:
            return t
    return VEHICLE_TYPES[-1]


class BlrVehicle:
    def __init__(self, i):
        self.vtype, prefix, _share, self.free, self.jam = pick_type(i)
        self.vid = f"{prefix}-{i:04d}"
        net = road_network()
        self.mover = roads.RoadMover(net, rng, stick_to_route=self.vtype == "bus",
                                     turn_prob=0.15 if self.vtype == "goods" else 0.3)
        self.lat, self.lon, self.heading = self.mover.lat, self.mover.lon, self.mover.heading
        self.fleet_b = i % 10 == 0          # aggregator feed B sends lon,lat (declared) - tests axis handling
        self.speed = max(5.0, rng.gauss(*self.free) * 0.6)
        self.stop_left = 0.0                # seconds still waiting at a signal / bus stop

    def step(self, dt_s, incidents, sim_now):
        net = road_network()
        cong = congestion_at(self.lat, self.lon, sim_now)
        target = max(4.0, rng.gauss(*self.free)) * day_factor(sim_now) * (1.0 - 0.85 * cong)
        if geo.haversine_m(self.lat, self.lon, net.center[0], net.center[1]) < 3000:
            target *= 0.85                                  # dense central business district
        jammed = False
        for inc in incidents:
            if inc.contains(self.lat, self.lon):
                target, jammed = rng.uniform(*self.jam), True
            elif inc.contains(self.lat, self.lon, margin=1.6):
                target = min(target, rng.uniform(5, 14))    # queue spilling back on approach roads
        # stop-and-go: red signals roughly every 0.8 km (longer and more frequent where it is congested),
        # BMTC buses also dwell at stops roughly every 0.5 km
        if self.stop_left > 0:
            self.stop_left -= dt_s
            target = rng.uniform(0.0, 2.5)
        elif not jammed:
            km = max(target, 5.0) * dt_s / 3600.0
            busy = 0.3 + 0.7 * peakness(sim_now)            # fewer, shorter signal stops late at night
            if rng.random() < busy * km / 0.6:              # Bengaluru signals: long cycles, queues 2-3 cycles deep
                self.stop_left = rng.uniform(15, 50) * busy * (1 + 1.5 * cong)
            elif self.vtype == "bus" and rng.random() < km / 0.5:
                self.stop_left = rng.uniform(15, 40)
        self.speed = max(0.0, 0.4 * self.speed + 0.6 * target)
        self.mover.advance(self.speed * dt_s / 3600.0)
        self.lat, self.lon, self.heading = self.mover.lat, self.mover.lon, self.mover.heading

    def send_towards(self, inc):
        """Put the vehicle on a road leading into the incident, 0.8-2 km upstream (traffic keeps arriving)."""
        net = road_network()
        through = net.routes_through(inc.lat, inc.lon, inc.radius_m / 1000.0)
        if not through:
            return
        ri, vi = rng.choice(through)
        c = net.cum[ri]
        back = rng.uniform(0.8, 2.0)
        if rng.random() < 0.5 and c[vi] - back > 0:
            s, d = c[vi] - back, 1
        elif c[vi] + back < c[-1]:
            s, d = c[vi] + back, -1
        else:
            return
        self.mover.place(ri, net.vertex_index(ri, s), d)
        self.lat, self.lon, self.heading = self.mover.lat, self.mover.lon, self.mover.heading


def make_event(v, ts_ms):
    ev = {"vid": v.vid, "ts": ts_ms, "speed": round(v.speed, 2), "heading": round(v.heading, 1),
          "crs": "EPSG:4326", "src": "synthetic"}
    if hasattr(v, "vtype"):
        ev["vtype"] = v.vtype
        ev["src"] = "sim-blr"
    if v.fleet_b:
        ev.update({"lat": round(v.lon, 6), "lon": round(v.lat, 6), "axis": "lon,lat"})
    else:
        ev.update({"lat": round(v.lat, 6), "lon": round(v.lon, 6), "axis": "lat,lon"})
    return ev


def corrupt(ev):
    kind = rng.choice(["range", "missing", "crs", "speed"])
    if kind == "range":
        ev["lat"] = 999.0
    elif kind == "missing":
        ev.pop("vid", None)
    elif kind == "crs":
        ev["crs"] = "EPSG:9999"
    else:
        ev["speed"] = -40
    return ev


class Sender:
    """Delivers events, deliberately delaying some of them to create out-of-order / late arrivals."""

    def __init__(self, producer, metrics):
        self.p, self.m = producer, metrics
        self.delayed = []    # heap of (release_wall, seq, event)
        self.seq = 0
        self.sent = self.failed = 0
        self.stats = {"ooo": 0, "very_late": 0, "malformed": 0}

    def _cb(self, err, _msg):
        if err is not None:
            self.failed += 1
            log.warning("delivery failed: %s", err)

    def submit(self, ev):
        r = rng.random()
        if r < config.MALFORMED_FRAC:
            ev = corrupt(ev)
            self.stats["malformed"] += 1
        elif r < config.MALFORMED_FRAC + config.VERY_LATE_FRAC:
            self.stats["very_late"] += 1
            return self._delay(ev, rng.uniform(75, 95) / config.SPEEDUP)   # > window size + watermark + lateness
        elif r < config.MALFORMED_FRAC + config.VERY_LATE_FRAC + config.OUT_OF_ORDER_FRAC:
            self.stats["ooo"] += 1
            return self._delay(ev, rng.uniform(0.5, 3.5) / config.SPEEDUP)
        self._send(ev)

    def _delay(self, ev, s):
        self.seq += 1
        heapq.heappush(self.delayed, (time.time() + s, self.seq, ev))

    def release(self):
        now = time.time()
        while self.delayed and self.delayed[0][0] <= now:
            self._send(heapq.heappop(self.delayed)[2])

    def _send(self, ev):
        ev["ingest_ms"] = int(time.time() * 1000)
        part = None
        try:
            lat, lon = (ev["lon"], ev["lat"]) if ev.get("axis") == "lon,lat" else (ev["lat"], ev["lon"])
            key = geo.parent(geo.tile(lat, lon, config.TILE_RES), config.PARTITION_RES)
            if config.CITY_PARTITIONS:              # region partitioning: each city on its own partitions
                parts = config.CITY_PARTITIONS[config.city_of(lat, lon)]
                part = parts[zlib.crc32(key.encode()) % len(parts)]
        except Exception:  # noqa: BLE001  malformed record: route by random key, fast plane will DLQ it
            key = "malformed"
            if config.CITY_PARTITIONS:
                part = config.CITY_PARTITIONS[config.CITY][0]
        while True:
            try:
                if part is None:
                    self.p.produce(config.T_RAW, key=key.encode(), value=dumps(ev), on_delivery=self._cb)
                else:
                    self.p.produce(config.T_RAW, key=key.encode(), value=dumps(ev), partition=part,
                                   on_delivery=self._cb)
                break
            except BufferError:      # local queue full -> back-pressure: wait for deliveries
                self.p.poll(0.2)
        self.sent += 1
        self.p.poll(0)


def run_synthetic(stop):
    p = make_producer()
    metrics = Metrics(p, "producer")
    sender = Sender(p, metrics)
    blr = config.CITY == "bengaluru"
    if blr:
        net = road_network()
        log.info("Bengaluru road network: %d BMTC route segments, %.0f km of road geometry, %d bottleneck "
                 "landmarks", len(net.routes), net.total_km, len(net.landmarks))
    fleet = [BlrVehicle(i) if blr else Vehicle(i) for i in range(config.NUM_VEHICLES)]
    incidents = []
    wall0 = time.time()
    sim0 = wall0
    next_incident = sim0 + 45          # first incident after ~45 s so baselines warm up
    last_metric = time.time()
    tick = config.REPORT_INTERVAL_S
    log.info("synthetic fleet of %d vehicles in %s around (%.4f, %.4f), speedup x%g",
             len(fleet), config.CITY_NAME, config.CITY_LAT, config.CITY_LON, config.SPEEDUP)
    next_tick = time.time()
    while not stop.stop:
        sim_now = sim0 + (time.time() - wall0) * config.SPEEDUP
        if sim_now >= next_incident:
            inc = BlrIncident(sim_now, avoid={i.place for i in incidents}) if blr else Incident(sim_now)
            incidents.append(inc)
            next_incident = sim_now + config.INCIDENT_EVERY_S
            truth = {"incident_id": inc.id, "lat": inc.lat, "lon": inc.lon, "radius_m": inc.radius_m,
                     "start_ts": inc.start, "end_ts": inc.end}
            if blr:
                truth.update({"place": inc.place, "kind": inc.kind})
            p.produce(config.T_TRUTH, key=inc.id.encode(), value=dumps(truth))
            # some traffic is drawn towards the incident (queues build up)
            movers = [v for v in fleet if getattr(v, "vtype", "") != "bus"]
            for v in rng.sample(movers, k=max(1, len(fleet) // 7)):
                if blr:
                    v.send_towards(inc)
                else:
                    v.new_target(near=inc)
            log.info("INJECTED incident %s %s at (%.5f, %.5f) r=%.0fm for %ds", inc.id,
                     f"[{inc.kind} @ {inc.place}]" if blr else "", inc.lat, inc.lon, inc.radius_m,
                     config.INCIDENT_DURATION_S)
        incidents = [i for i in incidents if i.end > sim_now]

        ts_ms = int(sim_now * 1000)
        order = list(fleet)
        rng.shuffle(order)
        for v in order:
            if blr:
                v.step(tick, incidents, sim_now)
            else:
                v.step(tick, incidents)
            # stagger timestamps inside the tick like real asynchronous GPS units
            sender.submit(make_event(v, ts_ms - int(rng.random() * tick * 1000)))
        sender.release()

        if time.time() - last_metric >= 5:
            dt = time.time() - last_metric
            metrics.emit({"events_per_s": sender.sent / dt, "delivery_failures": sender.failed,
                          "active_incidents": len(incidents), **sender.stats})
            log.info("sent %.0f ev/s, active incidents=%d, delayed=%d, ooo=%d very_late=%d malformed=%d",
                     sender.sent / dt, len(incidents), len(sender.delayed), sender.stats["ooo"],
                     sender.stats["very_late"], sender.stats["malformed"])
            sender.sent = 0
            last_metric = time.time()

        next_tick += tick / config.SPEEDUP
        while time.time() < next_tick and not stop.stop:
            sender.release()
            p.poll(0.05)
    log.info("flushing producer ...")
    p.flush(30)


# ======================================================================== Delhi: real live feed
PARKED_AFTER_S = 180      # a bus that has not moved 40 m in 3 min is parked (depot / terminal) - not traffic
PARKED_RADIUS_M = 40


def parse_vehicle_positions(blob, speed_unit="ms"):
    """GTFS-Realtime FeedMessage bytes -> list of dicts (vid, ts_s, lat, lon, speed_kmh or None)."""
    from google.transit import gtfs_realtime_pb2  # noqa: PLC0415  (only needed in this mode)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(blob)
    header_ts = feed.header.timestamp if feed.header.HasField("timestamp") else None
    out = []
    for ent in feed.entity:
        if not ent.HasField("vehicle") or not ent.vehicle.HasField("position"):
            continue
        v = ent.vehicle
        vid = (v.vehicle.id or v.vehicle.label or ent.id) if v.HasField("vehicle") else ent.id
        ts = v.timestamp or header_ts
        pos = v.position
        if not vid or not ts:
            continue
        speed = None
        if pos.HasField("speed"):
            speed = pos.speed * (3.6 if speed_unit == "ms" else 1.0)
        label = v.vehicle.label if v.HasField("vehicle") else ""
        out.append({"vid": str(vid), "ts_s": int(ts), "lat": pos.latitude, "lon": pos.longitude, "speed": speed,
                    "label": label,
                    "heading": pos.bearing if pos.HasField("bearing") else None,
                    "route": v.trip.route_id if v.HasField("trip") else None})
    return out, header_ts


class LiveFeedState:
    """Turns repeated snapshots into a stream: new reports only, speed from movement if missing,
    parked buses filtered out, positions outside the city ignored."""

    def __init__(self, city=None, parked_filter=True, src="otd-delhi", default_vtype="bus", speed_from_movement=True):
        key = city or config.CITY
        self.clat, self.clon, self.radius_km = config.CITY_PROFILES[key][:3] if city else (
            config.CITY_LAT, config.CITY_LON, config.CITY_RADIUS_KM)
        self.parked_filter, self.src, self.default_vtype = parked_filter, src, default_vtype
        self.speed_from_movement = speed_from_movement
        self.last = {}      # vid -> (ts, lat, lon)
        self.still = {}     # vid -> (since_ts, lat, lon)
        self.stats = {"new": 0, "duplicate": 0, "parked": 0, "outside_city": 0}

    def events(self, rows):
        out = []
        for r in rows:
            vid, ts, lat, lon = r["vid"], r["ts_s"], r["lat"], r["lon"]
            prev = self.last.get(vid)
            if prev and ts <= prev[0]:
                self.stats["duplicate"] += 1
                continue
            if geo.haversine_m(lat, lon, self.clat, self.clon) > self.radius_km * 1000:
                self.stats["outside_city"] += 1
                self.last[vid] = (ts, lat, lon)
                continue
            speed = r["speed"]
            if self.speed_from_movement and prev and 5 <= ts - prev[0] <= 120:
                # speed from actual movement between two fixes: many GPS units report 0 or a stale speed
                speed = geo.haversine_m(prev[1], prev[2], lat, lon) / (ts - prev[0]) * 3.6
            self.last[vid] = (ts, lat, lon)
            st = self.still.get(vid)
            if st is None or geo.haversine_m(st[1], st[2], lat, lon) > PARKED_RADIUS_M:
                self.still[vid] = (ts, lat, lon)
            elif self.parked_filter and ts - st[0] >= PARKED_AFTER_S:
                self.stats["parked"] += 1
                continue
            if speed is None:
                continue                                   # first sighting without speed: wait for the next one
            self.stats["new"] += 1
            ev = {"vid": vid, "ts": ts * 1000, "lat": round(lat, 6), "lon": round(lon, 6),
                  "speed": round(min(max(speed, 0.0), 120.0), 2), "crs": "EPSG:4326", "axis": "lat,lon",
                  "src": self.src, "vtype": r.get("label") or self.default_vtype}
            if r.get("heading") is not None:
                ev["heading"] = round(r["heading"], 1)
            if r.get("route"):
                ev["route"] = r["route"]
            out.append(ev)
        return out


def run_delhi(stop):
    import urllib.request  # noqa: PLC0415
    if not config.OTD_API_KEY:
        log.error("SOURCE_MODE=delhi needs OTD_API_KEY (free key: https://otd.delhi.gov.in/data/realtime/). "
                  "Start with start-delhi.bat, which asks for it.")
        while not stop.stop:
            time.sleep(5)
        return
    p = make_producer()
    metrics = Metrics(p, "producer")
    sender = Sender(p, metrics)
    state = LiveFeedState()
    url = f"{config.OTD_URL}?key={config.OTD_API_KEY}"
    log.info("live mode: polling Delhi Open Transit Data every %.0f s", config.OTD_POLL_S)
    failures = 0
    while not stop.stop:
        t0 = time.time()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "TESSERA-student-project/1.0"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                blob = resp.read()
            rows, header_ts = parse_vehicle_positions(blob, config.OTD_SPEED_UNIT)
            evs = state.events(rows)
            for ev in evs:
                sender._send(ev)                          # real data: no artificial defects added
            p.poll(0)
            failures = 0
            age = (time.time() - header_ts) if header_ts else None
            metrics.emit({"events_per_s": len(evs) / config.OTD_POLL_S, "feed_vehicles": len(rows),
                          "feed_age_s": age if age is not None else -1, **state.stats})
            log.info("OTD feed: %d buses in snapshot, %d new positions sent, feed age %s s, parked %d, "
                     "outside city %d", len(rows), len(evs), f"{age:.0f}" if age is not None else "?",
                     state.stats["parked"], state.stats["outside_city"])
        except Exception as e:  # noqa: BLE001  network / key / parse problems: keep retrying with backoff
            failures += 1
            log.warning("OTD feed request failed (%s) - retry %d", str(e).replace(config.OTD_API_KEY, "***"),
                        failures)
        wait = config.OTD_POLL_S if failures == 0 else min(60.0, config.OTD_POLL_S * (2 ** min(failures, 3)))
        while time.time() - t0 < wait and not stop.stop:
            p.poll(0.2)
    p.flush(30)


def _fetch(url, timeout=30):
    import urllib.request  # noqa: PLC0415
    req = urllib.request.Request(url, headers={"User-Agent": "TESSERA-student-project/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def run_india(stop):
    """Both cities through ONE GTFS-Realtime ingestion path:
       Delhi     - real live buses from Delhi Open Transit Data (every ~10 s)
       Bengaluru - simulated BMTC-style GTFS-Realtime feed served by the blr-feed service (every 5 s);
                   as a simulated device feed it also gets the usual late / out-of-order / malformed defects
                 + REAL BMTC buses from the Namma BMTC API (tessera/bmtc.py) when BMTC_LIVE=1."""
    p = make_producer()
    metrics = Metrics(p, "producer")
    sender = Sender(p, metrics)
    feeds = [{"name": "Bengaluru", "url": config.BLR_FEED_URL, "every": config.BLR_REPORT_S, "next": 0.0,
              "state": LiveFeedState("bengaluru", parked_filter=False, src="sim-blr-gtfsrt", default_vtype="vehicle",
                                     speed_from_movement=False),            # simulated units report true speed
              "defects": True, "fail": 0, "sent": 0, "speed_unit": "ms"}]
    if config.OTD_API_KEY:
        feeds.append({"name": "Delhi", "url": f"{config.OTD_URL}?key={config.OTD_API_KEY}", "every": config.OTD_POLL_S,
                      "next": 0.0, "state": LiveFeedState("delhi", parked_filter=True, src="otd-delhi"),
                      "defects": False, "fail": 0, "sent": 0, "speed_unit": config.OTD_SPEED_UNIT})
    else:
        log.warning("no OTD_API_KEY - running Bengaluru only (get a free key: https://otd.delhi.gov.in/data/realtime/)")
    if config.BMTC_LIVE:                       # REAL Bengaluru buses (Namma BMTC API), on top of the simulated traffic
        from ..bmtc import BmtcPoller  # noqa: PLC0415
        poller = BmtcPoller()
        poller.start()
        feeds.append({"name": "BMTC-live", "fetch": lambda: (poller.drain(), None), "count": poller.live_buses,
                      "every": 5.0, "next": 0.0,
                      "state": LiveFeedState("bengaluru", parked_filter=True, src="bmtc-live"),
                      "defects": False, "fail": 0, "sent": 0})
        log.info("BMTC live: polling %d routes, one request every %.1f s", len(poller.routes),
                 config.BMTC_REQ_INTERVAL_S)
    log.info("india mode: %s", ", ".join(f"{f['name']} every {f['every']:.0f} s" for f in feeds))
    last_metric = time.time()
    while not stop.stop:
        now = time.time()
        for f in feeds:
            if now < f["next"]:
                continue
            try:
                rows, header_ts = f["fetch"]() if "fetch" in f else parse_vehicle_positions(_fetch(f["url"]),
                                                                                           f["speed_unit"])
                evs = f["state"].events(rows)
                for ev in evs:
                    if f["defects"]:
                        sender.submit(ev)
                    else:
                        sender._send(ev)
                f["sent"] += len(evs)
                f["age"] = (time.time() - header_ts) if header_ts else None
                if evs:                                # what the feed looks like: speeds and how old the fixes are
                    sp = sorted(e["speed"] for e in evs)
                    lag = sorted(time.time() - e["ts"] / 1000 for e in evs)
                    f["diag"] = (sp[len(sp) // 2], 100.0 * sum(x < 10 for x in sp) / len(sp), lag[len(lag) // 2])
                f["n"] = f["count"]() if "count" in f else len(rows)
                f["fail"] = 0
                f["next"] = now + f["every"]
            except Exception as e:  # noqa: BLE001
                f["fail"] += 1
                f["next"] = now + min(60.0, f["every"] * (2 ** min(f["fail"], 3)))
                msg = str(e).replace(config.OTD_API_KEY, "***") if config.OTD_API_KEY else str(e)
                if f["fail"] <= 3 or f["fail"] % 10 == 0:
                    log.warning("%s feed request failed (%s) - retry %d", f["name"], msg, f["fail"])
        sender.release()
        if time.time() - last_metric >= 10:
            dt = time.time() - last_metric
            vals = {"events_per_s": sum(f["sent"] for f in feeds) / dt, **sender.stats}
            for f in feeds:
                k = f["name"].lower().replace("-", "_")
                vals[f"{k}_events_per_s"] = f["sent"] / dt
                vals[f"{k}_vehicles"] = f.get("n", 0)
                vals[f"{k}_feed_ok"] = 1 if f["fail"] == 0 and "n" in f else 0
            metrics.emit(vals)
            log.info(" | ".join(f"{f['name']}: {f.get('n', 0)} vehicles, {f['sent'] / dt:.0f} ev/s"
                                + (f", feed age {f['age']:.0f} s" if f.get("age") is not None else "")
                                + (", median speed {:.0f} km/h, {:.0f}% < 10 km/h, fixes {:.0f} s old".format(*f["diag"])
                                   if f.get("diag") else "")
                                + (" [FAILING]" if f["fail"] else "") for f in feeds))
            for f in feeds:
                f["sent"] = 0
            last_metric = time.time()
        p.poll(0.1)
    p.flush(30)


def load_tdrive(path, max_taxis):
    files = sorted(glob.glob(os.path.join(path, "*.txt")))[:max_taxis]
    pts = []
    for f in files:
        last = None
        with open(f, encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                parts = line.strip().split(",")
                if len(parts) != 4:
                    continue
                try:
                    vid, ts = parts[0], time.mktime(time.strptime(parts[1], "%Y-%m-%d %H:%M:%S"))
                    lon, lat = float(parts[2]), float(parts[3])
                except ValueError:
                    continue
                speed = 0.0
                if last and ts > last[0]:
                    speed = geo.haversine_m(last[1], last[2], lat, lon) / (ts - last[0]) * 3.6
                last = (ts, lat, lon)
                pts.append((ts, vid, lat, lon, min(speed, 150.0)))
    pts.sort()
    return pts, len(files)


def run_tdrive(stop):
    pts, n = load_tdrive(config.TDRIVE_DIR, int(os.environ.get("MAX_TAXIS", "500")))
    if not pts:
        log.error("no T-Drive files found in %s - falling back to synthetic mode", config.TDRIVE_DIR)
        return run_synthetic(stop)
    log.info("replaying %d T-Drive points from %d taxis at x%g", len(pts), n, config.SPEEDUP)
    p = make_producer()
    metrics = Metrics(p, "producer")
    sender = Sender(p, metrics)
    t_data0, wall0 = pts[0][0], time.time()
    last_metric = time.time()
    for ts, vid, lat, lon, speed in pts:
        if stop.stop:
            break
        due = wall0 + (ts - t_data0) / config.SPEEDUP
        while time.time() < due and not stop.stop:
            sender.release()
            p.poll(0.02)
        ev = {"vid": vid, "ts": int((wall0 + (ts - t_data0)) * 1000), "lat": lat, "lon": lon,
              "speed": round(speed, 2), "crs": "EPSG:4326", "axis": "lat,lon", "src": "tdrive"}
        sender.submit(ev)
        if time.time() - last_metric >= 5:
            metrics.emit({"events_per_s": sender.sent / (time.time() - last_metric)})
            sender.sent = 0
            last_metric = time.time()
    p.flush(30)


def main():
    wait_for_kafka()
    wait_for_topics([config.T_RAW, config.T_TRUTH])
    stop = GracefulExit()
    if config.SOURCE_MODE == "tdrive":
        run_tdrive(stop)
    elif config.SOURCE_MODE == "delhi":
        run_delhi(stop)
    elif config.SOURCE_MODE == "india":
        run_india(stop)
    else:
        run_synthetic(stop)


if __name__ == "__main__":
    main()
