"""Unit tests for the deterministic core (no Kafka or database needed).
Run:  docker compose run --rm tests      (or:  python -m pytest -q  with requirements installed)"""
import math
import random

from tessera import config, geo, gsir, stats, verifier
from tessera.windows import WindowState

LAT, LON = 12.918, 77.623          # Central Silk Board, Bengaluru


# --- P3: filter-and-refine has no false negatives ----------------------------------------
def test_candidate_ring_has_no_false_negatives():
    rng = random.Random(1)
    for res in (7, 8, 9):
        for _ in range(40):
            r = rng.uniform(50, 2500)
            clat, clon = LAT + rng.uniform(-0.05, 0.05), LON + rng.uniform(-0.05, 0.05)
            k = geo.candidate_ring_k(r, res, clat, clon)
            cells = set(geo.ring(geo.tile(clat, clon, res), k))
            for _ in range(200):
                d = r * math.sqrt(rng.random())
                a = rng.random() * 2 * math.pi
                plat = clat + d * math.cos(a) / 111320
                plon = clon + d * math.sin(a) / (111320 * math.cos(math.radians(clat)))
                if geo.haversine_m(clat, clon, plat, plon) <= r:
                    assert geo.tile(plat, plon, res) in cells, (res, r, k)


def test_normalise_axis_order_and_rejections():
    ev, err = geo.normalise({"vid": "a", "ts": 1, "lat": LON, "lon": LAT, "axis": "lon,lat", "speed": 10})
    assert err is None and abs(ev["lat"] - LAT) < 1e-9
    assert geo.normalise({"vid": "a", "ts": 1, "lat": 999, "lon": 1, "speed": 1})[1]
    assert geo.normalise({"ts": 1, "lat": 1, "lon": 1, "speed": 1})[1]
    assert geo.normalise({"vid": "a", "ts": 1, "lat": 1, "lon": 1, "crs": "EPSG:9999"})[1]


# --- windows -------------------------------------------------------------------------------
def test_sliding_window_assignment_and_firing():
    ws = WindowState(60000, 10000, 4000, 3000)
    assert len(ws.window_ends(123456)) == 6
    for i in range(100):
        ws.add(1_000_000 + i * 100, "t1", 5.0 if i % 2 else 40.0, f"v{i % 7}", 10)
    assert ws.fire() == []                                    # watermark not past any end yet
    ws.add(1_080_000, "t2", 30, "z", 10)                       # advances watermark to 1_076_000
    out = ws.fire()
    ends = {e for e, _, _ in out}
    assert ends and all(e + 3000 <= 1_076_000 for e in ends)
    t1 = [a for e, t, a in out if t == "t1" and e == 1_010_000][0]
    assert t1["n_obs"] == 100 and t1["vehicles"] == 7 and abs(t1["slow_frac"] - 0.5) < 1e-9
    assert abs(t1["mean_speed"] - 22.5) < 1e-6


def test_late_event_goes_to_side_output_and_checkpoint_roundtrip():
    ws = WindowState(20000, 10000, 1000, 0)
    ws.add(100_000, "a", 10, "v", 10)
    ws.add(200_000, "a", 10, "v", 10)
    ws.fire()
    assert ws.add(100_500, "a", 10, "v", 10) is False         # every window containing it has fired
    snap = ws.snapshot()
    ws2 = WindowState.restore(snap)
    assert ws2.watermark == ws.watermark and ws2.fired_upto == ws.fired_upto
    assert ws2.windows.keys() == ws.windows.keys()


# --- gate statistics -------------------------------------------------------------------------
def test_gstar_detects_planted_hotspot_and_bh_controls_noise():
    rng = random.Random(3)
    base = geo.tile(LAT, LON, 8)
    region = list(geo.ring(base, 8))
    hot = set(geo.ring(geo.tile(LAT + 0.01, LON + 0.01, 8), 1))
    values = {t: rng.poisson(2) if hasattr(rng, "poisson") else rng.randint(0, 4) for t in region}
    for t in hot:
        values[t] = 40
    active = set(values)
    z = stats.gstar(values, active)
    p = {t: stats.p_two_sided(v) for t, v in z.items()}
    rej = stats.benjamini_hochberg(p, 0.01)
    assert hot & rej, "planted hotspot not detected"
    false = {t for t in rej if z[t] > 0} - set().union(*[set(geo.ring(h, 1)) for h in hot])
    assert len(false) <= 0.02 * len(region)


def test_benjamini_hochberg_simple():
    rej = stats.benjamini_hochberg({"a": 0.001, "b": 0.008, "c": 0.039, "d": 0.041, "e": 0.9}, 0.05)
    assert rej == {"a", "b"}


def test_variogram_reuse_radius():
    v = stats.DecisionVariogram(eps=0.02, min_pairs=2)
    c = geo.tile(LAT, LON, 8)
    for i, t in enumerate(geo.ring(c, 2)):
        v.add(t, 1000 + i, 0.8)                                   # perfectly smooth field
    assert v.k_star >= 2
    v2 = stats.DecisionVariogram(eps=0.02, min_pairs=2)
    for i, t in enumerate(geo.ring(c, 2)):
        v2.add(t, 1000 + i, (i % 2) * 1.0)                         # rough field
    assert v2.k_star == 0


def test_distiller_learns_separable_rule():
    rng = random.Random(5)
    X, y = [], []
    for _ in range(60):
        hot = rng.random() < 0.5
        agg = {"slow_frac": rng.uniform(0.6, 0.95) if hot else rng.uniform(0, 0.2),
               "mean_speed": rng.uniform(2, 8) if hot else rng.uniform(25, 45), "n_obs": rng.randint(20, 90),
               "vehicles": rng.randint(5, 25)}
        X.append(stats.features(agg))
        y.append(int(hot))
    w, b = stats.fit_logistic(X, y)
    thr, f1 = stats.calibrate_threshold(X, y, w, b)
    assert f1 > 0.95


# --- GS-IR + verifier --------------------------------------------------------------------------
def test_default_plan_verifies_and_renders():
    plan = gsir.default_plan()
    assert verifier.verify(plan)["passed"]
    assert 'plan "urban_mobility_monitor" v1' in gsir.render(plan)


def test_valid_geofence_patch_passes():
    plan = gsir.default_plan()
    op = gsir.geofence_operator("gf_1", LAT + 0.01, LON - 0.01, 900, 10**13, "ep_test")
    cand = gsir.apply_patch(plan, {"ops": [{"op": "add_operator", "operator": op}]})
    assert cand["version"] == 2
    r = verifier.verify(cand, {"observed_ooo_ms": 3500, "observed_p99_ms": 12})
    assert r["passed"], r["errors"]


def test_every_injected_fault_class_is_caught():
    plan = gsir.default_plan()
    report = verifier.run_fault_suite(plan, trials_per_fault=10)
    assert report["overall_catch_rate"] >= 0.95, report
    assert report["false_rejection_rate"] <= 0.02, report
    for name, rate in report["catch_rate"].items():
        assert rate == 1.0, (name, rate)


# ---------------------------------------------------------------- Bengaluru scenario
def test_default_city_is_bengaluru():
    assert config.CITY == "bengaluru"
    assert 12.8 < config.CITY_LAT < 13.1 and 77.4 < config.CITY_LON < 77.8


def test_vehicles_drive_on_bengaluru_roads():
    """Every simulated position lies on the BMTC road geometry and inside the city circle."""
    import random
    from tessera import roads
    net = roads.RoadNetwork()
    assert len(net.routes) > 1000 and net.total_km > 10000
    rng = random.Random(3)
    movers = [roads.RoadMover(net, rng) for _ in range(50)]
    for _ in range(300):                       # 300 steps of ~110 m each
        for m in movers:
            m.advance(0.11)
            assert geo.haversine_m(m.lat, m.lon, *net.center) <= net.radius_km * 1000 + 50
            near = net.nearest_vertex(m.lat, m.lon, max_km=0.7)   # simplified polylines: <= 600 m per segment
            assert near is not None


def test_bengaluru_incidents_at_bottlenecks_and_on_ordinary_roads():
    from tessera.services import producer
    names = {lm["name"] for lm in producer.road_network().landmarks}
    assert "Central Silk Board" in names and "Hebbal Flyover" in names
    net = producer.road_network()
    kinds = set()
    for _ in range(30):
        inc = producer.BlrIncident(0.0)
        assert inc.kind in producer.INCIDENT_TYPES
        if inc.place.startswith("near "):          # anywhere on the road network, named after a BMTC stop
            kinds.add("road")
            assert net.nearest_vertex(inc.lat, inc.lon, max_km=0.7) is not None    # on a road segment
            assert geo.haversine_m(inc.lat, inc.lon, *net.center) < 11000
        else:                                      # at a known bottleneck
            kinds.add("bottleneck")
            lm = next(l for l in net.landmarks if l["name"] == inc.place)
            assert geo.haversine_m(inc.lat, inc.lon, lm["lat"], lm["lon"]) < 400
    assert kinds == {"road", "bottleneck"}


# ---------------------------------------------------------------- Delhi live feed (GTFS-Realtime)
def test_gtfs_realtime_feed_is_parsed_and_parked_buses_dropped():
    from google.transit import gtfs_realtime_pb2
    from tessera.services import producer
    def feed(t, moving_lat):
        f = gtfs_realtime_pb2.FeedMessage()
        f.header.gtfs_realtime_version = "2.0"
        f.header.timestamp = t
        for vid, lat, sp in (("DL1PC1001", moving_lat, 8.0), ("DL1PC1002", config.CITY_LAT, 0.0)):
            e = f.entity.add()
            e.id = vid
            e.vehicle.vehicle.id = vid
            e.vehicle.timestamp = t
            e.vehicle.position.latitude = lat
            e.vehicle.position.longitude = config.CITY_LON
            e.vehicle.position.speed = sp
        return f.SerializeToString()
    st = producer.LiveFeedState()
    sent = []
    for k in range(40):                                         # 40 snapshots, 10 s apart
        rows, _ = producer.parse_vehicle_positions(feed(1_800_000_000 + 10 * k, config.CITY_LAT + 0.0007 * k))
        sent += st.events(rows)
        sent += st.events(rows)                                 # same snapshot again -> duplicates dropped
    moving = [e for e in sent if e["vid"] == "DL1PC1001"]
    parked = [e for e in sent if e["vid"] == "DL1PC1002"]
    assert len(moving) == 40 and abs(moving[0]["speed"] - 28.8) < 0.01      # 8 m/s -> 28.8 km/h
    assert len(parked) == 18                                    # dropped once still for >= 180 s
    assert st.stats["duplicate"] == 80


def test_two_city_scope_accepts_both_cities_and_rejects_elsewhere():
    """india mode: a geofence is valid in Bengaluru or Delhi, but not in between (e.g. Nagpur)."""
    plan = gsir.default_plan()
    plan["policy"]["bboxes"] = [gsir.city_bbox("bengaluru"), gsir.city_bbox("delhi")]
    for i, (lat, lon, ok) in enumerate([(12.918, 77.623, True), (28.632, 77.219, True), (21.146, 79.088, False)]):
        op = gsir.geofence_operator(f"gf_t{i}", lat, lon, 800, 1 << 50, "test:episode")
        res = verifier.verify(gsir.apply_patch(plan, {"ops": [{"op": "add_operator", "operator": op}]}))
        assert res["passed"] is ok, (lat, lon, res["errors"])


def test_live_speed_comes_from_movement_not_the_reported_field():
    """A GPS unit reporting speed 0 while the bus moved 100 m in 10 s is really doing 36 km/h."""
    from tessera.services import producer
    st = producer.LiveFeedState()
    lat0 = config.CITY_LAT
    def rows(t, lat):
        return [{"vid": "B1", "ts_s": t, "lat": lat, "lon": config.CITY_LON, "speed": 0.0}]
    st.events(rows(1_800_000_000, lat0))
    ev = st.events(rows(1_800_000_010, lat0 + 100 / 111320))[0]
    assert abs(ev["speed"] - 36.0) < 0.5


def test_bengaluru_peak_hour_is_much_slower_than_night():
    """Calibration: evening peak at Silk Board crawls; 2 AM flows freely."""
    from tessera.services import producer
    producer.SIM_HOUR[0] = 18.5
    peak = producer.day_factor(0) * (1 - 0.85 * producer.congestion_at(12.918153, 77.62326, 0))
    producer.SIM_HOUR[0] = 2.0
    night = producer.day_factor(0) * (1 - 0.85 * producer.congestion_at(12.918153, 77.62326, 0))
    producer.SIM_HOUR[0] = None
    assert night > 0.95 and peak < 0.2
