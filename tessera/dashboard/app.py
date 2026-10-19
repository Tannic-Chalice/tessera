"""ANALYTICS / DASHBOARD: live map + KPIs + alerts + Kafka topology. Consumes Kafka directly for
real-time state (its own broadcast consumer group) and TimescaleDB for history."""
import json
import os
import threading
from contextlib import asynccontextmanager
import time
import uuid
from collections import defaultdict, deque

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from .. import config, geo, gsir, verifier, mobility
from ..db import DB
from ..kafka_utils import dumps, loads, make_consumer, make_producer, setup_logging, wait_for_kafka

log = setup_logging("dashboard")
STATIC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
LIVE_TOPICS = [config.T_RAW, config.T_AGG, config.T_ALERTS, config.T_METRICS, config.T_PLAN, config.T_TRUTH,
               config.T_SURPRISE, config.T_DECISIONS]


class Live:
    def __init__(self):
        self.lock = threading.Lock()
        self.tiles = {}
        self.vehicles = {}
        self.boundaries = {}
        self.alerts = deque(maxlen=150)
        self.city_alerts = defaultdict(lambda: deque(maxlen=80))   # per-city buffers: a busy city can't push out a quiet one
        self.alert_counts = defaultdict(int)
        self.metrics = defaultdict(dict)        # component -> instance -> {values, labels, ts}
        self.series = deque(maxlen=240)
        self.city_series = defaultdict(lambda: deque(maxlen=240))
        self.plan = gsir.default_plan()
        self.shadow = None
        self.truth = {}
        self.surprises = deque(maxlen=30)
        self.decisions = deque(maxlen=60)
        self.kafka = {"topics": [], "groups": [], "ts": 0}
        self.started = time.time()


live = Live()


def consume_loop():
    wait_for_kafka()
    c = make_consumer(f"tessera-dashboard-{uuid.uuid4().hex[:6]}", offset_reset="latest")
    # the plan is a compacted topic: read it from the beginning to learn the current version
    pc = make_consumer(f"tessera-dashboard-plan-{uuid.uuid4().hex[:6]}", offset_reset="earliest")
    c.subscribe([t for t in LIVE_TOPICS if t != config.T_PLAN])
    pc.subscribe([config.T_PLAN, config.T_TRUTH])
    last_series = time.time()
    while True:
        try:
            msgs = c.consume(num_messages=1000, timeout=0.3) + pc.consume(num_messages=200, timeout=0.05)
        except Exception as e:  # noqa: BLE001
            log.warning("consume error %s", e)
            time.sleep(2)
            continue
        with live.lock:
            for m in msgs:
                if m.error():
                    continue
                try:
                    v = loads(m.value())
                except Exception:  # noqa: BLE001
                    continue
                t = m.topic()
                if t == config.T_RAW:
                    ev, reason = geo.normalise(v) if isinstance(v, dict) else (None, "not JSON")
                    if ev:
                        ev["_received"] = time.time()
                        live.vehicles[ev["vid"]] = ev
                        if len(live.vehicles) > 6000:
                            cutoff = time.time() - 180
                            live.vehicles = {vid: row for vid, row in live.vehicles.items()
                                             if row.get("_received", 0) >= cutoff}
                elif t == config.T_AGG:
                    prev = live.tiles.get(v["tile"])
                    if prev is None or prev["window_end"] <= v["window_end"]:
                        live.tiles[v["tile"]] = v
                elif t == config.T_ALERTS:
                    live.alerts.appendleft(v)
                    if v.get("lat") is not None and len(config.ACTIVE_CITIES) > 1:
                        live.city_alerts[config.city_of(v["lat"], v["lon"])].appendleft(v)
                    live.alert_counts[v.get("type")] += 1
                elif t == config.T_METRICS:
                    labels = v.get("labels", {})
                    metric_key = v["instance"] + (":" + str(labels.get("city")) if labels.get("city") else "")
                    live.metrics[v["component"]][metric_key] = {**v["values"], "_labels": labels, "_ts": v["ts"]}
                elif t == config.T_PLAN:
                    if v["stage"] == "active" and v["plan"]["version"] >= live.plan["version"]:
                        live.plan = v["plan"]
                        if live.shadow and live.shadow["version"] <= v["plan"]["version"]:
                            live.shadow = None
                    elif v["stage"] == "shadow":
                        live.shadow = v["plan"]
                    elif v["stage"] == "rejected":
                        live.shadow = None
                elif t == config.T_TRUTH:
                    live.truth[v["incident_id"]] = v
                elif t == config.T_SURPRISE:
                    live.surprises.appendleft({k: v[k] for k in ("event_id", "ts", "head", "tiles", "max_z", "p",
                                                                  "summary", "lat", "lon")})
                elif t == config.T_DECISIONS:
                    if v.get("is_cluster_head") or v.get("source") == "cache":
                        live.decisions.appendleft(v)
            if time.time() - last_series >= 5:
                now = time.time()
                fp = [x for x in live.metrics["fastplane"].values() if now - x["_ts"] < 15]
                gate = next(iter(live.metrics["gate"].values()), {})
                prod = [x for x in live.metrics["producer"].values() if now - x["_ts"] < 15]
                sp = next(iter(live.metrics["slowplane"].values()), {})
                live.series.append({"t": now, "eps": sum(x.get("events_per_s", 0) for x in fp),
                                    "produced": sum(x.get("events_per_s", 0) for x in prod),
                                    "p50": max([x.get("p50_ms", 0) for x in fp] or [0]),
                                    "p99": max([x.get("p99_ms", 0) for x in fp] or [0]),
                                    "invocations": gate.get("invocations", 0), "cache_hits": gate.get("cache_hits", 0),
                                    "agent_delay": sp.get("agent_delay_s", 0)})
                for city_key in config.ACTIVE_CITIES:
                    fp_city = [x for x in live.metrics["fastplane_city"].values()
                               if x.get("_labels", {}).get("city") == city_key and now - x["_ts"] < 20]
                    gate_city = [x for x in live.metrics["gate_city"].values()
                                 if x.get("_labels", {}).get("city") == city_key and now - x["_ts"] < 20]
                    slow_city = [x for x in live.metrics["slowplane_city"].values()
                                 if x.get("_labels", {}).get("city") == city_key and now - x["_ts"] < 20]
                    slow_global = [x for x in live.metrics["slowplane"].values() if now - x["_ts"] < 20]
                    live.city_series[city_key].append({
                        "t": now, "eps": sum(x.get("events_per_s", 0) for x in fp_city),
                        "produced": sum(x.get("events_per_s", 0) for x in fp_city),
                        "p50": max([x.get("p50_ms", 0) for x in fp_city] or [0]),
                        "p99": max([x.get("p99_ms", 0) for x in fp_city] or [0]),
                        "invocations": sum(x.get("invocations", 0) for x in gate_city),
                        "cache_hits": sum(x.get("cache_hits", 0) for x in gate_city),
                        "agent_delay": max([x.get("agent_delay_s", 0) for x in slow_city + slow_global] or [0]),
                    })
                last_series = now
                # keep each city's latest windows: a real live feed closes its windows later than a
                # simulated one, so one global "newest window" would hide the slower city's tiles
                newest = {}
                for x in live.tiles.values():
                    tc = config.city_of(x["lat"], x["lon"])
                    newest[tc] = max(newest.get(tc, 0), x["window_end"])
                live.tiles = {k: x for k, x in live.tiles.items()
                              if x["window_end"] >= newest[config.city_of(x["lat"], x["lon"])] - 25}


def kafka_loop():
    wait_for_kafka()
    a = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    wc = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": "tessera-dashboard-admin",
                   "enable.auto.commit": False})
    while True:
        try:
            md = a.list_topics(timeout=5)
            topics, hw = [], {}
            for name in sorted(config.TOPICS):
                if name not in md.topics:
                    continue
                parts = []
                for pid in sorted(md.topics[name].partitions):
                    lo, hi = wc.get_watermark_offsets(TopicPartition(name, pid), timeout=3)
                    hw[(name, pid)] = hi
                    parts.append({"p": pid, "end": hi, "start": lo})
                topics.append({"name": name, "partitions": parts, "messages": sum(p["end"] - p["start"] for p in parts)})
            groups = []
            try:
                listing = a.list_consumer_groups().result(timeout=10)
                ids = sorted(g.group_id for g in listing.valid if g.group_id.startswith("tessera-")
                             and "dashboard" not in g.group_id)
            except Exception:  # noqa: BLE001
                ids = [config.G_FAST, config.G_GATE, config.G_SLOW, config.G_SINK]
            main_groups = [g for g in ids if g in (config.G_FAST, config.G_GATE, config.G_SLOW, config.G_SINK)]
            broadcast = [g for g in ids if g not in main_groups]
            if main_groups:
                for gid, fut in a.describe_consumer_groups(main_groups).items():
                    try:
                        d = fut.result(timeout=10)
                    except Exception:  # noqa: BLE001
                        continue
                    members = []
                    for mem in d.members:
                        tps = mem.assignment.topic_partitions if mem.assignment else []
                        members.append({"client": mem.client_id, "host": mem.host,
                                        "assigned": sorted(f"{tp.topic}[{tp.partition}]" for tp in tps)})
                    lag = 0
                    try:
                        from confluent_kafka import ConsumerGroupTopicPartitions  # noqa: PLC0415
                        res = a.list_consumer_group_offsets([ConsumerGroupTopicPartitions(gid)])
                        for _, f2 in res.items():
                            for tp in f2.result(timeout=10).topic_partitions:
                                if tp.offset >= 0:
                                    lag += max(0, hw.get((tp.topic, tp.partition), tp.offset) - tp.offset)
                    except Exception:  # noqa: BLE001
                        lag = None
                    groups.append({"group": gid, "state": str(d.state).split(".")[-1], "members": members, "lag": lag})
            with live.lock:
                live.kafka = {"topics": topics, "groups": groups, "broadcast_groups": len(broadcast),
                              "ts": time.time()}
        except Exception as e:  # noqa: BLE001
            log.warning("kafka admin poll failed: %s", e)
        time.sleep(4)


app = FastAPI(title="TESSERA dashboard")
_db = None
_producer = None


def db():
    global _db
    if _db is None:
        _db = DB().connect()
    return _db


@asynccontextmanager
async def lifespan(_app):
    threading.Thread(target=consume_loop, daemon=True).start()
    threading.Thread(target=kafka_loop, daemon=True).start()
    threading.Thread(target=mobility.transit.load_all, daemon=True).start()
    yield


app.router.lifespan_context = lifespan


def _landmarks():
    if "bengaluru" not in config.ACTIVE_CITIES:
        return []
    try:
        import gzip  # noqa: PLC0415
        from .. import roads  # noqa: PLC0415
        with gzip.open(roads.ROADS_FILE, "rt", encoding="utf-8") as fh:
            return json.load(fh)["landmarks"]
    except Exception:  # noqa: BLE001
        return []


LANDMARKS = _landmarks()
CITY_SOURCES = {
    "bengaluru": "BMTC mixed-traffic simulation + BMTC live source when available",
    "delhi": "Delhi OTD live positions when configured + clearly marked simulated bus positions",
    "chennai": "Mobility Database GTFS schedule + clearly marked simulated GPS positions",
    "hyderabad": "Mobility Database GTFS schedule + clearly marked simulated GPS positions",
    "hubli_dharwad": "Mobility Database GTFS schedule + clearly marked simulated GPS positions",
    "beijing": "T-Drive replay",
}
CITIES = [{"key": k, "name": config.CITY_PROFILES[k][3], "center": list(config.CITY_PROFILES[k][:2]),
           "radius_km": config.CITY_PROFILES[k][2],
           "zoom": 11 if config.CITY_PROFILES[k][2] > 15 else 12,
           "source": CITY_SOURCES.get(k, "Configured source")} for k in config.ACTIVE_CITIES]

_ROADS = None


@app.get("/api/roads")
def road_overlay(city: str = "bengaluru"):
    """Return road geometry only for cities where the project has a verified local road overlay."""
    global _ROADS
    if city != "bengaluru" or "bengaluru" not in config.ACTIVE_CITIES:
        return JSONResponse({"edges": [], "attribution": "No local road overlay configured for this city"})
    if _ROADS is None:
        from .. import roads  # noqa: PLC0415
        _ROADS = roads.overlay_edges()
    return JSONResponse({"edges": _ROADS,
                         "attribution": "Road geometry: BMTC routes via github.com/Vonter/bmtc-gtfs (ODbL)"})


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))


def boundary(tile):
    b = live.boundaries.get(tile)
    if b is None:
        b = live.boundaries[tile] = [[round(x, 6), round(y, 6)] for x, y in geo.boundary(tile)]
    return b


def _recent_alerts():
    """Newest alerts overall plus the newest of every city, newest first (no duplicates)."""
    pool = list(live.alerts)[:80]
    for q in live.city_alerts.values():
        pool += list(q)[:60]
    seen, out = set(), []
    for a in sorted(pool, key=lambda a: a.get("ts", 0), reverse=True):
        if a.get("alert_id") not in seen:
            seen.add(a.get("alert_id"))
            out.append(a)
    return out[:240]


@app.get("/api/snapshot")
def snapshot(city: str = "bengaluru"):
    now = time.time()
    if city not in config.ACTIVE_CITIES and city != "all":
        city = config.ACTIVE_CITIES[0]
    def belongs(row):
        if city == "all":
            return True
        row_city = row.get("city")
        if row_city not in config.CITY_PROFILES:
            row_city = config.city_of(row.get("lat", 0), row.get("lon", 0))
        return row_city == city
    with live.lock:
        selected_tiles = [r for r in live.tiles.values() if belongs(r)]
        tiles = [{"tile": r["tile"], "b": boundary(r["tile"]), "n": r["n_obs"], "v": r["vehicles"],
                  "sp": r["mean_speed"], "sf": r["slow_frac"], "we": r["window_end"],
                  "inst": r.get("instance"), "part": r.get("partition"), "city": r.get("city")} for r in selected_tiles]
        all_alerts = _recent_alerts()
        selected_alerts = [a for a in all_alerts if belongs(a) or (city == "all" and a.get("lat") is None)]
        selected_vehicles = [v for v in live.vehicles.values() if belongs(v) and now-v.get("_received", now) < 180]
        metrics = {c: {i: v for i, v in d.items() if now - v["_ts"] < 30} for c, d in live.metrics.items()}
        if city != "all":
            for component in ("fastplane_city", "gate_city", "slowplane_city"):
                metrics[component] = {i: v for i, v in metrics.get(component, {}).items()
                                      if v.get("_labels", {}).get("city") == city}
        now_ms = now * 1000
        gfs = [{"id": g["id"], "lat": g["shape"]["lat"], "lon": g["shape"]["lon"], "r": g["shape"]["radius"],
                "prov": g.get("provenance_id"), "expires": g.get("expires_ms")}
               for g in gsir.active_geofences(live.plan, now_ms)
               if city == "all" or config.city_of(g["shape"]["lat"], g["shape"]["lon"]) == city]
        shadow_gfs = []
        if live.shadow:
            shadow_gfs = [{"id": g["id"], "lat": g["shape"]["lat"], "lon": g["shape"]["lon"], "r": g["shape"]["radius"]}
                          for g in gsir.active_geofences(live.shadow, now_ms)
                          if city == "all" or config.city_of(g["shape"]["lat"], g["shape"]["lon"]) == city]
        truth = [v for v in live.truth.values() if v["end_ts"] > now - 30 and
                 (city == "all" or config.city_of(v["lat"], v["lon"]) == city)]
        if selected_tiles:
            n_obs = sum(r.get("n_obs", 0) for r in selected_tiles)
            city_stats = {"tile_count": len(selected_tiles), "observations": n_obs,
                          "vehicles": len({v["vid"] for v in selected_vehicles}),
                          "mean_speed": sum(r.get("mean_speed", 0) * r.get("n_obs", 0) for r in selected_tiles) / max(1, n_obs),
                          "slow_frac": sum(r.get("slow_frac", 0) * r.get("n_obs", 0) for r in selected_tiles) / max(1, n_obs)}
        else:
            city_stats = {"tile_count": 0, "observations": 0, "vehicles": len(selected_vehicles),
                          "mean_speed": 0, "slow_frac": 0}
        feed_status = {}
        producer_values = {}
        for metric in live.metrics.get("producer", {}).values():
            if now - metric.get("_ts", 0) < 30:
                producer_values.update({k: v for k, v in metric.items() if not k.startswith("_")})
        for c in config.ACTIVE_CITIES:
            src = next((x for x in CITIES if x["key"] == c), {})
            transit_status = mobility.transit.get(c)["status"] if c in config.MOBILITY_FEED_IDS else None
            if c == "bengaluru":
                bmtc_ok = config.BMTC_LIVE and producer_values.get("bmtc_feed_ok", 0) == 1
                positions = "BMTC live feed healthy + simulated mixed traffic" if bmtc_ok else "simulated mixed traffic; BMTC live feed not confirmed"
            elif c == "delhi":
                otd_ok = config.OTD_API_KEY and producer_values.get("delhi_feed_ok", 0) == 1
                positions = "OTD live feed healthy + simulated demo GPS" if otd_ok else (
                    "OTD configured; awaiting live feed + simulated GPS" if config.OTD_API_KEY else
                    "simulated demo GPS (OTD_API_KEY not set)")
            else:
                sim_ok = producer_values.get(f"sim_{c}_feed_ok", 0) == 1
                positions = "SIM GPS active" if sim_ok else "SIM GPS starting"
            feed_status[c] = {"source": src.get("source", "Configured source"), "schedule": transit_status,
                              "live_positions": positions}
        centre_city = config.ACTIVE_CITIES[0] if city == "all" else city
        return JSONResponse({
            "now": now, "selected_city": city, "tiles": tiles, "vehicles": [
                {"vid": v["vid"], "lat": v["lat"], "lon": v["lon"], "speed": v["speed"],
                 "heading": v.get("heading", 0), "src": v.get("src"), "data_mode": v.get("data_mode", "simulated"),
                 "route": v.get("route"), "city": v.get("city")} for v in selected_vehicles[:1600]],
            "alerts": selected_alerts, "alert_counts": dict(live.alert_counts),
            "metrics": metrics, "series": list(live.series if city == "all" else live.city_series.get(city, [])),
            "city_stats": city_stats,
            "geofences": gfs, "shadow_geofences": shadow_gfs,
            "plan": {"version": live.plan["version"], "text": gsir.render(live.plan),
                     "distilled": gsir.operators(live.plan, "distilled_filter")},
            "shadow_version": live.shadow["version"] if live.shadow else None,
            "truth": truth,
            "surprises": [v for v in live.surprises if city == "all" or
                          config.city_of(v.get("lat", 0), v.get("lon", 0)) == city][:12],
            "decisions": [v for v in live.decisions if city == "all" or
                          config.city_of(v.get("lat", 0), v.get("lon", 0)) == city][:15],
            "kafka": live.kafka, "center": list(config.CITY_PROFILES[centre_city][:2]),
            "city": config.CITY_PROFILES[centre_city][3],
            "zoom": 11 if config.CITY_PROFILES[centre_city][2] > 15 else 12,
            "live_feed": bool(config.OTD_API_KEY), "cities": CITIES, "feed_status": feed_status,
            "landmarks": LANDMARKS if city in ("all", "bengaluru") else []})


@app.get("/api/transit/{city}")
def transit_data(city: str):
    if city not in config.MOBILITY_FEED_IDS or city not in config.ACTIVE_CITIES:
        return JSONResponse({"status": {"state": "not_configured"}, "transit": None}, status_code=404)
    result = mobility.transit.get(city)
    if result["status"].get("state") == "not_loaded":
        threading.Thread(target=mobility.transit.load_city, args=(city,), daemon=True).start()
    return JSONResponse(result)


@app.get("/api/history")
def history(city: str = "bengaluru"):
    try:
        d = db()
        return JSONResponse({
            "city": d.query("SELECT extract(epoch from bucket) AS t, active_tiles, observations, avg_speed, avg_slow_frac "
                            "FROM city_minute WHERE city=%s AND bucket > now() - interval '60 minutes' ORDER BY bucket",
                            (city,)),
            "alerts_by_type": d.query("SELECT type, count(*) AS n FROM alerts WHERE city=%s GROUP BY type ORDER BY n DESC", (city,)),
            "verifier": d.query("SELECT coalesce(injected_fault, 'agent error (repair)') AS fault, "
                                "count(*) FILTER (WHERE NOT passed) AS caught, count(*) AS total "
                                "FROM verifier_reports WHERE city=%s AND (injected_fault IS NOT NULL OR NOT passed) GROUP BY 1 ORDER BY 1", (city,)),
            "verifier_valid": d.query("SELECT count(*) FILTER (WHERE passed) AS passed, count(*) AS total "
                                      "FROM verifier_reports WHERE city=%s AND injected_fault IS NULL", (city,)),
            "plans": d.query("SELECT version, stage, disposition, extract(epoch from created_at) AS t, "
                             "diff->>'reason' AS reason FROM plan_versions ORDER BY created_at DESC LIMIT 25"),
            "held": d.query("SELECT version, diff->>'reason' AS reason, extract(epoch from created_at) AS t "
                            "FROM plan_versions WHERE stage='held' "
                            "AND disposition='hold_for_approval' ORDER BY version"),
            "detection": d.query("""
                SELECT s.incident_id, s.place, s.kind, extract(epoch from s.start_ts) AS start_ts,
                  (SELECT extract(epoch from min(a.ts)) - extract(epoch from s.start_ts) FROM alerts a
                     WHERE a.type IN ('HOTSPOT_DETECTED','AGENT_DECISION','CACHED_DECISION') AND a.ts >= s.start_ts
                       AND a.ts <= s.end_ts AND a.lat IS NOT NULL
                       AND abs(a.lat - s.lat) < 0.02 AND abs(a.lon - s.lon) < 0.025) AS detect_s
                FROM scenario_truth s WHERE s.city=%s AND s.start_ts > now() - interval '2 hours' ORDER BY s.start_ts DESC LIMIT 10""", (city,)),
            "gem": d.query("SELECT fact_id, fact_class, claim, confidence0, half_life_s, "
                           "extract(epoch from valid_from) AS vf FROM gem_facts ORDER BY valid_from DESC LIMIT 8"),
            "storage": d.query("""SELECT 'tile_aggregates' AS t, count(*) AS n FROM tile_aggregates UNION ALL
                                  SELECT 'alerts', count(*) FROM alerts UNION ALL
                                  SELECT 'agent_decisions', count(*) FROM agent_decisions UNION ALL
                                  SELECT 'ops_metrics', count(*) FROM ops_metrics UNION ALL
                                  SELECT 'gem_facts', count(*) FROM gem_facts UNION ALL
                                  SELECT 'checkpoints', count(*) FROM checkpoints"""),
            "checkpoints": d.query("SELECT partition, next_offset, instance, extract(epoch from updated_at) AS t "
                                   "FROM checkpoints WHERE group_id=%s ORDER BY partition", (config.G_FAST,)),
        })
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"error": str(e)}, status_code=503)


class Control(BaseModel):
    type: str
    version: int | None = None
    seconds: float | None = None
    rate: float | None = None
    hour: float | None = None


@app.post("/api/control")
def control(cmd: Control):
    global _producer
    if _producer is None:
        _producer = make_producer()
    body = cmd.model_dump(exclude_none=True)
    if cmd.type == "sim_hour":
        body["hour"] = cmd.hour                       # None = back to the live clock
    _producer.produce(config.T_GOV, key=cmd.type.encode(), value=dumps(body))
    _producer.flush(10)
    return {"ok": True}


@app.get("/api/verifier-bench")
def bench():
    return verifier.run_fault_suite(gsir.default_plan(), trials_per_fault=20)


def main():
    import uvicorn  # noqa: PLC0415
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), log_level="warning")


if __name__ == "__main__":
    main()
