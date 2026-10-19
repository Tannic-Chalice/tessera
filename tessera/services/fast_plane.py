"""FAST PLANE (stream processing).  Kafka consumer group `tessera-fastplane`.

gps.raw ──► normalise (L0) ──► H3 tile ──► sliding event-time windows (L1) ──► tile.aggregates
               │ malformed ─► gps.dlq        │ too late ─► gps.late            │
               │                             └► geofence / distilled operators (L2) ─► alerts
plan.patches (broadcast, compacted) ──► hot-swap at a window boundary

Deterministic, no model inference anywhere on this path.  Fault tolerance: window state + Kafka
offsets are checkpointed together to PostgreSQL; on restart or rebalance a partition's state is
restored and consumption resumes exactly from the checkpointed offset (results are re-emitted
with deterministic keys, so the idempotent sink stores them once)."""
import time
import uuid
from collections import defaultdict, deque

from confluent_kafka import KafkaError, TopicPartition, OFFSET_BEGINNING

from .. import config, geo, gsir, stats
from ..db import DB, jsonb
from ..kafka_utils import GracefulExit, Metrics, dumps, loads, make_consumer, make_producer, setup_logging, \
    wait_for_kafka, wait_for_topics
from ..windows import WindowState

log = setup_logging("fastplane")


def pct(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    i = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[i]


class FastPlane:
    def __init__(self):
        self.stop = GracefulExit()
        self.db = DB().connect()
        self.p = make_producer()
        self.metrics = Metrics(self.p, "fastplane")
        self.city_metrics = Metrics(self.p, "fastplane_city")
        self.city_counts = defaultdict(lambda: defaultdict(int))
        self.city_lat_samples = defaultdict(lambda: deque(maxlen=2000))
        self.c = make_consumer(config.G_FAST)
        # every instance must see every plan -> unique group per instance (broadcast, not load-sharing)
        self.pc = make_consumer(f"tessera-fastplane-plans-{config.INSTANCE}-{uuid.uuid4().hex[:6]}")
        self.states = {}                    # partition -> WindowState
        self.gf_state = {}                  # partition -> {gf_id: {vid: last_alert_ts}}
        self.offsets = {}                   # partition -> next offset to read
        self.plan = gsir.default_plan()
        self.pending_plan = None
        self.shadow = None                  # (plan, started_wall)
        self.gf_cells = {}                  # gf_id -> set(cells) (filter step)
        self.shadow_cells = {}
        self.dm_last = {}                   # tile -> last distilled alert window
        self.lat_samples = deque(maxlen=5000)
        self.counts = defaultdict(int)
        self.shadow_counts = defaultdict(int)
        self.shadow_gf_state = defaultdict(dict)
        self.last_ckpt = time.time()
        self.last_metric = time.time()
        self.degradation = 0
        self.ooo_samples = deque(maxlen=5000)
        self.ooo_live = deque(maxlen=5000)
        self.slow_kmh = 10.0

    # ---------------------------------------------------------------- plans
    def load_initial_plan(self):
        self.pc.subscribe([config.T_PLAN])
        t0, last_msg = time.time(), time.time()
        while time.time() - t0 < 30 and time.time() - last_msg < 4:
            m = self.pc.poll(0.5)
            if m is None or m.error():
                continue
            last_msg = time.time()
            self.on_plan_message(loads(m.value()), initial=True)
        if self.pending_plan:
            self.swap_plan()
        log.info("running plan v%d", self.plan["version"])

    def on_plan_message(self, msg, initial=False):
        stage = msg.get("stage")
        if stage == "active":
            if msg["plan"]["version"] >= self.plan["version"]:
                self.pending_plan = msg["plan"]
                if not initial:
                    log.info("received plan v%d - will hot-swap at next window boundary", msg["plan"]["version"])
        elif stage == "shadow":
            if initial:
                return
            self.shadow = (msg["plan"], time.time())
            self.shadow_cells = self._cells_for(msg["plan"])
            self.shadow_counts = defaultdict(int)
            self.shadow_gf_state = defaultdict(dict)
            log.info("shadow-running candidate plan v%d", msg["plan"]["version"])
        elif stage in ("rejected", "rolled_back"):
            if self.shadow and self.shadow[0]["version"] == msg.get("version"):
                self.shadow = None

    def _cells_for(self, plan):
        res = plan["tile"]["res"]
        out = {}
        for g in gsir.operators(plan, "geofence"):
            sh = g["shape"]
            k = geo.candidate_ring_k(sh["radius"], res, sh["lat"], sh["lon"])
            out[g["id"]] = set(geo.ring(geo.tile(sh["lat"], sh["lon"], res), k))
        return out

    def swap_plan(self):
        new = self.pending_plan
        self.pending_plan = None
        old_v = self.plan["version"]
        self.plan = new
        self.gf_cells = self._cells_for(new)
        aggs = gsir.operators(new, "aggregate")
        self.slow_kmh = float(aggs[0].get("slow_kmh", 10.0)) if aggs else 10.0
        w = new["window"]
        for part, ws in self.states.items():
            ws.reconfigure(w["size_ms"], w["slide_ms"], *self.time_policy(part, new))
        if self.shadow and self.shadow[0]["version"] <= new["version"]:
            self.shadow = None
        if old_v != new["version"]:
            log.info("HOT-SWAP plan v%d -> v%d (%d geofences, %d distilled operators)", old_v, new["version"],
                     len(gsir.operators(new, "geofence")), len(gsir.operators(new, "distilled_filter")))

    # ---------------------------------------------------------------- rebalance + checkpoints
    def time_policy(self, part, plan=None):
        """Watermark bound and allowed lateness for a partition: the live Delhi partitions get a wider
        bound because real GPS fixes reach the open-data feed late and unevenly."""
        t = (plan or self.plan)["time"]
        if part in config.LIVE_PARTITIONS:
            return max(t["watermark_ms"], config.LIVE_WATERMARK_MS), max(t["allowed_lateness_ms"], config.LIVE_LATENESS_MS)
        return t["watermark_ms"], t["allowed_lateness_ms"]

    def new_state(self, part=None):
        w = self.plan["window"]
        return WindowState(w["size_ms"], w["slide_ms"], *self.time_policy(part))

    def on_assign(self, consumer, partitions):
        rows = {}
        try:
            for r in self.db.query("SELECT partition, next_offset, state FROM checkpoints WHERE group_id=%s AND topic=%s",
                                   (config.G_FAST, config.T_RAW)):
                rows[r["partition"]] = r
        except Exception as e:  # noqa: BLE001
            log.warning("could not read checkpoints: %s", e)
        for tp in partitions:
            ck = rows.get(tp.partition)
            if ck and ck["state"]:
                self.states[tp.partition] = WindowState.restore(ck["state"]["window"])
                self.states[tp.partition].reconfigure(self.plan["window"]["size_ms"], self.plan["window"]["slide_ms"],
                                                      *self.time_policy(tp.partition))
                self.gf_state[tp.partition] = ck["state"].get("geofence", {})
                tp.offset = int(ck["next_offset"])
                self.offsets[tp.partition] = tp.offset
                log.info("RECOVERED partition %d from checkpoint: offset=%d, %d open windows, watermark=%s",
                         tp.partition, tp.offset, len(self.states[tp.partition].windows),
                         self.states[tp.partition].watermark)
            else:
                # New city partitions have no checkpoint yet. Start them at the beginning and
                # persist a numeric next_offset (not Kafka's symbolic OFFSET_INVALID value).
                tp.offset = OFFSET_BEGINNING
                self.offsets[tp.partition] = 0
                self.states[tp.partition] = self.new_state(tp.partition)
                self.gf_state[tp.partition] = {}
                log.info("assigned partition %d (fresh state; replay from beginning)", tp.partition)
        consumer.assign(partitions)
        self.counts["rebalances"] += 1

    def on_revoke(self, consumer, partitions):
        parts = [tp.partition for tp in partitions]
        log.info("partitions %s revoked - checkpointing before hand-off", parts)
        try:
            self.checkpoint(parts)
        except Exception as e:  # noqa: BLE001
            log.warning("checkpoint on revoke failed: %s", e)
        for p in parts:
            self.states.pop(p, None)
            self.gf_state.pop(p, None)
            self.offsets.pop(p, None)

    def checkpoint(self, parts=None):
        parts = [p for p in (parts if parts is not None else list(self.states)) if p in self.offsets]
        if not parts:
            return
        self.p.flush(30)          # outputs durable in Kafka BEFORE we advance the input position
        rows = [(config.G_FAST, config.T_RAW, p, self.offsets[p],
                 jsonb({"window": self.states[p].snapshot(), "geofence": self.gf_state.get(p, {})}), config.INSTANCE)
                for p in parts]
        self.db.execute_values(
            """INSERT INTO checkpoints (group_id, topic, partition, next_offset, state, instance) VALUES %s
               ON CONFLICT (group_id, topic, partition) DO UPDATE
               SET next_offset=EXCLUDED.next_offset, state=EXCLUDED.state, instance=EXCLUDED.instance, updated_at=now()""",
            rows)
        try:
            self.c.commit(offsets=[TopicPartition(config.T_RAW, p, self.offsets[p]) for p in parts], asynchronous=False)
        except Exception as e:  # noqa: BLE001  (commit can fail mid-rebalance; the DB checkpoint is authoritative)
            log.debug("offset commit skipped: %s", e)
        self.counts["checkpoints"] += 1

    # ---------------------------------------------------------------- per-event path
    def handle(self, msg):
        part = msg.partition()
        self.offsets[part] = msg.offset() + 1
        try:
            raw = loads(msg.value())
        except Exception:  # noqa: BLE001
            raw = None
        ev, reason = geo.normalise(raw) if isinstance(raw, dict) else (None, "not JSON")
        if ev is None:
            self.p.produce(config.T_DLQ, key=msg.key(), value=dumps({"reason": reason, "raw": raw,
                                                                     "partition": part, "offset": msg.offset()}))
            self.counts["dlq"] += 1
            for ckey, partitions in config.CITY_PARTITIONS.items():
                if part in partitions:
                    self.city_counts[ckey]["dlq_total"] += 1
                    break
            return
        city = ev.get("city") or config.city_of(ev["lat"], ev["lon"])
        city_count = self.city_counts[city]
        ws = self.states.get(part)
        if ws is None:
            return
        res = self.plan["tile"]["res"]
        tile = geo.tile(ev["lat"], ev["lon"], res)
        if ws.max_ts is not None:                   # lateness of EVERY event (0 if in order) -> p95 over all events
            lag = max(0, ws.max_ts - ev["ts"])
            if part in config.LIVE_PARTITIONS:      # live feed has its own wider time policy (LIVE_WATERMARK_MS)
                self.ooo_live.append(lag)
            else:
                self.ooo_samples.append(lag)
        if not ws.add(ev["ts"], tile, ev["speed"], ev["vid"], self.slow_kmh):
            self.counts["late"] += 1
            city_count["late_total"] += 1
            if self.plan["time"]["late_policy"] == "side_output":
                self.p.produce(config.T_LATE, key=tile.encode(), value=dumps({**ev, "tile": tile, "partition": part,
                                                                            "watermark": ws.watermark}))
        else:
            self.counts["events"] += 1
            city_count["events_window"] += 1
            city_count["events_total"] += 1

        if ev.get("ingest_ms"):
            self.city_lat_samples[city].append(time.time() * 1000 - ev["ingest_ms"])

        # L2 continuous spatial operators: filter by k-ring cells, refine by exact geodesic distance
        if self.gf_cells:
            self.eval_geofences(self.plan, self.gf_cells, self.gf_state.setdefault(part, {}), ev, tile, shadow=False)
        if self.shadow and self.degradation < 2:
            self.eval_geofences(self.shadow[0], self.shadow_cells, self.shadow_gf_state, ev, tile, shadow=True)
        if ev.get("ingest_ms"):
            self.lat_samples.append(time.time() * 1000 - ev["ingest_ms"])

    def eval_geofences(self, plan, cells, state, ev, tile, shadow):
        for g in gsir.active_geofences(plan, ev["ts"]):
            if tile not in cells.get(g["id"], ()):
                continue                                     # filter: pruned without geometry
            sh = g["shape"]
            if self.degradation == 0:                        # refine: exact geodesic predicate
                if geo.haversine_m(ev["lat"], ev["lon"], sh["lat"], sh["lon"]) > sh["radius"]:
                    continue
            seen = state.setdefault(g["id"], {})
            last = seen.get(ev["vid"])
            if last is not None and ev["ts"] - last < g.get("cooldown_s", 120) * 1000:
                continue
            seen[ev["vid"]] = ev["ts"]
            if shadow:
                self.shadow_counts[plan["version"]] += 1
                continue
            city = ev.get("city") or config.city_of(ev["lat"], ev["lon"])
            alert = {"alert_id": f"gf:{g['id']}:{ev['vid']}:{ev['ts']}", "ts": ev["ts"] / 1000,
                     "type": "GEOFENCE_ENTER", "severity": "high", "tile": tile, "lat": ev["lat"], "lon": ev["lon"],
                     "message": f"{ev['vid']} entered incident zone {g['id']} at {ev['speed']:.0f} km/h",
                     "plan_version": plan["version"], "provenance_id": g.get("provenance_id"),
                     "degradation_level": self.degradation, "payload": {"geofence": g["id"], "vid": ev["vid"]}}
            alert["city"] = city
            self.p.produce(config.T_ALERTS, key=tile.encode(), value=dumps(alert))
            self.counts["alerts"] += 1
            self.city_counts[city]["alerts_total"] += 1

    # ---------------------------------------------------------------- window firing
    def fire_windows(self):
        fired_any = False
        distilled = gsir.operators(self.plan, "distilled_filter")
        for part, ws in list(self.states.items()):
            ws.advance_idle(config.IDLE_TIMEOUT_S)
            for end, tile, agg in ws.fire():
                fired_any = True
                lat, lon = geo.center(tile)
                city = config.city_of(lat, lon)
                self.city_counts[city]["aggregates_total"] += 1
                rec = {"window_start": (end - ws.size) / 1000, "window_end": end / 1000, "tile": tile,
                       "lat": round(lat, 6), "lon": round(lon, 6), "city": city, **agg, "partition": part,
                       "plan_version": self.plan["version"], "degradation": self.degradation,
                       "instance": config.INSTANCE}
                self.p.produce(config.T_AGG, key=tile.encode(), value=dumps(rec), partition=part)
                self.counts["aggregates"] += 1
                for d in distilled:
                    if d.get("city") and len(config.ACTIVE_CITIES) > 1 and config.city_of(lat, lon) != d["city"]:
                        continue                 # a distilled judgement only applies where it was learned
                    s = stats.score(d["model"], stats.features(agg))
                    lo, hi = d.get("uncertain_band", [0.0, 0.0])
                    if lo <= s < hi:
                        self.counts["distilled_uncertain"] += 1
                    if s >= d["threshold"] and agg["n_obs"] >= 5:
                        if end - self.dm_last.get(tile, 0) < 60000:
                            continue
                        self.dm_last[tile] = end
                        alert = {"alert_id": f"dm:{d['id']}:{tile}:{end}", "ts": end / 1000,
                                 "type": "DISTILLED_CONGESTION", "severity": "medium", "tile": tile,
                                 "lat": lat, "lon": lon,
                                 "message": f"distilled operator {d['id']} flags congestion (score {s:.2f})",
                                 "plan_version": self.plan["version"], "provenance_id": d.get("provenance_id"),
                                 "degradation_level": self.degradation, "payload": {"score": round(s, 3), **agg}}
                        alert["city"] = city
                        self.p.produce(config.T_ALERTS, key=tile.encode(), value=dumps(alert))
                        self.counts["alerts"] += 1
                        self.city_counts[city]["alerts_total"] += 1
        return fired_any

    def emit_metrics(self):
        now = time.time()
        dt = now - self.last_metric
        lat = sorted(self.lat_samples)
        p50, p95, p99 = pct(lat, 0.5), pct(lat, 0.95), pct(lat, 0.99)
        # monotone degradation ladder driven by backlog (tuple-path latency)
        self.degradation = 0 if p50 < 2000 else (1 if p50 < 10000 else 2)
        vals = {"events_per_s": self.counts["events"] / dt, "p50_ms": p50, "p95_ms": p95, "p99_ms": p99,
                "late_total": self.counts["late"], "dlq_total": self.counts["dlq"],
                "alerts_total": self.counts["alerts"], "aggregates_total": self.counts["aggregates"],
                "partitions_owned": len(self.states), "plan_version": self.plan["version"],
                "degradation_level": self.degradation, "checkpoints": self.counts["checkpoints"],
                "ooo_p95_ms": pct(sorted(self.ooo_samples), 0.95),
                "ooo_live_p95_ms": pct(sorted(self.ooo_live), 0.95), "rebalances": self.counts["rebalances"],
                "distilled_uncertain": self.counts["distilled_uncertain"]}
        labels = {"partitions": sorted(self.states)}
        if self.shadow:
            vals["shadow_alerts"] = self.shadow_counts[self.shadow[0]["version"]]
            vals["shadow_seconds"] = now - self.shadow[1]
            labels["shadow_version"] = self.shadow[0]["version"]
        self.metrics.emit(vals, labels)
        for city, counts in self.city_counts.items():
            city_lat = sorted(self.city_lat_samples[city])
            city_vals = {
                "events_per_s": counts["events_window"] / max(dt, 0.001),
                "events_total": counts["events_total"],
                "p50_ms": pct(city_lat, 0.5), "p95_ms": pct(city_lat, 0.95), "p99_ms": pct(city_lat, 0.99),
                "late_total": counts["late_total"], "dlq_total": counts["dlq_total"],
                "alerts_total": counts["alerts_total"], "aggregates_total": counts["aggregates_total"],
                "degradation_level": self.degradation,
            }
            self.city_metrics.emit(city_vals, {"city": city})
            counts["events_window"] = 0
            self.city_lat_samples[city].clear()
        log.info("%.0f ev/s | p50 %.0f ms p99 %.0f ms | partitions %s | late %d dlq %d | alerts %d | plan v%d%s",
                 vals["events_per_s"], p50, p99, sorted(self.states), self.counts["late"], self.counts["dlq"],
                 self.counts["alerts"], self.plan["version"],
                 f" | shadow v{self.shadow[0]['version']}={vals['shadow_alerts']}" if self.shadow else "")
        self.counts["events"] = 0
        self.lat_samples.clear()
        self.last_metric = now

    # ---------------------------------------------------------------- main loop
    def run(self):
        self.load_initial_plan()
        self.c.subscribe([config.T_RAW], on_assign=self.on_assign, on_revoke=self.on_revoke)
        log.info("fast plane %s joined consumer group %s", config.INSTANCE, config.G_FAST)
        while not self.stop.stop:
            msgs = self.c.consume(num_messages=1000, timeout=0.5)
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("consumer error: %s", m.error())
                    continue
                self.handle(m)
            pm = self.pc.poll(0)
            while pm is not None:
                if not pm.error():
                    self.on_plan_message(loads(pm.value()))
                pm = self.pc.poll(0)
            fired = self.fire_windows()
            if self.pending_plan and (fired or not self.states):
                self.swap_plan()          # watermark-aligned: swap only between window firings
            self.p.poll(0)
            if time.time() - self.last_ckpt >= config.CHECKPOINT_INTERVAL_S:
                self.checkpoint()
                self.last_ckpt = time.time()
            if time.time() - self.last_metric >= 5:
                self.emit_metrics()
        log.info("shutting down: final checkpoint")
        self.checkpoint()
        self.c.close()
        self.pc.close()
        self.p.flush(10)


def main():
    wait_for_kafka()
    wait_for_topics([config.T_RAW, config.T_PLAN, config.T_AGG])
    FastPlane().run()


if __name__ == "__main__":
    main()
