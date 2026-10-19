"""STORAGE SINK: Kafka -> TimescaleDB.  Consumer group `tessera-sink`.

At-least-once delivery + idempotent writes (deterministic primary keys, ON CONFLICT) = each result is
stored exactly once, even after crashes, replays or fast-plane recovery. Offsets are committed only
after the database transaction commits; if the DB is down the batch is retried from the same offsets."""
import time

from confluent_kafka import KafkaError, TopicPartition

from .. import config
from ..db import DB, jsonb
from ..kafka_utils import GracefulExit, Metrics, loads, make_consumer, make_producer, setup_logging, \
    wait_for_kafka, wait_for_topics

log = setup_logging("sink")

TOPICS = [config.T_AGG, config.T_ALERTS, config.T_DECISIONS, config.T_SURPRISE, config.T_METRICS, config.T_TRUTH]


def build_rows(msgs):
    rows = {t: [] for t in TOPICS}
    for m in msgs:
        try:
            rows[m.topic()].append(loads(m.value()))
        except Exception:  # noqa: BLE001
            log.warning("skipping undecodable record %s[%d]@%d", m.topic(), m.partition(), m.offset())
    return rows


def write(db, rows):
    def tx(cur):
        from psycopg2.extras import execute_values  # noqa: PLC0415
        if rows[config.T_AGG]:
            execute_values(cur, """INSERT INTO tile_aggregates (window_end, window_start, tile, lat, lon, n_obs, vehicles,
                   mean_speed, std_speed, slow_frac, plan_version, degradation, instance, city) VALUES %s
                   ON CONFLICT (tile, window_end) DO UPDATE SET n_obs=EXCLUDED.n_obs, vehicles=EXCLUDED.vehicles,
                   mean_speed=EXCLUDED.mean_speed, std_speed=EXCLUDED.std_speed, slow_frac=EXCLUDED.slow_frac,
                   city=EXCLUDED.city""",
                           [(r["window_end"], r["window_start"], r["tile"], r["lat"], r["lon"], r["n_obs"], r["vehicles"],
                             r["mean_speed"], r["std_speed"], r["slow_frac"], r.get("plan_version"),
                             r.get("degradation", 0), r.get("instance"), r.get("city") or config.city_of(r["lat"], r["lon"]))
                            for r in rows[config.T_AGG]],
                           template="(to_timestamp(%s), to_timestamp(%s), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        if rows[config.T_ALERTS]:
            execute_values(cur, """INSERT INTO alerts (ts, alert_id, type, severity, tile, lat, lon, message, payload,
                   plan_version, provenance_id, city) VALUES %s ON CONFLICT DO NOTHING""",
                           [(a["ts"], a["alert_id"], a.get("type"), a.get("severity"), a.get("tile"), a.get("lat"),
                             a.get("lon"), a.get("message"), jsonb(a.get("payload")), a.get("plan_version"),
                             a.get("provenance_id"), a.get("city") or
                             (config.city_of(a["lat"], a["lon"]) if a.get("lat") is not None and a.get("lon") is not None else None))
                            for a in rows[config.T_ALERTS]],
                           template="(to_timestamp(%s), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        if rows[config.T_DECISIONS]:
            execute_values(cur, """INSERT INTO agent_decisions (ts, decision_id, tile, lat, lon, label, risk, source,
                   features, episode_id, city) VALUES %s ON CONFLICT DO NOTHING""",
                           [(d["ts"], d["decision_id"], d.get("tile"), d.get("lat"), d.get("lon"), d.get("label"),
                             d.get("risk"), d.get("source"), jsonb(d.get("features")), d.get("episode_id"),
                             d.get("city") or (config.city_of(d["lat"], d["lon"])
                             if d.get("lat") is not None and d.get("lon") is not None else None))
                            for d in rows[config.T_DECISIONS]],
                           template="(to_timestamp(%s), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        if rows[config.T_SURPRISE]:
            execute_values(cur, """INSERT INTO surprise_events (ts, event_id, window_end, tiles, max_z, lat, lon, payload, city)
                   VALUES %s ON CONFLICT DO NOTHING""",
                           [(e["ts"], e["event_id"], e["window_end"], e["tiles"], e["max_z"], e["lat"], e["lon"],
                             jsonb({"summary": e["summary"], "gate": e["gate"]}),
                             e.get("city") or config.city_of(e["lat"], e["lon"])) for e in rows[config.T_SURPRISE]],
                           template="(to_timestamp(%s), %s, to_timestamp(%s), %s,%s,%s,%s,%s,%s)")
        if rows[config.T_TRUTH]:
            execute_values(cur, """INSERT INTO scenario_truth (incident_id, start_ts, end_ts, lat, lon, radius_m,
                   place, kind, city) VALUES %s ON CONFLICT DO NOTHING""",
                           [(t["incident_id"], t["start_ts"], t["end_ts"], t["lat"], t["lon"], t["radius_m"],
                             t.get("place"), t.get("kind"), t.get("city") or config.city_of(t["lat"], t["lon"]))
                            for t in rows[config.T_TRUTH]],
                           template="(%s, to_timestamp(%s), to_timestamp(%s), %s,%s,%s,%s,%s,%s)")
        mrows = []
        for m in rows[config.T_METRICS]:
            for k, v in m.get("values", {}).items():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    mrows.append((m["ts"], m["component"], m["instance"], k, float(v)))
        if mrows:
            execute_values(cur, "INSERT INTO ops_metrics (ts, component, instance, name, value) VALUES %s", mrows,
                           template="(to_timestamp(%s), %s,%s,%s,%s)")
    db.run(tx)


def main():
    wait_for_kafka()
    wait_for_topics(TOPICS)
    db = DB().connect()
    # databases created by an older version lack the Bengaluru columns - add them in place
    db.execute("ALTER TABLE scenario_truth ADD COLUMN IF NOT EXISTS place TEXT, ADD COLUMN IF NOT EXISTS kind TEXT")
    db.ensure_schema()
    stop = GracefulExit()
    c = make_consumer(config.G_SINK, offset_reset="earliest")
    p = make_producer()
    metrics = Metrics(p, "sink")
    c.subscribe(TOPICS)
    log.info("sink joined consumer group %s for %s", config.G_SINK, TOPICS)
    written, failures, last_metric = 0, 0, time.time()
    while not stop.stop:
        batch = c.consume(num_messages=2000, timeout=1.0)
        for m in batch:
            if m.error() and m.error().code() != KafkaError._PARTITION_EOF:
                log.warning("consumer error: %s", m.error())
        msgs = [m for m in batch if not m.error()]
        if msgs:
            try:
                write(db, build_rows(msgs))
                c.commit(asynchronous=False)       # only after the DB transaction is durable
                written += len(msgs)
            except Exception as e:  # noqa: BLE001
                failures += 1
                log.error("write failed (%s) - rewinding batch and retrying", e)
                first = {}
                for m in msgs:
                    k = (m.topic(), m.partition())
                    first[k] = min(first.get(k, m.offset()), m.offset())
                for (t, part), off in first.items():
                    try:
                        c.seek(TopicPartition(t, part, off))
                    except Exception:  # noqa: BLE001
                        pass
                time.sleep(5)
        if time.time() - last_metric > 5:
            dt = time.time() - last_metric
            metrics.emit({"records_per_s": written / dt, "write_failures": failures})
            p.poll(0)
            if written:
                log.info("stored %.0f records/s", written / dt)
            written, last_metric = 0, time.time()
    c.close()


if __name__ == "__main__":
    main()
