"""One-shot: create every Kafka topic (explicit partition counts) and the DB schema, publish plan v1."""
import time

from .. import config, gsir
from ..db import DB, jsonb
from ..kafka_utils import admin, dumps, ensure_topics, make_producer, setup_logging, wait_for_kafka

log = setup_logging("init")


def main():
    wait_for_kafka()
    created = ensure_topics()
    md = admin().list_topics(timeout=10)
    for name in sorted(config.TOPICS):
        t = md.topics.get(name)
        log.info("topic %-18s partitions=%d", name, len(t.partitions) if t else -1)

    db = DB().connect()
    db.ensure_schema()
    log.info("database schema ready")

    # Publish the human-authored baseline plan (v1) only if no plan exists yet.
    existing = db.query("SELECT max(version) AS v FROM plan_versions WHERE stage='active'")
    if not existing or existing[0]["v"] is None:
        plan = gsir.default_plan()
        p = make_producer()
        msg = {"stage": "active", "version": plan["version"], "plan": plan, "ts": time.time(),
               "provenance": {"author": "human", "reason": "baseline plan"}, "disposition": "approved"}
        p.produce(config.T_PLAN, key=b"active", value=dumps(msg))
        p.flush(30)
        db.execute("""INSERT INTO plan_versions (version, stage, disposition, plan, diff, verification, provenance)
                      VALUES (%s,'active','approved',%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                   (plan["version"], jsonb(plan), jsonb({}), jsonb({"passed": True}), jsonb(msg["provenance"])))
        log.info("published baseline plan v1")
    log.info("init complete (%d new topics)", len(created))


if __name__ == "__main__":
    main()
