"""Thin helpers around confluent-kafka: reliable producers, consumers, admin, JSON serde."""
import json
import logging
import signal
import time

from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions

from . import config

log = logging.getLogger("tessera.kafka")


def setup_logging(name):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    for noisy in ("httpx", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(name)


def dumps(obj):
    return json.dumps(obj, separators=(",", ":"), default=str).encode("utf-8")


def loads(b):
    return json.loads(b.decode("utf-8")) if b is not None else None


def admin():
    return AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})


def wait_for_kafka(timeout_s=180):
    """Block until the broker answers metadata requests (services start before Kafka is ready)."""
    t0 = time.time()
    while True:
        try:
            md = admin().list_topics(timeout=5)
            if md.brokers:
                return md
        except Exception as e:  # noqa: BLE001
            log.info("waiting for Kafka at %s (%s)", config.KAFKA_BOOTSTRAP, e)
        if time.time() - t0 > timeout_s:
            raise RuntimeError("Kafka not reachable at " + config.KAFKA_BOOTSTRAP)
        time.sleep(3)


def wait_for_topics(names, timeout_s=180):
    t0 = time.time()
    while True:
        try:
            md = admin().list_topics(timeout=5)
            missing = [n for n in names if n not in md.topics]
            if not missing:
                return
            log.info("waiting for topics %s", missing)
        except Exception as e:  # noqa: BLE001
            log.info("waiting for topics (%s)", e)
        if time.time() - t0 > timeout_s:
            raise RuntimeError(f"topics missing: {names}")
        time.sleep(3)


def ensure_topics():
    """Create missing topics and expand partition counts without deleting existing Kafka data."""
    a = admin()
    existing = a.list_topics(timeout=10).topics
    new = [NewTopic(name, num_partitions=p, replication_factor=1, config=cfg)
           for name, (p, cfg) in config.TOPICS.items() if name not in existing]
    created = []
    if new:
        for name, fut in a.create_topics(new).items():
            try:
                fut.result()
                log.info("created topic %s (%d partitions)", name, config.TOPICS[name][0])
                created.append(name)
            except KafkaException as e:
                if "TOPIC_ALREADY_EXISTS" not in str(e):
                    raise
    # Kafka can increase, but never decrease, a topic's partition count. This safely upgrades
    # existing 6-partition gps.raw/tile.aggregates topics to the 15 city-aware partitions.
    md = a.list_topics(timeout=10).topics
    expansions = {name: NewPartitions(name, count) for name, (count, _cfg) in config.TOPICS.items()
                  if name in md and len(md[name].partitions) < count}
    if expansions:
        for name, fut in a.create_partitions(expansions).items():
            try:
                fut.result()
                log.info("expanded topic %s to %d partitions", name, config.TOPICS[name][0])
            except KafkaException as e:
                raise RuntimeError(f"failed to expand Kafka topic {name}: {e}") from e
    return created


def make_producer(**extra):
    """Idempotent, acks=all producer: retries cannot create duplicates or reorder a partition."""
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "lz4",
        "message.timeout.ms": 120000,
        "client.id": f"{config.INSTANCE}-producer",
    }
    conf.update(extra)
    return Producer(conf)


def make_consumer(group_id, offset_reset="earliest", **extra):
    """Manual-commit consumer: offsets are committed only after results are durable."""
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": offset_reset,
        "session.timeout.ms": 15000,
        "max.poll.interval.ms": 300000,
        "partition.assignment.strategy": "range",
        "fetch.wait.max.ms": 100,
        "client.id": f"{config.INSTANCE}-{group_id}",
    }
    conf.update(extra)
    return Consumer(conf)


class Metrics:
    """Publishes component telemetry to the ops.metrics topic."""

    def __init__(self, producer, component):
        self.p = producer
        self.component = component

    def emit(self, values, labels=None):
        msg = {"ts": time.time(), "component": self.component, "instance": config.INSTANCE,
               "values": values, "labels": labels or {}}
        try:
            self.p.produce(config.T_METRICS, key=self.component.encode(), value=dumps(msg))
        except BufferError:
            self.p.poll(0.5)


class GracefulExit:
    """Turn SIGTERM/SIGINT (docker stop, Ctrl+C) into a flag so services can checkpoint before exit."""

    def __init__(self):
        self.stop = False
        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(s, self._handler)
            except (ValueError, AttributeError):
                pass

    def _handler(self, *_):
        self.stop = True
