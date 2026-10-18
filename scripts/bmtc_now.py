"""How many REAL BMTC buses are in TESSERA right now, and their details.

Reads the last few minutes of Kafka topic gps.raw (Bengaluru partitions) and keeps only events with
src = bmtc-live. Run from the tessera folder while the system is running (no rebuild needed):

    Get-Content scripts\\bmtc_now.py | docker compose exec -T producer python - 3

The last number is how many minutes to look back (default 3)."""
import json
import sys
import time
from collections import Counter

from confluent_kafka import Consumer, TopicPartition

from tessera import config

MIN = float(sys.argv[1]) if len(sys.argv) > 1 else 3.0
since_ms = int((time.time() - MIN * 60) * 1000)
parts = config.CITY_PARTITIONS.get("bengaluru", list(range(6)))

c = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "group.id": f"bmtc-now-{int(time.time())}",
              "enable.auto.commit": False, "auto.offset.reset": "latest"})
tps = c.offsets_for_times([TopicPartition(config.T_RAW, p, since_ms) for p in parts], timeout=10)
ends = {p: c.get_watermark_offsets(TopicPartition(config.T_RAW, p), timeout=10)[1] for p in parts}
c.assign(tps)

buses, todo = {}, {tp.partition for tp in tps if 0 <= tp.offset < ends[tp.partition]}
while todo:
    msgs = c.consume(2000, timeout=3)
    if not msgs:
        break
    for m in msgs:
        if m.error():
            continue
        if m.offset() >= ends[m.partition()] - 1:
            todo.discard(m.partition())
        try:
            ev = json.loads(m.value())
        except ValueError:
            continue
        if ev.get("src") == "bmtc-live":
            ev["_part"] = m.partition()
            buses[ev["vid"]] = ev                  # keep each bus's newest position
c.close()

now = time.time()
print(f"\nREAL BMTC buses in TESSERA (last {MIN:.0f} min): {len(buses)}")
if not buses:
    print("none - BMTC runs ~5 am-11 pm; right after a restart wait 2 minutes")
    sys.exit()
routes = Counter(b.get("route", "?") for b in buses.values())
sp = sorted(b["speed"] for b in buses.values())
print(f"routes: {', '.join(f'{r} ({n})' for r, n in routes.most_common())}")
print(f"median speed {sp[len(sp) // 2]:.0f} km/h, {100 * sum(s < 10 for s in sp) / len(sp):.0f}% below 10 km/h\n")
print(f"{'bus (plate)':18s} {'route':8s} {'lat':>9s} {'lon':>9s} {'km/h':>5s} {'head':>5s} {'fix age':>8s} part")
for b in sorted(buses.values(), key=lambda b: (b.get("route", ""), b["vid"])):
    print(f"{b['vid']:18s} {b.get('route', '?'):8s} {b['lat']:9.5f} {b['lon']:9.5f} {b['speed']:5.1f} "
          f"{b.get('heading', 0):5.0f} {now - b['ts'] / 1000:7.0f}s  {b['_part']}")
