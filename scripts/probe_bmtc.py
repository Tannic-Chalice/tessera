"""Quick check: does the Namma BMTC API give live bus positions we can stream into TESSERA?

Run on your laptop (needs only Python, no installs):   python scripts/probe_bmtc.py
It makes ~12 small requests, 1 second apart, so it is gentle on BMTC's servers."""
import json
import time
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://bmtcmobileapi.karnataka.gov.in/WebAPI/"
HDR = {"Content-Type": "application/json", "Accept": "application/json, text/plain, */*",
       "lan": "en", "deviceType": "WEB",
       "Origin": "https://bmtckiosk.karnataka.gov.in", "Referer": "https://bmtckiosk.karnataka.gov.in/",
       "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
IST = timezone(timedelta(hours=5, minutes=30))
# busy routes through the bottlenecks TESSERA models (Silk Board, ORR, Hosur Rd, Majestic, Hebbal)
ROUTES = ["500-D", "335-E", "KIA-9", "201", "356-C", "G-4"]


def post(ep, body):
    req = urllib.request.Request(BASE + ep, data=json.dumps(body).encode(), headers=HDR, method="POST")
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def age_s(stamp):
    try:
        t = datetime.strptime(stamp, "%d-%m-%Y %H:%M:%S").replace(tzinfo=IST)
        return (datetime.now(IST) - t).total_seconds()
    except (TypeError, ValueError):
        return None


total, ages = 0, []
for rno in ROUTES:
    try:
        hits = post("SearchRoute_v2", {"routetext": rno}).get("data") or []
        time.sleep(1)
        if not hits:
            print(f"{rno:8s} no route found")
            continue
        pid = hits[0]["routeparentid"]
        live = post("SearchByRouteDetails_v4", {"routeid": pid, "servicetypeid": 0})
        time.sleep(1)
    except Exception as e:  # noqa: BLE001
        print(f"{rno:8s} ERROR {type(e).__name__}: {e}")
        continue
    with open(f"bmtc_raw_{rno}.json", "w", encoding="utf-8") as fh:      # raw reply, to check the structure
        json.dump(live, fh, indent=1, ensure_ascii=False)
    up = live.get("up") or {}
    print(f"   raw: keys={list(live)[:8]} message={live.get('message') or live.get('Message')!r} "
          f"up.stops={len(up.get('data') or [])} up.mapData={len(up.get('mapData') or [])}")
    buses = {}
    for d in ("up", "down"):
        for stop in (live.get(d) or {}).get("data") or []:
            for v in stop.get("vehicleDetails") or []:
                buses[v["vehicleid"]] = v
        for v in (live.get(d) or {}).get("mapData") or []:
            buses.setdefault(v["vehicleid"], v)
    a = [x for x in (age_s(v.get("lastrefreshon")) for v in buses.values()) if x is not None]
    ages += a
    total += len(buses)
    print(f"{hits[0]['routeno']:14s} parentid={pid:<6} live buses={len(buses):3d}  "
          f"median fix age={sorted(a)[len(a) // 2]:.0f}s" if a else f"{hits[0]['routeno']:14s} live buses={len(buses)}")
    if buses:
        v = next(iter(buses.values()))
        print("   sample:", {k: v.get(k) for k in ("vehiclenumber", "centerlat", "centerlong", "heading",
                                                    "lastrefreshon", "servicetype")})

print(f"\nTOTAL live buses on {len(ROUTES)} routes: {total}")
if ages:
    ages.sort()
    print(f"fix age: median {ages[len(ages) // 2]:.0f}s, 90th pct {ages[int(len(ages) * .9)]:.0f}s")
