"""Real live BMTC bus positions for Bengaluru.

BMTC has no public open-data feed, but its own commuter website (bmtckiosk.karnataka.gov.in) and the
Namma BMTC app read live bus positions from bmtcmobileapi.karnataka.gov.in. That API is unofficial and
undocumented (community documentation: github.com/PratyushBalaji/BMTC-API), and it answers ONE route
per request, so this poller asks for a fixed list of busy routes in turn, one small request every
~1.2 s (each route refreshed about every 40 s). It runs in a background thread and hands rows in the same
shape as the GTFS-Realtime parser to LiveFeedState, so the rest of the pipeline treats it like Delhi.

    SearchRoute_v2          {"routetext": "500-D"}                     -> routeparentid   (once per route)
    SearchByRouteDetails_v4 {"routeid": <routeparentid>, "servicetypeid": 0}
                            -> up/down stops, each with vehicleDetails[] (vehicleid, vehiclenumber,
                               centerlat, centerlong, heading, lastrefreshon "DD-MM-YYYY HH:MM:SS" IST)"""
import json
import logging
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone

from . import config

log = logging.getLogger("bmtc")
IST = timezone(timedelta(hours=5, minutes=30))
HEADERS = {"Content-Type": "application/json", "Accept": "application/json, text/plain, */*", "lan": "en",
           "deviceType": "WEB", "Origin": "https://bmtckiosk.karnataka.gov.in",
           "Referer": "https://bmtckiosk.karnataka.gov.in/", "User-Agent": "Mozilla/5.0 (TESSERA student project)"}


def parse_ist(stamp):
    try:
        return int(datetime.strptime(stamp, "%d-%m-%Y %H:%M:%S").replace(tzinfo=IST).timestamp())
    except (TypeError, ValueError):
        return None


def parse_route_details(doc, route_no=None):
    """SearchByRouteDetails_v4 reply -> rows {vid, ts_s, lat, lon, speed=None, label, heading, route};
    one row per bus (a bus is listed under many stops - keep its newest fix)."""
    best = {}
    for d in ("up", "down"):
        part = doc.get(d) or {}
        lists = [s.get("vehicleDetails") or [] for s in part.get("data") or []] + [part.get("mapData") or []]
        for vs in lists:
            for v in vs:
                vid, ts = v.get("vehicleid"), parse_ist(v.get("lastrefreshon"))
                lat, lon = v.get("centerlat"), v.get("centerlong")
                if not vid or not ts or not lat or not lon:
                    continue
                if vid in best and best[vid]["ts_s"] >= ts:
                    continue
                best[vid] = {"vid": f"BMTC-{v.get('vehiclenumber') or vid}", "ts_s": ts, "lat": float(lat),
                             "lon": float(lon), "speed": None, "label": "bus",
                             "heading": float(v["heading"]) if v.get("heading") is not None else None,
                             "route": route_no}
    return list(best.values())


class BmtcPoller(threading.Thread):
    def __init__(self, routes=None, interval_s=None, api=None):
        super().__init__(daemon=True, name="bmtc-poller")
        self.routes = list(routes or config.BMTC_ROUTES)
        self.interval = interval_s or config.BMTC_REQ_INTERVAL_S
        self.api = api or config.BMTC_API
        self.ids = {}                 # route no -> routeparentid (None = not found)
        self.rows, self.lock = [], threading.Lock()
        self.seen = {}                # vid -> last fix time (for the live-bus count)
        self.ok = self.fail = 0
        self.stop_flag = False

    def _post(self, ep, body):
        req = urllib.request.Request(self.api + ep, data=json.dumps(body).encode(), headers=HEADERS, method="POST")
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode("utf-8"))

    def _resolve(self, rno):
        hits = self._post("SearchRoute_v2", {"routetext": rno}).get("data") or []
        exact = [h for h in hits if str(h.get("routeno", "")).replace(" ", "").upper() == rno.replace(" ", "").upper()]
        self.ids[rno] = exact[0]["routeparentid"] if exact else None
        if not exact:
            log.info("BMTC route %s not found - skipped", rno)

    def run(self):
        i = 0
        while not self.stop_flag:
            rno = self.routes[i % len(self.routes)]
            i += 1
            if self.ids.get(rno, 0) is None:          # known-missing route: no request
                if len(self.ids) == len(self.routes) and all(v is None for v in self.ids.values()):
                    log.warning("none of the BMTC routes were found - BMTC live feed idle")
                    time.sleep(300)
                continue
            t0 = time.time()
            try:
                if rno not in self.ids:
                    self._resolve(rno)
                    time.sleep(self.interval)
                    if self.ids[rno] is None:
                        continue
                rows = parse_route_details(self._post("SearchByRouteDetails_v4",
                                                      {"routeid": self.ids[rno], "servicetypeid": 0}), rno)
                now = time.time()
                rows = [r for r in rows if now - r["ts_s"] <= config.BMTC_MAX_FIX_AGE_S]
                with self.lock:
                    self.rows.extend(rows)
                    for r in rows:
                        self.seen[r["vid"]] = r["ts_s"]
                self.ok += 1
                self.fail = 0
            except Exception as e:  # noqa: BLE001  network / API change: back off, keep going
                self.fail += 1
                if self.fail <= 3 or self.fail % 20 == 0:
                    log.warning("BMTC request for %s failed (%s) - retry %d", rno, e, self.fail)
                time.sleep(min(60.0, self.interval * 2 ** min(self.fail, 5)))
            time.sleep(max(0.0, self.interval - (time.time() - t0)))

    def drain(self):
        with self.lock:
            rows, self.rows = self.rows, []
        return rows

    def live_buses(self, within_s=180):
        now = time.time()
        with self.lock:
            return sum(1 for t in self.seen.values() if now - t <= within_s)
