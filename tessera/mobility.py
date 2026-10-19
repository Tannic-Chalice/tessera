"""Mobility Database GTFS schedule catalog + static feed loader.

The catalog is authenticated with a long-lived refresh token from the environment. GTFS schedule
feeds are NOT live GPS; this module exposes routes/stops and keeps that distinction explicit.
"""
import csv
import io
import json
import math
import os
import threading
import time
import urllib.request
import zipfile

from . import config, geo


class MobilityTransit:
    def __init__(self):
        self.lock = threading.RLock()
        self.cache = {}
        self.status = {c: {"state": "not_loaded", "mode": "schedule", "feed_id": config.MOBILITY_FEED_IDS[c],
                           "routes": 0, "stops": 0, "last_updated": None, "error": None}
                       for c in config.MOBILITY_FEED_IDS}
        self._access_token = None
        self._token_expiry = 0

    def _request(self, url, headers=None, data=None, timeout=35):
        req = urllib.request.Request(url, headers=headers or {}, data=data)
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.read()

    def _token(self):
        if not config.MOBILITY_DATABASE_REFRESH_TOKEN:
            return None
        if self._access_token and time.time() < self._token_expiry - 60:
            return self._access_token
        payload = json.dumps({"refresh_token": config.MOBILITY_DATABASE_REFRESH_TOKEN}).encode()
        body = self._request(config.MOBILITY_DATABASE_API_BASE.rstrip("/") + "/tokens",
                             {"Content-Type": "application/json", "Accept": "application/json"}, payload)
        result = json.loads(body)
        token = result.get("access_token") or result.get("token")
        if not token:
            raise RuntimeError("Mobility Database token endpoint returned no access_token")
        self._access_token = token
        self._token_expiry = time.time() + int(result.get("expires_in", 3600))
        return token

    def _feed_url(self, city):
        fallback = config.MOBILITY_FEED_URLS[city]
        try:
            token = self._token()
        except Exception:
            return fallback, "catalog authentication failed; fallback URL"
        if not token:
            return fallback, "catalog fallback URL (set MOBILITY_DATABASE_REFRESH_TOKEN to use authenticated catalog)"
        feed_id = config.MOBILITY_FEED_IDS[city]
        url = config.MOBILITY_DATABASE_API_BASE.rstrip("/") + "/gtfs_feeds/" + feed_id
        try:
            body = self._request(url, {"Authorization": "Bearer " + token, "Accept": "application/json"})
            obj = json.loads(body)
            info = obj.get("source_info") or {}
            latest = obj.get("latest_dataset") or {}
            # Prefer Mobility Database's validated, versioned copy; fall back to the producer URL.
            producer_url = (latest.get("hosted_url") or info.get("producer_url")
                            or obj.get("producer_url"))
            if producer_url:
                source = "Mobility Database hosted dataset" if latest.get("hosted_url") else "Mobility Database producer URL"
                return producer_url, source
            return fallback, "catalog response had no downloadable URL; fallback URL"
        except Exception:
            # Catalog/API hiccups must not prevent schedule data from loading.
            return fallback, "catalog unavailable; fallback URL"

    def _download(self, city, url):
        os.makedirs(config.MOBILITY_CACHE_DIR, exist_ok=True)
        path = os.path.join(config.MOBILITY_CACHE_DIR, city + ".zip")
        try:
            if os.path.isfile(path) and os.path.getsize(path) > 1000 and time.time() - os.path.getmtime(path) < config.MOBILITY_REFRESH_S:
                return open(path, "rb").read()
        except OSError:
            pass
        blob = self._request(url, {"User-Agent": "TESSERA-student-project/1.0"}, timeout=90)
        if not blob.startswith(b"PK"):
            raise RuntimeError("GTFS producer URL did not return a ZIP archive")
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            f.write(blob)
        os.replace(tmp, path)
        return blob

    @staticmethod
    def _rows(zf, filename):
        names = zf.namelist()
        found = next((n for n in names if n.rsplit("/", 1)[-1].lower() == filename.lower()), None)
        if not found:
            return iter(())
        payload = zf.read(found)
        def read_rows():
            with io.BytesIO(payload) as raw:
                text = io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace", newline="")
                yield from csv.DictReader(text)
        return read_rows()

    def _parse(self, city, blob):
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            routes_raw = list(self._rows(zf, "routes.txt"))
            stops_raw = self._rows(zf, "stops.txt")
            trips_raw = self._rows(zf, "trips.txt")
            shapes_raw = self._rows(zf, "shapes.txt")
        clat, clon, radius, *_ = config.CITY_PROFILES[city]
        route_by_id = {}
        for r in routes_raw:
            rid = r.get("route_id")
            if rid:
                route_by_id[rid] = {"id": rid, "short": r.get("route_short_name", ""),
                                    "name": r.get("route_long_name", "") or r.get("route_short_name", ""),
                                    "type": r.get("route_type", "3")}
        route_shapes = {}
        for t in trips_raw:
            rid, sid = t.get("route_id"), t.get("shape_id")
            if rid in route_by_id and sid and rid not in route_shapes:
                route_shapes[rid] = sid
        shapes_by_id = {}
        for row in shapes_raw:
            try:
                sid = row.get("shape_id")
                lat, lon = float(row["shape_pt_lat"]), float(row["shape_pt_lon"])
                seq = int(row.get("shape_pt_sequence") or 0)
            except (ValueError, KeyError, TypeError):
                continue
            if not sid:
                continue
            # Keep the city footprint plus a margin to preserve routes entering/leaving the centre.
            if geo.haversine_m(lat, lon, clat, clon) > radius * 1500:
                continue
            shapes_by_id.setdefault(sid, []).append((seq, lat, lon))
        for pts in shapes_by_id.values():
            pts.sort()
        routes = []
        shapes = []
        seen_shapes = set()
        shape_count = len(set(route_shapes.values()) & set(shapes_by_id))
        for rid, route in route_by_id.items():
            route = dict(route)
            route["shape_id"] = route_shapes.get(rid)
            routes.append(route)
            sid = route_shapes.get(rid)
            if sid and sid in shapes_by_id and sid not in seen_shapes and len(shapes) < 150:
                pts = shapes_by_id[sid]
                if len(pts) > 90:
                    stride = math.ceil(len(pts) / 90)
                    pts = pts[::stride]
                if len(pts) >= 2:
                    shapes.append({"id": sid, "route_id": rid,
                                   "name": route["short"] or route["name"] or rid,
                                   "points": [[round(lat, 6), round(lon, 6)] for _, lat, lon in pts]})
                    seen_shapes.add(sid)
        stops = []
        for row in stops_raw:
            try:
                lat, lon = float(row["stop_lat"]), float(row["stop_lon"])
            except (ValueError, KeyError, TypeError):
                continue
            if geo.haversine_m(lat, lon, clat, clon) <= radius * 1000:
                stops.append({"id": row.get("stop_id", ""), "name": row.get("stop_name", "Stop"),
                              "lat": round(lat, 6), "lon": round(lon, 6)})
        return {"city": city, "feed_id": config.MOBILITY_FEED_IDS[city], "mode": "schedule",
                "route_count": len(routes), "routes": routes, "shapes": shapes, "shape_count": shape_count,
                "stops": stops, "map_stops": stops[:1500],
                "attribution": "Mobility Database GTFS schedule feed; static timetable/routes, not live GPS"}

    def load_city(self, city):
        if city not in config.MOBILITY_FEED_IDS:
            return
        with self.lock:
            if self.status[city]["state"] == "loading":
                return
            self.status[city].update(state="loading", error=None)
        try:
            url, source = self._feed_url(city)
            blob = self._download(city, url)
            data = self._parse(city, blob)
            with self.lock:
                self.cache[city] = data
                self.status[city].update(state="ready", routes=data["route_count"], stops=len(data["stops"]),
                                         last_updated=time.time(), source=source, error=None)
        except Exception as exc:
            with self.lock:
                self.status[city].update(state="unavailable", error=str(exc)[:240], source="Mobility Database GTFS")
        return

    def load_all(self):
        # Load sequentially to avoid holding several large GTFS archives in memory at once.
        for city in config.MOBILITY_FEED_IDS:
            if city in config.ACTIVE_CITIES:
                self.load_city(city)

    def get(self, city):
        with self.lock:
            data = self.cache.get(city)
            status = dict(self.status.get(city, {"state": "not_configured"}))
            return {"status": status, "transit": data}


transit = MobilityTransit()
