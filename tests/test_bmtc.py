"""BMTC live feed: parsing the Namma BMTC reply and polling a fake BMTC server."""
import http.server
import json
import threading
import time
from datetime import datetime, timedelta, timezone

from tessera import bmtc
from tessera.services.producer import LiveFeedState

IST = timezone(timedelta(hours=5, minutes=30))


def stamp(age_s):
    return (datetime.now(IST) - timedelta(seconds=age_s)).strftime("%d-%m-%Y %H:%M:%S")


def reply(lat=12.920611, lon=77.665192, age=30):
    bus = {"vehicleid": 21950, "vehiclenumber": "KA57F1907", "centerlat": lat, "centerlong": lon, "heading": 256,
           "lastrefreshon": stamp(age), "servicetype": "Non AC/Ordinary"}
    old = dict(bus, vehicleid=1, vehiclenumber="KA01OLD", lastrefreshon=stamp(900))
    return {"up": {"data": [{"stationid": 1, "vehicleDetails": [bus]}, {"stationid": 2, "vehicleDetails": [bus]}],
                   "mapData": [bus]},
            "down": {"data": [{"stationid": 3, "vehicleDetails": [old]}], "mapData": []},
            "message": "Success", "issuccess": True, "exception": None, "rowCount": 2, "responsecode": 200}


def test_parse_dedupes_bus_listed_under_many_stops():
    rows = bmtc.parse_route_details(reply(), "500-D")
    assert sorted(r["vid"] for r in rows) == ["BMTC-KA01OLD", "BMTC-KA57F1907"]
    r = next(r for r in rows if r["vid"] == "BMTC-KA57F1907")
    assert abs(time.time() - r["ts_s"] - 30) < 3 and r["route"] == "500-D" and r["label"] == "bus"


def test_speed_from_movement_and_city_partition():
    st = LiveFeedState("bengaluru", parked_filter=True, src="bmtc-live")
    assert st.events(bmtc.parse_route_details(reply(age=60))) == []          # first sighting: no speed yet
    evs = st.events(bmtc.parse_route_details(reply(lat=12.9226, age=30)))     # ~220 m in 30 s
    ev = next(e for e in evs if e["vid"] == "BMTC-KA57F1907")
    assert 20 < ev["speed"] < 32 and ev["src"] == "bmtc-live" and ev["vtype"] == "bus"


class _Fake(http.server.BaseHTTPRequestHandler):
    calls = []

    def do_POST(self):  # noqa: N802
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        _Fake.calls.append((self.path, body, self.headers.get("lan"), self.headers.get("deviceType")))
        if self.path.endswith("SearchRoute_v2"):
            data = [{"routeno": body["routetext"], "routeparentid": 1066}] if body["routetext"] == "500-D" else []
            out = {"data": data, "Message": "Success"}
        else:
            out = reply()
        b = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def log_message(self, *a):
        pass


def test_poller_against_fake_server():
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Fake)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    p = bmtc.BmtcPoller(routes=["500-D", "NOPE-1"], interval_s=0.05,
                        api=f"http://127.0.0.1:{srv.server_port}/WebAPI/")
    p.start()
    time.sleep(1.0)
    p.stop_flag = True
    srv.shutdown()
    rows = p.drain()
    assert p.ids == {"500-D": 1066, "NOPE-1": None}
    assert rows and all(r["vid"] == "BMTC-KA57F1907" for r in rows)       # 900 s old fix dropped
    assert p.live_buses() == 1
    details = [c for c in _Fake.calls if c[0].endswith("SearchByRouteDetails_v4")]
    assert details[0][1] == {"routeid": 1066, "servicetypeid": 0} and details[0][2:] == ("en", "WEB")
    assert sum(c[1].get("routetext") == "NOPE-1" for c in _Fake.calls) == 1   # unknown route asked only once
