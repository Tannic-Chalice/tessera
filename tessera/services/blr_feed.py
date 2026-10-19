"""SIMULATED BENGALURU GTFS-REALTIME FEED  (used when SOURCE_MODE=india)

Bengaluru's real-time BMTC feed is not public yet (it was shared only with selected startups in 2024),
so this service plays its part. It simulates 4,500 vehicles of Indian mixed traffic (two-wheelers, cars, autos, cabs, goods) on the real BMTC road
geometry (see tessera/roads.py), injects incidents at real bottlenecks (ground truth -> Kafka topic
scenario.truth), and publishes the current positions in exactly the format Delhi Open Transit Data uses:

    GET http://localhost:8090/VehiclePositions.pb      (GTFS-Realtime FeedMessage, protobuf)

The producer reads this feed and Delhi's real feed with the same GTFS-Realtime code, so the pipeline
cannot tell a simulated city from a real one. vehicle.label carries the vehicle type (two_wheeler, bus ...)."""
import http.server
import threading
import time
import uuid

from .. import config

from ..kafka_utils import GracefulExit, dumps, loads, make_consumer, make_producer, setup_logging, wait_for_kafka, \
    wait_for_topics
from . import producer as sim

log = setup_logging("blr-feed")


class FeedState:
    def __init__(self):
        self.blob = b""
        self.lock = threading.Lock()
        self.served = 0


STATE = FeedState()


def build_feed(fleet, now):
    from google.transit import gtfs_realtime_pb2  # noqa: PLC0415
    f = gtfs_realtime_pb2.FeedMessage()
    f.header.gtfs_realtime_version = "2.0"
    f.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    f.header.timestamp = int(now)
    for v in fleet:
        e = f.entity.add()
        e.id = v.vid
        vp = e.vehicle
        vp.vehicle.id = v.vid
        vp.vehicle.label = v.vtype
        vp.timestamp = int(now)
        vp.position.latitude = v.lat
        vp.position.longitude = v.lon
        vp.position.speed = v.speed / 3.6               # GTFS-Realtime speed is metres per second
        vp.position.bearing = v.heading
        if v.vtype == "bus":
            vp.trip.route_id = sim.road_network().names[v.mover.ri]
    return f.SerializeToString()


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if not self.path.startswith("/VehiclePositions.pb"):
            body = (b"TESSERA simulated Bengaluru GTFS-Realtime feed\n"
                    b"GET /VehiclePositions.pb  -> FeedMessage (protobuf)\n")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(body)
            return
        with STATE.lock:
            body = STATE.blob
        STATE.served += 1
        self.send_response(200 if body else 503)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_a):
        pass


def main():
    stop = GracefulExit()
    if config.SOURCE_MODE != "india":
        log.info("SOURCE_MODE=%s - the simulated Bengaluru feed is only used in india mode; idling",
                 config.SOURCE_MODE)
        while not stop.stop:
            time.sleep(5)
        return
    wait_for_kafka()
    wait_for_topics([config.T_TRUTH, config.T_GOV])
    p = make_producer()
    net = sim.road_network()
    fleet = [sim.BlrVehicle(i) for i in range(config.BLR_VEHICLES)]
    ctl = make_consumer(f"tessera-blrfeed-{uuid.uuid4().hex[:6]}", offset_reset="latest")
    ctl.subscribe([config.T_GOV])
    log.info("Bengaluru: %d vehicles on %d BMTC road segments; serving GTFS-Realtime on :%d/VehiclePositions.pb",
             len(fleet), len(net.routes), config.BLR_FEED_PORT)
    srv = http.server.ThreadingHTTPServer(("0.0.0.0", config.BLR_FEED_PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    tick = config.BLR_REPORT_S
    incidents = []
    sim_now = time.time()
    next_incident = sim_now + 45
    last_log = time.time()
    next_tick = time.time()
    while not stop.stop:
        sim_now = time.time()
        if sim_now >= next_incident:
            inc = sim.BlrIncident(sim_now, avoid={i.place for i in incidents})
            incidents.append(inc)
            next_incident = sim_now + config.INCIDENT_EVERY_S
            p.produce(config.T_TRUTH, key=inc.id.encode(), value=dumps({
                "incident_id": inc.id, "lat": inc.lat, "lon": inc.lon, "radius_m": inc.radius_m,
                "start_ts": inc.start, "end_ts": inc.end, "place": inc.place, "kind": inc.kind}))
            for v in sim.rng.sample([v for v in fleet if v.vtype != "bus"], k=min(260, len(fleet) // 17)):
                v.send_towards(inc)
            log.info("INJECTED incident %s [%s @ %s] r=%.0fm for %ds", inc.id, inc.kind, inc.place, inc.radius_m,
                     config.INCIDENT_DURATION_S)
        incidents = [i for i in incidents if i.end > sim_now]
        while True:                                   # dashboard demo control: force a time of day
            m = ctl.poll(0)
            if m is None or m.error():
                break
            try:
                c = loads(m.value())
            except Exception:  # noqa: BLE001
                continue
            if c.get("type") == "sim_hour":
                sim.SIM_HOUR[0] = c.get("hour")
                log.info("Bengaluru clock: %s", "live IST" if c.get("hour") is None else f"{c['hour']:04.1f} h (forced)")
        for v in fleet:
            v.step(tick, incidents, sim_now)
        blob = build_feed(fleet, sim_now)
        with STATE.lock:
            STATE.blob = blob
        p.poll(0)
        if time.time() - last_log >= 30:
            sp = sorted(v.speed for v in fleet)
            log.info("feed: %d vehicles, %d KB, %d requests, incidents %d | hour %.1f, city speed factor %.2f, "
                     "median %.0f km/h, %.0f%% < 10 km/h", len(fleet), len(blob) // 1024, STATE.served, len(incidents),
                     sim.ist_hour(sim_now), sim.day_factor(sim_now), sp[len(sp) // 2],
                     100.0 * sum(x < 10 for x in sp) / len(sp))
            last_log = time.time()
        next_tick += tick
        while time.time() < next_tick and not stop.stop:
            time.sleep(0.05)
    srv.shutdown()
    p.flush(10)


if __name__ == "__main__":
    main()
