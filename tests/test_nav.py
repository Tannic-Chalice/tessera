"""Navigator: routing on the Bengaluru road graph, rerouting around incident zones and crawling traffic."""
import time

import h3
import pytest

from tessera import gsir, nav
from tessera.roads import RoadNetwork

RVCE, SILK_BOARD = (12.92529, 77.49918), (12.918153, 77.62326)


@pytest.fixture(scope="module")
def graph():
    return nav.RoadGraph(RoadNetwork())


def drive(n, trip, nv):
    for _ in range(n):
        trip.last_check = trip.last_pub = 0
        trip.last_reroute -= 1
        nv.step_all(1.0)
        if trip.status != "driving":
            break


def make(graph, state):
    pub, ctl = [], []
    nv = nav.Navigator(graph, lambda: nav.TrafficView(state["tiles"], state["plan"]), pub.append, ctl.append)
    return nv, pub, ctl


def test_route_rvce_to_silk_board(graph):
    v = nav.TrafficView({}, {"operators": []})
    p = graph.route(graph.nearest(*RVCE), graph.nearest(*SILK_BOARD), lambda c: v.at(c, graph.pos[c])[:2])
    km = nav.path_km(graph, p)[-1]
    assert p and 12 < km < 25


@pytest.mark.parametrize("mode", ["geofence", "crawl"])
def test_reroutes_around_hazard_and_publishes_gps(graph, mode):
    state = {"tiles": {}, "plan": {"operators": []}}
    nv, pub, ctl = make(graph, state)
    t = nv.start(RVCE, SILK_BOARD, speedup=5)
    drive(10, t, nv)
    here = t.done_km()
    k = next(i for i, c in enumerate(t.cum) if c > here + 1.2)
    lat, lon = graph.pos[t.path[k]]
    if mode == "geofence":
        state["plan"] = {"operators": [gsir.geofence_operator("gf_t", lat, lon, 500, time.time() * 1000 + 6e5, "t")]}
    else:
        for c in h3.grid_disk(h3.latlng_to_cell(lat, lon, 8), 1):
            state["tiles"][c] = {"mean_speed": 4.0, "slow_frac": 0.9, "vehicles": 12}
    drive(60, t, nv)
    kinds = [m["kind"] for m in t.messages]
    assert "reroute" in kinds and t.version >= 2
    msg = next(m["text"] for m in t.messages if m["kind"] == "reroute")
    assert "metres ahead" in msg and ("Turn" in msg or "Continue" in msg or "U-turn" in msg)
    assert pub and pub[0]["src"] == "nav-car" and pub[0]["vid"].startswith("MYCAR-")


def test_turn_words():
    assert nav.turn_word(0, 90) == "right" and nav.turn_word(0, 270) == "left" and nav.turn_word(10, 12) == "straight"


def test_block_ahead_reroutes_immediately_and_reports_it(graph):
    state = {"tiles": {}, "plan": {"operators": []}}
    nv, pub, ctl = make(graph, state)
    t = nv.start(RVCE, SILK_BOARD, speedup=5)
    drive(10, t, nv)
    v0, before = t.version, t.position()
    lat, lon = nv.block_ahead(t)
    assert ctl and ctl[0]["type"] == "inject_incident"               # TESSERA still has to confirm it
    assert t.version > v0 and t.messages[-1]["kind"] == "reroute"    # but this car is rerouted right away
    assert "You reported an accident" in t.messages[-1]["text"]
    assert nav._km(before, t.position()) < 0.001                     # without moving the car
    assert all(nav._km(graph.pos[c], (lat, lon)) > 0.08 for c in t.path)   # the new route avoids the spot
    st = nv.state(t)
    assert st.get("old_path") and st["next"]["word"] in ("around", "left", "right", "straight", "slightly left",
                                                           "slightly right")
