"""Rerouting around an incident zone: the car never jumps, and a U-turn is never disguised as a turn ahead."""
import random
import time

from tessera import geo, gsir, nav, roads

G = nav.RoadGraph(roads.RoadNetwork())


def test_reroutes_keep_position_and_never_fake_a_turn(monkeypatch):
    clock = [time.time()]
    monkeypatch.setattr(nav.time, "time", lambda: clock[0])
    rng, nodes, checked, uturns = random.Random(3), list(G.adj), 0, 0
    while checked < 20:
        a, b = rng.sample(nodes, 2)
        if nav._km(G.pos[a], G.pos[b]) < 8:
            continue
        holder = {"v": nav.TrafficView({}, gsir.default_plan())}
        nv = nav.Navigator(G, lambda: holder["v"], lambda e: None, lambda c: None)
        t = nv.start(G.pos[a], G.pos[b], 4)
        for _ in range(rng.randint(5, 60)):                     # stop somewhere in the middle of a segment
            clock[0] += 1
            nv.step_all(1.0)
        j = next((k for k, c in enumerate(t.cum) if c >= t.done_km() + 1.4), None)
        if t.status != "driving" or j is None:
            continue
        la, lo = G.pos[t.path[j]]
        fence = gsir.geofence_operator("gf_t", la, lo, 900, clock[0] * 1000 + 6e7, "ep_t")
        holder["v"] = nav.TrafficView({}, gsir.apply_patch(gsir.default_plan(),
                                                            {"ops": [{"op": "add_operator", "operator": fence}]}))
        before, n0 = t.position(), len(t.messages)
        t.last_reroute = 0
        nv._look_ahead(t, holder["v"])
        assert geo.haversine_m(*before, *t.position()) < 1.0       # no jump back to the previous node
        msgs = [m for m in t.messages[n0:] if m["kind"] == "reroute"]
        if msgs:
            checked += 1
            uturns += "U-turn now" in msgs[-1]["text"]
            if len(t.path) > 2 and t.path[2] == t.path[0]:          # route doubles back at the next node ...
                assert t.turn and t.turn["word"] == "around"         # ... so it must say U-turn, not left/right
    assert uturns >= 1                                               # turning round on the spot is considered
