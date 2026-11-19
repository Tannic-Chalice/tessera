"""A driver report (Navigator 'Block road ahead') makes a *statistically confirmed* hotspot skip the cache."""
import json

import h3

from tessera import config
from tessera.services import gate as G

SPOT = (12.9592, 77.5662)


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, key=None, value=None, **k):
        self.sent.append((topic, json.loads(value)))

    def poll(self, *a):
        return 0


class AlwaysHit:
    def lookup(self, *a, **k):
        return {"tile": "x", "similarity": 0.99, "decision": {"label": "dense_traffic", "risk": 0.4}}

    def stats(self):
        return {"hit_rate": 1.0}


def window():
    c = h3.latlng_to_cell(*SPOT, config.TILE_RES)
    recs = {}
    for i, t in enumerate(h3.grid_disk(c, 4)):
        la, lo = h3.cell_to_latlng(t)
        hot = t in h3.grid_disk(c, 1)
        recs[t] = {"tile": t, "lat": la, "lon": lo, "n_obs": 120 if hot else 20, "slow_frac": 0.95 if hot else 0.2,
                   "mean_speed": 4.0 if hot else 25.0, "vehicles": 40 if hot else 6}
    return recs


def make(monkeypatch):
    fp = FakeProducer()
    monkeypatch.setattr(G, "make_producer", lambda: fp)
    monkeypatch.setattr(G, "make_consumer", lambda *a, **k: None)
    g = G.Gate()
    g.cache = AlwaysHit()
    for _ in range(6):                                   # warm the city baseline with quiet traffic
        quiet = {t: {**r, "n_obs": 20, "slow_frac": 0.2} for t, r in window().items()}
        g.evaluate(1, quiet)
    return g, fp


def surprises(fp):
    return [v for t, v in fp.sent if t == config.T_SURPRISE]


def test_without_a_report_the_cache_answers(monkeypatch):
    g, fp = make(monkeypatch)
    g.evaluate(2, window())
    assert not surprises(fp) and g.c_stats["cache_hits"] >= 1


def test_a_driver_report_skips_the_cache_and_is_flagged(monkeypatch):
    g, fp = make(monkeypatch)
    g.on_control({"type": "inject_incident", "lat": SPOT[0] + 0.002, "lon": SPOT[1], "kind": "Accident"})
    g.evaluate(2, window())
    s = surprises(fp)
    assert len(s) == 1 and s[0]["reported"]["lat"] == SPOT[0] + 0.002


def test_a_report_alone_never_wakes_the_agents(monkeypatch):
    g, fp = make(monkeypatch)
    g.on_control({"type": "inject_incident", "lat": SPOT[0], "lon": SPOT[1], "kind": "Accident"})
    g.evaluate(2, {t: {**r, "n_obs": 20, "slow_frac": 0.2} for t, r in window().items()})
    assert not surprises(fp)
