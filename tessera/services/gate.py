"""SURPRISE GATE (L3) + SPATIO-SEMANTIC CACHE.  Consumer group `tessera-gate`.

tile.aggregates ──► per window: Getis-Ord G*_i over k-ring(1) ──► Benjamini-Hochberg FDR control
   ──► hotspot clusters ──► cache hit?  yes ─► reuse decision (alerts, agent.decisions source=cache)
                                         no  ─► token bucket ─► surprise.events (evidence bundle)
agent.decisions ──► cache insert + decision-field variogram ──► reuse radius k*

This is the throttle: agent invocations scale with *surprise* (bounded by q*m0 + m1), not with
the number of GPS tuples."""
import heapq
import time
from collections import defaultdict
import uuid

from confluent_kafka import KafkaError

from .. import config, geo, stats
from ..kafka_utils import GracefulExit, Metrics, dumps, loads, make_consumer, make_producer, setup_logging, \
    wait_for_kafka, wait_for_topics
from ..memory import SpatioSemanticCache, regime

log = setup_logging("gate")
SETTLE_S = 3.0          # a window is evaluated once no partition has updated it for this long


def cluster_summary(tiles, recs):
    n = sum(recs[t]["n_obs"] for t in tiles)
    slow = sum(recs[t]["n_obs"] * recs[t]["slow_frac"] for t in tiles)
    speed = sum(recs[t]["n_obs"] * recs[t]["mean_speed"] for t in tiles) / max(1, n)
    veh = sum(recs[t]["vehicles"] for t in tiles)
    return {"n_obs": n, "slow_frac": round(slow / max(1, n), 4), "mean_speed": round(speed, 2), "vehicles": veh}


class Gate:
    def __init__(self):
        self.stop = GracefulExit()
        self.p = make_producer()
        self.metrics = Metrics(self.p, "gate")
        self.city_metrics = Metrics(self.p, "gate_city")
        self.city_counts = defaultdict(lambda: defaultdict(int))
        self.c = make_consumer(config.G_GATE, offset_reset="latest")
        self.windows = {}             # (city, window_end) -> {tile: rec}
        self.touched = {}             # (city, window_end) -> last update wall time
        self.evaluated_upto = {}      # city -> newest evaluated window end (cities close windows at different times)
        self.baselines = {}          # city -> EWMA: each city is tested against its own normal traffic
        self.cache = SpatioSemanticCache(tau_s=config.CACHE_TTL_S, sigma=config.CACHE_COS)
        self.vario = stats.DecisionVariogram(eps=config.VARIOGRAM_EPS, tau_s=900)
        self.bucket = stats.TokenBucket(config.TOKENS_PER_MIN, config.TOKEN_BURST)
        self.deferred = []            # heap (-|z|, seq, bundle)
        self.seq = 0
        self.inflight = {}            # tile -> wall time of last escalation (avoid re-asking while agent thinks)
        self.c_stats = {"windows": 0, "tests": 0, "escalated_tiles": 0, "clusters": 0, "invocations": 0,
                        "cache_hits": 0, "deferred": 0, "expired_deferred": 0, "inflight_suppressed": 0,
                        "expected_false_wakeups": 0.0}
        self.last_metric = time.time()
        self.last_commit = time.time()
        self.last_eval = {}

    def on_aggregate(self, rec):
        end = int(round(rec["window_end"] * 1000))
        city = config.city_of(rec["lat"], rec["lon"])
        if end <= self.evaluated_upto.get(city, 0):
            return               # straggler for an already-evaluated window (replay after restart)
        self.windows.setdefault((city, end), {})[rec["tile"]] = rec
        self.touched[(city, end)] = time.time()

    def on_decision(self, d):
        if d.get("source") != "agent":
            return
        t = d["ts"]
        self.vario.add(d["tile"], t, d["risk"])
        if d.get("is_cluster_head"):
            self.cache.insert(d["tile"], t, d["features_vec"], d.get("regime") or regime(t), d)

    def evaluate_ready(self):
        now = time.time()
        blocked = set()
        for key in sorted(self.windows, key=lambda k: k[1]):
            city, end = key
            if city in blocked or now - self.touched[key] < SETTLE_S:
                blocked.add(city)                    # keep each city's windows in order
                continue
            recs = self.windows.pop(key)
            self.touched.pop(key, None)
            self.evaluated_upto[city] = max(self.evaluated_upto.get(city, 0), end)
            self.evaluate(end, recs)

    def evaluate(self, end, recs):
        q = float(config.FDR_Q)
        primary = next(iter(recs.values()), None)
        city_key = config.city_of(primary["lat"], primary["lon"]) if primary else None
        city_count = self.city_counts[city_key] if city_key else None
        values = {t: r["n_obs"] * r["slow_frac"] for t, r in recs.items()}     # monitored quantity
        by_city = {}
        for t in values:
            by_city.setdefault(config.city_of(recs[t]["lat"], recs[t]["lon"]), []).append(t)
        z = {}
        for city, tiles in by_city.items():
            cv = {t: values[t] for t in tiles}
            active = set(cv)
            for t in tiles:
                active.update(geo.ring(t, 1))
            z.update(stats.gstar(cv, active, self.baselines.setdefault(city, stats.EWMA(alpha=0.2)), k=1))
        pvals = {t: stats.p_two_sided(zz) for t, zz in z.items()}
        rejected = stats.benjamini_hochberg(pvals, q)
        hot = [t for t in rejected if z[t] > 0 and values[t] >= config.GATE_MIN_SLOW]
        m = len(pvals)
        self.c_stats["windows"] += 1
        self.c_stats["tests"] += m
        self.c_stats["escalated_tiles"] += len(hot)
        if city_count is not None:
            city_count["windows"] += 1
            city_count["tests"] += m
            city_count["escalated_tiles"] += len(hot)
        self.c_stats["expected_false_wakeups"] += q * m
        self.last_eval = {"window_end": end / 1000, "tiles_tested": m, "escalated": len(hot),
                          "fdr_q": q, "max_z": round(max(z.values()), 2) if z else 0.0}
        if not hot:
            return
        for comp in stats.clusters(hot):
            self.c_stats["clusters"] += 1
            head = max(comp, key=lambda t: z[t])
            summ = cluster_summary(comp, recs)
            if summ["vehicles"] < config.GATE_MIN_VEHICLES:
                continue                     # too few distinct vehicles to call it a traffic hotspot
            vec = stats.features(summ)
            lat = sum(recs[t]["lat"] for t in comp) / len(comp)
            lon = sum(recs[t]["lon"] for t in comp) / len(comp)
            t_s = end / 1000
            alert = {"alert_id": f"hs:{end}:{head}", "ts": t_s, "type": "HOTSPOT_DETECTED", "severity": "low",
                     "tile": head, "lat": lat, "lon": lon,
                     "message": f"G* hotspot: {len(comp)} tile(s), z={z[head]:.1f}, p={pvals[head]:.1e} "
                                f"(BH q={q}), slow frac {summ['slow_frac']:.0%}",
                     "plan_version": None, "provenance_id": "gate:gstar_bh",
                     "payload": {"tiles": comp, "z": round(z[head], 3), "p": pvals[head], **summ}}
            self.p.produce(config.T_ALERTS, key=head.encode(), value=dumps(alert))

            # regime = time-of-day band + severity band: a decision is never reused across a structural
            # break (e.g. an earlier "dense traffic" verdict must not mask a now-severe incident)
            reg = f"{regime(t_s)}|sev{min(3, int(summ['slow_frac'] * 4))}"
            hit = self.cache.lookup(head, t_s, vec, reg, self.vario.k_star)
            if hit:
                self.c_stats["cache_hits"] += 1
                if city_count is not None:
                    city_count["cache_hits"] += 1
                dec = hit["decision"]
                reuse = {"decision_id": f"cache:{end}:{head}", "ts": t_s, "tile": head, "lat": lat, "lon": lon,
                         "label": dec["label"], "risk": dec["risk"], "source": "cache",
                         "features": summ, "episode_id": dec.get("episode_id"),
                         "reused_from": hit["tile"], "similarity": hit["similarity"], "k_star": self.vario.k_star}
                self.p.produce(config.T_DECISIONS, key=head.encode(), value=dumps(reuse))
                if dec["label"] == "congestion_incident":
                    self.p.produce(config.T_ALERTS, key=head.encode(), value=dumps({
                        "alert_id": f"rc:{end}:{head}", "ts": t_s, "type": "CACHED_DECISION", "severity": "medium",
                        "tile": head, "lat": lat, "lon": lon,
                        "message": f"reused agent judgement from {hit['tile']} (k*={self.vario.k_star}, "
                                   f"cos={hit['similarity']}) - no model call",
                        "provenance_id": dec.get("episode_id"), "plan_version": None, "payload": reuse}))
                continue
            if any(time.time() - self.inflight.get(t, 0) < 45 for t in comp):
                self.c_stats["inflight_suppressed"] += 1
                continue
            bundle = {
                "event_id": "ev_" + uuid.uuid4().hex[:10], "ts": t_s, "window_end": t_s,
                "city": city_key or config.city_of(lat, lon), "head": head, "tiles": comp, "lat": lat, "lon": lon, "max_z": round(z[head], 3),
                "p": pvals[head], "summary": summ, "features_vec": vec, "regime": reg,
                "top_tiles": sorted(([t, recs[t]] for t in comp), key=lambda x: -z[x[0]])[:12],
                "neighbours": [[t, recs[t], round(z.get(t, 0.0), 3)]
                               for t in {n for c in comp for n in geo.ring(c, 2)} if t in recs and t not in comp][:30],
                "gate": {"statistic": "getis_ord_gstar", "fdr_q": q, "m": m, "k_star": self.vario.k_star},
            }
            self.dispatch(bundle)

    def dispatch(self, bundle):
        if self.bucket.try_acquire():
            for t in bundle["tiles"]:
                self.inflight[t] = time.time()
            self.p.produce(config.T_SURPRISE, key=bundle["head"].encode(), value=dumps(bundle))
            self.c_stats["invocations"] += 1
            city_key = bundle.get("city") or config.city_of(bundle["lat"], bundle["lon"])
            self.city_counts[city_key]["invocations"] += 1
            log.info("ESCALATE %s: %d tiles z=%.1f -> slow plane", bundle["event_id"], len(bundle["tiles"]),
                     bundle["max_z"])
        else:
            self.seq += 1
            heapq.heappush(self.deferred, (-abs(bundle["max_z"]), self.seq, time.time(), bundle))
            self.c_stats["deferred"] += 1

    def drain_deferred(self):
        while self.deferred:
            _, _, t_in, bundle = self.deferred[0]
            if time.time() - t_in > 60:
                heapq.heappop(self.deferred)
                self.c_stats["expired_deferred"] += 1
                continue
            if not self.bucket.try_acquire():
                return
            heapq.heappop(self.deferred)
            self.bucket.tokens += 1   # give the token back; dispatch() takes it
            self.dispatch(bundle)

    def emit_metrics(self):
        s = self.c_stats
        vals = {**s, "cache_hit_rate": self.cache.stats()["hit_rate"], "k_star": self.vario.k_star,
                "deferred_queue": len(self.deferred), "tokens": round(self.bucket.tokens, 2),
                "invocations_per_window": s["invocations"] / max(1, s["windows"])}
        self.metrics.emit(vals, {"variogram": self.vario.state(), "last_eval": self.last_eval,
                                 "cache": self.cache.stats()})
        for city, counts in self.city_counts.items():
            self.city_metrics.emit(dict(counts), {"city": city})
        log.info("windows=%d tests=%d escalated=%d invocations=%d cache_hits=%d k*=%d deferred=%d",
                 s["windows"], s["tests"], s["escalated_tiles"], s["invocations"], s["cache_hits"],
                 self.vario.k_star, len(self.deferred))
        self.last_metric = time.time()

    def run(self):
        self.c.subscribe([config.T_AGG, config.T_DECISIONS])
        log.info("gate joined consumer group %s (FDR q=%.3f, %g invocations/min)", config.G_GATE, config.FDR_Q,
                 config.TOKENS_PER_MIN)
        while not self.stop.stop:
            for m in self.c.consume(num_messages=500, timeout=0.5):
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("consumer error: %s", m.error())
                    continue
                v = loads(m.value())
                if m.topic() == config.T_AGG:
                    self.on_aggregate(v)
                else:
                    self.on_decision(v)
            self.evaluate_ready()
            self.drain_deferred()
            self.p.poll(0)
            if time.time() - self.last_commit > 5:
                try:
                    self.c.commit(asynchronous=True)
                except Exception:  # noqa: BLE001
                    pass
                self.last_commit = time.time()
            if time.time() - self.last_metric > 5:
                self.emit_metrics()
        self.p.flush(10)
        self.c.close()


def main():
    wait_for_kafka()
    wait_for_topics([config.T_AGG, config.T_DECISIONS, config.T_SURPRISE])
    Gate().run()


if __name__ == "__main__":
    main()
