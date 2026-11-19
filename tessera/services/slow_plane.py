"""SLOW PLANE: reasoning agents that never touch a tuple.  Consumer group `tessera-slowplane`.

surprise.events ──► Planner (reason over evidence + Geo-Episodic Memory) ──► GS-IR patch
   ──► Geo-Verifier (static checks; typed repair loop) ──► Governor (blast radius)
   ──► shadow run on the live stream (plan.patches stage=shadow) ──► promote (stage=active) / reject
   ──► Curator (GEM facts) + agent.decisions (feeds the gate's cache & variogram)
   ──► Distiller (judgement -> deterministic operator) and automatic demotion on agreement drift

The agent's only output type is a plan patch; its latency never enters the data path (P1)."""
import json
import math
import os
import random
import time
import urllib.request
import uuid
from collections import defaultdict, deque

from confluent_kafka import KafkaError

from .. import agents, config, geo, gsir, roads, stats, verifier
from ..db import DB, jsonb
from ..kafka_utils import GracefulExit, Metrics, dumps, loads, make_consumer, make_producer, setup_logging, \
    wait_for_kafka, wait_for_topics
from ..memory import GeoEpisodicMemory

log = setup_logging("slowplane")
rng = random.Random()
GEOFENCE_TTL_MS = 6 * 60 * 1000          # temporary geofences expire unless the incident persists
HOLD_TTL_S = int(os.environ.get("HOLD_TTL_S", "300"))                        # an unanswered approval request is withdrawn after 5 min (incident over)


def tile_risk(f):
    """Rule-based judgement over named features (the default, free, deterministic 'reasoner')."""
    x = 9.0 * (f.get("slow_frac", 0) - 0.4) - 0.06 * (f.get("mean_speed", 30) - 15) \
        + 0.5 * math.log1p(f.get("n_obs", 0) / 40.0)
    return 1.0 / (1.0 + math.exp(-x))


class RulePlanner:
    name = "rule-based"

    def reason(self, bundle, facts, conflict):
        s = bundle["summary"]
        risk = tile_risk(s)
        risk = min(1.0, risk + 0.03 * max(0.0, bundle["max_z"] - 4))
        prior = [f for f in facts if f["fact_class"] == "congestion_incident"]
        if prior and risk > 0.45:
            risk = max(risk, 0.6)       # memory: an incident was recently confirmed here
        if conflict:
            risk *= 0.9
        label = "congestion_incident" if risk >= 0.6 else "dense_traffic"
        return {"label": label, "risk": round(risk, 3),
                "rationale": f"slow fraction {s['slow_frac']:.0%}, mean speed {s['mean_speed']:.0f} km/h, "
                             f"{s['vehicles']} vehicles, G* z={bundle['max_z']:.1f}; "
                             f"{len(prior)} prior incident fact(s) in memory" + ("; conflicting facts" if conflict else "")}


class OllamaPlanner(RulePlanner):
    """Optional: a free local LLM (Ollama) fills the *typed slots* of the decision. Output is JSON
    constrained, sanitised, and still has to pass the Geo-Verifier. Falls back to rules on any error."""
    name = "ollama"

    LLM_SWAY = 0.15          # how far the LLM may move the statistically grounded risk

    def reason(self, bundle, facts, conflict):
        base = super().reason(bundle, facts, conflict)
        prompt = agents.planner_prompt(config.CITY_PROFILES[config.city_of(bundle["lat"], bundle["lon"])][3],
                                       bundle["summary"], bundle["max_z"], len(bundle["tiles"]), facts)
        try:
            req = urllib.request.Request(config.OLLAMA_URL.rstrip("/") + "/api/generate", data=json.dumps(
                {"model": config.OLLAMA_MODEL, "prompt": prompt, "format": "json", "stream": False,
                 "options": {"temperature": 0.1}}).encode(),
                headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=60) as r:
                out = json.loads(json.loads(r.read())["response"])
            llm_risk = float(out.get("risk"))
            if not 0 <= llm_risk <= 1:
                raise ValueError("schema violation")
            label = "congestion_incident" if llm_risk >= 0.6 else "dense_traffic"   # label follows the number
        except Exception as e:  # noqa: BLE001
            log.warning("ollama planner failed (%s) - falling back to rules", e)
            return base
        # Grounded fusion: the statistics (G*, slow fraction, memory) set the risk; a small local LLM is
        # an advisor that may nudge it by at most +-LLM_SWAY - enough to tip borderline cases, never to
        # overturn a z=15 hotspot or invent one out of free-flowing traffic.
        risk = base["risk"] + max(-self.LLM_SWAY, min(self.LLM_SWAY, llm_risk - base["risk"]))
        risk = round(min(1.0, max(0.0, risk)), 3)
        return {"label": "congestion_incident" if risk >= 0.6 else "dense_traffic", "risk": risk,
                "rationale": f"{base['rationale']}; LLM: {label} (risk {llm_risk:.2f}) - "
                             + str(out.get("rationale", ""))[:160]}


class SlowPlane:
    def __init__(self):
        self.stop = GracefulExit()
        self.db = DB().connect()
        self.gem = GeoEpisodicMemory(self.db)
        self.p = make_producer()
        self.metrics = Metrics(self.p, "slowplane")
        self.city_metrics = Metrics(self.p, "slowplane_city")
        self.city_counts = defaultdict(lambda: defaultdict(int))
        self.c = make_consumer(config.G_SLOW, offset_reset="latest")
        self.bc = make_consumer(f"tessera-slowplane-watch-{uuid.uuid4().hex[:6]}", offset_reset="earliest")
        self.planner = OllamaPlanner() if config.OLLAMA_URL else RulePlanner()
        self.plan = gsir.default_plan()
        self.max_version = 1
        self.shadow = None                 # {"plan", "started", "meta"}
        self.queue = deque()               # patches waiting for the shadow slot
        self.held = {}                     # version -> candidate awaiting human approval
        self.agent_delay = config.AGENT_LATENCY_S
        self.fault_rate = config.FAULT_INJECTION_RATE
        self.fast_metrics = {}             # instance -> latest fast plane metrics
        self.episodes = deque(maxlen=400)  # (features, label) for the distiller
        self.agreement = deque(maxlen=20)
        self.last_demotion = 0
        self.n = {"episodes": 0, "patches": 0, "verifier_rejections": 0, "repairs": 0, "promoted": 0,
                  "shadow_rejected": 0, "held": 0, "holds_expired": 0, "distilled": 0, "demoted": 0, "gem_written": 0,
                  "gem_excluded_decayed": 0}
        self.last_metric = time.time()
        # multi-agent mode: city agents + critic + coordinator talking over agent.messages
        self.bus = agents.Bus(lambda m: self.p.produce(config.T_AGENT_MSGS, key=m["conv"].encode(), value=dumps(m)), log)
        self.city_agents = {c: agents.CityAgent(c, self.planner) for c in config.ACTIVE_CITIES}
        self.critic = agents.Critic(use_llm=bool(config.OLLAMA_URL) and config.OLLAMA_CRITIC)
        self.coordinator = agents.Coordinator(self.bus)
        self.mas = {"proposals": 0, "accepted": 0, "revised": 0, "rejected_by_critic": 0}

    def say(self, conv, frm, perf, text, bundle=None, content=None, to="all"):
        if config.MAS_ENABLED and conv:
            self.bus.send(conv, frm, to, perf, text, content, bundle)

    # ------------------------------------------------------------- helpers
    def alert(self, kind, severity, message, payload=None, lat=None, lon=None, tile=None):
        a = {"alert_id": f"sp:{kind}:{uuid.uuid4().hex[:10]}", "ts": time.time(), "type": kind, "severity": severity,
             "tile": tile, "lat": lat, "lon": lon, "message": message, "plan_version": self.plan["version"],
             "provenance_id": "slowplane", "payload": payload or {}}
        self.p.produce(config.T_ALERTS, key=kind.encode(), value=dumps(a))

    def alloc_version(self):
        self.max_version += 1
        return self.max_version

    def ctx(self):
        p99 = max([m.get("p99_ms", 0) for m in self.fast_metrics.values()] or [5.0])
        ooo = max([m.get("ooo_p95_ms", 0) for m in self.fast_metrics.values()] or [0])
        return {"observed_p99_ms": min(p99, 200.0), "observed_ooo_ms": ooo}

    def record_plan(self, plan, stage, disposition, diff, verification, provenance):
        self.db.execute("""INSERT INTO plan_versions (version, stage, disposition, plan, diff, verification, provenance)
                           VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (version, stage) DO UPDATE
                           SET disposition=EXCLUDED.disposition, created_at=now()""",
                        (plan["version"], stage, disposition, jsonb(plan), jsonb(diff), jsonb(verification),
                         jsonb(provenance)))

    def publish_plan(self, stage, plan, meta):
        key = b"active" if stage == "active" else b"shadow"
        self.p.produce(config.T_PLAN, key=key, value=dumps({"stage": stage, "version": plan["version"], "plan": plan,
                                                            "ts": time.time(), **meta}))
        self.p.flush(10)

    # ------------------------------------------------------------- reasoning episode
    def on_bundle(self, b, ep=None, agent=None):
        ep = ep or "ep_" + uuid.uuid4().hex[:10]
        self.n["episodes"] += 1
        t0 = time.time()
        if self.agent_delay > 0:
            time.sleep(self.agent_delay)            # stands in for LLM time-to-first-token + generation
        facts, excluded, conflict = self.gem.retrieve(b["tiles"], k=2, delta_s=3600)
        self.n["gem_excluded_decayed"] += excluded
        decision = (agent or self.planner).reason(b, facts, conflict)
        self._facts = facts
        city = config.city_of(b["lat"], b["lon"])
        place = roads.nearest_landmark(b["lat"], b["lon"]) if city == "bengaluru" else None
        if place is None and len(config.ACTIVE_CITIES) > 1:
            place = config.CITY_PROFILES[city][3]
        if place:
            decision["place"] = place
            decision["rationale"] = f"near {place}: " + decision["rationale"]
        log.info("episode %s: %s risk=%.2f (%s) [%.1fs]", ep, decision["label"], decision["risk"],
                 decision["rationale"], time.time() - t0)

        # decision field: one judgement per tile in and around the hotspot (feeds variogram + distiller)
        decs = []
        for t, rec in b["top_tiles"]:
            decs.append((t, rec, decision["risk"] if t == b["head"] else round(0.5 * decision["risk"] + 0.5 * tile_risk(rec), 3)))
        for t, rec, _z in b["neighbours"]:
            decs.append((t, rec, round(tile_risk(rec), 3)))
        for t, rec, r in decs:
            label = decision["label"] if t == b["head"] else ("congestion_incident" if r >= 0.6 else "dense_traffic")
            msg = {"decision_id": f"{ep}:{t}", "ts": b["ts"], "tile": t, "lat": rec["lat"], "lon": rec["lon"],
                   "label": label, "risk": r, "source": "agent", "episode_id": ep,
                   "features": {k: rec[k] for k in ("n_obs", "vehicles", "mean_speed", "slow_frac")},
                   "is_cluster_head": t == b["head"], "rationale": decision["rationale"] if t == b["head"] else None}
            if t == b["head"]:
                msg["features_vec"] = b["features_vec"]
                msg["regime"] = b.get("regime")
            self.p.produce(config.T_DECISIONS, key=t.encode(), value=dumps(msg))
            self.episodes.append((stats.features(rec), 1 if label == "congestion_incident" else 0,
                                  config.city_of(rec["lat"], rec["lon"])))

        self.alert("AGENT_DECISION", "high" if decision["label"] == "congestion_incident" else "info",
                   f"{decision['label']} (risk {decision['risk']:.2f}) - {decision['rationale']}",
                   {"episode_id": ep, "event_id": b["event_id"], "planner": self.planner.name, **decision},
                   b["lat"], b["lon"], b["head"])

        # Curator: georeferenced, time-bounded, decaying fact
        cls = "congestion_incident" if decision["label"] == "congestion_incident" else "clear"
        self.gem.write(b["tiles"], b["lat"], b["lon"],
                       f"{decision['label']} near ({b['lat']:.4f},{b['lon']:.4f}): {decision['rationale']}",
                       cls, decision["risk"] if cls != "clear" else 1 - decision["risk"],
                       900 if cls != "clear" else 300, {"episode_id": ep, "event_id": b["event_id"]},
                       self.plan["version"])
        self.n["gem_written"] += 1

        self.check_distilled_agreement(b["summary"], decision)

        if agent is not None:
            self.say(ep, agent.name, "INFORM", f"judgement: {decision['label']} risk {decision['risk']:.2f} - "
                     f"{decision['rationale']}", b, {"decision": decision})
        if decision["label"] == "congestion_incident":
            if agent is not None:
                self.mas["proposals"] += 1
                p = agents.negotiate(self.bus, agent, self.critic, b, decision, facts, self.plan,
                                     int(time.time() * 1000), ep)
                if p is None:
                    self.mas["rejected_by_critic"] += 1
                else:
                    self.mas["accepted"] += 1
                    if any(m["performative"] == "REVISE" for m in list(self.bus.recent)[-6:] if m["conv"] == ep):
                        self.mas["revised"] += 1
                    self.submit_proposal(p, b, ep)
            else:
                self.propose_geofence(b, decision, ep)
        else:
            self.prune_expired(ep)
        self.maybe_distill(ep)

    # ------------------------------------------------------------- patch construction
    def propose_geofence(self, b, decision, ep):
        now_ms = int(time.time() * 1000)
        covering = [g for g in gsir.active_geofences(self.plan, now_ms)
                    if geo.haversine_m(g["shape"]["lat"], g["shape"]["lon"], b["lat"], b["lon"]) < g["shape"]["radius"]]
        pending = self.held_covering(b["lat"], b["lon"])
        if not covering and pending:
            log.info("incident near (%.4f, %.4f) already awaiting human approval as v%d - not re-proposing",
                     b["lat"], b["lon"], pending)
            return
        ops = self.expired_ops(now_ms)
        if covering:
            g = dict(covering[0])
            g["expires_ms"] = now_ms + GEOFENCE_TTL_MS          # refresh TTL instead of duplicating
            g["provenance_id"] = ep
            ops.append({"op": "add_operator", "operator": g})
            reason = f"refresh geofence {g['id']} (incident persists)"
        else:
            extent = max(geo.haversine_m(b["lat"], b["lon"], *geo.center(t)) for t in b["tiles"])
            radius = min(max(500.0, extent + 450.0), 0.85 * self.plan["policy"].get("max_geofence_radius_m", 3000))
            gid = "gf_" + uuid.uuid4().hex[:6]
            ops.append({"op": "add_operator", "operator": gsir.geofence_operator(
                gid, b["lat"], b["lon"], radius, now_ms + GEOFENCE_TTL_MS, ep, self.plan["tile"]["res"])})
            if len(gsir.active_geofences(self.plan, now_ms)) + 1 > self.plan["policy"]["max_geofences"]:
                oldest = min(gsir.active_geofences(self.plan, now_ms), key=lambda g: g["expires_ms"])
                ops.append({"op": "remove_operator", "id": oldest["id"]})
            reason = f"add corridor-avoidance geofence {gid} r={radius:.0f} m"
        self.submit({"ops": ops}, ep, reason, b)

    def submit_proposal(self, p, b, ep):
        """Turn an accepted multi-agent proposal into a typed GS-IR patch (same checks as every patch)."""
        now_ms = int(time.time() * 1000)
        ops = self.expired_ops(now_ms)
        if p["action"] == "refresh":
            g = next((dict(g) for g in gsir.active_geofences(self.plan, now_ms) if g["id"] == p["gid"]), None)
            if g is None:
                return
            g["expires_ms"] = now_ms + GEOFENCE_TTL_MS
            g["provenance_id"] = ep
            ops.append({"op": "add_operator", "operator": g})
            reason = f"refresh geofence {g['id']} (incident persists)"
        else:
            if self.held_covering(p["lat"], p["lon"]):
                return
            ops.append({"op": "add_operator", "operator": gsir.geofence_operator(
                p["gid"], p["lat"], p["lon"], p["radius"], now_ms + GEOFENCE_TTL_MS, ep, self.plan["tile"]["res"])})
            active = gsir.active_geofences(self.plan, now_ms)
            if len(active) + 1 > self.plan["policy"]["max_geofences"]:
                oldest = min(active, key=lambda g: g["expires_ms"])
                ops.append({"op": "remove_operator", "id": oldest["id"]})
            reason = f"add corridor-avoidance geofence {p['gid']} r={p['radius']:.0f} m (multi-agent)"
        self.submit({"ops": ops}, ep, reason, b)

    def held_covering(self, lat, lon):
        """Version of a pending approval whose geofence already covers this point (avoid duplicate requests)."""
        for v, (patch, *_rest) in self.held.items():
            for op in patch["ops"]:
                o = op.get("operator") or {}
                if o.get("kind") == "geofence" and geo.haversine_m(
                        o["shape"]["lat"], o["shape"]["lon"], lat, lon) < o["shape"]["radius"]:
                    return v
        return None

    def expire_holds(self):
        """Approval requests are time-bound: if no human answers within HOLD_TTL_S the incident is likely over,
        so the request is withdrawn instead of piling up."""
        now = time.time()
        for v in [v for v, h in self.held.items() if now - h[4] > HOLD_TTL_S]:
            self.held.pop(v)
            self.db.execute("UPDATE plan_versions SET disposition='expired' WHERE version=%s AND stage='held'", (v,))
            self.n["holds_expired"] += 1
            log.info("approval request v%d expired after %ds without a decision", v, HOLD_TTL_S)
            self.alert("APPROVAL_EXPIRED", "info", f"approval request for plan v{v} withdrawn after "
                       f"{HOLD_TTL_S // 60} min without a decision", {"version": v})

    def expired_ops(self, now_ms):
        return [{"op": "remove_operator", "id": g["id"]} for g in gsir.operators(self.plan, "geofence")
                if g.get("expires_ms", 0) <= now_ms]

    def prune_expired(self, ep):
        ops = self.expired_ops(int(time.time() * 1000))
        if ops:
            self.submit({"ops": ops}, ep, f"retire {len(ops)} expired geofence(s)", None)

    def submit(self, patch, ep, reason, bundle):
        self.n["patches"] += 1
        city_key = config.city_of(bundle["lat"], bundle["lon"]) if bundle and bundle.get("lat") is not None else None
        if city_key:
            self.city_counts[city_key]["patches"] += 1
        if self.shadow is not None:
            self.queue.append((patch, ep, reason, bundle))
            return
        self.process_patch(patch, ep, reason, bundle)

    def process_patch(self, patch, ep, reason, bundle, approved=False, members=None):
        members = members or [(ep, bundle)]             # every agent conversation this patch answers
        city_key = config.city_of(bundle["lat"], bundle["lon"]) if bundle and bundle.get("lat") is not None else None
        # Verifier gate 1, with a typed repair loop. The agent sometimes "hallucinates" a broken patch
        # (FAULT_INJECTION_RATE) - exactly the mechanical errors LLMs are documented to make.
        base = self.plan
        injected = None
        for attempt in range(3):
            try:
                cand = gsir.apply_patch(base, patch)
            except ValueError as e:
                log.warning("malformed patch: %s", e)
                return
            if attempt == 0 and any(o["op"] == "add_operator" and o["operator"]["kind"] == "geofence"
                                    for o in patch["ops"]) and rng.random() < self.fault_rate:
                cand, injected = verifier.inject_fault(cand, rng=rng)
            cand["version"] = self.max_version + 1
            vr = verifier.verify(cand, self.ctx())
            rid = "vr_" + uuid.uuid4().hex[:10]
            self.db.execute("INSERT INTO verifier_reports (report_id, plan_version, passed, injected_fault, errors, city) "
                            "VALUES (%s,%s,%s,%s,%s,%s)", (rid, cand["version"], vr["passed"],
                                                        injected if attempt == 0 else None, jsonb(vr["errors"]), city_key))
            if vr["passed"]:
                for m_ep, m_b in members:
                    self.say(m_ep, "verifier", "VERIFIED", "12 geo checks passed" + (" after repair" if attempt else ""), m_b)
                break
            for m_ep, m_b in members:
                self.say(m_ep, "verifier", "BLOCKED", "; ".join(sorted({e["code"] for e in vr["errors"]}))
                         + (" (injected test fault)" if injected else "") + " - repairing", m_b)
            self.n["verifier_rejections"] += 1
            if city_key:
                self.city_counts[city_key]["verifier_rejections"] += 1
            codes = sorted({e["code"] for e in vr["errors"]})
            log.info("VERIFIER rejected candidate (%s): %s", injected or "agent error",
                     "; ".join(e["msg"] for e in vr["errors"][:3]))
            self.alert("VERIFIER_REJECTED", "info",
                       f"static verification blocked a patch [{', '.join(codes)}]"
                       + (f" - injected fault '{injected}'" if injected else "") + "; agent repairing",
                       {"errors": vr["errors"], "injected_fault": injected, "episode_id": ep})
            self.n["repairs"] += 1
            injected = None
        else:
            log.warning("patch abandoned after repeated verification failures")
            return
        cand["version"] = self.alloc_version()
        diff = {"ops": patch["ops"], "reason": reason, "base_version": base["version"]}
        prov = {"episode_id": ep, "reason": reason, "planner": self.planner.name}

        # Governor: blast-radius classification
        disposition = self.classify(patch) if not approved else "approved"
        if disposition == "reject":
            self.record_plan(cand, "rejected", "reject", diff, vr, prov)
            return
        if disposition == "hold":
            self.held[cand["version"]] = (patch, ep, reason, bundle, time.time())
            self.n["held"] += 1
            if city_key:
                self.city_counts[city_key]["held"] += 1
            self.record_plan(cand, "held", "hold_for_approval", diff, vr, prov)
            self.alert("APPROVAL_REQUIRED", "medium", f"plan v{cand['version']} needs human approval: {reason}",
                       {"version": cand["version"], "diff": diff},
                       bundle["lat"] if bundle else None, bundle["lon"] if bundle else None)
            log.info("GOVERNOR: holding v%d for approval (%s)", cand["version"], reason)
            self.say(ep, "governor", "HELD", f"plan v{cand['version']} waits for human approval (zone > 1.3 km)", bundle)
            return
        # gate 2: shadow promotion on the live stream
        self.record_plan(cand, "shadow", disposition, diff, vr, prov)
        self.publish_plan("shadow", cand, {"provenance": prov, "diff": diff, "disposition": disposition})
        self.shadow = {"plan": cand, "started": time.time(), "ep": ep, "bundle": bundle, "members": members,
                       "meta": {"provenance": prov, "diff": diff, "disposition": disposition, "verification": vr}}
        for m_ep, m_b in members:
            self.say(m_ep, "governor", "SHADOW", f"plan v{cand['version']} running in shadow for {config.SHADOW_SOAK_S:.0f} s"
                     + (f" (batched with {len(members) - 1} other change(s))" if len(members) > 1 else ""), m_b)
        log.info("SHADOW v%d started (%s, %s)", cand["version"], disposition, reason)

    def classify(self, patch):
        adds = [o["operator"] for o in patch["ops"] if o["op"] == "add_operator"]
        if any(o.get("kind") == "geofence" and o["shape"]["radius"] > 1300 for o in adds):
            return "hold"                       # large area affected -> human decides
        if any(o.get("kind") == "distilled_filter" for o in adds):
            return "auto_promote_notify"
        return "auto_promote"

    def check_shadow(self):
        if not self.shadow:
            return
        v = self.shadow["plan"]["version"]
        elapsed = time.time() - self.shadow["started"]
        if elapsed < config.SHADOW_SOAK_S:
            return
        reports = [m for m in self.fast_metrics.values() if m.get("_labels", {}).get("shadow_version") == v]
        shadow_alerts = sum(m.get("shadow_alerts", 0) for m in reports)
        p99 = max([m.get("p99_ms", 0) for m in self.fast_metrics.values()] or [0])
        rate = shadow_alerts / max(1.0, elapsed / 60)
        budget = self.shadow["plan"]["budget"]["p99_ms"]
        ok = rate <= 400 and p99 <= max(budget, 2000)
        if not reports and elapsed < 3 * config.SHADOW_SOAK_S:
            return                              # wait until the fast plane has reported on this version
        meta = self.shadow["meta"]
        if ok:
            self.plan = self.shadow["plan"]
            self.publish_plan("active", self.plan, {"provenance": meta["provenance"], "diff": meta["diff"],
                                                     "disposition": meta["disposition"],
                                                     "shadow": {"alerts": shadow_alerts, "seconds": round(elapsed, 1),
                                                                "p99_ms": p99}})
            self.record_plan(self.plan, "active", meta["disposition"], meta["diff"], meta["verification"],
                             {**meta["provenance"], "shadow_alerts": shadow_alerts})
            self.n["promoted"] += 1
            if self.shadow.get("city"):
                self.city_counts[self.shadow["city"]]["promoted"] += 1
            self.alert("PLAN_PROMOTED", "info",
                       f"plan v{v} promoted after {elapsed:.0f}s shadow ({shadow_alerts} would-be alerts, "
                       f"p99 {p99:.0f} ms): {meta['diff']['reason']}", {"version": v, "diff": meta["diff"]})
            log.info("PROMOTED v%d after %.0fs shadow (%d would-be alerts)", v, elapsed, shadow_alerts)
            for m_ep, m_b in self.shadow.get("members") or [(self.shadow.get("ep"), self.shadow.get("bundle"))]:
                self.say(m_ep, "governor", "PROMOTED", f"plan v{v} is live: {meta['diff']['reason']}", m_b)
        else:
            self.publish_plan("rejected", self.shadow["plan"], {"reason": f"shadow alert rate {rate:.0f}/min"})
            self.record_plan(self.shadow["plan"], "rejected", "shadow_rejected", meta["diff"], meta["verification"],
                             meta["provenance"])
            self.n["shadow_rejected"] += 1
            self.alert("SHADOW_REJECTED", "medium", f"plan v{v} discarded after shadow: {rate:.0f} alerts/min")
            for m_ep, m_b in self.shadow.get("members") or [(self.shadow.get("ep"), self.shadow.get("bundle"))]:
                self.say(m_ep, "governor", "ROLLED_BACK", f"plan v{v} discarded after shadow", m_b)
        self.shadow = None
        self.drain_queue()

    def drain_queue(self):
        """Start the next shadow run. Everything that queued up while the slot was busy (from any city) goes
        through verification and one shadow run *together*, so a busy multi-city stream never builds a backlog
        and every incident waits at most one shadow period."""
        while self.queue and self.shadow is None:
            if len(self.queue[0]) > 4:                   # human-approved patch: its own run
                self.process_patch(*self.queue.popleft())
                continue
            batch, rest = [], deque()
            while self.queue:
                it = self.queue.popleft()
                if len(it) > 4 or self.classify(it[0]) == "hold":
                    rest.append(it)
                else:
                    batch.append(it)
            holds = [it for it in rest if len(it) == 4]
            self.queue = deque(it for it in rest if len(it) > 4)
            for it in holds:                             # held patches never occupy the shadow slot
                self.process_patch(*it)
            if not batch:
                continue
            if len(batch) == 1:
                self.process_patch(*batch[0])
                continue
            ops = [op for it in batch for op in it[0]["ops"]]
            now_ms = int(time.time() * 1000)
            added = {op["operator"]["id"] for op in ops if op["op"] == "add_operator"}
            cand = gsir.apply_patch(self.plan, {"ops": ops})
            active = sorted(gsir.active_geofences(cand, now_ms), key=lambda g: (g["id"] in added, g["expires_ms"]))
            for g in active[:max(0, len(active) - self.plan["policy"]["max_geofences"])]:
                ops.append({"op": "remove_operator", "id": g["id"]})
            reasons = [r for _p, _e, r, _b in batch]
            log.info("batching %d queued patches into one shadow run", len(batch))
            self.process_patch({"ops": ops}, batch[-1][1], f"{len(batch)} changes: " + "; ".join(reasons),
                               batch[-1][3], members=[(e, b) for _p, e, _r, b in batch])

    # ------------------------------------------------------------- distillation (bidirectional)
    def maybe_distill(self, ep):
        if gsir.operators(self.plan, "distilled_filter") or time.time() - self.last_demotion < 300:
            return
        if self.shadow and gsir.operators(self.shadow["plan"], "distilled_filter"):
            return
        cities = [c for *_, c in self.episodes]
        city = max(set(cities), key=cities.count) if cities else config.CITY     # learn where most judgements are
        X = [f for f, _, c in self.episodes if c == city]
        y = [lab for _, lab, c in self.episodes if c == city]
        if self.n["episodes"] < 4 or sum(y) < config.DISTILL_MIN_EPISODES \
                or len(y) - sum(y) < config.DISTILL_MIN_EPISODES:
            return
        w, b = stats.fit_logistic(X, y)
        thr, f1 = stats.calibrate_threshold(X, y, w, b)
        if f1 < 0.8:
            return
        op = {"id": "dm_" + uuid.uuid4().hex[:6], "kind": "distilled_filter", "features": stats.FEATURE_NAMES,
              "model": {"type": "logistic", "weights": [round(x, 4) for x in w], "bias": round(b, 4)},
              "threshold": thr, "uncertain_band": [round(max(0.05, thr - 0.15), 3), thr],
              "trained_on": len(y), "train_f1": round(f1, 3), "provenance_id": ep, "city": city,
              "review_interval_s": 21600, "audit_sample_rate": 1.0}
        self.n["distilled"] += 1
        log.info("DISTILLER: %d episodes, F1=%.2f -> proposing %s", len(y), f1, op["id"])
        self.alert("DISTILLED_PROPOSED", "info", f"distilled agent judgement into {op['id']} "
                   f"(logistic, {len(y)} episodes, F1 {f1:.2f}) - runs inline in microseconds",
                   {"operator": op})
        self.submit({"ops": [{"op": "add_operator", "operator": op}]}, ep, f"distil judgement into {op['id']}", None)

    def check_distilled_agreement(self, summary, decision):
        ops = gsir.operators(self.plan, "distilled_filter")
        if not ops:
            return
        d = ops[0]
        pred = stats.score(d["model"], stats.features(summary)) >= d["threshold"]
        self.agreement.append(pred == (decision["label"] == "congestion_incident"))
        rate = sum(self.agreement) / len(self.agreement)
        if len(self.agreement) >= 8 and rate < 0.7:
            log.info("DEMOTE %s: agreement with agent %.0f%% < 70%%", d["id"], rate * 100)
            self.n["demoted"] += 1
            self.last_demotion = time.time()
            self.agreement.clear()
            self.alert("OPERATOR_DEMOTED", "medium",
                       f"{d['id']} demoted: agreement drift ({rate:.0%}) - reasoning path reopened")
            self.submit({"ops": [{"op": "remove_operator", "id": d["id"]}]}, "drift", f"demote {d['id']}", None)

    # ------------------------------------------------------------- governance + watching
    def on_control(self, cmd):
        t = cmd.get("type")
        if t == "approve" and cmd.get("version") in self.held:
            patch, ep, reason, bundle, _t = self.held.pop(cmd["version"])
            log.info("human approved held v%d", cmd["version"])
            now_ms = int(time.time() * 1000)
            for op in patch["ops"]:          # TTL starts when the human approves, not when it was proposed
                o = op.get("operator") or {}
                if o.get("kind") == "geofence":
                    o["expires_ms"] = now_ms + GEOFENCE_TTL_MS
            self.db.execute("UPDATE plan_versions SET disposition='approved' WHERE version=%s AND stage='held'",
                            (cmd["version"],))
            if self.shadow:
                self.queue.append((patch, ep, reason + " [human-approved]", bundle, True))
            else:
                self.process_patch(patch, ep, reason + " [human-approved]", bundle, approved=True)
        elif t in ("approve", "reject") and cmd.get("version") not in self.held:
            log.info("%s for v%s ignored - request no longer pending", t, cmd.get("version"))
            self.db.execute("UPDATE plan_versions SET disposition='expired' WHERE version=%s AND stage='held' "
                            "AND disposition='hold_for_approval'", (cmd.get("version"),))
        elif t == "reject" and cmd.get("version") in self.held:
            self.held.pop(cmd["version"])
            self.db.execute("UPDATE plan_versions SET disposition='human_rejected' WHERE version=%s AND stage='held'",
                            (cmd["version"],))
            log.info("human rejected held v%d", cmd["version"])
        elif t == "set_agent_delay":
            self.agent_delay = float(cmd.get("seconds", 0))
            log.info("agent latency set to %.1fs (demo of P1)", self.agent_delay)
            self.alert("CONTROL", "info", f"slow-plane reasoning latency set to {self.agent_delay:.0f}s")
        elif t == "set_fault_rate":
            self.fault_rate = float(cmd.get("rate", 0))
            self.alert("CONTROL", "info", f"agent fault-injection rate set to {self.fault_rate:.0%}")

    def on_watch(self, msg):
        if msg.topic() == config.T_PLAN:
            v = loads(msg.value())
            self.max_version = max(self.max_version, v["version"])
            if v["stage"] == "active" and v["plan"]["version"] >= self.plan["version"]:
                self.plan = v["plan"]
        else:
            m = loads(msg.value())
            if m.get("component") == "fastplane":
                self.fast_metrics[m["instance"]] = {**m["values"], "_labels": m.get("labels", {}), "_ts": m["ts"]}

    def emit_metrics(self):
        now = time.time()
        self.fast_metrics = {k: v for k, v in self.fast_metrics.items() if now - v["_ts"] < 30}
        self.metrics.emit({**self.n, "agent_delay_s": self.agent_delay, "fault_rate": self.fault_rate,
                           "queue": len(self.queue), "held": len(self.held), "plan_version": self.plan["version"],
                           "shadow_version": self.shadow["plan"]["version"] if self.shadow else 0,
                           **{"mas_" + k: v for k, v in self.mas.items()},
                           **{"coord_" + k: v for k, v in self.coordinator.n.items()},
                           "coord_queue": len(self.coordinator.bids)},
                          {"planner": (f"multi-agent ({self.planner.name}"
                                       + (" + LLM critic" if self.critic.use_llm else "") + ")")
                           if config.MAS_ENABLED else self.planner.name,
                           "held_versions": sorted(self.held)})
        for city, counts in self.city_counts.items():
            self.city_metrics.emit(dict(counts), {"city": city, "planner": self.planner.name})
        self.last_metric = now

    def run(self):
        self.bc.subscribe([config.T_PLAN, config.T_METRICS])
        # catch up on the current plan before reasoning about anything
        t0 = time.time()
        while time.time() - t0 < 8:
            m = self.bc.poll(0.5)
            if m is not None and not m.error() and m.topic() == config.T_PLAN:
                self.on_watch(m)
        self.bc.unsubscribe()
        self.bc.close()
        self.bc = make_consumer(f"tessera-slowplane-watch-{uuid.uuid4().hex[:6]}", offset_reset="latest")
        self.bc.subscribe([config.T_PLAN, config.T_METRICS])
        try:
            mv = self.db.query("SELECT max(version) AS v FROM plan_versions")
            if mv and mv[0]["v"]:
                self.max_version = max(self.max_version, int(mv[0]["v"]))
        except Exception as e:  # noqa: BLE001
            log.warning("could not read plan versions: %s", e)
        # pending approvals live in this process; any left over from a previous run can no longer be applied
        try:
            self.db.execute("UPDATE plan_versions SET disposition='expired' WHERE stage='held' "
                            "AND disposition='hold_for_approval'")
        except Exception as e:  # noqa: BLE001
            log.warning("could not expire stale approvals: %s", e)
        self.c.subscribe([config.T_SURPRISE, config.T_GOV])
        if config.MAS_ENABLED:
            log.info("multi-agent mode: %s + critic%s + coordinator", ", ".join(a.name for a in self.city_agents.values()),
                     " (with LLM review)" if self.critic.use_llm else "")
        log.info("slow plane ready (planner=%s, plan v%d, simulated reasoning latency %.1fs)",
                 self.planner.name, self.plan["version"], self.agent_delay)
        while not self.stop.stop:
            for m in self.bc.consume(num_messages=200, timeout=0.1):
                if not m.error():
                    self.on_watch(m)
            m = self.c.poll(0.5)
            if m is not None:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log.warning("consumer error: %s", m.error())
                else:
                    try:
                        v = loads(m.value())
                        if m.topic() == config.T_SURPRISE:
                            if time.time() - v["ts"] < 120 or v["ts"] > time.time():
                                agent = self.city_agents.get(config.city_of(v["lat"], v["lon"]))
                                if config.MAS_ENABLED and agent is not None:
                                    self.coordinator.bid(agent, v, "ep_" + uuid.uuid4().hex[:10])
                                else:
                                    self.on_bundle(v)
                        else:
                            self.on_control(v)
                    except Exception as e:  # noqa: BLE001  (one bad episode must not kill the agent)
                        log.exception("episode failed: %s", e)
                    self.c.commit(message=m, asynchronous=False)
            if config.MAS_ENABLED:
                aw = self.coordinator.award()
                if aw:
                    try:
                        self.on_bundle(aw[1], ep=aw[0], agent=aw[2])
                    except Exception as e:  # noqa: BLE001
                        log.exception("multi-agent episode failed: %s", e)
            self.check_shadow()
            self.expire_holds()
            self.p.poll(0)
            if time.time() - self.last_metric > 5:
                self.emit_metrics()
        self.p.flush(10)
        self.c.close()
        self.bc.close()


def main():
    wait_for_kafka()
    wait_for_topics([config.T_SURPRISE, config.T_PLAN, config.T_GOV])
    SlowPlane().run()


if __name__ == "__main__":
    main()
