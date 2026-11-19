"""Multi-agent incident response (runs inside the slow plane, never on the data path).

Agents and their roles
  CityAgent (one per active city)  - bids for reasoning time, judges hotspots in its city, PROPOSES actions
                                     (add / refresh an incident zone) and REVISES them when criticised.
  Critic                           - reviews every proposal BEFORE the rule-based Geo-Verifier: weak evidence,
                                     overlap with an existing zone, a zone larger than the evidence supports,
                                     memory that contradicts the claim. Optional local LLM (Ollama) second opinion.
  Coordinator                      - shares the limited reasoning budget between cities (contract-net style:
                                     city agents BID with an urgency score, the coordinator AWARDS), drops stale bids.

Agents never call each other's internals - they exchange typed messages (performatives) on a bus that is
mirrored to the Kafka topic `agent.messages`, so every conversation is visible on the dashboard and auditable:
  BID, AWARD, DEFER, PROPOSE, CRITIQUE, REVISE, ACCEPT, REJECT, WITHDRAW, SUBMIT, VERIFIED, BLOCKED, HELD,
  SHADOW, PROMOTED, ROLLED_BACK.
A conversation id (conv) is the episode id, which also becomes the provenance of any geofence it produces."""
import json
import math
import time
import urllib.request
import uuid
from collections import deque

from . import config, geo, gsir


def tile_risk(f):
    x = 9.0 * (f.get("slow_frac", 0) - 0.4) - 0.06 * (f.get("mean_speed", 30) - 15) \
        + 0.5 * math.log1p(f.get("n_obs", 0) / 40.0)
    return 1.0 / (1.0 + math.exp(-x))


def city_name(key):
    return config.CITY_PROFILES.get(key, (0, 0, 0, key))[3]


# ------------------------------------------------------------------ message bus
class Bus:
    """Every inter-agent message goes through here; `emit` publishes it (Kafka in the service, a list in tests)."""

    def __init__(self, emit=None, log=None):
        self.emit = emit or (lambda m: None)
        self.log = log
        self.recent = deque(maxlen=400)

    def send(self, conv, frm, to, perf, text, content=None, bundle=None, city=None):
        msg = {"msg_id": "m_" + uuid.uuid4().hex[:10], "conv": conv, "ts": time.time(), "from": frm, "to": to,
               "performative": perf, "text": text, "content": content or {},
               "city": city or (config.city_of(bundle["lat"], bundle["lon"]) if bundle else None),
               "lat": bundle.get("lat") if bundle else None, "lon": bundle.get("lon") if bundle else None,
               "report": bundle.get("reported") if bundle else None}
        self.recent.append(msg)
        try:
            self.emit(msg)
        except Exception:  # noqa: BLE001  (telemetry must never break reasoning)
            pass
        if self.log:
            self.log.info("[%s] %s -> %s %s: %s", conv, frm, to, perf, text)
        return msg


# ------------------------------------------------------------------ optional local LLM
def ollama_json(prompt, timeout=60):
    """Ask the local Ollama server for a JSON answer; raises on any problem (callers fall back to rules)."""
    req = urllib.request.Request(config.OLLAMA_URL.rstrip("/") + "/api/generate", data=json.dumps(
        {"model": config.OLLAMA_MODEL, "prompt": prompt, "format": "json", "stream": False,
         "options": {"temperature": 0.1}}).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(json.loads(r.read())["response"])


# ------------------------------------------------------------------ LLM prompts (small local models need
# explicit definitions and numbered checks; vague prompts like "be cautious" make them reject everything)
CRITIC_CHECKS = {
    1: "fewer than 10 vehicles were measured",
    2: "traffic is not really slow: under 50% of vehicles below 10 km/h, or mean speed above 15 km/h",
    3: "not unusual for this place and time: G* z-score below 4",
    4: "the zone is far bigger than the hotspot: radius more than 3 times the hotspot extent",
}


def critic_check_failed(check, p, b):
    s = b["summary"]
    return {1: s.get("vehicles", 0) < 10,
            2: s.get("slow_frac", 0) < 0.5 or s.get("mean_speed", 0) > 15,
            3: b["max_z"] < 4,
            4: p.get("radius", 0) > 3 * max(150.0, p.get("extent_m", 0))}.get(check, False)


def critic_prompt(p, b, decision, n_prior):
    s = b["summary"]
    checks = "\n".join(f"{k}. {v}" for k, v in CRITIC_CHECKS.items())
    return (
        "You review a proposal from a city traffic agent. Your job is to catch real mistakes, not to block by "
        "default. A wrong rejection leaves drivers stuck in a jam; a wrong acceptance only adds a 6-minute "
        "detour zone.\n"
        f"REJECT only if one of these checks fails:\n{checks}\nIf none fails, ACCEPT.\n\n"
        f"Proposal: {'refresh the existing' if p.get('action') == 'refresh' else 'add a'} no-go zone of radius "
        f"{p.get('radius', 0):.0f} m in {city_name(config.city_of(b['lat'], b['lon']))} for 6 minutes.\n"
        f"Evidence: {s.get('vehicles', 0)} vehicles measured; {s.get('slow_frac', 0):.0%} of them below 10 km/h; "
        f"mean speed {s.get('mean_speed', 0):.1f} km/h; G* z-score {b['max_z']:.1f}; hotspot spans "
        f"{len(b['tiles'])} tiles, about {p.get('extent_m', 0):.0f} m; {n_prior} incident(s) confirmed here "
        f"recently; agent risk {decision['risk']:.2f}.\n\n"
        'Reply ONLY with JSON {"check": <number of the failed check, or 0 if none fails>, '
        '"verdict": "accept" or "reject", "reason": "<20 words, quote the numbers>"}')


def planner_prompt(city, s, max_z, n_tiles, facts):
    mem = "; ".join(f"{f['fact_class']} (confidence {f['confidence']:.1f})" for f in facts[:5]) or "none"
    return (
        f"You are a traffic analyst for {city}. A statistical test flagged an area where traffic is slower than "
        "normal for this city at this time. Decide which of two things it is:\n"
        "- congestion_incident: an abnormal blockage (accident, breakdown, road work, procession). Typical signs: "
        "most vehicles crawling (over 70% below 10 km/h), mean speed under 10 km/h, G* z-score above 6, or "
        "incidents confirmed here recently.\n"
        "- dense_traffic: ordinary heavy traffic that is still moving. Typical signs: under 60% of vehicles below "
        "10 km/h, mean speed above 12 km/h, z-score under 5.\n\n"
        f"Evidence: {s.get('vehicles', 0)} vehicles; {s.get('slow_frac', 0):.0%} below 10 km/h; mean speed "
        f"{s.get('mean_speed', 0):.1f} km/h; G* z-score {max_z:.1f}; {n_tiles} adjacent tiles; memory: {mem}.\n\n"
        'Reply ONLY with JSON {"risk": <probability 0..1 that this is a congestion_incident>, '
        '"label": "congestion_incident" if risk >= 0.6 else "dense_traffic", '
        '"rationale": "<20 words, quote the numbers>"}')


# ------------------------------------------------------------------ city agent
class CityAgent:
    def __init__(self, city, planner):
        self.city, self.planner = city, planner
        self.name = f"{city}-agent"

    def urgency(self, b):
        """How badly this city needs the agent now (used for the bid): severity x significance x size."""
        s = b["summary"]
        u = tile_risk(s) * min(1.0, max(0.0, b["max_z"]) / 8.0) + 0.1 * min(1.0, len(b["tiles"]) / 6.0)
        if b.get("reported"):
            u += 0.5                          # a driver on the road reported it: someone is about to drive into it
        return round(u, 3)

    def reason(self, b, facts, conflict):
        return self.planner.reason(b, facts, conflict)

    def propose(self, b, plan, now_ms):
        """Draft an action: refresh a zone that already covers the hotspot, else add a new one sized to the evidence."""
        covering = [g for g in gsir.active_geofences(plan, now_ms)
                    if geo.haversine_m(g["shape"]["lat"], g["shape"]["lon"], b["lat"], b["lon"]) < g["shape"]["radius"]]
        extent = max([geo.haversine_m(b["lat"], b["lon"], *geo.center(t)) for t in b["tiles"]] or [0.0])
        if covering:
            g = covering[0]
            return {"action": "refresh", "gid": g["id"], "lat": g["shape"]["lat"], "lon": g["shape"]["lon"],
                    "radius": g["shape"]["radius"], "extent_m": round(extent)}
        radius = min(max(500.0, extent + 450.0), 0.85 * plan["policy"].get("max_geofence_radius_m", 3000))
        return {"action": "add", "gid": "gf_" + uuid.uuid4().hex[:6], "lat": b["lat"], "lon": b["lon"],
                "radius": round(radius), "extent_m": round(extent)}

    def revise(self, proposal, critique):
        s = critique.get("suggest") or {}
        if "refresh" in s:
            return {**proposal, "action": "refresh", "gid": s["refresh"]["gid"], "lat": s["refresh"]["lat"],
                    "lon": s["refresh"]["lon"], "radius": s["refresh"]["radius"]}
        if "radius" in s:
            return {**proposal, "radius": round(s["radius"])}
        return None                                                     # nothing to change -> withdraw


# ------------------------------------------------------------------ critic
class Critic:
    name = "critic"
    HOLD_RADIUS_M = 1300          # bigger zones go to a human; only worth it with strong evidence

    def __init__(self, use_llm=False):
        self.use_llm = use_llm

    def review(self, p, b, decision, facts, plan, now_ms):
        """Returns {"verdict": accept|revise|reject, "reasons": [...], "suggest": {...}}. Rules are hard checks;
        the optional LLM can only make a verdict stricter, never approve what the rules reject."""
        s, reasons, suggest = b["summary"], [], {}
        verdict = "accept"
        if s.get("vehicles", 0) < 4 or s.get("slow_frac", 0) < 0.45:
            return {"verdict": "reject", "reasons": [f"weak evidence: {s.get('vehicles', 0)} vehicles, "
                                                     f"{s.get('slow_frac', 0):.0%} slow"], "suggest": {}}
        recent_clear = [f for f in facts if f["fact_class"] == "clear" and f["confidence"] >= 0.7]
        if recent_clear and decision["risk"] < 0.8:
            return {"verdict": "reject", "reasons": ["memory: this area was confirmed clear recently and the risk "
                                                     f"({decision['risk']:.2f}) is not high enough to overturn it"],
                    "suggest": {}}
        if p["action"] == "add":
            for g in gsir.active_geofences(plan, now_ms):
                d = geo.haversine_m(g["shape"]["lat"], g["shape"]["lon"], p["lat"], p["lon"])
                if d < g["shape"]["radius"] + 0.5 * p["radius"]:
                    verdict = "revise"
                    reasons.append(f"overlaps existing zone {g['id']} ({d:.0f} m away) - refresh it instead of "
                                   "creating a duplicate")
                    suggest["refresh"] = {"gid": g["id"], "lat": g["shape"]["lat"], "lon": g["shape"]["lon"],
                                          "radius": g["shape"]["radius"]}
                    break
            if "refresh" not in suggest and p["radius"] > self.HOLD_RADIUS_M:
                strong = b["max_z"] >= 8 and len(b["tiles"]) >= 6
                target = max(500.0, min(self.HOLD_RADIUS_M - 100, p["extent_m"] + 300.0))
                verdict = "revise"
                if strong:      # real, but a zone this big would sit waiting for a human: act on the core now
                    reasons.append(f"{p['radius']:.0f} m would wait for human approval - protect the core "
                                   f"({target:.0f} m) now; the rest is ordinary heavy traffic")
                else:
                    reasons.append(f"{p['radius']:.0f} m is more than the evidence supports ({len(b['tiles'])} "
                                   f"tiles, z={b['max_z']:.1f}) - shrink to {target:.0f} m")
                suggest["radius"] = target
        prior = [f for f in facts if f["fact_class"] == "congestion_incident"]
        if prior and verdict == "accept":
            reasons.append(f"consistent with {len(prior)} earlier incident fact(s) at this place")
        if self.use_llm and verdict == "accept":
            try:
                out = ollama_json(critic_prompt(p, b, decision, len(prior)))
                why = str(out.get("reason", ""))[:160]
                try:
                    check = int(out.get("check", 0) or 0)
                except (TypeError, ValueError):
                    check = 0
                failed = critic_check_failed(check, p, b)      # does the cited check really fail?
                truly = [k for k in (1, 2, 3) if critic_check_failed(k, p, b)]
                if out.get("verdict") == "reject" and failed:
                    verdict = "reject"
                    reasons.append(f"LLM review: check {check} failed - {why}")
                elif truly:                                    # the numbers fail a check the LLM missed
                    verdict = "reject"
                    reasons.append("check " + ", ".join(str(k) for k in truly) + " failed ("
                                   + "; ".join(CRITIC_CHECKS[k] for k in truly) + ") - LLM missed it: " + why)
                elif out.get("verdict") == "reject":
                    reasons.append(f"LLM objected ({why}) - overruled: the check it cites "
                                   f"({check or 'none'}) passes on the numbers")
                else:
                    reasons.append("LLM review: " + why)
            except Exception as e:  # noqa: BLE001
                reasons.append(f"LLM review unavailable ({type(e).__name__}) - rule checks only")
        if verdict == "accept" and not reasons:
            reasons.append(f"evidence supports it: {s['vehicles']} vehicles, {s['slow_frac']:.0%} slow, "
                           f"z={b['max_z']:.1f}")
        return {"verdict": verdict, "reasons": reasons, "suggest": suggest}


# ------------------------------------------------------------------ coordinator
class Coordinator:
    """Contract-net style allocation of the reasoning budget: one award per `interval_s`, highest urgency first,
    ties go to the city that was served least recently; bids older than `max_wait_s` are dropped."""
    name = "coordinator"

    def __init__(self, bus, interval_s=None, max_wait_s=90.0):
        self.bus = bus
        self.interval = config.MAS_AWARD_INTERVAL_S if interval_s is None else interval_s
        self.max_wait = max_wait_s
        self.bids = []                      # (urgency, ts, conv, bundle, agent)
        self.last_award = 0.0
        self.last_served = {}
        self.n = {"bids": 0, "awards": 0, "deferred": 0, "expired": 0}

    def bid(self, agent, b, conv):
        u = agent.urgency(b)
        self.bids.append((u, time.time(), conv, b, agent))
        self.n["bids"] += 1
        self.bus.send(conv, agent.name, self.name, "BID",
                      f"hotspot: {len(b['tiles'])} tiles, {b['summary']['slow_frac']:.0%} slow, z={b['max_z']:.1f} "
                      + ("(confirms a driver report) " if b.get("reported") else "") + f"- urgency {u:.2f}", {"urgency": u, "queue": len(self.bids)}, b, agent.city)
        if len(self.bids) > 1:
            self.n["deferred"] += 1
            self.bus.send(conv, self.name, agent.name, "DEFER",
                          f"{len(self.bids) - 1} other bid(s) waiting - you are in the queue", {"queue": len(self.bids)},
                          b, agent.city)

    def award(self, now=None):
        now = now or time.time()
        for bid in [x for x in self.bids if now - x[1] > self.max_wait]:
            self.bids.remove(bid)
            self.n["expired"] += 1
            self.bus.send(bid[2], self.name, bid[4].name, "REJECT", f"bid expired after {self.max_wait:.0f} s - "
                          "the next window will show if the hotspot persists", {}, bid[3], bid[4].city)
        if not self.bids or now - self.last_award < self.interval:
            return None
        best = max(self.bids, key=lambda x: (round(x[0], 2), -self.last_served.get(x[4].city, 0.0)))
        self.bids.remove(best)
        u, ts, conv, b, agent = best
        self.last_award = now
        self.last_served[agent.city] = now
        self.n["awards"] += 1
        self.bus.send(conv, self.name, agent.name, "AWARD",
                      f"reasoning slot granted (urgency {u:.2f}, waited {now - ts:.0f} s, {len(self.bids)} still queued)",
                      {"urgency": u, "waited_s": round(now - ts, 1)}, b, agent.city)
        return conv, b, agent


# ------------------------------------------------------------------ conversation (propose / critique / revise)
def negotiate(bus, agent, critic, b, decision, facts, plan, now_ms, conv):
    """One proposal, at most one revision. Returns the accepted proposal or None."""
    p = agent.propose(b, plan, now_ms)
    what = (f"refresh zone {p['gid']}" if p["action"] == "refresh"
            else f"add incident zone {p['gid']} r={p['radius']} m")
    bus.send(conv, agent.name, critic.name, "PROPOSE", f"{what} - {decision['label']} risk {decision['risk']:.2f}",
             {"proposal": p, "decision": decision}, b, agent.city)
    for rnd in range(2):
        c = critic.review(p, b, decision, facts, plan, now_ms)
        bus.send(conv, critic.name, agent.name, "CRITIQUE", f"{c['verdict'].upper()}: " + "; ".join(c["reasons"]),
                 {"verdict": c["verdict"], "reasons": c["reasons"], "suggest": c["suggest"], "round": rnd + 1},
                 b, agent.city)
        if c["verdict"] == "accept":
            what = f"refresh {p['gid']}" if p["action"] == "refresh" else f"add {p['gid']} r={p['radius']} m"
            bus.send(conv, agent.name, "verifier", "SUBMIT", "submitting: " + what, {"proposal": p}, b, agent.city)
            return p
        if c["verdict"] == "reject" or rnd == 1:
            bus.send(conv, agent.name, critic.name, "WITHDRAW", "proposal withdrawn", {}, b, agent.city)
            return None
        p2 = agent.revise(p, c)
        if p2 is None:
            bus.send(conv, agent.name, critic.name, "WITHDRAW", "cannot address the critique", {}, b, agent.city)
            return None
        p = p2
        what = (f"refresh zone {p['gid']}" if p["action"] == "refresh" else f"zone {p['gid']} r={p['radius']} m")
        bus.send(conv, agent.name, critic.name, "REVISE", f"revised: {what}", {"proposal": p}, b, agent.city)
    return None
