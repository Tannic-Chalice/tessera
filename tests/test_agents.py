"""Multi-agent layer: coordinator bidding, critic rules, propose -> critique -> revise -> submit."""
import time

import h3

from tessera import agents, gsir

SILK = (12.918153, 77.62326)


def bundle(lat=SILK[0], lon=SILK[1], n_tiles=3, vehicles=40, slow=0.85, speed=6.0, z=6.0):
    c = h3.latlng_to_cell(lat, lon, 8)
    tiles = list(h3.grid_disk(c, 2))[:n_tiles]
    return {"event_id": "ev_t", "ts": time.time(), "head": c, "tiles": tiles, "lat": lat, "lon": lon, "max_z": z,
            "summary": {"slow_frac": slow, "mean_speed": speed, "vehicles": vehicles, "n_obs": 300}}


class FixedPlanner:
    name = "fixed"

    def reason(self, b, facts, conflict):
        return {"label": "congestion_incident", "risk": 0.95, "rationale": "test"}


def plan_with(*fences):
    p = gsir.default_plan()
    for g in fences:
        p = gsir.apply_patch(p, {"ops": [{"op": "add_operator", "operator": g}]})
    return p


def fence(gid, lat, lon, r):
    return gsir.geofence_operator(gid, lat, lon, r, time.time() * 1000 + 600000, "test")


def test_coordinator_awards_highest_urgency_and_logs_messages():
    out = []
    bus = agents.Bus(out.append)
    co = agents.Coordinator(bus, interval_s=0)
    blr, dl = agents.CityAgent("bengaluru", FixedPlanner()), agents.CityAgent("delhi", FixedPlanner())
    co.bid(blr, bundle(slow=0.55, z=4.5), "c1")
    co.bid(dl, bundle(lat=28.61, lon=77.21, slow=0.95, z=9.0, n_tiles=7), "c2")
    conv, b, agent = co.award()
    assert conv == "c2" and agent.city == "delhi"
    assert [m["performative"] for m in out] == ["BID", "BID", "DEFER", "AWARD"]
    assert co.award()[0] == "c1"


def test_critic_rejects_weak_evidence():
    c = agents.Critic()
    b = bundle(vehicles=2)
    p = agents.CityAgent("bengaluru", FixedPlanner()).propose(b, gsir.default_plan(), int(time.time() * 1000))
    assert c.review(p, b, {"label": "congestion_incident", "risk": 0.9}, [], gsir.default_plan(),
                    int(time.time() * 1000))["verdict"] == "reject"


def test_overlap_is_revised_into_a_refresh_and_submitted():
    out = []
    bus = agents.Bus(out.append)
    plan = plan_with(fence("gf_old", SILK[0] + 0.004, SILK[1], 700))      # ~450 m away, but not covering the hotspot
    b = bundle(lat=SILK[0] - 0.0035, lon=SILK[1])
    agent = agents.CityAgent("bengaluru", FixedPlanner())
    p = agents.negotiate(bus, agent, agents.Critic(), b, {"label": "congestion_incident", "risk": 0.95}, [], plan,
                         int(time.time() * 1000), "c3")
    perfs = [m["performative"] for m in out]
    assert perfs == ["PROPOSE", "CRITIQUE", "REVISE", "CRITIQUE", "SUBMIT"], perfs
    assert p["action"] == "refresh" and p["gid"] == "gf_old"


def test_oversized_zone_without_strong_evidence_is_shrunk():
    out = []
    b = bundle(n_tiles=19, z=5.0)                                           # wide hotspot, moderate significance
    agent = agents.CityAgent("bengaluru", FixedPlanner())
    first = agent.propose(b, gsir.default_plan(), int(time.time() * 1000))
    assert first["radius"] > agents.Critic.HOLD_RADIUS_M
    p = agents.negotiate(agents.Bus(out.append), agent, agents.Critic(), b, {"label": "congestion_incident", "risk": 0.9},
                         [], gsir.default_plan(), int(time.time() * 1000), "c4")
    assert p is not None and p["radius"] < agents.Critic.HOLD_RADIUS_M
    assert "REVISE" in [m["performative"] for m in out]


def test_memory_that_says_clear_blocks_a_moderate_claim():
    b = bundle()
    agent = agents.CityAgent("bengaluru", FixedPlanner())
    out = []
    facts = [{"fact_class": "clear", "confidence": 0.9}]
    p = agents.negotiate(agents.Bus(out.append), agent, agents.Critic(), b, {"label": "congestion_incident", "risk": 0.7},
                         facts, gsir.default_plan(), int(time.time() * 1000), "c5")
    assert p is None and out[-1]["performative"] == "WITHDRAW"


def test_strong_but_huge_hotspot_is_trimmed_to_its_core_instead_of_waiting_for_a_human():
    b = bundle(n_tiles=19, z=12.0)
    agent = agents.CityAgent("bengaluru", FixedPlanner())
    assert agent.propose(b, gsir.default_plan(), int(time.time() * 1000))["radius"] > agents.Critic.HOLD_RADIUS_M
    p = agents.negotiate(agents.Bus(), agent, agents.Critic(), b, {"label": "congestion_incident", "risk": 0.95},
                         [], gsir.default_plan(), int(time.time() * 1000), "c6")
    assert p is not None and p["radius"] < agents.Critic.HOLD_RADIUS_M


def test_llm_veto_counts_only_when_the_check_it_cites_really_fails(monkeypatch):
    agent, critic, now = agents.CityAgent("bengaluru", FixedPlanner()), agents.Critic(use_llm=True), int(time.time() * 1000)
    decision = {"label": "congestion_incident", "risk": 0.9}

    def review(b, answer):
        monkeypatch.setattr(agents, "ollama_json", lambda prompt: answer)
        return critic.review(agent.propose(b, gsir.default_plan(), now), b, decision, [], gsir.default_plan(), now)

    strong = bundle(n_tiles=1, vehicles=48, slow=0.9, z=8.1)
    r = review(strong, {"verdict": "reject", "check": 0, "reason": "insufficient evidence"})     # vague veto
    assert r["verdict"] == "accept" and "overruled" in r["reasons"][-1]
    r = review(strong, {"verdict": "reject", "check": 3, "reason": "z too low"})                  # false claim
    assert r["verdict"] == "accept" and "overruled" in r["reasons"][-1]
    weak = bundle(n_tiles=1, vehicles=6, slow=0.5, z=4.2)
    r = review(weak, {"verdict": "reject", "check": 1, "reason": "only 6 vehicles"})               # true claim
    assert r["verdict"] == "reject"


def test_prompts_carry_definitions_and_numbers():
    b = bundle(n_tiles=1, vehicles=48, slow=0.9, z=8.1)
    p = agents.CityAgent("bengaluru", FixedPlanner()).propose(b, gsir.default_plan(), int(time.time() * 1000))
    cp = agents.critic_prompt(p, b, {"risk": 0.9}, 2)
    assert "48 vehicles" in cp and "90%" in cp and "z-score 8.1" in cp and "REJECT only if" in cp
    pp = agents.planner_prompt("Bengaluru", b["summary"], 8.1, 1, [])
    assert "congestion_incident: an abnormal blockage" in pp and "48 vehicles" in pp


def test_llm_cannot_wave_through_a_check_that_fails(monkeypatch):
    monkeypatch.setattr(agents, "ollama_json", lambda prompt: {"verdict": "accept", "check": 0, "reason": "fine"})
    agent, critic, now = agents.CityAgent("bengaluru", FixedPlanner()), agents.Critic(use_llm=True), int(time.time() * 1000)
    weak = bundle(n_tiles=1, vehicles=7, slow=0.48, speed=16.0, z=3.6)
    r = critic.review(agent.propose(weak, gsir.default_plan(), now), weak, {"label": "congestion_incident", "risk": 0.7},
                      [], gsir.default_plan(), now)
    assert r["verdict"] == "reject" and "LLM missed" in r["reasons"][-1]
