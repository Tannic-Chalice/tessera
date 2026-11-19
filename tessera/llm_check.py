"""Ask the local LLM (Ollama) to judge a strong and a weak example, to see how it answers the TESSERA prompts.

    docker compose exec slowplane python -m tessera.llm_check
"""
import time

import h3

from . import agents, config, gsir

EXAMPLES = {
    "strong (accident)": dict(vehicles=476, slow=0.81, speed=5.5, z=8.2, n_tiles=3),
    "weak (busy road)": dict(vehicles=7, slow=0.48, speed=16.0, z=3.6, n_tiles=1),
}


def bundle(vehicles, slow, speed, z, n_tiles, lat=12.9592, lon=77.5662):
    c = h3.latlng_to_cell(lat, lon, 8)
    return {"tiles": list(h3.grid_disk(c, 1))[:n_tiles], "lat": lat, "lon": lon, "max_z": z, "head": c,
            "summary": {"vehicles": vehicles, "slow_frac": slow, "mean_speed": speed, "n_obs": vehicles * 4}}


class Fixed:
    name = "fixed"

    def reason(self, b, facts, conflict):
        return {"label": "congestion_incident", "risk": 0.9, "rationale": ""}


def main():
    if not config.OLLAMA_URL:
        print("OLLAMA_URL is not set - the agents run on rules only.")
        return
    print(f"model {config.OLLAMA_MODEL} at {config.OLLAMA_URL}\n")
    critic, agent, now = agents.Critic(use_llm=True), agents.CityAgent("bengaluru", Fixed()), int(time.time() * 1000)
    for name, ex in EXAMPLES.items():
        b = bundle(**ex)
        t0 = time.time()
        plan_out = agents.ollama_json(agents.planner_prompt("Bengaluru", b["summary"], b["max_z"], len(b["tiles"]), []))
        t1 = time.time()
        p = agent.propose(b, gsir.default_plan(), now)
        crit_raw = agents.ollama_json(agents.critic_prompt(p, b, {"risk": 0.9}, 0))
        t2 = time.time()
        r = critic.review(p, b, {"label": "congestion_incident", "risk": 0.9}, [], gsir.default_plan(), now)
        print(f"== {name}")
        print(f"   planner LLM ({t1 - t0:.1f}s): {plan_out}")
        print(f"   critic  LLM ({t2 - t1:.1f}s): {crit_raw}")
        print(f"   critic final verdict: {r['verdict'].upper()} - {'; '.join(r['reasons'])}\n")


if __name__ == "__main__":
    main()
