"""Robustness study, input noise: how stable is the simulator's call when the
race state is measured with realistic error?

    python -m pitwall.eval.sensitivity --out ../benchmarks/robustness/sensitivity.json

For every scenario the request the agent sends to the simulator is perturbed
N times (seeded), within the error of the inputs:

- gaps to the leader: +-0.5 s for every car (OpenF1 intervals update every few
  seconds and are carried forward); positions are re-derived from the gaps
- tyre age: +-1 lap for every car (stint boundaries can be off by a lap)
- our measured wear: +-20 ms/lap (a 5-lap OLS slope)
- pit loss: +-2 s green, neutral loss scaled with it
- track wetness: +-0.1 where the track is wet (an analyst estimate)

Discrete facts (compounds used, penalties, track status, pit entry) are not
perturbed. The simulator seed is fixed, so a change of call comes from the
inputs, not from Monte Carlo noise. Reported per scenario: stability (share
of perturbations with the unperturbed call), ground-truth agreement under
noise, and the margin between the best and second-best legal plans.
"""
from __future__ import annotations

import argparse
import asyncio
import copy
import json
import random
import time
import zlib
from collections import Counter
from pathlib import Path

from pitwall.agents.graph import sim_request
from pitwall.eval.scenarios import load_scenarios, materialise
from pitwall.mcp_client.client import HTTPMCPClient
from pitwall.olap import Lakehouse

GAP_MS, AGE_LAPS, DEG_MS, PIT_S, WET = 500, 1, 20.0, 2.0, 0.1


def perturb(state: dict, rng: random.Random) -> dict:
    s = copy.deepcopy(state)
    cars = [s["car"], *s.get("field", [])]
    for c in cars:
        c["gap_to_leader_ms"] = c.get("gap_to_leader_ms", 0) + rng.uniform(-GAP_MS, GAP_MS)
        c["tyre_age_laps"] = max(0, c["tyre_age_laps"] + rng.randint(-AGE_LAPS, AGE_LAPS))
    lead = min(c["gap_to_leader_ms"] for c in cars)
    for i, c in enumerate(sorted(cars, key=lambda c: c["gap_to_leader_ms"]), start=1):
        c["gap_to_leader_ms"] = round(c["gap_to_leader_ms"] - lead)
        c["position"] = i
    if s["car"].get("deg_slope_ms_per_lap") is not None:
        s["car"]["deg_slope_ms_per_lap"] = max(0.0, s["car"]["deg_slope_ms_per_lap"] + rng.uniform(-DEG_MS, DEG_MS))
    green = s.get("pit_loss_green_s", 21.8)
    ratio = s.get("pit_loss_neutral_s", 13.2) / green
    s["pit_loss_green_s"] = green + rng.uniform(-PIT_S, PIT_S)
    s["pit_loss_neutral_s"] = s["pit_loss_green_s"] * ratio
    if s.get("track_wetness"):
        s["track_wetness"] = min(1.0, max(0.0, s["track_wetness"] + rng.uniform(-WET, WET)))
    return s


async def study(scn: dict, mcp: HTTPMCPClient, n: int) -> dict:
    base = await mcp.call_tool("simulate_pit_strategies", sim_request(scn["state"]))
    legal = sorted((o for o in base["outcomes"] if o["legal"]), key=lambda o: o["expected_delta_ms"])
    margin = (legal[1]["expected_delta_ms"] / 1000) if len(legal) > 1 else None
    rng = random.Random(zlib.crc32(scn["id"].encode()))
    calls = Counter()
    for _ in range(n):
        r = await mcp.call_tool("simulate_pit_strategies", sim_request(perturb(scn["state"], rng)))
        calls[r["recommended"]] += 1
    gt = scn["ground_truth"]["acceptable"]
    return {"id": scn["id"], "kind": scn["kind"], "category": scn["category"],
            "baseline_call": base["recommended"], "baseline_agrees": base["recommended"] in gt,
            "stability": round(calls[base["recommended"]] / n, 3),
            "agreement_under_noise": round(sum(v for k, v in calls.items() if k in gt) / n, 3),
            "margin_to_second_best_s": round(margin, 2) if margin is not None else None,
            "second_best": legal[1]["strategy"] if len(legal) > 1 else None,
            "calls": dict(calls.most_common())}


async def main_async(out: Path, n: int, only: list[str] | None) -> None:
    lh = Lakehouse()
    mcp = HTTPMCPClient()
    await mcp.initialize()
    t0 = time.perf_counter()
    rows = []
    for s in load_scenarios():
        if only and s["id"] not in only:
            continue
        r = await study(materialise(s, lh), mcp, n)
        rows.append(r)
        print(f"{r['id']} stability {r['stability']:.2f} agree-under-noise {r['agreement_under_noise']:.2f} "
              f"margin {r['margin_to_second_best_s']}s {r['calls']}", flush=True)
    await mcp.aclose()
    knife = [r["id"] for r in rows if r["stability"] < 0.8]
    report = {
        "perturbations_per_scenario": n,
        "noise": {"gap_ms": GAP_MS, "tyre_age_laps": AGE_LAPS, "our_deg_ms_per_lap": DEG_MS, "pit_loss_s": PIT_S,
                  "wetness": WET},
        "scenarios": len(rows),
        "mean_stability": round(sum(r["stability"] for r in rows) / len(rows), 3),
        "mean_agreement_under_noise": round(sum(r["agreement_under_noise"] for r in rows) / len(rows), 3),
        "baseline_agreement": sum(r["baseline_agrees"] for r in rows),
        "knife_edge": knife,  # stability below 0.8
        "elapsed_s": round(time.perf_counter() - t0, 1),
        "rows": rows,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "rows"}, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("-n", type=int, default=200, help="perturbations per scenario")
    ap.add_argument("--only", nargs="*")
    args = ap.parse_args()
    asyncio.run(main_async(args.out, args.n, args.only))


if __name__ == "__main__":
    main()
