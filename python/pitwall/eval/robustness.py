"""Robustness study: merge the three axes into benchmarks/robustness.json.

    python -m pitwall.eval.robustness

Inputs (written by scripts/robustness.sh):
- faults: benchmarks/agent_eval.json (fault seed 0, the main run) and
  benchmarks/robustness/faults_seed{1,2}.json, all at the production setting
  (decision calls greedy, 10% injected primary faults)
- sampling: benchmarks/robustness/temp_{0.0,0.3,0.7}_seed{1,2,3}.json, faults
  off, temperature on the decision calls only; the temperature-0 repeats are
  the determinism check
- inputs: benchmarks/robustness/sensitivity.json
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from pitwall import config

BENCH = config.ROOT / "benchmarks"
ROB = BENCH / "robustness"
TEMPS = (0.0, 0.3, 0.7)
SEEDS = (1, 2, 3)
ARMS = ("single_pass", "agent")


def load(p: Path) -> dict | None:
    return json.loads(p.read_text()) if p.exists() else None


def call(r: dict) -> tuple | None:
    f = r.get("final")
    return (f["recommended_action"], f.get("pit_lap"), f.get("target_compound")) if f else None


def by_arm(run: dict, arm: str) -> dict[str, dict]:
    return {r["id"]: r for r in run["runs"] if r["mode"] == arm}


def spread(values: list[float]) -> dict:
    return {"mean": round(sum(values) / len(values), 4), "min": min(values), "max": max(values)} if values else {}


def faults_axis() -> dict | None:
    runs = [load(BENCH / "agent_eval.json"), load(ROB / "faults_seed1.json"), load(ROB / "faults_seed2.json")]
    runs = [r for r in runs if r]
    if len(runs) < 2:
        return None
    out = {"fault_seeds": [r.get("fault_seed", 0) for r in runs]}
    for arm in ARMS:
        s = [r[arm] for r in runs]
        rows = [by_arm(r, arm) for r in runs]
        changed = []
        for sid in rows[0]:
            calls = {call(x[sid]) for x in rows}
            if len(calls) > 1:
                changed.append({"id": sid, "calls": sorted(str(c) for c in calls),
                                "failover_involved": any(x[sid]["llm"]["failovers"] for x in rows)})
        out[arm] = {
            "injected_faults": sum(x["injected_faults"] for x in s),
            "primary_failures": sum(x["primary_failures"] for x in s),
            "recovered_by_fallback": sum(x["recovered_by_fallback"] for x in s),
            "client_errors": sum(x["client_errors"] for x in s),
            "compliance": spread([x["compliance_rate"] for x in s]),
            "agreement": spread([x["agreement_rate"] for x in s]),
            "calls_changed_across_fault_seeds": changed,
        }
    return out


def sampling_axis() -> dict | None:
    out: dict = {}
    for t in TEMPS:
        runs = [load(ROB / f"temp_{t}_seed{s}.json") for s in SEEDS]
        runs = [r for r in runs if r]
        if not runs:
            continue
        tk = f"{t}"
        out[tk] = {"repeats": len(runs)}
        for arm in ARMS:
            s = [r[arm] for r in runs]
            rows = [by_arm(r, arm) for r in runs]
            flips = {}
            identical = 0
            for sid in rows[0]:
                calls = [call(x[sid]) for x in rows]
                modal, n = Counter(calls).most_common(1)[0]
                flips[sid] = round(1 - n / len(calls), 3)
                identical += n == len(calls)
            entry = {
                "compliance": spread([x["compliance_rate"] for x in s]),
                "traffic_clean": spread([x["traffic_clean_rate"] for x in s]),
                "agreement": spread([x["agreement_rate"] for x in s]),
                "latency_p50_ms": spread([x["latency_p50_ms"] for x in s]),
                "citation_validity": spread([x["citation_validity_rate"] for x in s if x.get("citation_validity_rate") is not None]),
                "identical_calls_across_repeats": identical,
                "mean_flip_rate": round(sum(flips.values()) / len(flips), 4),
                "scenarios_that_flip": sorted(k for k, v in flips.items() if v > 0),
            }
            if arm == "agent":
                entry["natural_illegal_first_drafts"] = sum(x.get("first_proposals_illegal", 0) for x in s)
                entry["corrected_by_revision"] = sum(x.get("corrected_by_revision", 0) for x in s)
                entry["corrected_in_one_cycle"] = sum(x.get("corrected_in_one_cycle", 0) for x in s)
                entry["corrected_by_guardrail"] = sum(x.get("corrected_by_guardrail", 0) for x in s)
            out[tk][arm] = entry
    if "0.0" in out:
        # greedy decoding with faults off: identical inputs should give identical calls
        out["determinism_at_temperature_0"] = {arm: f"{out['0.0'][arm]['identical_calls_across_repeats']}/"
                                                    f"{len(by_arm(load(ROB / 'temp_0.0_seed1.json'), arm))}"
                                               for arm in ARMS}
    return out or None


def main() -> None:
    sens = load(ROB / "sensitivity.json")
    report = {"faults": faults_axis(), "sampling": sampling_axis(), "inputs": None, "cross_axis": None}
    if sens:
        report["inputs"] = {k: v for k, v in sens.items() if k != "rows"}
        report["inputs"]["per_scenario"] = {r["id"]: {k: r[k] for k in ("stability", "agreement_under_noise",
                                                                        "margin_to_second_best_s", "baseline_call")}
                                            for r in sens["rows"]}
    hot = report["sampling"] and report["sampling"].get("0.7", {}).get("agent")
    if sens and hot:
        knife = set(sens["knife_edge"])
        flip = set(hot["scenarios_that_flip"])
        report["cross_axis"] = {"knife_edge_inputs": sorted(knife), "agent_flips_at_0.7": sorted(flip),
                                "both": sorted(knife & flip), "inputs_only": sorted(knife - flip),
                                "sampling_only": sorted(flip - knife)}
    (BENCH / "robustness.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2)[:4000])


if __name__ == "__main__":
    main()
