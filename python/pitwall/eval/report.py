"""Merge the benchmark outputs into benchmarks/results.json.

    python -m pitwall.eval.report

Reads benchmarks/stream.json, rag_ablation.json and agent_eval.json (written by
stream_bench, rag_eval and agent_eval) and records the machine, the software
versions and the git commit next to the headline numbers.
"""
from __future__ import annotations

import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from pitwall import config

BENCH = config.ROOT / "benchmarks"


def sh(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, cwd=config.ROOT, timeout=10).stdout.strip()
    except Exception:  # noqa: BLE001
        return ""


def machine() -> dict:
    return {
        "cpu": sh("sysctl", "-n", "machdep.cpu.brand_string") or platform.processor(),
        "cores": int(sh("sysctl", "-n", "hw.ncpu") or 0),
        "memory_gb": round(int(sh("sysctl", "-n", "hw.memsize") or 0) / 2**30),
        "os": f"{platform.system()} {platform.release()}",
        "go": sh(str(config.ROOT / ".venv" / "bin" / "go"), "version"),
        "python": platform.python_version(),
        "kafka": "4.4.0 (KRaft, single broker)",
        "qdrant": sh(str(config.ROOT / ".venv" / "bin" / "qdrant"), "--version"),
        "ollama": sh("ollama", "--version").splitlines()[-1] if sh("ollama", "--version") else "",
    }


def main() -> None:
    stream = json.loads((BENCH / "stream.json").read_text())
    rag = json.loads((BENCH / "rag_ablation.json").read_text())
    agent = json.loads((BENCH / "agent_eval.json").read_text())
    rm = rag["modes"]
    results = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_commit": sh("git", "describe", "--always", "--dirty"),
        "machine": machine(),
        "stream": {
            "events": stream["events"],
            "sustained_events_per_s": stream["burst"]["sustained_events_per_s"],
            "sustained_events_per_s_range": stream["burst"]["sustained_events_per_s_range"],
            "committed_to_parquet_events_per_s": stream["burst"]["committed_to_parquet_events_per_s"],
            "cold_start_events_per_s": stream["burst"]["cold_start_events_per_s"],
            "burst_stream_to_trigger_ms": {k: stream["burst"]["stream_to_trigger_ms"][k] for k in ("p50_ms", "p95_ms", "p99_ms")},
            "paced_rate_events_per_s": stream["paced"]["rate_events_per_s"],
            "paced_stream_to_trigger_ms": {k: stream["paced"]["stream_to_trigger_ms"][k] for k in ("p50_ms", "p95_ms", "p99_ms")},
            "sink_errors": stream["burst"]["sink_errors"],
            "duckdb_concurrent_queries": stream["paced"]["duckdb_concurrent"]["queries_run"],
            "duckdb_concurrent_errors": stream["paced"]["duckdb_concurrent"]["errors"],
            "duckdb_concurrent_p50_ms": stream["paced"]["duckdb_concurrent"]["p50_ms"],
            "duckdb_olap_p50_ms": stream["duckdb_olap"]["median_of_p50_ms"],
            "monte_carlo_10k_x6_in_process_p50_ms": stream["monte_carlo"]["in_process"]["p50_ms"],
            "monte_carlo_10k_x6_mcp_round_trip_p50_ms": stream["monte_carlo"]["over_mcp_http"]["round_trip_p50_ms"],
            "monte_carlo_10k_single_strategy_p50_ms": stream["monte_carlo"]["in_process"]["single_strategy_p50_ms"],
        },
        "rag": {
            "queries": rag["queries"],
            "reranker": rag["reranker"],
            "modes": {m: {k: v[k] for k in ("recall_at_1", "recall_at_3", "mrr_at_10", "recall_at_3_exact", "recall_at_3_semantic",
                                            "qdrant_p50_ms", "qdrant_p95_ms", "end_to_end_p50_ms", "end_to_end_p95_ms")}
                      for m, v in rm.items()},
        },
        "agent": {
            "scenarios": agent["scenarios"],
            "fault_rate": agent["fault_rate"],
            "models": agent["models"],
            "single_pass": agent["single_pass"],
            "cyclic_agent": agent["agent"],
            # illegal first drafts: caught by the verifier, then fixed by the
            # model's own revision or by the guardrail after the budget
            "cyclic_self_correction": {k: agent["agent"].get(k) for k in (
                "first_proposals_illegal", "caught_and_corrected", "corrected_by_revision",
                "corrected_in_one_cycle", "corrected_by_guardrail", "revisions_total")},
            "radio_override_traps": {"single_pass": agent["single_pass"].get("radio_override_traps"),
                                     "cyclic_agent": agent["agent"].get("radio_override_traps")},
        },
    }
    (BENCH / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
