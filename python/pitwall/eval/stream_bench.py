"""Benchmark 1: Go + Kafka + Parquet + DuckDB pipeline, and Monte Carlo over MCP.

    python -m pitwall.eval.stream_bench --out ../benchmarks/stream.json

1. Burst: 200,000 real events through Kafka -> Go engines -> Parquet, five
   measured trials after one warm-up (cmd/bench-stream), for throughput.
2. Paced: the same 200,000 events at 20,000 events/s while four DuckDB threads
   query the lakehouse being written, for steady-state stream-to-trigger
   latency and concurrent-read errors.
3. Monte Carlo: 10,000 rollouts x 6 strategies, timed in-process (Go) and as
   MCP JSON-RPC calls over HTTP from Python.
4. DuckDB analytical query latency over the committed lakehouse.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from pitwall import config
from pitwall.mcp_client.client import HTTPMCPClient
from pitwall.olap import Lakehouse

BIN = config.ROOT / "bin" / "bench-stream"


def pct(xs: list[float], q: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))] if s else 0.0


def run_bench(args: list[str]) -> dict:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        out = f.name
    subprocess.run([str(BIN), *args, "-json", out], cwd=config.ROOT, check=True, capture_output=True, text=True)
    return json.loads(Path(out).read_text())


QUERIES = {
    "telemetry_count": "SELECT count(*) FROM telemetry",
    "degradation_by_driver": """SELECT driver_number, regr_slope(fuel_corrected_ms, lap_number) FILTER (is_clean) AS deg
                                FROM laps GROUP BY 1 ORDER BY 2 DESC NULLS LAST""",
    "sector_speeds": """SELECT driver_number, sector, avg(speed_kph), max(speed_kph) FROM telemetry GROUP BY 1, 2""",
    "trigger_mix": "SELECT kind, count(*) FROM triggers GROUP BY 1",
}


def concurrent_readers(root: Path, stop: threading.Event, n_threads: int = 4) -> dict:
    lat: dict[str, list[float]] = {k: [] for k in QUERIES}
    errors: list[str] = []
    counts: list[int] = []
    lock = threading.Lock()

    def worker():
        lh = None
        while not stop.is_set():
            try:
                if lh is None:
                    if not any(root.glob("telemetry/session=*/*.parquet")):
                        time.sleep(0.02)
                        continue
                    lh = Lakehouse(root)
                for name, sql in QUERIES.items():
                    r = lh.query(sql)
                    with lock:
                        lat[name].append(r.elapsed_ms)
                        if name == "telemetry_count":
                            counts.append(r.rows[0][0])
            except Exception as e:  # noqa: BLE001 - every error is a finding
                with lock:
                    errors.append(f"{type(e).__name__}: {str(e)[:200]}")

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(n_threads)]
    for t in threads:
        t.start()
    return {"threads": threads, "lat": lat, "errors": errors, "counts": counts}


def paced_with_readers(rate: int, events: int) -> dict:
    work = Path(tempfile.mkdtemp(prefix="pitwall-paced-"))
    stop = threading.Event()
    readers = concurrent_readers(work / "trial_0", stop)
    res = run_bench(["-events", str(events), "-rate", str(rate), "-trials", "1", "-warmup", "1", "-out", str(work)])
    stop.set()
    for t in readers["threads"]:
        t.join(timeout=10)
    final = Lakehouse(work / "trial_0").query("SELECT count(*) FROM telemetry").rows[0][0]
    counts = readers["counts"]
    monotonic = all(b >= a for a, b in zip(counts, counts[1:])) if counts else False
    all_lat = [x for v in readers["lat"].values() for x in v]
    return {
        "bench": res,
        "duckdb_concurrent": {
            "reader_threads": 4,
            "queries_run": len(all_lat),
            "errors": len(readers["errors"]),
            "error_samples": readers["errors"][:3],
            "p50_ms": round(pct(all_lat, 0.5), 2),
            "p95_ms": round(pct(all_lat, 0.95), 2),
            "by_query_p50_ms": {k: round(pct(v, 0.5), 2) for k, v in readers["lat"].items()},
            "row_count_never_decreased": monotonic,
            "final_rows_equal_events": final == events,
        },
    }


async def mcp_latency(calls: int) -> dict:
    from pitwall.agents.graph import sim_request
    from pitwall.eval.scenarios import load_scenarios, materialise

    scn = materialise({s["id"]: s for s in load_scenarios()}["GP17"])
    req = sim_request(scn["state"])
    client = HTTPMCPClient()
    await client.initialize()
    rtt, server = [], []
    for i in range(calls + 5):
        req["seed"] = i
        t0 = time.perf_counter()
        r = await client.call_tool("simulate_pit_strategies", req)
        if i >= 5:
            rtt.append((time.perf_counter() - t0) * 1000)
            server.append(r["elapsed_ms"])
    await client.aclose()
    return {"calls": calls, "trajectories_per_call": r["total_trajectories"], "workers": r["workers"],
            "round_trip_p50_ms": round(pct(rtt, 0.5), 2), "round_trip_p95_ms": round(pct(rtt, 0.95), 2),
            "server_p50_ms": round(pct(server, 0.5), 2), "server_p95_ms": round(pct(server, 0.95), 2)}


def olap_latency(runs: int = 50) -> dict:
    lh = Lakehouse()
    at = lh.decision_point(9165, 44, 44)
    cases = {
        "degradation_leaderboard": lambda: lh.degradation_leaderboard(9165, 40),
        "stint_summary": lambda: lh.stint_summary(9165, 55),
        "pace_delta": lambda: lh.pace_delta(9165, 55, 4, 30, 50),
        "decision_scoped_model_sql": lambda: lh.safe_query(
            "SELECT lap_number, lap_time_ms, fuel_corrected_ms FROM laps WHERE driver_number = 44 ORDER BY lap_number DESC LIMIT 8",
            as_of=at),
        "field_at_decision_instant": lambda: lh.query(
            "SELECT count(*) FROM (SELECT driver_number FROM telemetry WHERE session_key = 9165 AND ts_ms <= ? "
            "QUALIFY row_number() OVER (PARTITION BY driver_number ORDER BY ts_ms DESC) = 1)", [at.ts_ms]),
    }
    out = {}
    for name, fn in cases.items():
        fn()  # warm
        xs = [fn().elapsed_ms for _ in range(runs)]
        out[name] = {"p50_ms": round(pct(xs, 0.5), 2), "p95_ms": round(pct(xs, 0.95), 2)}
    rows = lh.query("SELECT (SELECT count(*) FROM telemetry), (SELECT count(*) FROM laps)").rows[0]
    allp50 = [v["p50_ms"] for v in out.values()]
    return {"lakehouse_rows": {"telemetry": rows[0], "laps": rows[1]}, "queries": out,
            "median_of_p50_ms": round(statistics.median(allp50), 2)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--events", type=int, default=200_000)
    args = ap.parse_args()
    print("burst: 1 warm-up + 5 measured trials ...", flush=True)
    burst = run_bench(["-events", str(args.events), "-trials", "5", "-warmup", "1", "-sim-runs", "200"])
    print("paced 20k events/s with 4 concurrent DuckDB readers ...", flush=True)
    paced = paced_with_readers(20_000, args.events)
    print("Monte Carlo over MCP JSON-RPC ...", flush=True)
    mcp = asyncio.run(mcp_latency(100))
    print("DuckDB analytical queries ...", flush=True)
    olap = olap_latency()
    m = burst["median"]["median_trial"]
    pm = paced["bench"]["median"]["median_trial"]
    report = {
        "events": args.events,
        "burst": {"trials": burst["median"]["trials"],
                  "sustained_events_per_s": round(m["sustained_events_per_s"]),
                  "sustained_events_per_s_range": [round(x) for x in burst["median"]["sustained_events_per_s_range"]],
                  "committed_to_parquet_events_per_s": round(m["committed_events_per_s"]),
                  "producer_events_per_s": round(m["producer_events_per_s"]),
                  "stream_to_trigger_ms": m["stream_to_trigger_latency"],
                  "cold_start_events_per_s": round(burst["warmup"][0]["sustained_events_per_s"]),
                  "parquet_files": m["parquet_files"], "parquet_rows": m["parquet_rows"],
                  "sink_errors": m["sink_errors"], "decode_errors": m["decode_errors"], "triggers": m["triggers"]},
        "paced": {"rate_events_per_s": 20_000,
                  "sustained_events_per_s": round(pm["sustained_events_per_s"]),
                  "stream_to_trigger_ms": pm["stream_to_trigger_latency"],
                  "produce_to_processed_ms": pm["produce_to_processed_latency"],
                  "duckdb_concurrent": paced["duckdb_concurrent"]},
        "monte_carlo": {"in_process": burst["monte_carlo"], "over_mcp_http": mcp},
        "duckdb_olap": olap,
        "raw": {"burst": burst, "paced": paced["bench"]},
    }
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "raw"}, indent=2))


if __name__ == "__main__":
    main()
