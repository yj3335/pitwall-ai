"""DuckDB OLAP over the Go-written Parquet lakehouse (real Singapore 2023 data)."""
from __future__ import annotations

import subprocess
import threading
import time

import pytest

from conftest import ROOT
from pitwall.olap import Lakehouse, UnsafeSQLError, guard_sql


@pytest.fixture(scope="module")
def lh(lakehouse):
    return Lakehouse(lakehouse)


def test_views_and_row_counts(lh):
    assert lh.sessions() == [9165]
    n = lh.query("SELECT count(*) FROM telemetry").rows[0][0]
    laps = lh.query("SELECT count(*) FROM laps").rows[0][0]
    assert n == 113_771  # every replayed event landed in Parquet exactly once
    assert 1000 < laps < 1200


def test_go_degradation_matches_duckdb_regr_slope(lh):
    """Cross-language check: the Go window slope equals DuckDB REGR_SLOPE over
    the same five clean fuel-corrected laps."""
    rows = lh.query(
        """
        WITH clean AS (
          SELECT driver_number, lap_number, fuel_corrected_ms, lap_number - tyre_age_laps AS stint
          FROM laps WHERE is_clean
        ), ranked AS (
          SELECT l.driver_number, l.lap_number, l.deg_slope_ms_per_lap AS go_slope,
                 (SELECT regr_slope(c.fuel_corrected_ms, c.lap_number) FROM (
                      SELECT * FROM clean c WHERE c.driver_number = l.driver_number
                        AND c.stint = l.lap_number - l.tyre_age_laps AND c.lap_number <= l.lap_number
                      ORDER BY c.lap_number DESC LIMIT 5) c) AS duck_slope
          FROM laps l WHERE l.window_laps = 5 AND l.deg_slope_ms_per_lap IS NOT NULL
        )
        SELECT count(*), max(abs(go_slope - duck_slope)) FROM ranked
        """
    ).rows[0]
    compared, max_err = rows
    assert compared > 300
    # Parquet stores fuel_corrected_ms rounded to the millisecond
    assert max_err < 1.0, max_err


def test_safety_car_laps_are_not_clean(lh):
    r = lh.query("SELECT count(*) FROM laps WHERE track_status IN ('SC','VSC') AND is_clean").rows[0][0]
    assert r == 0
    sc_laps = lh.query("SELECT DISTINCT lap_number FROM laps WHERE track_status = 'SC' ORDER BY 1").rows
    assert {20, 21}.issubset({x[0] for x in sc_laps})  # real 2023 Singapore SC


def test_driver_state_and_field(lh):
    s = lh.driver_state(9165, 44, 43)  # Hamilton before the lap-44 VSC stop
    assert s["compound"] == "HARD" and s["compounds_used"] == ["HARD", "MEDIUM"]
    field = lh.field_at_lap(9165, 43)
    assert len(field) >= 15 and field[0]["position"] == 1


def test_stint_summary_and_leaderboard(lh):
    stints = lh.stint_summary(9165, 55).records()
    assert [s["compound"] for s in stints] == ["MEDIUM", "HARD"]  # Sainz: one stop, lap 20
    board = lh.degradation_leaderboard(9165, 40).records()
    assert board and board[0]["deg_ms_per_lap"] >= board[-1]["deg_ms_per_lap"]


def test_query_latency(lh):
    times = []
    for _ in range(30):
        times.append(lh.degradation_leaderboard(9165, 40).elapsed_ms)
    times.sort()
    assert times[len(times) // 2] < 50


@pytest.mark.parametrize("bad", [
    "DROP TABLE laps", "SELECT 1; DELETE FROM laps", "COPY laps TO 'x.csv'",
    "SELECT * FROM read_csv('/etc/passwd')", "ATTACH 'x.db'", "INSTALL httpfs",
    "SELECT * FROM read_parquet('/tmp/*.parquet')", "PRAGMA version",
])
def test_guard_rejects_unsafe_sql(bad):
    with pytest.raises(UnsafeSQLError):
        guard_sql(bad)


def test_guard_bounds_and_allows_reads(lh):
    q = guard_sql("SELECT driver_code, avg(lap_time_ms) FROM laps GROUP BY 1")
    assert q.endswith("LIMIT 200")
    assert lh.safe_query("```sql\nSELECT count(*) AS n FROM laps WHERE compound = 'HARD'\n```").rows[0][0] > 0
    # keywords inside string literals are data, not commands
    assert guard_sql("SELECT 'drop the hammer' AS radio LIMIT 1")


def test_concurrent_reads_while_go_writes(binaries, tmp_path):
    """The Go processor commits hundreds of small batches while four threads
    query the same directory: zero read errors, row counts only grow."""
    out = tmp_path / "live"
    proc = subprocess.Popen(
        [str(binaries / "processor"), "-from-file", str(ROOT / "data/races/session_9636_events.jsonl.gz"),
         "-out", str(out), "-flush-rows", "300"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    errors: list[str] = []
    counts: list[int] = []
    stop = threading.Event()

    def reader():
        lh = None
        while not stop.is_set():
            try:
                if lh is None:
                    if not list(out.glob("telemetry/session=*/*.parquet")):
                        continue
                    lh = Lakehouse(out)
                counts.append(lh.query("SELECT count(*) FROM telemetry").rows[0][0])
                lh.query("SELECT driver_number, regr_slope(fuel_corrected_ms, lap_number) FROM laps GROUP BY 1")
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e))

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    proc.wait(timeout=60)
    time.sleep(0.2)
    stop.set()
    for t in threads:
        t.join()
    assert not errors, errors[:3]
    assert len(counts) > 10
    final = Lakehouse(out).query("SELECT count(*) FROM telemetry").rows[0][0]
    assert final == 126_042
    files = list(out.glob("telemetry/session=*/*.parquet"))
    assert len(files) > 100
    assert not list(out.glob("**/*.inprogress"))
