"""DuckDB analytical engine over the Parquet lakehouse.

DuckDB scans the immutable batch files written by the Go processor in place
(no import step). The views wrap ``read_parquet`` globs, which are re-expanded
on every query, so newly committed batches are visible immediately while the
writer keeps appending; the writer's temp-file + rename commit means no query
can observe a half-written file and neither side takes a lock.
"""
from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from pitwall import config

TABLES = ("telemetry", "laps", "triggers")

# Column types of each table, used for the empty placeholder view that stands
# in until the writer commits the table's first batch.
COLUMNS = {
    "laps": "session_key INT, driver_number INT, driver_code VARCHAR, lap_number INT, compound VARCHAR, "
            "tyre_age_laps INT, position INT, lap_time_ms BIGINT, fuel_corrected_ms BIGINT, "
            "deg_slope_ms_per_lap DOUBLE, window_laps INT, gap_to_leader_ms BIGINT, interval_ahead_ms BIGINT, "
            "interval_behind_ms BIGINT, track_status VARCHAR, is_clean BOOLEAN, ts_ms BIGINT",
    "telemetry": "ts_ms BIGINT, seq BIGINT, session_key INT, driver_number INT, driver_code VARCHAR, lap_number INT, "
                 "sector INT, position INT, speed_kph INT, rpm INT, throttle_pct INT, brake INT, n_gear INT, drs INT, "
                 "compound VARCHAR, tyre_age_laps INT, lap_time_ms BIGINT, gap_to_leader_ms BIGINT, "
                 "interval_ahead_ms BIGINT, interval_behind_ms BIGINT, is_pit_out_lap BOOLEAN, track_temp_c DOUBLE, "
                 "rainfall INT, track_status VARCHAR, ingest_ms BIGINT",
    "triggers": "ts_ms BIGINT, detected_ms BIGINT, produced_ms BIGINT, session_key INT, driver_number INT, "
                "driver_code VARCHAR, lap_number INT, rival_driver_number INT, kind VARCHAR, detail VARCHAR, value DOUBLE",
}


def _empty_view_sql(table: str) -> str:
    cols = ", ".join(f"NULL::{c.split()[1]} AS {c.split()[0]}" for c in COLUMNS[table].split(", "))
    return f"CREATE OR REPLACE VIEW {table} AS SELECT {cols} WHERE false"

SCHEMA_DOC = """\
Tables (DuckDB views over Parquet, one row group per committed batch):

laps(session_key INT, driver_number INT, driver_code VARCHAR, lap_number INT, compound VARCHAR,
     tyre_age_laps INT, position INT, lap_time_ms BIGINT, fuel_corrected_ms BIGINT,
     deg_slope_ms_per_lap DOUBLE /* NULL unless clean */, window_laps INT, gap_to_leader_ms BIGINT,
     interval_ahead_ms BIGINT, interval_behind_ms BIGINT, track_status VARCHAR, is_clean BOOLEAN, ts_ms BIGINT)
  -- one row per completed lap; is_clean excludes lap 1, in/out laps, SC/VSC/RED and restart laps, >107% outliers

telemetry(ts_ms BIGINT, seq BIGINT, session_key INT, driver_number INT, driver_code VARCHAR, lap_number INT,
          sector INT, position INT, speed_kph INT, rpm INT, throttle_pct INT, brake INT, n_gear INT, drs INT,
          compound VARCHAR, tyre_age_laps INT, lap_time_ms BIGINT, gap_to_leader_ms BIGINT,
          interval_ahead_ms BIGINT, interval_behind_ms BIGINT, is_pit_out_lap BOOLEAN, track_temp_c DOUBLE,
          rainfall INT, track_status VARCHAR, ingest_ms BIGINT)
  -- ~1 Hz per car; -1 means unknown for gaps/intervals

triggers(ts_ms BIGINT, detected_ms BIGINT, produced_ms BIGINT, session_key INT, driver_number INT,
         driver_code VARCHAR, lap_number INT, rival_driver_number INT, kind VARCHAR, detail VARCHAR, value DOUBLE)
  -- kind in (UNDERCUT_WINDOW, TIRE_CLIFF, CHEAP_PIT_WINDOW, WET_CROSSOVER)
"""

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|create|alter|attach|detach|copy|export|import|install|load|pragma|set|call|"
    r"checkpoint|vacuum|read_csv\w*|read_json\w*|read_text|read_blob|glob|httpfs|getenv|system)\b",
    re.IGNORECASE,
)


class UnsafeSQLError(ValueError):
    """Raised when model-generated SQL is not a single read-only SELECT."""


def guard_sql(sql: str, max_rows: int = 200) -> str:
    """Validate model-written SQL: one SELECT/WITH statement, no side effects,
    no file/network access beyond the lakehouse views, bounded result size."""
    s = sql.strip().rstrip(";").strip()
    s = re.sub(r"^```(?:sql)?|```$", "", s, flags=re.IGNORECASE).strip()
    if not s:
        raise UnsafeSQLError("empty query")
    if ";" in s:
        raise UnsafeSQLError("multiple statements are not allowed")
    if not re.match(r"^(select|with)\b", s, re.IGNORECASE):
        raise UnsafeSQLError("only SELECT / WITH queries are allowed")
    stripped = re.sub(r"'(?:[^']|'')*'", "''", s)  # ignore string literals
    m = _FORBIDDEN.search(stripped)
    if m:
        raise UnsafeSQLError(f"forbidden keyword: {m.group(0)}")
    if "read_parquet" in stripped.lower():
        raise UnsafeSQLError("query the laps / telemetry / triggers views, not files")
    if not re.search(r"\blimit\s+\d+\s*$", s, re.IGNORECASE):
        s = f"SELECT * FROM ({s}) AS q LIMIT {max_rows}"
    return s


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[tuple]
    elapsed_ms: float

    def records(self) -> list[dict[str, Any]]:
        return [dict(zip(self.columns, r)) for r in self.rows]


class Lakehouse:
    """Thread-safe facade: one DuckDB connection, a cursor per call."""

    def __init__(self, root: str | Path | None = None, threads: int | None = None):
        self.root = Path(root or config.LAKEHOUSE)
        self._con = duckdb.connect(database=":memory:")
        if threads:
            self._con.execute(f"SET threads = {int(threads)}")
        self._lock = threading.Lock()
        self.refresh_views()

    def refresh_views(self) -> None:
        with self._lock:
            missing = set()
            for t in TABLES:
                files = list((self.root / t).glob("session=*/*.parquet"))
                glob = (self.root / t / "**" / "*.parquet").as_posix()
                if files:
                    self._con.execute(
                        f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{glob}', hive_partitioning = true, union_by_name = true)"
                    )
                else:
                    missing.add(t)
                    self._con.execute(_empty_view_sql(t))
            # a table with no committed batch yet gets a placeholder; it is
            # re-bound on the next query once the writer commits its first file
            self._missing = missing

    def query(self, sql: str, params: list | None = None) -> QueryResult:
        if self._missing and any(next((self.root / t).glob("session=*/*.parquet"), None) for t in self._missing):
            self.refresh_views()
        t0 = time.perf_counter()
        cur = self._con.cursor()  # cursors are independent: safe across threads
        try:
            rel = cur.execute(sql, params or [])
            rows = rel.fetchall()
            cols = [d[0] for d in rel.description] if rel.description else []
        finally:
            cur.close()
        return QueryResult(cols, rows, (time.perf_counter() - t0) * 1000)

    def safe_query(self, sql: str, max_rows: int = 200) -> QueryResult:
        return self.query(guard_sql(sql, max_rows))

    # ------------------------------------------------------------ analytics
    def sessions(self) -> list[int]:
        return [r[0] for r in self.query("SELECT DISTINCT session_key FROM laps ORDER BY 1").rows]

    def stint_summary(self, session: int, driver: int | None = None) -> QueryResult:
        """Per-stint pace and fuel-corrected degradation (REGR_SLOPE over clean laps)."""
        return self.query(
            """
            WITH l AS (
              SELECT *, lap_number - tyre_age_laps AS stint_start
              FROM laps WHERE session_key = ? AND (? IS NULL OR driver_number = ?)
            )
            SELECT driver_number, any_value(driver_code) AS driver_code, compound, min(lap_number) AS first_lap,
                   max(lap_number) AS last_lap, count(*) AS laps, count(*) FILTER (is_clean) AS clean_laps,
                   round(avg(lap_time_ms) FILTER (is_clean)) AS avg_clean_lap_ms,
                   min(lap_time_ms) FILTER (is_clean) AS best_lap_ms,
                   round(regr_slope(fuel_corrected_ms, lap_number) FILTER (is_clean), 1) AS deg_ms_per_lap
            FROM l GROUP BY driver_number, compound, stint_start
            ORDER BY driver_number, first_lap
            """,
            [session, driver, driver],
        )

    def degradation_leaderboard(self, session: int, at_lap: int) -> QueryResult:
        """Each car's latest 5-lap fuel-corrected degradation slope as of a lap."""
        return self.query(
            """
            SELECT driver_number, driver_code, compound, tyre_age_laps, lap_number,
                   round(deg_slope_ms_per_lap, 1) AS deg_ms_per_lap, position
            FROM laps
            WHERE session_key = ? AND lap_number <= ? AND deg_slope_ms_per_lap IS NOT NULL
            QUALIFY row_number() OVER (PARTITION BY driver_number ORDER BY lap_number DESC) = 1
            ORDER BY deg_ms_per_lap DESC
            """,
            [session, at_lap],
        )

    def pace_delta(self, session: int, a: int, b: int, from_lap: int, to_lap: int) -> QueryResult:
        """Lap-by-lap delta between two cars with LAG-based lap-on-lap change."""
        return self.query(
            """
            WITH x AS (
              SELECT lap_number,
                     max(lap_time_ms) FILTER (driver_number = ?) AS a_ms,
                     max(lap_time_ms) FILTER (driver_number = ?) AS b_ms
              FROM laps WHERE session_key = ? AND lap_number BETWEEN ? AND ? AND driver_number IN (?, ?)
              GROUP BY lap_number
            )
            SELECT lap_number, a_ms, b_ms, a_ms - b_ms AS delta_ms,
                   (a_ms - b_ms) - lag(a_ms - b_ms) OVER (ORDER BY lap_number) AS delta_change_ms
            FROM x ORDER BY lap_number
            """,
            [a, b, session, from_lap, to_lap, a, b],
        )

    def sector_speeds(self, session: int, lap: int, drivers: list[int]) -> QueryResult:
        """Average speed and full-throttle share per sector from raw telemetry."""
        ph = ",".join("?" for _ in drivers)
        return self.query(
            f"""
            SELECT driver_code, sector, round(avg(speed_kph), 1) AS avg_speed_kph, max(speed_kph) AS top_speed_kph,
                   round(100.0 * avg((throttle_pct >= 98)::INT), 1) AS full_throttle_pct
            FROM telemetry WHERE session_key = ? AND lap_number = ? AND driver_number IN ({ph})
            GROUP BY driver_code, sector ORDER BY driver_code, sector
            """,
            [session, lap, *drivers],
        )

    def driver_state(self, session: int, driver: int, lap: int) -> dict[str, Any] | None:
        """Race state of one car at the end of a lap (for the strategy agent)."""
        r = self.query(
            """
            WITH hist AS (
              SELECT * FROM laps WHERE session_key = ? AND driver_number = ? AND lap_number <= ?
            )
            SELECT h.driver_number, h.driver_code, h.lap_number, h.compound, h.tyre_age_laps, h.position,
                   h.gap_to_leader_ms, h.interval_ahead_ms, h.interval_behind_ms,
                   (SELECT deg_slope_ms_per_lap FROM hist WHERE deg_slope_ms_per_lap IS NOT NULL
                      ORDER BY lap_number DESC LIMIT 1) AS deg_slope_ms_per_lap,
                   (SELECT list(DISTINCT compound ORDER BY compound) FROM hist WHERE compound <> 'UNKNOWN') AS compounds_used,
                   (SELECT median(lap_time_ms) FROM hist WHERE is_clean AND lap_number > ? - 6) AS recent_pace_ms
            FROM hist h ORDER BY h.lap_number DESC LIMIT 1
            """,
            [session, driver, lap, lap],
        )
        recs = r.records()
        return recs[0] if recs else None

    def field_at_lap(self, session: int, lap: int) -> list[dict[str, Any]]:
        """Every car's position, gap, compound and age at a lap (MCP 'field' input)."""
        return self.query(
            """
            SELECT driver_number, driver_code, position, gap_to_leader_ms, compound, tyre_age_laps,
                   lap_time_ms AS last_lap_ms
            FROM laps WHERE session_key = ? AND lap_number = ? ORDER BY position
            """,
            [session, lap],
        ).records()

    def recent_triggers(self, session: int, lap: int, driver: int | None = None, limit: int = 20) -> list[dict]:
        return self.query(
            """
            SELECT lap_number, driver_code, kind, detail, value FROM triggers
            WHERE session_key = ? AND lap_number <= ? AND (? IS NULL OR driver_number = ?)
            ORDER BY ts_ms DESC LIMIT ?
            """,
            [session, lap, driver, driver, limit],
        ).records()
