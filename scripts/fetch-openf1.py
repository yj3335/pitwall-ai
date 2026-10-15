#!/usr/bin/env python3
"""Fetch real historical Grand Prix data from the OpenF1 API and build the
replayable telemetry event stream used by the Go replayer and benchmarks.

For each race session this writes:
  data/races/<key>/{meta,drivers,laps,stints,pit,race_control,weather,position,intervals}.json.gz
  data/races/session_<key>_events.jsonl.gz   (time-ordered, one event per car per ~1s)

Raw car_data responses are cached in data/races/.cache/ (gitignored) so
re-runs are offline. Only the standard library is used.
"""
from __future__ import annotations

import argparse
import bisect
import gzip
import json
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

API = "https://api.openf1.org/v1"
ROOT = Path(__file__).resolve().parent.parent
RACES = ROOT / "data" / "races"
CACHE = RACES / ".cache"

# Sessions committed to the repo: strategy-rich races (wet crossovers, SC/VSC, red flag).
DEFAULT_SESSIONS = [9558, 9165, 9636]  # 2024 British GP, 2023 Singapore GP, 2024 Sao Paulo GP

TABLES = ["drivers", "laps", "stints", "pit", "race_control", "weather", "position", "intervals"]


def get(endpoint: str, **params) -> list[dict]:
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    key = CACHE / f"{endpoint}_{qs.replace('&', '_').replace('=', '-')}.json.gz"
    if key.exists():
        with gzip.open(key, "rt") as f:
            return json.load(f)
    url = f"{API}/{endpoint}?{qs}"
    for attempt in range(6):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "pitwall-ai"}), timeout=120) as r:
                data = json.load(r)
            break
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and attempt < 5:
                time.sleep(2 ** attempt)
                continue
            raise
    if not isinstance(data, list):
        raise RuntimeError(f"unexpected response from {url}: {data}")
    CACHE.mkdir(parents=True, exist_ok=True)
    with gzip.open(key, "wt") as f:
        json.dump(data, f)
    time.sleep(0.4)  # stay well under the public rate limit
    return data


def ts(s: str | None) -> int | None:
    if not s:
        return None
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def write_gz(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", compresslevel=9) as f:
        json.dump(obj, f, separators=(",", ":"))


class Series:
    """Time-indexed step function: value at t is the last sample with time <= t."""

    def __init__(self, rows: list[tuple[int, object]]):
        rows = sorted(rows, key=lambda r: r[0])
        self.t = [r[0] for r in rows]
        self.v = [r[1] for r in rows]

    def at(self, t: int, default=None):
        i = bisect.bisect_right(self.t, t) - 1
        return self.v[i] if i >= 0 else default


def parse_gap(v, lap_ms: int) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(round(v * 1000))
    m = re.match(r"\+?(\d+)\s*LAP", str(v))
    if m:
        return int(m.group(1)) * lap_ms
    return None


def track_status_series(rc: list[dict]) -> tuple[Series, list[tuple[int, int, int]]]:
    """Derive a global track status (GREEN/SC/VSC/RED) step series plus
    sector-scoped yellow intervals from FIA race control messages."""
    rows, state = [], "GREEN"
    yellows: dict[int, int] = {}
    yellow_spans: list[tuple[int, int, int]] = []
    for m in sorted(rc, key=lambda r: r["date"]):
        t, msg, flag = ts(m["date"]), (m.get("message") or "").upper(), (m.get("flag") or "").upper()
        new = state
        track_wide = m.get("scope") == "Track"
        # word-boundary match: "CHEQUERED FLAG" contains the substring "RED FLAG"
        if flag == "RED" or re.search(r"\bRED FLAG\b", msg):
            new = "RED"
        elif flag == "CHEQUERED":
            new = "GREEN"
        elif state == "RED" and "SESSION STARTED" in msg:
            new = "SC"  # resumption is led by the safety car
        elif state in ("RED", "SC") and "ROLLING START" in msg:
            new = "GREEN"
        elif "VIRTUAL SAFETY CAR DEPLOYED" in msg or "VSC DEPLOYED" in msg:
            new = "VSC"
        elif "SAFETY CAR DEPLOYED" in msg:
            new = "SC"
        elif "VIRTUAL SAFETY CAR ENDING" in msg or "VSC ENDING" in msg:
            new = "GREEN"
        elif track_wide and (flag == "GREEN" or (flag == "CLEAR" and state != "RED")):
            # SC/VSC periods end with a track-scoped "TRACK CLEAR"; a red-flag
            # suspension only ends at the restart ("GREEN LIGHT" or SC restart).
            new = "GREEN"
        if new != state:
            rows.append((t, new))
            state = new
        sec = m.get("sector")
        if sec is not None and flag in ("YELLOW", "DOUBLE YELLOW"):
            yellows.setdefault(sec, t)
        elif sec is not None and flag == "CLEAR" and sec in yellows:
            yellow_spans.append((yellows.pop(sec), t, sec))
    return Series([(0, "GREEN")] + rows), yellow_spans


def build_events(key: int, t: dict[str, list[dict]], sample_ms: int) -> list[dict]:
    drivers = {d["driver_number"]: d["name_acronym"] for d in t["drivers"]}
    # ---- laps: reconstruct absolute start times (lap 1 start is often missing)
    laps_by_driver: dict[int, list[dict]] = {}
    for lap in t["laps"]:
        laps_by_driver.setdefault(lap["driver_number"], []).append(lap)
    lap_index: dict[int, tuple[list[int], list[dict]]] = {}
    race_start, race_end = None, None
    for dn, laps in laps_by_driver.items():
        laps.sort(key=lambda r: r["lap_number"])
        for i, lap in enumerate(laps):
            lap["_start"] = ts(lap.get("date_start"))
        for i in range(len(laps) - 2, -1, -1):
            if laps[i]["_start"] is None and laps[i + 1]["_start"] and laps[i].get("lap_duration"):
                laps[i]["_start"] = laps[i + 1]["_start"] - int(laps[i]["lap_duration"] * 1000)
        laps = [lp for lp in laps if lp["_start"] is not None]
        if not laps:
            continue
        lap_index[dn] = ([lp["_start"] for lp in laps], laps)
        s = laps[0]["_start"]
        last = laps[-1]
        e = last["_start"] + int((last.get("lap_duration") or 100) * 1000)
        race_start = s if race_start is None else min(race_start, s)
        race_end = e if race_end is None else max(race_end, e)

    stints: dict[int, list[dict]] = {}
    for s in t["stints"]:
        stints.setdefault(s["driver_number"], []).append(s)

    def stint_for(dn: int, lap_no: int) -> tuple[str, int]:
        for s in stints.get(dn, []):
            if s.get("lap_start") and s.get("lap_end") and s["lap_start"] <= lap_no <= s["lap_end"]:
                comp = (s.get("compound") or "UNKNOWN").upper()
                return comp, (s.get("tyre_age_at_start") or 0) + lap_no - s["lap_start"]
        return "UNKNOWN", 0

    intervals: dict[int, Series] = {}
    by_dn: dict[int, list] = {}
    for r in t["intervals"]:
        by_dn.setdefault(r["driver_number"], []).append((ts(r["date"]), (r.get("gap_to_leader"), r.get("interval"))))
    for dn, rows in by_dn.items():
        intervals[dn] = Series(rows)
    positions: dict[int, Series] = {}
    by_dn = {}
    for r in t["position"]:
        by_dn.setdefault(r["driver_number"], []).append((ts(r["date"]), r["position"]))
    for dn, rows in by_dn.items():
        positions[dn] = Series(rows)
    weather = Series([(ts(w["date"]), (w.get("track_temperature"), w.get("rainfall"))) for w in t["weather"]])
    status, yellow_spans = track_status_series(t["race_control"])
    max_minisector = max([s for _, _, s in yellow_spans] + [3])

    def pos_map(at: int) -> dict[int, int]:
        return {dn: s.at(at, 99) for dn, s in positions.items()}

    events: list[dict] = []
    for dn, code in drivers.items():
        if dn not in lap_index:
            continue
        cd = get("car_data", session_key=key, driver_number=dn)
        starts, laps = lap_index[dn]
        last_bucket = -1
        for row in cd:
            t_ms = ts(row["date"])
            if t_ms is None or t_ms < race_start or t_ms > race_end:
                continue
            bucket = t_ms // sample_ms
            if bucket == last_bucket:
                continue
            last_bucket = bucket
            li = bisect.bisect_right(starts, t_ms) - 1
            if li < 0:
                continue
            lap = laps[li]
            lap_no = lap["lap_number"]
            if li == len(laps) - 1:
                # final recorded lap: a retirement mid-lap has no lap_duration,
                # so cap it instead of streaming the parked car forever
                if t_ms > starts[li] + int((lap.get("lap_duration") or 180.0) * 1000) + 5000:
                    continue
            elapsed = (t_ms - lap["_start"]) / 1000.0
            s1, s2 = lap.get("duration_sector_1") or 30.0, lap.get("duration_sector_2") or 30.0
            sector = 1 if elapsed < s1 else 2 if elapsed < s1 + s2 else 3
            prev = laps[li - 1] if li > 0 else None
            last_lap_ms = int(prev["lap_duration"] * 1000) if prev and prev.get("lap_duration") else 0
            comp, age = stint_for(dn, lap_no)
            ref_lap = last_lap_ms or 95000
            gap, iv = intervals[dn].at(t_ms, (None, None)) if dn in intervals else (None, None)
            pm = pos_map(t_ms)
            my_pos = pm.get(dn, 99)
            behind = next((d for d, p in pm.items() if p == my_pos + 1), None)
            iv_behind = None
            if behind is not None and behind in intervals:
                iv_behind = parse_gap(intervals[behind].at(t_ms, (None, None))[1], ref_lap)
            temp, rain = weather.at(t_ms, (None, 0))
            st = status.at(t_ms, "GREEN")
            if st == "GREEN":
                for a, b, ms in yellow_spans:
                    if a <= t_ms <= b and min(3, 1 + (ms - 1) * 3 // max_minisector) == sector:
                        st = "YELLOW"
                        break
            events.append({
                "ts_ms": t_ms,
                "session_key": key,
                "driver_number": dn,
                "driver_code": code,
                "lap_number": lap_no,
                "sector": sector,
                "position": my_pos,
                "speed_kph": row.get("speed") or 0,
                "rpm": row.get("rpm") or 0,
                "throttle_pct": min(100, row.get("throttle") or 0),
                "brake": 1 if (row.get("brake") or 0) > 0 else 0,
                "n_gear": row.get("n_gear") or 0,
                "drs": 1 if (row.get("drs") or 0) >= 10 else 0,
                "compound": comp,
                "tyre_age_laps": age,
                "lap_time_ms": last_lap_ms,
                "gap_to_leader_ms": parse_gap(gap, ref_lap),
                "interval_ahead_ms": parse_gap(iv, ref_lap),
                "interval_behind_ms": iv_behind,
                "is_pit_out_lap": bool(lap.get("is_pit_out_lap")),
                "track_temp_c": temp,
                "rainfall": 1 if rain else 0,
                "track_status": st,
            })
        print(f"  car {dn:>2} {code}: {sum(1 for e in events if e['driver_number'] == dn)} events", file=sys.stderr)
    events.sort(key=lambda e: (e["ts_ms"], e["driver_number"]))
    for i, e in enumerate(events):
        e["seq"] = i
    return events


def fetch_session(key: int, sample_ms: int) -> None:
    sess = get("sessions", session_key=key)
    if not sess:
        raise SystemExit(f"session {key} not found")
    meta = sess[0]
    print(f"session {key}: {meta['year']} {meta['location']} {meta['session_name']}", file=sys.stderr)
    tables = {name: get(name, session_key=key) for name in TABLES}
    out = RACES / str(key)
    for name, rows in tables.items():
        write_gz(out / f"{name}.json.gz", rows)
    pit_lane = sorted(p["lane_duration"] for p in tables["pit"] if p.get("lane_duration") and 15 < p["lane_duration"] < 40)
    laps_total = max((lp["lap_number"] for lp in tables["laps"]), default=0)
    meta_out = {
        "session_key": key,
        "meeting_key": meta["meeting_key"],
        "year": meta["year"],
        "location": meta["location"],
        "country": meta["country_name"],
        "circuit": meta.get("circuit_short_name"),
        "date_start": meta["date_start"],
        "total_laps": laps_total,
        "median_pit_lane_s": pit_lane[len(pit_lane) // 2] if pit_lane else None,
        "source": f"{API} (session_key={key})",
    }
    (out / "meta.json").write_text(json.dumps(meta_out, indent=2) + "\n")
    events = build_events(key, tables, sample_ms)
    path = RACES / f"session_{key}_events.jsonl.gz"
    with gzip.open(path, "wt", compresslevel=9) as f:
        for e in events:
            f.write(json.dumps(e, separators=(",", ":")) + "\n")
    print(f"  wrote {len(events)} events -> {path.relative_to(ROOT)}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--session", type=int, action="append", help="OpenF1 session_key (repeatable)")
    ap.add_argument("--sample-ms", type=int, default=1000, help="per-car event sampling period")
    args = ap.parse_args()
    for key in args.session or DEFAULT_SESSIONS:
        fetch_session(key, args.sample_ms)


if __name__ == "__main__":
    main()
