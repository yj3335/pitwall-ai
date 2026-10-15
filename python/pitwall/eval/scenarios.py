"""Load the Grand Prix scenarios and materialise each into a full race state.

Real scenarios read the car and field from the Parquet lakehouse at the
decision lap (via DuckDB); synthetic ones carry their state inline.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from pitwall import config
from pitwall.olap import Lakehouse

SCENARIO_FILE = config.ROOT / "data" / "scenarios" / "grand_prix_scenarios.json"

PACE_OFFSET = {"SOFT": -650, "MEDIUM": 0, "HARD": 450, "INTERMEDIATE": 0, "WET": 0}


def load_scenarios(path: Path | None = None) -> list[dict]:
    return json.loads((path or SCENARIO_FILE).read_text())["scenarios"]


def _field_row(row: list) -> dict:
    code, num, pos, gap, comp, age, *rest = row
    out = {"driver_code": code, "driver_number": num, "position": pos, "gap_to_leader_ms": gap,
           "compound": comp, "tyre_age_laps": age}
    if rest:
        out["planned_stop_lap"] = rest[0]
    return out


def materialise(scn: dict, lakehouse: Lakehouse | None = None) -> dict[str, Any]:
    """Return the scenario with a complete ``state`` (car, field, laps, status)."""
    s = copy.deepcopy(scn)
    if s.get("kind") == "real":
        src = s["source"]
        lh = lakehouse or Lakehouse()
        meta = json.loads((config.RACES / str(src["session_key"]) / "meta.json").read_text())
        d = lh.driver_state(src["session_key"], src["driver_number"], src["lap"])
        if d is None:
            raise ValueError(f"{s['id']}: no lakehouse data for {src}")
        deg = d["deg_slope_ms_per_lap"]
        # weather-driven swings (rain, drying) are not tyre wear: let the model decide
        deg = deg if deg is not None and 0 <= deg <= 250 else None
        pace = d["recent_pace_ms"] or 95_000.0
        base = pace - PACE_OFFSET.get(d["compound"], 0) - (deg or 0) * d["tyre_age_laps"]
        car = {
            "driver_code": d["driver_code"], "driver_number": d["driver_number"], "position": d["position"],
            "compound": d["compound"], "tyre_age_laps": d["tyre_age_laps"], "deg_slope_ms_per_lap": deg,
            "compounds_used": d["compounds_used"], "gap_to_leader_ms": d["gap_to_leader_ms"],
            "base_lap_ms": round(max(70_000.0, base)), "penalty_s": 0,
        }
        field = [
            {k: f[k] for k in ("driver_code", "driver_number", "position", "gap_to_leader_ms", "compound", "tyre_age_laps")}
            for f in lh.field_at_lap(src["session_key"], src["lap"])
            if f["driver_number"] != src["driver_number"] and f["compound"] != "UNKNOWN"
        ]
        state = {"total_laps": meta["total_laps"], "current_lap": src["lap"], "track_status": "GREEN",
                 "track_wetness": 0.0, "car": car, "field": field,
                 "session": f"{meta['year']} {meta['location']}"}
        state.update(s.get("overrides", {}))
        s["state"] = state
    else:
        st = s["state"]
        st["field"] = [_field_row(r) if isinstance(r, list) else r for r in st.get("field", [])]
        st.setdefault("track_status", "GREEN")
        st.setdefault("track_wetness", 0.0)
    s["state"]["race_control"] = s.get("race_control", "")
    return s
