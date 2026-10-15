"""Load the Grand Prix scenarios and materialise each into a full race state.

Real scenarios are rebuilt from the Parquet lakehouse at the decision instant
(see ``Lakehouse.decision_point``): nothing recorded after the call, such as
the car's position inside the pit lane or laps not yet completed, is visible.
Each scenario carries the regulation edition in force and the instant used
for point-in-time retrieval. Synthetic scenarios carry their state inline and
are set in the 2025 season.
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pitwall import config, regulations
from pitwall.olap import Lakehouse

SCENARIO_FILE = config.ROOT / "data" / "scenarios" / "grand_prix_scenarios.json"

# mirrors internal/sim Compounds: pace offset vs MEDIUM on a dry track (ms)
PACE_OFFSET = {"SOFT": -650, "MEDIUM": 0, "HARD": 450, "INTERMEDIATE": 0, "WET": 0}
# generic pit losses of the simulator (internal/sim DefaultPitLoss*)
GENERIC_PIT_LOSS = {"green_s": 21.8, "neutral_s": 13.2}
SYNTHETIC_DECISION_DATE = "2025-12-31"


def _day_start_ts(day: str) -> int:
    return int(datetime.fromisoformat(day[:10]).replace(tzinfo=timezone.utc).timestamp())


def load_scenarios(path: Path | None = None) -> list[dict]:
    return json.loads((path or SCENARIO_FILE).read_text())["scenarios"]


def _field_row(row: list | dict) -> dict:
    if isinstance(row, dict):
        return dict(row)
    code, num, pos, gap, comp, age, *rest = row
    out = {"driver_code": code, "driver_number": num, "position": pos, "gap_to_leader_ms": gap,
           "compound": comp, "tyre_age_laps": age}
    if rest:
        out["planned_stop_lap"] = rest[0]
    return out


def pit_loss_for(session_key: int) -> dict:
    """Measured green-flag pit loss for a race when it is well estimated
    (>= 3 stops), else the simulator's generic defaults."""
    path = config.RACES / str(session_key) / "pit_loss.json"
    if path.exists():
        m = json.loads(path.read_text())
        if m.get("use_measured"):
            return {"green_s": m["green_s"], "neutral_s": m["neutral_s"],
                    "basis": f"measured from {m['green_n']} green-flag stops in this race ({m['method']}); neutral = 0.606 x green"}
    return {**GENERIC_PIT_LOSS, "basis": "generic simulator defaults (fewer than 3 measurable green-flag stops in this race)"}


def materialise(scn: dict, lakehouse: Lakehouse | None = None) -> dict[str, Any]:
    """Return the scenario with a complete ``state`` and a ``context``
    (edition, decision instant) for point-in-time retrieval."""
    s = copy.deepcopy(scn)
    if s.get("kind") == "real":
        src = s["source"]
        lh = lakehouse or Lakehouse()
        meta = json.loads((config.RACES / str(src["session_key"]) / "meta.json").read_text())
        at = lh.decision_point(src["session_key"], src["driver_number"], src["lap"])
        d = lh.decision_state(at, src["driver_number"])
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
            for f in lh.field_at(at, exclude_driver=src["driver_number"]) if f["compound"] != "UNKNOWN"
        ]
        loss = pit_loss_for(src["session_key"])
        race_day = meta["date_start"][:10]
        state = {"total_laps": meta["total_laps"], "current_lap": src["lap"], "track_status": d["track_status"],
                 "track_wetness": 0.0, "car": car, "field": field, "session": f"{meta['year']} {meta['location']}",
                 "pit_loss_green_s": loss["green_s"], "pit_loss_neutral_s": loss["neutral_s"]}
        state.update(s.get("overrides", {}))
        s["state"] = state
        s["context"] = {
            "season": meta["year"], "edition": regulations.edition_for(race_day),
            # context documents are dated by day: anything dated on or after the
            # race day (including digests of this very race) is excluded
            "decision_ts_ms": at.ts_ms, "context_before_ts": _day_start_ts(race_day),
            "session_key": src["session_key"], "lap": src["lap"], "pit_loss_basis": loss["basis"],
        }
    else:
        st = s["state"]
        st["field"] = [_field_row(r) for r in st.get("field", [])]
        st.setdefault("track_status", "GREEN")
        st.setdefault("track_wetness", 0.0)
        day = datetime.fromisoformat(SYNTHETIC_DECISION_DATE).replace(tzinfo=timezone.utc)
        s["context"] = {"season": 2025, "edition": regulations.edition_for(SYNTHETIC_DECISION_DATE),
                        "decision_ts_ms": int(day.timestamp() * 1000), "context_before_ts": _day_start_ts(SYNTHETIC_DECISION_DATE),
                        "session_key": None, "lap": st["current_lap"], "pit_loss_basis": "generic simulator defaults"}
    s["state"]["race_control"] = s.get("race_control", "")
    s["state"]["edition"] = s["context"]["edition"]
    return s
