"""Parse FIA race-control messages into structured strategy events.

The patterns follow the verbatim message formats seen in OpenF1 race_control
data, e.g. "VIRTUAL SAFETY CAR DEPLOYED", "FIA STEWARDS: 5 SECOND TIME PENALTY
FOR CAR 63 (RUS) - CAUSING A COLLISION", "PIT ENTRY CLOSED", "RED FLAG".
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any

_PENALTY = re.compile(r"(\d+)\s+SECOND\s+(?:TIME\s+)?PENALTY\s+FOR\s+CAR\s+(\d+)(?:\s+\((\w{3})\))?(?:\s*-\s*(.+))?")
_STOP_GO = re.compile(r"(?:STOP\s*(?:AND|&)\s*GO|DRIVE\s*THROUGH)\s+PENALTY\s+FOR\s+CAR\s+(\d+)")
_CAR = re.compile(r"\bCARS?\s+(\d+)")


@dataclass
class RaceControlEvent:
    message: str
    kind: str
    track_status: str | None = None     # GREEN / SC / VSC / RED when the message changes it
    pit_entry_closed: bool | None = None
    penalty_s: float | None = None
    car_number: int | None = None
    driver_code: str | None = None
    reason: str | None = None
    cars: list[int] = field(default_factory=list)
    strategic: bool = False             # worth a strategy re-evaluation

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse(message: str) -> RaceControlEvent:
    m = " ".join((message or "").upper().split())
    cars = [int(x) for x in _CAR.findall(m)]
    ev = RaceControlEvent(message=m, kind="INFO", cars=cars)
    if p := _PENALTY.search(m):
        ev.kind, ev.penalty_s, ev.car_number = "TIME_PENALTY", float(p.group(1)), int(p.group(2))
        ev.driver_code, ev.reason, ev.strategic = p.group(3), (p.group(4) or "").strip() or None, True
    elif p := _STOP_GO.search(m):
        ev.kind, ev.car_number, ev.strategic = "DRIVE_THROUGH_OR_STOP_GO", int(p.group(1)), True
    elif "VIRTUAL SAFETY CAR DEPLOYED" in m or "VSC DEPLOYED" in m:
        ev.kind, ev.track_status, ev.strategic = "VSC_DEPLOYED", "VSC", True
    elif "VIRTUAL SAFETY CAR ENDING" in m or "VSC ENDING" in m:
        ev.kind, ev.track_status = "VSC_ENDING", "GREEN"
    elif "SAFETY CAR IN THIS LAP" in m:
        ev.kind, ev.track_status = "SC_IN_THIS_LAP", "SC"
    elif "SAFETY CAR DEPLOYED" in m:
        ev.kind, ev.track_status, ev.strategic = "SC_DEPLOYED", "SC", True
    elif re.search(r"\bRED FLAG\b", m):
        ev.kind, ev.track_status, ev.strategic = "RED_FLAG", "RED", True
    elif "CHEQUERED FLAG" in m:
        ev.kind = "CHEQUERED_FLAG"
    elif "PIT ENTRY CLOSED" in m:
        ev.kind, ev.pit_entry_closed, ev.strategic = "PIT_ENTRY_CLOSED", True, True
    elif "PIT ENTRY OPEN" in m:
        ev.kind, ev.pit_entry_closed = "PIT_ENTRY_OPEN", False
    elif m == "TRACK CLEAR" or "GREEN LIGHT" in m:
        ev.kind, ev.track_status = "TRACK_CLEAR", "GREEN"
    elif "RAIN" in m or "SLIPPERY" in m or "LOW GRIP" in m:
        ev.kind, ev.strategic = "WEATHER", True
    if "PIT ENTRY CLOSED" in m and ev.kind != "PIT_ENTRY_CLOSED":
        ev.pit_entry_closed = True  # e.g. "SAFETY CAR DEPLOYED. PIT ENTRY CLOSED"
    return ev


def apply_to_state(state: dict, ev: RaceControlEvent) -> dict:
    """Overlay a race-control event on a scenario state (copy)."""
    s = {**state, "car": dict(state["car"])}
    if ev.track_status:
        s["track_status"] = ev.track_status
    if ev.pit_entry_closed is not None:
        s["pit_entry_closed"] = ev.pit_entry_closed
    if ev.kind == "TIME_PENALTY" and ev.car_number == s["car"].get("driver_number"):
        s["car"]["penalty_s"] = float(s["car"].get("penalty_s") or 0) + float(ev.penalty_s or 0)
    s["race_control"] = ev.message
    return s
