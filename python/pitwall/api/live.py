"""Live state from the Go processor and live scenario construction.

A live scenario is the same point-in-time object as a replayed one: the
decision instant is the target car's latest snapshot, the field is every other
car's snapshot, and the regulations and pit loss are those of the race being
streamed.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import httpx

from pitwall import config, regulations
from pitwall.eval.scenarios import PACE_OFFSET, pit_loss_for

SLICKS = {"SOFT", "MEDIUM", "HARD"}


class ProcessorClient:
    """Async client for the processor's live HTTP API (:8090)."""

    def __init__(self, url: str | None = None, timeout: float = 5.0):
        self.url = (url or config.PROCESSOR_URL).rstrip("/")
        self._http = httpx.AsyncClient(timeout=timeout)

    async def get(self, path: str) -> Any:
        r = await self._http.get(f"{self.url}{path}")
        r.raise_for_status()
        return r.json()

    async def tower(self) -> dict:
        return await self.get("/v1/tower")

    async def triggers(self) -> list:
        return await self.get("/v1/triggers")

    async def race_control(self) -> list:
        return await self.get("/v1/race-control")

    async def publish_race_control(self, msg: dict) -> dict:
        r = await self._http.post(f"{self.url}/v1/race-control", json=msg)
        r.raise_for_status()
        return r.json()

    async def healthy(self) -> bool:
        try:
            r = await self._http.get(f"{self.url}/healthz", timeout=1.0)
            return r.status_code == 200
        except httpx.HTTPError:
            return False

    async def aclose(self) -> None:
        await self._http.aclose()


def _meta(session_key: int) -> dict | None:
    path = config.RACES / str(session_key) / "meta.json"
    return json.loads(path.read_text()) if path.exists() else None


def live_scenario(tower: dict, driver_number: int, *, total_laps: int | None = None,
                  track_wetness: float | None = None, question: str | None = None) -> dict:
    """Build a scenario for one car from the processor's tower snapshot."""
    drivers = tower.get("drivers") or []
    car = next((d for d in drivers if d["driver_number"] == driver_number), None)
    if car is None:
        raise LookupError(f"car {driver_number} is not in the live tower")
    session = car["session_key"]
    meta = _meta(session)
    total = total_laps or (meta or {}).get("total_laps")
    if not total:
        raise ValueError(f"total_laps unknown for session {session}; pass it explicitly")
    race_day = (meta or {}).get("date_start", datetime.fromtimestamp(car["ts_ms"] / 1000, timezone.utc).isoformat())[:10]
    deg = car.get("deg_slope_ms_per_lap")
    deg = deg if deg is not None and 0 <= deg <= 250 else None
    last = car.get("last_lap_ms") or 95_000
    base = last - PACE_OFFSET.get(car["compound"], 0) - (deg or 0) * car["tyre_age_laps"]
    # OpenF1 has no track-wetness sensor: without an explicit value, use the rain flag
    if track_wetness is None:
        track_wetness = 0.0 if not car.get("rainfall") else (0.5 if car["compound"] in SLICKS else 0.6)
        wet_basis = "default from the weather-station rain flag (no wetness sensor in the feed)"
    else:
        wet_basis = "supplied by the caller"
    field = [{"driver_code": d["driver_code"], "driver_number": d["driver_number"], "position": d["position"],
              "gap_to_leader_ms": d["gap_to_leader_ms"], "compound": d["compound"], "tyre_age_laps": d["tyre_age_laps"],
              **({"deg_slope_ms_per_lap": d["deg_slope_ms_per_lap"]}
                 if d.get("deg_slope_ms_per_lap") is not None and 0 <= d["deg_slope_ms_per_lap"] <= 250 else {})}
             for d in drivers
             if d["driver_number"] != driver_number and d["session_key"] == session and d["compound"] != "UNKNOWN"]
    loss = pit_loss_for(session)
    state = {
        "total_laps": int(total), "current_lap": car["lap_number"], "track_status": car.get("track_status") or "GREEN",
        "track_wetness": track_wetness, "pit_entry_closed": False,
        "car": {"driver_code": car["driver_code"], "driver_number": car["driver_number"], "position": car["position"],
                "compound": car["compound"], "tyre_age_laps": car["tyre_age_laps"], "deg_slope_ms_per_lap": deg,
                "compounds_used": [c for c in car.get("compounds_used") or [car["compound"]] if c != "UNKNOWN"],
                "gap_to_leader_ms": car["gap_to_leader_ms"], "base_lap_ms": round(max(70_000.0, base)), "penalty_s": 0},
        "field": field, "pit_loss_green_s": loss["green_s"], "pit_loss_neutral_s": loss["neutral_s"],
        "session": f"{meta['year']} {meta['location']}" if meta else f"session {session}",
    }
    edition = regulations.edition_for(race_day)
    state["edition"] = edition
    day_start = int(datetime.fromisoformat(race_day).replace(tzinfo=timezone.utc).timestamp())
    return {
        "id": f"LIVE-{session}-{driver_number}-L{car['lap_number']}",
        "kind": "live", "category": "live", "title": f"Live: {car['driver_code']} lap {car['lap_number']}",
        "question": question or f"{car['driver_code']} is P{car['position']} on {car['tyre_age_laps']}-lap-old "
                                f"{car['compound'].lower()} tyres on lap {car['lap_number']} of {total}. What is the call?",
        "source": {"session_key": session, "driver_number": driver_number, "lap": car["lap_number"]},
        "state": state,
        "context": {"season": int(race_day[:4]), "edition": edition, "decision_ts_ms": car["ts_ms"],
                    "context_before_ts": day_start, "session_key": session, "lap": car["lap_number"],
                    "pit_loss_basis": loss["basis"], "wetness_basis": wet_basis},
    }
