"""Deterministic FIA rule and physics guardrail.

Used by the verifier node (to decide whether the cyclic graph must revise a
proposal) and by the evaluation (to score compliance of any proposal, from
the agent or the single-pass baseline, with the same checks).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pitwall.regulations import DEFAULT_EDITION, article_ref

DRY = {"SOFT", "MEDIUM", "HARD"}
WET = {"INTERMEDIATE", "WET"}
ACTIONS = ["PIT_NOW_HARD", "PIT_NOW_MEDIUM", "PIT_NOW_SOFT", "PIT_NOW_INTER", "STAY_OUT_EXTEND", "OPPOSITE_AHEAD"]
ACTION_COMPOUND = {"PIT_NOW_HARD": "HARD", "PIT_NOW_MEDIUM": "MEDIUM", "PIT_NOW_SOFT": "SOFT", "PIT_NOW_INTER": "INTERMEDIATE"}


def canonical(c: str | None) -> str:
    c = (c or "").upper().strip()
    return {"S": "SOFT", "M": "MEDIUM", "H": "HARD", "I": "INTERMEDIATE", "INTER": "INTERMEDIATE", "W": "WET"}.get(c, c)


@dataclass
class Violation:
    code: str
    message: str
    article: str | None = None
    severity: str = "hard"

    def as_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "article": self.article, "severity": self.severity}


@dataclass
class Constraints:
    """Legal and physical constraints derived for one scenario."""

    current_lap: int
    total_laps: int
    compound: str
    compounds_used: list[str]
    penalty_s: float = 0.0
    track_status: str = "GREEN"
    pit_entry_closed: bool = False
    track_wetness: float = 0.0
    forbid_compounds: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    edition: str = DEFAULT_EDITION

    def art(self, clause: str) -> str:
        """Article reference of a clause in the edition in force (e.g. 30.5(n) in 2023)."""
        return article_ref(self.edition, clause)

    @property
    def wet_used(self) -> bool:
        return any(canonical(c) in WET for c in self.compounds_used)

    @property
    def dry_used(self) -> set[str]:
        return {canonical(c) for c in self.compounds_used if canonical(c) in DRY}

    @property
    def needs_second_dry(self) -> bool:
        return not self.wet_used and len(self.dry_used) < 2

    def describe(self) -> list[str]:
        tc = self.art("two_compound")
        out = [f"Regulations: FIA Sporting Regulations edition {self.edition} (the issue in force on the race date)."]
        if self.needs_second_dry:
            out.append(f"Art. {tc}: only {'/'.join(sorted(self.dry_used)) or 'one'} dry spec used so far, a DIFFERENT dry compound "
                       f"must still be fitted before the flag (no intermediates used).")
        elif self.wet_used:
            out.append(f"Art. {tc}: intermediates/wets used, so the two dry-compound requirement no longer applies.")
        else:
            out.append(f"Art. {tc}: satisfied ({'+'.join(sorted(self.dry_used))} used).")
        if self.penalty_s:
            out.append(f"Art. {self.art('penalty_at_neutral_stop')} / {self.art('penalty_hold')}: pending {self.penalty_s:.0f}s time penalty. "
                       f"At the next stop the car must be stationary for {self.penalty_s:.0f}s before anyone touches it "
                       f"(+{self.penalty_s:.1f}s to the stop); with no further stop it is added to race time.")
        if self.pit_entry_closed:
            out.append(f"Art. {self.art('pit_entry_closed')}: PIT ENTRY CLOSED. Entering for tyres is penalised; no stop this lap.")
        if self.track_status in ("SC", "VSC"):
            clause = "sc_pit_entry" if self.track_status == "SC" else "vsc_pit_entry"
            out.append(f"Art. {self.art(clause)}: {self.track_status} deployed, cars may enter the pits to change tyres; "
                       "the field is slowed, so a stop costs less time than under green flag.")
        if self.track_status == "RED":
            out.append(f"Art. {self.art('red_flag_tyres')}: race suspended, wheels and tyres may be changed in the pit lane at no time cost.")
        if self.track_wetness >= 0.35:
            out.append(f"Track wetness {self.track_wetness:.2f}: slicks are not viable, intermediates/wets required.")
        elif self.track_wetness <= 0.15 and canonical(self.compound) in WET:
            out.append(f"Track wetness {self.track_wetness:.2f}: drying track, intermediates overheat, slicks are faster.")
        if self.forbid_compounds:
            out.append(f"No sets left of: {', '.join(self.forbid_compounds)}.")
        return out + self.notes


def plan_of(proposal: dict, c: Constraints) -> tuple[int, str]:
    """(pit_lap, compound) of a proposal; pit_lap 0 means no further stop."""
    action = (proposal.get("recommended_action") or "").upper()
    comp = canonical(proposal.get("target_compound"))
    pit_lap = int(proposal.get("pit_lap") or 0)
    if action in ACTION_COMPOUND:
        return c.current_lap, ACTION_COMPOUND[action]
    if comp in ("", "NONE"):
        pit_lap = 0
    return pit_lap, comp


def check(proposal: dict, c: Constraints, traffic: dict | None = None) -> list[Violation]:
    """All hard violations of a proposal. Empty list means compliant."""
    v: list[Violation] = []
    action = (proposal.get("recommended_action") or "").upper()
    if action not in ACTIONS:
        v.append(Violation("UNKNOWN_ACTION", f"recommended_action must be one of {ACTIONS}, got {action!r}"))
        return v
    pit_lap, comp = plan_of(proposal, c)
    stops = pit_lap > 0
    if action in ACTION_COMPOUND and canonical(proposal.get("target_compound")) not in ("", ACTION_COMPOUND[action]):
        v.append(Violation("INCONSISTENT", f"{action} must fit {ACTION_COMPOUND[action]}, proposal says {proposal.get('target_compound')}", severity="hard"))
    if stops and pit_lap < c.current_lap:
        v.append(Violation("PAST_LAP", f"pit lap {pit_lap} is before the current lap {c.current_lap}"))
    if stops and pit_lap > c.total_laps:
        v.append(Violation("PAST_FLAG", f"pit lap {pit_lap} is after the last lap {c.total_laps}"))
    # Art. 30.5(m): two dry specifications unless intermediates/wets were used
    used = set(c.compounds_used) | ({comp} if stops else set())
    wet = any(canonical(x) in WET for x in used)
    dry = {canonical(x) for x in used if canonical(x) in DRY}
    if not wet and len(dry) < 2:
        v.append(Violation("TWO_COMPOUND", f"finishing a dry race on {'+'.join(sorted(dry)) or 'one compound'} only violates the "
                           f"mandatory use of two different dry-weather specifications (disqualification)", c.art("two_compound")))
    if stops and pit_lap == c.current_lap and c.pit_entry_closed:
        v.append(Violation("PIT_ENTRY_CLOSED", "the pit entry is closed this lap; entering the pit lane for tyres is penalised",
                           c.art("pit_entry_closed")))
    if stops and comp in [canonical(x) for x in c.forbid_compounds]:
        v.append(Violation("NO_SETS", f"no {comp} sets available"))
    if stops and c.penalty_s > 0:
        held = float(proposal.get("penalty_hold_s") or 0)
        if abs(held - c.penalty_s) > 0.5:
            v.append(Violation("PENALTY_HOLD", f"the pending {c.penalty_s:.0f}s penalty adds a {c.penalty_s:.0f}s stationary hold "
                               f"to this stop (no work on the car until it has elapsed); proposal accounts {held:.0f}s", c.art("penalty_hold")))
    # physics: tyre choice vs track wetness at the stop
    if stops and comp in DRY and c.track_wetness >= 0.35:
        v.append(Violation("SLICKS_ON_WET", f"slicks on a wet track (wetness {c.track_wetness:.2f})", severity="hard"))
    if stops and comp in WET and c.track_wetness <= 0.12 and pit_lap == c.current_lap:
        v.append(Violation("INTERS_ON_DRY", f"intermediates on a dry track (wetness {c.track_wetness:.2f})", severity="hard"))
    if canonical(c.compound) in DRY and c.track_wetness >= 0.35 and (not stops or pit_lap > c.current_lap + 1):
        v.append(Violation("SLICKS_ON_WET", f"staying out on {c.compound} slicks with track wetness {c.track_wetness:.2f}"))
    if traffic and stops and traffic.get("in_drs_train") and traffic.get("suggested_clear_air_pit_laps"):
        v.append(Violation("DRS_TRAIN", f"pitting on lap {pit_lap} rejoins in a DRS train ({', '.join(traffic.get('drs_train', []))}); "
                           f"clear-air laps: {traffic['suggested_clear_air_pit_laps']}", severity="hard"))
    return v


def feedback(violations: list[Violation]) -> str:
    return "\n".join(f"- [{x.code}]{' Art. ' + x.article if x.article else ''}: {x.message}" for x in violations)


def constraints_from(scn: dict[str, Any]) -> Constraints:
    car = scn["car"]
    return Constraints(
        current_lap=int(scn["current_lap"]), total_laps=int(scn["total_laps"]), compound=canonical(car["compound"]),
        compounds_used=[canonical(x) for x in (car.get("compounds_used") or [car["compound"]])],
        penalty_s=float(car.get("penalty_s") or 0), track_status=(scn.get("track_status") or "GREEN").upper(),
        pit_entry_closed=bool(scn.get("pit_entry_closed")), track_wetness=float(scn.get("track_wetness") or 0),
        forbid_compounds=[canonical(x) for x in scn.get("forbid_compounds", [])],
        edition=scn.get("edition") or DEFAULT_EDITION,
    )
