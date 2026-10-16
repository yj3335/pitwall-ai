"""Scenario integrity: race data, decision-time state, ground-truth consistency."""
from __future__ import annotations

import json

import pytest

from pitwall import config
from pitwall.agents import rules
from pitwall.eval.scenarios import load_scenarios, materialise
from pitwall.olap import Lakehouse

SCN = load_scenarios()


@pytest.fixture(scope="module")
def materialised():
    lh = Lakehouse()
    return {s["id"]: materialise(s, lh) for s in SCN}


def test_race_lengths_come_from_the_chequered_flag():
    # OpenF1 also records the podium finishers' cool-down lap (a "lap 63" at Singapore 2023)
    for key, laps in {9165: 62, 9558: 52, 9636: 69}.items():
        assert json.loads((config.RACES / str(key) / "meta.json").read_text())["total_laps"] == laps


def proposal_for(action: str, state: dict) -> dict:
    c = rules.constraints_from(state)
    pit = action.startswith("PIT_NOW")
    comp = rules.ACTION_COMPOUND.get(action, "NONE")
    if action in ("STAY_OUT_EXTEND", "OPPOSITE_AHEAD") and c.needs_second_dry:
        # a deferred stop onto a different dry compound
        comp = "HARD" if "HARD" not in c.dry_used else "MEDIUM"
        return {"recommended_action": action, "target_compound": comp, "pit_lap": min(c.total_laps - 1, c.current_lap + 3),
                "penalty_hold_s": c.penalty_s}
    return {"recommended_action": action, "target_compound": comp, "pit_lap": c.current_lap if pit else 0,
            "penalty_hold_s": c.penalty_s if pit else 0}


def test_forbidden_means_illegal_and_acceptable_means_legal(materialised):
    """Ground truth must be consistent with the regulations: every forbidden
    call breaks a rule, every acceptable call is legal when the pending
    penalty hold is accounted for."""
    for sid, m in materialised.items():
        st, gt = m["state"], m["ground_truth"]
        c = rules.constraints_from(st)
        for action in gt.get("forbidden", []):
            v = [x.code for x in rules.check(proposal_for(action, st), c)]
            assert v, f"{sid}: forbidden {action} is actually legal"
        for action in gt["acceptable"]:
            v = [x for x in rules.check(proposal_for(action, st), c) if x.code not in ("DRS_TRAIN",)]
            assert not v, f"{sid}: acceptable {action} breaks {[x.code for x in v]}"
        assert not set(gt.get("forbidden", [])) & set(gt["acceptable"]), sid


def test_real_scenarios_use_decision_time_state(materialised):
    expect = {  # position and status at the decision instant (before the final sector of the decision lap)
        "GP01": ("NOR", 4, "SC", "MEDIUM", ["MEDIUM"]),
        "GP02": ("HAM", 4, "VSC", "HARD", ["HARD", "MEDIUM"]),
        "GP03": ("SAI", 1, "VSC", "HARD", ["HARD", "MEDIUM"]),
        "GP04": ("HAM", 3, "GREEN", "MEDIUM", ["MEDIUM"]),
        "GP05": ("HAM", 2, "GREEN", "INTERMEDIATE", ["INTERMEDIATE", "MEDIUM"]),
        "GP06": ("VER", 2, "RED", "INTERMEDIATE", ["INTERMEDIATE"]),
        "GP07": ("NOR", 2, "VSC", "INTERMEDIATE", ["INTERMEDIATE"]),
    }
    for sid, (code, pos, status, comp, used) in expect.items():
        st = materialised[sid]["state"]
        car = st["car"]
        assert (car["driver_code"], car["position"], st["track_status"], car["compound"], car["compounds_used"]) == \
            (code, pos, status, comp, used), sid
        ctx = materialised[sid]["context"]
        assert ctx["edition"] == {9165: "2023-i6", 9558: "2024-i6", 9636: "2024-i7"}[ctx["session_key"]]


def test_measured_pit_loss_only_when_well_estimated(materialised):
    assert materialised["GP02"]["state"]["pit_loss_green_s"] == 30.5  # Singapore: 3 measured green-flag stops
    assert materialised["GP04"]["state"]["pit_loss_green_s"] == 24.2  # Silverstone: 7 stops
    assert materialised["GP06"]["state"]["pit_loss_green_s"] == 21.8  # Sao Paulo: 1 stop, generic default
    assert "generic" in materialised["GP06"]["context"]["pit_loss_basis"]


def test_questions_match_the_data(materialised):
    """The free-text question of each real scenario must agree with the state."""
    for sid in ("GP01", "GP02", "GP03", "GP04", "GP05", "GP06", "GP07"):
        m = materialised[sid]
        car = m["state"]["car"]
        q = m["question"]
        assert f"P{car['position']}" in q or (car["position"] == 1 and " leads " in q), (sid, car["position"], q)
        assert f"{car['tyre_age_laps']}-lap-old" in q or f"aged {car['tyre_age_laps']}" in q, (sid, q)


def test_rivals_carry_their_own_decision_time_wear(materialised):
    """Rivals in real scenarios get the measured fuel-corrected slope of the set
    they are on, from laps completed before the decision lap (GP03: Singapore
    2023, lap 44), instead of the compound default."""
    from pitwall.olap import Lakehouse

    field = {f["driver_code"]: f for f in materialised["GP03"]["state"]["field"]}
    expected = dict(Lakehouse().query(
        """SELECT driver_code, deg_slope_ms_per_lap FROM laps
           WHERE session_key = 9165 AND lap_number < 44 AND deg_slope_ms_per_lap IS NOT NULL
           QUALIFY row_number() OVER (PARTITION BY driver_number ORDER BY lap_number DESC) = 1""").rows)
    for code in ("RUS", "NOR", "HAM", "LEC"):
        assert field[code]["deg_slope_ms_per_lap"] == pytest.approx(expected[code])
    assert field["RUS"]["deg_slope_ms_per_lap"] > 100  # Russell's hards were going off behind Sainz
