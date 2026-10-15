#!/usr/bin/env python3
"""Measure green-flag pit-stop time loss per race from the Parquet lakehouse.

Loss of a stop = change in the stopping car's gap to the leader from the end
of the lap before the in-lap to the end of the out-lap, minus the median
change for cars that did not stop in that window (field-relative, so pace
drift cancels). Reference cars whose gap jumps by more than 5 s across the
window are dropped: OpenF1 gap data glitches under neutralisations.

Only green-flag stops give a usable estimate. Under SC/VSC the gap data is too
noisy in these races (Singapore 2023 VSC IQR 23-42 s), so the neutralised
loss is written as a model assumption: 0.606 x green, the 13.2 s / 21.8 s
ratio of the generic defaults. The only clean readings (Hamilton 19.4 s and
Russell 16.7 s relative to the non-stopping leader under the 2023 Singapore
VSC) are consistent with it.

Writes data/races/<session>/pit_loss.json. Needs `make lakehouse` first.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "python"))

from pitwall.olap import Lakehouse  # noqa: E402

NEUTRAL_RATIO = 13.2 / 21.8

SQL = """
WITH l AS (SELECT session_key, driver_number, driver_code, lap_number, compound, tyre_age_laps,
                  gap_to_leader_ms g, track_status, position FROM laps),
chg AS (SELECT a.*, (b.compound <> a.compound OR b.tyre_age_laps < a.tyre_age_laps) AS stop_after,
               b.track_status AS out_status
        FROM l a JOIN l b ON a.session_key = b.session_key AND a.driver_number = b.driver_number
                         AND b.lap_number = a.lap_number + 1),
win AS (SELECT s.session_key, s.driver_code, s.lap_number AS in_lap,
          (SELECT o.g FROM l o WHERE o.session_key = s.session_key AND o.driver_number = s.driver_number AND o.lap_number = s.lap_number + 1)
        - (SELECT p.g FROM l p WHERE p.session_key = s.session_key AND p.driver_number = s.driver_number AND p.lap_number = s.lap_number - 1) AS d_stop
        FROM chg s
        WHERE s.stop_after AND s.position > 1 AND s.track_status = 'GREEN' AND s.out_status = 'GREEN'),
ref AS (SELECT w.*,
          (SELECT median(o.g - p.g) FROM l o JOIN l p ON o.session_key = p.session_key AND o.driver_number = p.driver_number
                 AND o.lap_number = w.in_lap + 1 AND p.lap_number = w.in_lap - 1
           WHERE o.session_key = w.session_key AND o.position > 1 AND o.g > 0 AND p.g > 0 AND abs(o.g - p.g) < 5000
             AND o.driver_number NOT IN (SELECT driver_number FROM chg c WHERE c.session_key = w.session_key AND c.stop_after
                                         AND c.lap_number BETWEEN w.in_lap - 2 AND w.in_lap + 2)) AS d_ref
        FROM win w)
SELECT session_key, count(*) AS n,
       median((d_stop - d_ref) / 1000.0) AS loss_s,
       quantile_cont((d_stop - d_ref) / 1000.0, 0.25) AS p25,
       quantile_cont((d_stop - d_ref) / 1000.0, 0.75) AS p75,
       list(driver_code || ' L' || in_lap ORDER BY in_lap) AS stops
FROM ref WHERE d_stop IS NOT NULL AND d_ref IS NOT NULL AND d_stop BETWEEN 0 AND 60000
GROUP BY session_key ORDER BY session_key
"""


def main() -> None:
    lh = Lakehouse(ROOT / "data" / "lakehouse")
    for r in lh.query(SQL).records():
        green = round(r["loss_s"], 1)
        out = {
            "session_key": r["session_key"],
            "green_s": green,
            "green_n": r["n"],
            "green_iqr_s": [round(r["p25"], 1), round(r["p75"], 1)],
            "green_stops": r["stops"],
            # a single stop is not an estimate: consumers fall back to the generic default
            "use_measured": r["n"] >= 3,
            "neutral_s": round(green * NEUTRAL_RATIO, 1),
            "neutral_basis": "assumption: 0.606 x green (13.2/21.8); SC/VSC gap data too noisy to measure",
            "method": "field-relative gap change across in-lap + out-lap, green-flag stops only",
        }
        path = ROOT / "data" / "races" / str(r["session_key"]) / "pit_loss.json"
        path.write_text(json.dumps(out, indent=2) + "\n")
        print(f"{r['session_key']}: green {green}s (n={r['n']}, IQR {out['green_iqr_s']}), neutral {out['neutral_s']}s")


if __name__ == "__main__":
    main()
