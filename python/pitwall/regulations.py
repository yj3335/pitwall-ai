"""Regulation editions and point-in-time article references.

A decision is judged against the FIA Sporting Regulations issue in force on
the race date. Article numbers and paragraph letters move between issues
(the two dry-compound rule is 30.5(n) in 2023 and 30.5(m) in 2024-2025; the
SC pit-entry rule is 55.11 in 2023-2024 and 55.12 in 2025), so citations are
resolved from the indexed text of the right edition by matching the clause's
wording, never from a hand-maintained table.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path

from pitwall import config

# Synthetic scenarios are set in the last season whose cars the simulator
# models (2022-2025 ground-effect cars with DRS). 2026 is a new formula.
DEFAULT_EDITION = "2025-i5"

# clause -> (article prefix, wording that identifies it)
CLAUSES: dict[str, tuple[str, str]] = {
    "two_compound": ("30.5", r"used intermediate or wet-?weather tyres during the race"),
    "penalty_hold": ("54.4", r"may not be worked on until the car has been stationary"),
    "penalty_at_neutral_stop": ("54.3", r"includes any stop the driver makes whilst a VSC or safety car"),
    "sc_pit_entry": ("55.", r"no car may enter the pits whilst the safety car is deployed unless"),
    "vsc_pit_entry": ("56.", r"no car may enter the pits whilst the VSC procedure is in use unless"),
    "red_flag_tyres": ("57.4", r"Changing wheels and tyres"),
    "pit_entry_closed": ("34.", r"pit entry to be closed"),
}


# sentences that apply to several paragraphs ("In both of the above cases ...")
WHOLE_ARTICLE = {"penalty_at_neutral_stop"}


@dataclass(frozen=True)
class Edition:
    key: str
    season: int
    issue: int
    effective_from: date
    effective_to: date
    title: str


def _norm(text: str) -> str:
    text = re.sub(r"(\w)-\s+(\w)", r"\1-\2", text)
    return re.sub(r"\s+", " ", text)


@lru_cache(maxsize=1)
def editions(directory: Path | None = None) -> dict[str, Edition]:
    out: dict[str, Edition] = {}
    for p in sorted(Path(directory or config.FIA_DIR).glob("sporting_regulations_*.jsonl")):
        first = json.loads(p.open().readline())
        out[first["edition"]] = Edition(first["edition"], first["season"], first["issue"],
                                        date.fromisoformat(first["effective_from"]),
                                        date.fromisoformat(first["effective_to"]), first["source"])
    return out


def edition_for(day: date | str) -> str:
    """The edition in force on a date (raises if none is indexed)."""
    d = date.fromisoformat(day[:10]) if isinstance(day, str) else day
    for e in editions().values():
        if e.effective_from <= d < e.effective_to:
            return e.key
    raise LookupError(f"no indexed Sporting Regulations edition in force on {d}")


@lru_cache(maxsize=None)
def _chunks(edition: str) -> tuple[dict, ...]:
    path = config.FIA_DIR / f"sporting_regulations_{edition}.jsonl"
    return tuple(json.loads(line) for line in path.open())


@lru_cache(maxsize=None)
def article_ref(edition: str, clause: str) -> str:
    """Citation such as "30.5(n)" for a clause in a given edition."""
    prefix, pattern = CLAUSES[clause]
    rx = re.compile(pattern, re.IGNORECASE)
    for c in _chunks(edition):
        if c["article"].startswith(prefix) and rx.search(_norm(c["text"])):
            para = c.get("paragraphs")
            if clause in WHOLE_ARTICLE:
                return c["article"]
            return f"{c['article']}({para})" if para and len(para) == 1 else c["article"]
    raise LookupError(f"clause {clause!r} not found in edition {edition}")


def cite(edition: str, clause: str) -> str:
    e = editions()[edition]
    return f"Art. {article_ref(edition, clause)} ({e.season} Sporting Regulations, Issue {e.issue})"
