"""Regulation editions: the issue in force on each race date, edition-correct
article references resolved from the indexed text."""
from __future__ import annotations

import pytest

from pitwall import regulations as r


def test_edition_in_force_on_each_race_date():
    assert r.edition_for("2023-09-17") == "2023-i6"   # Singapore GP: Issue 6 of 31 Aug 2023
    assert r.edition_for("2024-07-07") == "2024-i6"   # British GP: Issue 6 of 30 Apr 2024
    assert r.edition_for("2024-11-03") == "2024-i7"   # Sao Paulo GP: Issue 7 of 31 Jul 2024
    assert r.edition_for("2025-12-31") == "2025-i5"   # synthetic scenarios
    with pytest.raises(LookupError):
        r.edition_for("2026-03-08")  # 2026 is a new formula and is not indexed by default


@pytest.mark.parametrize("edition,clause,expected", [
    ("2023-i6", "two_compound", "30.5(n)"),
    ("2024-i6", "two_compound", "30.5(m)"),
    ("2025-i5", "two_compound", "30.5(m)"),
    ("2023-i6", "sc_pit_entry", "55.11"),
    ("2024-i6", "sc_pit_entry", "55.11"),
    ("2024-i7", "sc_pit_entry", "55.12"),
    ("2025-i5", "sc_pit_entry", "55.12"),
    ("2023-i6", "penalty_hold", "54.4(c)"),
    ("2025-i5", "penalty_hold", "54.4(c)"),
    ("2025-i5", "vsc_pit_entry", "56.4"),
    ("2025-i5", "pit_entry_closed", "34.15"),
    ("2025-i5", "penalty_at_neutral_stop", "54.3"),
])
def test_article_references_follow_the_edition(edition, clause, expected):
    assert r.article_ref(edition, clause) == expected


def test_every_clause_resolves_in_every_edition():
    for edition in r.editions():
        for clause in r.CLAUSES:
            assert r.article_ref(edition, clause)
    assert "2023 Sporting Regulations, Issue 6" in r.cite("2023-i6", "two_compound")
