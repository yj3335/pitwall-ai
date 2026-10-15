#!/usr/bin/env python3
"""Build the FIA regulations retrieval corpus in data/fia_regulations/.

A race may only be judged against the Sporting Regulations issue that was in
force on its date, never a later one. The corpus therefore holds one edition
per (season, issue) actually needed, each tagged with the window in which it
was in force:

  edition   document                                         in force           used for
  2023-i6   2023 F1 Sporting Regulations, Issue 6, 31 Aug 2023  31 Aug - 25 Oct 2023  2023 Singapore GP (17 Sep)
  2024-i6   2024 F1 Sporting Regulations, Issue 6, 30 Apr 2024  30 Apr - 31 Jul 2024  2024 British GP (7 Jul)
  2024-i7   2024 F1 Sporting Regulations, Issue 7, 31 Jul 2024  31 Jul - 31 Dec 2024  2024 Sao Paulo GP (3 Nov)
  2025-i5   2025 F1 Sporting Regulations, Issue 5, 30 Apr 2025  30 Apr - 31 Dec 2025  synthetic scenarios, retrieval benchmark

The 2026 regulations (Section B, Issue 04) are a new formula (active
aerodynamics replaces DRS, new power units) and are only built with
--include-2026; nothing in the agent or the simulator models 2026 cars.

Also written: verbatim race-control / steward notices from the OpenF1 sessions
in data/races/, dated and tagged with session and lap so retrieval can exclude
anything after a decision point. Precedent digests are hand-maintained in
data/fia_regulations/precedents.jsonl.

PDFs are cached in data/fia_regulations/.pdf/ (gitignored) and parsed with PyMuPDF.
Some issues are published as redlines; struck-through (deleted) text is detected
geometrically and removed, so only the wording in force is indexed.
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "fia_regulations"
PDF_DIR = OUT / ".pdf"

CLASSIC_SUB = re.compile(r"^\s?(\d{1,2})\.(\d{1,2})\s+(?=\S)")
# accented capitals matter: "60) POST SPRINT AND POST RACE PARC FERMÉ"
CLASSIC_ART = re.compile(r"^\s?(\d{1,2})\)\s+([A-ZÀ-Ý][A-ZÀ-Ý ,&\-/()']+?)\s*$")

EDITIONS = {
    "2023-i6": dict(season=2023, issue=6, effective_from="2023-08-31", effective_to="2023-10-25",
                    url="https://www.fia.com/sites/default/files/fia_2023_formula_1_sporting_regulations_-_issue_6_-_2023-08-31.pdf",
                    title="FIA 2023 Formula 1 Sporting Regulations, Issue 6, 31 August 2023"),
    "2024-i6": dict(season=2024, issue=6, effective_from="2024-04-30", effective_to="2024-07-31",
                    url="https://www.fia.com/sites/default/files/fia_2024_formula_1_sporting_regulations_-_issue_6_-_2024-04-30_v2.pdf",
                    title="FIA 2024 Formula 1 Sporting Regulations, Issue 6, 30 April 2024"),
    "2024-i7": dict(season=2024, issue=7, effective_from="2024-07-31", effective_to="2025-01-01",
                    url="https://www.fia.com/sites/default/files/fia_2024_formula_1_sporting_regulations_-_issue_7_-_2024-07-31.pdf",
                    title="FIA 2024 Formula 1 Sporting Regulations, Issue 7, 31 July 2024"),
    "2025-i5": dict(season=2025, issue=5, effective_from="2025-04-30", effective_to="2026-01-01",
                    url="https://api.fia.com/system/files/documents/fia_2025_formula_1_sporting_regulations_-_issue_5_-_2025-04-30.pdf",
                    title="FIA 2025 Formula 1 Sporting Regulations, Issue 5, 30 April 2025"),
}
EDITION_2026 = {
    "2026-i4": dict(season=2026, issue=4, effective_from="2026-01-01", effective_to="2027-01-01", sectioned=True,
                    url="https://www.fia.com/system/files/documents/fia_2026_f1_regulations_-_section_b_sporting_-_iss_04_-_2025-12-10_0.pdf",
                    title="FIA 2026 F1 Regulations Section B [Sporting], Issue 04, 10 December 2025"),
}
SECTIONED_SUB = re.compile(r"^\s?B(\d)\.(\d{1,2})\.(\d{1,2})\s+(?=\S)")
SECTIONED_ART = re.compile(r"^\s?B(\d)\.(\d{1,2})\s+([A-Z][^\d]{3,80}?)\s*$")

NOISE = [
    re.compile(r"^\s*20\d\d Formula 1 Sporting Regulations \d+/\d+"),
    re.compile(r"^\s*©20\d\d F[ée]d[ée]ration Internationale"),
    re.compile(r"^\s*SECTION B: SPORTING\s*$"),
    re.compile(r"^\s*(0|B|B\d+)\s*$"),
    re.compile(r"^\s*20\d\d F1 Regulations\s*:"),
    re.compile(r"^\s*\d{1,2} \w+ 20\d\d\s*$"),
    re.compile(r"^\s*Issue \d+\s*$"),
]
LETTER = re.compile(r"^\s?([a-z])\)\s")
LABEL_ONLY = re.compile(r"^\s*(B?\d{1,2}\.\d{1,2}(\.\d{1,2})?|[a-z]{1,4}\)|\d{1,2}\))\s*$")
MAX_CHARS = 1400
SOLO_CHARS = 350


def ts(date: str) -> int:
    return int(datetime.fromisoformat(date.replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp()) \
        if len(date) == 10 else int(datetime.fromisoformat(date.replace("Z", "+00:00")).timestamp())


def clean(text: str) -> str:
    text = text.replace("ﬁ", "fi").replace("ﬂ", "fl").replace("’", "'").replace("“", '"').replace("”", '"')
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"(\w) -(\w)", r"\1-\2", text)  # "re -join" -> "re-join"
    return text


STRUCK: dict[str, int] = {}


def _strike_segments(page) -> list[tuple[float, float, float]]:
    """Thin horizontal strokes (x0, x1, y) on a page: FIA redline issues draw
    deleted text with a strike-through rectangle across the glyphs."""
    segs = []
    for d in page.get_drawings():
        for it in d["items"]:
            if it[0] == "re" and it[1].height < 2.0 and it[1].width > 1.0:
                segs.append((it[1].x0, it[1].x1, (it[1].y0 + it[1].y1) / 2))
            elif it[0] == "l" and abs(it[1].y - it[2].y) < 0.6:
                segs.append((min(it[1].x, it[2].x), max(it[1].x, it[2].x), it[1].y))
    return segs


def _struck(bbox, segs) -> bool:
    x0, y0, x1, y1 = bbox
    h, w = y1 - y0, max(x1 - x0, 0.1)
    for sx0, sx1, sy in segs:
        # mid-height only, so underlines are not mistaken for deletions
        if y0 + 0.3 * h <= sy <= y0 + 0.75 * h and min(x1, sx1) - max(x0, sx0) >= 0.5 * w:
            return True
    return False


def extract(edition: str, meta: dict) -> list[str]:
    """Text lines of the regulation PDF with struck-through (deleted) text removed."""
    import pymupdf  # imported lazily: only needed when rebuilding the corpus

    PDF_DIR.mkdir(parents=True, exist_ok=True)
    pdf = PDF_DIR / f"sporting_regulations_{edition}.pdf"
    if not pdf.exists():
        req = urllib.request.Request(meta["url"], headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=120) as r:
            pdf.write_bytes(r.read())
    lines, struck = [], 0
    for page in pymupdf.open(str(pdf)):
        segs = _strike_segments(page)
        chars = []
        for block in page.get_text("rawdict")["blocks"]:
            for line in block.get("lines", []):
                for span in line["spans"]:
                    for c in span["chars"]:
                        if segs and _struck(c["bbox"], segs):
                            struck += 1
                        else:
                            chars.append(c)
        # PyMuPDF puts paragraph labels ("m)", "30.5") in their own blocks:
        # rebuild visual lines by grouping characters on the same baseline
        rows: list[list[dict]] = []
        for c in sorted(chars, key=lambda c: ((c["bbox"][1] + c["bbox"][3]) / 2, c["bbox"][0])):
            yc = (c["bbox"][1] + c["bbox"][3]) / 2
            if rows and abs(rows[-1][0]["_y"] - yc) <= 2.0:
                rows[-1].append(c)
            else:
                c["_y"] = yc
                rows.append([c])
                continue
            c["_y"] = rows[-1][0]["_y"]
        page_lines = []
        for row in rows:
            row.sort(key=lambda c: c["bbox"][0])
            out, prev = [], None
            for c in row:
                if prev is not None:
                    gap = c["bbox"][0] - prev["bbox"][2]
                    if gap > 0.25 * max(1.0, prev["bbox"][3] - prev["bbox"][1]) and prev["c"] != " " and c["c"] != " ":
                        out.append(" ")
                out.append(c["c"])
                prev = c
            ln = "".join(out)
            if not ln.strip() or any(n.search(ln) for n in NOISE):
                continue
            page_lines.append(clean(ln).rstrip())
        # a number or letter label alone on its line ("58.11", "m)") belongs to
        # the text on the next line
        merged: list[str] = []
        for ln in page_lines:
            if merged and LABEL_ONLY.match(merged[-1]):
                merged[-1] = merged[-1].strip() + " " + ln.strip()
            else:
                merged.append(ln)
        lines.extend(merged)
    STRUCK[edition] = struck
    return lines


def split_paragraphs(body: list[str]) -> list[tuple[str, str]]:
    """Split a long sub-article at lettered paragraphs, packing them to MAX_CHARS."""
    text = "\n".join(body).strip()
    if len(text) <= MAX_CHARS:
        return [("", text)]
    parts: list[tuple[str, list[str]]] = [("", [])]
    expected = "a"
    for ln in body:
        m = LETTER.match(ln)
        # only the next letter in sequence opens a paragraph, so roman-numeral
        # sub-items such as "i)", "v)", "x)" stay inside their parent paragraph
        if m and m.group(1) in (expected, "a"):
            parts.append((m.group(1), [ln]))
            expected = chr(ord(m.group(1)) + 1)
        else:
            parts[-1][1].append(ln)
    # a substantial paragraph (>= SOLO_CHARS) is always its own chunk so that a
    # citation such as 30.5(m) maps to exactly one chunk; short neighbouring
    # paragraphs are packed together up to MAX_CHARS
    packed: list[tuple[str, str]] = []
    cur_letters, cur = [], ""

    def flush_small():
        nonlocal cur_letters, cur
        if cur:
            packed.append(("".join(cur_letters), cur))
        cur_letters, cur = [], ""

    for letter, lines in parts:
        chunk = "\n".join(lines).strip()
        if not chunk:
            continue
        if len(chunk) >= SOLO_CHARS and letter:
            flush_small()
            packed.append((letter, chunk))
            continue
        if cur and len(cur) + len(chunk) > MAX_CHARS:
            flush_small()
        cur_letters.append(letter)
        cur = (cur + "\n" + chunk).strip()
    flush_small()
    # sub-articles without lettered paragraphs (tables, long lists) are cut at
    # line boundaries so no chunk is much larger than MAX_CHARS
    out: list[tuple[str, str]] = []
    for letters, text in packed:
        if len(text) <= MAX_CHARS * 1.5:
            out.append((letters, text))
            continue
        piece, n = "", 0
        for ln in text.splitlines():
            if piece and len(piece) + len(ln) > MAX_CHARS:
                n += 1
                out.append((f"{letters}p{n}", piece.strip()))
                piece = ""
            piece += ln + "\n"
        if piece.strip():
            n += 1
            out.append((f"{letters}p{n}", piece.strip()))
    return out


def build_edition(edition: str, meta: dict) -> list[dict]:
    sectioned = meta.get("sectioned", False)
    sub_re, art_re = (SECTIONED_SUB, SECTIONED_ART) if sectioned else (CLASSIC_SUB, CLASSIC_ART)
    lines = extract(edition, meta)
    # skip the table of contents: the body starts at the first full-length
    # "1.1" / "B1.1.1" paragraph (TOC entries are short "number title page" rows)
    first = "B1.1.1 " if sectioned else "1.1 "
    start = next(i for i, ln in enumerate(lines) if ln.strip().startswith(first) and len(ln.strip()) > 40)
    if not sectioned:  # include the "1) REGULATIONS" heading just above it
        start = next((j for j in range(start - 1, max(0, start - 4), -1) if art_re.match(lines[j])), start)
    articles: dict[str, str] = {}
    chunks: list[dict] = []
    seen: set[str] = set()
    cur_num, cur_tuple, cur_lines = None, (0,), []
    article_title = ""
    common = {"season": meta["season"], "edition": edition, "issue": meta["issue"],
              "effective_from": meta["effective_from"], "effective_to": meta["effective_to"],
              "source": meta["title"], "url": meta["url"]}

    def flush():
        if not cur_num or not cur_lines:
            return
        head = cur_lines[0]
        sub_title = head.split(None, 1)[1] if " " in head.strip() else ""
        sub_title = sub_title if len(sub_title) < 70 and not sub_title.rstrip().endswith((".", ",", ":")) else ""
        pieces = split_paragraphs(cur_lines)
        for letters, text in pieces:
            suffix = f"-{letters}" if letters and len(pieces) > 1 else ""
            cid = f"{edition}-{cur_num}{suffix}"
            while cid in seen:
                cid += "+"
            seen.add(cid)
            chunks.append({
                "id": cid, "kind": "regulation", "article": cur_num, "paragraphs": letters or None,
                "article_title": article_title, "title": sub_title.strip(),
                "text": text if text.startswith(cur_num) else f"{cur_num} {text}", **common,
            })

    cur_art = 0
    for ln in lines[start:]:
        a = art_re.match(ln)
        if a and not sectioned and cur_art < int(a.group(1)) <= cur_art + 3:
            # article headings ("54) INCIDENTS DURING ...") increase monotonically;
            # flush first so the buffered last sub-article keeps its own title
            flush()
            cur_num, cur_lines = None, []
            cur_art = int(a.group(1))
            article_title = a.group(2).strip().title()
            continue
        if a and sectioned:
            # 2026 headings like "B5.12 Virtual Safety Car (VSC)" often carry body text directly
            num_tuple = (int(a.group(1)), int(a.group(2)), 0)
            if num_tuple > cur_tuple and (num_tuple[0] - cur_tuple[0]) <= 2:
                flush()
                cur_tuple, cur_num = num_tuple, f"B{a.group(1)}.{a.group(2)}"
                article_title = a.group(3).strip()
                cur_lines = [ln.strip()]
                continue
        m = sub_re.match(ln)
        if m:
            num_tuple = tuple(int(g) for g in m.groups())
            # numbers must increase monotonically; cross-references at line
            # starts ("B3.4.2 or B3.4.3, only ...") would otherwise split chunks
            if num_tuple > cur_tuple and (num_tuple[0] - cur_tuple[0]) <= 2:
                flush()
                cur_tuple = num_tuple
                cur_num = ".".join(str(g) for g in num_tuple)
                if sectioned:
                    cur_num = "B" + cur_num
                    article_title = articles.get(f"B{num_tuple[0]}.{num_tuple[1]}", article_title)
                cur_lines = [ln.strip()]
                continue
        if cur_num:
            cur_lines.append(ln)
    flush()
    return [c for c in chunks if len(c["text"]) > 40]


def steward_notices() -> list[dict]:
    """Verbatim FIA steward and race-control messages from the OpenF1 sessions,
    with the session, lap and timestamp needed for point-in-time filtering."""
    docs = []
    keep = re.compile(r"PENALTY|UNSAFE|SAFETY CAR|VSC|RED FLAG|PIT ENTRY|PIT EXIT CLOSED|REPRIMAND|DISQUALIF|BLACK AND WHITE|TRACK LIMITS")
    for meta_path in sorted((ROOT / "data" / "races").glob("*/meta.json")):
        meta = json.loads(meta_path.read_text())
        with gzip.open(meta_path.parent / "race_control.json.gz", "rt") as f:
            rc = json.load(f)
        for i, m in enumerate(rc):
            msg = m.get("message") or ""
            if not keep.search(msg):
                continue
            docs.append({
                "id": f"notice-{meta['session_key']}-{i}",
                "kind": "steward_notice", "season": meta["year"], "edition": None,
                "article": None, "paragraphs": None,
                "article_title": "Race Control / Stewards notice",
                "title": f"{meta['year']} {meta['location']} GP, lap {m.get('lap_number')}",
                "text": f"{meta['year']} {meta['location']} Grand Prix, lap {m.get('lap_number')}: {msg}",
                "session_key": meta["session_key"], "lap": m.get("lap_number"),
                "date": m["date"], "date_ts": ts(m["date"]),
                "source": f"OpenF1 race_control, session_key={meta['session_key']}",
                "url": f"https://api.openf1.org/v1/race_control?session_key={meta['session_key']}",
            })
    return docs


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--include-2026", action="store_true", help="also build the 2026 (new formula) Section B regulations")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    editions = dict(EDITIONS, **(EDITION_2026 if args.include_2026 else {}))
    for stale in OUT.glob("sporting_regulations_*.jsonl"):
        if stale.stem.removeprefix("sporting_regulations_") not in editions:
            stale.unlink()
            print(f"removed {stale.relative_to(ROOT)} (edition not built)")
    total = 0
    for edition, meta in editions.items():
        chunks = build_edition(edition, meta)
        path = OUT / f"sporting_regulations_{edition}.jsonl"
        path.write_text("".join(json.dumps(c, ensure_ascii=False) + "\n" for c in chunks))
        print(f"{path.relative_to(ROOT)}: {len(chunks)} chunks ({STRUCK.get(edition, 0)} struck-through characters removed)")
        total += len(chunks)
    notices = steward_notices()
    path = OUT / "steward_notices.jsonl"
    path.write_text("".join(json.dumps(c, ensure_ascii=False) + "\n" for c in notices))
    print(f"{path.relative_to(ROOT)}: {len(notices)} notices")
    print(f"total chunks (excluding precedents.jsonl): {total + len(notices)}")


if __name__ == "__main__":
    main()
