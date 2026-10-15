#!/usr/bin/env python3
"""Build the FIA regulations retrieval corpus in data/fia_regulations/.

Sources:
  * FIA 2025 Formula 1 Sporting Regulations, Issue 5 (30 April 2025)   -> article chunks, season=2025
  * FIA 2026 F1 Regulations Section B [Sporting], Issue 04 (10 Dec 2025) -> article chunks, season=2026
  * Verbatim FIA steward / race-control notices from the OpenF1 sessions in data/races/
  * data/fia_regulations/precedents.jsonl (hand-written digests of well-documented
    steward decisions; kept in the repo, not generated here)

The PDFs are downloaded to data/fia_regulations/.pdf/ (gitignored) and parsed with
pypdf. Chunks are split at sub-article numbers (30.5, B1.10.3, ...) and long
sub-articles are split again at lettered paragraphs so each chunk stays focused.
"""
from __future__ import annotations

import gzip
import json
import re
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "fia_regulations"
PDF_DIR = OUT / ".pdf"

SOURCES = {
    2025: (
        "https://api.fia.com/system/files/documents/fia_2025_formula_1_sporting_regulations_-_issue_5_-_2025-04-30.pdf",
        "FIA 2025 Formula 1 Sporting Regulations, Issue 5, 30 April 2025",
        re.compile(r"^\s?(\d{1,2})\.(\d{1,2})\s+(?=\S)"),
        re.compile(r"^\s?(\d{1,2})\)\s+([A-Z][A-Z ,&\-/()]+?)\s*$"),
    ),
    2026: (
        "https://www.fia.com/system/files/documents/fia_2026_f1_regulations_-_section_b_sporting_-_iss_04_-_2025-12-10_0.pdf",
        "FIA 2026 F1 Regulations Section B [Sporting], Issue 04, 10 December 2025",
        re.compile(r"^\s?B(\d)\.(\d{1,2})\.(\d{1,2})\s+(?=\S)"),
        re.compile(r"^\s?B(\d)\.(\d{1,2})\s+([A-Z][^\d]{3,80}?)\s*$"),
    ),
}

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
MAX_CHARS = 1400


def clean(text: str) -> str:
    text = text.replace("ﬁ", "fi").replace("ﬂ", "fl").replace("’", "'").replace("“", '"').replace("”", '"')
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"(\w) -(\w)", r"\1-\2", text)  # "re -join" -> "re-join"
    return text


def extract(season: int) -> list[str]:
    import pypdf  # imported lazily: only needed when rebuilding the corpus

    url = SOURCES[season][0]
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    pdf = PDF_DIR / f"sporting_regulations_{season}.pdf"
    if not pdf.exists():
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=120) as r:
            pdf.write_bytes(r.read())
    reader = pypdf.PdfReader(str(pdf))
    lines = []
    for page in reader.pages:
        for ln in (page.extract_text() or "").splitlines():
            if any(n.search(ln) for n in NOISE):
                continue
            lines.append(clean(ln).rstrip())
    return lines


def split_paragraphs(header: str, body: list[str]) -> list[tuple[str, str]]:
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
    packed: list[tuple[str, str]] = []
    cur_letters, cur = [], ""
    for letter, lines in parts:
        chunk = "\n".join(lines).strip()
        if not chunk:
            continue
        if cur and len(cur) + len(chunk) > MAX_CHARS:
            packed.append(("".join(cur_letters), cur))
            cur_letters, cur = [], ""
        cur_letters.append(letter)
        cur = (cur + "\n" + chunk).strip()
    if cur:
        packed.append(("".join(cur_letters), cur))
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


def build_season(season: int) -> list[dict]:
    url, source, sub_re, art_re = SOURCES[season]
    lines = extract(season)
    # skip the table of contents: start at the first sub-article "1.1" / "B1.1.1"
    first = "1.1 " if season == 2025 else "B1.1.1 "
    start = next(i for i, ln in enumerate(lines) if ln.strip().startswith(first) and i > 60)
    articles: dict[str, str] = {}
    for ln in lines[: start + 1]:
        m = art_re.match(ln)
        if m:
            key = m.group(1) if season == 2025 else f"B{m.group(1)}.{m.group(2)}"
            articles.setdefault(key, m.groups()[-1].strip().title())
    chunks: list[dict] = []
    seen: set[str] = set()
    cur_num, cur_tuple, cur_lines = None, (0,), []
    article_title = ""

    def flush():
        if not cur_num or not cur_lines:
            return
        head = cur_lines[0]
        sub_title = head.split(None, 1)[1] if " " in head.strip() else ""
        sub_title = sub_title if len(sub_title) < 70 and not sub_title.rstrip().endswith((".", ",", ":")) else ""
        pieces = split_paragraphs(head, cur_lines)
        for letters, text in pieces:
            suffix = f"-{letters}" if letters and len(pieces) > 1 else ""
            cid = f"{season}-{cur_num}{suffix}"
            while cid in seen:
                cid += "+"
            seen.add(cid)
            chunks.append({
                "id": cid,
                "season": season,
                "kind": "regulation",
                "article": cur_num,
                "paragraphs": letters or None,
                "article_title": article_title,
                "title": sub_title.strip(),
                "text": text if text.startswith(cur_num) else f"{cur_num} {text}",
                "source": source,
                "url": url,
            })

    for ln in lines[start:]:
        a = art_re.match(ln)
        if a and season == 2025 and a.group(1) in articles:
            article_title = articles[a.group(1)]
            continue
        if a and season == 2026:
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
                if season == 2026:
                    cur_num = "B" + cur_num
                    article_title = articles.get(f"B{num_tuple[0]}.{num_tuple[1]}", article_title)
                cur_lines = [ln.strip()]
                continue
        if cur_num:
            cur_lines.append(ln)
    flush()
    return [c for c in chunks if len(c["text"]) > 40]


def steward_notices() -> list[dict]:
    """Verbatim FIA steward and race-control decisions from the OpenF1 sessions."""
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
                "season": meta["year"],
                "kind": "steward_notice",
                "article": None,
                "paragraphs": None,
                "article_title": "Race Control / Stewards notice",
                "title": f"{meta['year']} {meta['location']} GP, lap {m.get('lap_number')}",
                "text": f"{meta['year']} {meta['location']} Grand Prix, lap {m.get('lap_number')}: {msg}",
                "source": f"OpenF1 race_control, session_key={meta['session_key']}",
                "url": f"https://api.openf1.org/v1/race_control?session_key={meta['session_key']}",
            })
    return docs


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    total = 0
    for season in SOURCES:
        chunks = build_season(season)
        path = OUT / f"sporting_regulations_{season}.jsonl"
        path.write_text("".join(json.dumps(c, ensure_ascii=False) + "\n" for c in chunks))
        print(f"{path.relative_to(ROOT)}: {len(chunks)} chunks")
        total += len(chunks)
    notices = steward_notices()
    path = OUT / "steward_notices.jsonl"
    path.write_text("".join(json.dumps(c, ensure_ascii=False) + "\n" for c in notices))
    print(f"{path.relative_to(ROOT)}: {len(notices)} notices")
    print(f"total chunks (excluding precedents.jsonl): {total + len(notices)}")


if __name__ == "__main__":
    main()
