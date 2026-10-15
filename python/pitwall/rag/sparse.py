"""BM25 sparse vectors with a citation-preserving tokenizer.

Generic BM25 tokenizers split "30.5" into "30" and "5", which destroys exact
article lookups. This tokenizer keeps article numbers whole ("30.5", "54.3",
"b1.10.5") and adds a sub-paragraph token ("30.5m" for "30.5 m)" or
"Article 30.5(m)"), so a query that cites an article matches it exactly.

Documents carry BM25 term-frequency saturation weights; the IDF part is
applied server-side by Qdrant (sparse vector ``modifier=IDF``), so the index
never needs re-weighting as the corpus grows. Query tokens get weight 1.0.
"""
from __future__ import annotations

import re
import zlib
from collections import Counter
from dataclasses import dataclass

try:
    from py_rust_stemmers import SnowballStemmer

    _stem = SnowballStemmer("english").stem_word
except Exception:  # pragma: no cover - fallback when the stemmer wheel is absent
    def _stem(w: str) -> str:
        for suf in ("ings", "ing", "ies", "es", "ed", "s"):
            if w.endswith(suf) and len(w) > len(suf) + 2:
                return w[: -len(suf)]
        return w

STOPWORDS = frozenset(
    "a an and are as at be been but by can for from has have he him his if in into is it its may must no not of on "
    "or shall such that the their them then there these they this to under was were what when where which while who "
    "will with would any all each other than also only after before during between does do so".split()
)

_ARTICLE = re.compile(r"\b(b?\d{1,2}\.\d{1,2}(?:\.\d{1,2})?)\s*\(?\s*([a-z])\s*\)")
_NUMBER = re.compile(r"\bb?\d{1,2}\.\d{1,2}(?:\.\d{1,2})?\b")
_WORD = re.compile(r"[a-z][a-z\-]*[a-z]|[a-z]{2,}")
_INT = re.compile(r"\b\d+\b")

K1, B = 1.2, 0.75


def tokenize(text: str) -> list[str]:
    t = text.lower().replace("’", "'")
    toks: list[str] = []
    for m in _ARTICLE.finditer(t):
        toks.append(f"{m.group(1)}{m.group(2)}")
    for m in _NUMBER.finditer(t):
        toks.append(m.group(0))
    rest = _NUMBER.sub(" ", t)
    for w in _WORD.findall(rest):
        for part in w.split("-"):
            if len(part) >= 2 and part not in STOPWORDS:
                toks.append(_stem(part))
    for n in _INT.findall(rest):
        toks.append(n)  # "5 second", "80 km/h", car numbers
    return toks


def token_id(tok: str) -> int:
    return zlib.crc32(tok.encode()) & 0x7FFFFFFF


@dataclass
class SparseVec:
    indices: list[int]
    values: list[float]


class BM25Encoder:
    """Doc-side BM25 TF weights; avgdl is fixed at index time."""

    def __init__(self, avgdl: float = 120.0):
        self.avgdl = avgdl

    def fit(self, docs: list[str]) -> "BM25Encoder":
        lens = [len(tokenize(d)) for d in docs]
        self.avgdl = sum(lens) / max(1, len(lens))
        return self

    def encode_doc(self, text: str) -> SparseVec:
        toks = tokenize(text)
        dl = len(toks)
        tf = Counter(toks)
        idx, val = [], []
        for tok, f in tf.items():
            w = f * (K1 + 1) / (f + K1 * (1 - B + B * dl / self.avgdl))
            idx.append(token_id(tok))
            val.append(float(w))
        return _dedupe(idx, val)

    @staticmethod
    def encode_query(text: str) -> SparseVec:
        toks = set(tokenize(text))
        return _dedupe([token_id(t) for t in toks], [1.0] * len(toks))


def _dedupe(idx: list[int], val: list[float]) -> SparseVec:
    # crc32 collisions are rare; merge them so Qdrant gets unique indices
    acc: dict[int, float] = {}
    for i, v in zip(idx, val):
        acc[i] = acc.get(i, 0.0) + v
    items = sorted(acc.items())
    return SparseVec([i for i, _ in items], [v for _, v in items])
