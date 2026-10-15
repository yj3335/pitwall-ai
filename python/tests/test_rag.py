"""Hybrid RAG: tokenizer/BM25 units plus live Qdrant + bge-m3 integration."""
from __future__ import annotations

import pytest

from conftest import requires_ollama, requires_qdrant
from pitwall.rag.sparse import BM25Encoder, token_id, tokenize
from pitwall.rag.store import HybridRetriever, load_corpus


def test_tokenizer_keeps_article_citations():
    toks = tokenize("Under Article 30.5(m) and 54.3 a), B1.10.5 governs unsafe releases under the VSC")
    assert {"30.5m", "30.5", "54.3a", "54.3", "b1.10.5", "vsc"} <= set(toks)
    assert "30" not in toks and "under" not in toks
    # chunk text form "30.5 m) For all races" yields the same sub-paragraph token
    assert "30.5m" in tokenize("30.5 m) For all races except the race in Monaco")


def test_bm25_doc_weights_saturate_and_normalise():
    enc = BM25Encoder().fit(["penalty penalty penalty hold", "unsafe release pit lane " * 10])
    short = enc.encode_doc("penalty penalty penalty hold")
    w = dict(zip(short.indices, short.values))
    assert w[token_id("penalti")] > w[token_id("hold")]  # tf matters
    assert w[token_id("penalti")] < 3 * w[token_id("hold")]  # but saturates (k1)
    q = BM25Encoder.encode_query("Article 54.3 penalty penalty")
    assert sorted(q.indices) == q.indices and all(v == 1.0 for v in q.values)


def test_corpus_has_real_regulations():
    docs = load_corpus()
    ids = {d["id"] for d in docs}
    for must in ("2025-i5-30.5-m", "2025-i5-54.4-c", "2025-i5-34.14-ab", "2025-i5-56.4", "2025-i5-57.4-b", "2023-i6-30.5-n"):
        assert must in ids
    editions = {d.get("edition") for d in docs if d["kind"] == "regulation"}
    assert editions == {"2023-i6", "2024-i6", "2024-i7", "2025-i5"}  # 2026 is a new formula: not indexed by default
    assert all(d.get("date") for d in docs if d["kind"] != "regulation")  # context docs are dated for point-in-time filters
    m = next(d for d in docs if d["id"] == "2025-i5-30.5-m")
    assert "two (2) different" in m["text"] and "intermediate" in m["text"]


def test_redline_deletions_are_not_indexed():
    """Issues are published as redlines; struck-through text must not appear."""
    docs = {d["id"]: d for d in load_corpus()}
    assert docs["2024-i6-30.5-m"]["text"].startswith("30.5 m) Unless he has used")  # not "l m)"
    # sentence deleted from 2025 Issue 5 (was 58.12 in the struck-through text)
    assert not any("considered unsuitable to resume the sprint session or the race from a standing start" in d["text"]
                   for d in docs.values() if d.get("edition") == "2025-i5")


@pytest.fixture(scope="module")
def retriever():
    r = HybridRetriever(collection="pitwall_test_regs", reranker_model="Xenova/ms-marco-MiniLM-L-6-v2")
    docs = [d for d in load_corpus() if d.get("edition") in ("2025-i5", "2023-i6") or d["kind"] != "regulation"]
    keep = [d for d in docs if d.get("article") and d["article"].split(".")[0] in {"30", "33", "34", "54", "55", "56", "57"}]
    keep += [d for d in docs if d["kind"] in ("precedent_digest", "operational_note", "steward_notice")]
    keep = list({d["id"]: d for d in keep}.values())  # precedents also carry an article number
    stats = r.index(keep, recreate=True, log=None)
    assert stats["points"] == len(keep) and stats["dim"] == 1024
    yield r
    r.client.delete_collection("pitwall_test_regs")


@requires_qdrant
@requires_ollama
def test_sparse_exact_citation_hits_first(retriever):
    # precedent digests that cite "Article 30.5(m)" repeatedly may outrank the
    # article itself lexically; the article must still be in the top 3
    f = HybridRetriever.edition_filter("2025-i5")
    res = retriever.search("Article 30.5(m)", mode="sparse", k=3, qfilter=f)
    assert "2025-i5-30.5-m" in [h.id for h in res.hits]
    assert "2025-i5-54.4-c" in [h.id for h in retriever.search("Article 54.4 c)", mode="sparse", k=3, qfilter=f).hits]


@requires_qdrant
@requires_ollama
def test_dense_semantic_match(retriever):
    res = retriever.search("can we pit for fresh tyres while the virtual safety car is out?", mode="dense", k=5,
                           qfilter=HybridRetriever.edition_filter("2025-i5"))
    assert "2025-i5-56.4" in [h.id for h in res.hits]


@requires_qdrant
@requires_ollama
def test_hybrid_rrf_and_rerank(retriever):
    q = "My driver has a 5 second penalty, can mechanics touch the car while he serves it?"
    rrf = retriever.search(q, mode="hybrid", k=10)
    assert len(rrf.hits) == 10 and rrf.qdrant_ms > 0
    rr = retriever.search(q, mode="hybrid_rerank", k=3)
    assert rr.hits[0].rerank_score is not None
    assert {"2025-i5-54.4-c", "2023-i6-54.4-c", "prec-2023-sau-alo-penalty-service", "prec-2023-bhr-oco-penalty-service"} & {h.id for h in rr.hits}
    assert rr.hits[0].citation


@requires_qdrant
@requires_ollama
def test_edition_and_point_in_time_filters(retriever):
    f = HybridRetriever.edition_filter("2023-i6")
    res = retriever.search("virtual safety car pit lane", mode="hybrid", k=10, qfilter=f)
    regs = [h for h in res.hits if h.payload["kind"] == "regulation"]
    assert regs and all(h.payload["edition"] == "2023-i6" for h in regs)
    # Singapore 2023, lap 44: nothing dated on/after the race day, no notice from lap 44 on
    day = 1694908800  # 2023-09-17T00:00Z
    f = HybridRetriever.as_of_filter("2023-i6", before_ts=day, session_key=9165, lap=44)
    for q in ("Mercedes double stack VSC Singapore", "red flag free tyre change", "virtual safety car deployed"):
        for h in retriever.search(q, mode="hybrid", k=30, qfilter=f).hits:
            p = h.payload
            if p["kind"] == "regulation":
                assert p["edition"] == "2023-i6"
            elif p.get("session_key") == 9165:
                assert p["lap"] < 44, h.id
            else:
                assert p["date"][:10] < "2023-09-17", h.id
