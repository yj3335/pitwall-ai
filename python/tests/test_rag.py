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
    for must in ("2025-30.5-m", "2025-54.4-cde", "2025-34.14-abcd", "2025-56.4", "2025-57.4-bc"):
        assert must in ids
    assert any(d["kind"] == "steward_notice" for d in docs)
    assert sum(d["season"] == 2026 for d in docs) > 200
    m = next(d for d in docs if d["id"] == "2025-30.5-m")
    assert "two (2) different" in m["text"] and "intermediate" in m["text"]


@pytest.fixture(scope="module")
def retriever():
    r = HybridRetriever(collection="pitwall_test_regs", reranker_model="Xenova/ms-marco-MiniLM-L-6-v2")
    docs = [d for d in load_corpus() if d["season"] == 2025 or d["kind"] != "regulation"]
    keep = [d for d in docs if d.get("article", "") and d["article"].split(".")[0] in {"30", "33", "34", "54", "55", "56", "57"}]
    keep += [d for d in load_corpus() if d["season"] == 2026 and d["article"] and d["article"].startswith("B5.12")]
    keep += [d for d in docs if d["kind"] == "precedent_digest"]
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
    res = retriever.search("Article 30.5(m)", mode="sparse", k=3)
    assert "2025-30.5-m" in [h.id for h in res.hits]
    assert retriever.search("Article 54.4 c)", mode="sparse", k=3).hits[0].id == "2025-54.4-cde"


@requires_qdrant
@requires_ollama
def test_dense_semantic_match(retriever):
    res = retriever.search("can we pit for fresh tyres while the virtual safety car is out?", mode="dense", k=5)
    assert "2025-56.4" in [h.id for h in res.hits]


@requires_qdrant
@requires_ollama
def test_hybrid_rrf_and_rerank(retriever):
    q = "My driver has a 5 second penalty, can mechanics touch the car while he serves it?"
    rrf = retriever.search(q, mode="hybrid", k=10)
    assert len(rrf.hits) == 10 and rrf.qdrant_ms > 0
    rr = retriever.search(q, mode="hybrid_rerank", k=3)
    assert rr.hits[0].rerank_score is not None
    assert {"2025-54.4-cde", "prec-2023-sau-alo-penalty-service", "prec-2023-bhr-oco-penalty-service"} & {h.id for h in rr.hits}
    assert rr.hits[0].citation


@requires_qdrant
@requires_ollama
def test_payload_filter_by_season(retriever):
    f = HybridRetriever.make_filter(season=2026)
    res = retriever.search("virtual safety car pit lane", mode="hybrid", k=5, qfilter=f)
    assert res.hits and all(h.payload["season"] == 2026 for h in res.hits)
    f = HybridRetriever.make_filter(exclude_season=2026, kinds=["precedent_digest"])
    res = retriever.search("red flag tyre change", mode="hybrid", k=5, qfilter=f)
    assert res.hits and all(h.payload["kind"] == "precedent_digest" for h in res.hits)
