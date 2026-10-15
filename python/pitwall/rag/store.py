"""Qdrant dual-vector hybrid retrieval over the FIA regulations corpus.

Every point stores two named vectors:
  * ``dense``  - 1024-dim bge-m3 embedding, HNSW cosine index
  * ``sparse`` - BM25 term weights with server-side IDF (``Modifier.IDF``)

Hybrid search sends one ``query_points`` request with two ``Prefetch``
branches (dense + sparse, each optionally payload-filtered) fused by
Reciprocal Rank Fusion on the server; the fused candidates are then
re-scored by a cross-encoder.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from qdrant_client import QdrantClient, models

from pitwall import config
from pitwall.rag.embed import OllamaEmbedder
from pitwall.rag.sparse import BM25Encoder

MODES = ("dense", "sparse", "hybrid", "hybrid_rerank")
# On the 40-query benchmark every cross-encoder lands within one or two
# queries of plain RRF (docs/BENCHMARKS.md), so the cheapest one is the
# default: ms-marco-MiniLM-L-12 over 10 candidates (~0.2 s). bge-reranker-v2-m3
# is ~10x slower with no measurable gain on this set.
DEFAULT_RERANKER = os.environ.get("PITWALL_RERANKER", "Xenova/ms-marco-MiniLM-L-12-v2")
DEFAULT_RERANK_K = int(os.environ.get("PITWALL_RERANK_K", "10"))


def load_corpus(directory: Path | None = None) -> list[dict]:
    d = Path(directory or config.FIA_DIR)
    docs = []
    for p in sorted(d.glob("*.jsonl")):
        if p.name.startswith("eval_"):
            continue
        for line in p.read_text().splitlines():
            if line.strip():
                docs.append(json.loads(line))
    return docs


def doc_text(d: dict) -> str:
    """Text indexed for both vectors: article context + body."""
    head = []
    if d.get("article"):
        head.append(f"Article {d['article']}" + (f" {d['paragraphs']})" if d.get("paragraphs") and len(d["paragraphs"]) == 1 else ""))
    if d.get("article_title"):
        head.append(d["article_title"])
    if d.get("title"):
        head.append(d["title"])
    return " - ".join(head) + "\n" + d["text"]


def with_dates(d: dict) -> dict:
    """Ensure dated documents carry an integer ``date_ts`` for range filters."""
    if d.get("date") and "date_ts" not in d:
        d = {**d, "date_ts": int(datetime.fromisoformat(d["date"][:10]).replace(tzinfo=timezone.utc).timestamp())}
    return d


def point_id(chunk_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"pitwall/{chunk_id}"))


@dataclass
class Hit:
    id: str
    score: float
    payload: dict[str, Any]
    rank: int = 0
    rerank_score: float | None = None

    @property
    def citation(self) -> str:
        p = self.payload
        if p.get("kind") == "regulation":
            para = f"({p['paragraphs']})" if p.get("paragraphs") and len(p["paragraphs"]) == 1 else ""
            return f"FIA {p['season']} Sporting Regulations (Issue {p.get('issue', '?')}) Art. {p['article']}{para}"
        return p.get("title") or p.get("id", "")


@dataclass
class SearchResult:
    hits: list[Hit]
    mode: str
    qdrant_ms: float
    embed_ms: float = 0.0
    rerank_ms: float = 0.0
    timings: dict[str, float] = field(default_factory=dict)

    @property
    def total_ms(self) -> float:
        return self.qdrant_ms + self.embed_ms + self.rerank_ms


class HybridRetriever:
    def __init__(self, url: str | None = None, collection: str | None = None,
                 embedder: OllamaEmbedder | None = None, reranker_model: str | None = DEFAULT_RERANKER):
        self.client = QdrantClient(url=url or config.QDRANT_URL, timeout=60, prefer_grpc=False)
        self.collection = collection or config.QDRANT_COLLECTION
        self.embedder = embedder or OllamaEmbedder()
        self.reranker_model = reranker_model
        self._reranker = None

    # ---------------------------------------------------------------- index
    def ensure_collection(self, dim: int, recreate: bool = False) -> None:
        exists = self.client.collection_exists(self.collection)
        if exists and recreate:
            self.client.delete_collection(self.collection)
            exists = False
        if not exists:
            self.client.create_collection(
                self.collection,
                vectors_config={"dense": models.VectorParams(size=dim, distance=models.Distance.COSINE,
                                                              hnsw_config=models.HnswConfigDiff(m=16, ef_construct=128))},
                sparse_vectors_config={"sparse": models.SparseVectorParams(modifier=models.Modifier.IDF)},
            )
            for fieldname, schema in (("season", models.PayloadSchemaType.INTEGER),
                                      ("kind", models.PayloadSchemaType.KEYWORD),
                                      ("edition", models.PayloadSchemaType.KEYWORD),
                                      ("article", models.PayloadSchemaType.KEYWORD),
                                      ("date_ts", models.PayloadSchemaType.INTEGER),
                                      ("session_key", models.PayloadSchemaType.INTEGER),
                                      ("lap", models.PayloadSchemaType.INTEGER)):
                self.client.create_payload_index(self.collection, fieldname, field_schema=schema)

    def index(self, docs: list[dict], recreate: bool = True, batch: int = 32, log=print) -> dict:
        t0 = time.perf_counter()
        texts = [doc_text(d) for d in docs]
        bm25 = BM25Encoder().fit(texts)
        dense: list[list[float]] = []
        for i in range(0, len(texts), batch):
            dense.extend(self.embedder.embed(texts[i : i + batch], batch=batch))
            if log and (i // batch) % 5 == 0:
                log(f"  embedded {min(i + batch, len(texts))}/{len(texts)}")
        embed_s = time.perf_counter() - t0
        self.ensure_collection(len(dense[0]), recreate=recreate)
        points = []
        for d, text, vec in zip(docs, texts, dense):
            sp = bm25.encode_doc(text)
            points.append(models.PointStruct(
                id=point_id(d["id"]),
                vector={"dense": vec, "sparse": models.SparseVector(indices=sp.indices, values=sp.values)},
                payload={**with_dates(d), "indexed_text": text},
            ))
        for i in range(0, len(points), 128):
            self.client.upsert(self.collection, points=points[i : i + 128], wait=True)
        info = self.client.get_collection(self.collection)
        return {"points": info.points_count, "dim": len(dense[0]), "avgdl": bm25.avgdl,
                "embed_s": round(embed_s, 2), "total_s": round(time.perf_counter() - t0, 2)}

    # --------------------------------------------------------------- search
    CONTEXT_KINDS = ["precedent_digest", "operational_note", "steward_notice"]

    @staticmethod
    def edition_filter(edition: str) -> models.Filter:
        """One regulation edition plus every non-regulation document (benchmark scope)."""
        return models.Filter(should=[
            models.Filter(must=[models.FieldCondition(key="kind", match=models.MatchValue(value="regulation")),
                                models.FieldCondition(key="edition", match=models.MatchValue(value=edition))]),
            models.Filter(must=[models.FieldCondition(key="kind", match=models.MatchAny(any=HybridRetriever.CONTEXT_KINDS))]),
        ])

    @staticmethod
    def as_of_filter(edition: str, before_ts: int, session_key: int | None = None, lap: int | None = None) -> models.Filter:
        """What a pit wall could know at a decision point: the regulation edition
        in force, precedents and notes dated before the race, and race-control
        notices of the same race from laps before the decision lap."""
        should = [
            models.Filter(must=[models.FieldCondition(key="kind", match=models.MatchValue(value="regulation")),
                                models.FieldCondition(key="edition", match=models.MatchValue(value=edition))]),
            models.Filter(must=[models.FieldCondition(key="kind", match=models.MatchAny(any=HybridRetriever.CONTEXT_KINDS)),
                                models.FieldCondition(key="date_ts", range=models.Range(lt=before_ts))]),
        ]
        if session_key is not None and lap is not None:
            should.append(models.Filter(must=[
                models.FieldCondition(key="kind", match=models.MatchValue(value="steward_notice")),
                models.FieldCondition(key="session_key", match=models.MatchValue(value=session_key)),
                models.FieldCondition(key="lap", range=models.Range(lt=lap)),
            ]))
        return models.Filter(should=should)

    def _reranker_model(self):
        if self._reranker is None:
            from fastembed.rerank.cross_encoder import TextCrossEncoder

            self._reranker = TextCrossEncoder(self.reranker_model, cache_dir=config.FASTEMBED_CACHE)
        return self._reranker

    def search(self, query: str, mode: str = "hybrid_rerank", k: int = 5, prefetch_k: int = 30,
               rerank_k: int = DEFAULT_RERANK_K, qfilter: models.Filter | None = None,
               dense_vec: list[float] | None = None) -> SearchResult:
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        embed_ms = 0.0
        if mode != "sparse" and dense_vec is None:
            t = time.perf_counter()
            dense_vec = self.embedder.embed_one(query)
            embed_ms = (time.perf_counter() - t) * 1000
        sp = BM25Encoder.encode_query(query)
        sparse_vec = models.SparseVector(indices=sp.indices, values=sp.values)
        limit = rerank_k if mode == "hybrid_rerank" else k
        t = time.perf_counter()
        if mode == "dense":
            res = self.client.query_points(self.collection, query=dense_vec, using="dense", limit=limit,
                                           query_filter=qfilter, with_payload=True)
        elif mode == "sparse":
            res = self.client.query_points(self.collection, query=sparse_vec, using="sparse", limit=limit,
                                           query_filter=qfilter, with_payload=True)
        else:
            res = self.client.query_points(
                self.collection,
                prefetch=[
                    models.Prefetch(query=dense_vec, using="dense", limit=prefetch_k, filter=qfilter),
                    models.Prefetch(query=sparse_vec, using="sparse", limit=prefetch_k, filter=qfilter),
                ],
                query=models.FusionQuery(fusion=models.Fusion.RRF),
                limit=limit, with_payload=True,
            )
        qdrant_ms = (time.perf_counter() - t) * 1000
        hits = [Hit(id=p.payload.get("id", str(p.id)), score=p.score, payload=p.payload, rank=i + 1)
                for i, p in enumerate(res.points)]
        rerank_ms = 0.0
        if mode == "hybrid_rerank" and hits:
            t = time.perf_counter()
            scores = list(self._reranker_model().rerank(query, [h.payload["indexed_text"] for h in hits]))
            for h, s in zip(hits, scores):
                h.rerank_score = float(s)
            # cross-encoder score, with RRF rank as a tie-breaker
            hits.sort(key=lambda h: (-h.rerank_score, h.rank))
            hits = hits[:k]
            rerank_ms = (time.perf_counter() - t) * 1000
        return SearchResult(hits=hits[:k], mode=mode, qdrant_ms=qdrant_ms, embed_ms=embed_ms, rerank_ms=rerank_ms)
