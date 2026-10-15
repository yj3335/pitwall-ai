"""Benchmark 2: Qdrant retrieval ablation over 40 FIA regulation queries.

Compares dense-only (bge-m3), sparse-only (BM25), hybrid RRF and hybrid RRF +
cross-encoder rerank on the same Qdrant collection and the same cached query
embeddings. Recall@k is the hit rate of the gold chunk(s) in the top k.

    python -m pitwall.eval.rag_eval [--out ../benchmarks/rag.json]
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

from pitwall import config
from pitwall.rag.store import MODES, HybridRetriever


def pct(xs: list[float], q: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))]


def load_queries(path: Path | None = None) -> list[dict]:
    p = path or config.FIA_DIR / "eval_queries.jsonl"
    return [json.loads(line) for line in p.read_text().splitlines() if line.strip()]


def evaluate(retriever: HybridRetriever, queries: list[dict], modes=MODES, k_eval: int = 10, warmup: bool = True) -> dict:
    # current-season filter: the 2026 Section B duplicates many 2025 articles
    qfilter = HybridRetriever.make_filter(exclude_season=2026)
    t = time.perf_counter()
    vecs = retriever.embedder.embed([q["query"] for q in queries])
    embed_ms_per_query = (time.perf_counter() - t) * 1000 / len(queries)
    if warmup:
        retriever.search(queries[0]["query"], mode="hybrid_rerank", dense_vec=vecs[0], qfilter=qfilter)
    report: dict = {"queries": len(queries), "embed_ms_per_query": round(embed_ms_per_query, 2), "modes": {}}
    for mode in modes:
        r1 = r3 = r5 = mrr = 0.0
        qd_ms, total_ms = [], []
        by_type: dict[str, list[float]] = {}
        misses = []
        for q, vec in zip(queries, vecs):
            res = retriever.search(q["query"], mode=mode, k=k_eval, dense_vec=vec, qfilter=qfilter)
            ids = [h.id for h in res.hits]
            gold = set(q["gold"])
            rank = next((i + 1 for i, x in enumerate(ids) if x in gold), None)
            r1 += rank == 1
            r3 += rank is not None and rank <= 3
            r5 += rank is not None and rank <= 5
            mrr += 1.0 / rank if rank else 0.0
            by_type.setdefault(q["type"], []).append(1.0 if rank is not None and rank <= 3 else 0.0)
            qd_ms.append(res.qdrant_ms)
            total_ms.append(res.qdrant_ms + res.rerank_ms + (embed_ms_per_query if mode != "sparse" else 0.0))
            if not rank or rank > 3:
                misses.append({"qid": q["qid"], "rank": rank, "top3": ids[:3]})
        n = len(queries)
        report["modes"][mode] = {
            "recall_at_1": round(r1 / n, 4), "recall_at_3": round(r3 / n, 4), "recall_at_5": round(r5 / n, 4),
            "mrr_at_10": round(mrr / n, 4),
            "recall_at_3_exact": round(statistics.mean(by_type.get("exact", [0])), 4),
            "recall_at_3_semantic": round(statistics.mean(by_type.get("semantic", [0])), 4),
            "qdrant_p50_ms": round(pct(qd_ms, 0.5), 2), "qdrant_p95_ms": round(pct(qd_ms, 0.95), 2),
            "end_to_end_p50_ms": round(pct(total_ms, 0.5), 2), "end_to_end_p95_ms": round(pct(total_ms, 0.95), 2),
            "misses_at_3": misses,
        }
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--reranker", default=None)
    args = ap.parse_args()
    kwargs = {"reranker_model": args.reranker} if args.reranker else {}
    retriever = HybridRetriever(**kwargs)
    report = evaluate(retriever, load_queries())
    report["reranker"] = retriever.reranker_model
    report["embed_model"] = retriever.embedder.model
    print(f"{'mode':15} {'R@1':>6} {'R@3':>6} {'R@5':>6} {'MRR':>6} {'R@3 exact':>10} {'R@3 sem':>8} {'qdrant p50/p95 ms':>18}")
    for m, r in report["modes"].items():
        print(f"{m:15} {r['recall_at_1']:6.3f} {r['recall_at_3']:6.3f} {r['recall_at_5']:6.3f} {r['mrr_at_10']:6.3f} "
              f"{r['recall_at_3_exact']:10.3f} {r['recall_at_3_semantic']:8.3f} {r['qdrant_p50_ms']:8.2f}/{r['qdrant_p95_ms']:<8.2f}")
    if args.out:
        args.out.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
