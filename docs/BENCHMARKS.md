# Benchmarks

Machine: Apple M5 Pro (15 cores), 24 GB RAM, macOS. Models run locally through
Ollama 0.32.7. All numbers below were measured on this machine. Sections marked
**pending** have not been run yet; they will be produced by `make bench` and written
to `benchmarks/results.json`.

## 1. Stream pipeline (Go + Kafka + Parquet + DuckDB)

**Pending:** the full 200,000-event Kafka run with concurrent DuckDB queries
(`cmd/bench-stream`).

Measured so far during development:

| Measurement | Result |
|---|---|
| Offline processor, one engine, real events to Parquet | 93,276 events in 62-68 ms (about 1.4M events/s) |
| Live Kafka replay of Singapore 2023, burst produce | 113,771 events, 0 decode errors, 0 sink errors |
| Stream-to-trigger latency in that burst (all events produced within 109 ms) | p50 33.8 ms, p95 56.3 ms, p99 58.4 ms |
| DuckDB degradation leaderboard over the lakehouse | p50 1.27 ms, p95 1.53 ms |
| DuckDB decision-scoped model SQL | p50 0.94 ms, p95 1.00 ms |
| Concurrent Go commits vs DuckDB readers | more than 100 commits, 4 reader threads, 0 errors |
| Monte Carlo, 10,000 rollouts x 6 strategies (60,000 trajectories), 15 workers | 5.4 ms (mean of 50 runs) |
| Monte Carlo, 10,000 rollouts of one strategy | 1.1 ms |

The live replay ran before the post-flag cool-down lap was removed (113,771 events;
the file now has 113,469). The burst latency is queueing-dominated: the replayer
produced the whole race in 109 ms, so the p99 measures how fast 20 partitions drain a
114k-record backlog. The benchmark will report a paced replay as well.

## 2. Qdrant retrieval ablation (40 FIA queries)

**Corpus:** 2,077 chunks.
- The 2023 Issue 6, 2024 Issue 6, 2024 Issue 7 and 2025 Issue 5 Sporting Regulations,
  with redline deletions removed and chunked at paragraph level.
- 67 race-control notices, 11 precedent digests and 3 operational notes.

**Scope:** the queries are written against the 2025 regulations, so retrieval uses
`edition_filter("2025-i5")`: the 2025 edition plus all context documents.

**Queries:** 20 exact-article ("Article 54.4 c) ...") and 20 situational ("my driver
has a 5 s penalty, can the mechanics start work as soon as he stops?"). Gold labels
are in `data/fia_regulations/eval_queries.jsonl`, with a note on every label changed
after the first run.

**Metric:** Recall@k is the share of queries with a gold chunk in the top k. Query
embeddings are computed once and shared by every mode (18.4 ms per query with
bge-m3).

**Reproduce:** `python -m pitwall.eval.rag_eval`. Raw output:
`benchmarks/rag_ablation.json`.

| Mode | R@1 | R@3 | R@5 | MRR@10 | R@3 exact | R@3 situational | Qdrant p50 / p95 | End-to-end p50 / p95 |
|---|---|---|---|---|---|---|---|---|
| Dense (bge-m3) | 0.800 | 0.900 | 0.925 | 0.863 | 1.000 | 0.800 | 1.66 / 2.22 ms | 20.1 / 20.6 ms |
| Sparse (BM25) | 0.575 | 0.875 | 0.925 | 0.721 | 1.000 | 0.750 | 1.19 / 1.26 ms | 1.2 / 1.3 ms |
| Hybrid RRF | 0.775 | 0.950 | 0.975 | 0.859 | 1.000 | 0.900 | 2.65 / 2.96 ms | 21.1 / 21.4 ms |
| **Hybrid RRF + MiniLM-L-12 rerank (top 10)** | 0.750 | **0.975** | 0.975 | 0.850 | 1.000 | **0.950** | 3.35 / 6.33 ms | 168 / 277 ms |

What this shows, plainly:
- Every mode retrieves exact article citations perfectly, sparse included, thanks to
  the citation-preserving tokenizer.
- Fusion helps on situational questions. Hybrid RRF reaches 0.900 R@3 against 0.800
  for dense and 0.750 for sparse, and misses 2 queries overall against 4 for dense.
- The cross-encoder's further gain is one query (0.950 to 0.975 R@3), within noise on
  40 queries. R@1 and MRR dip slightly. The rerank stage is kept as a cheap safeguard,
  not as a proven improvement.
- The only remaining miss (S09, "drive-through in the final three laps") sits inside a
  long multi-paragraph chunk.

**Reranker sweep** (hybrid RRF candidates, same 40 queries, `benchmarks/rag_reranker_sweep.json`):

| Reranker | Candidates | R@1 | R@3 | MRR | End-to-end p50 |
|---|---|---|---|---|---|
| ms-marco-MiniLM-L-12-v2 | 10 | 0.750 | 0.975 | 0.850 | 183 ms |
| ms-marco-MiniLM-L-12-v2 | 20 | 0.750 | 0.950 | 0.848 | 352 ms |
| jina-reranker-v1-turbo-en | 20 | 0.675 | 0.875 | 0.770 | 233 ms |
| bge-reranker-v2-m3 | 10 | 0.775 | 0.950 | 0.863 | 1,538 ms |
| bge-reranker-v2-m3 | 15 | 0.775 | 0.925 | 0.860 | 2,260 ms |
| bge-reranker-v2-m3 | 20 | 0.775 | 0.925 | 0.860 | 2,922 ms |
| *(no rerank: hybrid RRF)* | - | 0.775 | 0.950 | 0.859 | 21 ms |

**Caveats.**
- The default was chosen on these same 40 queries. With differences of one or two
  queries, the honest reading is that rerankers and plain RRF are tied here.
- An earlier version of this benchmark ran on a corpus that still contained
  struck-through (deleted) regulation text and coarser chunks. There, RRF appeared to
  underperform dense retrieval and bge-reranker-v2-m3 appeared necessary. Both
  conclusions were artifacts of that corpus (`CHANGELOG.md`).

## 3. Agent evaluation (20 Grand Prix scenarios, real models)

**Pending:** single-pass qwen2.5-coder:14b vs the cyclic LangGraph agent, with 10%
injected primary faults. Metrics: FIA compliance rate, agreement with the ground truth
(reported separately for the 7 real and 13 synthetic scenarios), end-to-end p50/p95,
and failover recovery.

**Development runs** (no fault injection): the real scenarios GP02 (Singapore 2023)
and GP06 (Sao Paulo 2024) each took 15-16 s end to end on the real models. Both
produced compliant calls that match what the teams did, and cited the edition in force
(30.5(n) for 2023). Most of the time is the qwen2.5-coder:14b SQL and synthesis calls.
The 60,000-trajectory simulation takes about 6 ms.
