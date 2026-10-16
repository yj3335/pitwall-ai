# Benchmarks

Machine: Apple M5 Pro (15 cores), 24 GB RAM, macOS. Models run locally through
Ollama 0.32.7. Every number below was measured on this machine. `make bench` reruns
all three benchmarks and writes `benchmarks/results.json` (headline numbers, machine
and versions) next to the raw outputs `stream.json`, `rag_ablation.json` and
`agent_eval.json`.

## 1. Stream pipeline (Go + Kafka + Parquet + DuckDB)

**Setup** (`cmd/bench-stream`, `python -m pitwall.eval.stream_bench`):
- **Events:** 200,000 real OpenF1 events (Singapore 2023, then Sao Paulo 2024),
  produced by franz-go to a fresh 20-partition topic keyed by driver number, on the
  single-broker KRaft Kafka 4.4.0 from `.venv/`.
- **Consumer:** one consumer group runs one Go engine per partition. It computes the
  degradation windows and triggers and writes the telemetry, laps and triggers tables
  through the atomic Parquet sink.
- **Trials:** each trial uses a new topic, consumer group and lakehouse directory, and
  the topic is deleted afterwards.
- **Burst mode:** produces as fast as possible, after one warm-up trial that is
  excluded. It reports the median of 5 trials.
- **Paced mode:** produces at 20,000 events/s, roughly 50 times a real race's rate,
  while 4 DuckDB threads query the lakehouse being written.

| Measurement | Result |
|---|---|
| Sustained throughput, burst (produced to processed by the engines) | **1,003,571 events/s** median of 5 (range 962,293-1,018,831) |
| Committed throughput, burst (until the last Parquet batch is durable) | **826,214 events/s** |
| Producer rate | 1,028,214 events/s |
| Cold start (first trial after process start, excluded) | 39,440 events/s in this run; 39k-155k across runs |
| Stream-to-trigger latency, burst | p50 6.0 ms, p95 9.0 ms, p99 11.0 ms (295 triggers) |
| Stream-to-trigger latency, paced at 20k events/s | **p50 2.0 ms, p95 3.0 ms, p99 4.0 ms** (308 triggers) |
| Produce-to-processed latency, paced (all 200,000 events) | p50 2.1 ms, p95 3.4 ms, p99 5.8 ms |
| Concurrent DuckDB queries during the paced run | **32,684 queries, 0 errors**, p50 1.06 ms, p95 1.94 ms |
| Lakehouse consistency during the paced run | row count never decreased; final telemetry rows = 200,000 |
| Sink and decode errors (all trials) | 0 and 0 |
| Monte Carlo, 10,000 rollouts x 6 strategies (60,000 trajectories), 15 workers, in process | p50 6.5 ms, p95 7.0 ms, p99 7.3 ms (200 runs) |
| The same over MCP (JSON-RPC 2.0 over HTTP, round trip) | p50 7.6 ms, p95 8.0 ms (100 calls) |
| Monte Carlo, 10,000 rollouts of one strategy | p50 1.0 ms |
| DuckDB OLAP over the offline lakehouse (332,787 events, 3,126 laps) | median p50 1.19 ms over 5 queries; decision-scoped model SQL p50 0.91 ms |

How to read this:
- **Processed vs committed.** The processed rate measures how fast the engines keep
  up. The committed rate also waits for the last Parquet batch to be renamed into
  place. The Parquet row count for a burst trial (201,507) is the sum of three tables:
  200,000 telemetry, 1,212 laps and 295 triggers.
- **"0 file-lock errors"** is measured, not assumed. The sink writes each batch under a
  temporary name and commits it with an atomic rename. DuckDB only globs committed
  `*.parquet` files, so readers never open a half-written file.
  `internal/parquet` has a dedicated test for this, with 242 files and 4,000 reads.
- **Burst latency is queueing.** The whole 200k batch is produced in about 0.2 s, so
  burst latency measures how fast 20 partitions drain a backlog. The paced figure is
  the one that matters for a live race.
- **Cold start.** The first trial after a process start is 6 to 25 times slower,
  because of cold consumer fetch sessions and the Go heap growing. It is reported, not
  averaged in (`CHANGELOG.md`).
- **Limits.** It runs on a single broker on one machine, with no replication.
  Producer, broker and consumer share the same 15 cores.

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

**Setup** (`python -m pitwall.eval.agent_eval`, raw output `benchmarks/agent_eval.json`):
- **Scenarios.** 7 are real decisions taken at the decision instant of the 2023
  Singapore, 2024 British and 2024 Sao Paulo races. 13 are synthetic traps:
  two-compound rule, penalty hold, VSC/SC stops, wet and drying crossovers, closed pit
  entry, DRS trains.
- **Arms.** Each scenario runs once per arm on qwen2.5-coder:14b, with llama3.2:3b as
  the fast model and failover.
  - *Single pass:* one primary-model call. It gets the same situation text, the same
    system-prompt rules (with article numbers from the edition in force) and the same
    output contract as the agent's synthesiser, but no lakehouse SQL, retrieval,
    simulator or verifier.
  - *Cyclic agent:* the full LangGraph graph (DuckDB SQL, Qdrant hybrid RAG, MCP Monte
    Carlo, synthesis, rule checker + llama3.2:3b critic, at most 2 revisions).
- **Faults.** 10% of primary-model calls fail by injection, seeded per scenario and
  arm.
- **Scoring.** Every final call is scored by the same deterministic rule checker under
  the edition in force (`rules.check`) and compared with the scenario's acceptable and
  forbidden sets.

| Metric | Single pass | Cyclic agent |
|---|---|---|
| FIA rule compliance (final call breaks no regulation or tyre-physics rule) | 60% (12/20) | **100% (20/20)** |
| Agreement with ground truth | 35% (7/20) | **85% (17/20)** |
| Agreement, real scenarios | 43% (3/7) | 86% (6/7) |
| Agreement, synthetic scenarios | 31% (4/13) | 85% (11/13) |
| Forbidden calls (a call the scenario marks illegal) | 5 | 0 |
| End-to-end latency p50 / p95 | 9.0 / 11.6 s | 10.4 / 12.7 s |
| Primary-model failures (injected + real) | 3 (2 + 1) | 3 (3 + 0) |
| Recovered by llama3.2:3b failover | 3/3 | 3/3 |
| Client-visible errors | 0 | 0 |

**Where the single pass broke the rules** (8 calls):
- 6 calls finished a dry race on one compound (two-compound rule).
- 1 call fitted intermediates on a drying track.
- 1 call contradicted itself: PIT_NOW_HARD with a MEDIUM target. This was GP03,
  served by llama3.2:3b after a real primary failure.

**What the numbers do and do not show:**
- **The gain comes from grounding, not from the cycle.** In all 20 agent runs the
  first proposal already passed the rule checker and the critic, so no revision was
  needed and the guardrail never overrode a call (0 revisions, 0 caught-and-corrected,
  0 overrides). The compliance gain over the single pass comes from what the
  synthesiser is given before it answers:
  - the simulator's table, which marks every plan legal or illegal;
  - the deterministic constraint lines;
  - the retrieved regulation text.

  The self-correction path is verified by the scripted-model tests in
  `test_agent.py` (an illegal MEDIUM to MEDIUM call caught, revised and finalised
  legal) and the replayed trace in Playwright. In this run, it did not change any
  outcome.
- **Failover.** 6 primary failures across both arms (5 injected, 1 real), all served
  by the fast model: 6/6 recovered, 0 client errors. That is a small sample. Recovery
  keeps the request alive but not its quality. In GP03, both of the agent's primary
  calls (SQL and synthesis) failed by injection and llama3.2:3b served them. It gave
  a compliant but weaker call: a stop for hards against the simulator's soft stop,
  while Sainz stayed out and won.
- **Disagreements are reported, not relabelled.** The agent's 3 disagreements:
  - **GP03 (real).** Sainz stayed out at Singapore 2023, but neither the simulator
    (softs) nor the agent (hards) did.
  - **GP11 and GP12 (synthetic).** The agent followed the simulator's best legal plan,
    a soft stint of 23 and 28 laps. The hand-written ground truth says softs would not
    last that long. In the simulator's tyre table the soft cliff starts at 20 laps and
    adds 350 ms/lap per lap beyond it. That prices a 23-lap soft stint about 5 s
    faster than mediums, and a 28-lap stint roughly level with hards. The labels were
    written before the run and are unchanged. The disagreement is a calibration gap
    between the simulator's tyre model and that reasoning.
- **Latency.** The agent costs 1.4 s more at p50 than one model call. Most of its time
  is the qwen2.5-coder:14b SQL and synthesis calls. The 60,000-trajectory simulation
  takes about 7 ms and retrieval about 0.2 s.
- **Fairness fix.** An earlier run of this benchmark gave the single pass a shorter
  prompt, without the rules or the action-to-compound mapping. It scored 35%
  compliance, and 6 of its 13 illegal calls contradicted their own action. That
  baseline was a strawman and was replaced (`CHANGELOG.md`).
