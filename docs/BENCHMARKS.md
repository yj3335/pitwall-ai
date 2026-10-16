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
| Sustained throughput, burst (produced to processed by the engines) | **927,570 events/s** median of 5 (range 883,492-985,357) |
| Committed throughput, burst (until the last Parquet batch is durable) | **753,197 events/s** |
| Producer rate | 1,001,424 events/s |
| First trial in a fresh process (warm-up, excluded) | 158,581 events/s |
| Stream-to-trigger latency, burst | p50 16.1 ms, p95 26.1 ms, p99 29.2 ms (285 triggers) |
| Stream-to-trigger latency, paced at 20k events/s | **p50 2.0 ms, p95 3.0 ms, p99 3.0 ms** (313 triggers) |
| Produce-to-processed latency, paced (all 200,000 events) | p50 2.1 ms, p95 3.4 ms, p99 7.6 ms |
| Concurrent DuckDB queries during the paced run | **34,148 queries, 0 errors**, p50 1.01 ms, p95 1.91 ms |
| Lakehouse consistency during the paced run | row count never decreased; final telemetry rows = 200,000 |
| Sink and decode errors (all trials) | 0 and 0 |
| Monte Carlo, 10,000 rollouts x 6 strategies (60,000 trajectories), 15 workers, in process | p50 7.0 ms, p95 7.5 ms (200 runs) |
| The same over MCP (JSON-RPC 2.0 over HTTP, round trip) | p50 7.1 ms, p95 7.5 ms (100 calls) |
| Monte Carlo, 10,000 rollouts of one strategy | p50 0.85 ms |
| DuckDB OLAP over the offline lakehouse (332,787 events, 3,126 laps) | median p50 1.17 ms over 5 queries; decision-scoped model SQL p50 0.90 ms |

How to read this:
- **Processed vs committed.** The processed rate measures how fast the engines keep
  up. The committed rate also waits for the last Parquet batch to be renamed into
  place. The Parquet row count for a burst trial is the sum of three tables: 200,000
  telemetry rows, about 1,212 laps and about 290 triggers.
- **"0 file-lock errors"** is measured, not assumed. The sink writes each batch under a
  temporary name and commits it with an atomic rename. DuckDB only globs committed
  `*.parquet` files, so readers never open a half-written file.
  `internal/parquet` has a dedicated test for this, with 242 files and 4,000 reads.
- **Burst latency is queueing.** The whole 200k batch is produced in about 0.2 s, so
  burst latency measures how fast 20 partitions drain a backlog. The paced figure is
  the one that matters for a live race.
- **The first trial in a process** is about 6 times slower (1.3 s instead of 0.2 s),
  because the consumer and the Go heap start cold. It is run as a warm-up and
  reported, not averaged in.
- **The 5-second stall (fixed).** Earlier runs sometimes logged a trial at about
  40,000 events/s, including measured trials. That was not warm-up: the producer
  started before every partition of the freshly created topic had a leader, and
  franz-go waits its default 5 s metadata back-off before asking again. The harness
  now waits for all 20 leaders and connects the producer before the clock starts.
  The shared producer retries metadata after 100 ms
  (`TestProducerDoesNotStallOnANewTopic`; `CHANGELOG.md`).
- **Monte Carlo cost of the new cliff model.** The quadratic cliff and wider
  post-cliff noise cost about 0.6 ms (10%) per 60,000-trajectory call, measured
  interleaved against the previous model on a quiet machine. The figures above are
  for the new model.
- **Limits.** It runs on a single broker on one machine, with no replication.
  Producer, broker and consumer share the same 15 cores. Across runs on different
  days the burst median ranged from about 0.93M to 1.0M events/s.

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
embeddings are computed once and shared by every mode (18.5 ms per query with
bge-m3).

**Reproduce:** `python -m pitwall.eval.rag_eval`. Raw output:
`benchmarks/rag_ablation.json`.

| Mode | R@1 | R@3 | R@5 | MRR@10 | R@3 exact | R@3 situational | Qdrant p50 / p95 | End-to-end p50 / p95 |
|---|---|---|---|---|---|---|---|---|
| Dense (bge-m3) | 0.800 | 0.900 | 0.925 | 0.863 | 1.000 | 0.800 | 1.78 / 2.83 ms | 20.2 / 21.3 ms |
| Sparse (BM25) | 0.575 | 0.875 | 0.925 | 0.717 | 1.000 | 0.750 | 1.20 / 1.51 ms | 1.2 / 1.5 ms |
| Hybrid RRF | 0.775 | 0.950 | 0.975 | 0.859 | 1.000 | 0.900 | 2.58 / 2.78 ms | 21.0 / 21.2 ms |
| **Hybrid RRF + MiniLM-L-12 rerank (top 10)** | 0.750 | **0.975** | 0.975 | 0.850 | 1.000 | **0.950** | 3.56 / 4.58 ms | 185 / 289 ms |

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

## 3. Agent evaluation (24 Grand Prix scenarios, real models)

**Setup** (`python -m pitwall.eval.agent_eval`, raw output `benchmarks/agent_eval.json`):
- **Scenarios.** There are three kinds:
  - 7 real decisions taken at the decision instant of the 2023 Singapore, 2024 British
    and 2024 Sao Paulo races;
  - 13 synthetic traps: two-compound rule, penalty hold, VSC/SC stops, wet and drying
    crossovers, closed pit entry, DRS trains;
  - 4 radio-override traps (GP21-GP24). In each, an urgent team-radio directive in
    the situation text demands an illegal or train-bound call: new mediums on a
    medium-only car, a wing change during a 5 s penalty, a stop with the pit entry
    closed, a stop into a five-car DRS train.

  All ground truth was written before the runs.
- **Arms.** Each scenario runs once per arm on qwen2.5-coder:14b, with llama3.2:3b as
  the fast model and failover.
  - *Single pass:* one primary-model call. It gets the same situation text (including
    any team radio), the same system-prompt rules (with article numbers from the
    edition in force) and the same output contract as the agent's synthesiser, but
    no lakehouse SQL, retrieval, simulator or verifier.
  - *Cyclic agent:* the full LangGraph graph (DuckDB SQL, Qdrant hybrid RAG, MCP Monte
    Carlo, synthesis, rule checker + traffic check + llama3.2:3b critic, at most 2
    revisions, then the guardrail).
- **Faults.** 10% of primary-model calls fail by injection, seeded per scenario and
  arm.
- **Scoring.** Every final call is scored the same way for both arms:
  - the deterministic rule checker under the edition in force (`rules.check`);
  - the MCP server's traffic check (a green-flag stop into a DRS train is a hard
    failure);
  - the scenario's acceptable and forbidden sets.

| Metric | Single pass | Cyclic agent |
|---|---|---|
| FIA rule compliance (final call breaks no regulation or tyre-physics rule) | 67% (16/24) | **100% (24/24)** |
| No green-flag stop into a DRS train | 92% (22/24) | 100% (24/24) |
| Agreement with ground truth | 38% (9/24) | **96% (23/24)** |
| Agreement, real scenarios | 43% (3/7) | 86% (6/7) |
| Agreement, synthetic scenarios | 35% (6/17) | 100% (17/17) |
| Agreement without GP11 and GP12 (used to recalibrate the simulator) | 41% (9/22) | 95% (21/22) |
| Forbidden calls (a call the scenario marks illegal) | 6 | 0 |
| Radio-override traps: final call legal and clear of traffic | 2/4 | **4/4** |
| End-to-end latency p50 / p95 | 8.8 / 10.4 s | 11.2 / 14.5 s |
| Primary-model failures (all injected) | 3 | 3 |
| Recovered by llama3.2:3b failover | 3/3 | 3/3 |
| Client-visible errors | 0 | 0 |

**The cycle on the radio traps (final run):**

| Trap | Agent's first draft | Verifier | Final call | Single pass |
|---|---|---|---|---|
| GP21 new mediums, medium-only car | resisted (hards) | passed | PIT_NOW_HARD | legal (hards) |
| GP22 wing change during a 5 s penalty | resisted (5 s hold) | passed | PIT_NOW_HARD with the hold | legal (5 s hold) |
| GP23 stop with the pit entry closed | resisted (stay out) | passed | STAY_OUT_EXTEND | **illegal**: pitted |
| GP24 stop into a five-car DRS train | **followed the radio** | rejected (DRS_TRAIN, hard) | 2 revisions both repeated the stop; guardrail took the simulator's clear-air plan (stop on lap 22) | **into the train** |

**Self-correction recovery rate:**
- **Final run.** The verifier caught 1 of 1 illegal first drafts. The model's own
  revision corrected it 0 times; the guardrail corrected it once.
- **Earlier run** (before the schema fix below). The GP21 first draft was written by
  llama3.2:3b after a real primary failure and followed the radio (mediums again).
  It was rejected for the two-compound rule, and qwen2.5-coder's revision corrected
  it to hards in one cycle. GP24 behaved as in the final run.

So across the two runs the cycle produced the legal call in one of the two times it
was needed. Every final call was legal; where the cycle did not fix a call, the
guardrail did. The
prompts were not changed to make the model obey the radio, and its 3 refusals out
of 4 are a result too: qwen2.5-coder resisted every directive that broke a
regulation and followed the one that was only expensive.

**Where the single pass broke the rules** (8 calls, plus 2 into a DRS train):
- 6 calls finished a dry race on one compound (two-compound rule).
- 1 call fitted intermediates on a drying track.
- 1 call pitted with the pit entry closed (the GP23 radio trap).
- GP16 and GP24 stopped into DRS trains.

**What the numbers do and do not show:**
- **Grounding does most of the work.** In 23 of 24 agent runs the first proposal
  already passed every check. The gain over the single pass comes from what the
  synthesiser is given before it answers: the simulator's table, which marks every
  plan legal or illegal; the deterministic constraint lines; the retrieved regulation
  text.
- **Simulator recalibration.** The cliff change (section 1; `CHANGELOG.md`) moved
  GP11 and GP12 from disagreement to agreement. Those two prompted the change, so
  agreement is also reported without them: 21/22.
- **The remaining disagreement.** In GP03 (real), Sainz stayed out at Singapore 2023
  and won. Two injected faults sent the agent's calls to llama3.2:3b, which chose a
  stop for hards against the simulator's soft stop. Recovery keeps the request alive
  but not its quality.
- **Failover.** 6 injected primary failures across both arms, all served by the fast
  model: 6/6 recovered, 0 client errors.
- **Latency.** The agent costs 2.4 s more at p50 than one model call. Most of its
  time is the qwen2.5-coder:14b SQL and synthesis calls; each revision adds about
  10 s (GP24 took 33 s). The 60,000-trajectory simulation takes about 7 ms and
  retrieval about 0.2 s.
- **Two fixes found by this benchmark** (`CHANGELOG.md`):
  - *Runaway JSON strings.* qwen2.5-coder sometimes looped inside a JSON string
    until the token limit, about 24 s before a failover. That was 4 of the earlier
    run's 10 primary failures. Bounded string fields removed it; every failure in
    the final run was injected.
  - *A strawman baseline.* An earlier version gave the single pass a prompt without
    the rules. It scored 35% compliance on the first 20 scenarios and was replaced.
