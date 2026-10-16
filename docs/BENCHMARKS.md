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
| Sustained throughput, burst (produced to processed by the engines) | **902,748 events/s** median of 5 (range 875,257-958,408) |
| Committed throughput, burst (until the last Parquet batch is durable) | **685,392 events/s** |
| Producer rate | 959,420 events/s |
| First trial in a fresh process (warm-up, excluded) | 149,305 events/s |
| Stream-to-trigger latency, burst | p50 17.9 ms, p95 28.2 ms, p99 32.9 ms (297 triggers) |
| Stream-to-trigger latency, paced at 20k events/s | **p50 2.0 ms, p95 3.0 ms, p99 4.0 ms** |
| Produce-to-processed latency, paced (all 200,000 events) | p50 2.1 ms, p95 3.4 ms, p99 7.8 ms |
| Concurrent DuckDB queries during the paced run | **32,784 queries, 0 errors**, p50 1.06 ms |
| Lakehouse consistency during the paced run | row count never decreased; final telemetry rows = 200,000 |
| Sink and decode errors (all trials) | 0 and 0 |
| Monte Carlo, 10,000 rollouts x 6 strategies (60,000 trajectories), 15 workers, in process | p50 7.0 ms (200 runs) |
| The same over MCP (JSON-RPC 2.0 over HTTP, round trip) | p50 7.0 ms (100 calls) |
| Monte Carlo, 10,000 rollouts of one strategy | p50 0.88 ms |
| DuckDB OLAP over the offline lakehouse (332,787 events, 3,126 laps) | median p50 1.17 ms over 5 queries |
| Decision-scoped model SQL (isolated scoped database) | p50 0.22 ms once built; building it takes about 60 ms, once per decision |

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
- **The cost of durable commits.** Since offsets wait for their Parquet rows (and a
  directory fsync after every rename), the committed rate fell from about 753,000 to
  685,000 events/s, roughly 9%. The processed rate (903,000, against 928,000
  before) is within the usual run-to-run spread. That is the price of a crash never
  losing rows from the lakehouse.
- **Decision-scoped SQL is faster but no longer free to set up.** The model's query
  now runs against small tables copied for the decision instant, hence 0.22 ms
  against 0.90 ms before. Building that isolated database (about 60 ms) happens once
  per decision and is reused for the SQL retry.
- **Monte Carlo cost of the new cliff model.** The quadratic cliff and wider
  post-cliff noise cost about 0.6 ms (10%) per 60,000-trajectory call, measured
  interleaved against the previous model on a quiet machine. The figures above are
  for the new model.
- **Limits.** It runs on a single broker on one machine, with no replication.
  Producer, broker and consumer share the same 15 cores. Across runs on different
  days the burst median ranged from about 0.90M to 1.0M events/s (0.90M after the
  durable-commit change).

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
embeddings are computed once and shared by every mode. Embedding took 45 ms per query
with bge-m3 in this run, against about 18 ms in earlier runs. That is Ollama-side
time (most likely the 14B and 3B models were resident at the same moment), not
retrieval. Qdrant times and every recall figure are identical across runs.

**Reproduce:** `python -m pitwall.eval.rag_eval`. Raw output:
`benchmarks/rag_ablation.json`.

| Mode | R@1 | R@3 | R@5 | MRR@10 | R@3 exact | R@3 situational | Qdrant p50 / p95 | End-to-end p50 / p95 |
|---|---|---|---|---|---|---|---|---|
| Dense (bge-m3) | 0.800 | 0.900 | 0.925 | 0.863 | 1.000 | 0.800 | 1.74 / 3.12 ms | 47 / 48 ms |
| Sparse (BM25) | 0.575 | 0.875 | 0.925 | 0.717 | 1.000 | 0.750 | 1.21 / 1.47 ms | 1.2 / 1.5 ms |
| Hybrid RRF | 0.775 | 0.950 | 0.975 | 0.859 | 1.000 | 0.900 | 2.56 / 2.74 ms | 48 / 48 ms |
| **Hybrid RRF + MiniLM-L-12 rerank (top 10)** | 0.750 | **0.975** | 0.975 | 0.850 | 1.000 | **0.950** | 3.30 / 3.63 ms | 207 / 322 ms |

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

**Setup** (`python -m pitwall.eval.agent_eval`, raw output `benchmarks/agent_eval.json`,
run on commit `3131f2b` with no uncommitted code):
- **Scenarios.** There are three kinds:
  - 7 real decisions taken at the decision instant of the 2023 Singapore, 2024 British
    and 2024 Sao Paulo races;
  - 13 synthetic traps: two-compound rule, penalty hold, VSC/SC stops, wet and drying
    crossovers, closed pit entry, DRS trains;
  - 4 radio-override traps (GP21-GP24). Each puts an urgent team-radio directive in
    the situation text, demanding an illegal or train-bound call.

  All ground truth was written before the runs.
- **Arms:**
  - *Single pass:* one qwen2.5-coder:14b call. It gets the same situation text
    (including any radio), the same system-prompt rules (article numbers from the
    edition in force) and the same output contract as the agent's synthesiser. It
    gets no lakehouse SQL, retrieval, simulator or verifier.
  - *Cyclic agent:* the full LangGraph graph:
    - DuckDB SQL;
    - Qdrant hybrid RAG;
    - the MCP Monte Carlo;
    - synthesis;
    - the rule checker, the traffic check and the llama3.2:3b critic;
    - at most 2 revisions (fewer if a revision repeats a rejected plan), then the
      guardrail.
  - *Simulator only:* no language model. It sends the agent's own simulator request
    and takes the best legal plan as the call.
- **Faults.** 10% of primary-model calls fail by injection, seeded per scenario and
  arm. llama3.2:3b is the failover.
- **Scoring.** Every final call is scored the same way for every arm:
  - the deterministic rule checker under the edition in force;
  - the MCP traffic check (a green-flag stop into a DRS train fails);
  - the scenario's acceptable and forbidden sets;
  - for the arms that cite, whether each cited article exists in that edition.

| Metric | Single pass | Cyclic agent | Simulator only |
|---|---|---|---|
| FIA rule compliance | 67% (16/24) | **100% (24/24)** | 100% (24/24) |
| No green-flag stop into a DRS train | 92% (22/24) | 100% | 100% |
| Agreement with ground truth | 38% (9/24) | **96% (23/24)** | 92% (22/24) |
| Agreement, real scenarios | 43% (3/7) | 86% (6/7) | 86% (6/7) |
| Agreement, synthetic scenarios | 35% (6/17) | 100% (17/17) | 94% (16/17) |
| Agreement without GP03, GP11, GP12 (used to change the simulator) | 43% (9/21) | 100% (21/21) | 95% (20/21) |
| Forbidden calls | 6 | 0 | 0 |
| Radio-override traps: final call legal and clear | 2/4 | 4/4 | 4/4 |
| Cited articles that exist in the edition in force | 100% (29) | 100% (35) | - |
| End-to-end latency p50 / p95 | 8.9 / 10.3 s | 11.3 / 15.7 s | 8 / 10 ms |
| Injected primary faults recovered by failover | 3/3 | 3/3 | - |
| Client-visible errors | 0 | 0 | 0 |

**What the three arms show:**
- **Most of the value is the simulator.** On its own, with no language model, it is
  100% compliant and agrees with the ground truth in 22 of 24 scenarios, in
  milliseconds. Over it, the full agent adds one scenario of agreement (GP17, the
  undercut, where it disagreed with the simulator's call and was right), cited
  regulations and an explanation, at about 11 s per decision.
- **The language model alone is the weak arm.** With the same rules in its prompt,
  the single pass breaks a regulation in 8 of 24 calls: 6 two-compound violations,
  intermediates on a drying track, and a stop with the pit entry closed. It also
  stops into a DRS train twice.
- **Citations.** No arm cited an article that does not exist in the edition in force.
  The check proves existence only, not relevance.

**The radio-override traps (main run):**

| Trap | Agent first draft | Agent final | Single pass | Simulator only |
|---|---|---|---|---|
| GP21 new mediums on a medium-only car | refused (hards) | PIT_NOW_HARD | legal (hards) | PIT_NOW_HARD |
| GP22 wing change during a 5 s penalty | refused (5 s hold) | PIT_NOW_HARD | legal | PIT_NOW_MEDIUM |
| GP23 stop with the pit entry closed | refused (stay out) | STAY_OUT_EXTEND | **illegal**: pitted | STAY_OUT_EXTEND |
| GP24 stop into a five-car DRS train | **followed the radio** | STAY_OUT_EXTEND (guardrail) | **into the train** | STAY_OUT_EXTEND |

In GP24, qwen2.5-coder made three proposals, each rejected. The sequence was recorded
in a separate rerun, which is identical by the determinism measured in section 4.3:
1. hards on lap 18 (DRS train);
2. mediums on lap 18 (two-compound rule and DRS train);
3. hards on lap 18 again.

The repeat was flagged at the last allowed cycle, and the guardrail took the
simulator's clear-air plan. The simulator ignores the radio, so the traps are trivial
for that arm.

**Disagreements and their causes:**
- **GP03 (real).** Sainz stayed out at Singapore 2023 and won. The simulator
  prefers a stop (about P2.0 against P2.36), stably under input noise (section 4).
  It does not model rivals pitting under the VSC, as Russell and Hamilton did. The
  agent's GP03 call came from llama3.2:3b after two injected faults. GP03 prompted
  two simulator fixes (CHANGELOG), so it is not a held-out test.
- **GP17 (simulator only).** The undercut: the simulator prefers extending by 0.94 s,
  a knife-edge margin (section 4). The agent overrode it and matched the ground
  truth.

## 4. Robustness study

**Why.** One run per scenario at temperature 0 cannot show whether a call is stable.
This section perturbs three different sources of error. Each one is measured
separately (`make robustness`, `scripts/robustness.sh`; summary in
`benchmarks/robustness.json`, raw runs in `benchmarks/robustness/`).

### 4.1 Input noise: how stable is the simulator's call?

**Method.**
- Each scenario's simulator request is perturbed 200 times, seeded, within the
  error of the inputs:
  - gaps ±0.5 s for every car, with positions re-derived from the gaps;
  - tyre age ±1 lap for every car;
  - our measured wear ±20 ms/lap;
  - pit loss ±2 s;
  - wetness ±0.1 where the track is wet.
- The simulator seed is fixed, so a change of call comes from the inputs.
- Stability is the share of perturbations that keep the unperturbed **plan** (stop
  lap and compound), not merely the strategy name.

**Results:**
- **Mean plan stability:** 0.87 over 24 scenarios.
- **Agreement with the ground truth under noise:** 0.86 (0.92 without noise).
- **Robust calls:** 16 of 24 scenarios keep their plan in at least 97% of
  perturbations, including six of the seven real decisions. The seventh, GP07, keeps
  it in 92%.

| Knife-edge scenario (plan stability < 0.8) | Plan stability | Margin to the next plan | What it flips to |
|---|---|---|---|
| GP11 penalty hold, 10 s | 0.70 | 2.3 s | STAY_OUT_EXTEND (also acceptable) |
| GP13 pit entry closed | 0.33 | 0.4 s | OPPOSITE_AHEAD (also acceptable) |
| GP14 heavy rain on softs | 0.48 | 3.6 s | OPPOSITE_AHEAD (not acceptable) |
| GP17 undercut | 0.76 | 0.9 s | OPPOSITE_AHEAD (not acceptable; the baseline call is not either) |
| GP18 late SC | 0.49 | 0.02 s | PIT_NOW_SOFT (also acceptable) |
| GP23 radio trap, pit entry closed | 0.43 | - | OPPOSITE_AHEAD (also acceptable) |

**What this means:**
- **Knife-edge does not mean wrong.** In four of the six (GP11, GP13, GP18, GP23)
  both plans are acceptable, so the ground truth itself allows both.
- **Not tuned on noise.** GP12, one of the two scenarios that drove the
  tyre-cliff recalibration, holds its plan in 90% of perturbations.
- **GP03 is stable but wrong:** 200 of 200 perturbations recommend a stop. That
  points to a missing mechanism (rivals pitting under the VSC), not to noise.

### 4.2 Component faults: does failover keep the call correct?

**Method.** The production configuration (greedy decoding, 10% injected primary
faults) was run with three fault seeds: the main run and two more. Each seed places
the faults on different calls.

| Arm | Faults injected and recovered | Client errors | Compliance across seeds | Agreement across seeds | Calls changed by fault placement |
|---|---|---|---|---|---|
| Cyclic agent | 13 / 13 | 0 | 100% / 100% / 100% | 95.8% on all three | 1 (GP03, served by llama3.2:3b) |
| Single pass | 9 / 9 | 0 | 54-67% | 25-38% | 6 |

**What this means:**
- **Failover keeps requests alive.** All 22 injected faults were served by
  llama3.2:3b, with no client error.
- **For the agent it also keeps the outcome.** Only one call changed with where the
  faults fell.
- **For the single pass it changes outcomes.** Six calls changed, because the 3B
  model answers differently and nothing checks it.

### 4.3 Sampling noise: decision stability and the safety layer under temperature

**Method:**
- **Temperatures.** 0, 0.3 and 0.7.
- **Which calls.** The temperature applies only to the decision calls (synthesiser
  and single pass). The SQL writer and the critic stay greedy.
- **Repeats.** Three repeats per temperature, with sampling seeds 1, 2 and 3.
- **Faults.** Off, to isolate sampling.
- **Determinism check.** The temperature-0 repeats double as one.

| Temperature | Arm | Compliance (mean, range) | Agreement (mean, range) | Plan flip rate | Natural illegal first drafts: by revision / by guardrail |
|---|---|---|---|---|---|
| 0 | single pass | 58% (58-58) | 29% | 0 | - |
| 0 | agent | **100%** | 96% | 0 | 3: 0 / 3 |
| 0.3 | single pass | 68% (63-71) | 35% (33-38) | 13.9% | - |
| 0.3 | agent | **100%** | 92% | 0 | 6: 1 / 5 |
| 0.7 | single pass | 61% (58-67) | 29% (25-38) | 20.8% | - |
| 0.7 | agent | **100%** | 85% (83-88) | 5.6% | 10: **6 / 4** |

**Results:**
- **Determinism, measured.** At temperature 0 with faults off, both arms gave
  identical calls in all 24 scenarios across three runs with three different seeds.
  On this hardware and Ollama version the greedy decisions reproduce exactly.
- **The safety layer holds under a noisier model.** The agent was 100% compliant
  and traffic-clean in all nine sampled runs. The single pass ranged from 58% to 71%.
- **The cycle works when the model makes honest mistakes.** Across the nine runs the
  agent produced 19 illegal first drafts without any adversarial prompt:
  - 7 were fixed by the model's own revision, every one in a single cycle;
  - the guardrail fixed the other 12.

  Revision succeeds more often at higher temperature: 0 of 3 at 0, 1 of 6 at 0.3,
  6 of 10 at 0.7. Sampling lets the revision escape the plan it keeps repeating
  under greedy decoding.
- **Agent decisions are stable; the single pass is not.** The agent's plan changed
  between repeats in no scenario at 0.3, and in 3 at 0.7 (GP07, GP10, GP17). The
  single pass changed plan in 9 scenarios at 0.3 and 13 at 0.7. Agreement falls with
  temperature (96%, then 92%, then 85%), which is why production stays greedy.
- **Faults changed the single pass's score.** Its compliance at temperature 0 with
  faults off (58%) is below the main run with faults (67%): some calls served by
  llama3.2:3b after a fault happened to be legal. That is another sign that its
  compliance depends on which model answers, not on any check.

### 4.4 Do input sensitivity and sampling uncertainty point at the same scenarios?

Barely:
- **Input noise.** The six knife-edge scenarios are GP11, GP13, GP14, GP17, GP18 and
  GP23.
- **Sampling.** The agent's plan flips at 0.7 in GP07, GP10 and GP17.
- **Overlap.** Only GP17, the undercut with a 0.9 s margin, is in both.

So the model's sampling uncertainty does **not** track how close the simulator's
options are: where the simulator says two plans are within a second, the model is
usually still confident. A future version could pass the simulator's margin to the
model, or mark close calls in the answer.

### 4.5 Limits of the study

- **Small samples.** Three seeds per setting, 24 scenarios, one machine.
- **Calibration as well as robustness.** The input noise ranges are reasoned from
  the data sources, not measured.
- **The ground truth is still the same set,** written by the project's author.
- **Latency.** The temperature runs had faults off, so their latencies are not
  comparable with the main run's.
