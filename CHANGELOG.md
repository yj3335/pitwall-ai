# Changelog

Engineering log for PitWall AI. Every entry under "Fixed" is a defect that was
actually hit while building and verifying the system, with how it was found.

## Unreleased

### Added in the handoff audit
- **A simulator-only arm in the agent benchmark** (`--modes sim_only`). It uses no
  language model: it sends the same request as the agent's Monte Carlo node and
  takes the simulator's best legal plan as the call, scored by the same judge. It
  answers what the LLM layers add over the simulator itself. The handoff audit had
  estimated this from recorded simulator picks.

- **Citation validity.** `regulations.check_citations` looks up every cited article
  and paragraph in the edition in force. A citation with no article number, such as
  "UNSAFE RELEASE", counts as invalid. Void articles are not citable. The verifier
  reports invalid citations as an advisory, and the benchmark reports citation
  validity for each arm. It checks existence, not relevance.

- **Revision stops when it repeats itself.** The verifier records every rejected
  plan with its violation codes, and the revision prompt lists all of them, not
  only the last. A revision that proposes an already-rejected plan cannot
  converge, so the graph skips the remaining cycle and the guardrail takes the
  best legal simulator plan. Each skipped cycle saves about 10 s on the real model.
  In the GP24 benchmark run, qwen2.5-coder repeated the same rejected stop twice.

### Fixed in the handoff audit
- **Defending cost nothing to the attacker.** The simulator applied overtaking
  difficulty only when our car attacked. A faster rival behind us finished ahead
  on raw race time, as if passing were free, so on a street circuit staying out in
  the lead was undervalued. In GP03 (Sainz leading Singapore 2023 under the VSC)
  staying out was projected to finish P5. A rival behind now has to pass on track
  under the same model, or gets by through a stop of its own.
  `TestLeaderDefendsOnAHardToPassCircuit` fails without the change. Found by
  reading the simulator's output for GP03, the one real scenario the agent and
  the simulator both got wrong.
- **Kafka offsets were committed before their Parquet rows were durable.** An
  offset was marked as soon as the engine had handled the record, while its rows
  waited up to 2 s in the sink buffers. On shutdown the processor also committed
  before it flushed. So a hard crash lost the buffered rows from the lakehouse, and
  the writer did not fsync the directory after the rename.
  - **Barriers.** Each partition handler now queues a barrier behind its rows in
    every sink, and the batch's offsets are marked only when all sinks have
    committed those rows. That includes a directory fsync after each rename.
  - **Failed writes.** After a failed write no barrier runs again, so offsets never
    pass lost rows.
  - **Shutdown order.** Flush, commit, then leave the group.
  - **Proof.** A live-broker test shows nothing committed while rows are buffered.
    It fails if records are acknowledged immediately.
- **Model SQL could read the future.** The point-in-time scope shadowed `laps`,
  `telemetry` and `triggers` with CTEs over the full views. A schema-qualified name
  went around the CTE:
  - `SELECT max(lap_number) FROM main.laps` returned lap 61 at a lap-44 decision;
  - `memory.main.laps` did the same;
  - `parquet_scan()` was missing from the blocklist and read the files directly.

  No recorded model query did this, but the docs claimed it was impossible. Found by
  probing the guard while writing the handoff report. Model SQL now runs in an
  isolated DuckDB database that holds only the scoped rows, with external access
  disabled and the configuration locked. The guard also rejects schema-qualified
  names and every Parquet reader. Tests try each escape against both layers.

### Changed after the first full benchmark
- **The simulator's tyre cliff is quadratic and earlier.** It was linear, with the soft
  at 20 laps and +350 ms/lap per lap. Under that model the agent's GP11 and GP12 calls
  (23- and 28-lap soft stints) came out about 5 s faster than mediums and level with
  hards, although no soft set in the recorded races ran more than 20 laps. The cliff
  is now `80 ms x (age - cliff)^2` for every slick, with ages soft 15, medium 27 and
  hard 40, and lap noise widens by 15% per lap past it. The ages are an assumption
  bounded by the recorded stints, and a test pins that bound. The lap-time data is too
  confounded to fit the curve. Because GP11 and GP12 prompted the change, they are no
  longer a held-out test of the simulator: the benchmark also reports agreement
  without them.
- **Four radio-override traps (GP21-GP24).** In the first full benchmark the agent
  never revised a call, so the cycle had not been exercised on a real model. Each
  trap adds an urgent `team_radio` directive asking for an illegal or train-bound
  call. Both arms see it in the situation text. The prompts were not changed to make
  the model obey it.
- **The benchmark judge checks traffic too.** It now asks the MCP server's
  `validate_physics_and_traffic` whether a final green-flag stop rejoins in a DRS
  train, the same hard check the verifier applies, for both arms. Agent runs whose
  first draft was illegal are now split by how they ended: corrected by the model's
  own revision (and in how many cycles), or rescued by the guardrail once the
  reflection budget was spent. The first measured trap run had one of each. In the
  final run, the one illegal first draft (GP24, into a DRS train) was rescued by the
  guardrail.
- The scenario list API and the dashboard show a scenario's team radio. The
  scenario-panel counts are derived from the data instead of hardcoded.

### Fixed after the first full benchmark
- **qwen2.5-coder looped inside a JSON string until the token limit.** The benchmark
  showed 4 primary-model failures that were not injected, each taking about 24 s
  before failover. Reproduced with the app's exact sampling settings (temperature 0,
  seed 42): the GP23 single pass wrote a valid start, then an endless run of hashtags
  inside `engineer_radio_message` until all 600 tokens were spent, leaving
  unterminated JSON. Every free-text field in the proposal, critic and SQL schemas
  now has a `maxLength`, and every array a `maxItems`. Ollama enforces both in the
  sampling grammar, so the same prompt returns valid JSON in about 8 s on the primary
  model. Failure reasons are now recorded per run (`llm.errors` in
  `agent_eval.json`).
- **That fix broke every real-race agent run.** A 2,000-character `maxLength` on the
  SQL field makes Ollama fail to compile the grammar ("failed to parse grammar",
  HTTP 400) on both models. 1,500 compiles. Only real scenarios call the SQL node, so
  the unit test (bounds exist) and the synthetic end-to-end test both passed while
  all 7 real runs errored. Found by reading the benchmark output before using it. The
  limit is now 1,200 (recorded agent SQL is about 200 characters), and a live test
  sends every schema to both models.
- **A 5-second producer stall on new topics.** A later benchmark run had a measured
  burst trial at 39,719 events/s, after the warm-up that was meant to absorb such
  trials. Its producer had taken 5.015 s to flush, against about 0.2 s for every
  other trial. The cause: the harness produced to a just-created topic before every
  partition had a leader. franz-go treats that as a metadata error and waits its
  default 5 s `MetadataMinAge` before asking again, holding every buffered record.
  The same stall explains the earlier 39k "cold starts". The harness now waits for all
  20 partition leaders and pings the producer before the clock starts. The shared
  producer (`kafka.NewProducer`, also used by the replayer and processor) retries
  metadata after 100 ms. `TestProducerDoesNotStallOnANewTopic` passes with the fix and
  fails with the old back-off. The genuine first-trial cost that remains is about
  158k events/s against about 930k.
- **The cliff model's extra map lookup in the rollout hot loop.** It is now looked up
  once per stint. With the quadratic cliff and noise growth, a 60,000-trajectory call
  costs about 0.6 ms (10%) more than before, measured interleaved on a quiet machine.

### Added
- Self-contained toolchain in `.venv/` (`scripts/bootstrap-venv.sh`): Go 1.27.2,
  Python 3.13 virtualenv, Qdrant 1.19.2, Temurin JDK 21, Apache Kafka 4.4.0 in
  KRaft mode (combined broker + controller, no ZooKeeper), npm and Playwright caches.
- `scripts/services.sh up|down|status` runs Kafka and Qdrant from `.venv/` and
  creates `f1.telemetry.raw` (20 partitions), `f1.race_control` (1) and `f1.triggers` (20).
- OpenF1 fetcher (`scripts/fetch-openf1.py`) for the 2024 British GP (9558),
  2023 Singapore GP (9165) and 2024 Sao Paulo GP (9636): 332,787 replayable events
  joined from `car_data`, `laps`, `stints`, `intervals`, `position`, `weather` and
  `race_control`, plus the raw tables, all committed for offline runs.
- FIA corpus builder (`scripts/build-fia-corpus.py`): one edition per Sporting
  Regulations issue actually needed, each tagged with the window in which it was
  in force. 2023 Issue 6 (482 chunks), 2024 Issue 6 (481), 2024 Issue 7 (500) and
  2025 Issue 5 (533), plus 67 dated race-control notices, 11 verified precedent
  digests, 3 operational notes and 40 labelled retrieval queries. The 2026
  regulations are opt-in (`--include-2026`).
- `python/pitwall/regulations.py`: picks the edition in force for a race date and
  resolves clause citations (two-compound rule, penalty hold, SC/VSC pit entry,
  red-flag tyre change, pit entry closed) from that edition's text.
- `scripts/measure-pit-loss.py`: field-relative green-flag pit loss per race from
  the lakehouse (`data/races/*/pit_loss.json`).
- Go stream engine: per-driver 5-lap fuel-corrected degradation windows,
  undercut / tyre-cliff / SC-VSC cheap-stop / wet-crossover triggers, lock-free
  `Board` of `atomic.Pointer` snapshots, atomic Parquet batch writer, replayer,
  and a processor serving live HTTP state on :8090.
- Go Monte Carlo simulator and MCP server (stdio, Streamable HTTP `POST /mcp`,
  legacy `GET /sse`) with `simulate_pit_strategies`, `compute_undercut_delta`,
  `validate_physics_and_traffic` and `get_live_driver_window`.
- Simulator models for overtaking difficulty (a faster car must still pass on
  track; a failed pass pins the finish), per-rival observed degradation, and
  rivals switching tyres when the track turns wet or dry.
- DuckDB OLAP layer with a read-only guard for model-written SQL and
  point-in-time scoping (`AsOf`) of the `laps`, `telemetry` and `triggers` views.
- Qdrant dual-vector index (bge-m3 dense + BM25 sparse with server-side IDF),
  server-side RRF fusion, cross-encoder rerank, edition and point-in-time filters.
- Cyclic LangGraph agent (`MemorySaver` checkpoints, at most two self-correction
  cycles), deterministic FIA rule and traffic guardrail, llama3.2:3b critic,
  qwen2.5-coder:14b -> llama3.2:3b failover with a circuit breaker and seeded
  fault injection, async MCP client, single-pass baseline, and 20 scenarios.

### Added in milestone 8 (control plane)
- FastAPI control plane on :8080: scenarios, strategy runs streamed over Server-Sent
  Events (ordered event log with `Last-Event-ID` resume and heartbeats), checkpoint
  history, synchronous runs, live tower and an SSE live feed, and race-control parse and
  trigger endpoints.
- Race-control trigger path: message, structured event, synchronous publish to
  `f1.race_control` through the processor's new `POST /v1/race-control`, then a run on
  the live, point-in-time state of the affected car.
- `make run` / `make stop` / `make status` (`scripts/run-stack.sh`).

### Fixed in milestone 8
- **The verifier vetoed the Monte Carlo under a VSC.** A live trigger run showed the
  deterministic DRS-train check hard-blocking a VSC stop three times, against a
  simulator that already prices rejoin traffic and track position. Deferring a stop
  under SC/VSC also gives up the cheaper neutralised stop. The traffic check is now
  enforced under green flag only and advisory under SC/VSC.
- **`finalize` claimed an override that changed nothing.** After the reflection budget
  it "overrode" a call with the simulator's best plan, which was the same plan. It now
  takes the best simulator plan that passes every hard check, or reports the
  violations as unresolved.
- **Blocking retrieval on the event loop.** The Qdrant client, embedding call and
  cross-encoder ran inside the async graph node and would freeze every SSE stream
  during retrieval. They now run in a worker thread.
- **Request bodies were treated as query parameters.** With `from __future__ import
  annotations`, Pydantic models declared inside the app factory could not be resolved
  by FastAPI. They are now module-level.
- **SSE tests passed while testing nothing streaming.** `httpx.ASGITransport` buffers
  the whole response, so the endless live feed hung the suite and the run-stream
  assertions saw a buffered body. The tests now run the app on a real uvicorn socket.
- **`make stop` reported success while Kafka was still running.** It now waits for the
  ports to close.

### Added in milestone 9 (dashboard)
- Next.js pit-wall dashboard: stream KPIs, live timing tower with lettered tyre badges
  and degradation bars, Monte Carlo strategy distribution (emphasis dot + P10-P90 with
  tooltip and table view), undercut decomposition, rejoin traffic window, live
  LangGraph trace with revision cycles, Qdrant citations, scenario runner (agent or
  single-pass, injectable faults), and a race-control console.
- Jest and Playwright suites driven by payloads recorded from the real system, plus a
  live Playwright test on the real stack and models (`make test-web-live`).

### Fixed in milestone 9
- **The processor ignored SIGTERM.** `make stop` killed it without leaving its consumer
  group or flushing buffered Parquet batches (data loss), and a restart waited about
  40 s for Kafka's session timeout before consuming. All Go commands now handle
  SIGTERM; a test checks the flush and a fast re-assignment.
- **Retired cars stayed on the live tower.** The 2023 Singapore lap-1 retirement showed
  as "P19 TSU" for the whole race. The tower now lists only cars updated within 60 s of
  race time in the current session, which also keeps them out of live scenarios.
- **The API only accepted the dashboard on port 3000.** CORS now admits any loopback
  origin plus `PITWALL_CORS_ORIGINS`.
- **The "live" indicator tracked the socket, not the data.** It now reports freshness
  (a tower update in the last 5 s).
- **One absurd plan flattened the strategy axis.** Intermediates on a dry track
  (+134 s) squeezed the contenders into a fifth of the chart. Outliers are now excluded
  from the domain and labelled off-scale.
- **The undercut chart was illegible** in a half-width column (6 px labels). It now has
  its own full-width card.
- **GP10 ground truth was too narrow.** It accepted only extending, but stopping now
  versus extending is close to a tie on tyre wear (about 22.3 s vs 23.1 s by hand).
  The trap in that scenario is the 5 s hold, which compliance measures. Found after a
  development run disagreed; the acceptable set is now every legal plan that accounts
  for the hold.

### Added in milestone 10 (benchmarks and packaging)
- `cmd/bench-stream`: 200,000 real OpenF1 events through a fresh 20-partition topic
  into the Go engines and Parquet sink. It reports burst and paced trials, producer,
  processed and committed-to-Parquet rates, stream-to-trigger latency, and concurrent
  DuckDB readers during the writes. It also measures Monte Carlo latency in process
  and over MCP.
- `pitwall.eval.agent_eval`: 20 scenarios, single pass against the cyclic agent on the
  real models with seeded 10% primary faults, judged by the rule checker under the
  edition in force.
- `make bench` (`scripts/bench.sh`) runs all three benchmarks and merges them into
  `benchmarks/results.json` (`pitwall.eval.report`), with the machine and versions.
- `make smoke` (`scripts/smoke-test.sh`): one live check each for Kafka, Ollama, the
  MCP server, the processor, DuckDB, Qdrant and the control plane.
- `docker-compose.yml` with Dockerfiles for the Go, Python and web images, plus
  `python/requirements.lock` pinning the versions the tests ran on. The compose file
  has not been run (no Docker on the development machine).

### Fixed in milestone 10
- **The first throughput number depended on which trial went first.** The first burst
  after a process start ran at 39k-155k events/s, while later ones ran at about 1M.
  This was attributed to cold consumer fetch sessions and the Go heap growing. That
  diagnosis was only half right (see "Fixed after the first full benchmark": the
  39k trials were a 5 s producer metadata stall). Warm-up trials are now run and
  excluded, and the report gives the median trial, the range, and the cold-start
  figure separately. It also reports a committed rate (until the last
  Parquet batch is durable) next to the processed rate.
- **The offline lakehouse held every live replay too.** The live processor wrote into
  `data/lakehouse`, the same tree the scenarios, tests and benchmarks read, so every
  live replay during development added its events again. Found when the benchmark's
  DuckDB step counted 752,787 telemetry rows for 332,787 recorded events. The live
  processor now writes `data/lakehouse_live`,
  the offline lakehouse was rebuilt, and a test asserts it holds exactly the
  332,787 committed events.
- **The single-pass baseline was a strawman.** Its prompt lacked the rules and the
  action-to-compound mapping that the agent's synthesiser gets. The first benchmark
  run scored it at 35% compliance, and 6 of its 13 illegal calls contradicted
  themselves (for example, PIT_NOW_MEDIUM with an INTERMEDIATE target). Found while
  reviewing why its violations differed in kind from the agent's. Both now share the
  same rules and output contract (`_RULES` in `graph.py`), so the comparison measures
  only what the graph adds. With that, the single pass scores 60%, and the agent 100%.
- **`make processor` passed a flag that no longer exists** (`-mcp`) and failed at start.

### Fixed during the first build
- **Kafka 4.4 refused to format KRaft storage.** `kafka-storage.sh format --standalone`
  rejects a static `controller.quorum.voters`; switched to
  `controller.quorum.bootstrap.servers` (dynamic quorum).
- **`services.sh up` never returned.** The Qdrant launch ran in a subshell that kept
  the caller's stdout pipe open. It is now launched directly with stdin from `/dev/null`.
- **Qdrant wrote `.qdrant-initialized` into the repo root** and it was committed.
  Amended out and ignored.
- **The 2024 British GP finish was classified as a red flag.** `"CHEQUERED FLAG"`
  contains the substring `"RED FLAG"`. Race-control parsing now matches on word boundaries.
- **Singapore 2023 had 40% of events under safety car.** SC and VSC periods end with a
  track-scoped `TRACK CLEAR` (flag `CLEAR`), not a green flag.
- **A car that retired on lap 1 kept streaming for the whole race.** A mid-lap
  retirement has no `lap_duration`, so the final-lap cut-off never triggered.
- **The red-flag restart was never detected.** Brazil 2024 resumes with
  `SESSION STARTED` / `ROLLING START`, not a green flag.
- **Phantom third pit stops on the final lap.** OpenF1 stints often end one lap before
  the flag, so the last lap read compound `UNKNOWN`.
- **Re-running the fetcher changed every committed `.json.gz`** because gzip embeds a
  timestamp. Output now uses `mtime=0` and is byte-identical across runs.
- **Degradation slopes of -1,300 ms/lap.** Lap 1 (standing start) and restart laps
  after a neutralisation counted as clean laps.
- **Field-wide "tyre cliff" alerts during a Silverstone shower.** Cliff detection is
  now suppressed on laps with rainfall.
- **Readers could open half-written Parquet files** (`TestConcurrentReadersNeverSeePartialFiles`:
  92 of 3,894 reads failed). Go's `filepath.Glob("*.parquet")` matches dotfiles, so
  the hidden temp file was visible. In-flight files now end in `.inprogress`.
- **The replayer dropped race-control messages when replaying several races.**
- **DuckDB `BinderException` while the writer was mid-run.** A view bound before a
  table's first commit kept a column-less placeholder. Placeholders are now typed and
  re-bound once data exists.
- **The simulator said every plan beats the car ahead (p = 1.00).** Rivals with no
  declared stop were projected to run past the tyre cliff to the flag.
- **SC laps skewed finishing positions.** Only our car's lap time was inflated under SC.
- **On-track passes were always impossible.** The catch-up gap was measured from the
  final race time instead of the time at rejoin.
- **The undercut gap had the wrong sign** in the agent's `compute_undercut_delta` call.

### Fixed in the pre-milestone-8 audit
A full audit before building the API found the following. Each fix has a test.

**Regulations judged by the wrong rulebook.** The 2023 and 2024 races were checked
against the 2025 regulations, and the 2026 regulations (a new formula: active
aerodynamics instead of DRS, new power units) were in the index. The corpus now holds
exactly the issues in force on each race date:
- 2023 Issue 6 (31 Aug 2023) for Singapore (17 Sep 2023);
- 2024 Issue 6 (30 Apr 2024) for Silverstone (7 Jul 2024);
- 2024 Issue 7 (31 Jul 2024) for Sao Paulo (3 Nov 2024);
- 2025 Issue 5 for the synthetic scenarios.

Article numbers really do move between issues. The two-compound rule is 30.5(n) in 2023
and 30.5(m) in 2024-2025. The SC pit-entry rule is 55.11 in 2023 and 2024 Issue 6, and
55.12 from 2024 Issue 7. The consequence of breaching the penalty-hold rule changed from
"may result in disqualification" (2023-2024) to "a further penalty that supersedes"
(2025). Citations are now resolved from each edition's text, not hard-coded; the
hard-coded "55.12" had been wrong for every real race.

**Deleted regulation text was indexed as live.** FIA issues are published as redlines,
and pypdf extracted struck-through words as ordinary text. Two eval labels pointed at a
2025 Article 58.12 sentence that the FIA had deleted. Extraction now uses PyMuPDF and
drops characters crossed at mid-height by the redline strike stroke: 5,115 / 2,480 /
641 / 2,257 characters removed per edition. The removed text was reviewed by colour and
content.

**Corpus parsing defects.**
- The last sub-article of every article inherited the next article's title (five
  chunks of Art. 54, including the penalty-hold paragraphs, were labelled "Safety Car").
- A heading containing "É" (PARC FERMÉ) was not recognised, so Article 60 was
  mislabelled "Finish".
- Sub-article numbers alone on their own line (58.11, 58.12) were merged into the
  previous chunk.
- The two-compound paragraph was packed together with its neighbours.

A completeness check now verifies there are no sub-article gaps in any edition.

**Hindsight leaks into the agent's inputs.**
- Real scenarios used each lap's end-of-lap record. For a car that pitted, that row is
  written after the line crossing in the pit lane: GP02 said Hamilton was P5, but he was
  P4 when the call was made. GP04 said P2; he was P3.
- Stint summaries included laps after the decision lap.
- Model-written SQL could read any lap of any session.
- Retrieval could return race-control notices from later laps and precedent digests of
  the same or later races. A day-level date cutoff was needed because a same-day
  digest, timestamped at midnight, passed a race-start-time filter. This was found by
  an adversarial probe.

The decision instant is now the car's last sample before the final sector of the
decision lap, and the field is taken at that same instant. SQL runs inside views
restricted to that instant, and retrieval is filtered to the edition in force, context
dated before the race day, and this race's notices from earlier laps only. A probe of
all 20 scenarios (170-205 retrieved documents each) finds zero future documents.

**Race length.** The 2023 Singapore GP was recorded as 63 laps because OpenF1 includes
the podium finishers' cool-down lap. Race length now comes from the chequered-flag lap,
and 302 post-race events were removed.

**Precedent digests contained unverified claims.**
- "Turn 10" (Norris, Austria 2024) and "effectively ended his race" (Verstappen,
  Mexico 2024) were not backed by any source. Verstappen finished P6.
- The Magnussen ban was tagged Art. 54.3(i); it is Art. 4.2.
- The Abu Dhabi 2021 digest overstated the FIA's findings.

Every digest was re-verified against OpenF1 race-control, stint and position data or
primary sources, is dated, and records its source. Three generic notes are now labelled
`operational_note`.

**Pit loss.** The simulator used 21.8 s everywhere, but measured green-flag loss is
30.5 s at Singapore (3 stops) and 24.2 s at Silverstone (7 stops). Real scenarios now
use the measured value when it rests on at least 3 stops. My first SC/VSC measurement
was wrong: gap glitches under neutralisation polluted it. It is not used; neutralised
loss stays the 0.606 x green model assumption, consistent with the two clean readings
(Hamilton 19.4 s, Russell 16.7 s under the 2023 Singapore VSC).

**Simulator gaps exposed by scenarios.**
- Scenarios GP10 and GP17 described fading rivals the model could not represent; rivals
  now take an observed degradation.
- Projected rivals stayed on slicks in heavy rain; they now switch tyres when the
  weather turns.
- A planned stop from intermediates always went onto hards; the tyre is now chosen to
  suit the weather.

**Scenario ground truth.**
- GP08 accepted extending on mediums already past their life.
- GP16 accepted OPPOSITE_AHEAD, which resolves to stopping into the train.
- GP11 accepted softs for 23 laps.
- GP19 forced 62 laps on hards; it was redesigned as a mid-race red flag.
- "forbidden" listed legal-but-slow calls.

A test now enforces that every forbidden call is illegal and every acceptable call is
legal. Real-scenario questions were rewritten from decision-time data and are checked
against the state.

**Retrieval benchmark was measured on a dirty corpus.** The earlier finding that
"plain RRF scores below dense-only" was an artifact of the redline text and coarse
chunks. On the clean corpus hybrid RRF is the best fusion mode (R@3 0.950 vs dense
0.900), and no cross-encoder gives a significant gain on 40 queries. The default
reranker moved from bge-reranker-v2-m3 (about 2.3 s) to ms-marco-MiniLM-L-12 over 10
candidates (about 0.2 s).

### Measured, not hidden
- In the 20-scenario benchmark the agent's first proposal was legal every time: 0
  revisions, 0 guardrail overrides. Its compliance gain comes from grounding, not from
  the cycle.
- The agent's GP11 and GP12 calls (soft stints of 23 and 28 laps) follow the simulator
  and disagree with the ground truth, which says softs would not last. The labels were
  not changed: the simulator's soft cliff (20 laps, +350 ms/lap per lap) is gentler
  than that reasoning.
- The engineer radio message is free text from the model and is not checked by the
  guardrail; in a GP10 run it mentioned undercutting a rival that the same run's
  numbers show cannot be undercut.
- Ground truth for the synthetic scenarios is reasoned from the regulations, not taken
  from the simulator, but the agent is told to prefer the simulator's best legal plan.
  Agreement on synthetic scenarios therefore partly measures whether the simulator's
  physics agrees with that reasoning. Real-scenario agreement is reported separately.
- Track wetness is not in OpenF1. For the four wet-transition real scenarios it is an
  analyst estimate, and each scenario records the decision-time evidence behind it.
