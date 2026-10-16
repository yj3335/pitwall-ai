# Changelog

Engineering log for PitWall AI. Every entry under "Fixed" is a defect that was
actually hit while building and verifying the system, with how it was found.

## Unreleased

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
- The engineer radio message is free text from the model and is not checked by the
  guardrail; in a GP10 run it mentioned undercutting a rival that the same run's
  numbers show cannot be undercut.
- Ground truth for the synthetic scenarios is reasoned from the regulations, not taken
  from the simulator, but the agent is told to prefer the simulator's best legal plan.
  Agreement on synthetic scenarios therefore partly measures whether the simulator's
  physics agrees with that reasoning. Real-scenario agreement is reported separately.
- Track wetness is not in OpenF1. For the four wet-transition real scenarios it is an
  analyst estimate, and each scenario records the decision-time evidence behind it.
