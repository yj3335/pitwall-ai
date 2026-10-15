# Testing

```bash
make test-go    # go vet + go test -race ./...
make test-py    # pytest (unit + live Kafka/Qdrant/Ollama/MCP integration)
make test-web   # tsc --noEmit, Jest, next build, Playwright      (web/ lands in a later milestone)
```

Integration tests run against the real services and skip with a reason when a service
is down (`requires_ollama`, `requires_qdrant`, a Kafka dial check). Nothing is mocked
except where a test needs a scripted LLM to force a specific code path (the agent's
cyclic-loop tests), and those still call the real Go MCP server.

## Go (`go test -race ./...`): 33 tests in 6 packages, 0 data races

| Package | What is verified |
|---|---|
| `internal/stream` | Fuel-corrected OLS slope (80 ms/lap of true wear under 35 ms/lap of fuel burn is recovered exactly); ring eviction; the 107% outlier rule; null handling; lap 1, in-lap and out-lap exclusion and pit-stop counting; the undercut trigger across two engines sharing a `Board`; one cheap-pit trigger per SC/VSC period; rain-on-slicks crossover; **15 engines over the real Singapore 2023 stream with a concurrent reader under `-race`** |
| `internal/kafka` | murmur2 partitioning is stable and spreads 20 grid numbers over at least 10 partitions; **on the live broker**, 4,000 interleaved records for 20 cars on a fresh 20-partition topic, each car on exactly one partition, in order |
| `internal/parquet` | Partitioned atomic commits leave no temp files; **8 producers and 4 readers decoding every visible file while the sink commits: 242 files, 4,000 reads, 0 partial reads** (this test caught the dotfile glob bug) |
| `internal/sim` | Determinism for a fixed seed; 10k rollouts per strategy; two-compound legality (including the intermediate waiver); a 5 s hold costs about 5 s; a VSC stop saves pit time; a closed pit entry is never recommended; 8 concurrent simulations; exact 2-lap undercut arithmetic and the penalty's effect on it; DRS-train detection; **overtaking difficulty flips a Singapore-style VSC call from pit to stay out**; **a rival's observed degradation changes the projection**; **rivals switch to intermediates when it rains**; latency budget (10k rollouts < 15 ms, measured about 1 ms) |
| `internal/mcp` | Lifecycle, `tools/list` schemas, ID echo, parse / invalid-request / method-not-found codes, tool errors as `isError` results, batches with notifications; **8 concurrent HTTP clients**; the legacy SSE round trip; stdio; `get_live_driver_window` from a Board fed with the real 2024 British GP stream, with a sub-millisecond lookup |
| `internal/metrics` | Histogram quantiles within bucket error; concurrent recording; bucket monotonicity |

## Python (`pytest`): 52 tests

| File | What is verified |
|---|---|
| `test_olap.py` (17) | DuckDB over a lakehouse the Go processor builds from the real Singapore race: all 113,469 events land exactly once. **Go's window slope equals DuckDB `REGR_SLOPE` on the same five laps** (max error < 1 ms/lap over 300+ laps). SC laps are never clean. **The decision state is taken before the pit lane** (Hamilton P4 at the call, while the lap-44 row says P5 from inside the pit lane). **Decision-scoped SQL cannot see later laps, later telemetry, later triggers or other sessions.** Stint summary and query latency. The SQL guard rejects 8 classes of unsafe SQL. **The Go processor commits more than 100 batches while 4 threads query: 0 errors.** |
| `test_rag.py` (8) | Citation-preserving tokenizer; BM25 saturation. The corpus holds exactly the four editions in force, with no 2026. **Struck-through redline text is not indexed** (including the deleted 2025 58.12 sentence). Context documents are dated. **On live Qdrant + bge-m3:** exact citation via sparse; semantic match via dense; hybrid RRF + rerank; the **edition filter, and the point-in-time filter for the Singapore 2023 lap-44 decision** (no later edition, nothing dated on or after the race day, no notice from lap 44 on) |
| `test_regulations.py` (14) | The edition in force on each race date (2026 deliberately unresolvable). **Article references follow the edition** (30.5(n) in 2023; 55.11 to 55.12 at 2024 Issue 7). Every clause resolves in every edition. |
| `test_scenarios.py` (5) | Race lengths come from the chequered flag (Singapore 62, not 63). **Ground-truth integrity:** every `forbidden` call is illegal and every `acceptable` call legal under the rule checker. **Real scenarios use decision-time state** (position, status, compound, compounds used, edition). Measured pit loss is used only with at least 3 stops. **Real-scenario question text agrees with the data.** |
| `test_agent.py` (8) | Rule guardrail (two compounds, penalty hold, pit entry, wet/dry physics). **Cyclic self-correction:** an illegal MEDIUM to MEDIUM call is caught, revised once and finalised legal, with the trace order and `MemorySaver` history checked. The reflection budget and guardrail override on a closed pit entry. Penalty-hold enforcement. Failover with a 100% injected fault rate, and the circuit breaker opening after 2 failures. The HTTP MCP client. **End to end on real qwen2.5-coder:14b, llama3.2:3b, bge-m3/Qdrant and the Go MCP server.** |

## Checks done during the pre-milestone-8 audit

- **Leak probe.** For every one of the 20 scenarios, six broad queries (k = 60, three
  modes) under the agent's point-in-time filter, judged against the source documents'
  own date strings and lap numbers rather than the filter's timestamps: zero future or
  wrong-edition documents. The probe's first version found a real leak (a same-day
  digest passed a race-start-time cutoff).
- **Clause diff** of Art. 30.5, 34.15, 54.3, 54.4, 55.11, 56.4 and 57.4 across all four
  editions. This established which differences are numbering only and which are
  substantive (54.4(e)).
- **Redline review.** Every struck-through run removed from the PDFs was listed by
  colour and content: all in the FIA redline colour except one stray `". "`.
- **Live agent runs** on two real scenarios with real models: SQL scoped to completed
  laps, citations from the right edition (30.5(n) for 2023), compliant calls.

## Live checks done by hand during development

- Replayed the 2023 Singapore GP through Kafka into the processor: 0 decode errors,
  0 sink errors, tower and race-control endpoints correct.
- Exercised the MCP server over stdio with initialize, tools/list, tools/call and an
  unknown method.
