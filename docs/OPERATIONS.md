# Operations

Everything runs from the repository with tools installed under `.venv/`. Nothing
is installed globally except Ollama, which serves the models.

## Setup

```bash
make venv          # Go, Python venv + deps, Qdrant, JDK 21, Kafka 4.4 KRaft, npm/Playwright caches
make ollama-pull   # qwen2.5-coder:14b, llama3.2:3b, bge-m3 (skips models already present)
source .venv/env.sh
```

`.venv/env.sh` puts `go`, `qdrant`, the Kafka scripts and `java` on `PATH` and
points `GOPATH`, `GOCACHE`, `npm_config_cache` and `PLAYWRIGHT_BROWSERS_PATH` into `.venv/`.

Disk use is about 2.7 GB in `.venv/` after bootstrap. The default reranker adds about
130 MB in `.venv/fastembed_cache/` (bge-reranker-v2-m3 would add 2.1 GB). Ollama models
take about 12.2 GB in `~/.ollama`.

## Services

```bash
make services-up     # Kafka (KRaft) on :9092 and Qdrant on :6333
make status          # kafka / qdrant / ollama up or down
make services-down
```

- **Kafka** runs as one combined broker + controller with
  `controller.quorum.bootstrap.servers` (dynamic quorum, no ZooKeeper). Data is in
  `.kafka_data/`, logs in `.run/kafka.log`, config in `.run/kraft.properties`.
  Topics: `f1.telemetry.raw` (20 partitions), `f1.race_control` (1),
  `f1.triggers` (20); `auto.create.topics.enable=false`. Reset everything with
  `make services-down && rm -rf .kafka_data`.
- **Qdrant** stores data in `.qdrant_storage/`, logs in `.run/qdrant.log`. The
  collection is `fia_sporting_regs`. Rebuild it with `make index` (about 45 s for 2,077
  chunks with bge-m3 on an M5 Pro).
- **Ollama** must be running (`ollama serve`). Models are kept loaded for 30 minutes
  (`keep_alive`). qwen2.5-coder:14b (9 GB), llama3.2:3b (2 GB) and bge-m3 (1.2 GB)
  fit together in 24 GB alongside Kafka and Qdrant.

## Running the stack

```bash
make run      # Kafka + Qdrant, Go MCP simulator (:8081), Go processor (:8090), FastAPI (:8080)
make status   # every component up or down
make stop     # stops them and waits for the ports to close (Kafka shuts down gracefully)
```

Ollama must already be running (`ollama serve`). Logs and pids are in `.run/`.

The MCP simulator, the processor and the API listen on `127.0.0.1` only (the Go
binaries default to it; pass `-http 0.0.0.0:8081` to expose one deliberately). There
is no authentication, so nothing is reachable from the network by default. In Docker
Compose the services listen on `0.0.0.0` inside their containers, which the bridge
network needs, and every published port is bound to the host's loopback.

The dashboard runs separately:

```bash
make web-dev                       # http://localhost:3000 (NEXT_PUBLIC_API_URL defaults to http://127.0.0.1:8080)
cd web && npm run build && npx next start -p 3000   # production build
```

All processes handle SIGTERM: on `make stop` the processor leaves its consumer group
and flushes buffered Parquet batches, so a restart is assigned partitions within
seconds rather than after Kafka's 45 s session timeout.

```bash
make replay SESSION=9165                                    # stream Singapore 2023 into Kafka at 20x
curl -s localhost:8080/api/status | jq
curl -s localhost:8080/api/live/tower | jq '.drivers[:3]'

# a strategy run, streamed over SSE
RUN=$(curl -s -X POST localhost:8080/api/strategy/runs -H 'content-type: application/json' \
      -d '{"scenario_id":"GP02"}' | jq -r .run_id)
curl -N localhost:8080/api/strategy/runs/$RUN/events

# a race-control message: published to Kafka, then a run for the affected car on live state
curl -s -X POST localhost:8080/api/race-control/trigger -H 'content-type: application/json' \
     -d '{"message":"VIRTUAL SAFETY CAR DEPLOYED","lap_number":38,"driver_number":44}' | jq

# synchronous, single-pass baseline, with 10% injected primary-model faults
curl -s -X POST localhost:8080/api/strategy -H 'content-type: application/json' \
     -d '{"scenario_id":"GP08","mode":"single_pass","fault_rate":0.1}' | jq
```

For a live trigger, the lap count comes from the race's metadata. Track wetness, which
OpenF1 does not provide, defaults from the weather-station rain flag unless
`track_wetness` is given.

## Data

```bash
make data        # re-fetch the three OpenF1 races and rebuild the FIA corpus (both are committed)
make lakehouse   # replay all committed events into data/lakehouse/ and measure per-race pit loss
make index       # embed and index the FIA corpus into Qdrant (2,077 chunks, about 45 s)
```

Re-running `make data` is deterministic: gzip headers carry no timestamp, so
unchanged data produces byte-identical files. Raw `car_data` responses are cached
in `data/races/.cache/` (ignored).

**Regulation editions.** The corpus holds the Sporting Regulations issue in force on
each race date:

| Edition | Document | Used for |
|---|---|---|
| `2023-i6` | 2023 Sporting Regulations, Issue 6 (31 Aug 2023) | 2023 Singapore GP |
| `2024-i6` | 2024 Sporting Regulations, Issue 6 (30 Apr 2024) | 2024 British GP |
| `2024-i7` | 2024 Sporting Regulations, Issue 7 (31 Jul 2024) | 2024 Sao Paulo GP |
| `2025-i5` | 2025 Sporting Regulations, Issue 5 (30 Apr 2025) | synthetic scenarios, retrieval benchmark |

The PDFs are downloaded from fia.com to `data/fia_regulations/.pdf/` (ignored). They
are redlines; deleted (struck-through) text is removed at extraction, and the builder
prints how many characters it removed per edition. To add a race from another date,
add the issue in force to `EDITIONS` in `scripts/build-fia-corpus.py`. The 2026
regulations are a new formula that the simulator does not model, so they are only
built with `--include-2026`.

**Pit loss.** `scripts/measure-pit-loss.py` (run by `make lakehouse`) writes
`data/races/<session>/pit_loss.json`. Real scenarios use the measured green-flag loss
when it rests on at least 3 stops (`use_measured`), otherwise the generic 21.8 s.

## Replaying a race live

```bash
make mcp-sim                                   # Go MCP server on :8081
make processor                                 # Kafka consumer -> data/lakehouse_live + live tower on :8090
make replay SESSION=9165                       # replay Singapore 2023 at 20x
./bin/replayer -file data/races/session_9636_events.jsonl.gz -speed 1   # real time
./bin/replayer -file data/races/session_9558_events.jsonl.gz -speed 0   # as fast as possible
curl -s localhost:8090/v1/tower | jq '.drivers[:3]'
curl -s localhost:8090/v1/stats | jq
```

The processor starts at the end of the topic for a new consumer group, so a
replay must wait until it holds its partitions. `GET :8090/readyz` returns 200
from that moment (503 before), and `make run` waits for it.

**Two lakehouses.** The live processor writes to `data/lakehouse_live/`.
`data/lakehouse/` is the deterministic offline replay of the three committed races
(`make lakehouse`). Scenarios, tests and benchmarks read only the offline one. Never
point a live processor at it: every replay would append its events again, and
`test_committed_lakehouse_has_no_duplicate_events` would fail.

## Calling the simulator directly

```bash
curl -s localhost:8081/mcp -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"simulate_pit_strategies",
  "arguments":{"total_laps":57,"current_lap":30,"track_status":"VSC",
  "car":{"driver_code":"PIA","position":3,"compound":"MEDIUM","tyre_age_laps":22,"compounds_used":["MEDIUM"]}}}}' \
  | jq '.result.structuredContent | {recommended, elapsed_ms, total_trajectories}'
./bin/mcp-sim -stdio      # the same server over stdin/stdout for MCP desktop clients
```

## Benchmarks and smoke test

```bash
make run        # the benchmarks and the smoke test need the running stack and Ollama
make smoke      # one live check per component, about 12 s (one real model call)
make bench      # all three benchmarks -> benchmarks/results.json
make robustness # after make bench: faults, temperature sweep, input noise (about 2 hours)
```

`make bench` runs, in order:
- the stream benchmark, on its own topics, groups and temporary lakehouses, so the
  running processor is untouched;
- the 40-query retrieval ablation;
- the 24-scenario agent evaluation: single pass, cyclic agent and the
  simulator-only arm (48 model runs).

`make robustness` (`scripts/robustness.sh`) then runs the robustness study and
writes `benchmarks/robustness.json`, with the raw runs in `benchmarks/robustness/`:
- 200 input perturbations per scenario through the simulator (about 30 s);
- two more fault seeds at the production setting;
- temperatures 0, 0.3 and 0.7 on the decision calls, three seeds each, faults off.

The agent evaluation dominates the run time (about 10 minutes for 48 runs). Stop other Ollama clients first, because
they share the GPU and inflate its latencies. Each step can also run on its own
(`python -m pitwall.eval.stream_bench | rag_eval | agent_eval | report`). The agent
evaluation also takes `--modes`, `--fault-rate`, `--fault-seed`, `--temperature` and
`--seed`.

## Docker Compose

`docker-compose.yml` describes the same stack in containers:
- Kafka 4 in KRaft mode, plus a job that creates the topics;
- Qdrant;
- one-shot lakehouse and index jobs;
- the MCP simulator, the processor, the API and the dashboard;
- a `replay` profile for the replayer.

Ollama stays on the host and is reached at `host.docker.internal:11434`. The host
can reach the broker at `localhost:29092`.

```bash
docker compose up -d --build
docker compose --profile replay run --rm replayer -file data/races/session_9165_events.jsonl.gz -speed 20
```

**Status: smoke-tested in CI.** The development machine has no Docker, so the
`compose` job in `.github/workflows/ci.yml` runs it on every push:
1. Build the Go and Python images.
2. Start Kafka, the topic job, Qdrant, the lakehouse job, the simulator, the processor
   and the API.
3. From the host, check that `:8081`, `:8090` and `:8080` are healthy, that the
   processor holds its partitions (`/readyz`), and that `/api/status` sees the
   simulator, the processor, Qdrant and all three race sessions.
4. Check that MCP `tools/list` returns the four tools.

The check leaves out Ollama (absent on CI), the Qdrant index job (it embeds with
bge-m3) and the dashboard image. Everything in `docs/TESTING.md` and
`docs/BENCHMARKS.md` was measured on the native `.venv/` stack (`make run`). The
API does not wait for the index job: run `docker compose run --rm index` once
Ollama is up.

## Configuration

| Variable | Default | Used by |
|---|---|---|
| `OLLAMA_URL` | `http://localhost:11434` | embeddings, agent |
| `PITWALL_PRIMARY_MODEL` | `qwen2.5-coder:14b` | SQL + synthesis |
| `PITWALL_VERIFIER_MODEL` | `llama3.2:3b` | critic + failover |
| `PITWALL_EMBED_MODEL` | `bge-m3` | dense vectors |
| `PITWALL_RERANKER` | `Xenova/ms-marco-MiniLM-L-12-v2` | cross-encoder (`BAAI/bge-reranker-v2-m3` is about 10x slower with no measured gain) |
| `PITWALL_RERANK_K` | `10` | candidates reranked |
| `QDRANT_URL`, `QDRANT_COLLECTION` | `http://localhost:6333`, `fia_sporting_regs` | retrieval |
| `PITWALL_MCP_URL` | `http://localhost:8081/mcp` | agent |
| `PITWALL_LAKEHOUSE` | `data/lakehouse` | DuckDB |
| `PITWALL_PROCESSOR_URL` | `http://localhost:8090` | live tower and race-control publishing |
| `PITWALL_CORS_ORIGINS` | (loopback only) | extra dashboard origins, comma-separated |

## Troubleshooting

- `kafka-storage.sh format` errors about `controller.quorum.voters`: an old
  `.run/kraft.properties` is in place; `rm -rf .run .kafka_data` and run `make services-up` again.
- The DuckDB view shows no rows while the processor runs: every table commits a batch
  every 2 s, or sooner when a batch fills (telemetry 20,000 rows, `-flush-rows`; laps
  2,000; triggers 500).
- The first hybrid query is slow: the cross-encoder is downloaded and loaded on first use into `.venv/fastembed_cache/`.
