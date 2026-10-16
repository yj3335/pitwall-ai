#!/usr/bin/env bash
# Run the three benchmarks on this machine and write benchmarks/results.json.
#   1. stream: 200k real events through Kafka -> Go -> Parquet with concurrent DuckDB,
#      Monte Carlo latency in-process and over MCP, DuckDB OLAP latency
#   2. rag:    40-query Qdrant ablation (dense / sparse / hybrid RRF / + rerank)
#   3. agent:  24 scenarios, single-pass vs cyclic agent, real models, 10% faults
# Needs `make run` (Kafka, Qdrant, MCP simulator on :8081) and Ollama with the models.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/.venv/env.sh"
PY="$ROOT/.venv/bin/python"
mkdir -p "$ROOT/benchmarks"

curl -fs -m 2 localhost:8081/healthz >/dev/null || { echo "start the stack first: make run" >&2; exit 1; }
curl -fs -m 2 localhost:6333/healthz >/dev/null || { echo "Qdrant is not running" >&2; exit 1; }
curl -fs -m 2 localhost:11434/api/tags >/dev/null || { echo "Ollama is not running" >&2; exit 1; }
[ -n "$(ls "$ROOT"/data/lakehouse/telemetry/session=*/*.parquet 2>/dev/null)" ] || make -C "$ROOT" lakehouse

cd "$ROOT/python"
echo "== 1/3 stream pipeline"
"$PY" -m pitwall.eval.stream_bench --out ../benchmarks/stream.json >/dev/null
echo "== 2/3 Qdrant retrieval ablation"
"$PY" -m pitwall.eval.rag_eval --out ../benchmarks/rag_ablation.json
echo "== 3/3 agent evaluation on real models (about 10 minutes)"
"$PY" -m pitwall.eval.agent_eval --out ../benchmarks/agent_eval.json >/dev/null
"$PY" -m pitwall.eval.report >/dev/null
echo "wrote benchmarks/results.json"
