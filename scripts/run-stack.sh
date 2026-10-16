#!/usr/bin/env bash
# Start the full local stack: Kafka (KRaft) + Qdrant, the Go MCP simulator
# (:8081), the Go stream processor (:8090) and the FastAPI control plane
# (:8080). Logs and pids go to .run/.   scripts/run-stack.sh up|down|status
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$ROOT/.run"
mkdir -p "$RUN"
# shellcheck disable=SC1091
source "$ROOT/.venv/env.sh"

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }
wait_http() {
  for _ in $(seq 1 120); do curl -fs -m 1 "$1" >/dev/null && return 0; sleep 0.5; done
  echo "timed out waiting for $1" >&2; return 1
}
start() { # name port cmd...
  local name=$1 port=$2; shift 2
  if port_open "$port"; then echo "$name already listening on :$port"; return; fi
  nohup "$@" </dev/null >"$RUN/$name.log" 2>&1 &
  echo $! >"$RUN/$name.pid"
}

up() {
  bash "$ROOT/scripts/services.sh" up
  curl -fs -m 2 localhost:11434/api/tags >/dev/null || { echo "Ollama is not running: start it with 'ollama serve'" >&2; exit 1; }
  (cd "$ROOT" && mkdir -p bin && go build -o bin/ ./cmd/...)
  start mcp-sim 8081 "$ROOT/bin/mcp-sim" -http :8081 -brokers localhost:9092 -quiet
  # the live processor writes its own lakehouse: data/lakehouse is the deterministic
  # offline replay (make lakehouse) that scenarios, tests and benchmarks read
  start processor 8090 "$ROOT/bin/processor" -brokers localhost:9092 -out "$ROOT/data/lakehouse_live" -http :8090
  start api 8080 bash -c "cd '$ROOT/python' && exec '$ROOT/.venv/bin/python' -m uvicorn pitwall.api.app:app --host 0.0.0.0 --port 8080"
  # readyz: the processor holds its partitions, so an immediate replay is not skipped
  wait_http localhost:8081/healthz && wait_http localhost:8090/readyz && wait_http localhost:8080/healthz
  echo "stack up:"
  echo "  control plane  http://localhost:8080/api/status"
  echo "  MCP simulator  http://localhost:8081/mcp"
  echo "  live tower     http://localhost:8090/v1/tower"
  echo "replay a race:   make replay SESSION=9165"
}

down() {
  for name in api processor mcp-sim; do
    if [ -f "$RUN/$name.pid" ]; then
      kill "$(cat "$RUN/$name.pid")" 2>/dev/null && echo "stopped $name" || true
      rm -f "$RUN/$name.pid"
    fi
  done
  bash "$ROOT/scripts/services.sh" down
}

status() {
  bash "$ROOT/scripts/services.sh" status
  for p in 8081:mcp-sim 8090:processor 8080:api; do
    port_open "${p%%:*}" && echo "${p##*:}: up (:${p%%:*})" || echo "${p##*:}: down"
  done
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  status) status ;;
  *) echo "usage: $0 up|down|status" >&2; exit 2 ;;
esac
