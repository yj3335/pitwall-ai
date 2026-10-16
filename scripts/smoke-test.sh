#!/usr/bin/env bash
# End-to-end smoke test across Kafka, the Go MCP server, the Go processor,
# Qdrant, DuckDB, Ollama and the FastAPI control plane. Needs `make run`.
# Prints one PASS/FAIL line per check and exits non-zero on any failure.
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/.venv/env.sh"
PY="$ROOT/.venv/bin/python"
fails=0
check() { # name, command...
  local name=$1; shift
  local out
  if out=$("$@" 2>&1); then
    printf '  PASS  %-42s %s\n' "$name" "$(echo "$out" | tail -1)"
  else
    printf '  FAIL  %-42s %s\n' "$name" "$(echo "$out" | tail -1)"
    fails=$((fails + 1))
  fi
}

echo "PitWall AI smoke test"

check "Kafka KRaft broker and topics" bash -c \
  "kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic f1.telemetry.raw | grep -c 'Partition:' | grep -qx 20 && echo '20 partitions on f1.telemetry.raw'"

check "Ollama models" "$PY" - <<'EOF'
import httpx
names = {m["name"] for m in httpx.get("http://localhost:11434/api/tags", timeout=5).json()["models"]}
need = {"qwen2.5-coder:14b", "llama3.2:3b", "bge-m3:latest"}
missing = need - names
assert not missing, f"missing {missing}"
dim = len(httpx.post("http://localhost:11434/api/embed", json={"model": "bge-m3", "input": ["VSC"]}, timeout=60).json()["embeddings"][0])
assert dim == 1024, dim
print("3 models present, bge-m3 1024-dim")
EOF

check "Go MCP server: JSON-RPC 2.0 simulate" "$PY" - <<'EOF'
import httpx
url = "http://localhost:8081/mcp"
r = httpx.post(url, json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}, timeout=10).json()
assert r["result"]["protocolVersion"]
tools = httpx.post(url, json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, timeout=10).json()["result"]["tools"]
assert len(tools) == 4
args = {"total_laps": 57, "current_lap": 30, "car": {"compound": "MEDIUM", "tyre_age_laps": 22, "compounds_used": ["MEDIUM"]}}
s = httpx.post(url, json={"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                          "params": {"name": "simulate_pit_strategies", "arguments": args}}, timeout=30).json()["result"]["structuredContent"]
assert s["total_trajectories"] == 60000
print(f"{s['total_trajectories']:,} trajectories in {s['elapsed_ms']} ms, best legal {s['recommended']}")
EOF

check "Kafka -> Go processor stream" bash -c '
  before=$(curl -fs localhost:8090/v1/stats | '"$PY"' -c "import json,sys; print(json.load(sys.stdin)[\"events\"])")
  '"$ROOT"'/bin/replayer -file '"$ROOT"'/data/races/session_9165_events.jsonl.gz -speed 0 -limit 2000 >/dev/null 2>&1
  for i in $(seq 1 50); do
    after=$(curl -fs localhost:8090/v1/stats | '"$PY"' -c "import json,sys; print(json.load(sys.stdin)[\"events\"])")
    [ $((after - before)) -ge 2000 ] && { echo "consumed $((after - before)) events"; exit 0; }
    sleep 0.2
  done; echo "consumed only $((after - before))"; exit 1'

check "DuckDB over the Parquet lakehouse" bash -c "cd '$ROOT/python' && '$PY' - <<'EOF'
from pitwall.olap import Lakehouse
lh = Lakehouse()
n = lh.query('SELECT count(*) FROM telemetry').rows[0][0]
assert n == 332_787, n
r = lh.degradation_leaderboard(9165, 40)
print(f'{n:,} events; leaderboard in {r.elapsed_ms:.1f} ms')
EOF"

check "Qdrant hybrid search (edition-filtered)" bash -c "cd '$ROOT/python' && '$PY' - <<'EOF'
from pitwall.rag.store import HybridRetriever, load_corpus
r = HybridRetriever(reranker_model=None)
pts = r.client.get_collection('fia_sporting_regs').points_count
assert pts == len(load_corpus()), (pts, len(load_corpus()))
hits = r.search('Article 30.5 two different dry-weather specifications', mode='hybrid', k=3,
                qfilter=HybridRetriever.edition_filter('2023-i6')).hits
assert any(h.id == '2023-i6-30.5-n' for h in hits), [h.id for h in hits]
print(f'{pts} points; 2023 two-compound rule found as 30.5(n)')
EOF"

check "FastAPI control plane + real model call" "$PY" - <<'EOF'
import httpx
st = httpx.get("http://localhost:8080/api/status", timeout=10).json()
assert all(st[k] for k in ("mcp_sim", "qdrant", "ollama", "processor")), st
r = httpx.post("http://localhost:8080/api/strategy", json={"scenario_id": "GP12", "mode": "single_pass"}, timeout=300).json()
assert r["final"]["recommended_action"], r
print(f"GP12 single pass: {r['final']['recommended_action']}, compliant={r['assessment']['compliant']}, {r['elapsed_ms'] / 1000:.1f} s")
EOF

if [ "$fails" -eq 0 ]; then echo "all checks passed"; else echo "$fails check(s) failed"; exit 1; fi
