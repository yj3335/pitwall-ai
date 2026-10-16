#!/usr/bin/env bash
# Robustness study (about 2 hours on the M5 Pro). Needs `make run`, Ollama with
# the models, and a quiet machine. Run `make bench` first: its agent run is
# fault seed 0. Writes benchmarks/robustness/ and benchmarks/robustness.json.
#   faults:   2 more fault seeds at the production setting (10% faults, greedy)
#   sampling: temperature 0 / 0.3 / 0.7 on the decision calls, 3 seeds each, faults off
#   inputs:   200 perturbations per scenario of the simulator request
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/.venv/env.sh"
PY="$ROOT/.venv/bin/python"
OUT="$ROOT/benchmarks/robustness"
mkdir -p "$OUT"
curl -fs -m 2 localhost:8081/healthz >/dev/null || { echo "start the stack first: make run" >&2; exit 1; }
[ -f "$ROOT/benchmarks/agent_eval.json" ] || { echo "run make bench first (fault seed 0)" >&2; exit 1; }

cd "$ROOT/python"
echo "== inputs: simulator sensitivity"
"$PY" -m pitwall.eval.sensitivity --out "$OUT/sensitivity.json" >/dev/null
for seed in 1 2; do
  echo "== faults: seed $seed"
  "$PY" -m pitwall.eval.agent_eval --modes single_pass agent --fault-seed "$seed" --out "$OUT/faults_seed$seed.json" >/dev/null
done
for t in 0.0 0.3 0.7; do
  for seed in 1 2 3; do
    echo "== sampling: temperature $t, seed $seed"
    "$PY" -m pitwall.eval.agent_eval --modes single_pass agent --fault-rate 0 --temperature "$t" --seed "$seed" \
      --out "$OUT/temp_${t}_seed$seed.json" >/dev/null
  done
done
"$PY" -m pitwall.eval.robustness >/dev/null
echo "wrote benchmarks/robustness.json"
