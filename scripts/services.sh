#!/usr/bin/env bash
# Start/stop the local infrastructure from .venv: a single-node Kafka broker in
# KRaft mode (combined broker+controller, no ZooKeeper) and a Qdrant server.
#   scripts/services.sh up|down|status
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/.venv/env.sh"
RUN="$ROOT/.run"
mkdir -p "$RUN"

KAFKA_DATA="$ROOT/.kafka_data"
KAFKA_PORT="${KAFKA_PORT:-9092}"
QDRANT_PORT="${QDRANT_PORT:-6333}"

kafka_conf() {
  cat > "$RUN/kraft.properties" <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=localhost:9093
listeners=PLAINTEXT://localhost:${KAFKA_PORT},CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://localhost:${KAFKA_PORT}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=${KAFKA_DATA}
num.partitions=20
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
log.retention.hours=6
EOF
}

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

wait_port() {
  for _ in $(seq 1 120); do port_open "$1" && return 0; sleep 0.5; done
  echo "timed out waiting for port $1" >&2; return 1
}

up() {
  if ! port_open "$KAFKA_PORT"; then
    kafka_conf
    if [ ! -f "$KAFKA_DATA/meta.properties" ]; then
      echo "formatting KRaft storage at $KAFKA_DATA"
      kafka-storage.sh format --standalone -t "$(kafka-storage.sh random-uuid)" -c "$RUN/kraft.properties" >/dev/null
    fi
    KAFKA_HEAP_OPTS="-Xmx1G -Xms512M" nohup kafka-server-start.sh "$RUN/kraft.properties" </dev/null >"$RUN/kafka.log" 2>&1 &
    echo $! >"$RUN/kafka.pid"
    wait_port "$KAFKA_PORT"
    echo "kafka (KRaft) up on :$KAFKA_PORT"
  else
    echo "kafka already listening on :$KAFKA_PORT"
  fi
  for topic in f1.telemetry.raw:20 f1.race_control:1 f1.triggers:20; do
    name="${topic%%:*}"; parts="${topic##*:}"
    kafka-topics.sh --bootstrap-server "localhost:$KAFKA_PORT" --create --if-not-exists \
      --topic "$name" --partitions "$parts" --replication-factor 1 >/dev/null
  done
  echo "topics: $(kafka-topics.sh --bootstrap-server localhost:$KAFKA_PORT --list | tr '\n' ' ')"

  if ! port_open "$QDRANT_PORT"; then
    QDRANT__STORAGE__STORAGE_PATH="$ROOT/.qdrant_storage" \
      QDRANT__STORAGE__SNAPSHOTS_PATH="$ROOT/.qdrant_storage/snapshots" \
      QDRANT__SERVICE__HTTP_PORT="$QDRANT_PORT" QDRANT__TELEMETRY_DISABLED=true \
      nohup qdrant </dev/null >"$RUN/qdrant.log" 2>&1 &
    echo $! >"$RUN/qdrant.pid"
    wait_port "$QDRANT_PORT"
    echo "qdrant up on :$QDRANT_PORT"
  else
    echo "qdrant already listening on :$QDRANT_PORT"
  fi
}

down() {
  for svc in kafka qdrant; do
    if [ -f "$RUN/$svc.pid" ]; then
      pid="$(cat "$RUN/$svc.pid")"
      kill "$pid" 2>/dev/null && echo "stopped $svc ($pid)" || true
      rm -f "$RUN/$svc.pid"
    fi
  done
}

status() {
  port_open "$KAFKA_PORT" && echo "kafka:  up (:$KAFKA_PORT)" || echo "kafka:  down"
  port_open "$QDRANT_PORT" && echo "qdrant: up (:$QDRANT_PORT)" || echo "qdrant: down"
  curl -fs -m 2 localhost:11434/api/tags >/dev/null && echo "ollama: up (:11434)" || echo "ollama: down"
}

case "${1:-status}" in
  up) up ;;
  down) down ;;
  status) status ;;
  *) echo "usage: $0 up|down|status" >&2; exit 2 ;;
esac
