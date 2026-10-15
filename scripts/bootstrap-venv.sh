#!/usr/bin/env bash
# Installs every toolchain this repo needs into ./.venv so nothing touches the
# global OS: Go, a Python virtualenv, Qdrant, a portable JDK, Kafka (KRaft),
# and Node/npm/Playwright caches. Idempotent: re-running skips finished steps.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$ROOT/.venv"
DL="$VENV/downloads"
mkdir -p "$VENV/bin" "$DL" "$VENV/opt"

GO_VERSION="${GO_VERSION:-1.27.2}"
QDRANT_VERSION="${QDRANT_VERSION:-v1.19.2}"
KAFKA_VERSION="${KAFKA_VERSION:-4.4.0}"
KAFKA_SCALA="${KAFKA_SCALA:-2.13}"
JDK_MAJOR="${JDK_MAJOR:-21}"

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  arm64|aarch64) GOARCH=arm64; QARCH=aarch64; JARCH=aarch64 ;;
  x86_64|amd64)  GOARCH=amd64; QARCH=x86_64;  JARCH=x64 ;;
  *) echo "unsupported arch $ARCH" >&2; exit 1 ;;
esac
case "$OS" in
  darwin) QOS=apple-darwin; JOS=mac ;;
  linux)  QOS=unknown-linux-musl; JOS=linux ;;
  *) echo "unsupported os $OS" >&2; exit 1 ;;
esac

log() { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }
fetch() { [ -s "$2" ] || curl -fsSL --retry 3 -o "$2" "$1"; }

# ---------------------------------------------------------------- Go
if [ ! -x "$VENV/opt/go/bin/go" ]; then
  log "installing Go $GO_VERSION"
  fetch "https://go.dev/dl/go${GO_VERSION}.${OS}-${GOARCH}.tar.gz" "$DL/go.tgz"
  tar -xzf "$DL/go.tgz" -C "$VENV/opt"
fi
ln -sf "$VENV/opt/go/bin/go" "$VENV/bin/go"
ln -sf "$VENV/opt/go/bin/gofmt" "$VENV/bin/gofmt"

# ---------------------------------------------------------------- Python
PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  for c in python3.13 python3.12 /opt/homebrew/opt/python@3.13/bin/python3.13 python3; do
    if command -v "$c" >/dev/null 2>&1; then PY="$(command -v "$c")"; break; fi
  done
fi
if [ ! -x "$VENV/bin/python" ]; then
  log "creating Python virtualenv with $PY"
  "$PY" -m venv "$VENV"
fi
log "installing Python dependencies"
"$VENV/bin/python" -m pip install -q --upgrade pip
"$VENV/bin/python" -m pip install -q -r "$ROOT/python/requirements.txt"
"$VENV/bin/python" -m pip install -q -e "$ROOT/python"

# ---------------------------------------------------------------- Qdrant
if [ ! -x "$VENV/bin/qdrant" ]; then
  log "installing Qdrant $QDRANT_VERSION"
  fetch "https://github.com/qdrant/qdrant/releases/download/${QDRANT_VERSION}/qdrant-${QARCH}-${QOS}.tar.gz" "$DL/qdrant.tgz"
  tar -xzf "$DL/qdrant.tgz" -C "$VENV/bin"
  chmod +x "$VENV/bin/qdrant"
fi
mkdir -p "$ROOT/.qdrant_storage"

# ---------------------------------------------------------------- JDK + Kafka KRaft
if [ ! -d "$VENV/opt/jdk" ]; then
  log "installing Temurin JDK $JDK_MAJOR"
  fetch "https://api.adoptium.net/v3/binary/latest/${JDK_MAJOR}/ga/${JOS}/${JARCH}/jdk/hotspot/normal/eclipse" "$DL/jdk.tgz"
  mkdir -p "$VENV/opt/jdk-tmp"
  tar -xzf "$DL/jdk.tgz" -C "$VENV/opt/jdk-tmp"
  mv "$VENV/opt/jdk-tmp"/* "$VENV/opt/jdk"
  rmdir "$VENV/opt/jdk-tmp"
fi
if [ -d "$VENV/opt/jdk/Contents/Home" ]; then JHOME="$VENV/opt/jdk/Contents/Home"; else JHOME="$VENV/opt/jdk"; fi
ln -sfn "$JHOME" "$VENV/opt/java-home"

if [ ! -d "$VENV/opt/kafka" ]; then
  log "installing Apache Kafka $KAFKA_VERSION (KRaft mode, no ZooKeeper)"
  fetch "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz" "$DL/kafka.tgz" \
    || fetch "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz" "$DL/kafka.tgz"
  tar -xzf "$DL/kafka.tgz" -C "$VENV/opt"
  mv "$VENV/opt/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}" "$VENV/opt/kafka"
fi

# ---------------------------------------------------------------- Node / web
export npm_config_cache="$VENV/npm-cache"
export PLAYWRIGHT_BROWSERS_PATH="$VENV/ms-playwright"
if [ -f "$ROOT/web/package.json" ]; then
  log "installing web dependencies (npm cache in .venv/npm-cache)"
  (cd "$ROOT/web" && npm install --no-fund --no-audit --loglevel=error)
  log "installing Playwright chromium into .venv/ms-playwright"
  (cd "$ROOT/web" && npx --yes playwright install chromium >/dev/null)
fi

# ---------------------------------------------------------------- Go modules
export GOPATH="$VENV/gopath" GOMODCACHE="$VENV/gopath/pkg/mod" GOCACHE="$VENV/gocache"
log "downloading Go modules"
(cd "$ROOT" && "$VENV/bin/go" mod download)

cat > "$VENV/env.sh" <<EOF
# source .venv/env.sh to put the self-contained toolchain on PATH
export PITWALL_ROOT="$ROOT"
export JAVA_HOME="$VENV/opt/java-home"
export GOPATH="$VENV/gopath"
export GOMODCACHE="$VENV/gopath/pkg/mod"
export GOCACHE="$VENV/gocache"
export npm_config_cache="$VENV/npm-cache"
export PLAYWRIGHT_BROWSERS_PATH="$VENV/ms-playwright"
export PATH="$VENV/bin:$VENV/opt/kafka/bin:\$JAVA_HOME/bin:\$PATH"
EOF
log "done. run: source .venv/env.sh"
