from __future__ import annotations

import os
import shutil
import socket
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BIN = ROOT / "bin"
GO = ROOT / ".venv" / "bin" / "go"


def go_env() -> dict:
    env = dict(os.environ)
    venv = ROOT / ".venv"
    env.update(GOPATH=str(venv / "gopath"), GOMODCACHE=str(venv / "gopath" / "pkg" / "mod"), GOCACHE=str(venv / "gocache"))
    return env


@pytest.fixture(scope="session")
def binaries() -> Path:
    """Build the Go commands once per test session."""
    BIN.mkdir(exist_ok=True)
    subprocess.run([str(GO), "build", "-o", str(BIN) + "/", "./cmd/..."], cwd=ROOT, check=True, env=go_env())
    return BIN


@pytest.fixture(scope="session")
def lakehouse(tmp_path_factory, binaries) -> Path:
    """A fresh lakehouse built by the Go processor from the real Singapore 2023 race."""
    out = tmp_path_factory.mktemp("lakehouse")
    subprocess.run(
        [str(binaries / "processor"), "-from-file", str(ROOT / "data/races/session_9165_events.jsonl.gz"), "-out", str(out)],
        check=True, capture_output=True,
    )
    return out


def port_open(host: str, port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(0.3)
        return s.connect_ex((host, port)) == 0


@pytest.fixture(scope="session")
def mcp_server(binaries):
    """The real Go MCP server over HTTP on a free port."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen([str(binaries / "mcp-sim"), "-http", f"127.0.0.1:{port}", "-quiet"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    import time
    for _ in range(100):
        if port_open("127.0.0.1", port):
            break
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}/mcp"
    proc.terminate()
    proc.wait(timeout=5)


def ollama_ready(*models: str) -> bool:
    try:
        import httpx

        tags = httpx.get("http://localhost:11434/api/tags", timeout=1.0).json()
        have = {m["name"] for m in tags.get("models", [])} | {m["name"].split(":")[0] for m in tags.get("models", [])}
        return all(m in have for m in models)
    except Exception:
        return False


def qdrant_ready() -> bool:
    return port_open("127.0.0.1", 6333)


requires_ollama = pytest.mark.skipif(not ollama_ready("bge-m3", "llama3.2:3b", "qwen2.5-coder:14b"),
                                     reason="Ollama models not available")
requires_qdrant = pytest.mark.skipif(not qdrant_ready(), reason="Qdrant not running on :6333")
have_go = pytest.mark.skipif(shutil.which(str(GO)) is None, reason="Go toolchain not bootstrapped")
