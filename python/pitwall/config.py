"""Runtime configuration shared by every PitWall component (env-overridable)."""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(os.environ.get("PITWALL_ROOT", Path(__file__).resolve().parents[2]))
LAKEHOUSE = Path(os.environ.get("PITWALL_LAKEHOUSE", ROOT / "data" / "lakehouse"))
RACES = ROOT / "data" / "races"
FIA_DIR = ROOT / "data" / "fia_regulations"

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
PRIMARY_MODEL = os.environ.get("PITWALL_PRIMARY_MODEL", "qwen2.5-coder:14b")
VERIFIER_MODEL = os.environ.get("PITWALL_VERIFIER_MODEL", "llama3.2:3b")
EMBED_MODEL = os.environ.get("PITWALL_EMBED_MODEL", "bge-m3")

QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
QDRANT_COLLECTION = os.environ.get("QDRANT_COLLECTION", "fia_sporting_regs")

MCP_URL = os.environ.get("PITWALL_MCP_URL", "http://localhost:8081/mcp")
PROCESSOR_URL = os.environ.get("PITWALL_PROCESSOR_URL", "http://localhost:8090")

FASTEMBED_CACHE = os.environ.get("FASTEMBED_CACHE_PATH", str(ROOT / ".venv" / "fastembed_cache"))
