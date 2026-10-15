"""Dense embeddings from a local Ollama bge-m3 (1024-dim)."""
from __future__ import annotations

import time

import httpx

from pitwall import config


class OllamaEmbedder:
    def __init__(self, model: str | None = None, url: str | None = None, timeout: float = 120.0):
        self.model = model or config.EMBED_MODEL
        self.url = (url or config.OLLAMA_URL).rstrip("/")
        self._client = httpx.Client(timeout=timeout)
        self._async = httpx.AsyncClient(timeout=timeout)
        self.dim: int | None = None

    def embed(self, texts: list[str], batch: int = 16) -> list[list[float]]:
        out: list[list[float]] = []
        for i in range(0, len(texts), batch):
            chunk = texts[i : i + batch]
            for attempt in range(3):
                try:
                    r = self._client.post(f"{self.url}/api/embed", json={"model": self.model, "input": chunk, "truncate": True})
                    r.raise_for_status()
                    vecs = r.json()["embeddings"]
                    break
                except (httpx.HTTPError, KeyError):
                    if attempt == 2:
                        raise
                    time.sleep(1 + attempt)
            out.extend(vecs)
        if out:
            self.dim = len(out[0])
        return out

    def embed_one(self, text: str) -> list[float]:
        return self.embed([text])[0]

    async def aembed_one(self, text: str) -> list[float]:
        r = await self._async.post(f"{self.url}/api/embed", json={"model": self.model, "input": [text], "truncate": True})
        r.raise_for_status()
        return r.json()["embeddings"][0]
