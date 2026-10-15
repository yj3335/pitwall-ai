"""Local Ollama chat with structured JSON output, asymmetric failover and a
circuit breaker.

The primary model (qwen2.5-coder:14b) handles SQL, tool arguments and
strategy synthesis. When it fails (timeout, HTTP error, invalid JSON or an
injected fault), the same request is retried on the fast model
(llama3.2:3b). After ``trip_after`` consecutive primary failures the breaker
opens and requests go straight to the fallback until ``cooldown_s`` passes.
"""
from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

from pitwall import config


class InjectedFault(RuntimeError):
    """Synthetic upstream failure used by the failover benchmark."""


class LLMError(RuntimeError):
    pass


@dataclass
class FaultInjector:
    rate: float = 0.0
    seed: int = 7
    injected: int = 0

    def __post_init__(self):
        self._rng = random.Random(self.seed)

    def maybe_fail(self, model: str) -> None:
        if self.rate > 0 and self._rng.random() < self.rate:
            self.injected += 1
            raise InjectedFault(f"injected upstream fault on {model}")


@dataclass
class CallRecord:
    role: str
    model: str
    ms: float
    ok: bool
    failover: bool = False
    error: str | None = None
    prompt_tokens: int = 0
    output_tokens: int = 0


@dataclass
class LLMStats:
    calls: list[CallRecord] = field(default_factory=list)
    faults: int = 0
    recovered: int = 0

    def summary(self) -> dict:
        return {
            "calls": len(self.calls),
            "failovers": sum(c.failover for c in self.calls),
            "faults": self.faults,
            "recovered": self.recovered,
            "by_model": {m: sum(1 for c in self.calls if c.model == m) for m in {c.model for c in self.calls}},
        }


class OllamaChat:
    def __init__(self, url: str | None = None, timeout: float = 180.0):
        self.url = (url or config.OLLAMA_URL).rstrip("/")
        self._http = httpx.AsyncClient(timeout=timeout)

    async def chat(self, model: str, messages: list[dict], schema: dict | None = None,
                   temperature: float = 0.0, num_predict: int = 768, num_ctx: int = 8192) -> tuple[str, dict]:
        body: dict[str, Any] = {
            "model": model, "messages": messages, "stream": False, "keep_alive": "30m",
            "options": {"temperature": temperature, "num_predict": num_predict, "num_ctx": num_ctx, "seed": 42},
        }
        if schema is not None:
            body["format"] = schema
        r = await self._http.post(f"{self.url}/api/chat", json=body)
        r.raise_for_status()
        data = r.json()
        return data["message"]["content"], data

    async def aclose(self) -> None:
        await self._http.aclose()


def parse_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text[text.find("{"):]
    start, end = text.find("{"), text.rfind("}")
    if start < 0 or end < 0:
        raise LLMError(f"no JSON object in model output: {text[:120]!r}")
    return json.loads(text[start : end + 1])


class FailoverLLM:
    def __init__(self, primary: str | None = None, fallback: str | None = None, chat: OllamaChat | None = None,
                 faults: FaultInjector | None = None, trip_after: int = 3, cooldown_s: float = 30.0):
        self.primary = primary or config.PRIMARY_MODEL
        self.fallback = fallback or config.VERIFIER_MODEL
        self.chat_client = chat or OllamaChat()
        self.faults = faults or FaultInjector(0.0)
        self.trip_after = trip_after
        self.cooldown_s = cooldown_s
        self._consecutive = 0
        self._open_until = 0.0
        self.stats = LLMStats()

    @property
    def breaker_open(self) -> bool:
        return time.monotonic() < self._open_until

    async def _call(self, model: str, role: str, messages: list[dict], schema: dict | None, **kw) -> tuple[dict, CallRecord]:
        t0 = time.perf_counter()
        text, raw = await self.chat_client.chat(model, messages, schema=schema, **kw)
        obj = parse_json(text) if schema is not None else {"text": text}
        rec = CallRecord(role=role, model=model, ms=(time.perf_counter() - t0) * 1000, ok=True,
                         prompt_tokens=raw.get("prompt_eval_count", 0), output_tokens=raw.get("eval_count", 0))
        return obj, rec

    async def complete_json(self, role: str, messages: list[dict], schema: dict | None, *, model: str | None = None,
                            inject: bool = True, **kw) -> tuple[dict, CallRecord]:
        """Run on ``model`` (default: primary) with failover to the fast model."""
        target = model or self.primary
        if target == self.primary and not self.breaker_open:
            try:
                if inject:
                    self.faults.maybe_fail(target)
                obj, rec = await self._call(target, role, messages, schema, **kw)
                self._consecutive = 0
                self.stats.calls.append(rec)
                return obj, rec
            except (InjectedFault, httpx.HTTPError, LLMError, json.JSONDecodeError, KeyError) as e:
                self.stats.faults += 1
                self._consecutive += 1
                if self._consecutive >= self.trip_after:
                    self._open_until = time.monotonic() + self.cooldown_s
                err = f"{type(e).__name__}: {e}"
            target_fb = self.fallback
            try:
                obj, rec = await self._call(target_fb, role, messages, schema, **kw)
            except Exception as e2:  # noqa: BLE001
                self.stats.calls.append(CallRecord(role, target_fb, 0, False, True, f"{err}; fallback {e2}"))
                raise LLMError(f"primary and fallback failed: {err}; {e2}") from e2
            rec.failover, rec.error = True, err
            self.stats.recovered += 1
            self.stats.calls.append(rec)
            return obj, rec
        if target == self.primary:  # breaker open: go straight to the fast model
            obj, rec = await self._call(self.fallback, role, messages, schema, **kw)
            rec.failover, rec.error = True, "circuit open"
            self.stats.calls.append(rec)
            return obj, rec
        obj, rec = await self._call(target, role, messages, schema, **kw)
        self.stats.calls.append(rec)
        return obj, rec
