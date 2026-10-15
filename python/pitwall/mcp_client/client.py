"""Async Model Context Protocol client for the Go Monte Carlo server.

Speaks JSON-RPC 2.0 over Streamable HTTP (POST /mcp) or stdio (spawning
``bin/mcp-sim -stdio``). Both transports perform the initialize handshake and
return the tool's ``structuredContent``.
"""
from __future__ import annotations

import asyncio
import itertools
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from pitwall import config

PROTOCOL_VERSION = "2025-06-18"


class MCPError(RuntimeError):
    pass


@dataclass
class CallStats:
    calls: int = 0
    total_ms: float = 0.0
    last_ms: float = 0.0
    by_tool: dict[str, list[float]] = field(default_factory=dict)


class _Base:
    def __init__(self):
        self._ids = itertools.count(1)
        self.server_info: dict = {}
        self.tools: list[dict] = []
        self.stats = CallStats()

    async def _rpc(self, method: str, params: dict | None = None, notify: bool = False) -> Any:
        raise NotImplementedError

    async def initialize(self) -> dict:
        res = await self._rpc("initialize", {"protocolVersion": PROTOCOL_VERSION, "capabilities": {},
                                             "clientInfo": {"name": "pitwall-agent", "version": "0.1.0"}})
        self.server_info = res
        await self._rpc("notifications/initialized", notify=True)
        self.tools = (await self._rpc("tools/list"))["tools"]
        return res

    async def call_tool(self, name: str, arguments: dict) -> dict:
        t0 = time.perf_counter()
        res = await self._rpc("tools/call", {"name": name, "arguments": arguments})
        ms = (time.perf_counter() - t0) * 1000
        self.stats.calls += 1
        self.stats.total_ms += ms
        self.stats.last_ms = ms
        self.stats.by_tool.setdefault(name, []).append(ms)
        if res.get("isError"):
            raise MCPError(f"{name}: {res['content'][0]['text'] if res.get('content') else 'tool error'}")
        if "structuredContent" in res:
            return res["structuredContent"]
        return json.loads(res["content"][0]["text"])

    @staticmethod
    def _unwrap(msg: dict) -> Any:
        if "error" in msg and msg["error"]:
            e = msg["error"]
            raise MCPError(f"JSON-RPC {e.get('code')}: {e.get('message')}")
        return msg.get("result")


class HTTPMCPClient(_Base):
    def __init__(self, url: str | None = None, timeout: float = 30.0):
        super().__init__()
        self.url = url or config.MCP_URL
        self._http = httpx.AsyncClient(timeout=timeout)
        self._session: str | None = None

    async def _rpc(self, method: str, params: dict | None = None, notify: bool = False) -> Any:
        msg: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        if not notify:
            msg["id"] = next(self._ids)
        headers = {"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": PROTOCOL_VERSION}
        if self._session:
            headers["Mcp-Session-Id"] = self._session
        r = await self._http.post(self.url, json=msg, headers=headers)
        r.raise_for_status()
        if sid := r.headers.get("Mcp-Session-Id"):
            self._session = sid
        if notify:
            return None
        return self._unwrap(r.json())

    async def aclose(self) -> None:
        await self._http.aclose()


class StdioMCPClient(_Base):
    """Spawns the Go server and talks newline-delimited JSON-RPC over pipes."""

    def __init__(self, binary: str | Path | None = None):
        super().__init__()
        self.binary = str(binary or config.ROOT / "bin" / "mcp-sim")
        self._proc: asyncio.subprocess.Process | None = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        self._proc = await asyncio.create_subprocess_exec(
            self.binary, "-stdio", "-quiet", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, limit=16 << 20)

    async def _rpc(self, method: str, params: dict | None = None, notify: bool = False) -> Any:
        assert self._proc and self._proc.stdin and self._proc.stdout, "call start() first"
        msg: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            msg["params"] = params
        rid = None if notify else next(self._ids)
        if rid is not None:
            msg["id"] = rid
        async with self._lock:  # one request in flight on the pipe
            self._proc.stdin.write((json.dumps(msg) + "\n").encode())
            await self._proc.stdin.drain()
            if notify:
                return None
            line = await self._proc.stdout.readline()
        if not line:
            raise MCPError("mcp-sim exited")
        resp = json.loads(line)
        if resp.get("id") != rid:
            raise MCPError(f"response id {resp.get('id')} != request id {rid}")
        return self._unwrap(resp)

    async def aclose(self) -> None:
        if self._proc:
            self._proc.stdin.close()
            try:
                await asyncio.wait_for(self._proc.wait(), 3)
            except asyncio.TimeoutError:
                self._proc.kill()
