"""PitWall control plane: FastAPI on :8080.

    uvicorn pitwall.api.app:app --port 8080

Strategy runs stream their LangGraph trace over Server-Sent Events; live
timing is proxied from the Go processor; race-control messages are parsed,
published to Kafka through the processor and can trigger an agent run on the
live, point-in-time state of a car.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from pitwall import config
from pitwall.api import live as live_mod
from pitwall.api.race_control import apply_to_state, parse
from pitwall.api.runs import MODES, RunManager
from pitwall.eval.scenarios import load_scenarios, materialise
from pitwall.mcp_client.client import HTTPMCPClient
from pitwall.olap import Lakehouse

HEARTBEAT_S = 15.0


class RunRequest(BaseModel):
    scenario_id: str | None = None
    scenario: dict[str, Any] | None = Field(None, description="custom synthetic scenario with an inline state")
    mode: str = "agent"
    fault_rate: float = Field(0.0, ge=0.0, le=1.0)
    use_critic: bool = True


class ParseRequest(BaseModel):
    message: str = Field(..., min_length=1)


class TriggerRequest(BaseModel):
    message: str = Field(..., min_length=1)
    lap_number: int | None = None
    publish: bool = True
    driver_number: int | None = Field(None, description="car to re-evaluate; defaults to the penalised car")
    run_agent: bool = True
    mode: str = "agent"
    fault_rate: float = Field(0.0, ge=0.0, le=1.0)
    track_wetness: float | None = Field(None, ge=0.0, le=1.0)
    total_laps: int | None = None


class Services:
    """Process-wide clients, created in the lifespan (overridable in tests)."""

    def __init__(self, mcp=None, retriever=None, lakehouse=None, processor=None, runs: RunManager | None = None):
        self.mcp = mcp
        self.retriever = retriever
        self.lakehouse = lakehouse
        self.processor = processor
        self.runs = runs
        self.scenarios = {s["id"]: s for s in load_scenarios()}


def _default_services() -> Services:
    from pitwall.rag.store import HybridRetriever

    mcp = HTTPMCPClient()
    lakehouse = Lakehouse()
    retriever = HybridRetriever()
    return Services(mcp=mcp, retriever=retriever, lakehouse=lakehouse, processor=live_mod.ProcessorClient(),
                    runs=RunManager(mcp, retriever, lakehouse))


def create_app(services: Services | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.svc = services or _default_services()
        svc = app.state.svc
        if svc.mcp is not None and not svc.mcp.server_info:
            try:
                await svc.mcp.initialize()
            except Exception:  # noqa: BLE001 - reported by /api/status; runs retry on demand
                pass
        yield
        if svc.runs:
            await svc.runs.shutdown()

    app = FastAPI(title="PitWall AI control plane", version="0.1.0", lifespan=lifespan)
    # the dashboard runs on loopback (dev on :3000, e2e on :3100); extra origins via env
    extra = [o.strip() for o in os.environ.get("PITWALL_CORS_ORIGINS", "").split(",") if o.strip()]
    app.add_middleware(CORSMiddleware, allow_origins=extra, allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?",
                       allow_methods=["*"], allow_headers=["*"])

    def svc(request: Request) -> Services:
        return request.app.state.svc

    async def ensure_mcp(s: Services) -> None:
        if not s.mcp.server_info:
            try:
                await s.mcp.initialize()
            except Exception as e:  # noqa: BLE001
                raise HTTPException(503, f"MCP simulator unavailable at {config.MCP_URL}: {e}") from e

    # ------------------------------------------------------------ health
    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/api/status")
    async def status(request: Request):
        s = svc(request)

        async def probe(url: str) -> bool:
            try:
                async with httpx.AsyncClient(timeout=1.0) as c:
                    return (await c.get(url)).status_code < 500
            except httpx.HTTPError:
                return False

        checks = await asyncio.gather(
            probe(config.MCP_URL.rsplit("/", 1)[0] + "/healthz"), probe(f"{config.QDRANT_URL}/healthz"),
            probe(f"{config.OLLAMA_URL}/api/tags"),
            s.processor.healthy() if s.processor else asyncio.sleep(0, result=False))
        sessions = []
        if s.lakehouse is not None:
            try:
                sessions = s.lakehouse.sessions()
            except Exception:  # noqa: BLE001
                sessions = []
        runs = list(s.runs.runs.values()) if s.runs else []
        return {"mcp_sim": checks[0], "qdrant": checks[1], "ollama": checks[2], "processor": checks[3],
                "lakehouse_sessions": sessions, "models": {"primary": config.PRIMARY_MODEL, "verifier": config.VERIFIER_MODEL,
                                                           "embedding": config.EMBED_MODEL},
                "runs": {"total": len(runs), "active": sum(not r.done for r in runs)}}

    # --------------------------------------------------------- scenarios
    @app.get("/api/scenarios")
    async def list_scenarios(request: Request):
        return [{"id": x["id"], "kind": x["kind"], "category": x["category"], "title": x["title"], "question": x["question"],
                 "team_radio": x.get("team_radio", "")} for x in svc(request).scenarios.values()]

    @app.get("/api/scenarios/{sid}")
    async def get_scenario(sid: str, request: Request):
        s = svc(request)
        if sid not in s.scenarios:
            raise HTTPException(404, f"unknown scenario {sid}")
        m = await asyncio.to_thread(materialise, s.scenarios[sid], s.lakehouse)
        return {k: m.get(k) for k in ("id", "kind", "category", "title", "question", "race_control", "team_radio", "state", "context",
                                      "ground_truth", "source")}

    # ------------------------------------------------------------- runs

    async def _scenario_for(s: Services, req: RunRequest) -> dict:
        if req.scenario_id:
            if req.scenario_id not in s.scenarios:
                raise HTTPException(404, f"unknown scenario {req.scenario_id}")
            return await asyncio.to_thread(materialise, s.scenarios[req.scenario_id], s.lakehouse)
        if req.scenario:
            custom = {"id": req.scenario.get("id", "CUSTOM"), "kind": "synthetic", "category": "custom",
                      "title": req.scenario.get("title", "Custom scenario"), "question": req.scenario.get("question", ""),
                      "race_control": req.scenario.get("race_control", ""), "team_radio": req.scenario.get("team_radio", ""),
                      "state": req.scenario.get("state")}
            if not isinstance(custom["state"], dict) or "car" not in custom["state"]:
                raise HTTPException(422, "scenario.state with a car is required")
            try:
                return materialise(custom)
            except (KeyError, ValueError, TypeError) as e:
                raise HTTPException(422, f"invalid scenario: {e}") from e
        raise HTTPException(422, "give scenario_id or scenario")

    @app.post("/api/strategy/runs", status_code=202)
    async def start_run(req: RunRequest, request: Request):
        s = svc(request)
        if req.mode not in MODES:
            raise HTTPException(422, f"mode must be one of {MODES}")
        scenario = await _scenario_for(s, req)
        await ensure_mcp(s)
        run = s.runs.start(scenario, mode=req.mode, fault_rate=req.fault_rate, use_critic=req.use_critic)
        return {**run.summary(), "events_url": f"/api/strategy/runs/{run.id}/events"}

    @app.post("/api/strategy")
    async def run_and_wait(req: RunRequest, request: Request):
        """Synchronous convenience: start a run and return its result."""
        s = svc(request)
        scenario = await _scenario_for(s, req)
        await ensure_mcp(s)
        run = s.runs.start(scenario, mode=req.mode, fault_rate=req.fault_rate, use_critic=req.use_critic)
        deadline = time.monotonic() + 300
        while not run.done and time.monotonic() < deadline:
            await run.wait(5)
        if not run.done:
            raise HTTPException(504, f"run {run.id} still running; follow /api/strategy/runs/{run.id}/events")
        if run.status == "failed":
            raise HTTPException(500, run.error)
        return {"run_id": run.id, **{k: run.result.get(k) for k in ("final", "assessment", "elapsed_ms", "llm")}}

    @app.get("/api/strategy/runs")
    async def list_runs(request: Request):
        return [r.summary() for r in sorted(svc(request).runs.runs.values(), key=lambda r: -r.created_ms)]

    def _run(request: Request, run_id: str):
        run = svc(request).runs.runs.get(run_id)
        if run is None:
            raise HTTPException(404, f"unknown run {run_id}")
        return run

    @app.get("/api/strategy/runs/{run_id}")
    async def get_run(run_id: str, request: Request):
        run = _run(request, run_id)
        return {**run.summary(), "scenario": {k: run.scenario.get(k) for k in ("id", "title", "question", "state", "context")},
                "result": run.result, "events": run.events}

    @app.get("/api/strategy/runs/{run_id}/checkpoints")
    async def get_checkpoints(run_id: str, request: Request):
        run = _run(request, run_id)
        if run.mode != "agent":
            return []
        return svc(request).runs.checkpoints(run_id)

    @app.get("/api/strategy/runs/{run_id}/events")
    async def run_events(run_id: str, request: Request):
        run = _run(request, run_id)
        last = request.headers.get("last-event-id") or request.query_params.get("from")
        start = int(last) + 1 if last is not None and str(last).lstrip("-").isdigit() else 0

        async def stream() -> AsyncIterator[bytes]:
            idx = start
            yield b"retry: 2000\n\n"
            while True:
                while idx < len(run.events):
                    ev = run.events[idx]
                    yield f"id: {ev['id']}\nevent: {ev['type']}\ndata: {json.dumps(ev, default=str)}\n\n".encode()
                    idx += 1
                if run.done and idx >= len(run.events):
                    return
                if await request.is_disconnected():
                    return
                if not await run.wait(HEARTBEAT_S):
                    yield b": keep-alive\n\n"

        return StreamingResponse(stream(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    # ------------------------------------------------------------- live
    async def _processor(request: Request):
        p = svc(request).processor
        if p is None:
            raise HTTPException(503, "processor client not configured")
        return p

    @app.get("/api/live/tower")
    async def live_tower(request: Request):
        p = await _processor(request)
        try:
            return await p.tower()
        except httpx.HTTPError as e:
            raise HTTPException(503, f"processor unavailable: {e}") from e

    @app.get("/api/live/stream")
    async def live_stream(request: Request, interval_ms: int = 1000):
        """SSE: tower snapshot every interval, plus new triggers and race-control messages."""
        p = await _processor(request)
        interval = max(0.2, min(10.0, interval_ms / 1000))

        async def stream() -> AsyncIterator[bytes]:
            seen_trig: set[tuple] = set()
            seen_rc: set[tuple] = set()
            n = 0
            yield b"retry: 2000\n\n"
            while not await request.is_disconnected():
                try:
                    tower, trig, rc = await asyncio.gather(p.tower(), p.triggers(), p.race_control())
                except httpx.HTTPError as e:
                    yield f"event: unavailable\ndata: {json.dumps({'error': str(e)})}\n\n".encode()
                    await asyncio.sleep(interval * 2)
                    continue
                yield f"id: {n}\nevent: tower\ndata: {json.dumps(tower)}\n\n".encode()
                n += 1
                for t in trig:
                    k = (t["driver_number"], t["kind"], t["ts_ms"])
                    if k not in seen_trig:
                        seen_trig.add(k)
                        yield f"event: trigger\ndata: {json.dumps(t)}\n\n".encode()
                for m in rc:
                    k = (m["ts_ms"], m["message"])
                    if k not in seen_rc:
                        seen_rc.add(k)
                        yield f"event: race_control\ndata: {json.dumps({**m, 'parsed': parse(m['message']).as_dict()})}\n\n".encode()
                await asyncio.sleep(interval)

        return StreamingResponse(stream(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    # ---------------------------------------------------- race control

    @app.post("/api/race-control/parse")
    async def parse_message(req: ParseRequest):
        return parse(req.message).as_dict()


    @app.post("/api/race-control/trigger", status_code=202)
    async def trigger(req: TriggerRequest, request: Request):
        s = svc(request)
        ev = parse(req.message)
        out: dict[str, Any] = {"event": ev.as_dict(), "published": None, "run": None}
        p = s.processor
        if req.publish:
            if p is None:
                raise HTTPException(503, "processor client not configured")
            msg = {"message": ev.message, "category": "SafetyCar" if ev.track_status in ("SC", "VSC") else "Other",
                   "lap_number": req.lap_number or 0, "driver_number": ev.car_number or 0,
                   "flag": "RED" if ev.kind == "RED_FLAG" else ""}
            try:
                out["published"] = await p.publish_race_control(msg)
            except httpx.HTTPError as e:
                raise HTTPException(502, f"could not publish to Kafka via the processor: {e}") from e
        target = req.driver_number or ev.car_number
        if req.run_agent and target is not None:
            if p is None:
                raise HTTPException(503, "processor client not configured")
            try:
                tower = await p.tower()
                scenario = live_mod.live_scenario(tower, target, total_laps=req.total_laps, track_wetness=req.track_wetness,
                                                  question=f"Race control: {ev.message}. What is the call for car {target}?")
            except httpx.HTTPError as e:
                raise HTTPException(503, f"processor unavailable: {e}") from e
            except (LookupError, ValueError) as e:
                raise HTTPException(409, str(e)) from e
            scenario["state"] = apply_to_state(scenario["state"], ev)
            await ensure_mcp(s)
            run = s.runs.start(scenario, mode=req.mode, fault_rate=req.fault_rate)
            out["run"] = {**run.summary(), "events_url": f"/api/strategy/runs/{run.id}/events"}
        return out

    return app


app = create_app()
