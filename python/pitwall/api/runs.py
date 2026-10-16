"""Strategy run lifecycle for the control plane.

Each run executes the LangGraph agent (or the single-pass baseline) in a
background task and appends every node event to an ordered, append-only log.
SSE subscribers replay the log from any offset (Last-Event-ID) and then wait
for new events, so a client can connect late or reconnect without losing
anything. Runs share the compiled graph and its MemorySaver checkpointer, so
the checkpoint history of every run is inspectable by thread id.
"""
from __future__ import annotations

import asyncio
import itertools
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from pitwall.agents import rules
from pitwall.agents.graph import AgentDeps, build_graph, run_agent, run_single_pass
from pitwall.agents.llm import FailoverLLM, FaultInjector, OllamaChat

MODES = ("agent", "single_pass")
_seq = itertools.count(1)


@dataclass
class Run:
    id: str
    scenario: dict
    mode: str
    fault_rate: float
    created_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    status: str = "queued"        # queued -> running -> succeeded | failed
    events: list[dict] = field(default_factory=list)
    result: dict | None = None
    error: str | None = None
    finished_ms: int | None = None
    _waiters: list[asyncio.Future] = field(default_factory=list, repr=False)

    def push(self, kind: str, data: dict) -> dict:
        ev = {"id": len(self.events), "type": kind, "ts_ms": int(time.time() * 1000), "data": data}
        self.events.append(ev)
        for f in self._waiters:
            if not f.done():
                f.set_result(None)
        self._waiters.clear()
        return ev

    async def wait(self, timeout: float) -> bool:
        """Wait for the next event; False on timeout."""
        f = asyncio.get_running_loop().create_future()
        self._waiters.append(f)
        try:
            await asyncio.wait_for(f, timeout)
            return True
        except asyncio.TimeoutError:
            return False

    @property
    def done(self) -> bool:
        return self.status in ("succeeded", "failed")

    def summary(self) -> dict:
        final = (self.result or {}).get("final") or {}
        return {"run_id": self.id, "scenario_id": self.scenario.get("id"), "title": self.scenario.get("title"),
                "mode": self.mode, "status": self.status, "created_ms": self.created_ms, "finished_ms": self.finished_ms,
                "recommended_action": final.get("recommended_action"), "events": len(self.events), "error": self.error}


class RunManager:
    def __init__(self, mcp, retriever=None, lakehouse=None, max_concurrent: int = 1, keep: int = 200, chat=None):
        self.mcp, self.retriever, self.lakehouse = mcp, retriever, lakehouse
        self.chat = chat or OllamaChat()
        self.graph = build_graph()
        self.runs: dict[str, Run] = {}
        self._sem = asyncio.Semaphore(max_concurrent)  # local Ollama serves one heavy request at a time
        self._keep = keep
        self._tasks: set[asyncio.Task] = set()

    def start(self, scenario: dict, mode: str = "agent", fault_rate: float = 0.0, use_critic: bool = True) -> Run:
        if mode not in MODES:
            raise ValueError(f"mode must be one of {MODES}")
        if not 0.0 <= fault_rate <= 1.0:
            raise ValueError("fault_rate must be within [0, 1]")
        run = Run(id=f"run-{next(_seq):04d}-{uuid.uuid4().hex[:6]}", scenario=scenario, mode=mode, fault_rate=fault_rate)
        self.runs[run.id] = run
        self._evict()
        task = asyncio.create_task(self._execute(run, use_critic))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return run

    def _evict(self) -> None:
        done = [r for r in self.runs.values() if r.done]
        for r in sorted(done, key=lambda r: r.created_ms)[: max(0, len(self.runs) - self._keep)]:
            self.runs.pop(r.id, None)

    async def _execute(self, run: Run, use_critic: bool) -> None:
        run.push("queued", {"scenario_id": run.scenario.get("id"), "mode": run.mode, "fault_rate": run.fault_rate})
        async with self._sem:
            run.status = "running"
            llm = FailoverLLM(chat=self.chat, faults=FaultInjector(rate=run.fault_rate, seed=hash(run.id) & 0xFFFF))
            deps = AgentDeps(llm=llm, mcp=self.mcp, retriever=self.retriever, lakehouse=self.lakehouse,
                             use_critic=use_critic, on_event=lambda ev: run.push("node", ev))
            run.push("started", {"title": run.scenario.get("title"), "edition": run.scenario.get("context", {}).get("edition")})
            t0 = time.perf_counter()
            try:
                if run.mode == "agent":
                    st = await run_agent(run.scenario, deps, graph=self.graph, thread_id=run.id)
                    result = {k: st.get(k) for k in ("final", "proposals", "verdict", "simulation", "regulations",
                                                       "constraints", "telemetry", "trace")}
                else:
                    sp = await run_single_pass(run.scenario, deps)
                    result = {"final": sp["final"], "model": sp["model"], "failover": sp["failover"]}
                result["elapsed_ms"] = round((time.perf_counter() - t0) * 1000, 1)
                result["llm"] = llm.stats.summary()
                result["assessment"] = assess(run.scenario, result["final"])
                run.result, run.status = result, "succeeded"
                run.push("final", {"final": result["final"], "assessment": result["assessment"],
                                   "elapsed_ms": result["elapsed_ms"], "llm": result["llm"]})
            except Exception as e:  # noqa: BLE001 - reported to the client, the server keeps running
                run.error, run.status = f"{type(e).__name__}: {e}", "failed"
                run.push("error", {"error": run.error})
            finally:
                run.finished_ms = int(time.time() * 1000)
                run.push("done", {"status": run.status})

    def checkpoints(self, run_id: str) -> list[dict]:
        out = []
        for snap in self.graph.get_state_history({"configurable": {"thread_id": run_id}}):
            out.append({"step": snap.metadata.get("step"), "next": list(snap.next),
                         "writes": list((snap.metadata.get("writes") or {}).keys()),
                         "checkpoint_id": snap.config["configurable"].get("checkpoint_id"),
                         "reflection_count": snap.values.get("reflection_count")})
        return out

    async def shutdown(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if hasattr(self.chat, "aclose"):
            await self.chat.aclose()


def assess(scenario: dict, final: dict) -> dict:
    """Compliance under the edition in force, and agreement with ground truth if known."""
    c = rules.constraints_from(scenario["state"])
    violations = [v.as_dict() for v in rules.check(final, c)]
    # compliance covers the FIA regulations and tyre physics; verifier items
    # such as rejoin traffic are reported separately
    out: dict[str, Any] = {"compliant": not violations, "violations": violations, "edition": c.edition,
                           "unresolved_verifier_items": final.get("unresolved_violations", [])}
    gt = scenario.get("ground_truth")
    if gt:
        out["agrees_with_ground_truth"] = final.get("recommended_action") in gt["acceptable"]
        out["acceptable"] = gt["acceptable"]
    return out
