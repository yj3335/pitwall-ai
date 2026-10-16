"""FastAPI control plane: SSE run streaming, live state, race-control triggers.

The app runs in-process against the real Go MCP server (stdio); the LLM is
scripted so the cyclic path is deterministic, and the processor is a fake
except in the Kafka integration test at the bottom.
"""
from __future__ import annotations

import asyncio
import json
import socket
import subprocess
import time

import httpx
import pytest
import uvicorn

from conftest import ROOT, port_open
from pitwall.api.app import Services, create_app
from pitwall.api.live import ProcessorClient, live_scenario
from pitwall.api.race_control import apply_to_state, parse
from pitwall.api.runs import RunManager
from pitwall.mcp_client.client import StdioMCPClient
from test_agent import ScriptedChat, proposal


# ---------------------------------------------------------------- parser
@pytest.mark.parametrize("msg,kind,status,pen,car,closed", [
    ("VIRTUAL SAFETY CAR DEPLOYED", "VSC_DEPLOYED", "VSC", None, None, None),
    ("SAFETY CAR DEPLOYED. PIT ENTRY CLOSED", "SC_DEPLOYED", "SC", None, None, True),
    ("RED FLAG", "RED_FLAG", "RED", None, None, None),
    ("CHEQUERED FLAG", "CHEQUERED_FLAG", None, None, None, None),  # contains "RED FLAG" as a substring
    ("FIA STEWARDS: 5 SECOND TIME PENALTY FOR CAR 63 (RUS) - CAUSING A COLLISION", "TIME_PENALTY", None, 5.0, 63, None),
    ("VIRTUAL SAFETY CAR ENDING", "VSC_ENDING", "GREEN", None, None, None),
    ("PIT ENTRY CLOSED", "PIT_ENTRY_CLOSED", None, None, None, True),
    ("WAVED BLUE FLAG FOR CAR 2 (SAR) TIMED AT 13:05:01", "INFO", None, None, None, None),
])
def test_race_control_parser(msg, kind, status, pen, car, closed):
    ev = parse(msg)
    assert (ev.kind, ev.track_status, ev.penalty_s, ev.car_number, ev.pit_entry_closed) == (kind, status, pen, car, closed)


def test_race_control_parser_covers_real_messages():
    import gzip

    kinds = set()
    for key in (9558, 9165, 9636):
        for m in json.load(gzip.open(ROOT / f"data/races/{key}/race_control.json.gz", "rt")):
            kinds.add(parse(m["message"]).kind)
    assert {"SC_DEPLOYED", "VSC_DEPLOYED", "RED_FLAG", "TIME_PENALTY", "CHEQUERED_FLAG"} <= kinds


def tower(lap: int = 30) -> dict:
    def snap(n, code, pos, gap, comp, age, deg=None):
        return {"session_key": 9165, "driver_number": n, "driver_code": code, "lap_number": lap, "position": pos,
                "compound": comp, "tyre_age_laps": age, "last_lap_ms": 99000, "gap_to_leader_ms": gap,
                "interval_ahead_ms": 1000, "interval_behind_ms": 1000, "deg_slope_ms_per_lap": deg, "window": [],
                "pit_stops": 1, "compounds_used": ["MEDIUM", comp], "speed_kph": 250, "track_status": "GREEN",
                "rainfall": 0, "track_temp_c": 30, "ts_ms": 1694958000000}
    return {"drivers": [snap(55, "SAI", 1, 0, "HARD", 10, 30.0), snap(63, "RUS", 2, 1500, "HARD", 10, 40.0),
                        snap(44, "HAM", 3, 3200, "HARD", 10)], "stats": {"events": 1}}


def test_live_scenario_is_point_in_time():
    s = live_scenario(tower(), 63)
    assert s["state"]["total_laps"] == 62 and s["state"]["current_lap"] == 30
    assert s["context"]["edition"] == "2023-i6" and s["context"]["decision_ts_ms"] == 1694958000000
    assert s["state"]["pit_loss_green_s"] == 30.5  # measured Singapore green-flag loss
    assert [f["driver_number"] for f in s["state"]["field"]] == [55, 44]
    ev = parse("FIA STEWARDS: 5 SECOND TIME PENALTY FOR CAR 63 (RUS) - CAUSING A COLLISION")
    st = apply_to_state(s["state"], ev)
    assert st["car"]["penalty_s"] == 5 and s["state"]["car"]["penalty_s"] == 0  # copy, not mutation
    with pytest.raises(LookupError):
        live_scenario(tower(), 99)


# ---------------------------------------------------------------- app
class FakeProcessor:
    def __init__(self):
        self.published: list[dict] = []

    async def tower(self):
        return tower()

    async def triggers(self):
        return [{"driver_number": 63, "kind": "UNDERCUT_WINDOW", "ts_ms": 1, "detail": "x", "lap_number": 30}]

    async def race_control(self):
        return [{"ts_ms": 1, "message": "VIRTUAL SAFETY CAR DEPLOYED", "lap_number": 30}]

    async def publish_race_control(self, msg):
        self.published.append(msg)
        return {**msg, "ts_ms": 2, "session_key": 9165}

    async def healthy(self):
        return True


@pytest.fixture
async def api(binaries):
    mcp = StdioMCPClient(binaries / "mcp-sim")
    await mcp.start()
    await mcp.initialize()
    chat = ScriptedChat([proposal("PIT_NOW_MEDIUM", "MEDIUM", 32), proposal("PIT_NOW_HARD", "HARD", 32)])
    proc = FakeProcessor()
    svc = Services(mcp=mcp, retriever=None, lakehouse=None, processor=proc,
                   runs=RunManager(mcp, None, None, chat=chat))
    app = create_app(svc)
    # a real server on a socket: httpx.ASGITransport buffers whole responses,
    # which would hide (or hang on) Server-Sent Event streaming
    with socket.socket() as sk:
        sk.bind(("127.0.0.1", 0))
        port = sk.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on"))
    task = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.02)
    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=120) as client:
        yield client, svc, proc, chat
    server.should_exit = True
    await asyncio.wait_for(task, 20)
    await mcp.aclose()


async def sse(client, url, headers=None, limit=200):
    events = []
    async with client.stream("GET", url, headers=headers or {}) as r:
        assert r.headers["content-type"].startswith("text/event-stream")
        buf = {}
        async for line in r.aiter_lines():
            if line.startswith("id: "):
                buf["id"] = int(line[4:])
            elif line.startswith("event: "):
                buf["event"] = line[7:]
            elif line.startswith("data: "):
                buf["data"] = json.loads(line[6:])
            elif line == "" and "event" in buf:
                events.append(buf)
                buf = {}
                if len(events) >= limit:
                    break
    return events


async def test_scenarios_endpoints(api):
    client, *_ = api
    lst = (await client.get("/api/scenarios")).json()
    assert len(lst) == 24 and {"id", "title", "question"} <= set(lst[0])
    gp08 = (await client.get("/api/scenarios/GP08")).json()
    assert gp08["context"]["edition"] == "2025-i5" and gp08["state"]["car"]["compounds_used"] == ["MEDIUM"]
    assert (await client.get("/api/scenarios/NOPE")).status_code == 404


async def test_run_streams_cyclic_trace_over_sse(api):
    client, svc, _, _ = api
    r = await client.post("/api/strategy/runs", json={"scenario_id": "GP08"})
    assert r.status_code == 202
    run_id = r.json()["run_id"]
    events = await sse(client, f"/api/strategy/runs/{run_id}/events")
    types = [e["event"] for e in events]
    assert types[0] == "queued" and types[1] == "started" and types[-2:] == ["final", "done"]
    assert [e["id"] for e in events] == list(range(len(events)))  # ordered, gap-free
    nodes = [e["data"]["data"]["node"] for e in events if e["event"] == "node"]
    assert nodes == ["telemetry_olap", "fia_regulation_rag", "monte_carlo_mcp", "strategy_synthesizer", "verifier",
                     "strategy_synthesizer", "verifier", "finalize"]
    final = events[-2]["data"]["data"]
    assert final["final"]["recommended_action"] == "PIT_NOW_HARD"
    assert final["assessment"]["compliant"] and final["assessment"]["agrees_with_ground_truth"]
    # resume from an event id: only later events are replayed
    resumed = await sse(client, f"/api/strategy/runs/{run_id}/events", headers={"Last-Event-ID": "5"})
    assert [e["id"] for e in resumed] == list(range(6, len(events)))
    detail = (await client.get(f"/api/strategy/runs/{run_id}")).json()
    assert detail["status"] == "succeeded" and detail["result"]["simulation"]["rollouts"] == 10_000
    cps = (await client.get(f"/api/strategy/runs/{run_id}/checkpoints")).json()
    assert len(cps) >= 8 and any(c["reflection_count"] == 1 for c in cps)
    assert run_id in [x["run_id"] for x in (await client.get("/api/strategy/runs")).json()]


async def test_late_subscriber_receives_live_events(api):
    client, *_ = api
    run_id = (await client.post("/api/strategy/runs", json={"scenario_id": "GP08"})).json()["run_id"]
    # subscribe immediately: events arrive while the run is still executing
    events = await sse(client, f"/api/strategy/runs/{run_id}/events")
    assert events[-1]["event"] == "done" and events[-1]["data"]["data"]["status"] == "succeeded"


async def test_sync_endpoint_custom_scenario_and_validation(api):
    client, _, _, chat = api
    chat.synth = [proposal("PIT_NOW_HARD", "HARD", 30)]
    custom = {"title": "custom VSC", "state": {
        "total_laps": 58, "current_lap": 30, "track_status": "VSC",
        "car": {"driver_code": "PIA", "driver_number": 81, "position": 3, "compound": "MEDIUM", "tyre_age_laps": 22,
                "compounds_used": ["MEDIUM"], "gap_to_leader_ms": 6000},
        "field": [["VER", 1, 1, 0, "MEDIUM", 23]]}}
    r = await client.post("/api/strategy", json={"scenario": custom, "mode": "agent"})
    assert r.status_code == 200, r.text
    assert r.json()["final"]["recommended_action"] == "PIT_NOW_HARD" and r.json()["assessment"]["compliant"]
    sp = await client.post("/api/strategy", json={"scenario_id": "GP12", "mode": "single_pass"})
    assert sp.status_code == 200 and sp.json()["final"]["recommended_action"] in {"PIT_NOW_HARD", "PIT_NOW_MEDIUM"}
    assert (await client.post("/api/strategy/runs", json={"scenario_id": "GP08", "mode": "bogus"})).status_code == 422
    assert (await client.post("/api/strategy/runs", json={"scenario_id": "GP08", "fault_rate": 2})).status_code == 422
    assert (await client.post("/api/strategy/runs", json={})).status_code == 422
    assert (await client.post("/api/strategy/runs", json={"scenario": {"state": {}}})).status_code == 422


async def test_race_control_trigger_publishes_and_runs_live(api):
    client, svc, proc, chat = api
    chat.synth = [proposal("PIT_NOW_MEDIUM", "MEDIUM", 30, hold=5)]
    r = await client.post("/api/race-control/trigger",
                          json={"message": "FIA STEWARDS: 5 SECOND TIME PENALTY FOR CAR 63 (RUS) - CAUSING A COLLISION",
                                "lap_number": 30})
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["event"]["kind"] == "TIME_PENALTY" and proc.published[0]["driver_number"] == 63
    run = svc.runs.runs[body["run"]["run_id"]]
    assert run.scenario["state"]["car"]["penalty_s"] == 5.0          # penalised car is the one evaluated
    assert run.scenario["context"]["edition"] == "2023-i6"           # Singapore 2023 rules
    events = await sse(client, body["run"]["events_url"])
    assert events[-1]["data"]["data"]["status"] == "succeeded"
    final = (await client.get(f"/api/strategy/runs/{run.id}")).json()["result"]["final"]
    assert final["penalty_hold_s"] == 5.0
    # car not in the live tower
    r = await client.post("/api/race-control/trigger", json={"message": "VSC DEPLOYED", "driver_number": 99, "publish": False})
    assert r.status_code == 409
    # parse-only endpoint
    assert (await client.post("/api/race-control/parse", json={"message": "RED FLAG"})).json()["track_status"] == "RED"


async def test_live_stream_emits_tower_triggers_and_race_control(api):
    client, *_ = api
    got = []
    async with client.stream("GET", "/api/live/stream?interval_ms=200") as r:
        async for line in r.aiter_lines():
            if line.startswith("event: "):
                got.append(line[7:])
            if {"tower", "trigger", "race_control"} <= set(got) and got.count("tower") >= 2:
                break
    assert got.count("trigger") == 1 and got.count("race_control") == 1  # de-duplicated across polls


# ------------------------------------------------- Kafka integration
@pytest.mark.skipif(not port_open("127.0.0.1", 9092), reason="Kafka not running on :9092")
async def test_trigger_round_trips_through_kafka(binaries, tmp_path):
    """Real processor on Kafka: replay part of a race, publish a race-control
    message through the API path, and read it back from the processor after
    it has been consumed from f1.race_control."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen([str(binaries / "processor"), "-group", f"pytest-{time.time_ns()}", "-out", str(tmp_path),
                             "-http", f"127.0.0.1:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    client = ProcessorClient(f"http://127.0.0.1:{port}")
    try:
        # a new group starts at the end of the topic: replay only once it holds partitions
        assert await client.wait_ready(30), "processor was never assigned telemetry partitions"
        subprocess.run([str(binaries / "replayer"), "-file", str(ROOT / "data/races/session_9165_events.jsonl.gz"),
                        "-speed", "0", "-limit", "5000"], check=True, capture_output=True)
        for _ in range(100):
            t = await client.tower()
            if t["stats"]["events"] >= 5000:
                break
            await asyncio.sleep(0.1)
        assert t["stats"]["events"] >= 5000 and len(t["drivers"]) >= 15
        marker = f"PITWALL TEST {time.time_ns()} VIRTUAL SAFETY CAR DEPLOYED"
        echoed = await client.publish_race_control({"message": marker, "lap_number": 3})
        assert echoed["session_key"] == 9165
        for _ in range(100):
            if any(m["message"] == marker for m in await client.race_control()):
                break
            await asyncio.sleep(0.1)
        assert any(m["message"] == marker for m in await client.race_control())
        scn = live_scenario(t, t["drivers"][0]["driver_number"])
        assert scn["context"]["edition"] == "2023-i6" and scn["state"]["field"]
    finally:
        await client.aclose()
        proc.terminate()
        proc.wait(timeout=10)


@pytest.mark.skipif(not port_open("127.0.0.1", 9092), reason="Kafka not running on :9092")
async def test_sigterm_leaves_group_and_flushes(binaries, tmp_path):
    """`make stop` sends SIGTERM. The processor must leave its consumer group
    (so a replacement is assigned partitions immediately, not after the 45 s
    session timeout) and flush buffered Parquet batches on the way out."""
    import signal

    group = f"pytest-sigterm-{time.time_ns()}"

    def start(port: int, out) -> subprocess.Popen:
        return subprocess.Popen([str(binaries / "processor"), "-group", group, "-out", str(out), "-http", f"127.0.0.1:{port}",
                                 "-flush-rows", "1000000"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def free_port() -> int:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    async def wait_events(client: ProcessorClient, n: int, timeout: float) -> int:
        deadline = time.monotonic() + timeout
        got = 0
        while time.monotonic() < deadline:
            try:
                got = (await client.tower())["stats"]["events"]
            except httpx.HTTPError:
                got = 0
            if got >= n:
                break
            await asyncio.sleep(0.2)
        return got

    p1, port1 = start(port := free_port(), tmp_path / "a"), port
    c1 = ProcessorClient(f"http://127.0.0.1:{port1}")
    try:
        assert await c1.wait_ready(30), "first member was never assigned partitions"
        replay = [str(binaries / "replayer"), "-file", str(ROOT / "data/races/session_9165_events.jsonl.gz"), "-speed", "0"]
        subprocess.run([*replay, "-limit", "3000"], check=True, capture_output=True)
        assert await wait_events(c1, 3000, 20) >= 3000
        p1.send_signal(signal.SIGTERM)
        assert p1.wait(timeout=15) == 0
        # flushed on exit although the batch size (1M rows) was never reached
        assert list((tmp_path / "a").glob("telemetry/session=*/*.parquet"))
    finally:
        await c1.aclose()
        if p1.poll() is None:
            p1.kill()
    port2 = free_port()
    p2 = start(port2, tmp_path / "b")
    c2 = ProcessorClient(f"http://127.0.0.1:{port2}")
    try:
        t0 = time.monotonic()
        assert await c2.wait_ready(20), "replacement waited for the dead member's session timeout"
        subprocess.run([*replay, "-limit", "2000"], check=True, capture_output=True)
        assert await wait_events(c2, 2000, 20) >= 2000
        assert time.monotonic() - t0 < 20, "replacement waited for the dead member's session timeout"
    finally:
        await c2.aclose()
        p2.terminate()
        p2.wait(timeout=15)
