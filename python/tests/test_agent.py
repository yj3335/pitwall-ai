"""LangGraph agent: rule guardrail, cyclic self-correction, failover, MCP clients."""
from __future__ import annotations

import json

import pytest

from conftest import requires_ollama, requires_qdrant
from pitwall.agents import rules
from pitwall.agents.graph import AgentDeps, build_graph, run_agent, run_single_pass
from pitwall.agents.llm import FailoverLLM, FaultInjector
from pitwall.eval.scenarios import load_scenarios, materialise
from pitwall.mcp_client.client import HTTPMCPClient, StdioMCPClient

SCN = {s["id"]: s for s in load_scenarios()}


def proposal(action, compound, pit_lap, hold=0.0):
    return {"recommended_action": action, "target_compound": compound, "pit_lap": pit_lap, "expected_net_gain_ms": 0,
            "rejoin_position": 5, "penalty_hold_s": hold, "fia_article_citations": ["Art. 30.5(m)"],
            "engineer_radio_message": "Box box.", "rationale": "test"}


# ---------------------------------------------------------------- guardrail
def test_rules_two_compound_and_penalty_and_pit_entry():
    c = rules.Constraints(current_lap=32, total_laps=53, compound="MEDIUM", compounds_used=["MEDIUM"])
    assert [v.code for v in rules.check(proposal("PIT_NOW_MEDIUM", "MEDIUM", 32), c)] == ["TWO_COMPOUND"]
    assert rules.check(proposal("PIT_NOW_HARD", "HARD", 32), c) == []
    assert "TWO_COMPOUND" in [v.code for v in rules.check(proposal("STAY_OUT_EXTEND", "NONE", 0), c)]
    c.penalty_s = 5
    codes = [v.code for v in rules.check(proposal("PIT_NOW_HARD", "HARD", 32, hold=0), c)]
    assert codes == ["PENALTY_HOLD"]
    assert rules.check(proposal("PIT_NOW_HARD", "HARD", 32, hold=5), c) == []
    c.penalty_s, c.pit_entry_closed = 0, True
    assert [v.code for v in rules.check(proposal("PIT_NOW_HARD", "HARD", 32), c)] == ["PIT_ENTRY_CLOSED"]
    assert rules.check(proposal("STAY_OUT_EXTEND", "HARD", 34), c) == []


def test_rules_wet_track_physics_and_exemption():
    c = rules.Constraints(current_lap=15, total_laps=52, compound="SOFT", compounds_used=["SOFT"], track_wetness=0.6)
    assert "SLICKS_ON_WET" in [v.code for v in rules.check(proposal("PIT_NOW_MEDIUM", "MEDIUM", 15), c)]
    assert rules.check(proposal("PIT_NOW_INTER", "INTERMEDIATE", 15), c) == []  # inters also waive 30.5(m)
    dry = rules.Constraints(current_lap=40, total_laps=52, compound="INTERMEDIATE", compounds_used=["INTERMEDIATE"], track_wetness=0.05)
    assert "INTERS_ON_DRY" in [v.code for v in rules.check(proposal("PIT_NOW_INTER", "INTERMEDIATE", 40), dry)]


# ---------------------------------------------------------------- fake LLM
class ScriptedChat:
    """Returns queued JSON replies per role, recording which model was asked."""

    def __init__(self, synth: list[dict], fail_models: set[str] = frozenset()):
        self.synth = list(synth)
        self.fail_models = set(fail_models)
        self.calls: list[tuple[str, str]] = []

    async def chat(self, model, messages, schema=None, **kw):
        sys_prompt = messages[0]["content"]
        role = "critic" if "reviewer" in sys_prompt else "sql" if "DuckDB" in sys_prompt else "synth"
        self.calls.append((model, role))
        if model in self.fail_models:
            import httpx
            raise httpx.ConnectError("model down")
        if role == "critic":
            return json.dumps({"ok": True, "issues": []}), {}
        if role == "sql":
            return json.dumps({"sql": "SELECT lap_number FROM laps LIMIT 1"}), {}
        p = self.synth.pop(0) if len(self.synth) > 1 else self.synth[0]
        return json.dumps(p), {}


@pytest.fixture
async def stdio_mcp(binaries):
    c = StdioMCPClient(binaries / "mcp-sim")
    await c.start()
    await c.initialize()
    yield c
    await c.aclose()


async def test_cyclic_self_correction_on_two_compound_trap(stdio_mcp):
    scn = materialise(SCN["GP08"])
    chat = ScriptedChat([proposal("PIT_NOW_MEDIUM", "MEDIUM", 32), proposal("PIT_NOW_HARD", "HARD", 32)])
    deps = AgentDeps(llm=FailoverLLM(chat=chat), mcp=stdio_mcp)
    graph = build_graph()
    st = await run_agent(scn, deps, graph=graph, thread_id="t-gp08")
    nodes = [e["node"] for e in st["trace"]]
    assert nodes == ["telemetry_olap", "fia_regulation_rag", "monte_carlo_mcp", "strategy_synthesizer", "verifier",
                     "strategy_synthesizer", "verifier", "finalize"]
    first_verdict = [e for e in st["trace"] if e["node"] == "verifier"][0]
    assert first_verdict["violations"][0]["code"] == "TWO_COMPOUND"
    assert st["final"]["recommended_action"] == "PIT_NOW_HARD" and st["final"]["reflections"] == 1
    assert not st["final"]["guardrail_override"]
    assert [p["recommended_action"] for p in st["proposals"]] == ["PIT_NOW_MEDIUM", "PIT_NOW_HARD"]
    assert st["simulation"]["rollouts"] == 10_000
    # MemorySaver checkpointed every super-step of the thread
    snap = graph.get_state({"configurable": {"thread_id": "t-gp08"}})
    assert snap.values["final"]["recommended_action"] == "PIT_NOW_HARD"
    assert len(list(graph.get_state_history({"configurable": {"thread_id": "t-gp08"}}))) >= 8


async def test_reflection_budget_then_guardrail_override(stdio_mcp):
    scn = materialise(SCN["GP13"])  # pit entry closed under SC
    chat = ScriptedChat([proposal("PIT_NOW_HARD", "HARD", 25)])  # keeps proposing the illegal stop
    deps = AgentDeps(llm=FailoverLLM(chat=chat), mcp=stdio_mcp)
    st = await run_agent(scn, deps)
    assert sum(e["node"] == "strategy_synthesizer" for e in st["trace"]) == 3  # 1 + 2 revisions
    assert st["final"]["guardrail_override"] is True
    assert not st["final"]["recommended_action"].startswith("PIT_NOW")
    c = rules.constraints_from(scn["state"])
    assert rules.check(st["final"], c) == []


async def test_penalty_hold_is_enforced(stdio_mcp):
    scn = materialise(SCN["GP11"])  # 10 s penalty, hard-only car
    chat = ScriptedChat([proposal("PIT_NOW_MEDIUM", "MEDIUM", 35, hold=0), proposal("PIT_NOW_MEDIUM", "MEDIUM", 35, hold=10)])
    st = await run_agent(scn, AgentDeps(llm=FailoverLLM(chat=chat), mcp=stdio_mcp))
    v = [e for e in st["trace"] if e["node"] == "verifier"][0]["violations"]
    assert [x["code"] for x in v] == ["PENALTY_HOLD"]
    assert st["final"]["penalty_hold_s"] == 10


async def test_failover_and_circuit_breaker(stdio_mcp):
    scn = materialise(SCN["GP12"])
    chat = ScriptedChat([proposal("PIT_NOW_HARD", "HARD", 30)])
    llm = FailoverLLM(chat=chat, faults=FaultInjector(rate=1.0, seed=1), trip_after=2, cooldown_s=60)
    st = await run_agent(scn, AgentDeps(llm=llm, mcp=stdio_mcp, use_critic=False))
    assert st["final"]["recommended_action"] == "PIT_NOW_HARD"
    synth = [e for e in st["trace"] if e["node"] == "strategy_synthesizer"][0]
    assert synth["failover"] and synth["model"] == llm.fallback
    assert llm.stats.faults == 1 and llm.stats.recovered == 1
    # a hard outage on the primary trips the breaker; later calls skip it entirely
    chat2 = ScriptedChat([proposal("PIT_NOW_HARD", "HARD", 30)], fail_models={"qwen2.5-coder:14b"})
    llm2 = FailoverLLM(chat=chat2, trip_after=2, cooldown_s=60)
    for _ in range(4):
        await llm2.complete_json("synthesizer", [{"role": "system", "content": "x"}, {"role": "user", "content": "y"}], {})
    assert llm2.breaker_open
    primary_attempts = [m for m, _ in chat2.calls if m == "qwen2.5-coder:14b"]
    assert len(primary_attempts) == 2  # breaker opened after two failures
    assert llm2.stats.recovered == 2 and len(llm2.stats.calls) == 4


async def test_http_mcp_client(mcp_server):
    c = HTTPMCPClient(mcp_server)
    info = await c.initialize()
    assert info["protocolVersion"] == "2025-06-18"
    assert {t["name"] for t in c.tools} >= {"simulate_pit_strategies", "compute_undercut_delta"}
    scn = materialise(SCN["GP17"])
    from pitwall.agents.graph import sim_request
    res = await c.call_tool("simulate_pit_strategies", sim_request(scn["state"]))
    assert res["rollouts_per_strategy"] == 10_000 and res["recommended"]
    await c.aclose()


@requires_ollama
@requires_qdrant
async def test_real_models_end_to_end(mcp_server):
    """Full graph on qwen2.5-coder:14b + llama3.2:3b + bge-m3/Qdrant + Go MCP."""
    from pitwall.olap import Lakehouse
    from pitwall.rag.store import HybridRetriever

    mcp = HTTPMCPClient(mcp_server)
    await mcp.initialize()
    lh = Lakehouse()
    scn = materialise(SCN["GP12"], lh)
    deps = AgentDeps(llm=FailoverLLM(), mcp=mcp, retriever=HybridRetriever(reranker_model="Xenova/ms-marco-MiniLM-L-6-v2"), lakehouse=lh)
    st = await run_agent(scn, deps)
    assert rules.check(st["final"], rules.constraints_from(scn["state"])) == []
    assert any(r["citation"] for r in st["regulations"])
    base = await run_single_pass(scn, deps)
    assert base["final"]["recommended_action"] in rules.ACTIONS
    await mcp.aclose()


def test_generated_strings_are_bounded():
    """Every string a model writes has a maxLength (and every array a maxItems),
    so a repetition loop inside a field cannot run to the token limit."""
    from pitwall.agents.graph import CRITIC_SCHEMA, PROPOSAL_SCHEMA, SQL_SCHEMA

    def walk(node, path):
        if node.get("type") == "string" and "enum" not in node:
            assert node.get("maxLength"), f"{path} is unbounded"
        if node.get("type") == "array":
            assert node.get("maxItems"), f"{path} is unbounded"
            walk(node["items"], path + "[]")
        for k, v in node.get("properties", {}).items():
            walk(v, f"{path}.{k}")

    for name, schema in {"proposal": PROPOSAL_SCHEMA, "critic": CRITIC_SCHEMA, "sql": SQL_SCHEMA}.items():
        walk(schema, name)


@requires_ollama
@pytest.mark.parametrize("model", ["qwen2.5-coder:14b", "llama3.2:3b"])
async def test_ollama_accepts_every_output_schema(model):
    """Ollama compiles each schema into a sampling grammar and rejects ones it
    cannot build with HTTP 400 (a 2,000-character maxLength on the SQL field
    did; 1,500 does not), which failed every real-race agent run."""
    import httpx
    from pitwall import config
    from pitwall.agents.graph import CRITIC_SCHEMA, PROPOSAL_SCHEMA, SQL_SCHEMA

    async with httpx.AsyncClient(timeout=120) as http:
        for schema in (PROPOSAL_SCHEMA, CRITIC_SCHEMA, SQL_SCHEMA):
            r = await http.post(f"{config.OLLAMA_URL}/api/chat", json={
                "model": model, "stream": False, "format": schema, "options": {"num_predict": 8},
                "messages": [{"role": "user", "content": "Reply with JSON."}]})
            assert r.status_code == 200, (sorted(schema["properties"]), r.text[:200])


@requires_ollama
async def test_single_pass_json_completes_on_the_primary_model():
    """GP23 single pass used to loop on hashtags in engineer_radio_message until
    the 600-token budget, leaving truncated JSON and a failover after ~24 s."""
    deps = AgentDeps(llm=FailoverLLM(), mcp=None, retriever=None, lakehouse=None)
    out = await run_single_pass(materialise(SCN["GP23"]), deps)
    assert not out["failover"] and out["model"] == "qwen2.5-coder:14b", out
    assert out["elapsed_ms"] < 20_000
