"""Stateful cyclic LangGraph pit-wall strategy agent.

    telemetry_olap -> fia_regulation_rag -> monte_carlo_mcp -> strategy_synthesizer -> verifier
                                                                      ^                  |
                                                                      +-- violations ----+  (<= 2 revisions)
                                                                                         v
                                                                                     finalize

qwen2.5-coder:14b writes the DuckDB SQL and the strategy call; llama3.2:3b is
the fast critic and the failover target. A deterministic FIA rule / traffic
guardrail decides whether the conditional back-edge fires.
"""
from __future__ import annotations

import asyncio
import json
import operator
import time
from dataclasses import dataclass
from typing import Annotated, Any, Callable, TypedDict

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph

from pitwall.agents import rules
from pitwall.regulations import check_citations
from pitwall.agents.llm import FailoverLLM
from pitwall.mcp_client.client import MCPError
from pitwall.olap import SCHEMA_DOC, AsOf, Lakehouse, UnsafeSQLError

MAX_REFLECTIONS = 2

# Every free-text field is bounded. Under deterministic sampling qwen2.5-coder
# can loop inside a string (a run of hashtags in the radio message) until the
# token budget ends, leaving unterminated JSON and forcing a failover; Ollama
# enforces maxLength/maxItems in the generation grammar.
PROPOSAL_SCHEMA = {
    "type": "object",
    "properties": {
        "recommended_action": {"type": "string", "enum": rules.ACTIONS},
        "target_compound": {"type": "string", "enum": ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET", "NONE"]},
        "pit_lap": {"type": "integer"},
        "expected_net_gain_ms": {"type": "number"},
        "rejoin_position": {"type": "integer"},
        "penalty_hold_s": {"type": "number"},
        "fia_article_citations": {"type": "array", "items": {"type": "string", "maxLength": 40}, "maxItems": 6},
        "engineer_radio_message": {"type": "string", "maxLength": 240},
        "rationale": {"type": "string", "maxLength": 600},
    },
    "required": ["recommended_action", "target_compound", "pit_lap", "expected_net_gain_ms", "rejoin_position",
                 "penalty_hold_s", "fia_article_citations", "engineer_radio_message", "rationale"],
}

CRITIC_SCHEMA = {
    "type": "object",
    "properties": {"ok": {"type": "boolean"},
                   "issues": {"type": "array", "items": {"type": "string", "maxLength": 240}, "maxItems": 6}},
    "required": ["ok", "issues"],
}

SQL_SCHEMA = {"type": "object", "properties": {"sql": {"type": "string", "maxLength": 1200}}, "required": ["sql"]}


class PitWallState(TypedDict, total=False):
    scenario: dict
    telemetry: dict
    constraints: dict
    regulations: list
    simulation: dict
    proposal: dict
    proposals: Annotated[list, operator.add]
    verdict: dict
    reflection_count: int
    final: dict
    trace: Annotated[list, operator.add]


@dataclass
class AgentDeps:
    llm: FailoverLLM
    mcp: Any  # HTTPMCPClient | StdioMCPClient
    retriever: Any | None = None  # HybridRetriever
    lakehouse: Lakehouse | None = None
    use_critic: bool = True
    on_event: Callable[[dict], None] | None = None


def _deps(config) -> AgentDeps:
    return config["configurable"]["deps"]


def _event(deps: AgentDeps, node: str, t0: float, summary: str, **detail) -> list[dict]:
    ev = {"node": node, "ms": round((time.perf_counter() - t0) * 1000, 1), "summary": summary, **detail}
    if deps.on_event:
        deps.on_event(ev)
    return [ev]


def _constraints(s: dict) -> rules.Constraints:
    return rules.constraints_from(s)


def situation_text(s: dict) -> str:
    c = s["car"]
    lines = [
        f"Lap {s['current_lap']}/{s['total_laps']} ({s['total_laps'] - s['current_lap']} laps after this one). Track status {s['track_status']}"
        + (f", track wetness {s['track_wetness']:.2f}" if s.get("track_wetness") else "")
        + (", PIT ENTRY CLOSED" if s.get("pit_entry_closed") else "") + ".",
        f"Our car: {c['driver_code']} (#{c.get('driver_number', '?')}) P{c['position']} on {c['compound']} aged {c['tyre_age_laps']} laps; "
        f"compounds used so far: {', '.join(c.get('compounds_used') or [c['compound']])}; "
        f"degradation {c.get('deg_slope_ms_per_lap') if c.get('deg_slope_ms_per_lap') is not None else 'unknown'} ms/lap; "
        f"pending time penalty: {c.get('penalty_s') or 0:.0f}s.",
    ]
    if s.get("race_control"):
        lines.append(f"Race control: {s['race_control']}")
    if s.get("team_radio"):
        lines.append(f"Team radio: {s['team_radio']}")
    near = sorted(s.get("field", []), key=lambda f: f["position"])
    rows = [f"P{f['position']} {f['driver_code']} gap_to_leader {f['gap_to_leader_ms'] / 1000:.1f}s {f['compound']} age {f['tyre_age_laps']}"
            for f in near if abs(f["position"] - c["position"]) <= 4]
    if rows:
        lines.append("Nearby cars: " + "; ".join(rows))
    return "\n".join(lines)


# ---------------------------------------------------------------- nodes
async def telemetry_olap_node(state: PitWallState, config) -> dict:
    deps, t0 = _deps(config), time.perf_counter()
    scn = state["scenario"]
    s = scn["state"]
    src = scn.get("source")
    out: dict[str, Any] = {"source": "scenario_state", "sql": None, "rows": [], "summary": []}
    if src and deps.lakehouse is not None:
        out["source"] = "lakehouse"
        ctx = scn["context"]
        # every query below sees only this session and only data that existed at the decision instant
        at = AsOf(session=ctx["session_key"], ts_ms=ctx["decision_ts_ms"], lap=ctx["lap"])
        out["as_of"] = {"session_key": at.session, "ts_ms": at.ts_ms, "completed_laps_before": at.lap}
        question = (f"Return lap_number, compound, tyre_age_laps, lap_time_ms, fuel_corrected_ms, deg_slope_ms_per_lap, "
                    f"position, interval_ahead_ms, is_clean for the last 8 completed laps of driver_number {src['driver_number']}, "
                    f"newest first.")
        messages = [
            {"role": "system", "content": "You write a single DuckDB SQL SELECT over these views. Reply as JSON {\"sql\": ...}.\n" + SCHEMA_DOC},
            {"role": "user", "content": question},
        ]
        err = None
        for attempt in range(2):
            if err:
                messages.append({"role": "user", "content": f"That query failed: {err}. Fix it and return only the JSON."})
            obj, rec = await deps.llm.complete_json("olap_sql", messages, SQL_SCHEMA, num_predict=300)
            sql = obj.get("sql", "")
            try:
                res = deps.lakehouse.safe_query(sql, max_rows=20, as_of=at)
                out.update(sql=sql, rows=res.records(), olap_ms=round(res.elapsed_ms, 2), sql_model=rec.model,
                           sql_attempts=attempt + 1)
                err = None
                break
            except (UnsafeSQLError, Exception) as e:  # noqa: BLE001 - feed DuckDB errors back to the model
                err = str(e).splitlines()[0][:300]
                messages.append({"role": "assistant", "content": json.dumps(obj)})
        if err:
            out["sql_error"] = err
        stint = deps.lakehouse.stint_summary(src["session_key"], src["driver_number"], before_lap=at.lap).records()
        out["stints"] = [{k: r[k] for k in ("compound", "first_lap", "last_lap", "clean_laps", "deg_ms_per_lap")} for r in stint]
        out["triggers"] = deps.lakehouse.recent_triggers(at, src["driver_number"], limit=5)
        clean = [r for r in out["rows"] if r.get("is_clean")]
        if clean:
            out["summary"].append(f"last clean laps: " + ", ".join(f"L{r['lap_number']} {r['lap_time_ms'] / 1000:.3f}s" for r in clean[:4]))
        for st in out["stints"]:
            out["summary"].append(f"stint {st['compound']} L{st['first_lap']}-{st['last_lap']} deg {st['deg_ms_per_lap']} ms/lap")
        for tr in out["triggers"][:3]:
            out["summary"].append(f"stream trigger L{tr['lap_number']} {tr['kind']}: {tr['detail']}")
    summary = (f"DuckDB: {len(out['rows'])} rows via model SQL ({out.get('olap_ms', 0)} ms)" if out["source"] == "lakehouse"
               else "no lakehouse session: using the scenario state")
    return {"telemetry": out, "trace": _event(deps, "telemetry_olap", t0, summary, sql=out.get("sql"),
                                                sql_error=out.get("sql_error"))}


def _rag_queries(s: dict, c: rules.Constraints) -> list[str]:
    q = []
    if c.pit_entry_closed:
        q.append("pit entry closed during the race, can a car enter the pit lane")
    if c.track_status == "RED":
        q.append("race suspended red flag: work permitted on cars, changing wheels and tyres")
    elif c.track_status == "VSC":
        q.append("entering the pits for tyres while the VSC procedure is in use")
    elif c.track_status == "SC":
        q.append("entering the pits to change tyres while the safety car is deployed")
    if c.penalty_s:
        q.append(f"serving a {c.penalty_s:.0f} second time penalty at a pit stop, the car may not be worked on until stationary")
    if c.needs_second_dry or c.wet_used or c.track_wetness > 0.3:
        q.append(f"Article {c.art('two_compound')} at least two different specifications of dry-weather tyres unless intermediate or wet used")
    if not q:
        q.append(s.get("race_control") or "pit stop strategy tyre change during the race")
    return q[:3]


async def fia_regulation_rag_node(state: PitWallState, config) -> dict:
    deps, t0 = _deps(config), time.perf_counter()
    s = state["scenario"]["state"]
    c = _constraints(s)
    regs: list[dict] = []
    timings = []
    if deps.retriever is not None:
        from pitwall.rag.store import HybridRetriever

        ctx = state["scenario"]["context"]
        # only what the pit wall could know: the regulation issue in force, earlier
        # precedents, and this race's notices from laps before the decision lap
        qfilter = HybridRetriever.as_of_filter(ctx["edition"], before_ts=ctx["context_before_ts"],
                                               session_key=ctx.get("session_key"), lap=ctx.get("lap"))
        seen = set()
        for q in _rag_queries(s, c):
            # the Qdrant client, embedding call and cross-encoder are blocking:
            # keep them off the event loop so SSE streams stay live
            res = await asyncio.to_thread(deps.retriever.search, q, mode="hybrid_rerank", k=3, qfilter=qfilter)
            timings.append(round(res.total_ms, 1))
            for h in res.hits:
                if h.id in seen:
                    continue
                seen.add(h.id)
                regs.append({"id": h.id, "citation": h.citation, "query": q, "rrf_rank": h.rank,
                             "rerank_score": round(h.rerank_score or 0, 3), "text": h.payload["text"][:600]})
    constraints = {"lines": c.describe(), "needs_second_dry": c.needs_second_dry, "penalty_s": c.penalty_s,
                   "pit_entry_closed": c.pit_entry_closed, "track_status": c.track_status}
    return {"regulations": regs, "constraints": constraints,
            "trace": _event(deps, "fia_regulation_rag", t0, f"{len(regs)} citations from Qdrant hybrid RRF + rerank",
                            citations=[r["citation"] for r in regs], queries_ms=timings, constraints=constraints["lines"],
                            hits=[{k: r[k] for k in ("id", "citation", "query", "rrf_rank", "rerank_score", "text")} for r in regs])}


def pit_loss_for_lap(s: dict, pit_lap: int) -> float:
    """Pit loss for a stop on ``pit_lap``: neutralised only if the stop is this lap under SC/VSC."""
    green, neutral = s.get("pit_loss_green_s", 21.8), s.get("pit_loss_neutral_s", 13.2)
    return neutral if pit_lap == s["current_lap"] and s.get("track_status") in ("SC", "VSC") else green


def sim_request(s: dict, strategies: list[str] | None = None, rollouts: int = 10_000) -> dict:
    car = {k: v for k, v in s["car"].items() if v is not None}
    req = {"total_laps": s["total_laps"], "current_lap": s["current_lap"], "track_status": s.get("track_status", "GREEN"),
           "track_wetness": s.get("track_wetness", 0.0), "wetness_trend_per_lap": s.get("wetness_trend_per_lap", 0.0),
           "pit_entry_closed": bool(s.get("pit_entry_closed")), "car": car, "field": s.get("field", []),
           "rollouts": rollouts, "seed": 42}
    for k in ("overtake_difficulty", "extend_laps", "forbid_compounds", "pit_loss_green_s", "pit_loss_neutral_s"):
        if k in s:
            req[k] = s[k]
    if strategies:
        req["strategies"] = strategies
    return req


async def monte_carlo_mcp_node(state: PitWallState, config) -> dict:
    deps, t0 = _deps(config), time.perf_counter()
    s = state["scenario"]["state"]
    sim = await deps.mcp.call_tool("simulate_pit_strategies", sim_request(s))
    table = [{"strategy": o["strategy"], "pit_lap": o["pit_lap"], "compound": o["target_compound"], "legal": o["legal"],
              "violations": o["violations"], "expected_delta_s": round(o["expected_delta_ms"] / 1000, 2),
              "p10_p90_s": [round(o["p10_delta_ms"] / 1000, 2), round(o["p90_delta_ms"] / 1000, 2)],
              "expected_finish": round(o["expected_finish_position"], 2), "rejoin_position": round(o["mean_rejoin_position"], 1),
              "p_ahead_of_car_in_front": round(o["p_finish_ahead_of_car_ahead"], 2), "notes": o.get("notes", "")}
             for o in sim["outcomes"]]
    out: dict[str, Any] = {"recommended": sim["recommended"], "table": table, "elapsed_ms": sim["elapsed_ms"],
                           "rollouts": sim["rollouts_per_strategy"], "total_trajectories": sim["total_trajectories"],
                           "workers": sim["workers"], "assumptions": sim["assumptions"]}
    car = s["car"]
    ahead = next((f for f in s.get("field", []) if f["position"] == car["position"] - 1), None)
    if ahead and abs(ahead["gap_to_leader_ms"] - car.get("gap_to_leader_ms", 0)) < 3500 and s.get("track_status") == "GREEN":
        defender = {"compound": ahead["compound"], "tyre_age_laps": ahead["tyre_age_laps"], "base_lap_ms": car.get("base_lap_ms", 95000)}
        if ahead.get("deg_slope_ms_per_lap") is not None:
            defender["deg_slope_ms_per_lap"] = ahead["deg_slope_ms_per_lap"]
        try:
            out["undercut"] = await deps.mcp.call_tool("compute_undercut_delta", {
                "gap_ms": max(0, car.get("gap_to_leader_ms", 0) - ahead["gap_to_leader_ms"]),  # attacker behind defender
                "attacker": {k: v for k, v in car.items() if v is not None}, "defender": defender,
                "attacker_new_compound": "HARD" if car["compound"] != "HARD" else "MEDIUM",
                "pit_loss_s": s.get("pit_loss_green_s", 21.8), "seed": 1})
        except MCPError as e:
            out["undercut_error"] = str(e)
    rec = next(o for o in sim["outcomes"] if o["strategy"] == sim["recommended"])
    # under a red flag every car may change tyres in the pit lane: no rejoin traffic to check
    if rec["pit_lap"] and s.get("field") and s.get("track_status") != "RED":
        out["traffic"] = await deps.mcp.call_tool("validate_physics_and_traffic", {
            "current_lap": s["current_lap"], "pit_lap": rec["pit_lap"], "penalty_s": car.get("penalty_s", 0),
            "pit_loss_s": pit_loss_for_lap(s, rec["pit_lap"]),
            "car": {k: v for k, v in car.items() if v is not None}, "field": s["field"]})
    return {"simulation": out, "trace": _event(
        deps, "monte_carlo_mcp", t0,
        f"Go MCP: {sim['total_trajectories']:,} trajectories in {sim['elapsed_ms']:.1f} ms on {sim['workers']} workers, best legal {sim['recommended']}",
        table=table, rollouts=sim["rollouts_per_strategy"], recommended=sim["recommended"],
        undercut=out.get("undercut"), traffic=out.get("traffic"))}


_RULES = """Rules you must respect (article numbers are those of the regulations in force for this race):
- recommended_action is exactly one of: PIT_NOW_HARD, PIT_NOW_MEDIUM, PIT_NOW_SOFT, PIT_NOW_INTER, STAY_OUT_EXTEND, OPPOSITE_AHEAD.
- PIT_NOW_* means pit at the end of the current lap: pit_lap = current lap and target_compound = that compound.
- STAY_OUT_EXTEND / OPPOSITE_AHEAD: pit_lap is the later lap you will stop (0 if no further stop) and target_compound the tyre then (NONE if no stop).
- Article {two_compound}: in a dry race (no intermediates/wets used) a car must use two DIFFERENT dry compounds before the flag.
- Article {penalty_hold}: a pending 5s/10s time penalty is served as a stationary hold at the next stop. If the plan stops, penalty_hold_s MUST equal the pending penalty, otherwise 0.
- Article {pit_entry_closed}: no stop while the pit entry is closed.
"""

SYSTEM_STRATEGIST = """You are the race strategist on a Formula 1 pit wall. Decide the pit call for our car.
""" + _RULES + """- Prefer the simulator's best LEGAL option unless you have a concrete reason grounded in the regulations or race situation.
- expected_net_gain_ms: race time gained versus the next-best legal option (from the simulator table).
- fia_article_citations: the specific articles you relied on, e.g. "Art. {two_compound}".
- engineer_radio_message: one or two short sentences the race engineer says to the driver.
Reply with JSON only."""

# The baseline gets the same rules and output contract as the agent, so the comparison
# isolates what the graph adds (lakehouse SQL, retrieval, simulator, verifier, cycles).
SYSTEM_SINGLE = """You are the race strategist on a Formula 1 pit wall. Decide the pit call for our car in one shot.
""" + _RULES + """- expected_net_gain_ms: your estimate of race time gained versus the next-best legal option.
- fia_article_citations: the specific articles you relied on, e.g. "Art. {two_compound}".
- engineer_radio_message: one or two short sentences the race engineer says to the driver.
Reply with JSON only."""


def strategist_prompt(c: rules.Constraints, template: str = SYSTEM_STRATEGIST) -> str:
    return template.format(two_compound=c.art("two_compound"), penalty_hold=c.art("penalty_hold"),
                           pit_entry_closed=c.art("pit_entry_closed"))


def _synth_prompt(state: PitWallState) -> list[dict]:
    s = state["scenario"]["state"]
    parts = [f"SITUATION\n{situation_text(s)}", f"QUESTION\n{state['scenario'].get('question', '')}"]
    tel = state.get("telemetry", {})
    if tel.get("summary"):
        parts.append("TELEMETRY (DuckDB over the Parquet lakehouse)\n" + "\n".join(tel["summary"]))
    parts.append("REGULATORY CONSTRAINTS\n" + "\n".join(state.get("constraints", {}).get("lines", [])))
    regs = state.get("regulations") or []
    if regs:
        parts.append("RETRIEVED FIA TEXT (Qdrant hybrid search)\n" + "\n\n".join(f"[{r['citation']}] {r['text'][:420]}" for r in regs[:5]))
    sim = state.get("simulation", {})
    if sim:
        ordered = sorted(sim["table"], key=lambda o: (not o["legal"], o["expected_delta_s"]))
        rows = "\n".join(
            f"{'BEST LEGAL -> ' if o['strategy'] == sim['recommended'] else ''}{o['strategy']}: pit_lap={o['pit_lap']} compound={o['compound']} legal={o['legal']} "
            f"expected_delta={o['expected_delta_s']}s p10/p90={o['p10_p90_s']} expected_finish=P{o['expected_finish']} "
            f"rejoin=P{o['rejoin_position']} {('VIOLATIONS: ' + '; '.join(o['violations'])) if o['violations'] else ''} {o['notes']}"
            for o in ordered)
        parts.append(f"MONTE CARLO ({sim['rollouts']:,} rollouts per strategy; delta vs best legal, lower is better)\n{rows}")
        if sim.get("undercut"):
            parts.append("UNDERCUT CHECK\n" + sim["undercut"]["explanation"])
        if sim.get("traffic"):
            parts.append("TRAFFIC CHECK for the simulator's best plan\n" + sim["traffic"]["verdict"])
    if state.get("verdict") and state["verdict"].get("feedback"):
        parts.append("YOUR PREVIOUS PROPOSAL WAS REJECTED BY THE VERIFIER. Fix every issue:\n"
                     + json.dumps(state.get("proposal", {}), indent=None)[:900] + "\n" + state["verdict"]["feedback"])
    return [{"role": "system", "content": strategist_prompt(_constraints(s))}, {"role": "user", "content": "\n\n".join(parts)}]


def _normalise(p: dict, s: dict) -> dict:
    p = dict(p)
    p["recommended_action"] = (p.get("recommended_action") or "").upper()
    p["target_compound"] = rules.canonical(p.get("target_compound")) or "NONE"
    for k in ("pit_lap", "rejoin_position"):
        try:
            p[k] = int(p.get(k) or 0)
        except (TypeError, ValueError):
            p[k] = 0
    try:
        p["penalty_hold_s"] = float(p.get("penalty_hold_s") or 0)
    except (TypeError, ValueError):
        p["penalty_hold_s"] = 0.0
    p.setdefault("fia_article_citations", [])
    return p


async def strategy_synthesizer_node(state: PitWallState, config) -> dict:
    deps, t0 = _deps(config), time.perf_counter()
    s = state["scenario"]["state"]
    obj, rec = await deps.llm.complete_json("synthesizer", _synth_prompt(state), PROPOSAL_SCHEMA, num_predict=700)
    p = _normalise(obj, s)
    p["_model"], p["_failover"], p["_ms"] = rec.model, rec.failover, round(rec.ms)
    cycle = state.get("reflection_count", 0)
    return {"proposal": p, "proposals": [{"cycle": cycle, **p}],
            "trace": _event(deps, "strategy_synthesizer", t0,
                            f"{'revision ' + str(cycle) + ': ' if cycle else ''}{p['recommended_action']} -> {p['target_compound']} "
                            f"(lap {p['pit_lap']}) by {rec.model}{' [FAILOVER]' if rec.failover else ''}",
                            proposal={k: v for k, v in p.items() if not k.startswith('_')}, model=rec.model, failover=rec.failover)}


CRITIC_PROMPT = """You are a strict FIA regulations and race-strategy reviewer. Check the proposed pit call against the constraints.
Flag only concrete problems: an illegal tyre plan (Art. {two_compound}), a missing or wrong penalty hold (Art. {penalty_hold}),
pitting while the pit entry is closed (Art. {pit_entry_closed}), slicks on a wet track or inters on a dry one, or ignoring a traffic warning.
Return JSON {{"ok": true|false, "issues": [short strings]}}. If nothing is wrong return ok=true and an empty list."""


async def verifier_node(state: PitWallState, config) -> dict:
    deps, t0 = _deps(config), time.perf_counter()
    s = state["scenario"]["state"]
    c = _constraints(s)
    p = state["proposal"]
    traffic = await traffic_for(deps, s, c, p)
    found = rules.check(p, c, traffic)
    hard = [v for v in found if v.severity == "hard"]
    soft = [v for v in found if v.severity != "hard"]
    critic: dict = {"ok": True, "issues": [], "model": None}
    if deps.use_critic:
        msg = (f"CONSTRAINTS\n" + "\n".join(c.describe()) + f"\nTRAFFIC: {traffic['verdict'] if traffic else 'n/a'}"
               f"\n\nPROPOSAL\n{json.dumps({k: v for k, v in p.items() if not k.startswith('_')})}")
        try:
            critic_prompt = CRITIC_PROMPT.format(two_compound=c.art("two_compound"), penalty_hold=c.art("penalty_hold"),
                                                 pit_entry_closed=c.art("pit_entry_closed"))
            obj, rec = await deps.llm.complete_json("critic", [{"role": "system", "content": critic_prompt},
                                                               {"role": "user", "content": msg}],
                                                    CRITIC_SCHEMA, model=deps.llm.fallback, inject=False, num_predict=200)
            critic = {"ok": bool(obj.get("ok", True)), "issues": [str(x) for x in obj.get("issues", [])][:4], "model": rec.model,
                      "ms": round(rec.ms)}
        except Exception as e:  # noqa: BLE001 - the deterministic guardrail still decides
            critic = {"ok": True, "issues": [], "error": str(e)}
    cycle = state.get("reflection_count", 0)
    revise = bool(hard) and cycle < MAX_REFLECTIONS
    fb = rules.feedback(hard)
    if soft:
        fb += ("\n" if fb else "") + "Advisory: " + rules.feedback(soft)
    if critic["issues"] and not critic["ok"]:
        fb += ("\n" if fb else "") + "Reviewer notes (advisory): " + "; ".join(critic["issues"])
    # advisory: cited articles must exist in the edition in force (existence only)
    cites = check_citations(c.edition, p.get("fia_article_citations") or [])
    if cites["invalid"]:
        fb += ("\n" if fb else "") + (f"Advisory: these citations are not articles of the {c.edition} regulations: "
                                       + ", ".join(cites["invalid"]))
    verdict = {"ok": not hard, "violations": [v.as_dict() for v in hard], "advisories": [v.as_dict() for v in soft],
               "critic": critic, "feedback": fb, "citations": cites,
               "traffic": traffic.get("verdict") if traffic else None, "revise": revise}
    update: dict[str, Any] = {"verdict": verdict}
    if revise:
        update["reflection_count"] = cycle + 1
    summary = ("PASS" if not hard else f"{len(hard)} violation(s): " + ", ".join(v.code for v in hard)) + \
              (f" (advisory: {', '.join(v.code for v in soft)})" if soft else "") + \
              (f"; critic ({critic.get('model')}): {'ok' if critic['ok'] else '; '.join(critic['issues'])}" if critic.get("model") else "")
    update["trace"] = _event(deps, "verifier", t0, summary + (" -> REVISE" if revise else ""), violations=verdict["violations"],
                             critic=critic, cycle=cycle)
    return update


def route_after_verifier(state: PitWallState) -> str:
    return "strategy_synthesizer" if state.get("verdict", {}).get("revise") else "finalize"


async def traffic_for(deps: AgentDeps, s: dict, c: rules.Constraints, proposal: dict) -> dict | None:
    """Rejoin traffic for a proposal's stop (none for no stop or a red-flag change)."""
    pit_lap, comp = rules.plan_of(proposal, c)
    if not pit_lap or not s.get("field") or c.track_status == "RED":
        return None
    try:
        return await deps.mcp.call_tool("validate_physics_and_traffic", {
            "current_lap": c.current_lap, "pit_lap": pit_lap, "penalty_s": c.penalty_s, "new_compound": comp,
            "pit_loss_s": pit_loss_for_lap(s, pit_lap),
            "car": {k: v for k, v in s["car"].items() if v is not None}, "field": s["field"]})
    except MCPError:
        return None


async def finalize_node(state: PitWallState, config) -> dict:
    deps, t0 = _deps(config), time.perf_counter()
    s = state["scenario"]["state"]
    c = _constraints(s)
    p = dict(state["proposal"])
    verdict = state.get("verdict", {})
    override, unresolved = False, []
    if not verdict.get("ok", True):
        # Still violating after the reflection budget: take the best simulator
        # plan (by expected race time) that passes every hard check. If none
        # does, keep the proposal and report the violations as unresolved.
        sim = state.get("simulation", {})
        candidates = sorted((o for o in sim.get("table", []) if o["legal"]), key=lambda o: o["expected_delta_s"])
        chosen = None
        for o in candidates:
            cand = {**p, "recommended_action": o["strategy"], "target_compound": o["compound"] if o["pit_lap"] else "NONE",
                    "pit_lap": o["pit_lap"], "penalty_hold_s": c.penalty_s if o["pit_lap"] else 0.0}
            hard = [v for v in rules.check(cand, c, await traffic_for(deps, s, c, cand)) if v.severity == "hard"]
            if not hard:
                chosen = cand
                break
        if chosen is not None:
            changed = (chosen["recommended_action"], chosen["pit_lap"], chosen["target_compound"]) != \
                      (p.get("recommended_action"), p.get("pit_lap"), p.get("target_compound"))
            p = chosen
            if changed:
                override = True
                p["rationale"] = (p.get("rationale", "") + " [guardrail override: best simulator plan that passes every check]").strip()
        else:
            unresolved = verdict.get("violations", [])
    final = {k: v for k, v in p.items() if not k.startswith("_")}
    final["guardrail_override"] = override
    final["unresolved_violations"] = unresolved
    final["reflections"] = state.get("reflection_count", 0)
    tail = " (guardrail override)" if override else (f" (unresolved: {', '.join(v['code'] for v in unresolved)})" if unresolved else "")
    return {"final": final, "trace": _event(deps, "finalize", t0,
                                            f"{final['recommended_action']} {final['target_compound']} lap {final['pit_lap']}" + tail,
                                            final=final)}


def build_graph(checkpointer=None):
    g = StateGraph(PitWallState)
    g.add_node("telemetry_olap", telemetry_olap_node)
    g.add_node("fia_regulation_rag", fia_regulation_rag_node)
    g.add_node("monte_carlo_mcp", monte_carlo_mcp_node)
    g.add_node("strategy_synthesizer", strategy_synthesizer_node)
    g.add_node("verifier", verifier_node)
    g.add_node("finalize", finalize_node)
    g.add_edge(START, "telemetry_olap")
    g.add_edge("telemetry_olap", "fia_regulation_rag")
    g.add_edge("fia_regulation_rag", "monte_carlo_mcp")
    g.add_edge("monte_carlo_mcp", "strategy_synthesizer")
    g.add_edge("strategy_synthesizer", "verifier")
    g.add_conditional_edges("verifier", route_after_verifier, {"strategy_synthesizer": "strategy_synthesizer", "finalize": "finalize"})
    g.add_edge("finalize", END)
    return g.compile(checkpointer=checkpointer if checkpointer is not None else MemorySaver())


async def run_agent(scenario: dict, deps: AgentDeps, graph=None, thread_id: str | None = None) -> dict:
    graph = graph or build_graph()
    cfg = {"configurable": {"thread_id": thread_id or f"{scenario['id']}-{time.time_ns()}", "deps": deps},
           "recursion_limit": 25}
    t0 = time.perf_counter()
    final_state = await graph.ainvoke({"scenario": scenario, "reflection_count": 0, "trace": [], "proposals": []}, cfg)
    final_state["elapsed_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return final_state


async def run_single_pass(scenario: dict, deps: AgentDeps) -> dict:
    """Baseline: one primary-model call with the same situation and rules, no RAG, no simulator, no verifier."""
    t0 = time.perf_counter()
    s = scenario["state"]
    msgs = [{"role": "system", "content": strategist_prompt(_constraints(s), SYSTEM_SINGLE)},
            {"role": "user", "content": f"SITUATION\n{situation_text(s)}\n\nQUESTION\n{scenario.get('question', '')}"}]
    obj, rec = await deps.llm.complete_json("single_pass", msgs, PROPOSAL_SCHEMA, num_predict=600)
    p = _normalise(obj, s)
    return {"final": {k: v for k, v in p.items() if not k.startswith("_")}, "model": rec.model, "failover": rec.failover,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1)}
