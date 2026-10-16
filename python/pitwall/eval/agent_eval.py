"""Benchmark 3: single-pass LLM vs the cyclic LangGraph agent on 24 scenarios,
with a simulator-only reference arm.

    python -m pitwall.eval.agent_eval --out ../benchmarks/agent_eval.json

Both arms run on the real local models (qwen2.5-coder:14b primary,
llama3.2:3b fast model) with the same seeded 10% fault injection on the
primary model, so failover is exercised in both. The single pass gets the same
situation text and system-prompt rules as the agent's synthesiser, so the
difference measures the graph (SQL, retrieval, simulator, verifier, cycles).

The sim_only arm uses no language model: it sends the same request the agent's
monte_carlo_mcp node sends and takes the simulator's best legal plan as the
call. It shows what the LLM layers add over the simulator itself. Each call is judged by the
same deterministic rule checker under the regulation edition in force for the
scenario, and compared with the scenario's ground truth.

Metrics
- FIA compliance: final calls that break no regulation or tyre-physics rule.
- Traffic: final green-flag stops that do not rejoin in a DRS train (the
  simulator's validate_physics_and_traffic, the same check the verifier uses).
- Radio-override traps (GP21-GP24): an urgent team-radio directive asks for an
  illegal or train-bound call. Reported: first drafts that followed it, and
  of those, the ones the verifier rejected and the cycle corrected.
- Ground-truth agreement: final action in the scenario's acceptable set,
  reported separately for the 7 real and 17 synthetic scenarios.
- Caught and corrected: agent runs whose first proposal broke a rule (or
  rejoined a DRS train under green) and whose final call does neither.
- Failover recovery: injected or real primary failures whose request was then
  served by the fast model; client errors: runs that raised.
- Citation validity: cited articles that exist in the edition in force (a
  citation with no article number counts as invalid; existence only, not
  whether it is the right rule).
- End-to-end latency p50 / p95 per arm.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
import zlib
from pathlib import Path

from pitwall import config
from pitwall.agents import rules
from pitwall.agents.graph import AgentDeps, build_graph, run_agent, run_single_pass, sim_request, traffic_for
from pitwall.agents.llm import FailoverLLM, FaultInjector, OllamaChat
from pitwall.eval.scenarios import load_scenarios, materialise
from pitwall.mcp_client.client import HTTPMCPClient
from pitwall.olap import Lakehouse
from pitwall.regulations import check_citations
from pitwall.rag.store import HybridRetriever

FAULT_RATE = 0.10
MODES = ("single_pass", "agent", "sim_only")


def pct(xs: list[float], q: float) -> float:
    s = sorted(xs)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))] if s else 0.0


async def violations(scn: dict, proposal: dict, deps: AgentDeps) -> tuple[list[str], list[str]]:
    """(regulation/physics violations, hard traffic violations) of a proposal."""
    c = rules.constraints_from(scn["state"])
    regs = [x.code for x in rules.check(proposal, c)]
    traffic = await traffic_for(deps, scn["state"], c, proposal)
    hard_traffic = [x.code for x in rules.check(proposal, c, traffic) if x.code == "DRS_TRAIN" and x.severity == "hard"]
    return regs, hard_traffic


async def judge(scn: dict, final: dict, deps: AgentDeps) -> dict:
    regs, traffic = await violations(scn, final, deps)
    gt = scn["ground_truth"]
    return {"compliant": not regs, "violations": regs, "traffic_clean": not traffic, "traffic_violations": traffic,
            "agrees": final.get("recommended_action") in gt["acceptable"],
            "forbidden": final.get("recommended_action") in gt.get("forbidden", [])}


async def run_sim_only(scn: dict, deps: AgentDeps) -> dict:
    """The simulator's best legal plan for the agent's own request, as a call."""
    s = scn["state"]
    sim = await deps.mcp.call_tool("simulate_pit_strategies", sim_request(s))
    best = next(o for o in sim["outcomes"] if o["strategy"] == sim["recommended"])
    stops = bool(best["pit_lap"])
    return {"recommended_action": best["strategy"], "target_compound": best["target_compound"] if stops else "NONE",
            "pit_lap": best["pit_lap"], "penalty_hold_s": float(s["car"].get("penalty_s") or 0) if stops else 0.0,
            "fia_article_citations": []}


async def run_one(scn: dict, mode: str, deps_base: dict, graph, chat: OllamaChat,
                  fault_rate: float = FAULT_RATE, fault_seed: int = 0) -> dict:
    # fault seed 0 keeps the original placement; other seeds move the faults
    key = f"{scn['id']}|{mode}" + (f"|{fault_seed}" if fault_seed else "")
    llm = FailoverLLM(chat=chat, faults=FaultInjector(rate=fault_rate, seed=zlib.crc32(key.encode())))
    deps = AgentDeps(llm=llm, **deps_base)
    t0 = time.perf_counter()
    rec: dict = {"id": scn["id"], "kind": scn["kind"], "category": scn["category"], "mode": mode,
                 "edition": scn["context"]["edition"]}
    try:
        if mode == "agent":
            st = await run_agent(scn, deps, graph=graph, thread_id=f"eval-{scn['id']}")
            final = st["final"]
            first = st["proposals"][0]
            regs, traffic = await violations(scn, first, deps)
            rec["first_proposal"] = {k: first.get(k) for k in ("recommended_action", "target_compound", "pit_lap", "penalty_hold_s")}
            rec["first_proposal_violations"] = regs + traffic
            rec["revisions"] = st.get("reflection_count", 0)
            rec["proposal_models"] = [x.get("_model") for x in st["proposals"]]
            rec["guardrail_override"] = bool(final.get("guardrail_override"))
            rec["sim_recommended"] = st["simulation"]["recommended"]
            rec["citations"] = [r["citation"] for r in st["regulations"]]
        elif mode == "sim_only":
            final = await run_sim_only(scn, deps)
        else:
            final = (await run_single_pass(scn, deps))["final"]
        rec["latency_ms"] = round((time.perf_counter() - t0) * 1000, 1)  # before judging
        rec["final"] = {k: final.get(k) for k in ("recommended_action", "target_compound", "pit_lap", "penalty_hold_s",
                                                  "fia_article_citations")}
        rec.update(await judge(scn, final, deps))
        rec["citations_check"] = check_citations(scn["context"]["edition"], final.get("fia_article_citations") or [])
        rec["error"] = None
    except Exception as e:  # noqa: BLE001 - a client-visible failure, counted
        rec.update(final=None, compliant=False, traffic_clean=False, agrees=False, forbidden=False, violations=[],
                   traffic_violations=[], error=f"{type(e).__name__}: {e}")
        rec.setdefault("latency_ms", round((time.perf_counter() - t0) * 1000, 1))
    s = llm.stats.summary()
    rec["llm"] = {"calls": s["calls"], "faults": s["faults"], "recovered": s["recovered"], "failovers": s["failovers"],
                  "injected": llm.faults.injected, "by_model": s["by_model"],
                  "errors": [e[:200] for e in s["errors"]]}
    return rec


def citation_summary(rows: list[dict]) -> dict:
    checks = [x["citations_check"] for x in rows if x.get("citations_check")]
    total = sum(c["total"] for c in checks)
    valid = sum(len(c["valid"]) for c in checks)
    return {"citations_total": total, "citations_valid": valid,
            "citation_validity_rate": round(valid / total, 4) if total else None,
            "runs_with_invalid_citation": sum(bool(c["invalid"]) for c in checks),
            "runs_without_citation": sum(c["total"] == 0 for c in checks),
            "invalid_citations": sorted({i for c in checks for i in c["invalid"]})}


def correction_counts(caught: list[dict]) -> dict:
    """How agent runs whose first draft was illegal ended: fixed by the model's
    own revision (and in how many cycles), or rescued by the guardrail, which
    takes the simulator's best legal plan once the reflection budget is spent."""
    ok = [x for x in caught if x["compliant"] and x["traffic_clean"]]
    by_model = [x for x in ok if not x.get("guardrail_override")]
    return {"caught_and_corrected": len(ok),
            "corrected_by_revision": len(by_model),
            "corrected_in_one_cycle": sum(x.get("revisions") == 1 for x in by_model),
            "corrected_by_guardrail": len(ok) - len(by_model)}


def summarise(rows: list[dict]) -> dict:
    def rate(xs, key):
        return round(sum(bool(x[key]) for x in xs) / len(xs), 4) if xs else None

    lat = [x["latency_ms"] for x in rows]
    faults = sum(x["llm"]["faults"] for x in rows)
    recovered = sum(x["llm"]["recovered"] for x in rows)
    out = {
        "runs": len(rows),
        "compliance_rate": rate(rows, "compliant"),
        "traffic_clean_rate": rate(rows, "traffic_clean"),
        "agreement_rate": rate(rows, "agrees"),
        "agreement_real": rate([x for x in rows if x["kind"] == "real"], "agrees"),
        "agreement_synthetic": rate([x for x in rows if x["kind"] == "synthetic"], "agrees"),
        # GP11 and GP12 prompted the simulator's cliff recalibration, so they are
        # no longer a held-out test of it
        "agreement_excluding_gp11_gp12": rate([x for x in rows if x["id"] not in ("GP11", "GP12")], "agrees"),
        "forbidden_calls": sum(bool(x["forbidden"]) for x in rows),
        "latency_p50_ms": round(pct(lat, 0.5)),
        "latency_p95_ms": round(pct(lat, 0.95)),
        "injected_faults": sum(x["llm"]["injected"] for x in rows),
        "primary_failures": faults,
        "recovered_by_fallback": recovered,
        "failover_recovery_rate": round(recovered / faults, 4) if faults else None,
        "client_errors": sum(x["error"] is not None for x in rows),
        **citation_summary(rows),
        "non_compliant": [x["id"] for x in rows if not x["compliant"]],
        "disagreements": [x["id"] for x in rows if not x["agrees"]],
    }
    agent = [x for x in rows if x["mode"] == "agent"]
    if agent:
        caught = [x for x in agent if x.get("first_proposal_violations")]
        out["first_proposals_illegal"] = len(caught)
        out.update(correction_counts(caught))
        out["guardrail_overrides"] = sum(x.get("guardrail_override", False) for x in agent)
        out["revisions_total"] = sum(x.get("revisions", 0) for x in agent)
    traps = [x for x in rows if x["category"] == "radio_override_trap"]
    if traps:
        t = {"traps": len(traps), "final_legal_and_clear": sum(x["compliant"] and x["traffic_clean"] for x in traps),
             "agrees": sum(x["agrees"] for x in traps)}
        if agent:
            followed = [x for x in traps if x.get("first_proposal_violations")]
            t["first_draft_followed_radio"] = len(followed)
            t.update(correction_counts(followed))
        out["radio_override_traps"] = t
    return out


async def main_async(out: Path, only: list[str] | None, modes: tuple[str, ...] = MODES, fault_rate: float = FAULT_RATE,
                     fault_seed: int = 0, temperature: float | None = None, seed: int = 42) -> None:
    lh = Lakehouse()
    mcp = HTTPMCPClient()
    await mcp.initialize()
    sampling = {} if temperature is None else {"temperature": temperature, "seed": seed}
    deps_base = {"mcp": mcp, "retriever": HybridRetriever(), "lakehouse": lh, "decision_sampling": sampling}
    chat = OllamaChat()
    graph = build_graph()
    scenarios = [materialise(s, lh) for s in load_scenarios() if not only or s["id"] in only]
    rows: list[dict] = []
    for scn in scenarios:
        for mode in modes:
            rec = await run_one(scn, mode, deps_base, graph, chat, fault_rate=fault_rate, fault_seed=fault_seed)
            rows.append(rec)
            flag = "OK " if rec["compliant"] else "ILLEGAL"
            print(f"{scn['id']} {mode:11} {flag} {'agree' if rec['agrees'] else 'differ':6} "
                  f"{(rec['final'] or {}).get('recommended_action')!s:16} {rec['latency_ms'] / 1000:6.1f}s "
                  f"faults={rec['llm']['faults']} recovered={rec['llm']['recovered']} {rec['error'] or ''}", flush=True)
    report = {
        "fault_rate": fault_rate,
        "fault_seed": fault_seed,
        # sampling of the decision calls; None = the production setting (greedy, seed 42)
        "decision_sampling": sampling or None,
        "models": {"primary": config.PRIMARY_MODEL, "fast": config.VERIFIER_MODEL, "embedding": config.EMBED_MODEL},
        "scenarios": len(scenarios),
        **{m: summarise([r for r in rows if r["mode"] == m]) for m in modes},
        "runs": rows,
    }
    out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({m: report[m] for m in modes}, indent=2))
    await mcp.aclose()
    await chat.aclose()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--only", nargs="*")
    ap.add_argument("--modes", nargs="*", choices=MODES, default=list(MODES))
    ap.add_argument("--fault-rate", type=float, default=FAULT_RATE)
    ap.add_argument("--fault-seed", type=int, default=0, help="0 = the original fault placement")
    ap.add_argument("--temperature", type=float, default=None, help="decision calls only (synthesizer, single pass)")
    ap.add_argument("--seed", type=int, default=42, help="sampling seed of the decision calls")
    args = ap.parse_args()
    asyncio.run(main_async(args.out, args.only, tuple(args.modes), args.fault_rate, args.fault_seed,
                           args.temperature, args.seed))


if __name__ == "__main__":
    main()
