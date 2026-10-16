"use client";

import { useState } from "react";
import { fmtAction, fmtMs, NODE_LABEL } from "@/lib/format";
import { verifierOutcome, type RunView } from "@/lib/run";
import type { RegulationHit } from "@/lib/types";
import { SectionTitle, StatusPill, TyreBadge } from "./Bits";

/** Live LangGraph trace: one row per node execution, in order, with its
 *  duration as a thin single-hue bar on a shared scale. */
export function TraceTimeline({ run }: { run: RunView }) {
  const maxMs = Math.max(1, ...run.steps.map((s) => s.ms));
  let synth = 0;
  return (
    <div data-testid="trace">
      <SectionTitle
        title="LangGraph trace"
        sub={run.edition ? `Regulations in force: ${run.edition}` : "telemetry → regulations → Monte Carlo → synthesis → verifier (↺ on violation, at most twice)"}
        right={
          run.status === "running" || run.status === "queued" ? (
            <StatusPill kind="warning" label={run.status === "queued" ? "queued" : "running"} testId="run-status" />
          ) : run.status === "succeeded" ? (
            <StatusPill kind="good" label="complete" testId="run-status" />
          ) : run.status === "failed" ? (
            <StatusPill kind="critical" label="failed" testId="run-status" />
          ) : null
        }
      />
      {run.steps.length === 0 && run.status === "idle" && <p className="muted text-sm">Pick a scenario and run the agent.</p>}
      <ol className="space-y-1.5">
        {run.steps.map((s, i) => {
          const outcome = verifierOutcome(s);
          const isRevision = s.node === "strategy_synthesizer" && synth++ > 0;
          return (
            <li key={i} data-testid="trace-step" data-node={s.node} className="grid grid-cols-[180px_1fr_72px] items-start gap-3 text-sm">
              <div className="flex items-center gap-1.5">
                {isRevision && <span title="self-correction cycle" aria-label="revision" style={{ color: "var(--warning)" }}>↺</span>}
                <span className="font-medium">{NODE_LABEL[s.node] ?? s.node}</span>
              </div>
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  {outcome === "pass" && <StatusPill kind="good" label="pass" />}
                  {outcome === "revise" && <StatusPill kind="warning" label="revise" testId="verifier-revise" />}
                  {outcome === "violation" && <StatusPill kind="critical" label="unresolved" />}
                  <span className="secondary truncate" title={s.summary}>{s.summary}</span>
                </div>
                <svg width="100%" height="4" className="mt-1" aria-hidden preserveAspectRatio="none" viewBox="0 0 100 4">
                  <rect x="0" y="0" width={Math.max(1, (s.ms / maxMs) * 100)} height="4" rx="2" fill="var(--accent)" opacity="0.7" />
                </svg>
              </div>
              <span className="tnum muted text-right text-xs">{fmtMs(s.ms)}</span>
            </li>
          );
        })}
      </ol>
    </div>
  );
}

export function FinalCallCard({ run }: { run: RunView }) {
  const f = run.final;
  if (!f) return null;
  const a = run.assessment;
  return (
    <div className="card p-4" data-testid="final-call" style={{ borderColor: "var(--accent)" }}>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <div className="muted text-xs">Pit-wall call</div>
          <div className="text-xl font-semibold" data-testid="final-action">{fmtAction(f.recommended_action)}</div>
        </div>
        <div className="flex flex-wrap gap-2">
          {a && <StatusPill kind={a.compliant ? "good" : "critical"} label={a.compliant ? `compliant · ${a.edition}` : "breaks a rule"} testId="compliance" />}
          {a?.agrees_with_ground_truth !== undefined && (
            <StatusPill kind={a.agrees_with_ground_truth ? "good" : "serious"} label={a.agrees_with_ground_truth ? "matches ground truth" : "differs from ground truth"} testId="ground-truth" />
          )}
          {f.guardrail_override && <StatusPill kind="serious" label="guardrail override" />}
        </div>
      </div>
      <dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-sm sm:grid-cols-4">
        <dt className="muted">Tyre</dt>
        <dd>{f.pit_lap ? <TyreBadge compound={f.target_compound} /> : "no stop"}</dd>
        <dt className="muted">Pit lap</dt>
        <dd className="tnum">{f.pit_lap || "–"}</dd>
        <dt className="muted">Penalty hold</dt>
        <dd className="tnum">{f.penalty_hold_s ? `${f.penalty_hold_s.toFixed(0)} s` : "none"}</dd>
        <dt className="muted">Revisions</dt>
        <dd className="tnum" data-testid="revisions">{f.reflections ?? run.revisions}</dd>
      </dl>
      <blockquote className="mt-3 border-l-2 pl-3 text-sm italic" style={{ borderColor: "var(--axis)" }} data-testid="radio">
        “{f.engineer_radio_message}”
      </blockquote>
      {f.fia_article_citations.length > 0 && (
        <p className="secondary mt-2 text-xs">Cites: {f.fia_article_citations.join(", ")}</p>
      )}
      {run.elapsedMs !== undefined && (
        <p className="muted mt-1 text-xs">
          {fmtMs(run.elapsedMs)} end to end
          {run.llm ? ` · ${run.llm.calls} model calls · ${run.llm.failovers} failovers` : ""}
        </p>
      )}
    </div>
  );
}

export function Citations({ hits, constraints }: { hits: RegulationHit[]; constraints: string[] }) {
  const [open, setOpen] = useState<string | null>(null);
  if (!hits.length && !constraints.length) return null;
  return (
    <div data-testid="citations">
      <SectionTitle title="FIA regulations · Qdrant hybrid search" sub="dense bge-m3 + sparse BM25, fused by RRF on the server, then cross-encoder rerank; filtered to the issue in force and to what was known before the decision" />
      {constraints.length > 0 && (
        <ul className="secondary mb-3 list-disc space-y-0.5 pl-5 text-xs">
          {constraints.map((c) => <li key={c}>{c}</li>)}
        </ul>
      )}
      <ul className="space-y-1.5">
        {hits.map((h) => (
          <li key={h.id} className="text-sm" data-testid="citation">
            <button type="button" className="focus-ring w-full text-left" onClick={() => setOpen(open === h.id ? null : h.id)} aria-expanded={open === h.id}>
              <span className="font-medium">{h.citation}</span>
              <span className="muted tnum ml-2 text-xs">RRF #{h.rrf_rank} · rerank {h.rerank_score.toFixed(2)}</span>
            </button>
            {open === h.id && <p className="secondary mt-1 whitespace-pre-line text-xs">{h.text}</p>}
          </li>
        ))}
      </ul>
    </div>
  );
}
