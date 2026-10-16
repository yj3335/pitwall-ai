// Pure reducer from the control plane's run event log to a view model.
import type { Assessment, FinalCall, RegulationHit, RunEvent, SimRow, TraceEvent, Traffic, Undercut } from "./types";

export interface RunView {
  status: "idle" | "queued" | "running" | "succeeded" | "failed";
  title?: string;
  edition?: string;
  steps: TraceEvent[];
  revisions: number;
  simulation?: { table: SimRow[]; recommended: string; rollouts: number; undercut: Undercut | null; traffic: Traffic | null };
  hits: RegulationHit[];
  constraints: string[];
  final?: FinalCall;
  assessment?: Assessment;
  elapsedMs?: number;
  llm?: { calls: number; failovers: number; faults: number; recovered: number; by_model: Record<string, number> };
  error?: string;
  lastEventId: number;
}

export const emptyRun = (): RunView => ({ status: "idle", steps: [], revisions: 0, hits: [], constraints: [], lastEventId: -1 });

export function reduceRun(view: RunView, ev: RunEvent): RunView {
  if (ev.id <= view.lastEventId) return view; // replayed after a reconnect
  const v: RunView = { ...view, lastEventId: ev.id };
  const d = ev.data as Record<string, unknown>;
  switch (ev.type) {
    case "queued":
      return { ...v, status: "queued" };
    case "started":
      return { ...v, status: "running", title: d.title as string, edition: d.edition as string };
    case "node": {
      const step = d as unknown as TraceEvent;
      const out: RunView = { ...v, status: "running", steps: [...v.steps, step] };
      if (step.node === "monte_carlo_mcp" && step.table) {
        out.simulation = {
          table: step.table,
          recommended: step.recommended ?? "",
          rollouts: step.rollouts ?? 0,
          undercut: step.undercut ?? null,
          traffic: step.traffic ?? null,
        };
      }
      if (step.node === "fia_regulation_rag") {
        out.hits = step.hits ?? [];
        out.constraints = step.constraints ?? [];
      }
      // every synthesizer step after the first is a self-correction cycle
      out.revisions = Math.max(0, out.steps.filter((s) => s.node === "strategy_synthesizer").length - 1);
      return out;
    }
    case "final":
      return {
        ...v,
        final: d.final as FinalCall,
        assessment: d.assessment as Assessment,
        elapsedMs: d.elapsed_ms as number,
        llm: d.llm as RunView["llm"],
      };
    case "error":
      return { ...v, status: "failed", error: d.error as string };
    case "done":
      return { ...v, status: (d.status as RunView["status"]) ?? v.status };
    default:
      return v;
  }
}

/** Verifier step outcome for the timeline. */
export function verifierOutcome(step: TraceEvent): "pass" | "revise" | "violation" | null {
  if (step.node !== "verifier") return null;
  if (step.summary.includes("REVISE")) return "revise";
  if ((step.violations ?? []).length > 0) return "violation";
  return "pass";
}
