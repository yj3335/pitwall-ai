import type { Compound } from "./types";

/** Gap/interval in seconds; OpenF1 reports unknown as -1. */
export function fmtGap(ms: number | null | undefined, opts: { leader?: boolean } = {}): string {
  if (opts.leader) return "Leader";
  if (ms == null || ms < 0) return "–";
  return `+${(ms / 1000).toFixed(1)}`;
}

/** Lap time as m:ss.sss */
export function fmtLap(ms: number | null | undefined): string {
  if (!ms || ms <= 0) return "–";
  const m = Math.floor(ms / 60000);
  const s = (ms % 60000) / 1000;
  return `${m}:${s.toFixed(3).padStart(6, "0")}`;
}

/** Signed seconds with one decimal, using a true minus sign. */
export function fmtSigned(seconds: number, digits = 1): string {
  const v = Number(seconds.toFixed(digits));
  if (v === 0) return (0).toFixed(digits);
  return `${v > 0 ? "+" : "−"}${Math.abs(v).toFixed(digits)}`;
}

export function fmtPct(p: number): string {
  return `${Math.round(p * 100)}%`;
}

/** 1,284 / 12.9K / 1.2M */
export function compact(n: number): string {
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (Math.abs(n) >= 1e4) return `${(n / 1e3).toFixed(1)}K`;
  return Math.round(n).toLocaleString("en-US");
}

export function fmtMs(ms: number): string {
  if (ms >= 10_000) return `${(ms / 1000).toFixed(1)} s`;
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`;
  if (ms >= 10) return `${Math.round(ms)} ms`;
  return `${ms.toFixed(1)} ms`;
}

const LETTER: Record<Compound, string> = {
  SOFT: "S",
  MEDIUM: "M",
  HARD: "H",
  INTERMEDIATE: "I",
  WET: "W",
  UNKNOWN: "?",
};

export function compoundLetter(c: string): string {
  return LETTER[(c as Compound) in LETTER ? (c as Compound) : "UNKNOWN"];
}

/** "PIT_NOW_HARD" -> "Pit now · Hard" */
export function fmtAction(action: string): string {
  const map: Record<string, string> = {
    PIT_NOW_HARD: "Pit now · Hard",
    PIT_NOW_MEDIUM: "Pit now · Medium",
    PIT_NOW_SOFT: "Pit now · Soft",
    PIT_NOW_INTER: "Pit now · Intermediate",
    STAY_OUT_EXTEND: "Stay out · extend",
    OPPOSITE_AHEAD: "Opposite to car ahead",
  };
  return map[action] ?? action;
}

export const NODE_LABEL: Record<string, string> = {
  telemetry_olap: "Telemetry · DuckDB",
  fia_regulation_rag: "FIA regulations · Qdrant",
  monte_carlo_mcp: "Monte Carlo · Go MCP",
  strategy_synthesizer: "Strategy synthesis",
  verifier: "Verifier",
  finalize: "Final call",
};
