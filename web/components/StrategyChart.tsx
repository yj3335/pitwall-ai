"use client";

import { useState } from "react";
import { fmtAction, fmtPct, fmtSigned } from "@/lib/format";
import { linear, niceDomain, ticks } from "@/lib/scale";
import type { SimRow } from "@/lib/types";
import { SectionTitle } from "./Bits";

const W = 640;
const ROW = 34;
const LEFT = 168;
const RIGHT = 96;
const TOP = 8;
const AXIS = 26;
// A plan far outside the contenders (e.g. intermediates on a dry track,
// +130 s) would flatten the axis: such outliers are excluded from the domain
// and drawn at the edge with an explicit off-scale label.
const OUTLIER_FLOOR_S = 20;

export interface StrategyChartProps {
  rows: SimRow[];
  recommended: string;
  rollouts: number;
}

function median(xs: number[]): number {
  const s = [...xs].sort((a, b) => a - b);
  return s.length ? s[Math.floor((s.length - 1) / 2)] : 0;
}

export function isOffScale(row: SimRow, rows: SimRow[]): boolean {
  const cut = Math.max(OUTLIER_FLOOR_S, 3 * median(rows.filter((r) => r.legal).map((r) => r.expected_delta_s)));
  return row.expected_delta_s > cut;
}

export function domainFor(rows: SimRow[]): [number, number] {
  const inScale = rows.filter((r) => !isOffScale(r, rows));
  const lo = Math.min(0, ...inScale.map((r) => r.p10_p90_s[0]));
  const hi = Math.max(1, ...inScale.map((r) => Math.max(r.p10_p90_s[1], r.expected_delta_s)));
  return niceDomain(lo, hi, 5);
}

export function StrategyChart({ rows, recommended, rollouts }: StrategyChartProps) {
  const [view, setView] = useState<"chart" | "table">("chart");
  const [focus, setFocus] = useState<number | null>(null);
  const sorted = [...rows].sort((a, b) => Number(!a.legal) - Number(!b.legal) || a.expected_delta_s - b.expected_delta_s);
  const [d0, d1] = domainFor(sorted);
  const x = linear([d0, d1], [LEFT, W - RIGHT]);
  const H = TOP + sorted.length * ROW + AXIS;
  const clampX = (v: number) => x(Math.max(d0, Math.min(d1, v)));

  return (
    <div data-testid="strategy-chart">
      <SectionTitle
        title="Monte Carlo strategy distribution"
        sub={`${rollouts.toLocaleString("en-US")} rollouts per strategy · expected race-time delta to the best legal plan, P10–P90 band (lower is better)`}
        right={
          <button
            type="button"
            className="focus-ring rounded px-2 py-1 text-xs"
            style={{ border: "1px solid var(--border)" }}
            onClick={() => setView(view === "chart" ? "table" : "chart")}
            data-testid="strategy-view-toggle"
          >
            {view === "chart" ? "Table view" : "Chart view"}
          </button>
        }
      />
      {view === "chart" ? (
        <div className="relative">
          <div className="muted mb-1 flex gap-4 text-xs" aria-hidden>
            <span className="inline-flex items-center gap-1.5">
              <svg width="10" height="10"><circle cx="5" cy="5" r="4" fill="var(--accent)" /></svg>best legal plan
            </span>
            <span className="inline-flex items-center gap-1.5">
              <svg width="10" height="10"><circle cx="5" cy="5" r="4" fill="var(--deemph)" /></svg>other legal plans
            </span>
            <span className="inline-flex items-center gap-1.5">
              <svg width="10" height="10"><circle cx="5" cy="5" r="3.5" fill="none" stroke="var(--deemph)" strokeWidth="1.5" /></svg>breaks a rule
            </span>
          </div>
          <svg viewBox={`0 0 ${W} ${H}`} width="100%" role="img" aria-label="Expected race-time delta per strategy with P10 to P90 band">
            {ticks(d0, d1, 5).map((t) => (
              <g key={t}>
                <line x1={x(t)} x2={x(t)} y1={TOP} y2={H - AXIS} stroke={t === 0 ? "var(--axis)" : "var(--grid)"} strokeWidth="1" />
                <text x={x(t)} y={H - 8} textAnchor="middle" fontSize="11" fill="var(--text-muted)" className="tnum">
                  {t === 0 ? "0 s" : `${fmtSigned(t, Number.isInteger(t) ? 0 : 1)} s`}
                </text>
              </g>
            ))}
            {sorted.map((r, i) => {
              const cy = TOP + i * ROW + ROW / 2;
              const best = r.strategy === recommended;
              const ink = best ? "var(--accent)" : "var(--deemph)";
              const off = r.expected_delta_s > d1 || isOffScale(r, sorted);
              const [p10, p90] = r.p10_p90_s;
              return (
                <g
                  key={r.strategy}
                  data-testid="strategy-row"
                  data-strategy={r.strategy}
                  tabIndex={0}
                  className="focus-ring outline-none"
                  onMouseEnter={() => setFocus(i)}
                  onMouseLeave={() => setFocus(null)}
                  onFocus={() => setFocus(i)}
                  onBlur={() => setFocus(null)}
                >
                  {/* hit target: the whole row, larger than the mark */}
                  <rect x={0} y={cy - ROW / 2} width={W} height={ROW} fill={focus === i ? "var(--surface-2)" : "transparent"} />
                  <text x={8} y={cy + 4} fontSize="12" fill="var(--text-primary)" fontWeight={best ? 600 : 400}>
                    {fmtAction(r.strategy)}
                  </text>
                  {!r.legal && (
                    <text x={LEFT - 10} y={cy + 4} fontSize="10" textAnchor="end" fill="var(--text-muted)">
                      illegal
                    </text>
                  )}
                  <line x1={clampX(p10)} x2={clampX(p90)} y1={cy} y2={cy} stroke={ink} strokeWidth="2" strokeLinecap="round" />
                  {r.legal ? (
                    <circle cx={clampX(r.expected_delta_s)} cy={cy} r={5} fill={ink} stroke="var(--surface-1)" strokeWidth="2" />
                  ) : (
                    <circle cx={clampX(r.expected_delta_s)} cy={cy} r={4.5} fill="var(--surface-1)" stroke={ink} strokeWidth="2" />
                  )}
                  <text x={W - RIGHT + 8} y={cy + 4} fontSize="12" fill={best ? "var(--text-primary)" : "var(--text-secondary)"} className="tnum">
                    {off ? `${fmtSigned(r.expected_delta_s)} s ›` : best ? "best" : `${fmtSigned(r.expected_delta_s)} s`}
                  </text>
                </g>
              );
            })}
          </svg>
          {focus !== null && <Tip row={sorted[focus]} topPct={((TOP + focus * ROW + ROW) / H) * 100} />}
        </div>
      ) : (
        <StrategyTable rows={sorted} recommended={recommended} />
      )}
    </div>
  );
}

function Tip({ row, topPct }: { row: SimRow; topPct: number }) {
  return (
    <div
      role="tooltip"
      data-testid="strategy-tooltip"
      className="pointer-events-none absolute left-40 z-10 rounded-md px-3 py-2 text-xs shadow-lg"
      style={{ top: `calc(${topPct.toFixed(2)}% + 18px)`, background: "var(--surface-2)", border: "1px solid var(--border)" }}
    >
      <div className="text-sm font-semibold">{fmtSigned(row.expected_delta_s)} s expected</div>
      <div className="secondary">{fmtAction(row.strategy)}</div>
      <div className="mt-1 grid grid-cols-2 gap-x-3 gap-y-0.5">
        <span className="muted">P10–P90</span>
        <span className="tnum">{fmtSigned(row.p10_p90_s[0])} to {fmtSigned(row.p10_p90_s[1])} s</span>
        <span className="muted">Expected finish</span>
        <span className="tnum">P{row.expected_finish.toFixed(1)}</span>
        <span className="muted">Ahead of car in front</span>
        <span className="tnum">{fmtPct(row.p_ahead_of_car_in_front)}</span>
        <span className="muted">Stop</span>
        <span>{row.pit_lap ? `lap ${row.pit_lap} · ${row.compound.toLowerCase()}` : "no further stop"}</span>
        {row.pit_lap > 0 && (
          <>
            <span className="muted">Rejoin</span>
            <span className="tnum">P{row.rejoin_position.toFixed(1)}</span>
          </>
        )}
      </div>
      {row.violations.length > 0 && <div className="mt-1 max-w-80" style={{ color: "var(--text-secondary)" }}>{row.violations[0]}</div>}
    </div>
  );
}

export function StrategyTable({ rows, recommended }: { rows: SimRow[]; recommended: string }) {
  return (
    <table className="w-full text-xs" data-testid="strategy-table">
      <thead>
        <tr className="muted text-left">
          <th className="py-1 font-normal">Strategy</th>
          <th className="text-right font-normal">Δ expected</th>
          <th className="text-right font-normal">P10 / P90</th>
          <th className="text-right font-normal">Finish</th>
          <th className="text-right font-normal">Ahead of car in front</th>
          <th className="text-right font-normal">Rejoin</th>
          <th className="pl-2 font-normal">Stop</th>
          <th className="pl-2 font-normal">Legal</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.strategy} style={{ borderTop: "1px solid var(--grid)" }}>
            <td className="py-1" style={{ fontWeight: r.strategy === recommended ? 600 : 400 }}>{fmtAction(r.strategy)}</td>
            <td className="tnum text-right">{fmtSigned(r.expected_delta_s)} s</td>
            <td className="tnum text-right">{fmtSigned(r.p10_p90_s[0])} / {fmtSigned(r.p10_p90_s[1])}</td>
            <td className="tnum text-right">P{r.expected_finish.toFixed(1)}</td>
            <td className="tnum text-right">{fmtPct(r.p_ahead_of_car_in_front)}</td>
            <td className="tnum text-right">{r.pit_lap ? `P${r.rejoin_position.toFixed(1)}` : "–"}</td>
            <td className="pl-2">{r.pit_lap ? `L${r.pit_lap} ${r.compound.toLowerCase()}` : "none"}</td>
            <td className="pl-2">{r.legal ? "yes" : "no"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
