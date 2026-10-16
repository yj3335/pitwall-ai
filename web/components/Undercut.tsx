"use client";

import { useState } from "react";
import { fmtPct, fmtSigned } from "@/lib/format";
import { linear, niceDomain, ticks } from "@/lib/scale";
import type { Traffic, Undercut } from "@/lib/types";
import { SectionTitle, StatusPill } from "./Bits";

const W = 560;
const ROW = 30;
const LEFT = 210;
const RIGHT = 64;
const TOP = 6;
const AXIS = 24;

export function undercutRows(u: Undercut) {
  const [lap1, lap2] = u.lap_deltas_ms;
  const rows = [
    { label: "Lap 1 · new-tyre out-lap vs old tyre", ms: lap1 },
    { label: "Lap 2 · rival's out-lap", ms: lap2 },
  ];
  if (u.attacker_penalty_hold_ms) rows.push({ label: "Penalty hold at the stop", ms: -u.attacker_penalty_hold_ms });
  rows.push({ label: "Net gain", ms: u.expected_gain_ms });
  return rows;
}

/** Diverging bars: time gained (blue) or lost (red) by the attacker. */
export function UndercutChart({ undercut }: { undercut: Undercut }) {
  const [focus, setFocus] = useState<number | null>(null);
  const rows = undercutRows(undercut);
  const ext = Math.max(1, ...rows.map((r) => Math.abs(r.ms) / 1000));
  const [d0, d1] = niceDomain(-ext, ext, 4);
  const lim = Math.max(Math.abs(d0), Math.abs(d1));
  const x = linear([-lim, lim], [LEFT, W - RIGHT]);
  const H = TOP + rows.length * ROW + AXIS;
  const works = undercut.expected_gap_after_ms < 0;
  return (
    <div data-testid="undercut">
      <SectionTitle
        title={`${undercut.mode === "OVERCUT" ? "Overcut" : "Undercut"} on the car ahead`}
        sub={`Gap ${(undercut.gap_before_ms / 1000).toFixed(1)} s → ${fmtSigned(undercut.expected_gap_after_ms / 1000)} s after both stops · attacker ahead in ${fmtPct(undercut.p_attacker_ahead_after_stops)} of rollouts`}
        right={<StatusPill kind={works ? "good" : "critical"} label={works ? "works" : "fails"} testId="undercut-verdict" />}
      />
      <svg viewBox={`0 0 ${W} ${H}`} width="100%" role="img" aria-label="Undercut time components">
        {ticks(-lim, lim, 4).map((t) => (
          <g key={t}>
            <line x1={x(t)} x2={x(t)} y1={TOP} y2={H - AXIS} stroke={t === 0 ? "var(--div-mid)" : "var(--grid)"} strokeWidth={t === 0 ? 2 : 1} />
            <text x={x(t)} y={H - 6} textAnchor="middle" fontSize="11" fill="var(--text-muted)" className="tnum">
              {t === 0 ? "0" : `${fmtSigned(t, 0)} s`}
            </text>
          </g>
        ))}
        {rows.map((r, i) => {
          const cy = TOP + i * ROW + ROW / 2;
          const v = r.ms / 1000;
          const x0 = x(0);
          const x1 = x(v);
          const left = Math.min(x0, x1);
          const width = Math.max(2, Math.abs(x1 - x0));
          const fill = v >= 0 ? "var(--div-pos)" : "var(--div-neg)";
          const net = i === rows.length - 1;
          return (
            <g key={r.label} tabIndex={0} className="focus-ring outline-none" onMouseEnter={() => setFocus(i)} onMouseLeave={() => setFocus(null)} onFocus={() => setFocus(i)} onBlur={() => setFocus(null)}>
              <rect x={0} y={cy - ROW / 2} width={W} height={ROW} fill={focus === i ? "var(--surface-2)" : "transparent"} />
              <text x={8} y={cy + 4} fontSize="12" fill="var(--text-primary)" fontWeight={net ? 600 : 400}>{r.label}</text>
              {/* rounded data-end, square at the zero baseline */}
              <path
                d={roundedBar(left, cy - 7, width, 14, v >= 0 ? "right" : "left")}
                fill={fill}
              />
              <text
                x={v >= 0 ? x1 + 6 : x1 - 6}
                y={cy + 4}
                fontSize="12"
                textAnchor={v >= 0 ? "start" : "end"}
                fill="var(--text-secondary)"
                className="tnum"
              >
                {fmtSigned(v)} s
              </text>
            </g>
          );
        })}
      </svg>
      <p className="muted mt-1 text-xs">{undercut.explanation}</p>
    </div>
  );
}

/** Bar path with a 4px rounded data-end and a square end at the baseline. */
function roundedBar(x: number, y: number, w: number, h: number, end: "left" | "right") {
  const r = Math.min(4, w / 2, h / 2);
  if (end === "right") {
    return `M${x},${y} H${x + w - r} Q${x + w},${y} ${x + w},${y + r} V${y + h - r} Q${x + w},${y + h} ${x + w - r},${y + h} H${x} Z`;
  }
  return `M${x + w},${y} H${x + r} Q${x},${y} ${x},${y + r} V${y + h - r} Q${x},${y + h} ${x + r},${y + h} H${x + w} Z`;
}

export function TrafficCard({ traffic }: { traffic: Traffic }) {
  return (
    <div data-testid="traffic">
      <SectionTitle
        title="Pit-rejoin traffic window"
        sub="Field projected to the stop lap (Go MCP validate_physics_and_traffic)"
        right={<StatusPill kind={traffic.in_drs_train ? "serious" : "good"} label={traffic.in_drs_train ? "DRS train" : "clear air"} />}
      />
      <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
        <dt className="muted">Rejoin position</dt>
        <dd className="tnum">P{traffic.rejoin_position}</dd>
        <dt className="muted">Car ahead on rejoin</dt>
        <dd>{traffic.car_ahead ? `${traffic.car_ahead} · ${(traffic.gap_to_car_ahead_ms / 1000).toFixed(1)} s` : "none"}</dd>
        <dt className="muted">Within 1.2 s</dt>
        <dd>{traffic.cars_within_1_2s.length ? traffic.cars_within_1_2s.join(", ") : "none"}</dd>
        {traffic.in_drs_train && (
          <>
            <dt className="muted">DRS train</dt>
            <dd>{traffic.drs_train.join(" · ")}</dd>
          </>
        )}
        {traffic.suggested_clear_air_pit_laps?.length ? (
          <>
            <dt className="muted">Clear-air laps</dt>
            <dd className="tnum">{traffic.suggested_clear_air_pit_laps.join(", ")}</dd>
          </>
        ) : null}
      </dl>
      <p className="muted mt-1 text-xs">{traffic.verdict}</p>
    </div>
  );
}
