"use client";

import { useState } from "react";
import { fmtGap, fmtLap } from "@/lib/format";
import type { Snapshot } from "@/lib/types";
import { SectionTitle, TrackStatusPill, TyreBadge } from "./Bits";

// Degradation bars share one fixed scale so rows compare: 0 to 200 ms/lap.
const DEG_MAX = 200;

/** Fuel-corrected degradation as a thin single-hue bar with the value beside it. */
export function DegBar({ value }: { value: number | null }) {
  if (value == null) return <span className="muted">–</span>;
  const w = Math.max(0, Math.min(1, value / DEG_MAX)) * 64;
  const improving = value < 0;
  return (
    <span
      className={`inline-flex items-center gap-2 ${improving ? "muted" : ""}`}
      title={improving
        ? `${value.toFixed(1)} ms/lap: lap times improving faster than fuel burn (track evolution, traffic clearing), not tyre wear`
        : `${value.toFixed(1)} ms/lap fuel-corrected degradation`}
    >
      <svg width="64" height="8" aria-hidden className="shrink-0">
        <rect x="0" y="0" width="64" height="8" rx="4" fill="var(--grid)" />
        {value > 0 && <rect x="0" y="0" width={Math.max(4, w)} height="8" rx="4" fill="var(--accent)" />}
      </svg>
      <span className="tnum w-12 text-right">{improving ? `\u2212${Math.abs(value).toFixed(0)}` : value.toFixed(0)}</span>
    </span>
  );
}

export function TowerTable({ drivers, onSelect, selected }: {
  drivers: Snapshot[];
  onSelect?: (driver: number) => void;
  selected?: number | null;
}) {
  const [hover, setHover] = useState<number | null>(null);
  const lead = drivers[0];
  return (
    <div>
      <SectionTitle
        title="Live timing tower"
        sub={lead ? `Lap ${lead.lap_number} · session ${lead.session_key} · fuel-corrected degradation over the last 5 clean laps` : "waiting for the stream"}
        right={lead && <TrackStatusPill status={lead.track_status} />}
      />
      <div className="overflow-x-auto">
        <table className="w-full text-sm" data-testid="tower">
          <thead>
            <tr className="muted text-left text-xs">
              <th className="py-1.5 pr-2 font-normal">Pos</th>
              <th className="pr-2 font-normal">Driver</th>
              <th className="pr-2 text-right font-normal">Gap</th>
              <th className="pr-2 text-right font-normal">Int</th>
              <th className="pr-2 font-normal">Tyre</th>
              <th className="pr-2 font-normal">Deg ms/lap</th>
              <th className="pr-2 text-right font-normal">Last lap</th>
              <th className="text-right font-normal">Stops</th>
            </tr>
          </thead>
          <tbody>
            {drivers.map((d, i) => (
              <tr
                key={d.driver_number}
                data-testid="tower-row"
                onMouseEnter={() => setHover(d.driver_number)}
                onMouseLeave={() => setHover(null)}
                onClick={() => onSelect?.(d.driver_number)}
                className="cursor-pointer"
                style={{
                  borderTop: "1px solid var(--grid)",
                  background: selected === d.driver_number ? "var(--accent-wash)" : hover === d.driver_number ? "var(--surface-2)" : undefined,
                }}
              >
                <td className="tnum py-1.5 pr-2">{d.position}</td>
                <td className="pr-2 font-medium">
                  {d.driver_code} <span className="muted text-xs">#{d.driver_number}</span>
                </td>
                <td className="tnum pr-2 text-right">{fmtGap(d.gap_to_leader_ms, { leader: i === 0 })}</td>
                <td className="tnum pr-2 text-right">{i === 0 ? "" : fmtGap(d.interval_ahead_ms)}</td>
                <td className="pr-2">
                  <TyreBadge compound={d.compound} age={d.tyre_age_laps} />
                </td>
                <td className="pr-2">
                  <DegBar value={d.deg_slope_ms_per_lap} />
                </td>
                <td className="tnum pr-2 text-right">{fmtLap(d.last_lap_ms)}</td>
                <td className="tnum text-right">{d.pit_stops}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
