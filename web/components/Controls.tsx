"use client";

import { useState } from "react";
import type { RaceControlMsg, ScenarioSummary, Snapshot, StreamTrigger } from "@/lib/types";
import { SectionTitle, StatusPill } from "./Bits";

const KIND_LABEL: Record<StreamTrigger["kind"], string> = {
  UNDERCUT_WINDOW: "Undercut window",
  TIRE_CLIFF: "Tyre cliff",
  CHEAP_PIT_WINDOW: "Cheap pit window",
  WET_CROSSOVER: "Wet crossover",
};

export function ScenarioPanel({
  scenarios,
  busy,
  onRun,
}: {
  scenarios: ScenarioSummary[];
  busy: boolean;
  onRun: (id: string, mode: "agent" | "single_pass", faultRate: number) => void;
}) {
  const [id, setId] = useState<string>("");
  const [mode, setMode] = useState<"agent" | "single_pass">("agent");
  const [fault, setFault] = useState(0);
  const current = scenarios.find((s) => s.id === (id || scenarios[0]?.id));
  const real = scenarios.filter((s) => s.kind === "real").length;
  return (
    <div data-testid="scenario-panel">
      <SectionTitle
        title="Strategy scenarios"
        sub={`${real} rebuilt from real OpenF1 races at the decision instant · ${scenarios.length - real} rule traps set in 2025`}
      />
      <div className="flex flex-wrap items-center gap-2">
        <select
          aria-label="Scenario"
          data-testid="scenario-select"
          className="focus-ring min-w-0 flex-1 rounded px-2 py-1.5 text-sm"
          style={{ background: "var(--surface-2)", border: "1px solid var(--border)" }}
          value={current?.id ?? ""}
          onChange={(e) => setId(e.target.value)}
        >
          {scenarios.map((s) => (
            <option key={s.id} value={s.id}>
              {s.id} · {s.title}
            </option>
          ))}
        </select>
        <select
          aria-label="Mode"
          data-testid="mode-select"
          className="focus-ring rounded px-2 py-1.5 text-sm"
          style={{ background: "var(--surface-2)", border: "1px solid var(--border)" }}
          value={mode}
          onChange={(e) => setMode(e.target.value as "agent" | "single_pass")}
        >
          <option value="agent">Cyclic agent</option>
          <option value="single_pass">Single-pass baseline</option>
        </select>
        <select
          aria-label="Injected fault rate"
          className="focus-ring rounded px-2 py-1.5 text-sm"
          style={{ background: "var(--surface-2)", border: "1px solid var(--border)" }}
          value={fault}
          onChange={(e) => setFault(Number(e.target.value))}
        >
          <option value={0}>No injected faults</option>
          <option value={0.1}>10% primary-model faults</option>
          <option value={1}>Primary model down</option>
        </select>
        <button
          type="button"
          data-testid="run-button"
          disabled={!current || busy}
          onClick={() => current && onRun(current.id, mode, fault)}
          className="focus-ring rounded px-3 py-1.5 text-sm font-semibold disabled:opacity-50"
          style={{ background: "var(--accent)", color: "#fff" }}
        >
          {busy ? "Running…" : "Run"}
        </button>
      </div>
      {current && (
        <p className="secondary mt-2 text-sm" data-testid="scenario-question">
          <span className="muted mr-1 text-xs uppercase">{current.kind}</span>
          {current.question}
        </p>
      )}
      {current?.team_radio && (
        <p className="mt-1 text-sm" data-testid="scenario-radio">
          <span className="muted mr-1 text-xs uppercase">Team radio</span>
          {current.team_radio}
        </p>
      )}
    </div>
  );
}

const PRESETS = [
  "VIRTUAL SAFETY CAR DEPLOYED",
  "SAFETY CAR DEPLOYED. PIT ENTRY CLOSED",
  "RED FLAG",
  "FIA STEWARDS: 5 SECOND TIME PENALTY FOR CAR {car} - CAUSING A COLLISION",
];

export function RaceControlConsole({
  drivers,
  selected,
  onSelect,
  onTrigger,
  busy,
  result,
}: {
  drivers: Snapshot[];
  selected: number | null;
  onSelect: (n: number) => void;
  onTrigger: (message: string, driver: number) => void;
  busy: boolean;
  result: string | null;
}) {
  const [msg, setMsg] = useState(PRESETS[0]);
  const car = selected ?? drivers[0]?.driver_number ?? null;
  const resolved = car != null ? msg.replace("{car}", String(car)) : msg;
  return (
    <div data-testid="race-control-console">
      <SectionTitle title="Race control" sub="Publishes to Kafka f1.race_control, then runs the agent on the car's live state" />
      <div className="flex flex-wrap gap-2">
        <select
          aria-label="Race-control message"
          data-testid="rc-message"
          className="focus-ring min-w-0 flex-1 rounded px-2 py-1.5 text-sm"
          style={{ background: "var(--surface-2)", border: "1px solid var(--border)" }}
          value={msg}
          onChange={(e) => setMsg(e.target.value)}
        >
          {PRESETS.map((p) => (
            <option key={p} value={p}>{p.replace("{car}", "N")}</option>
          ))}
        </select>
        <select
          aria-label="Car"
          data-testid="rc-car"
          className="focus-ring rounded px-2 py-1.5 text-sm"
          style={{ background: "var(--surface-2)", border: "1px solid var(--border)" }}
          value={car ?? ""}
          onChange={(e) => onSelect(Number(e.target.value))}
        >
          {drivers.map((d) => (
            <option key={d.driver_number} value={d.driver_number}>P{d.position} {d.driver_code}</option>
          ))}
        </select>
        <button
          type="button"
          data-testid="rc-trigger"
          disabled={car == null || busy}
          onClick={() => car != null && onTrigger(resolved, car)}
          className="focus-ring rounded px-3 py-1.5 text-sm font-semibold disabled:opacity-50"
          style={{ border: "1px solid var(--accent)" }}
        >
          Trigger
        </button>
      </div>
      {result && <p className="secondary mt-2 text-xs" data-testid="rc-result">{result}</p>}
    </div>
  );
}

export function Feeds({ triggers, raceControl }: { triggers: StreamTrigger[]; raceControl: RaceControlMsg[] }) {
  return (
    <div className="grid gap-4 md:grid-cols-2">
      <div>
        <SectionTitle title="Stream triggers" sub="Detected by the Go processor" />
        <ul className="space-y-1 text-xs" data-testid="trigger-feed">
          {triggers.length === 0 && <li className="muted">none yet</li>}
          {triggers.slice(0, 8).map((t) => (
            <li key={`${t.ts_ms}-${t.driver_number}-${t.kind}`}>
              <span className="font-medium">{KIND_LABEL[t.kind] ?? t.kind}</span>
              <span className="muted"> · L{t.lap_number} · </span>
              <span className="secondary">{t.detail}</span>
            </li>
          ))}
        </ul>
      </div>
      <div>
        <SectionTitle title="Race control" sub="FIA messages consumed from Kafka" />
        <ul className="space-y-1 text-xs" data-testid="rc-feed">
          {raceControl.length === 0 && <li className="muted">none yet</li>}
          {raceControl.slice(0, 8).map((m) => (
            <li key={`${m.ts_ms}-${m.message}`} className="flex items-start gap-2">
              {m.parsed?.strategic && <StatusPill kind="warning" label={m.parsed.kind.replaceAll("_", " ").toLowerCase()} />}
              <span className="secondary">L{m.lap_number} · {m.message}</span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
