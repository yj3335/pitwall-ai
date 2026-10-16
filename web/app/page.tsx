"use client";

import { useEffect, useState } from "react";
import { StatTile, StatusPill } from "@/components/Bits";
import { Feeds, RaceControlConsole, ScenarioPanel } from "@/components/Controls";
import { StrategyChart } from "@/components/StrategyChart";
import { TowerTable } from "@/components/Tower";
import { Citations, FinalCallCard, TraceTimeline } from "@/components/Trace";
import { TrafficCard, UndercutChart } from "@/components/Undercut";
import { api, useLive, useRun, useStatus } from "@/lib/api";
import { compact, fmtMs } from "@/lib/format";
import type { ScenarioSummary } from "@/lib/types";

export default function Dashboard() {
  const { status, error: statusError } = useStatus();
  const { tower, triggers, raceControl, freshness, eventsPerSec } = useLive();
  const [scenarios, setScenarios] = useState<ScenarioSummary[]>([]);
  const [runId, setRunId] = useState<string | null>(null);
  const [runError, setRunError] = useState<string | null>(null);
  const [selected, setSelected] = useState<number | null>(null);
  const [rcResult, setRcResult] = useState<string | null>(null);
  const run = useRun(runId);
  const busy = run.status === "queued" || run.status === "running";

  useEffect(() => {
    api.scenarios().then(setScenarios).catch((e: Error) => setRunError(e.message));
  }, []);

  const startRun = async (id: string, mode: "agent" | "single_pass", faultRate: number) => {
    setRunError(null);
    try {
      const r = await api.startRun({ scenario_id: id, mode, fault_rate: faultRate });
      setRunId(r.run_id);
    } catch (e) {
      setRunError((e as Error).message);
    }
  };

  const trigger = async (message: string, driver: number) => {
    setRcResult(null);
    try {
      const r = await api.trigger({ message, driver_number: driver, lap_number: tower?.drivers[0]?.lap_number, run_agent: true });
      setRcResult(`${r.published ? "Published to Kafka" : "Parsed"}: ${message}${r.run ? ` · run ${r.run.run_id}` : ""}`);
      if (r.run) setRunId(r.run.run_id);
    } catch (e) {
      setRcResult((e as Error).message);
    }
  };

  const stats = tower?.stats;
  const services: [string, boolean | undefined][] = [
    ["Go MCP", status?.mcp_sim],
    ["Processor", status?.processor],
    ["Qdrant", status?.qdrant],
    ["Ollama", status?.ollama],
  ];

  return (
    <main className="mx-auto max-w-[1600px] space-y-4 px-4 py-4">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-lg font-semibold">PitWall AI</h1>
          <p className="muted text-xs">
            {status ? `${status.models.primary} · ${status.models.verifier} · ${status.models.embedding} on local Ollama` : "connecting to the control plane…"}
          </p>
        </div>
        <div className="flex flex-wrap gap-2" data-testid="services">
          {statusError && <StatusPill kind="critical" label="control plane unreachable" />}
          {services.map(([name, up]) => (
            <StatusPill key={name} kind={up === undefined ? "warning" : up ? "good" : "critical"} label={name} />
          ))}
          <StatusPill
            kind={freshness === "live" ? "good" : "warning"}
            label={freshness === "live" ? "live stream" : freshness === "stale" ? "stream stale" : "connecting"}
            testId="live-pill"
          />
        </div>
      </header>

      <section className="grid grid-cols-2 gap-3 md:grid-cols-5" aria-label="Stream health">
        <StatTile label="Events processed" value={stats ? compact(stats.events) : "–"} sub="Kafka → Go processor" testId="kpi-events" />
        <StatTile label="Throughput" value={eventsPerSec != null ? `${compact(eventsPerSec)}/s` : "–"} sub="events per second, live" />
        <StatTile label="Trigger latency p99" value={stats?.trigger_latency.count ? fmtMs(stats.trigger_latency.p99_ms) : "–"} sub="produce → detection" />
        <StatTile label="Strategy triggers" value={stats ? compact(stats.triggers) : "–"} sub={stats ? `${compact(stats.laps)} laps completed` : undefined} />
        <StatTile label="Lakehouse" value={stats ? `${compact(stats.parquet_files)} files` : "–"} sub={stats ? `${compact(stats.parquet_rows)} rows · ${stats.sink_errors} errors` : undefined} />
      </section>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1.15fr)]">
        <div className="space-y-4">
          <section className="card p-4">
            {tower?.drivers.length ? (
              <TowerTable drivers={tower.drivers} selected={selected} onSelect={setSelected} />
            ) : (
              <p className="muted text-sm">No live session. Start the stack and replay a race: <code>make run && make replay SESSION=9165</code></p>
            )}
          </section>
          <section className="card p-4">
            <RaceControlConsole drivers={tower?.drivers ?? []} selected={selected} onSelect={setSelected} onTrigger={trigger} busy={busy} result={rcResult} />
          </section>
          <section className="card p-4">
            <Feeds triggers={triggers} raceControl={raceControl} />
          </section>
        </div>

        <div className="space-y-4">
          <section className="card p-4">
            <ScenarioPanel scenarios={scenarios} busy={busy} onRun={startRun} />
            {runError && <p className="mt-2 text-xs" style={{ color: "var(--critical)" }} role="alert">{runError}</p>}
          </section>
          <FinalCallCard run={run} />
          {run.error && <p className="card p-3 text-sm" role="alert" style={{ color: "var(--critical)" }}>{run.error}</p>}
          <section className="card p-4">
            <TraceTimeline run={run} />
          </section>
          {run.simulation && (
            <section className="card p-4">
              <StrategyChart rows={run.simulation.table} recommended={run.simulation.recommended} rollouts={run.simulation.rollouts} />
            </section>
          )}
          {run.simulation?.undercut && (
            <section className="card p-4">
              <UndercutChart undercut={run.simulation.undercut} />
            </section>
          )}
          {run.simulation?.traffic && (
            <section className="card p-4">
              <TrafficCard traffic={run.simulation.traffic} />
            </section>
          )}
          {run.hits.length > 0 && (
            <section className="card p-4">
              <Citations hits={run.hits} constraints={run.constraints} />
            </section>
          )}
        </div>
      </div>
    </main>
  );
}
