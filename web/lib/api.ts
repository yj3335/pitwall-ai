"use client";

import { useEffect, useRef, useState } from "react";
import { emptyRun, reduceRun, type RunView } from "./run";
import type { RaceControlMsg, RunEvent, RunStart, ScenarioSummary, ServiceStatus, StreamTrigger, Tower } from "./types";

export const API = (process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8080").replace(/\/$/, "");

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`${API}${path}`, { ...init, headers: { "content-type": "application/json", ...(init?.headers ?? {}) } });
  if (!r.ok) {
    let detail = r.statusText;
    try {
      const body = await r.json();
      detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail ?? body);
    } catch {
      /* keep statusText */
    }
    throw new Error(`${r.status}: ${detail}`);
  }
  return r.json() as Promise<T>;
}

export const api = {
  status: () => json<ServiceStatus>("/api/status"),
  scenarios: () => json<ScenarioSummary[]>("/api/scenarios"),
  startRun: (body: { scenario_id: string; mode: "agent" | "single_pass"; fault_rate: number }) =>
    json<RunStart>("/api/strategy/runs", { method: "POST", body: JSON.stringify(body) }),
  trigger: (body: { message: string; driver_number?: number; lap_number?: number; run_agent: boolean }) =>
    json<{ event: Record<string, unknown>; published: RaceControlMsg | null; run: RunStart | null }>("/api/race-control/trigger", {
      method: "POST",
      body: JSON.stringify(body),
    }),
};

/** Poll /api/status. */
export function useStatus(intervalMs = 5000) {
  const [status, setStatus] = useState<ServiceStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    const tick = () =>
      api
        .status()
        .then((s) => live && (setStatus(s), setError(null)))
        .catch((e: Error) => live && setError(e.message));
    tick();
    const t = setInterval(tick, intervalMs);
    return () => {
      live = false;
      clearInterval(t);
    };
  }, [intervalMs]);
  return { status, error };
}

/** Follow one run's SSE event log; resumes from the last event id on reconnect. */
export function useRun(runId: string | null): RunView {
  const [view, setView] = useState<RunView>(emptyRun());
  useEffect(() => {
    setView(emptyRun());
    if (!runId) return;
    const es = new EventSource(`${API}/api/strategy/runs/${runId}/events`);
    const onEvent = (msg: MessageEvent) => {
      const ev = JSON.parse(msg.data) as RunEvent;
      setView((v) => reduceRun(v, ev));
      if (ev.type === "done") es.close();
    };
    for (const t of ["queued", "started", "node", "final", "error", "done"]) es.addEventListener(t, onEvent as EventListener);
    return () => es.close();
  }, [runId]);
  return view;
}

const MAX_FEED = 40;
const FRESH_MS = 5000;

/** Live tower + trigger + race-control feed over /api/live/stream. */
export function useLive() {
  const [tower, setTower] = useState<Tower | null>(null);
  const [triggers, setTriggers] = useState<StreamTrigger[]>([]);
  const [raceControl, setRaceControl] = useState<RaceControlMsg[]>([]);
  // Freshness, not socket state: "live" means a tower update arrived recently.
  const [lastUpdate, setLastUpdate] = useState<number | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const rate = useRef<{ t: number; events: number } | null>(null);
  const [eventsPerSec, setEventsPerSec] = useState<number | null>(null);
  useEffect(() => {
    const tick = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(tick);
  }, []);
  useEffect(() => {
    const es = new EventSource(`${API}/api/live/stream?interval_ms=1000`);
    es.addEventListener("tower", (m) => {
      const t = JSON.parse((m as MessageEvent).data) as Tower;
      const now = Date.now();
      setLastUpdate(now);
      const prev = rate.current;
      if (prev && now > prev.t) setEventsPerSec(((t.stats.events - prev.events) * 1000) / (now - prev.t));
      rate.current = { t: now, events: t.stats.events };
      setTower(t);
    });
    // a reconnect opens a new server-side stream that resends recent items: de-duplicate by identity
    es.addEventListener("trigger", (m) => {
      const t = JSON.parse((m as MessageEvent).data) as StreamTrigger;
      setTriggers((xs) =>
        xs.some((x) => x.ts_ms === t.ts_ms && x.driver_number === t.driver_number && x.kind === t.kind) ? xs : [t, ...xs].slice(0, MAX_FEED),
      );
    });
    es.addEventListener("race_control", (m) => {
      const r = JSON.parse((m as MessageEvent).data) as RaceControlMsg;
      setRaceControl((xs) => (xs.some((x) => x.ts_ms === r.ts_ms && x.message === r.message) ? xs : [r, ...xs].slice(0, MAX_FEED)));
    });
    return () => es.close();
  }, []);
  const freshness: "connecting" | "live" | "stale" = lastUpdate == null ? "connecting" : now - lastUpdate <= FRESH_MS ? "live" : "stale";
  return { tower, triggers, raceControl, freshness, eventsPerSec };
}
