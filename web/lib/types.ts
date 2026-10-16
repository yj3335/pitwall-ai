// Payload shapes of the PitWall control plane (python/pitwall/api).

export type Compound = "SOFT" | "MEDIUM" | "HARD" | "INTERMEDIATE" | "WET" | "UNKNOWN";
export type TrackStatus = "GREEN" | "YELLOW" | "SC" | "VSC" | "RED";

export interface LapSample {
  Lap: number;
  LapTimeMs: number;
  FuelCorrectedMs: number;
}

export interface Snapshot {
  session_key: number;
  driver_number: number;
  driver_code: string;
  lap_number: number;
  position: number;
  compound: Compound;
  tyre_age_laps: number;
  last_lap_ms: number;
  gap_to_leader_ms: number;
  interval_ahead_ms: number;
  interval_behind_ms: number;
  deg_slope_ms_per_lap: number | null;
  window: LapSample[] | null;
  pit_stops: number;
  compounds_used: string[];
  speed_kph: number;
  track_status: TrackStatus;
  rainfall: number;
  track_temp_c: number;
  ts_ms: number;
}

export interface LatencySummary {
  count: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
  mean_ms: number;
  max_ms: number;
}

export interface StreamStats {
  events: number;
  laps: number;
  triggers: number;
  decode_errors: number;
  uptime_s: number;
  events_per_sec: number;
  event_latency: LatencySummary;
  trigger_latency: LatencySummary;
  parquet_files: number;
  parquet_rows: number;
  sink_errors: number;
}

export interface Tower {
  drivers: Snapshot[];
  stats: StreamStats;
}

export interface StreamTrigger {
  ts_ms: number;
  driver_number: number;
  driver_code: string;
  lap_number: number;
  rival_driver_number: number;
  kind: "UNDERCUT_WINDOW" | "TIRE_CLIFF" | "CHEAP_PIT_WINDOW" | "WET_CROSSOVER";
  detail: string;
  value: number;
}

export interface ParsedRaceControl {
  kind: string;
  track_status: TrackStatus | null;
  pit_entry_closed: boolean | null;
  penalty_s: number | null;
  car_number: number | null;
  strategic: boolean;
}

export interface RaceControlMsg {
  ts_ms: number;
  session_key: number;
  lap_number: number;
  message: string;
  parsed?: ParsedRaceControl;
}

export interface ServiceStatus {
  mcp_sim: boolean;
  qdrant: boolean;
  ollama: boolean;
  processor: boolean;
  lakehouse_sessions: number[];
  models: { primary: string; verifier: string; embedding: string };
  runs: { total: number; active: number };
}

export interface ScenarioSummary {
  id: string;
  kind: "real" | "synthetic" | "live";
  category: string;
  title: string;
  question: string;
  team_radio?: string;
}

export interface SimRow {
  strategy: string;
  pit_lap: number;
  compound: string;
  legal: boolean;
  violations: string[];
  expected_delta_s: number;
  p10_p90_s: [number, number];
  expected_finish: number;
  rejoin_position: number;
  p_ahead_of_car_in_front: number;
  notes: string;
}

export interface Undercut {
  mode: string;
  expected_gain_ms: number;
  gap_before_ms: number;
  expected_gap_after_ms: number;
  p_attacker_ahead_after_stops: number;
  lap_deltas_ms: [number, number];
  attacker_penalty_hold_ms: number;
  explanation: string;
}

export interface Traffic {
  valid: boolean;
  rejoin_position: number;
  car_ahead?: string;
  gap_to_car_ahead_ms: number;
  cars_within_1_2s: string[];
  in_drs_train: boolean;
  drs_train: string[];
  verdict: string;
  suggested_clear_air_pit_laps?: number[];
}

export interface RegulationHit {
  id: string;
  citation: string;
  query: string;
  rrf_rank: number;
  rerank_score: number;
  text: string;
}

export interface Violation {
  code: string;
  message: string;
  article: string | null;
  severity: "hard" | "soft";
}

export interface FinalCall {
  recommended_action: string;
  target_compound: string;
  pit_lap: number;
  expected_net_gain_ms: number;
  rejoin_position: number;
  penalty_hold_s: number;
  fia_article_citations: string[];
  engineer_radio_message: string;
  rationale: string;
  guardrail_override?: boolean;
  unresolved_violations?: Violation[];
  reflections?: number;
}

export interface Assessment {
  compliant: boolean;
  violations: Violation[];
  edition: string;
  unresolved_verifier_items: Violation[];
  agrees_with_ground_truth?: boolean;
  acceptable?: string[];
}

export interface TraceEvent {
  node: string;
  ms: number;
  summary: string;
  // node-specific details
  table?: SimRow[];
  rollouts?: number;
  recommended?: string;
  undercut?: Undercut | null;
  traffic?: Traffic | null;
  hits?: RegulationHit[];
  citations?: string[];
  constraints?: string[];
  violations?: Violation[];
  critic?: { ok: boolean; issues: string[]; model: string | null };
  cycle?: number;
  model?: string;
  failover?: boolean;
  sql?: string | null;
  sql_error?: string | null;
  final?: FinalCall;
}

export type RunEventType = "queued" | "started" | "node" | "final" | "error" | "done";

export interface RunEvent {
  id: number;
  type: RunEventType;
  ts_ms: number;
  data: Record<string, unknown>;
}

export interface RunStart {
  run_id: string;
  status: string;
  events_url: string;
}
