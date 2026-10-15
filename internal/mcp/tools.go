package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/yj3335/pitwall-ai/internal/sim"
	"github.com/yj3335/pitwall-ai/internal/stream"
)

const carSchema = `{
  "type": "object",
  "properties": {
    "driver_number": {"type": "integer"},
    "driver_code": {"type": "string"},
    "position": {"type": "integer"},
    "compound": {"type": "string", "enum": ["SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"]},
    "tyre_age_laps": {"type": "integer"},
    "deg_slope_ms_per_lap": {"type": "number", "description": "observed fuel-corrected degradation of the current set"},
    "base_lap_ms": {"type": "number"},
    "compounds_used": {"type": "array", "items": {"type": "string"}},
    "penalty_s": {"type": "number", "description": "pending 5/10 s time penalty served as a stationary hold at the next stop"},
    "gap_to_leader_ms": {"type": "number"}
  },
  "required": ["compound", "tyre_age_laps"]
}`

const fieldSchema = `{
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "driver_number": {"type": "integer"}, "driver_code": {"type": "string"}, "position": {"type": "integer"},
      "gap_to_leader_ms": {"type": "number"}, "compound": {"type": "string"}, "tyre_age_laps": {"type": "integer"},
      "base_lap_ms": {"type": "number"}, "planned_stop_lap": {"type": "integer"}
    }
  }
}`

// RegisterSimTools exposes the simulator (and, when board is non-nil, the
// live Kafka-fed driver windows) as MCP tools.
func RegisterSimTools(s *Server, board *stream.Board) {
	s.AddTool(Tool{
		Name: "simulate_pit_strategies",
		Description: "Monte Carlo race simulation (default 10,000 rollouts per strategy across a goroutine worker pool) comparing " +
			"PIT_NOW_HARD, PIT_NOW_MEDIUM, PIT_NOW_SOFT, PIT_NOW_INTER, STAY_OUT_EXTEND and OPPOSITE_AHEAD. Models green (~21.8s) vs " +
			"SC/VSC (~13.2s) pit loss, 5s/10s stationary penalty holds, out-lap warm-up, Gaussian lap noise, random safety cars, " +
			"DRS dirty-air rejoin penalties and the Article 30.5(m) two-compound rule.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "total_laps": {"type": "integer"}, "current_lap": {"type": "integer"},
    "track_status": {"type": "string", "enum": ["GREEN", "YELLOW", "SC", "VSC", "RED"]},
    "track_wetness": {"type": "number", "minimum": 0, "maximum": 1},
    "wetness_trend_per_lap": {"type": "number"},
    "pit_loss_green_s": {"type": "number"}, "pit_loss_neutral_s": {"type": "number"},
    "pit_entry_closed": {"type": "boolean"},
    "sc_hazard_per_lap": {"type": "number"},
    "overtake_difficulty": {"type": "number", "minimum": 0, "maximum": 1, "description": "0.3 typical, 0.85 Singapore/Monaco"},
    "lap_sigma_ms": {"type": "number"},
    "car": ` + carSchema + `,
    "field": ` + fieldSchema + `,
    "strategies": {"type": "array", "items": {"type": "string"}},
    "rollouts": {"type": "integer", "default": 10000},
    "extend_laps": {"type": "integer"},
    "forbid_compounds": {"type": "array", "items": {"type": "string"}},
    "seed": {"type": "integer"}
  },
  "required": ["total_laps", "current_lap", "car"]
}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var req sim.Request
			if err := json.Unmarshal(args, &req); err != nil {
				return nil, fmt.Errorf("invalid arguments: %w", err)
			}
			return sim.Simulate(req)
		},
	})
	s.AddTool(Tool{
		Name:        "compute_undercut_delta",
		Description: "Exact 2-lap undercut/overcut net time delta between two battling cars, including out-lap warm-up, tyre-age pace and any attacker penalty hold, with the probability the attacker emerges ahead.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "current_lap": {"type": "integer"}, "total_laps": {"type": "integer"},
    "gap_ms": {"type": "number", "description": "attacker is behind the defender by this gap"},
    "attacker": ` + carSchema + `, "defender": ` + carSchema + `,
    "attacker_new_compound": {"type": "string"}, "defender_new_compound": {"type": "string"},
    "pit_loss_s": {"type": "number"},
    "mode": {"type": "string", "enum": ["UNDERCUT", "OVERCUT"]},
    "rollouts": {"type": "integer"}, "seed": {"type": "integer"}
  },
  "required": ["gap_ms", "attacker", "defender"]
}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var req sim.UndercutRequest
			if err := json.Unmarshal(args, &req); err != nil {
				return nil, fmt.Errorf("invalid arguments: %w", err)
			}
			return sim.UndercutDelta(req)
		},
	})
	s.AddTool(Tool{
		Name:        "validate_physics_and_traffic",
		Description: "Deterministically projects the field to a proposed pit lap and reports the rejoin position, cars within 1.2s and whether the car drops into a multi-car DRS train, with clear-air alternatives.",
		InputSchema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "current_lap": {"type": "integer"}, "pit_lap": {"type": "integer"},
    "pit_loss_s": {"type": "number"}, "penalty_s": {"type": "number"}, "new_compound": {"type": "string"},
    "car": ` + carSchema + `, "field": ` + fieldSchema + `
  },
  "required": ["current_lap", "pit_lap", "car", "field"]
}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var req sim.TrafficRequest
			if err := json.Unmarshal(args, &req); err != nil {
				return nil, fmt.Errorf("invalid arguments: %w", err)
			}
			return sim.ValidateTraffic(req)
		},
	})
	s.AddTool(Tool{
		Name:        "get_live_driver_window",
		Description: "In-memory sliding-window telemetry for one driver from the live Kafka stream: last 5 clean laps, fuel-corrected degradation slope, gaps ahead/behind, compounds used.",
		InputSchema: json.RawMessage(`{"type": "object", "properties": {"driver_number": {"type": "integer"}}, "required": ["driver_number"]}`),
		Handler: func(ctx context.Context, args json.RawMessage) (any, error) {
			var a struct {
				DriverNumber int32 `json:"driver_number"`
			}
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, err
			}
			if board == nil {
				return nil, fmt.Errorf("live stream disabled (start mcp-sim with -brokers)")
			}
			start := time.Now()
			snap := board.Driver(a.DriverNumber)
			if snap == nil {
				return nil, fmt.Errorf("no live data for car %d", a.DriverNumber)
			}
			var ahead, behind *stream.Snapshot
			if snap.Position > 1 {
				ahead = board.AtPosition(snap.Position - 1)
			}
			behind = board.AtPosition(snap.Position + 1)
			return map[string]any{
				"driver":      snap,
				"car_ahead":   ahead,
				"car_behind":  behind,
				"lookup_us":   time.Since(start).Microseconds(),
				"events_seen": board.Events(),
			}, nil
		},
	})
}
