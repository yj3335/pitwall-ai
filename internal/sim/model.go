// Package sim is the concurrent Monte Carlo race strategy simulator exposed
// over MCP. It rolls out the remainder of a race thousands of times per
// candidate strategy with common random numbers, so strategies are compared
// on identical luck (same lap-time noise, same safety-car draws).
package sim

import (
	"fmt"
	"math"
	"strings"
)

// Defaults for the race model. Pit losses are relative to staying on track.
const (
	DefaultPitLossGreenS   = 21.8
	DefaultPitLossNeutralS = 13.2 // SC / VSC: the field is slowed, so the stop is cheap
	DefaultLapSigmaMs      = 350.0
	DefaultSCHazardPerLap  = 0.012
	FuelEffectMsPerLap     = 35.0
	DirtyAirMsPerLap       = 450.0 // DRS/dirty-air loss when stuck behind slower traffic
	DirtyAirWindowMs       = 1200.0
	DRSWindowMs            = 1000.0
	SlowStopProb           = 0.03
	SlowStopMs             = 3000.0
	StopNoiseSigmaMs       = 350.0
	DefaultRollouts        = 10_000
	DefaultExtendLaps      = 8
	// DefaultOvertakeDifficulty suits a typical permanent circuit; street
	// circuits such as Singapore or Monaco sit at 0.8-0.95.
	DefaultOvertakeDifficulty = 0.3
	// Wetness levels at which projected rivals switch tyres. The pace model's
	// slick/intermediate crossover is at w = 0.27; rivals react with a margin.
	SlickToInterWetness = 0.35
	InterToSlickWetness = 0.15
)

// Compound is the tyre model for one specification.
type Compound struct {
	PaceMs   float64 // base pace offset vs MEDIUM on a dry track
	DegMs    float64 // ms/lap of wear, fuel-corrected
	CliffLap int     // tyre age where the cliff starts
	CliffMs  float64 // additional ms/lap per lap beyond the cliff
	WarmupMs float64 // out-lap warm-up loss
	Dry      bool
}

// Compounds are calibrated to typical Pirelli C2-C4 behaviour.
var Compounds = map[string]Compound{
	"SOFT":         {PaceMs: -650, DegMs: 85, CliffLap: 20, CliffMs: 350, WarmupMs: 500, Dry: true},
	"MEDIUM":       {PaceMs: 0, DegMs: 55, CliffLap: 30, CliffMs: 250, WarmupMs: 900, Dry: true},
	"HARD":         {PaceMs: 450, DegMs: 35, CliffLap: 42, CliffMs: 200, WarmupMs: 1500, Dry: true},
	"INTERMEDIATE": {PaceMs: 0, DegMs: 60, CliffLap: 35, CliffMs: 200, WarmupMs: 700},
	"WET":          {PaceMs: 0, DegMs: 40, CliffLap: 40, CliffMs: 150, WarmupMs: 900},
}

// wetPenaltyMs is the pace loss of a compound on a track with wetness w in
// [0,1]. Slicks lose up to 12 s on a soaked track, intermediates lose up to
// 4.5 s on a dry one; the inter/slick crossover sits near w = 0.27.
func wetPenaltyMs(compound string, w float64) float64 {
	switch compound {
	case "INTERMEDIATE":
		return (1-w)*4500 + math.Max(0, w-0.75)*8000
	case "WET":
		return (1-w)*7500 + 1000*math.Max(0, 0.75-w)
	default:
		return w * 12000
	}
}

// CarState describes the car being simulated.
type CarState struct {
	DriverNumber     int      `json:"driver_number"`
	DriverCode       string   `json:"driver_code,omitempty"`
	Position         int      `json:"position"`
	Compound         string   `json:"compound"`
	TyreAgeLaps      int      `json:"tyre_age_laps"`
	DegSlopeMsPerLap *float64 `json:"deg_slope_ms_per_lap,omitempty"`
	BaseLapMs        float64  `json:"base_lap_ms"`
	CompoundsUsed    []string `json:"compounds_used"`
	PenaltyS         float64  `json:"penalty_s"` // pending 5/10 s time penalty to serve at the next stop
	GapToLeaderMs    float64  `json:"gap_to_leader_ms"`
}

// FieldCar is another car used for traffic and position projection.
type FieldCar struct {
	DriverNumber   int     `json:"driver_number"`
	DriverCode     string  `json:"driver_code,omitempty"`
	Position       int     `json:"position"`
	GapToLeaderMs  float64 `json:"gap_to_leader_ms"`
	Compound       string  `json:"compound"`
	TyreAgeLaps    int     `json:"tyre_age_laps"`
	BaseLapMs      float64 `json:"base_lap_ms,omitempty"`
	PlannedStopLap int     `json:"planned_stop_lap,omitempty"` // 0 = no further stop expected
	// observed fuel-corrected degradation of the rival's current set (ms/lap)
	DegSlopeMsPerLap *float64 `json:"deg_slope_ms_per_lap,omitempty"`
}

// Request is the input of simulate_pit_strategies.
type Request struct {
	TotalLaps          int        `json:"total_laps"`
	CurrentLap         int        `json:"current_lap"`
	TrackStatus        string     `json:"track_status"`
	TrackWetness       float64    `json:"track_wetness"`
	WetnessTrend       float64    `json:"wetness_trend_per_lap"`
	PitLossGreenS      float64    `json:"pit_loss_green_s"`
	PitLossNeutralS    float64    `json:"pit_loss_neutral_s"`
	PitEntryClosed     bool       `json:"pit_entry_closed"`
	SCHazardPerLap     *float64   `json:"sc_hazard_per_lap,omitempty"`
	OvertakeDifficulty *float64   `json:"overtake_difficulty,omitempty"`
	LapSigmaMs         float64    `json:"lap_sigma_ms"`
	Car                CarState   `json:"car"`
	Field              []FieldCar `json:"field"`
	Strategies         []string   `json:"strategies"`
	Rollouts           int        `json:"rollouts"`
	ExtendLaps         int        `json:"extend_laps"`
	Seed               uint64     `json:"seed"`
	ForbidCompounds    []string   `json:"forbid_compounds,omitempty"` // e.g. no sets left
	RedFlagFreeTyres   bool       `json:"-"`
}

// AllStrategies is the default candidate set.
var AllStrategies = []string{"PIT_NOW_HARD", "PIT_NOW_MEDIUM", "PIT_NOW_SOFT", "PIT_NOW_INTER", "STAY_OUT_EXTEND", "OPPOSITE_AHEAD"}

func (r *Request) normalise() error {
	if r.TotalLaps <= 0 || r.CurrentLap <= 0 || r.CurrentLap > r.TotalLaps {
		return fmt.Errorf("need 0 < current_lap (%d) <= total_laps (%d)", r.CurrentLap, r.TotalLaps)
	}
	r.Car.Compound = canonical(r.Car.Compound)
	if _, ok := Compounds[r.Car.Compound]; !ok {
		return fmt.Errorf("unknown compound %q", r.Car.Compound)
	}
	if r.PitLossGreenS == 0 {
		r.PitLossGreenS = DefaultPitLossGreenS
	}
	if r.PitLossNeutralS == 0 {
		r.PitLossNeutralS = DefaultPitLossNeutralS
	}
	if r.LapSigmaMs == 0 {
		r.LapSigmaMs = DefaultLapSigmaMs
	}
	if r.Rollouts <= 0 {
		r.Rollouts = DefaultRollouts
	}
	if r.Rollouts > 200_000 {
		r.Rollouts = 200_000
	}
	if r.ExtendLaps <= 0 {
		r.ExtendLaps = DefaultExtendLaps
	}
	if r.Car.BaseLapMs == 0 {
		r.Car.BaseLapMs = 95_000
	}
	if r.TrackStatus == "" {
		r.TrackStatus = "GREEN"
	}
	r.TrackStatus = strings.ToUpper(r.TrackStatus)
	if len(r.Strategies) == 0 {
		r.Strategies = AllStrategies
	}
	for i := range r.Car.CompoundsUsed {
		r.Car.CompoundsUsed[i] = canonical(r.Car.CompoundsUsed[i])
	}
	if !contains(r.Car.CompoundsUsed, r.Car.Compound) {
		r.Car.CompoundsUsed = append(r.Car.CompoundsUsed, r.Car.Compound)
	}
	for i := range r.Field {
		r.Field[i].Compound = canonical(r.Field[i].Compound)
		if r.Field[i].BaseLapMs == 0 {
			r.Field[i].BaseLapMs = r.Car.BaseLapMs
		}
	}
	return nil
}

func canonical(c string) string {
	c = strings.ToUpper(strings.TrimSpace(c))
	switch c {
	case "S":
		return "SOFT"
	case "M":
		return "MEDIUM"
	case "H":
		return "HARD"
	case "I", "INTER", "INTERS":
		return "INTERMEDIATE"
	case "W":
		return "WET"
	}
	return c
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
