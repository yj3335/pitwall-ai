package stream

import (
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"
)

// MaxDriverNumber bounds permanent F1 car numbers (1..99).
const MaxDriverNumber = 100

// Trigger kinds emitted by the detector.
const (
	TriggerUndercut  = "UNDERCUT_WINDOW"
	TriggerTireCliff = "TIRE_CLIFF"
	TriggerCheapPit  = "CHEAP_PIT_WINDOW"
	TriggerWetCross  = "WET_CROSSOVER"
)

// Detector thresholds.
const (
	UndercutIntervalMs   = 2300 // car ahead within pit-stop striking distance
	UndercutRivalDegMs   = 95.0 // rival's fuel-corrected degradation, ms/lap
	CliffSlopeMsPerLap   = 150.0
	CliffDropMs          = 800.0 // latest clean lap vs best lap in the window
	DryingInterSlopeMs   = 150.0 // inters overheating on a drying track
	TriggerCooldownLaps  = 3
	minLapsForDegSignals = 3
)

// Snapshot is an immutable view of one driver, published through an
// atomic.Pointer so any goroutine can read it without locks.
type Snapshot struct {
	SessionKey       int32       `json:"session_key"`
	DriverNumber     int32       `json:"driver_number"`
	DriverCode       string      `json:"driver_code"`
	LapNumber        int32       `json:"lap_number"`
	Position         int32       `json:"position"`
	Compound         string      `json:"compound"`
	TyreAgeLaps      int32       `json:"tyre_age_laps"`
	LastLapMs        int64       `json:"last_lap_ms"`
	GapToLeaderMs    int64       `json:"gap_to_leader_ms"`
	IntervalAheadMs  int64       `json:"interval_ahead_ms"`
	IntervalBehindMs int64       `json:"interval_behind_ms"`
	DegSlopeMsPerLap *float64    `json:"deg_slope_ms_per_lap"`
	WindowLaps       []LapSample `json:"window"`
	PitStops         int         `json:"pit_stops"`
	CompoundsUsed    []string    `json:"compounds_used"`
	SpeedKph         int32       `json:"speed_kph"`
	TrackStatus      string      `json:"track_status"`
	Rainfall         int32       `json:"rainfall"`
	TrackTempC       float64     `json:"track_temp_c"`
	TsMs             int64       `json:"ts_ms"`
}

// Deg returns the degradation slope or NaN when not yet known.
func (s *Snapshot) Deg() float64 {
	if s == nil || s.DegSlopeMsPerLap == nil {
		return math.NaN()
	}
	return *s.DegSlopeMsPerLap
}

// Board is the shared, lock-free live state: one atomic slot per car number
// plus a position -> car index. Writers are the partition goroutines (each
// car is written by exactly one of them); readers are anything else.
type Board struct {
	drivers [MaxDriverNumber]atomic.Pointer[Snapshot]
	byPos   [MaxDriverNumber]atomic.Int32
	events  atomic.Int64
}

// NewBoard returns an empty board.
func NewBoard() *Board { return &Board{} }

// Driver returns the latest snapshot for a car, or nil.
func (b *Board) Driver(n int32) *Snapshot {
	if n <= 0 || n >= MaxDriverNumber {
		return nil
	}
	return b.drivers[n].Load()
}

// AtPosition returns the car currently at a race position, or nil.
func (b *Board) AtPosition(pos int32) *Snapshot {
	if pos <= 0 || pos >= MaxDriverNumber {
		return nil
	}
	s := b.Driver(b.byPos[pos].Load())
	if s == nil || s.Position != pos {
		return nil
	}
	return s
}

// All returns every known car ordered by position.
func (b *Board) All() []*Snapshot {
	out := make([]*Snapshot, 0, 20)
	for i := range b.drivers {
		if s := b.drivers[i].Load(); s != nil {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Position < out[j].Position })
	return out
}

// Events is the number of events applied to the board.
func (b *Board) Events() int64 { return b.events.Load() }

func (b *Board) publish(s *Snapshot) {
	b.drivers[s.DriverNumber].Store(s)
	if s.Position > 0 && s.Position < MaxDriverNumber {
		b.byPos[s.Position].Store(s.DriverNumber)
	}
	b.events.Add(1)
}

// LapRecord is one completed lap with its window statistics.
type LapRecord struct {
	SessionKey       int32    `json:"session_key" parquet:"session_key"`
	DriverNumber     int32    `json:"driver_number" parquet:"driver_number"`
	DriverCode       string   `json:"driver_code" parquet:"driver_code,dict"`
	LapNumber        int32    `json:"lap_number" parquet:"lap_number"`
	Compound         string   `json:"compound" parquet:"compound,dict"`
	TyreAgeLaps      int32    `json:"tyre_age_laps" parquet:"tyre_age_laps"`
	Position         int32    `json:"position" parquet:"position"`
	LapTimeMs        int64    `json:"lap_time_ms" parquet:"lap_time_ms"`
	FuelCorrectedMs  int64    `json:"fuel_corrected_ms" parquet:"fuel_corrected_ms"`
	DegSlopeMsPerLap *float64 `json:"deg_slope_ms_per_lap" parquet:"deg_slope_ms_per_lap,optional"`
	WindowLaps       int32    `json:"window_laps" parquet:"window_laps"`
	GapToLeaderMs    int64    `json:"gap_to_leader_ms" parquet:"gap_to_leader_ms"`
	IntervalAheadMs  int64    `json:"interval_ahead_ms" parquet:"interval_ahead_ms"`
	IntervalBehindMs int64    `json:"interval_behind_ms" parquet:"interval_behind_ms"`
	TrackStatus      string   `json:"track_status" parquet:"track_status,dict"`
	IsClean          bool     `json:"is_clean" parquet:"is_clean"`
	TsMs             int64    `json:"ts_ms" parquet:"ts_ms"`
}

// Trigger is a detected strategy opportunity or risk.
type Trigger struct {
	TsMs              int64   `json:"ts_ms" parquet:"ts_ms"`
	DetectedMs        int64   `json:"detected_ms" parquet:"detected_ms"`
	ProducedMs        int64   `json:"produced_ms" parquet:"produced_ms"`
	SessionKey        int32   `json:"session_key" parquet:"session_key"`
	DriverNumber      int32   `json:"driver_number" parquet:"driver_number"`
	DriverCode        string  `json:"driver_code" parquet:"driver_code,dict"`
	LapNumber         int32   `json:"lap_number" parquet:"lap_number"`
	RivalDriverNumber int32   `json:"rival_driver_number" parquet:"rival_driver_number"`
	Kind              string  `json:"kind" parquet:"kind,dict"`
	Detail            string  `json:"detail" parquet:"detail"`
	Value             float64 `json:"value" parquet:"value"`
}

type driverState struct {
	lap           int32
	lapPitOut     bool
	lapNeutral    bool
	prevNeutral   bool // previous lap was under SC/VSC/RED: this lap is a restart lap
	lapRain       bool
	lapStatus     string
	compound      string
	tyreAge       int32
	window        DegWindow
	windowCopy    []LapSample
	pitStops      int
	compounds     []string
	lastLapMs     int64
	lastTrigger   map[string]int32
	neutralPeriod string // SC/VSC/RED period already signalled
	rainSignalled bool
	slope         *float64
}

// Engine is the per-partition stream processor. It is NOT safe for
// concurrent use: each Kafka partition gets its own Engine, and since a car
// always maps to one partition, a car's state is touched by one goroutine.
type Engine struct {
	board  *Board
	states map[int32]*driverState
	Now    func() int64
}

// NewEngine creates an engine writing snapshots to board.
func NewEngine(board *Board) *Engine {
	return &Engine{
		board:  board,
		states: make(map[int32]*driverState),
		Now:    func() int64 { return time.Now().UnixMilli() },
	}
}

// Result is what one event produced.
type Result struct {
	Lap      *LapRecord
	Triggers []Trigger
}

// Process applies one event, returning a completed lap record (when the car
// crossed the line) and any triggers that fired.
func (e *Engine) Process(ev Event) Result {
	var res Result
	st := e.states[ev.DriverNumber]
	if st == nil {
		st = &driverState{lap: ev.LapNumber, compound: ev.Compound, tyreAge: ev.TyreAgeLaps,
			lapPitOut: ev.IsPitOutLap, lapStatus: ev.TrackStatus, lastTrigger: map[string]int32{}}
		st.window.Reset(ev.LapNumber - ev.TyreAgeLaps)
		st.compounds = appendUnique(nil, ev.Compound)
		e.states[ev.DriverNumber] = st
	}

	if ev.LapNumber > st.lap {
		res.Lap = e.completeLap(st, ev, &res)
	}

	// per-event state
	if Neutralised(ev.TrackStatus) {
		st.lapNeutral = true
	}
	if ev.Rainfall == 1 {
		st.lapRain = true
	}
	if ev.TrackStatus != StatusGreen {
		st.lapStatus = ev.TrackStatus
	}
	if ev.LapTimeMs > 0 {
		st.lastLapMs = ev.LapTimeMs
	}

	snap := e.snapshot(st, ev)
	e.board.publish(snap)
	e.eventTriggers(st, ev, snap, &res)
	return res
}

func (e *Engine) completeLap(st *driverState, ev Event, res *Result) *LapRecord {
	done := st.lap
	inLap := ev.IsPitOutLap // the lap before an out-lap is an in-lap
	// an UNKNOWN compound is missing data, never evidence of a tyre change
	newStint := ev.Compound != "UNKNOWN" && (ev.Compound != st.compound || ev.TyreAgeLaps < st.tyreAge)
	lapTime := ev.LapTimeMs
	// lap 1 is a standing start and the first lap after a neutralisation is a
	// restart: both are seconds off representative pace, not tyre wear
	clean := lapTime > 0 && done > 1 && !st.lapPitOut && !inLap && !st.lapNeutral && !st.prevNeutral &&
		ev.LapNumber == done+1
	fc := st.window.FuelCorrect(done, lapTime)
	if clean && st.window.IsOutlier(fc) {
		clean = false
	}
	if clean {
		st.window.Push(done, lapTime)
		st.windowCopy = st.window.Samples()
		if s := st.window.Slope(); !math.IsNaN(s) {
			v := s
			st.slope = &v
		} else {
			st.slope = nil
		}
		e.lapTriggers(st, ev, done, fc, res)
	}
	status := st.lapStatus
	if status == "" {
		status = StatusGreen
	}
	rec := &LapRecord{
		SessionKey: ev.SessionKey, DriverNumber: ev.DriverNumber, DriverCode: ev.DriverCode,
		LapNumber: done, Compound: st.compound, TyreAgeLaps: st.tyreAge, Position: ev.Position,
		LapTimeMs: lapTime, FuelCorrectedMs: int64(math.Round(fc)),
		WindowLaps: int32(st.window.Len()), GapToLeaderMs: ev.GapToLeaderMs,
		IntervalAheadMs: ev.IntervalAheadMs, IntervalBehindMs: ev.IntervalBehindMs,
		TrackStatus: status, IsClean: clean, TsMs: ev.TsMs,
	}
	if clean {
		rec.DegSlopeMsPerLap = st.slope
	}

	if newStint {
		st.pitStops++
		st.compound = ev.Compound
		st.window.Reset(ev.LapNumber - ev.TyreAgeLaps)
		st.windowCopy = nil
		st.slope = nil
		st.compounds = appendUnique(st.compounds, ev.Compound)
	}
	st.lap = ev.LapNumber
	st.tyreAge = ev.TyreAgeLaps
	st.lapPitOut = ev.IsPitOutLap
	st.prevNeutral = st.lapNeutral
	st.lapNeutral = false
	st.lapRain = false
	st.lapStatus = ev.TrackStatus
	if !Neutralised(ev.TrackStatus) {
		st.neutralPeriod = ""
	}
	return rec
}

func (e *Engine) lapTriggers(st *driverState, ev Event, lap int32, fc float64, res *Result) {
	if st.window.Len() < minLapsForDegSignals || st.slope == nil {
		return
	}
	slope := *st.slope
	best := math.Inf(1)
	for _, s := range st.windowCopy {
		best = math.Min(best, s.FuelCorrectedMs)
	}
	// rain slows slicks across the whole field; that is a crossover signal,
	// not a tyre cliff
	if slope >= CliffSlopeMsPerLap && fc-best >= CliffDropMs && !st.lapRain {
		e.fire(st, ev, res, TriggerTireCliff, 0, slope,
			fmt.Sprintf("%s on %s age %d: fuel-corrected deg %.0f ms/lap, lap %d is %.0f ms off window best",
				ev.DriverCode, st.compound, st.tyreAge, slope, lap, fc-best))
	}
	if (st.compound == "INTERMEDIATE" || st.compound == "WET") && !st.lapRain && slope >= DryingInterSlopeMs {
		e.fire(st, ev, res, TriggerWetCross, 0, slope,
			fmt.Sprintf("%s on %s with no rainfall for a full lap and wet-tyre deg %.0f ms/lap: track drying, slick crossover",
				ev.DriverCode, st.compound, slope))
	}
}

func (e *Engine) eventTriggers(st *driverState, ev Event, snap *Snapshot, res *Result) {
	// SC / VSC: discounted pit-lane loss for every car, once per period
	if (ev.TrackStatus == StatusSC || ev.TrackStatus == StatusVSC) && st.neutralPeriod != ev.TrackStatus {
		st.neutralPeriod = ev.TrackStatus
		e.fire(st, ev, res, TriggerCheapPit, 0, 0,
			fmt.Sprintf("%s deployed: %s can pit at reduced time loss (on %s, age %d)",
				ev.TrackStatus, ev.DriverCode, ev.Compound, ev.TyreAgeLaps))
	}
	// rain on slicks
	if ev.Rainfall == 1 && IsSlick(ev.Compound) && !st.rainSignalled {
		st.rainSignalled = true
		e.fire(st, ev, res, TriggerWetCross, 0, 1,
			fmt.Sprintf("rainfall detected with %s on %s slicks: evaluate intermediates", ev.DriverCode, ev.Compound))
	}
	if ev.Rainfall == 0 && IsSlick(ev.Compound) {
		st.rainSignalled = false
	}
	// undercut: close behind a rival whose tyres are going off
	if ev.TrackStatus != StatusGreen || ev.IntervalAheadMs < 0 || ev.IntervalAheadMs >= UndercutIntervalMs || ev.Position <= 1 {
		return
	}
	rival := e.board.AtPosition(ev.Position - 1)
	if rival == nil || rival.SessionKey != ev.SessionKey || rival.DriverNumber == ev.DriverNumber {
		return
	}
	if d := rival.Deg(); !math.IsNaN(d) && d > UndercutRivalDegMs {
		e.fire(st, ev, res, TriggerUndercut, rival.DriverNumber, d,
			fmt.Sprintf("%s is %.1fs behind %s whose %s (age %d) is degrading %.0f ms/lap: undercut window open",
				ev.DriverCode, float64(ev.IntervalAheadMs)/1000, rival.DriverCode, rival.Compound, rival.TyreAgeLaps, d))
	}
	_ = snap
}

func (e *Engine) fire(st *driverState, ev Event, res *Result, kind string, rival int32, value float64, detail string) {
	if last, ok := st.lastTrigger[kind]; ok && kind != TriggerCheapPit && ev.LapNumber-last < TriggerCooldownLaps {
		return
	}
	st.lastTrigger[kind] = ev.LapNumber
	res.Triggers = append(res.Triggers, Trigger{
		TsMs: ev.TsMs, DetectedMs: e.Now(), ProducedMs: ev.IngestMs, SessionKey: ev.SessionKey,
		DriverNumber: ev.DriverNumber, DriverCode: ev.DriverCode, LapNumber: ev.LapNumber,
		RivalDriverNumber: rival, Kind: kind, Detail: detail, Value: value,
	})
}

func (e *Engine) snapshot(st *driverState, ev Event) *Snapshot {
	return &Snapshot{
		SessionKey: ev.SessionKey, DriverNumber: ev.DriverNumber, DriverCode: ev.DriverCode,
		LapNumber: ev.LapNumber, Position: ev.Position, Compound: ev.Compound, TyreAgeLaps: ev.TyreAgeLaps,
		LastLapMs: st.lastLapMs, GapToLeaderMs: ev.GapToLeaderMs, IntervalAheadMs: ev.IntervalAheadMs,
		IntervalBehindMs: ev.IntervalBehindMs, DegSlopeMsPerLap: st.slope, WindowLaps: st.windowCopy,
		PitStops: st.pitStops, CompoundsUsed: st.compounds, SpeedKph: ev.SpeedKph,
		TrackStatus: ev.TrackStatus, Rainfall: ev.Rainfall, TrackTempC: ev.TrackTempC, TsMs: ev.TsMs,
	}
}

func appendUnique(xs []string, s string) []string {
	for _, x := range xs {
		if x == s {
			return xs
		}
	}
	// copy-on-write: published snapshots keep pointing at the old slice
	out := make([]string, len(xs), len(xs)+1)
	copy(out, xs)
	return append(out, s)
}

// PartitionKey implements the lakehouse hive partition (session=<key>).
func (r LapRecord) PartitionKey() int32 { return r.SessionKey }

// PartitionKey implements the lakehouse hive partition (session=<key>).
func (t Trigger) PartitionKey() int32 { return t.SessionKey }
