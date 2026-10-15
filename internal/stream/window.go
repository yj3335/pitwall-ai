package stream

import (
	"math"
	"sort"
)

// Model constants for the degradation window.
const (
	// FuelEffectMsPerLap is the lap-time gain from burning fuel (~1.1 kg/lap at
	// ~0.03 s/kg). Raw lap times fall by this much every lap even on constant
	// rubber, so it is added back to isolate true tyre wear.
	FuelEffectMsPerLap = 35.0
	// WindowLaps is the rolling window used for the degradation slope.
	WindowLaps = 5
	// OutlierRatio rejects laps slower than 107% of the window median
	// (traffic, lock-ups, yellow flags).
	OutlierRatio = 1.07
)

// LapSample is one completed, clean lap inside a stint.
type LapSample struct {
	Lap             int32
	LapTimeMs       int64
	FuelCorrectedMs float64
}

// DegWindow is a fixed-size ring of the last WindowLaps clean laps of the
// current stint. It is owned by exactly one goroutine; no locking.
type DegWindow struct {
	laps      [WindowLaps]LapSample
	n         int
	head      int
	stintLap0 int32 // first lap of the current stint (fuel correction origin)
}

// Reset starts a new stint at lap0.
func (w *DegWindow) Reset(lap0 int32) {
	*w = DegWindow{stintLap0: lap0}
}

// Len is the number of laps held.
func (w *DegWindow) Len() int { return w.n }

// FuelCorrect returns the lap time with the fuel burn-off effect removed,
// referenced to the start of the stint.
func (w *DegWindow) FuelCorrect(lap int32, lapTimeMs int64) float64 {
	return float64(lapTimeMs) + FuelEffectMsPerLap*float64(lap-w.stintLap0)
}

// Median fuel-corrected time in the window (0 when empty).
func (w *DegWindow) Median() float64 {
	if w.n == 0 {
		return 0
	}
	v := make([]float64, 0, w.n)
	for i := 0; i < w.n; i++ {
		v = append(v, w.laps[i].FuelCorrectedMs)
	}
	sort.Float64s(v)
	if w.n%2 == 1 {
		return v[w.n/2]
	}
	return (v[w.n/2-1] + v[w.n/2]) / 2
}

// IsOutlier reports whether a fuel-corrected lap is >107% of the window median.
func (w *DegWindow) IsOutlier(fuelCorrected float64) bool {
	if w.n < 2 {
		return false
	}
	return fuelCorrected > w.Median()*OutlierRatio
}

// Push adds a clean lap, evicting the oldest when full.
func (w *DegWindow) Push(lap int32, lapTimeMs int64) LapSample {
	s := LapSample{Lap: lap, LapTimeMs: lapTimeMs, FuelCorrectedMs: w.FuelCorrect(lap, lapTimeMs)}
	w.laps[w.head] = s
	w.head = (w.head + 1) % WindowLaps
	if w.n < WindowLaps {
		w.n++
	}
	return s
}

// Samples returns the window contents oldest-first.
func (w *DegWindow) Samples() []LapSample {
	out := make([]LapSample, 0, w.n)
	start := (w.head - w.n + WindowLaps) % WindowLaps
	for i := 0; i < w.n; i++ {
		out = append(out, w.laps[(start+i)%WindowLaps])
	}
	return out
}

// Slope is the ordinary-least-squares slope (ms/lap) of fuel-corrected lap
// time against lap number: the tyre degradation rate. NaN with < 3 laps.
func (w *DegWindow) Slope() float64 {
	if w.n < 3 {
		return math.NaN()
	}
	var sx, sy, sxx, sxy float64
	start := (w.head - w.n + WindowLaps) % WindowLaps
	for i := 0; i < w.n; i++ {
		s := w.laps[(start+i)%WindowLaps]
		x, y := float64(s.Lap), s.FuelCorrectedMs
		sx += x
		sy += y
		sxx += x * x
		sxy += x * y
	}
	n := float64(w.n)
	den := n*sxx - sx*sx
	if den == 0 {
		return math.NaN()
	}
	return (n*sxy - sx*sy) / den
}
