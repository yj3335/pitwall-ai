package sim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
)

// UndercutRequest is the input of compute_undercut_delta.
type UndercutRequest struct {
	CurrentLap       int      `json:"current_lap"`
	TotalLaps        int      `json:"total_laps"`
	GapMs            float64  `json:"gap_ms"` // attacker behind defender by this much
	Attacker         CarState `json:"attacker"`
	Defender         CarState `json:"defender"`
	AttackerCompound string   `json:"attacker_new_compound"`
	DefenderCompound string   `json:"defender_new_compound"`
	PitLossS         float64  `json:"pit_loss_s"`
	Mode             string   `json:"mode"` // UNDERCUT (attacker stops first) or OVERCUT
	Rollouts         int      `json:"rollouts"`
	LapSigmaMs       float64  `json:"lap_sigma_ms"`
	Seed             uint64   `json:"seed"`
}

// UndercutResult is the 2-lap net delta of the pit sequence.
type UndercutResult struct {
	Mode            string    `json:"mode"`
	ExpectedGainMs  float64   `json:"expected_gain_ms"` // positive: attacker gains time
	GapBeforeMs     float64   `json:"gap_before_ms"`
	GapAfterMs      float64   `json:"expected_gap_after_ms"` // negative: attacker ahead after both stops
	PSuccess        float64   `json:"p_attacker_ahead_after_stops"`
	LapDeltasMs     []float64 `json:"lap_deltas_ms"`
	PenaltyHoldMs   float64   `json:"attacker_penalty_hold_ms"`
	Explanation     string    `json:"explanation"`
	AttackerOutLap  float64   `json:"attacker_out_lap_ms"`
	DefenderOldLap  float64   `json:"defender_old_tyre_lap_ms"`
	AttackerNewLap2 float64   `json:"attacker_second_new_lap_ms"`
	DefenderOutLap  float64   `json:"defender_out_lap_ms"`
}

func carLap(base float64, compound string, age int, deg *float64, wetness float64, outLap bool) float64 {
	m, ok := Compounds[compound]
	if !ok {
		m = Compounds["MEDIUM"]
	}
	d := m.DegMs
	if deg != nil {
		d = math.Max(0, *deg)
	}
	t := base + d*float64(age)
	if m.Dry {
		t += m.PaceMs * (1 - wetness)
	}
	t += wetPenaltyMs(compound, wetness)
	if age > m.CliffLap {
		t += m.CliffMs * float64(age-m.CliffLap)
	}
	if outLap {
		t += m.WarmupMs
	}
	return t
}

// UndercutDelta computes the net time delta of a 2-lap undercut (attacker
// pits at the end of lap N, defender responds at the end of lap N+1) or
// overcut (defender pits first). Pit-lane losses cancel when equal; any
// stationary penalty hold of the attacker is charged explicitly.
func UndercutDelta(r UndercutRequest) (*UndercutResult, error) {
	if r.Attacker.BaseLapMs == 0 {
		r.Attacker.BaseLapMs = 95_000
	}
	if r.Defender.BaseLapMs == 0 {
		r.Defender.BaseLapMs = r.Attacker.BaseLapMs
	}
	r.Attacker.Compound, r.Defender.Compound = canonical(r.Attacker.Compound), canonical(r.Defender.Compound)
	r.AttackerCompound, r.DefenderCompound = canonical(r.AttackerCompound), canonical(r.DefenderCompound)
	if r.AttackerCompound == "" {
		r.AttackerCompound = "HARD"
	}
	if r.DefenderCompound == "" {
		r.DefenderCompound = r.AttackerCompound
	}
	for _, c := range []string{r.Attacker.Compound, r.Defender.Compound, r.AttackerCompound, r.DefenderCompound} {
		if _, ok := Compounds[c]; !ok {
			return nil, fmt.Errorf("unknown compound %q", c)
		}
	}
	if r.Mode == "" {
		r.Mode = "UNDERCUT"
	}
	if r.Rollouts <= 0 {
		r.Rollouts = 2000
	}
	if r.LapSigmaMs == 0 {
		r.LapSigmaMs = DefaultLapSigmaMs
	}
	A, D := r.Attacker, r.Defender
	var aOut, dOld, aNew2, dOut float64
	if r.Mode == "OVERCUT" {
		// defender stops first; attacker stays out one more lap on old tyres
		dOutFirst := carLap(D.BaseLapMs, r.DefenderCompound, 0, nil, 0, true)
		aOldLap := carLap(A.BaseLapMs, A.Compound, A.TyreAgeLaps+1, A.DegSlopeMsPerLap, 0, false)
		dNew2 := carLap(D.BaseLapMs, r.DefenderCompound, 1, nil, 0, false)
		aOutLate := carLap(A.BaseLapMs, r.AttackerCompound, 0, nil, 0, true)
		aOut, dOld, aNew2, dOut = aOldLap, dOutFirst, aOutLate, dNew2
	} else {
		aOut = carLap(A.BaseLapMs, r.AttackerCompound, 0, nil, 0, true)
		dOld = carLap(D.BaseLapMs, D.Compound, D.TyreAgeLaps+1, D.DegSlopeMsPerLap, 0, false)
		aNew2 = carLap(A.BaseLapMs, r.AttackerCompound, 1, nil, 0, false)
		dOut = carLap(D.BaseLapMs, r.DefenderCompound, 0, nil, 0, true)
	}
	lap1 := dOld - aOut
	lap2 := dOut - aNew2
	hold := A.PenaltyS * 1000
	gain := lap1 + lap2 - hold
	rng := rand.New(rand.NewPCG(r.Seed, 7))
	succ := 0
	for i := 0; i < r.Rollouts; i++ {
		g := gain + rng.NormFloat64()*r.LapSigmaMs*2 + rng.NormFloat64()*StopNoiseSigmaMs*math.Sqrt2
		if r.GapMs-g < 0 {
			succ++
		}
	}
	res := &UndercutResult{
		Mode: r.Mode, ExpectedGainMs: gain, GapBeforeMs: r.GapMs, GapAfterMs: r.GapMs - gain,
		PSuccess: float64(succ) / float64(r.Rollouts), LapDeltasMs: []float64{lap1, lap2}, PenaltyHoldMs: hold,
		AttackerOutLap: aOut, DefenderOldLap: dOld, AttackerNewLap2: aNew2, DefenderOutLap: dOut,
	}
	verdict := "fails"
	if res.GapAfterMs < 0 {
		verdict = "works"
	}
	res.Explanation = fmt.Sprintf("%s %s: lap 1 delta %+.0fms (fresh %s out-lap vs %s age %d), lap 2 delta %+.0fms, penalty hold %.0fms; gap %.0fms -> %.0fms",
		r.Mode, verdict, lap1, r.AttackerCompound, D.Compound, D.TyreAgeLaps+1, lap2, hold, r.GapMs, res.GapAfterMs)
	return res, nil
}

// TrafficRequest is the input of validate_physics_and_traffic.
type TrafficRequest struct {
	CurrentLap  int        `json:"current_lap"`
	PitLap      int        `json:"pit_lap"`
	PitLossS    float64    `json:"pit_loss_s"`
	PenaltyS    float64    `json:"penalty_s"`
	NewCompound string     `json:"new_compound"`
	Car         CarState   `json:"car"`
	Field       []FieldCar `json:"field"`
}

// TrafficResult describes where the car rejoins.
type TrafficResult struct {
	Valid            bool     `json:"valid"`
	RejoinPosition   int      `json:"rejoin_position"`
	CarAhead         string   `json:"car_ahead,omitempty"`
	GapToCarAheadMs  float64  `json:"gap_to_car_ahead_ms"`
	CarsWithin1_2s   []string `json:"cars_within_1_2s"`
	InDRSTrain       bool     `json:"in_drs_train"`
	TrainCars        []string `json:"drs_train"`
	ExpectedLossMs   float64  `json:"expected_dirty_air_loss_ms"`
	Verdict          string   `json:"verdict"`
	SuggestedPitLaps []int    `json:"suggested_clear_air_pit_laps,omitempty"`
}

// ValidateTraffic deterministically projects the field to the proposed pit
// lap and checks whether the car rejoins inside a DRS train.
func ValidateTraffic(r TrafficRequest) (*TrafficResult, error) {
	if r.PitLap < r.CurrentLap {
		return nil, fmt.Errorf("pit_lap %d is before current_lap %d", r.PitLap, r.CurrentLap)
	}
	if r.PitLossS == 0 {
		r.PitLossS = DefaultPitLossGreenS
	}
	if r.Car.BaseLapMs == 0 {
		r.Car.BaseLapMs = 95_000
	}
	check := func(pitLap int) *TrafficResult {
		laps := pitLap - r.CurrentLap
		comp := canonical(r.Car.Compound)
		var ours float64
		for k := 1; k <= laps; k++ {
			ours += carLap(r.Car.BaseLapMs, comp, r.Car.TyreAgeLaps+k, r.Car.DegSlopeMsPerLap, 0, false)
		}
		ours += r.Car.GapToLeaderMs + (r.PitLossS+r.PenaltyS)*1000
		type proj struct {
			code string
			t    float64
		}
		var ps []proj
		for _, f := range r.Field {
			base := f.BaseLapMs
			if base == 0 {
				base = r.Car.BaseLapMs
			}
			t := f.GapToLeaderMs
			for k := 1; k <= laps; k++ {
				t += carLap(base, canonical(f.Compound), f.TyreAgeLaps+k, nil, 0, false)
			}
			if f.PlannedStopLap > r.CurrentLap && f.PlannedStopLap <= pitLap {
				t += r.PitLossS * 1000
			}
			code := f.DriverCode
			if code == "" {
				code = fmt.Sprint(f.DriverNumber)
			}
			ps = append(ps, proj{code, t})
		}
		sort.Slice(ps, func(i, j int) bool { return ps[i].t < ps[j].t })
		res := &TrafficResult{RejoinPosition: 1, CarsWithin1_2s: []string{}, TrainCars: []string{}}
		aheadIdx := -1
		for i, p := range ps {
			if p.t < ours {
				res.RejoinPosition++
				aheadIdx = i
				if ours-p.t < DirtyAirWindowMs {
					res.CarsWithin1_2s = append(res.CarsWithin1_2s, p.code)
				}
			}
		}
		if aheadIdx >= 0 {
			res.CarAhead = ps[aheadIdx].code
			res.GapToCarAheadMs = ours - ps[aheadIdx].t
			// walk forward while each car is within DRS range of the one ahead
			train := []string{ps[aheadIdx].code}
			for i := aheadIdx; i > 0 && ps[i].t-ps[i-1].t < DRSWindowMs; i-- {
				train = append(train, ps[i-1].code)
			}
			if res.GapToCarAheadMs < DirtyAirWindowMs && len(train) >= 2 {
				res.InDRSTrain = true
				res.TrainCars = train
			}
		}
		switch {
		case res.InDRSTrain:
			res.ExpectedLossMs = DirtyAirMsPerLap * float64(2+len(res.TrainCars))
			res.Verdict = fmt.Sprintf("REJOINS IN DRS TRAIN behind %s (%d cars within DRS range): ~%.1fs dirty-air loss",
				res.CarAhead, len(res.TrainCars), res.ExpectedLossMs/1000)
		case len(res.CarsWithin1_2s) > 0:
			res.ExpectedLossMs = DirtyAirMsPerLap * 2
			res.Verdict = fmt.Sprintf("rejoins %.1fs behind %s: short dirty-air phase", res.GapToCarAheadMs/1000, res.CarAhead)
		default:
			res.Verdict = "clear air on rejoin"
		}
		res.Valid = !res.InDRSTrain
		return res
	}
	res := check(r.PitLap)
	if !res.Valid {
		for d := 1; d <= 4; d++ {
			for _, lap := range []int{r.PitLap - d, r.PitLap + d} {
				if lap >= r.CurrentLap && check(lap).Valid && len(res.SuggestedPitLaps) < 3 {
					res.SuggestedPitLaps = append(res.SuggestedPitLaps, lap)
				}
			}
		}
	}
	return res, nil
}
