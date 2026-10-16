package sim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Plan is a resolved, concrete strategy: at most one stop.
type Plan struct {
	Name       string   `json:"strategy"`
	StopLap    int      `json:"pit_lap"` // 0 = no stop before the flag
	Compound   string   `json:"target_compound"`
	Legal      bool     `json:"legal"`
	Violations []string `json:"violations"`
	Notes      string   `json:"notes,omitempty"`
}

// Outcome aggregates the rollouts of one plan.
type Outcome struct {
	Plan
	MeanRaceTimeMs   float64 `json:"mean_remaining_race_ms"`
	StdMs            float64 `json:"std_ms"`
	ExpectedDeltaMs  float64 `json:"expected_delta_ms"` // vs the best legal plan (0 = best)
	P10DeltaMs       float64 `json:"p10_delta_ms"`
	P50DeltaMs       float64 `json:"p50_delta_ms"`
	P90DeltaMs       float64 `json:"p90_delta_ms"`
	ExpectedFinish   float64 `json:"expected_finish_position"`
	PGainPosition    float64 `json:"p_finish_ahead_of_car_ahead"`
	MeanRejoinPos    float64 `json:"mean_rejoin_position"`
	PTraffic         float64 `json:"p_rejoin_in_traffic"`
	MeanTrafficMs    float64 `json:"mean_traffic_loss_ms"`
	PenaltyHoldMs    float64 `json:"penalty_hold_ms"`
	PWinVsBest       float64 `json:"p_faster_than_best"`
	PitLossAppliedMs float64 `json:"mean_pit_loss_ms"`
}

// Result is the output of simulate_pit_strategies.
type Result struct {
	Rollouts        int       `json:"rollouts_per_strategy"`
	TotalRollouts   int       `json:"total_trajectories"`
	Workers         int       `json:"workers"`
	ElapsedMs       float64   `json:"elapsed_ms"`
	LapsRemaining   int       `json:"laps_remaining"`
	Recommended     string    `json:"recommended"`
	RecommendedPlan *Outcome  `json:"recommended_plan"`
	Outcomes        []Outcome `json:"outcomes"`
	Assumptions     []string  `json:"assumptions"`
}

type fieldProj struct {
	car    FieldCar
	rel    float64   // gap relative to our car now (negative = ahead)
	cum    []float64 // expected cumulative time at end of lap current+k (k = 0..n-1)
	lapMs  []float64 // expected lap time per remaining lap
	finish float64
}

type plannerCtx struct {
	req      *Request
	n        int // laps remaining including the current one
	deg      float64
	hasDeg   bool
	field    []fieldProj
	ahead    int // index into field of the car directly ahead, -1 if none
	hazard   float64
	overtake float64 // circuit overtaking difficulty in [0,1)
	neutral  int     // laps of neutralisation remaining at the start
}

func (c *plannerCtx) wetness(lap int) float64 {
	w := c.req.TrackWetness + c.req.WetnessTrend*float64(lap-c.req.CurrentLap)
	return math.Max(0, math.Min(1, w))
}

// lapMs is the expected (noise-free) green-flag lap time.
func (c *plannerCtx) lapMs(compound string, age, lap int, own bool) float64 {
	return c.lapMsOf(Compounds[compound], compound, age, lap, own)
}

// lapMsOf is lapMs for an already looked-up compound (the rollout hot loop).
func (c *plannerCtx) lapMsOf(m Compound, compound string, age, lap int, own bool) float64 {
	deg := m.DegMs
	if own && c.hasDeg && compound == c.req.Car.Compound && age >= c.req.Car.TyreAgeLaps {
		deg = math.Max(0, c.deg) // observed fuel-corrected slope of the current set
	}
	t := c.req.Car.BaseLapMs + deg*float64(age) - FuelEffectMsPerLap*float64(lap-c.req.CurrentLap)
	w := c.wetness(lap)
	if m.Dry {
		t += m.PaceMs * (1 - w)
	}
	t += wetPenaltyMs(compound, w)
	return t + m.CliffMs(age)
}

func (c *plannerCtx) stintMs(compound string, startAge, fromLap, toLap int) float64 {
	var t float64
	for lap, age := fromLap, startAge; lap <= toLap; lap, age = lap+1, age+1 {
		t += c.lapMs(compound, age, lap, false)
	}
	return t
}

func isWetCompound(c string) bool { return c == "INTERMEDIATE" || c == "WET" }

// ruleViolations checks Article 30.5(m) and the pit-entry status for a plan.
func (c *plannerCtx) ruleViolations(p *Plan) []string {
	var v []string
	req := c.req
	if p.StopLap == req.CurrentLap && req.PitEntryClosed {
		v = append(v, "Article 34.15: pit entry is closed this lap; a car entering the pit lane is penalised")
	}
	if p.StopLap > 0 && contains(req.ForbidCompounds, p.Compound) {
		v = append(v, fmt.Sprintf("no %s sets available in the allocation", p.Compound))
	}
	used := append([]string{}, req.Car.CompoundsUsed...)
	if p.StopLap > 0 {
		used = append(used, p.Compound)
	}
	wet := false
	dry := map[string]bool{}
	for _, u := range used {
		if isWetCompound(u) {
			wet = true
		} else {
			dry[u] = true
		}
	}
	if !wet && len(dry) < 2 {
		var names []string
		for k := range dry {
			names = append(names, k)
		}
		sort.Strings(names)
		v = append(v, fmt.Sprintf("Article 30.5(m): would finish a dry race having used only %s; at least two different dry-weather specifications are mandatory (disqualification)", strings.Join(names, "+")))
	}
	return v
}

// bestCompound returns the fastest compound for a stint that keeps the plan legal.
func (c *plannerCtx) bestCompound(stopLap int) string {
	best, bestT := "", math.Inf(1)
	for _, comp := range []string{"SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET"} {
		p := Plan{StopLap: stopLap, Compound: comp}
		if len(c.ruleViolations(&p)) > 0 && stopLap != c.req.CurrentLap {
			continue
		}
		if len(c.ruleViolations(&p)) > 0 {
			// pit-entry violations apply to every compound equally; keep 30.5(m) filtering
			ok := true
			for _, v := range c.ruleViolations(&p) {
				if strings.HasPrefix(v, "Article 30.5") || strings.HasPrefix(v, "no ") {
					ok = false
				}
			}
			if !ok {
				continue
			}
		}
		t := c.stintMs(comp, 0, stopLap+1, c.req.TotalLaps) + Compounds[comp].WarmupMs
		if t < bestT {
			best, bestT = comp, t
		}
	}
	if best == "" {
		best = "HARD"
	}
	return best
}

func (c *plannerCtx) resolve(name string) Plan {
	req := c.req
	cur, total := req.CurrentLap, req.TotalLaps
	p := Plan{Name: name}
	switch {
	case strings.HasPrefix(name, "PIT_NOW_"):
		p.StopLap = cur
		p.Compound = canonical(strings.TrimPrefix(name, "PIT_NOW_"))
		if _, ok := Compounds[p.Compound]; !ok {
			p.Violations = []string{"unknown compound " + p.Compound}
			return p
		}
	case name == "STAY_OUT_EXTEND":
		stop := cur + req.ExtendLaps
		noStop := Plan{StopLap: 0}
		noStopLegal := len(c.ruleViolations(&noStop)) == 0
		if stop >= total {
			stop = 0
		}
		if stop == 0 && !noStopLegal {
			stop = total - 1 // latest legal stop
		}
		if stop > 0 && noStopLegal {
			// the rule is already satisfied: only stop if it is expected to pay off
			comp := c.bestCompound(stop)
			withStop := c.stintMs(req.Car.Compound, req.Car.TyreAgeLaps+1, cur+1, stop) + req.PitLossGreenS*1000 +
				Compounds[comp].WarmupMs + c.stintMs(comp, 0, stop+1, total)
			noStopT := c.stintMs(req.Car.Compound, req.Car.TyreAgeLaps+1, cur+1, total)
			if noStopT <= withStop {
				stop = 0
			}
		}
		p.StopLap = stop
		if stop > 0 {
			p.Compound = c.bestCompound(stop)
			p.Notes = fmt.Sprintf("extend the %s stint %d laps, then pit on lap %d", req.Car.Compound, stop-cur, stop)
		} else {
			p.Compound = req.Car.Compound
			p.Notes = "no further stop: run the current set to the flag"
		}
	case name == "OPPOSITE_AHEAD":
		if c.ahead < 0 {
			return c.relabel(c.resolve("STAY_OUT_EXTEND"), name, "no car ahead: equivalent to STAY_OUT_EXTEND")
		}
		a := c.field[c.ahead].car
		if a.TyreAgeLaps >= req.Car.TyreAgeLaps {
			// the car ahead is on older tyres and will likely stop first: overcut
			stop := cur + 3
			if stop >= total {
				stop = total - 1
			}
			p.StopLap = stop
			p.Compound = c.bestCompound(stop)
			p.Notes = fmt.Sprintf("%s ahead is on older %s (age %d): stay out and overcut, pit on lap %d", a.DriverCode, a.Compound, a.TyreAgeLaps, stop)
		} else {
			p.StopLap = cur
			p.Compound = c.bestCompound(cur)
			p.Notes = fmt.Sprintf("%s ahead is on fresher %s (age %d): undercut now", a.DriverCode, a.Compound, a.TyreAgeLaps)
		}
	default:
		p.Violations = []string{"unknown strategy " + name}
		return p
	}
	p.Violations = c.ruleViolations(&p)
	p.Legal = len(p.Violations) == 0
	return p
}

func (c *plannerCtx) relabel(p Plan, name, notes string) Plan {
	p.Name = name
	p.Notes = notes
	return p
}

func newPlanner(req *Request) *plannerCtx {
	c := &plannerCtx{req: req, n: req.TotalLaps - req.CurrentLap + 1, ahead: -1}
	if req.Car.DegSlopeMsPerLap != nil {
		c.deg, c.hasDeg = *req.Car.DegSlopeMsPerLap, true
	}
	c.overtake = DefaultOvertakeDifficulty
	if req.OvertakeDifficulty != nil {
		c.overtake = math.Max(0, math.Min(0.98, *req.OvertakeDifficulty))
	}
	c.hazard = DefaultSCHazardPerLap
	if req.SCHazardPerLap != nil {
		c.hazard = *req.SCHazardPerLap
	}
	switch req.TrackStatus {
	case "SC":
		c.neutral = 3
	case "VSC":
		c.neutral = 2
	case "RED":
		c.neutral = 1
	}
	for _, f := range req.Field {
		if f.PlannedStopLap == 0 {
			f.PlannedStopLap = autoStopLap(f, req.CurrentLap, req.TotalLaps)
		}
		fp := fieldProj{car: f, rel: f.GapToLeaderMs - req.Car.GapToLeaderMs}
		var cum float64
		age, comp := f.TyreAgeLaps, f.Compound
		if _, ok := Compounds[comp]; !ok {
			comp = "MEDIUM"
		}
		firstSet := true // the observed degradation applies to the set fitted now
		weatherStop := false
		for k := 0; k < c.n; k++ {
			lap := req.CurrentLap + k
			m := Compounds[comp]
			deg := m.DegMs
			if firstSet && f.DegSlopeMsPerLap != nil {
				deg = math.Max(0, *f.DegSlopeMsPerLap)
			}
			t := f.BaseLapMs + deg*float64(age) - FuelEffectMsPerLap*float64(k)
			w := c.wetness(lap)
			if m.Dry {
				t += m.PaceMs * (1 - w)
			}
			t += wetPenaltyMs(comp, w)
			t += m.CliffMs(age)
			// rivals react to the weather too: slicks on a wet track or wet
			// tyres on a dry one trigger a crossover stop (once)
			crossover := !weatherStop && ((m.Dry && w >= SlickToInterWetness) || (!m.Dry && w <= InterToSlickWetness))
			if f.PlannedStopLap == lap || crossover {
				t += req.PitLossGreenS * 1000
				comp, age = fieldNextCompound(comp, w), -1
				firstSet = false
				if crossover {
					weatherStop = true
				}
			}
			age++
			cum += t
			fp.lapMs = append(fp.lapMs, t)
			fp.cum = append(fp.cum, fp.rel+cum)
		}
		fp.finish = fp.rel + cum
		c.field = append(c.field, fp)
	}
	for i, f := range c.field {
		if f.car.Position == req.Car.Position-1 {
			c.ahead = i
		}
	}
	return c
}

// autoStopLap gives a field car with no declared plan the stop it will need:
// rivals do not run worn tyres off the cliff to the flag. Cars that can reach
// the end before their cliff are assumed not to stop again.
func autoStopLap(f FieldCar, current, total int) int {
	m, ok := Compounds[canonical(f.Compound)]
	if !ok || !m.Dry {
		return 0
	}
	if f.TyreAgeLaps+(total-current) <= m.CliffLap+2 {
		return 0
	}
	lap := current + (m.CliffLap - 2 - f.TyreAgeLaps)
	if lap < current {
		lap = current
	}
	if lap >= total {
		return 0
	}
	return lap
}

// fieldNextCompound is the tyre a projected rival fits at a stop.
func fieldNextCompound(current string, wetness float64) string {
	if wetness >= SlickToInterWetness {
		return "INTERMEDIATE"
	}
	if !Compounds[current].Dry {
		return "MEDIUM"
	}
	return otherDry(current)
}

func otherDry(c string) string {
	if c == "HARD" {
		return "MEDIUM"
	}
	return "HARD"
}

type acc struct {
	sum, sumsq, finish, gain, rejoin, traffic, trafficMs, pitLoss float64
}

// Simulate runs the Monte Carlo comparison.
func Simulate(req Request) (*Result, error) {
	start := time.Now()
	if err := req.normalise(); err != nil {
		return nil, err
	}
	c := newPlanner(&req)
	plans := make([]Plan, 0, len(req.Strategies))
	seen := map[string]bool{}
	for _, s := range req.Strategies {
		s = strings.ToUpper(strings.TrimSpace(s))
		if seen[s] {
			continue
		}
		seen[s] = true
		plans = append(plans, c.resolve(s))
	}
	R := req.Rollouts
	P := len(plans)
	times := make([][]float32, P)
	for i := range times {
		times[i] = make([]float32, R)
	}

	const chunk = 256
	nChunks := (R + chunk - 1) / chunk
	workers := runtime.NumCPU()
	if workers > nChunks {
		workers = nChunks
	}
	partial := make([][]acc, nChunks)
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				ci := int(next.Add(1) - 1)
				if ci >= nChunks {
					return
				}
				// per-chunk generator: results do not depend on scheduling
				rng := rand.New(rand.NewPCG(req.Seed+0x9E3779B97F4A7C15, uint64(ci)))
				local := make([]acc, P)
				lo, hi := ci*chunk, min((ci+1)*chunk, R)
				noise := make([]float64, c.n)
				scStart := make([]bool, c.n)
				fieldNoise := make([]float64, len(c.field))
				fieldU := make([]float64, len(c.field))
				scratch := make([]int, 0, len(c.field))
				isAhead := make([]bool, len(c.field))
				for r := lo; r < hi; r++ {
					// common random numbers shared by every plan in this rollout
					for k := range noise {
						noise[k] = rng.NormFloat64() * req.LapSigmaMs
						scStart[k] = k > 0 && rng.Float64() < c.hazard
					}
					for j := range fieldNoise {
						fieldNoise[j] = rng.NormFloat64()
						fieldU[j] = rng.Float64()
					}
					stopNoise := rng.NormFloat64() * StopNoiseSigmaMs
					if rng.Float64() < SlowStopProb {
						stopNoise += SlowStopMs
					}
					passU := rng.Float64()
					for pi := range plans {
						t, fin, gain, rejoin, traffic, trafMs, pitLoss := c.rollout(&plans[pi], noise, scStart, fieldNoise, fieldU, scratch, isAhead, stopNoise, passU)
						times[pi][r] = float32(t)
						a := &local[pi]
						a.sum += t
						a.sumsq += t * t
						a.finish += fin
						a.gain += gain
						a.rejoin += rejoin
						a.traffic += traffic
						a.trafficMs += trafMs
						a.pitLoss += pitLoss
					}
				}
				partial[ci] = local
			}
		}()
	}
	wg.Wait()

	tot := make([]acc, P)
	for _, part := range partial {
		for pi := range part {
			a, b := &tot[pi], part[pi]
			a.sum += b.sum
			a.sumsq += b.sumsq
			a.finish += b.finish
			a.gain += b.gain
			a.rejoin += b.rejoin
			a.traffic += b.traffic
			a.trafficMs += b.trafficMs
			a.pitLoss += b.pitLoss
		}
	}
	n := float64(R)
	out := make([]Outcome, P)
	best := -1
	for pi, p := range plans {
		a := tot[pi]
		mean := a.sum / n
		out[pi] = Outcome{Plan: p, MeanRaceTimeMs: mean, StdMs: math.Sqrt(math.Max(0, a.sumsq/n-mean*mean)),
			ExpectedFinish: a.finish / n, PGainPosition: a.gain / n, MeanRejoinPos: a.rejoin / n,
			PTraffic: a.traffic / n, MeanTrafficMs: a.trafficMs / n, PitLossAppliedMs: a.pitLoss / n}
		if p.StopLap > 0 {
			out[pi].PenaltyHoldMs = req.Car.PenaltyS * 1000
		}
		if p.StopLap == 0 {
			out[pi].MeanRejoinPos = 0
		}
		if out[pi].Violations == nil {
			out[pi].Violations = []string{}
		}
		if p.Legal && (best < 0 || mean < out[best].MeanRaceTimeMs) {
			best = pi
		}
	}
	if best < 0 { // nothing legal: rank everything
		for pi := range out {
			if best < 0 || out[pi].MeanRaceTimeMs < out[best].MeanRaceTimeMs {
				best = pi
			}
		}
	}
	deltas := make([]float64, R)
	for pi := range out {
		var wins float64
		for r := 0; r < R; r++ {
			deltas[r] = float64(times[pi][r]) - float64(times[best][r])
			if deltas[r] < 0 {
				wins++
			}
		}
		sort.Float64s(deltas)
		out[pi].ExpectedDeltaMs = out[pi].MeanRaceTimeMs - out[best].MeanRaceTimeMs
		out[pi].P10DeltaMs = deltas[int(0.10*float64(R-1))]
		out[pi].P50DeltaMs = deltas[int(0.50*float64(R-1))]
		out[pi].P90DeltaMs = deltas[int(0.90*float64(R-1))]
		out[pi].PWinVsBest = wins / n
	}
	res := &Result{
		Rollouts: R, TotalRollouts: R * P, Workers: workers, LapsRemaining: c.n,
		Recommended: out[best].Name, Outcomes: out,
		Assumptions: []string{
			fmt.Sprintf("pit loss %.1fs green / %.1fs under SC-VSC, +%.1fs stationary penalty hold", req.PitLossGreenS, req.PitLossNeutralS, req.Car.PenaltyS),
			fmt.Sprintf("lap noise sigma %.0fms, random SC hazard %.3f/lap, fuel effect %.0fms/lap", req.LapSigmaMs, c.hazard, FuelEffectMsPerLap),
			fmt.Sprintf("dirty air +%.0fms/lap when rejoining <%.1fs behind slower traffic", DirtyAirMsPerLap, DirtyAirWindowMs/1000),
			fmt.Sprintf("overtaking difficulty %.2f: faster cars must still pass on track", c.overtake),
		},
	}
	bp := out[best]
	res.RecommendedPlan = &bp
	res.ElapsedMs = float64(time.Since(start).Microseconds()) / 1000
	return res, nil
}

// rollout simulates one trajectory of one plan. It returns the remaining race
// time, finishing position, whether we beat the car ahead, rejoin position,
// whether we rejoined in traffic, the traffic loss and the pit loss applied.
func (c *plannerCtx) rollout(p *Plan, noise []float64, scStart []bool, fieldNoise, fieldU []float64, ahead []int, isAhead []bool,
	stopNoise, passU float64) (float64, float64, float64, float64, float64, float64, float64) {
	req := c.req
	comp, age := req.Car.Compound, req.Car.TyreAgeLaps
	cm := Compounds[comp]
	// cars physically ahead on track: at the start, or at the rejoin after a stop
	ahead = ahead[:0]
	for j := range c.field {
		if c.field[j].rel < 0 {
			ahead = append(ahead, j)
		}
	}
	rejoinK, tRef := 0, -1.0 // reference lap/time for on-track passing
	neutralLeft := c.neutral
	var t, rejoinPos, traffic, trafficMs, pitLoss float64
	outLap := false
	dirty := 0
	penaltyServed := false
	for k := 0; k < c.n; k++ {
		lap := req.CurrentLap + k
		if scStart[k] && neutralLeft == 0 {
			neutralLeft = 3
		}
		neutral := neutralLeft > 0
		var lt float64
		if neutral && k > 0 {
			// the whole field circulates at the SC/VSC delta, so relative to the
			// projected field this is a reference lap with no tyre or noise effect
			lt = req.Car.BaseLapMs
		} else {
			lt = c.lapMsOf(cm, comp, age, lap, true) + noise[k]*cm.NoiseScale(age)
			if outLap {
				lt += cm.WarmupMs
			}
			if dirty > 0 {
				lt += DirtyAirMsPerLap
				trafficMs += DirtyAirMsPerLap
				dirty--
			}
		}
		outLap = false
		t += lt
		if lap == p.StopLap && p.StopLap > 0 {
			loss := req.PitLossGreenS * 1000
			if req.TrackStatus == "RED" && k == 0 {
				loss = 0 // tyres may be changed during the suspension (Art. 57.4)
			} else if neutral {
				loss = req.PitLossNeutralS * 1000
			}
			loss += math.Max(0, stopNoise)
			if req.Car.PenaltyS > 0 && !penaltyServed && !(req.TrackStatus == "RED" && k == 0) {
				loss += req.Car.PenaltyS * 1000 // Art. 54.4c: no work until the car has been stationary for the penalty
				penaltyServed = true
			}
			pitLoss = loss
			t += loss
			comp, age, outLap = p.Compound, 0, true
			cm = Compounds[comp]
			// rejoin: where do we come out relative to the projected field?
			if len(c.field) > 0 {
				pos := 1.0
				gapAhead := math.Inf(1)
				var aheadIdx = -1
				for j := range c.field {
					fj := c.field[j].cum[k] + fieldNoise[j]*req.LapSigmaMs*math.Sqrt(float64(k+1))
					if fj < t {
						pos++
						if d := t - fj; d < gapAhead {
							gapAhead, aheadIdx = d, j
						}
					}
				}
				rejoinPos = pos
				rejoinK, tRef = k, t
				ahead = ahead[:0]
				for j := range c.field {
					if c.field[j].cum[k]+fieldNoise[j]*req.LapSigmaMs*math.Sqrt(float64(k+1)) < t {
						ahead = append(ahead, j)
					}
				}
				if aheadIdx >= 0 && gapAhead < DirtyAirWindowMs && k+1 < c.n {
					theirs := c.field[aheadIdx].lapMs[k+1]
					ours := c.lapMs(comp, 1, lap+1, true)
					if theirs > ours {
						traffic = 1
						pPass := math.Min(0.8, math.Max(0.2, 0.2+(theirs-ours)/2000))
						// geometric number of laps stuck, from one uniform draw
						dirty = int(math.Floor(math.Log(1-passU) / math.Log(1-pPass)))
						if dirty < 1 {
							dirty = 1
						}
						if dirty > 10 {
							dirty = 10
						}
					}
				}
			}
		} else {
			age++
		}
		if k == 0 && tRef < 0 {
			tRef = t // no stop this lap: passing is measured from the end of the current lap
		}
		if neutralLeft > 0 {
			neutralLeft--
		}
	}
	if req.Car.PenaltyS > 0 && !penaltyServed {
		t += req.Car.PenaltyS * 1000 // added to race time when not served in the pits
	}
	t = c.trackPosition(t, tRef, ahead, rejoinK, fieldNoise, fieldU)
	for j := range isAhead {
		isAhead[j] = false
	}
	for _, j := range ahead {
		isAhead[j] = true
	}
	finish, gain := 1.0, 0.0
	if len(c.field) > 0 {
		for j := range c.field {
			fj := c.field[j].finish + fieldNoise[j]*req.LapSigmaMs*math.Sqrt(float64(c.n))
			beats := fj < t
			if beats && !isAhead[j] {
				// it is behind us on track at the reference point: being faster
				// is not enough, it has to get past
				beats = c.passesUs(j, t, fj, tRef, rejoinK, fieldNoise, fieldU)
			}
			if beats {
				finish++
			}
			if j == c.ahead && !beats {
				gain = 1
			}
		}
	} else {
		finish = float64(req.Car.Position)
	}
	return t, finish, gain, rejoinPos, traffic, trafficMs, pitLoss
}

// passesUs is the defending mirror of trackPosition: rival j, behind us on
// track at the reference point and faster to the flag, gets past only through
// a stop of its own after that point (the pass happens in the pits) or on
// track, with the same catch-up and per-lap pass model that our car faces when
// it attacks. A failed pass leaves it behind us at the flag.
func (c *plannerCtx) passesUs(j int, t, fj, tRef float64, fromK int, fieldNoise, fieldU []float64) bool {
	req := c.req
	f := c.field[j]
	if f.car.PlannedStopLap > req.CurrentLap+fromK {
		return true
	}
	lapsAfter := float64(c.n - fromK - 1)
	if lapsAfter <= 0 {
		return false
	}
	gap0 := math.Max(0, f.cum[fromK]+fieldNoise[j]*req.LapSigmaMs*math.Sqrt(float64(fromK+1))-tRef)
	adv := (gap0 + (t - fj)) / lapsAfter // its pace advantage, ms/lap
	attempts := lapsAfter - math.Ceil(gap0/math.Max(adv, 1))
	if attempts < 1 {
		return false
	}
	pLap := (1 - c.overtake) * math.Min(0.7, math.Max(0.05, 0.1+adv/2000))
	return fieldU[j] < 1-math.Pow(1-pLap, attempts)
}

// trackPosition enforces that a faster car must still overtake on track. For
// each car physically ahead (nearest first) that we are projected to beat,
// we get one pass attempt per lap once we have caught it; the per-lap pass
// probability falls with the circuit's overtaking difficulty. A failed pass
// pins us behind that car (and everything ahead of it) at the flag.
func (c *plannerCtx) trackPosition(t, tRef float64, ahead []int, fromK int, fieldNoise, fieldU []float64) float64 {
	if len(ahead) == 0 {
		return t
	}
	req := c.req
	sigma := req.LapSigmaMs * math.Sqrt(float64(c.n))
	// nearest car first: the one with the latest projected time at the reference lap
	sort.Slice(ahead, func(a, b int) bool { return c.field[ahead[a]].cum[fromK] > c.field[ahead[b]].cum[fromK] })
	lapsAfter := float64(c.n - fromK - 1)
	for _, j := range ahead {
		f := c.field[j]
		if f.car.PlannedStopLap > req.CurrentLap+fromK {
			continue // they stop after we pass the pit window: the pass happens in the pits
		}
		fj := f.finish + fieldNoise[j]*sigma
		if fj <= t || lapsAfter <= 0 {
			if fj <= t {
				return t // they stay ahead on pace alone; nothing further ahead is reachable
			}
			continue
		}
		// how far behind it we are at the reference point (rejoin or end of this lap)
		gap0 := math.Max(0, tRef-(f.cum[fromK]+fieldNoise[j]*req.LapSigmaMs*math.Sqrt(float64(fromK+1))))
		adv := (gap0 + (fj - t)) / lapsAfter // ms/lap pace advantage
		catch := gap0 / math.Max(adv, 1)
		attempts := lapsAfter - math.Ceil(catch)
		if attempts < 1 {
			return math.Max(t, fj+300)
		}
		pLap := (1 - c.overtake) * math.Min(0.7, math.Max(0.05, 0.1+adv/2000))
		pPass := 1 - math.Pow(1-pLap, attempts)
		if fieldU[j] >= pPass {
			// stuck in its dirty air to the flag
			return math.Max(t, fj+300)
		}
	}
	return t
}
