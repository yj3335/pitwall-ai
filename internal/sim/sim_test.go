package sim

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func f64(v float64) *float64 { return &v }

func baseRequest() Request {
	return Request{
		TotalLaps: 57, CurrentLap: 30, TrackStatus: "GREEN", Seed: 42,
		Car: CarState{DriverNumber: 16, DriverCode: "LEC", Position: 3, Compound: "MEDIUM", TyreAgeLaps: 22,
			DegSlopeMsPerLap: f64(70), BaseLapMs: 95000, CompoundsUsed: []string{"MEDIUM"}, GapToLeaderMs: 6000},
		Field: []FieldCar{
			{DriverNumber: 1, DriverCode: "VER", Position: 1, GapToLeaderMs: 0, Compound: "HARD", TyreAgeLaps: 10},
			{DriverNumber: 4, DriverCode: "NOR", Position: 2, GapToLeaderMs: 4000, Compound: "MEDIUM", TyreAgeLaps: 24},
			{DriverNumber: 81, DriverCode: "PIA", Position: 4, GapToLeaderMs: 9000, Compound: "HARD", TyreAgeLaps: 8},
			{DriverNumber: 44, DriverCode: "HAM", Position: 5, GapToLeaderMs: 25000, Compound: "HARD", TyreAgeLaps: 12},
			{DriverNumber: 63, DriverCode: "RUS", Position: 6, GapToLeaderMs: 25600, Compound: "HARD", TyreAgeLaps: 12},
			{DriverNumber: 55, DriverCode: "SAI", Position: 7, GapToLeaderMs: 26300, Compound: "MEDIUM", TyreAgeLaps: 20},
		},
	}
}

func outcome(t *testing.T, r *Result, name string) Outcome {
	t.Helper()
	for _, o := range r.Outcomes {
		if o.Name == name {
			return o
		}
	}
	t.Fatalf("no outcome %s", name)
	return Outcome{}
}

func TestSimulateTenThousandRolloutsDeterministic(t *testing.T) {
	a, err := Simulate(baseRequest())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Simulate(baseRequest())
	if a.Rollouts != 10_000 || a.TotalRollouts != 60_000 {
		t.Fatalf("rollouts %d total %d", a.Rollouts, a.TotalRollouts)
	}
	for i := range a.Outcomes {
		if a.Outcomes[i].MeanRaceTimeMs != b.Outcomes[i].MeanRaceTimeMs {
			t.Fatalf("same seed gave different results for %s", a.Outcomes[i].Name)
		}
	}
	if a.Recommended == "" || a.RecommendedPlan == nil || !a.RecommendedPlan.Legal {
		t.Fatalf("no legal recommendation: %+v", a)
	}
	t.Logf("10k rollouts x %d strategies on %d workers in %.2fms, recommended %s", len(a.Outcomes), a.Workers, a.ElapsedMs, a.Recommended)
}

func TestTwoCompoundRuleFlagsIllegalPlans(t *testing.T) {
	req := baseRequest()
	req.Strategies = []string{"PIT_NOW_MEDIUM", "PIT_NOW_HARD", "STAY_OUT_EXTEND"}
	r, err := Simulate(req)
	if err != nil {
		t.Fatal(err)
	}
	med := outcome(t, r, "PIT_NOW_MEDIUM")
	if med.Legal || !strings.Contains(strings.Join(med.Violations, " "), "30.5(m)") {
		t.Fatalf("MEDIUM->MEDIUM must violate Article 30.5(m): %+v", med)
	}
	if !outcome(t, r, "PIT_NOW_HARD").Legal {
		t.Fatal("MEDIUM->HARD is legal")
	}
	ext := outcome(t, r, "STAY_OUT_EXTEND")
	if ext.StopLap == 0 || ext.Compound == "MEDIUM" || !ext.Legal {
		t.Fatalf("extend must still plan a legal second-compound stop: %+v", ext.Plan)
	}
	if r.Recommended == "PIT_NOW_MEDIUM" {
		t.Fatal("illegal plan recommended")
	}
}

func TestWetRaceWaivesTwoCompoundRule(t *testing.T) {
	req := baseRequest()
	req.Car.CompoundsUsed = []string{"INTERMEDIATE", "MEDIUM"}
	req.Strategies = []string{"PIT_NOW_MEDIUM"}
	r, _ := Simulate(req)
	if !outcome(t, r, "PIT_NOW_MEDIUM").Legal {
		t.Fatal("after using intermediates any dry compound is legal")
	}
}

func TestPenaltyHoldAndVSCDiscount(t *testing.T) {
	req := baseRequest()
	req.Field = nil // isolate the pit-stop arithmetic from track-position effects
	req.Strategies = []string{"PIT_NOW_HARD"}
	clean, _ := Simulate(req)
	req.Car.PenaltyS = 5
	pen, _ := Simulate(req)
	diff := outcome(t, pen, "PIT_NOW_HARD").MeanRaceTimeMs - outcome(t, clean, "PIT_NOW_HARD").MeanRaceTimeMs
	if diff < 4900 || diff > 5100 {
		t.Fatalf("5s penalty hold changed race time by %.0fms", diff)
	}
	req.Car.PenaltyS = 0
	req.TrackStatus = "VSC"
	vsc, _ := Simulate(req)
	saving := outcome(t, clean, "PIT_NOW_HARD").MeanRaceTimeMs - outcome(t, vsc, "PIT_NOW_HARD").MeanRaceTimeMs
	if saving < 7000 {
		t.Fatalf("VSC stop should save ~8.6s of pit loss, saved %.0fms", saving)
	}
}

func TestPitEntryClosed(t *testing.T) {
	req := baseRequest()
	req.TrackStatus = "SC"
	req.PitEntryClosed = true
	r, _ := Simulate(req)
	if outcome(t, r, "PIT_NOW_HARD").Legal {
		t.Fatal("pitting into a closed pit entry must be illegal")
	}
	if strings.HasPrefix(r.Recommended, "PIT_NOW") {
		t.Fatalf("recommended %s with pit entry closed", r.Recommended)
	}
}

func TestConcurrentSimulationsRaceFree(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := baseRequest()
			req.Seed = uint64(i)
			req.Rollouts = 2000
			if _, err := Simulate(req); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
}

func TestUndercutDelta(t *testing.T) {
	r, err := UndercutDelta(UndercutRequest{
		GapMs:            1500,
		Attacker:         CarState{Compound: "MEDIUM", TyreAgeLaps: 18, BaseLapMs: 95000},
		Defender:         CarState{Compound: "MEDIUM", TyreAgeLaps: 20, BaseLapMs: 95000, DegSlopeMsPerLap: f64(120)},
		AttackerCompound: "HARD", DefenderCompound: "HARD", Seed: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	// lap1: defender old (95000+120*21) - attacker out-lap (95000+450+1500)
	wantLap1 := float64(120*21) - 1950
	if d := r.LapDeltasMs[0] - wantLap1; d > 1e-6 || d < -1e-6 {
		t.Fatalf("lap1 delta %.1f want %.1f", r.LapDeltasMs[0], wantLap1)
	}
	if r.ExpectedGainMs <= 0 {
		t.Fatalf("heavily degraded defender should be undercut: %+v", r)
	}
	pen := UndercutRequest{GapMs: 1500, Attacker: CarState{Compound: "MEDIUM", TyreAgeLaps: 18, PenaltyS: 5},
		Defender: CarState{Compound: "MEDIUM", TyreAgeLaps: 20, DegSlopeMsPerLap: f64(120)}, AttackerCompound: "HARD"}
	rp, _ := UndercutDelta(pen)
	if rp.ExpectedGainMs > r.ExpectedGainMs-4999 || rp.PSuccess > r.PSuccess {
		t.Fatalf("5s hold must cost the undercut 5s: %+v", rp)
	}
}

func TestTrafficTrainDetection(t *testing.T) {
	req := TrafficRequest{CurrentLap: 30, PitLap: 30, PitLossS: 21.8,
		Car: CarState{Compound: "MEDIUM", TyreAgeLaps: 20, GapToLeaderMs: 0, BaseLapMs: 95000},
		Field: []FieldCar{
			{DriverCode: "A", GapToLeaderMs: 20500, Compound: "HARD", TyreAgeLaps: 5},
			{DriverCode: "B", GapToLeaderMs: 21100, Compound: "HARD", TyreAgeLaps: 5},
			{DriverCode: "C", GapToLeaderMs: 21600, Compound: "HARD", TyreAgeLaps: 5},
			{DriverCode: "D", GapToLeaderMs: 40000, Compound: "HARD", TyreAgeLaps: 5},
		}}
	r, err := ValidateTraffic(req)
	if err != nil {
		t.Fatal(err)
	}
	if !r.InDRSTrain || r.Valid || r.CarAhead != "C" || len(r.TrainCars) < 3 {
		t.Fatalf("expected DRS train behind C/B/A: %+v", r)
	}
	req.Field = req.Field[3:]
	r2, _ := ValidateTraffic(req)
	if !r2.Valid || r2.InDRSTrain {
		t.Fatalf("clear air expected: %+v", r2)
	}
}

func BenchmarkSimulate10k(b *testing.B) {
	req := baseRequest()
	for i := 0; i < b.N; i++ {
		if _, err := Simulate(req); err != nil {
			b.Fatal(err)
		}
	}
}

func TestLatencyBudget(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("latency is measured without the race detector")
	}
	req := baseRequest()
	req.Strategies = []string{"PIT_NOW_HARD"}
	Simulate(req) // warm up
	start := time.Now()
	const runs = 20
	for i := 0; i < runs; i++ {
		Simulate(req)
	}
	per := time.Since(start) / runs
	t.Logf("10,000 rollouts of one strategy: %s", per)
	if per > 15*time.Millisecond {
		t.Fatalf("10k rollouts took %s (budget 15ms)", per)
	}
}

// A leader on old hards under a VSC at a street circuit: fresh tyres are
// faster, but rejoining behind four cars that cannot be passed loses the
// race. The same situation on an easy-overtaking circuit favours the stop.
func TestOvertakingDifficultyValuesTrackPosition(t *testing.T) {
	mk := func(difficulty float64) *Result {
		req := Request{TotalLaps: 62, CurrentLap: 44, TrackStatus: "VSC", Seed: 3, OvertakeDifficulty: &difficulty,
			Strategies: []string{"PIT_NOW_MEDIUM", "STAY_OUT_EXTEND"},
			Car: CarState{DriverCode: "SAI", Position: 1, Compound: "HARD", TyreAgeLaps: 23, BaseLapMs: 99000,
				CompoundsUsed: []string{"MEDIUM", "HARD"}, DegSlopeMsPerLap: f64(40)},
			Field: []FieldCar{
				{DriverCode: "NOR", Position: 2, GapToLeaderMs: 2000, Compound: "HARD", TyreAgeLaps: 23},
				{DriverCode: "LEC", Position: 3, GapToLeaderMs: 4600, Compound: "HARD", TyreAgeLaps: 23},
				{DriverCode: "VER", Position: 4, GapToLeaderMs: 7000, Compound: "HARD", TyreAgeLaps: 23},
				{DriverCode: "HAM", Position: 5, GapToLeaderMs: 9500, Compound: "HARD", TyreAgeLaps: 23},
			}}
		r, err := Simulate(req)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	street, open := mk(0.9), mk(0.0)
	if street.Recommended != "STAY_OUT_EXTEND" {
		t.Fatalf("street circuit should keep track position, got %s", street.Recommended)
	}
	if open.Recommended != "PIT_NOW_MEDIUM" {
		t.Fatalf("easy-overtaking circuit should take the cheap VSC stop, got %s", open.Recommended)
	}
	if outcome(t, street, "PIT_NOW_MEDIUM").ExpectedFinish <= outcome(t, open, "PIT_NOW_MEDIUM").ExpectedFinish {
		t.Fatal("harder overtaking must worsen the finishing position after a stop")
	}
}

// A rival's observed degradation must change the projection: with 140 ms/lap
// of wear the car ahead is much easier to beat than with the default model.
func TestFieldDegradationIsUsed(t *testing.T) {
	mk := func(deg *float64) *Result {
		req := baseRequest()
		req.Field[1].DegSlopeMsPerLap = deg // NOR, the car directly ahead of LEC
		req.Strategies = []string{"PIT_NOW_HARD"}
		r, err := Simulate(req)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	base, worn := mk(nil), mk(f64(140))
	if outcome(t, worn, "PIT_NOW_HARD").PGainPosition <= outcome(t, base, "PIT_NOW_HARD").PGainPosition {
		t.Fatalf("a heavily degrading rival should be easier to beat: %.2f vs %.2f",
			outcome(t, worn, "PIT_NOW_HARD").PGainPosition, outcome(t, base, "PIT_NOW_HARD").PGainPosition)
	}
}

// When rain arrives, rivals on slicks also switch to intermediates in the
// projection instead of losing 7 s a lap to the flag.
func TestFieldReactsToWeather(t *testing.T) {
	req := baseRequest()
	req.TrackWetness = 0.6
	req.Car.Compound, req.Car.CompoundsUsed = "SOFT", []string{"SOFT"}
	c := newPlanner(&req)
	for _, f := range c.field {
		dry := Compounds[canonical(f.car.Compound)].Dry
		if !dry {
			continue
		}
		// first lap on slicks in the rain carries the crossover stop, later laps are on inters
		if f.lapMs[1] > f.lapMs[0] {
			t.Fatalf("%s stayed on slicks in the rain: %.0f -> %.0f", f.car.DriverCode, f.lapMs[0], f.lapMs[1])
		}
		if f.lapMs[1] > req.Car.BaseLapMs+wetPenaltyMs("INTERMEDIATE", 0.6)+2000 {
			t.Fatalf("%s not on intermediates after the crossover: %.0f", f.car.DriverCode, f.lapMs[1])
		}
	}
}

func TestCliffIsQuadraticWithWideningSpread(t *testing.T) {
	s := Compounds["SOFT"]
	for age, want := range map[int]float64{0: 0, 15: 0, 16: 80, 18: 720, 20: 2000, 23: 5120} {
		if got := s.CliffMs(age); got != want {
			t.Errorf("SOFT CliffMs(%d) = %.0f, want %.0f", age, got, want)
		}
	}
	if s.NoiseScale(15) != 1 || s.NoiseScale(20) != 1.75 {
		t.Errorf("noise scale %.2f at the cliff, %.2f five laps past it", s.NoiseScale(15), s.NoiseScale(20))
	}
	// a set loses about 2 s/lap five laps past its cliff, whatever the compound
	for _, c := range []string{"SOFT", "MEDIUM", "HARD"} {
		m := Compounds[c]
		if got := m.CliffMs(m.CliffLap + 5); got != 2000 {
			t.Errorf("%s five laps past the cliff: %.0f ms", c, got)
		}
	}
}

// The pace advantage of the soft pays for a short final stint but not for a
// long one that runs it well past its cliff.
func TestSoftOnlyPaysWhenItStaysOffTheCliff(t *testing.T) {
	run := func(current int) (soft, medium float64) {
		req := baseRequest()
		req.CurrentLap = current
		req.Car.Compound, req.Car.TyreAgeLaps, req.Car.CompoundsUsed = "HARD", 20, []string{"HARD"}
		req.Car.DegSlopeMsPerLap = nil
		req.Strategies = []string{"PIT_NOW_SOFT", "PIT_NOW_MEDIUM"}
		r, err := Simulate(req)
		if err != nil {
			t.Fatal(err)
		}
		return outcome(t, r, "PIT_NOW_SOFT").MeanRaceTimeMs, outcome(t, r, "PIT_NOW_MEDIUM").MeanRaceTimeMs
	}
	if s, m := run(34); s <= m { // 23 laps on the new set: 8 laps past the soft cliff
		t.Errorf("23-lap soft stint %.1f s faster than mediums", (m-s)/1000)
	}
	if s, m := run(47); s >= m { // 10 laps: the soft never reaches its cliff
		t.Errorf("10-lap soft stint %.1f s slower than mediums", (s-m)/1000)
	}
}

// A leader on older tyres with a car about 1 s/lap faster close behind: where
// passing is easy the chaser always gets by; on a street circuit it must still
// pass on track, under the same per-lap model our car faces when attacking
// (about half the time here, over 11 laps). Before the defence model the
// faster car behind finished ahead whatever the overtaking difficulty.
func TestLeaderDefendsOnAHardToPassCircuit(t *testing.T) {
	finish := func(difficulty float64) float64 {
		req := Request{TotalLaps: 62, CurrentLap: 50, TrackStatus: "GREEN", Seed: 5, OvertakeDifficulty: &difficulty,
			Strategies: []string{"STAY_OUT_EXTEND"},
			Car: CarState{DriverCode: "SAI", Position: 1, Compound: "HARD", TyreAgeLaps: 29, BaseLapMs: 99000,
				CompoundsUsed: []string{"MEDIUM", "HARD"}},
			Field: []FieldCar{{DriverCode: "NOR", Position: 2, GapToLeaderMs: 900, Compound: "MEDIUM", TyreAgeLaps: 6}}}
		r, err := Simulate(req)
		if err != nil {
			t.Fatal(err)
		}
		return outcome(t, r, "STAY_OUT_EXTEND").ExpectedFinish
	}
	street, open := finish(0.9), finish(0.0)
	if open < 1.9 {
		t.Fatalf("with easy passing the faster chaser should get by (expected finish %.2f)", open)
	}
	if street > open-0.3 {
		t.Fatalf("overtaking difficulty must protect the leader: expected finish %.2f street vs %.2f open", street, open)
	}
}
