package stream

import (
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func TestDegWindowFuelCorrectedSlope(t *testing.T) {
	var w DegWindow
	w.Reset(10)
	// true tyre wear of +80 ms/lap, masked by -35 ms/lap of fuel burn-off:
	// raw lap times only rise by 45 ms/lap
	for lap := int32(11); lap <= 15; lap++ {
		raw := int64(95000 + 45*(lap-11))
		w.Push(lap, raw)
	}
	if got := w.Slope(); math.Abs(got-80) > 1e-9 {
		t.Fatalf("fuel-corrected slope = %.3f, want 80", got)
	}
	// window keeps only the last 5 laps
	w.Push(16, 95000+45*5)
	if w.Len() != WindowLaps || w.Samples()[0].Lap != 12 {
		t.Fatalf("ring eviction broken: len=%d first=%d", w.Len(), w.Samples()[0].Lap)
	}
}

func TestDegWindowNeedsThreeLapsAndRejectsOutliers(t *testing.T) {
	var w DegWindow
	w.Reset(1)
	w.Push(2, 90000)
	w.Push(3, 90100)
	if !math.IsNaN(w.Slope()) {
		t.Fatal("slope must be NaN with fewer than 3 laps")
	}
	if !w.IsOutlier(w.FuelCorrect(4, 99000)) { // ~110% of median
		t.Fatal("9s-slow lap should be an outlier")
	}
	if w.IsOutlier(w.FuelCorrect(4, 90300)) {
		t.Fatal("normal lap flagged as outlier")
	}
}

func TestDecodeEventNulls(t *testing.T) {
	ev, err := DecodeEvent([]byte(`{"ts_ms":1,"session_key":9558,"driver_number":44,"driver_code":"HAM","lap_number":3,
		"gap_to_leader_ms":null,"interval_ahead_ms":1200,"interval_behind_ms":null,"track_temp_c":null,"compound":"SOFT"}`))
	if err != nil {
		t.Fatal(err)
	}
	if ev.GapToLeaderMs != Unknown || ev.IntervalBehindMs != Unknown || ev.IntervalAheadMs != 1200 {
		t.Fatalf("null mapping wrong: %+v", ev)
	}
	if _, err := DecodeEvent([]byte(`{"driver_number":0}`)); err == nil {
		t.Fatal("driver 0 must be rejected")
	}
}

// lapEvents builds one event per lap for a car with a given raw lap time model.
func lapEvents(dn int32, code string, pos int32, laps int, lapTime func(lap int32) int64, ahead int64) []Event {
	var out []Event
	for lap := int32(1); lap <= int32(laps); lap++ {
		var last int64
		if lap > 1 {
			last = lapTime(lap - 1)
		}
		out = append(out, Event{TsMs: int64(lap) * 90000, SessionKey: 1, DriverNumber: dn, DriverCode: code,
			LapNumber: lap, Position: pos, Compound: "MEDIUM", TyreAgeLaps: lap - 1, LapTimeMs: last,
			IntervalAheadMs: ahead, IntervalBehindMs: Unknown, GapToLeaderMs: 0, TrackStatus: StatusGreen})
	}
	return out
}

func TestEngineLapRecordsExcludeStartAndPitLaps(t *testing.T) {
	eng := NewEngine(NewBoard())
	evs := lapEvents(16, "LEC", 1, 10, func(l int32) int64 { return 92000 + 60*int64(l) }, Unknown)
	// pit at end of lap 6: lap 7 is an out-lap on new HARD tyres
	for i := range evs {
		if evs[i].LapNumber >= 7 {
			evs[i].Compound = "HARD"
			evs[i].TyreAgeLaps = evs[i].LapNumber - 7
		}
		if evs[i].LapNumber == 7 {
			evs[i].IsPitOutLap = true
		}
	}
	var recs []*LapRecord
	for _, ev := range evs {
		if r := eng.Process(ev).Lap; r != nil {
			recs = append(recs, r)
		}
	}
	clean := map[int32]bool{}
	for _, r := range recs {
		clean[r.LapNumber] = r.IsClean
	}
	for lap, want := range map[int32]bool{1: false, 2: true, 5: true, 6: false, 7: false, 8: true} {
		if clean[lap] != want {
			t.Errorf("lap %d clean=%v want %v", lap, clean[lap], want)
		}
	}
	s := eng.board.Driver(16)
	if s.PitStops != 1 || len(s.CompoundsUsed) != 2 {
		t.Fatalf("pit stops %d compounds %v", s.PitStops, s.CompoundsUsed)
	}
}

func TestUndercutTriggerAcrossPartitions(t *testing.T) {
	board := NewBoard()
	// the leader and the chaser live on different partitions -> different engines
	leader, chaser := NewEngine(board), NewEngine(board)
	worn := func(l int32) int64 { return 95000 + 150*int64(l) } // ~185 ms/lap fuel-corrected
	for _, ev := range lapEvents(1, "VER", 1, 8, worn, Unknown) {
		leader.Process(ev)
	}
	var got []Trigger
	for _, ev := range lapEvents(4, "NOR", 2, 8, func(l int32) int64 { return 95000 }, 1800) {
		got = append(got, chaser.Process(ev).Triggers...)
	}
	var undercut *Trigger
	for i := range got {
		if got[i].Kind == TriggerUndercut {
			undercut = &got[i]
			break
		}
	}
	if undercut == nil || undercut.RivalDriverNumber != 1 || undercut.Value <= UndercutRivalDegMs {
		t.Fatalf("expected undercut on VER, got %+v", got)
	}
}

func TestCheapPitWindowFiresOncePerNeutralisation(t *testing.T) {
	eng := NewEngine(NewBoard())
	base := Event{SessionKey: 1, DriverNumber: 55, DriverCode: "SAI", LapNumber: 20, Position: 1, Compound: "MEDIUM",
		IntervalAheadMs: Unknown, IntervalBehindMs: Unknown}
	n := 0
	for i, st := range []string{StatusGreen, StatusSC, StatusSC, StatusSC, StatusGreen, StatusVSC, StatusVSC} {
		ev := base
		ev.TsMs = int64(i)
		ev.TrackStatus = st
		if st == StatusGreen && i > 0 {
			ev.LapNumber = 21 + int32(i) // lap boundary resets the period
		}
		for _, tr := range eng.Process(ev).Triggers {
			if tr.Kind == TriggerCheapPit {
				n++
			}
		}
	}
	if n != 2 {
		t.Fatalf("cheap pit window fired %d times, want 2 (one SC, one VSC)", n)
	}
}

func TestRainOnSlicksTriggersCrossover(t *testing.T) {
	eng := NewEngine(NewBoard())
	ev := Event{SessionKey: 1, DriverNumber: 44, DriverCode: "HAM", LapNumber: 27, Compound: "MEDIUM", Rainfall: 1,
		TrackStatus: StatusGreen, IntervalAheadMs: Unknown}
	tr := eng.Process(ev).Triggers
	if len(tr) != 1 || tr[0].Kind != TriggerWetCross {
		t.Fatalf("want WET_CROSSOVER, got %+v", tr)
	}
	if again := eng.Process(ev).Triggers; len(again) != 0 {
		t.Fatalf("crossover must not repeat while it keeps raining: %+v", again)
	}
}

// TestConcurrentEnginesSharedBoard runs one engine per goroutine over real
// race data while readers scan the board, under the race detector.
func TestConcurrentEnginesSharedBoard(t *testing.T) {
	path := filepath.Join("..", "..", "data", "races", "session_9165_events.jsonl.gz")
	if _, err := os.Stat(path); err != nil {
		t.Skip("race data not present")
	}
	events, err := ReadEvents(path)
	if err != nil {
		t.Fatal(err)
	}
	board := NewBoard()
	workers := runtime.NumCPU()
	parts := make([][]Event, workers)
	for _, ev := range events {
		parts[int(ev.DriverNumber)%workers] = append(parts[int(ev.DriverNumber)%workers], ev)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var readerWG sync.WaitGroup
	readerWG.Add(1)
	go func() { // concurrent reader, like the MCP server or HTTP API
		defer readerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				for _, s := range board.All() {
					_ = s.Deg()
					_ = board.AtPosition(s.Position)
				}
			}
		}
	}()
	cheap := make([]int, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			eng := NewEngine(board)
			for _, ev := range parts[w] {
				for _, tr := range eng.Process(ev).Triggers {
					if tr.Kind == TriggerCheapPit {
						cheap[w]++
					}
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	readerWG.Wait()
	if board.Events() != int64(len(events)) {
		t.Fatalf("board saw %d events, want %d", board.Events(), len(events))
	}
	total := 0
	for _, c := range cheap {
		total += c
	}
	// Singapore 2023 had one SC (lap 20) and one VSC (lap 44): every running
	// car gets a cheap-stop trigger for each
	if total < 30 {
		t.Fatalf("expected SC+VSC cheap pit windows for the field, got %d", total)
	}
	if len(board.All()) < 19 {
		t.Fatalf("board has %d cars", len(board.All()))
	}
}

// A car that retired early (no telemetry for over a minute of race time) and
// cars from a previously replayed session must not appear on the live tower.
func TestActiveDropsRetiredAndOldSessionCars(t *testing.T) {
	b := NewBoard()
	eng := NewEngine(b)
	mk := func(session, dn int32, code string, pos int32, ts int64) Event {
		return Event{TsMs: ts, SessionKey: session, DriverNumber: dn, DriverCode: code, LapNumber: 5, Position: pos,
			Compound: "HARD", TrackStatus: StatusGreen, IntervalAheadMs: Unknown}
	}
	eng.Process(mk(9558, 44, "HAM", 1, 1_000))             // older session
	eng.Process(mk(9165, 22, "TSU", 19, 2_000_000))        // retired on lap 1
	eng.Process(mk(9165, 55, "SAI", 1, 2_000_000+300_000)) // running
	eng.Process(mk(9165, 4, "NOR", 2, 2_000_000+299_500))
	var codes []string
	for _, s := range b.Active() {
		codes = append(codes, s.DriverCode)
	}
	if len(codes) != 2 || codes[0] != "SAI" || codes[1] != "NOR" {
		t.Fatalf("active cars %v, want [SAI NOR]", codes)
	}
	if len(b.All()) != 4 {
		t.Fatalf("All() must still return every car seen")
	}
}
