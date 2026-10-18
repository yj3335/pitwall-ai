package pipeline

import "testing"

// The three tables flush independently, so each keeps its own mark: telemetry
// being ahead must not hide laps or triggers that never reached disk.
func TestWatermarksAreKeptPerTable(t *testing.T) {
	w := &Watermarks{}
	if !w.admitTelemetry(1, 44, 1000) || w.admitTelemetry(1, 44, 900) || w.admitTelemetry(1, 44, 1000) {
		t.Fatal("telemetry: seq at or below the mark must be skipped")
	}
	if !w.admitTelemetry(1, 44, 1001) || !w.admitTelemetry(2, 44, 5) || !w.admitTelemetry(1, 4, 5) {
		t.Fatal("telemetry: a higher seq, another session or another car is new")
	}
	if !w.admitLap(1, 44, 30) {
		t.Fatal("laps: the lap mark is independent of the telemetry mark")
	}
	if w.admitLap(1, 44, 30) || w.admitLap(1, 44, 29) || !w.admitLap(1, 44, 31) {
		t.Fatal("laps: a lap at or below the mark must be skipped")
	}
	if !w.admitTrigger(1, 44, 5000, "UNDERCUT_WINDOW") || !w.admitTrigger(1, 44, 5000, "TIRE_CLIFF") {
		t.Fatal("triggers: two kinds at the same instant are both new")
	}
	if w.admitTrigger(1, 44, 5000, "TIRE_CLIFF") || w.admitTrigger(1, 44, 4999, "WET_CROSSOVER") {
		t.Fatal("triggers: a kind already written at the mark, or an earlier instant, must be skipped")
	}
	if !w.admitTrigger(1, 44, 5001, "TIRE_CLIFF") {
		t.Fatal("triggers: a later instant is new")
	}
	var none *Watermarks // no lakehouse configured: everything is written
	if !none.admitTelemetry(1, 1, 1) || !w.admitTelemetry(1, 250, 1) {
		t.Fatal("no marks, or a car number outside the board, must always be written")
	}
}

func TestLoadWatermarksOnAnEmptyLakehouse(t *testing.T) {
	w, err := LoadWatermarks(t.TempDir())
	if err != nil || !w.admitTelemetry(1, 44, 0) {
		t.Fatalf("empty lakehouse: err=%v", err)
	}
}
