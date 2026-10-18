package pipeline

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"

	pq "github.com/parquet-go/parquet-go"

	"github.com/yj3335/pitwall-ai/internal/stream"
)

// Watermarks record, per session and car, what each lakehouse table already
// holds, so records Kafka delivers again (after a crash, a restart or a
// rebalance: delivery is at-least-once) are not written twice. Replayed events
// still go through the engines, which rebuild their windows from them; only
// the Parquet rows are skipped. Each table has its own mark, because the three
// sinks flush independently: one mark for all would lose the rows of a table
// that had not flushed yet.
//
//	telemetry: highest event seq (the replayer numbers each race's events)
//	laps:      highest completed lap number
//	triggers:  latest trigger time, plus the kinds already written at that time
//
// The marks are loaded once from the Parquet files at startup and kept up to
// date as rows are handed to the sinks. Reads (DuckDB) are untouched.
type Watermarks struct {
	cars    [stream.MaxDriverNumber]carMarks
	Skipped atomic.Int64 // rows not written because the lakehouse already had them
}

type carMarks struct {
	mu   sync.Mutex // a car always arrives on one partition: uncontended
	tel  map[int32]int64
	lap  map[int32]int32
	trig map[int32]trigMark
}

type trigMark struct {
	ts    int64
	kinds map[string]bool // kinds written at ts
}

// key columns read back at startup (parquet-go reads only these columns)
type telKey struct {
	SessionKey   int32 `parquet:"session_key"`
	DriverNumber int32 `parquet:"driver_number"`
	Seq          int64 `parquet:"seq"`
}

type lapKey struct {
	SessionKey   int32 `parquet:"session_key"`
	DriverNumber int32 `parquet:"driver_number"`
	LapNumber    int32 `parquet:"lap_number"`
}

type trigKey struct {
	SessionKey   int32  `parquet:"session_key"`
	DriverNumber int32  `parquet:"driver_number"`
	TsMs         int64  `parquet:"ts_ms"`
	Kind         string `parquet:"kind"`
}

// LoadWatermarks scans the committed Parquet files under root (in-flight
// temp files never match *.parquet). An empty or missing lakehouse gives
// empty marks.
func LoadWatermarks(root string) (*Watermarks, error) {
	w := &Watermarks{}
	if err := scan(root, "telemetry", func(r telKey) { w.admitTelemetry(r.SessionKey, r.DriverNumber, r.Seq) }); err != nil {
		return w, err
	}
	if err := scan(root, "laps", func(r lapKey) { w.admitLap(r.SessionKey, r.DriverNumber, r.LapNumber) }); err != nil {
		return w, err
	}
	err := scan(root, "triggers", func(r trigKey) { w.admitTrigger(r.SessionKey, r.DriverNumber, r.TsMs, r.Kind) })
	return w, err
}

func scan[T any](root, table string, fn func(T)) error {
	files, err := filepath.Glob(filepath.Join(root, table, "session=*", "*.parquet"))
	if err != nil {
		return err
	}
	for _, f := range files {
		rows, err := pq.ReadFile[T](f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		for _, r := range rows {
			fn(r)
		}
	}
	return nil
}

func (w *Watermarks) car(driver int32) *carMarks {
	if w == nil || driver < 0 || int(driver) >= len(w.cars) {
		return nil // no mark: always written
	}
	return &w.cars[driver]
}

// admitTelemetry reports whether an event's telemetry row is new, and if so
// raises the mark. Each car's seq only grows within a race.
func (w *Watermarks) admitTelemetry(session, driver int32, seq int64) bool {
	c := w.car(driver)
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tel == nil {
		c.tel = map[int32]int64{}
	}
	if last, ok := c.tel[session]; ok && seq <= last {
		return false
	}
	c.tel[session] = seq
	return true
}

func (w *Watermarks) admitLap(session, driver, lap int32) bool {
	c := w.car(driver)
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lap == nil {
		c.lap = map[int32]int32{}
	}
	if last, ok := c.lap[session]; ok && lap <= last {
		return false
	}
	c.lap[session] = lap
	return true
}

func (w *Watermarks) admitTrigger(session, driver int32, ts int64, kind string) bool {
	c := w.car(driver)
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.trig == nil {
		c.trig = map[int32]trigMark{}
	}
	m, ok := c.trig[session]
	switch {
	case !ok || ts > m.ts:
		c.trig[session] = trigMark{ts: ts, kinds: map[string]bool{kind: true}}
		return true
	case ts < m.ts || m.kinds[kind]:
		return false
	default: // another kind at the same instant
		m.kinds[kind] = true
		return true
	}
}
