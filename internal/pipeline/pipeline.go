// Package pipeline wires the stream engine to the lakehouse sinks and the
// live board. It is shared by cmd/processor and cmd/bench-stream.
package pipeline

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yj3335/pitwall-ai/internal/kafka"
	"github.com/yj3335/pitwall-ai/internal/metrics"
	pq "github.com/yj3335/pitwall-ai/internal/parquet"
	"github.com/yj3335/pitwall-ai/internal/stream"
)

// Config controls the sinks.
type Config struct {
	LakehouseDir   string
	WriterID       string // file-name prefix, unique per process
	FlushRows      int
	FlushEvery     time.Duration
	WriteTelemetry bool
	TriggerSink    func(stream.Trigger) // optional: e.g. publish to f1.triggers
}

// Pipeline is the processor core.
type Pipeline struct {
	Board *stream.Board
	cfg   Config

	telemetry *pq.Sink[stream.Event]
	laps      *pq.Sink[stream.LapRecord]
	triggers  *pq.Sink[stream.Trigger]

	EventLatency   metrics.Hist // Kafka produce -> processed
	TriggerLatency metrics.Hist // Kafka produce -> trigger detected
	Events         atomic.Int64
	Laps           atomic.Int64
	TriggerCount   atomic.Int64
	DecodeErrors   atomic.Int64
	started        time.Time

	mu       sync.Mutex // guards the small HTTP-facing rings only
	recent   []stream.Trigger
	rcRecent []stream.RaceControlMsg
}

// New creates the pipeline and starts its sink goroutines.
func New(cfg Config) *Pipeline {
	if cfg.FlushRows == 0 {
		cfg.FlushRows = 20_000
	}
	if cfg.FlushEvery == 0 {
		cfg.FlushEvery = 2 * time.Second
	}
	p := &Pipeline{Board: stream.NewBoard(), cfg: cfg, started: time.Now()}
	if cfg.LakehouseDir != "" {
		if cfg.WriteTelemetry {
			p.telemetry = pq.NewSink(pq.NewBatchWriter[stream.Event](cfg.LakehouseDir, "telemetry", cfg.WriterID), cfg.FlushRows, cfg.FlushEvery)
		}
		lapRows, trigRows := min(2_000, cfg.FlushRows), min(500, cfg.FlushRows)
		p.laps = pq.NewSink(pq.NewBatchWriter[stream.LapRecord](cfg.LakehouseDir, "laps", cfg.WriterID), lapRows, cfg.FlushEvery)
		p.triggers = pq.NewSink(pq.NewBatchWriter[stream.Trigger](cfg.LakehouseDir, "triggers", cfg.WriterID), trigRows, cfg.FlushEvery)
	}
	return p
}

// NewEngine returns an engine bound to this pipeline's board. Use one per
// partition / goroutine.
func (p *Pipeline) NewEngine() *stream.Engine { return stream.NewEngine(p.Board) }

// Apply processes one decoded event with the given engine.
func (p *Pipeline) Apply(eng *stream.Engine, ev stream.Event) {
	res := eng.Process(ev)
	now := time.Now().UnixMilli()
	if ev.IngestMs > 0 {
		p.EventLatency.RecordMicros((time.Now().UnixMicro()) - ev.IngestMs*1000)
	}
	p.Events.Add(1)
	if p.telemetry != nil {
		p.telemetry.Add(ev)
	}
	if res.Lap != nil {
		p.Laps.Add(1)
		if p.laps != nil {
			p.laps.Add(*res.Lap)
		}
	}
	for _, t := range res.Triggers {
		p.TriggerCount.Add(1)
		if t.ProducedMs > 0 {
			p.TriggerLatency.RecordMicros((now - t.ProducedMs) * 1000)
		}
		if p.triggers != nil {
			p.triggers.Add(t)
		}
		if p.cfg.TriggerSink != nil {
			p.cfg.TriggerSink(t)
		}
		p.mu.Lock()
		p.recent = append(p.recent, t)
		if len(p.recent) > 500 {
			p.recent = p.recent[len(p.recent)-500:]
		}
		p.mu.Unlock()
	}
}

// PartitionHandler returns a Kafka handler owning one Engine. Each partition
// gets its own handler and goroutine, so engines never share state.
func (p *Pipeline) PartitionHandler(topic string, _ int32) kafka.Handler {
	if topic == kafka.TopicRaceControl {
		return func(_ int32, recs []*kgo.Record) {
			for _, r := range recs {
				if m, err := decodeRC(r.Value); err == nil {
					p.mu.Lock()
					p.rcRecent = append(p.rcRecent, m)
					if len(p.rcRecent) > 200 {
						p.rcRecent = p.rcRecent[len(p.rcRecent)-200:]
					}
					p.mu.Unlock()
				}
			}
		}
	}
	eng := p.NewEngine()
	return func(_ int32, recs []*kgo.Record) {
		for _, r := range recs {
			ev, err := stream.DecodeEvent(r.Value)
			if err != nil {
				p.DecodeErrors.Add(1)
				continue
			}
			if ev.IngestMs == 0 {
				ev.IngestMs = r.Timestamp.UnixMilli()
			}
			p.Apply(eng, ev)
		}
	}
}

// RecentTriggers returns up to n most recent triggers, newest last.
func (p *Pipeline) RecentTriggers(n int) []stream.Trigger {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n > len(p.recent) {
		n = len(p.recent)
	}
	return append([]stream.Trigger(nil), p.recent[len(p.recent)-n:]...)
}

// RecentRaceControl returns up to n recent race control messages.
func (p *Pipeline) RecentRaceControl(n int) []stream.RaceControlMsg {
	p.mu.Lock()
	defer p.mu.Unlock()
	if n > len(p.rcRecent) {
		n = len(p.rcRecent)
	}
	return append([]stream.RaceControlMsg(nil), p.rcRecent[len(p.rcRecent)-n:]...)
}

// Stats is a JSON-friendly pipeline summary.
type Stats struct {
	Events         int64           `json:"events"`
	Laps           int64           `json:"laps"`
	Triggers       int64           `json:"triggers"`
	DecodeErrors   int64           `json:"decode_errors"`
	UptimeS        float64         `json:"uptime_s"`
	EventsPerSec   float64         `json:"events_per_sec"`
	EventLatency   metrics.Summary `json:"event_latency"`
	TriggerLatency metrics.Summary `json:"trigger_latency"`
	ParquetFiles   int64           `json:"parquet_files"`
	ParquetRows    int64           `json:"parquet_rows"`
	SinkErrors     int64           `json:"sink_errors"`
}

// Stats returns live counters.
func (p *Pipeline) Stats() Stats {
	up := time.Since(p.started).Seconds()
	s := Stats{
		Events: p.Events.Load(), Laps: p.Laps.Load(), Triggers: p.TriggerCount.Load(),
		DecodeErrors: p.DecodeErrors.Load(), UptimeS: up,
		EventLatency: p.EventLatency.Summarize(), TriggerLatency: p.TriggerLatency.Summarize(),
	}
	if up > 0 {
		s.EventsPerSec = float64(s.Events) / up
	}
	if p.telemetry != nil {
		s.ParquetFiles += p.telemetry.Writer().Files()
		s.ParquetRows += p.telemetry.Writer().Rows()
		s.SinkErrors += p.telemetry.Errors()
	}
	if p.laps != nil {
		s.ParquetFiles += p.laps.Writer().Files()
		s.ParquetRows += p.laps.Writer().Rows()
		s.SinkErrors += p.laps.Errors()
		s.ParquetFiles += p.triggers.Writer().Files()
		s.ParquetRows += p.triggers.Writer().Rows()
		s.SinkErrors += p.triggers.Errors()
	}
	return s
}

// ResetClock restarts the throughput clock (used by benchmarks).
func (p *Pipeline) ResetClock() { p.started = time.Now() }

// Close flushes every sink.
func (p *Pipeline) Close() error {
	var first error
	if p.telemetry != nil {
		if err := p.telemetry.Close(); err != nil && first == nil {
			first = err
		}
	}
	if p.laps != nil {
		if err := p.laps.Close(); err != nil && first == nil {
			first = err
		}
		if err := p.triggers.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func decodeRC(b []byte) (stream.RaceControlMsg, error) {
	var m stream.RaceControlMsg
	err := json.Unmarshal(b, &m)
	return m, err
}
