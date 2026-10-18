package pipeline_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yj3335/pitwall-ai/internal/kafka"
	"github.com/yj3335/pitwall-ai/internal/pipeline"
	"github.com/yj3335/pitwall-ai/internal/stream"
)

func brokers(t *testing.T) []string {
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		b = "localhost:9092"
	}
	conn, err := net.DialTimeout("tcp", strings.Split(b, ",")[0], 500*time.Millisecond)
	if err != nil {
		t.Skipf("no Kafka broker at %s", b)
	}
	conn.Close()
	return strings.Split(b, ",")
}

func committed(t *testing.T, adm *kadm.Client, group, topic string) int64 {
	t.Helper()
	offs, err := adm.FetchOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	offs.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic && o.At > 0 {
			sum += o.At
		}
	})
	return sum
}

// Kafka offsets are committed only for records whose rows are on disk. While
// the rows sit in the Parquet buffers nothing is committed (a hard crash would
// re-read them); after the flush every offset is.
func TestOffsetsCommitOnlyAfterParquetIsDurable(t *testing.T) {
	bl := brokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := kgo.NewClient(kgo.SeedBrokers(bl...))
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	adm := kadm.NewClient(admin)
	topic := fmt.Sprintf("pitwall.test.durable.%d", time.Now().UnixNano())
	if _, err := adm.CreateTopic(ctx, 4, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { adm.DeleteTopics(context.Background(), topic) })

	const n = 400
	prod, err := kafka.NewProducer(bl)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		ev := stream.Event{TsMs: int64(1000 * i), Seq: int64(i), SessionKey: 1, DriverNumber: int32(1 + i%4),
			LapNumber: int32(2 + i/40), Compound: "MEDIUM", Position: int32(1 + i%4), TrackStatus: stream.StatusGreen}
		prod.Produce(ctx, &kgo.Record{Topic: topic, Key: kafka.DriverKey(ev.DriverNumber), Value: ev.Encode()}, nil)
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	prod.Close()

	dir := t.TempDir()
	p := pipeline.New(pipeline.Config{LakehouseDir: dir, WriterID: "t_", WriteTelemetry: true,
		FlushRows: 1_000_000, FlushEvery: time.Hour}) // rows stay buffered until Close
	group := "pitwall-test-" + topic
	cl, err := kafka.NewConsumer(bl, group, []string{topic}, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	rctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- kafka.Run(rctx, cl, p.PartitionHandler) }()
	for p.Events.Load() < n && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(2500 * time.Millisecond) // more than two auto-commit intervals
	if c := committed(t, adm, group, topic); c != 0 {
		t.Fatalf("%d offsets committed while their rows were only in memory", c)
	}

	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil { // flush: the barriers now mark every offset
		t.Fatal(err)
	}
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		t.Fatal(err)
	}
	cl.Close()
	if c := committed(t, adm, group, topic); c != n {
		t.Fatalf("committed %d of %d offsets after the flush", c, n)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "telemetry", "session=1", "*.parquet"))
	if len(files) == 0 || p.Stats().ParquetRows < n {
		t.Fatalf("telemetry not on disk: %d files, %d rows", len(files), p.Stats().ParquetRows)
	}
}

type lapRow struct {
	Driver int32 `parquet:"driver_number"`
	Lap    int32 `parquet:"lap_number"`
}

type trigRow struct {
	Driver int32  `parquet:"driver_number"`
	Ts     int64  `parquet:"ts_ms"`
	Kind   string `parquet:"kind"`
}

func readAll[T any](t *testing.T, dir, table string) []T {
	t.Helper()
	files, _ := filepath.Glob(filepath.Join(dir, table, "session=*", "*.parquet"))
	var out []T
	for _, f := range files {
		rows, err := pq.ReadFile[T](f)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rows...)
	}
	return out
}

// A restart re-reads records from the last committed offset (at-least-once).
// The second processor, on the same lakehouse, must write none of the rows
// the first one already committed, in any of the three tables, and must still
// write everything new.
func TestRestartDoesNotDuplicateLakehouseRows(t *testing.T) {
	events, err := stream.ReadEvents("../../data/races/session_9165_events.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	events = events[:30_000]
	run := func(dir string, evs []stream.Event) *pipeline.Pipeline {
		p := pipeline.New(pipeline.Config{LakehouseDir: dir, WriterID: fmt.Sprintf("w%d_", time.Now().UnixNano()), WriteTelemetry: true})
		eng := p.NewEngine()
		for _, ev := range evs {
			p.Apply(eng, ev)
		}
		if err := p.Close(); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ref := t.TempDir()
	run(ref, events)
	dir := t.TempDir()
	run(dir, events[:20_000])
	second := run(dir, events[15_000:]) // 5,000 events delivered again
	if got := second.Stats().SkippedRows; got < 5_000 {
		t.Fatalf("second run skipped %d rows, want at least the 5,000 re-delivered telemetry rows", got)
	}

	tel := readAll[telKeyRow](t, dir, "telemetry")
	seen := map[int64]bool{}
	for _, r := range tel {
		if seen[r.Seq] {
			t.Fatalf("telemetry seq %d written twice", r.Seq)
		}
		seen[r.Seq] = true
	}
	if len(tel) != len(events) {
		t.Fatalf("telemetry rows %d, want %d", len(tel), len(events))
	}
	lapSet := func(rows []lapRow) map[lapRow]int {
		m := map[lapRow]int{}
		for _, r := range rows {
			m[r]++
		}
		return m
	}
	laps, refLaps := lapSet(readAll[lapRow](t, dir, "laps")), lapSet(readAll[lapRow](t, ref, "laps"))
	for k, n := range laps {
		if n > 1 {
			t.Fatalf("lap %+v written %d times", k, n)
		}
	}
	if len(laps) != len(refLaps) {
		t.Fatalf("%d distinct laps, want %d (as in one uninterrupted run)", len(laps), len(refLaps))
	}
	trig := map[trigRow]int{}
	for _, r := range readAll[trigRow](t, dir, "triggers") {
		if trig[r]++; trig[r] > 1 {
			t.Fatalf("trigger %+v written twice", r)
		}
	}
}

type telKeyRow struct {
	Seq int64 `parquet:"seq"`
}
