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
