// Command processor consumes f1.telemetry.raw (one goroutine per partition),
// maintains per-driver sliding windows with fuel-corrected degradation,
// detects strategy triggers, commits Parquet batches to the lakehouse, and
// serves the live timing tower over HTTP for the API and dashboard.
//
// With -from-file it builds the lakehouse offline from an event file without
// Kafka (used by tests and `make lakehouse`).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yj3335/pitwall-ai/internal/kafka"
	"github.com/yj3335/pitwall-ai/internal/pipeline"
	"github.com/yj3335/pitwall-ai/internal/stream"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	group := flag.String("group", "pitwall-processor", "consumer group")
	out := flag.String("out", "data/lakehouse", "lakehouse root ('' disables Parquet)")
	httpAddr := flag.String("http", ":8090", "live state HTTP address ('' disables)")
	fromFile := flag.String("from-file", "", "process an events file offline instead of Kafka")
	noTelemetry := flag.Bool("no-telemetry-table", false, "skip the raw telemetry table (laps/triggers only)")
	publish := flag.Bool("publish-triggers", true, "produce triggers to f1.triggers")
	flushRows := flag.Int("flush-rows", 20_000, "telemetry rows per committed Parquet batch")
	flag.Parse()

	// SIGTERM too (make stop, containers): a clean exit leaves the consumer group
	// and flushes pending Parquet batches instead of losing them
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := pipeline.Config{LakehouseDir: *out, WriterID: fmt.Sprintf("p%d_", os.Getpid()), WriteTelemetry: !*noTelemetry,
		FlushRows: *flushRows}

	if *fromFile != "" {
		runOffline(cfg, *fromFile)
		return
	}

	bl := strings.Split(*brokers, ",")
	if err := kafka.EnsureTopics(ctx, bl); err != nil {
		log.Fatalf("topics: %v", err)
	}
	prod, err := kafka.NewProducer(bl)
	if err != nil {
		log.Fatal(err)
	}
	defer prod.Close()
	if *publish {
		cfg.TriggerSink = func(t stream.Trigger) {
			b, _ := json.Marshal(t)
			prod.Produce(ctx, &kgo.Record{Topic: kafka.TopicTriggers, Key: kafka.DriverKey(t.DriverNumber), Value: b}, nil)
		}
	}
	p := pipeline.New(cfg)
	cl, err := kafka.NewConsumer(bl, *group, []string{kafka.TopicTelemetry, kafka.TopicRaceControl},
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	if err != nil {
		log.Fatal(err)
	}
	if *httpAddr != "" {
		publishRC := func(ctx context.Context, m stream.RaceControlMsg) error {
			b, _ := json.Marshal(m)
			return prod.ProduceSync(ctx, &kgo.Record{Topic: kafka.TopicRaceControl, Value: b}).FirstErr()
		}
		go serveHTTP(*httpAddr, p, publishRC)
	}
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				s := p.Stats()
				log.Printf("events=%d laps=%d triggers=%d files=%d p99(event)=%.1fms p99(trigger)=%.1fms",
					s.Events, s.Laps, s.Triggers, s.ParquetFiles, s.EventLatency.P99Ms, s.TriggerLatency.P99Ms)
			}
		}
	}()
	log.Printf("consuming %s from %s (group %s)", kafka.TopicTelemetry, *brokers, *group)
	if err := kafka.Run(ctx, cl, p.PartitionHandler); err != nil {
		log.Printf("consumer: %v", err)
	}
	// flush first: the sink barriers then mark the offsets of every record
	// that reached disk, which are committed before the group is left
	sinkErr := p.Close()
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := cl.CommitMarkedOffsets(cctx); err != nil {
		log.Printf("commit: %v", err)
	}
	cancel()
	cl.Close()
	if sinkErr != nil {
		log.Fatalf("sink: %v", sinkErr)
	}
}

func runOffline(cfg pipeline.Config, path string) {
	events, err := stream.ReadEvents(path)
	if err != nil {
		log.Fatal(err)
	}
	p := pipeline.New(cfg)
	eng := p.NewEngine()
	start := time.Now()
	for _, ev := range events {
		p.Apply(eng, ev)
	}
	if err := p.Close(); err != nil {
		log.Fatal(err)
	}
	s := p.Stats()
	log.Printf("processed %d events -> %d laps, %d triggers, %d parquet files (%d rows) in %s",
		s.Events, s.Laps, s.Triggers, s.ParquetFiles, s.ParquetRows, time.Since(start).Round(time.Millisecond))
}

func serveHTTP(addr string, p *pipeline.Pipeline, publish publishFunc) {
	log.Printf("live state on http://%s/v1/tower", addr)
	if err := http.ListenAndServe(addr, newHandler(p, publish)); err != nil {
		log.Printf("http: %v", err)
	}
}
