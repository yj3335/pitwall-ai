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
	"strconv"
	"strings"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
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
	var prod *kgo.Client
	if *publish {
		var err error
		if prod, err = kafka.NewProducer(bl); err != nil {
			log.Fatal(err)
		}
		defer prod.Close()
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
		go serveHTTP(*httpAddr, p)
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
	cl.Close()
	if err := p.Close(); err != nil {
		log.Fatalf("sink: %v", err)
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

func serveHTTP(addr string, p *pipeline.Pipeline) {
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		_ = json.NewEncoder(w).Encode(v)
	}
	mux.HandleFunc("GET /v1/tower", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"drivers": p.Board.All(), "stats": p.Stats()})
	})
	mux.HandleFunc("GET /v1/driver/{n}", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.PathValue("n"))
		s := p.Board.Driver(int32(n))
		if s == nil {
			http.Error(w, "unknown driver", http.StatusNotFound)
			return
		}
		writeJSON(w, s)
	})
	mux.HandleFunc("GET /v1/triggers", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, p.RecentTriggers(100))
	})
	mux.HandleFunc("GET /v1/race-control", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, p.RecentRaceControl(50))
	})
	mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, p.Stats()) })
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	log.Printf("live state on http://%s/v1/tower", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("http: %v", err)
	}
}
