// Command replayer streams real OpenF1 race telemetry into Kafka, keyed by
// driver_number on the 20-partition f1.telemetry.raw topic, with FIA race
// control messages interleaved on f1.race_control. -speed 1 replays in real
// time, -speed 100 is a 100x stress replay, -speed 0 is as fast as possible.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yj3335/pitwall-ai/internal/kafka"
	"github.com/yj3335/pitwall-ai/internal/stream"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	files := flag.String("file", "data/races/session_9558_events.jsonl.gz", "comma-separated event files")
	speed := flag.Float64("speed", 20, "replay speed multiplier (0 = unthrottled)")
	limit := flag.Int("limit", 0, "stop after N telemetry events (0 = all)")
	fromLap := flag.Int("from-lap", 0, "skip events before this lap")
	loop := flag.Bool("loop", false, "restart from the beginning when the file ends")
	flag.Parse()

	// SIGTERM too (make stop, containers): a clean exit leaves the consumer group
	// and flushes pending Parquet batches instead of losing them
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	bl := strings.Split(*brokers, ",")
	if err := kafka.EnsureTopics(ctx, bl); err != nil {
		log.Fatalf("topics: %v", err)
	}
	prod, err := kafka.NewProducer(bl)
	if err != nil {
		log.Fatal(err)
	}
	defer prod.Close()

	var events []stream.Event
	rcBySession := map[int32][]stream.RaceControlMsg{}
	nrc := 0
	for _, f := range strings.Split(*files, ",") {
		evs, err := stream.ReadEvents(f)
		if err != nil {
			log.Fatalf("read %s: %v", f, err)
		}
		if len(evs) > 0 {
			msgs, err := stream.ReadRaceControl(filepath.Dir(f), evs[0].SessionKey)
			if err == nil {
				rcBySession[evs[0].SessionKey] = msgs
				nrc += len(msgs)
			}
		}
		events = append(events, evs...)
	}
	if *fromLap > 0 {
		kept := events[:0]
		for _, e := range events {
			if e.LapNumber >= int32(*fromLap) {
				kept = append(kept, e)
			}
		}
		events = kept
	}
	if *limit > 0 && len(events) > *limit {
		events = events[:*limit]
	}
	log.Printf("replaying %d events and %d race control messages at %.0fx", len(events), nrc, *speed)

	var produced, failed atomic.Int64
	cb := func(_ *kgo.Record, err error) {
		if err != nil {
			failed.Add(1)
			return
		}
		produced.Add(1)
	}
	start := time.Now()
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n := produced.Load()
				log.Printf("produced %d (%.0f ev/s), failed %d", n, float64(n)/time.Since(start).Seconds(), failed.Load())
			}
		}
	}()

	for pass := 0; ; pass++ {
		rcIdx := map[int32]int{}
		t0 := events[0].TsMs
		wall0 := time.Now()
		session := events[0].SessionKey
		for i := range events {
			if ctx.Err() != nil {
				break
			}
			ev := events[i]
			if ev.SessionKey != session {
				// files are replayed back to back: restart the pacing clock per race
				session, t0, wall0 = ev.SessionKey, ev.TsMs, time.Now()
			}
			if *speed > 0 {
				due := wall0.Add(time.Duration(float64(ev.TsMs-t0)/(*speed)) * time.Millisecond)
				if d := time.Until(due); d > 0 {
					time.Sleep(d)
				}
			}
			rc := rcBySession[ev.SessionKey]
			for rcIdx[ev.SessionKey] < len(rc) && rc[rcIdx[ev.SessionKey]].TsMs <= ev.TsMs {
				b, _ := json.Marshal(rc[rcIdx[ev.SessionKey]])
				prod.Produce(ctx, &kgo.Record{Topic: kafka.TopicRaceControl, Value: b}, cb)
				rcIdx[ev.SessionKey]++
			}
			ev.IngestMs = time.Now().UnixMilli()
			prod.Produce(ctx, &kgo.Record{Topic: kafka.TopicTelemetry, Key: kafka.DriverKey(ev.DriverNumber), Value: ev.Encode()}, cb)
		}
		if !*loop || ctx.Err() != nil {
			break
		}
		log.Printf("pass %d complete, looping", pass+1)
	}
	if err := prod.Flush(context.Background()); err != nil {
		log.Printf("flush: %v", err)
	}
	el := time.Since(start)
	log.Printf("done: %d records in %s (%.0f rec/s), %d failed", produced.Load(), el.Round(time.Millisecond),
		float64(produced.Load())/el.Seconds(), failed.Load())
}
