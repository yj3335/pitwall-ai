// Command bench-stream measures the Kafka -> Go processor -> Parquet pipeline
// on real OpenF1 events. Each run uses its own 20-partition topic (deleted
// afterwards) and a fresh consumer group, so it never interferes with a live
// processor. It reports producer and end-to-end throughput, produce->processed
// and produce->trigger latency percentiles, Parquet output and, optionally,
// the in-process latency of 10,000-rollout Monte Carlo simulations.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/yj3335/pitwall-ai/internal/kafka"
	"github.com/yj3335/pitwall-ai/internal/metrics"
	"github.com/yj3335/pitwall-ai/internal/pipeline"
	"github.com/yj3335/pitwall-ai/internal/sim"
	"github.com/yj3335/pitwall-ai/internal/stream"
)

type result struct {
	Mode               string          `json:"mode"`
	Events             int             `json:"events"`
	Sessions           []int32         `json:"sessions"`
	Partitions         int             `json:"partitions"`
	TargetRate         float64         `json:"target_rate_events_per_s"`
	ProduceSeconds     float64         `json:"produce_seconds"`
	ProduceRate        float64         `json:"producer_events_per_s"`
	EndToEndSeconds    float64         `json:"end_to_end_seconds"`
	EndToEndRate       float64         `json:"sustained_events_per_s"`
	CommittedSeconds   float64         `json:"committed_seconds"`
	CommittedRate      float64         `json:"committed_events_per_s"`
	EventLatency       metrics.Summary `json:"produce_to_processed_latency"`
	TriggerLatency     metrics.Summary `json:"stream_to_trigger_latency"`
	Triggers           int64           `json:"triggers"`
	Laps               int64           `json:"laps"`
	ParquetFiles       int64           `json:"parquet_files"`
	ParquetRows        int64           `json:"parquet_rows"`
	SinkErrors         int64           `json:"sink_errors"`
	DecodeErrors       int64           `json:"decode_errors"`
	Lakehouse          string          `json:"lakehouse"`
	TelemetryFilesSeen int             `json:"telemetry_files_on_disk"`
}

type simResult struct {
	Runs        int     `json:"runs"`
	Rollouts    int     `json:"rollouts_per_strategy"`
	Strategies  int     `json:"strategies"`
	Workers     int     `json:"workers"`
	P50Ms       float64 `json:"p50_ms"`
	P95Ms       float64 `json:"p95_ms"`
	P99Ms       float64 `json:"p99_ms"`
	MeanMs      float64 `json:"mean_ms"`
	OneStratP50 float64 `json:"single_strategy_p50_ms"`
}

func main() {
	brokers := flag.String("brokers", "localhost:9092", "Kafka brokers")
	files := flag.String("files", "data/races/session_9165_events.jsonl.gz,data/races/session_9636_events.jsonl.gz,data/races/session_9558_events.jsonl.gz", "event files, used in order")
	n := flag.Int("events", 200_000, "events to stream")
	rate := flag.Float64("rate", 0, "events per second (0 = unthrottled burst)")
	out := flag.String("out", "", "lakehouse directory for this run (default: a temp dir)")
	jsonOut := flag.String("json", "", "write the result as JSON to this file")
	simRuns := flag.Int("sim-runs", 0, "also time this many 10,000-rollout simulations in-process")
	trials := flag.Int("trials", 1, "measured runs (fresh topic and group each); the median is reported")
	warmup := flag.Int("warmup", 1, "unmeasured warm-up runs first (Go heap and GC targets start tiny in a fresh process)")
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	bl := strings.Split(*brokers, ",")

	events := loadEvents(strings.Split(*files, ","), *n)
	if len(events) < *n {
		log.Fatalf("only %d events available, need %d", len(events), *n)
	}
	dir := *out
	if dir == "" {
		d, err := os.MkdirTemp("", "pitwall-bench-*")
		if err != nil {
			log.Fatal(err)
		}
		dir = d
	}

	var cold []result
	for t := 0; t < *warmup; t++ {
		r := runStream(ctx, bl, events, *rate, filepath.Join(dir, fmt.Sprintf("warmup_%d", t)))
		log.Printf("warm-up %d (not in the median): %.0f events/s end to end, trigger p99 %.1f ms", t+1, r.EndToEndRate, r.TriggerLatency.P99Ms)
		cold = append(cold, r)
	}
	var runs []result
	for t := 0; t < *trials; t++ {
		r := runStream(ctx, bl, events, *rate, filepath.Join(dir, fmt.Sprintf("trial_%d", t)))
		log.Printf("trial %d: %.0f events/s end to end, trigger p99 %.1f ms", t+1, r.EndToEndRate, r.TriggerLatency.P99Ms)
		runs = append(runs, r)
	}
	report := map[string]any{"warmup": cold, "trials": runs, "median": medianOf(runs)}
	if *simRuns > 0 {
		report["monte_carlo"] = benchSim(*simRuns)
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if *jsonOut != "" {
		if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
			log.Fatal(err)
		}
	}
}

func loadEvents(files []string, n int) []stream.Event {
	var out []stream.Event
	for _, f := range files {
		evs, err := stream.ReadEvents(f)
		if err != nil {
			log.Fatalf("read %s: %v", f, err)
		}
		out = append(out, evs...)
		if len(out) >= n {
			return out[:n]
		}
	}
	return out
}

// waitForLeaders blocks until every partition of a new topic has a leader, so
// the timed run does not include the broker finishing the topic creation.
func waitForLeaders(ctx context.Context, adm *kadm.Client, topic string) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		md, err := adm.Metadata(ctx, topic)
		if err == nil {
			if td, ok := md.Topics[topic]; ok && td.Err == nil && len(td.Partitions) == kafka.TelemetryPartitions {
				ready := true
				for _, p := range td.Partitions {
					ready = ready && p.Err == nil && p.Leader >= 0
				}
				if ready {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	log.Fatalf("topic %s: partitions without a leader after 30s", topic)
}

func runStream(ctx context.Context, bl []string, events []stream.Event, rate float64, dir string) result {
	topic := fmt.Sprintf("f1.telemetry.bench.%d", time.Now().UnixNano())
	admin, err := kgo.NewClient(kgo.SeedBrokers(bl...))
	if err != nil {
		log.Fatal(err)
	}
	adm := kadm.NewClient(admin)
	if _, err := adm.CreateTopic(ctx, kafka.TelemetryPartitions, 1, nil, topic); err != nil {
		log.Fatalf("create topic: %v", err)
	}
	waitForLeaders(ctx, adm, topic)
	defer func() {
		adm.DeleteTopics(context.Background(), topic)
		admin.Close()
	}()

	p := pipeline.New(pipeline.Config{LakehouseDir: dir, WriterID: "bench_", WriteTelemetry: true})
	assigned := make(chan struct{})
	var once atomic.Bool
	cl, err := kafka.NewConsumer(bl, "bench-"+topic, []string{topic},
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			if len(m[topic]) == kafka.TelemetryPartitions && once.CompareAndSwap(false, true) {
				close(assigned)
			}
		}))
	if err != nil {
		log.Fatal(err)
	}
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := kafka.Run(cctx, cl, p.PartitionHandler); err != nil {
			log.Printf("consumer: %v", err)
		}
	}()
	select {
	case <-assigned:
	case <-time.After(30 * time.Second):
		log.Fatal("consumer group was not assigned all partitions within 30s")
	}

	prod, err := kafka.NewProducer(bl)
	if err != nil {
		log.Fatal(err)
	}
	if err := prod.Ping(ctx); err != nil { // connect before the clock starts
		log.Fatal(err)
	}
	var failed atomic.Int64
	sessions := map[int32]bool{}
	p.ResetClock()
	start := time.Now()
	firstSeen := make(chan time.Duration, 1)
	go func() {
		for p.Events.Load() == 0 {
			time.Sleep(200 * time.Microsecond)
		}
		firstSeen <- time.Since(start)
	}()
	for i := range events {
		if rate > 0 {
			due := start.Add(time.Duration(float64(i) / rate * float64(time.Second)))
			if d := time.Until(due); d > 0 {
				time.Sleep(d)
			}
		}
		ev := events[i]
		sessions[ev.SessionKey] = true
		ev.IngestMs = time.Now().UnixMilli()
		prod.Produce(ctx, &kgo.Record{Topic: topic, Key: kafka.DriverKey(ev.DriverNumber), Value: ev.Encode()},
			func(_ *kgo.Record, err error) {
				if err != nil {
					failed.Add(1)
				}
			})
	}
	if err := prod.Flush(ctx); err != nil {
		log.Fatal(err)
	}
	produced := time.Since(start)
	prod.Close()
	if os.Getenv("BENCH_DEBUG") != "" {
		log.Printf("  flushed after %s; first record processed after %s; processed at flush %d",
			produced.Round(time.Millisecond), (<-firstSeen).Round(time.Millisecond), p.Events.Load())
	}
	if failed.Load() > 0 {
		log.Fatalf("%d produce failures", failed.Load())
	}
	deadline := time.Now().Add(180 * time.Second)
	for p.Events.Load() < int64(len(events)) && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	e2e := time.Since(start)
	cancel()
	<-done
	cl.Close()
	if err := p.Close(); err != nil { // flushes the last Parquet batches
		log.Printf("sink: %v", err)
	}
	committed := time.Since(start)
	st := p.Stats()
	if st.Events != int64(len(events)) {
		log.Fatalf("processed %d of %d events", st.Events, len(events))
	}
	files, _ := filepath.Glob(filepath.Join(dir, "telemetry", "session=*", "*.parquet"))
	mode := "burst"
	if rate > 0 {
		mode = "paced"
	}
	var keys []int32
	for k := range sessions {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return result{
		Mode: mode, Events: len(events), Sessions: keys, Partitions: kafka.TelemetryPartitions, TargetRate: rate,
		ProduceSeconds: produced.Seconds(), ProduceRate: float64(len(events)) / produced.Seconds(),
		EndToEndSeconds: e2e.Seconds(), EndToEndRate: float64(len(events)) / e2e.Seconds(),
		CommittedSeconds: committed.Seconds(), CommittedRate: float64(len(events)) / committed.Seconds(),
		EventLatency: st.EventLatency, TriggerLatency: st.TriggerLatency, Triggers: st.Triggers, Laps: st.Laps,
		ParquetFiles: st.ParquetFiles, ParquetRows: st.ParquetRows, SinkErrors: st.SinkErrors,
		DecodeErrors: st.DecodeErrors, Lakehouse: dir, TelemetryFilesSeen: len(files),
	}
}

// benchSim times the Monte Carlo engine in-process on a representative
// mid-race request (6 strategies x 10,000 rollouts, a 7-car field).
func benchSim(runs int) simResult {
	deg := 60.0
	req := sim.Request{
		TotalLaps: 57, CurrentLap: 30, Seed: 1,
		Car: sim.CarState{DriverCode: "LEC", Position: 3, Compound: "MEDIUM", TyreAgeLaps: 22, DegSlopeMsPerLap: &deg,
			BaseLapMs: 95000, CompoundsUsed: []string{"MEDIUM"}, GapToLeaderMs: 6000},
		Field: []sim.FieldCar{
			{DriverCode: "VER", Position: 1, GapToLeaderMs: 0, Compound: "HARD", TyreAgeLaps: 10},
			{DriverCode: "NOR", Position: 2, GapToLeaderMs: 4000, Compound: "MEDIUM", TyreAgeLaps: 24},
			{DriverCode: "PIA", Position: 4, GapToLeaderMs: 9000, Compound: "HARD", TyreAgeLaps: 8},
			{DriverCode: "HAM", Position: 5, GapToLeaderMs: 25000, Compound: "HARD", TyreAgeLaps: 12},
			{DriverCode: "RUS", Position: 6, GapToLeaderMs: 25600, Compound: "HARD", TyreAgeLaps: 12},
			{DriverCode: "SAI", Position: 7, GapToLeaderMs: 26300, Compound: "MEDIUM", TyreAgeLaps: 20},
			{DriverCode: "ALO", Position: 8, GapToLeaderMs: 33000, Compound: "HARD", TyreAgeLaps: 15},
		},
	}
	var h metrics.Hist
	var workers int
	for i := 0; i < runs+5; i++ {
		req.Seed = uint64(i)
		t0 := time.Now()
		r, err := sim.Simulate(req)
		if err != nil {
			log.Fatal(err)
		}
		if i >= 5 { // warm-up
			h.Record(time.Since(t0))
		}
		workers = r.Workers
	}
	var one metrics.Hist
	single := req
	single.Strategies = []string{"PIT_NOW_HARD"}
	for i := 0; i < runs; i++ {
		t0 := time.Now()
		sim.Simulate(single)
		one.Record(time.Since(t0))
	}
	s := h.Summarize()
	return simResult{Runs: runs, Rollouts: sim.DefaultRollouts, Strategies: len(sim.AllStrategies), Workers: workers,
		P50Ms: s.P50Ms, P95Ms: s.P95Ms, P99Ms: s.P99Ms, MeanMs: s.MeanMs, OneStratP50: one.QuantileMs(0.5)}
}

// medianOf summarises trials by their median end-to-end throughput trial,
// plus the min/max range of the headline numbers. "sustained" counts events
// processed by the engines; "committed" also waits for the final Parquet flush.
func medianOf(runs []result) map[string]any {
	idx := make([]int, len(runs))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return runs[idx[a]].EndToEndRate < runs[idx[b]].EndToEndRate })
	med := runs[idx[len(idx)/2]]
	minR, maxR := runs[idx[0]].EndToEndRate, runs[idx[len(idx)-1]].EndToEndRate
	p99 := make([]float64, len(runs))
	for i, r := range runs {
		p99[i] = r.TriggerLatency.P99Ms
	}
	sort.Float64s(p99)
	return map[string]any{
		"trials":                       len(runs),
		"median_trial":                 med,
		"sustained_events_per_s_range": []float64{minR, maxR},
		"trigger_p99_ms_range":         []float64{p99[0], p99[len(p99)-1]},
		"committed_events_per_s":       med.CommittedRate,
	}
}
