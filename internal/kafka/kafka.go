// Package kafka wraps franz-go for the telemetry pipeline: a producer that
// keys every record by driver_number (Kafka's murmur2 partitioner, identical
// to the Java client, so one car always lands on one of the 20 partitions and
// its events stay in order), topic administration, and a consumer that fans
// partitions out to dedicated worker goroutines.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Topic names and layout.
const (
	TopicTelemetry      = "f1.telemetry.raw"
	TopicRaceControl    = "f1.race_control"
	TopicTriggers       = "f1.triggers"
	TelemetryPartitions = 20
)

// Topics is the full topic layout created by EnsureTopics.
var Topics = map[string]int32{
	TopicTelemetry:   TelemetryPartitions,
	TopicRaceControl: 1,
	TopicTriggers:    TelemetryPartitions,
}

// DriverKey is the record key for a car.
func DriverKey(driverNumber int32) []byte {
	return []byte(strconv.Itoa(int(driverNumber)))
}

var keyPartitioner = kgo.StickyKeyPartitioner(nil)

// PartitionFor returns the partition the default producer partitioner picks
// for a key (murmur2, Java-compatible).
func PartitionFor(topic string, key []byte, partitions int) int32 {
	return int32(keyPartitioner.ForTopic(topic).Partition(&kgo.Record{Key: key}, partitions))
}

// NewProducer returns an idempotent, batching producer.
func NewProducer(brokers []string, extra ...kgo.Opt) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		// a partition without a leader (a topic just created, a leader moving)
		// otherwise holds every buffered record for the default 5 s before the
		// client asks again
		kgo.MetadataMinAge(100 * time.Millisecond),
		kgo.ProducerLinger(2 * time.Millisecond),
		kgo.ProducerBatchMaxBytes(1 << 20),
		kgo.ProducerBatchCompression(kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.MaxBufferedRecords(200_000),
	}
	return kgo.NewClient(append(opts, extra...)...)
}

// EnsureTopics creates the pipeline topics if they do not exist.
func EnsureTopics(ctx context.Context, brokers []string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	for topic, parts := range Topics {
		resp, err := adm.CreateTopic(ctx, parts, 1, nil, topic)
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create %s: %w", topic, err)
		}
		if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create %s: %w", topic, resp.Err)
		}
	}
	return nil
}

// TopicPartitions returns the partition count of a topic.
func TopicPartitions(ctx context.Context, brokers []string, topic string) (int, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return 0, err
	}
	defer cl.Close()
	td, err := kadm.NewClient(cl).ListTopics(ctx, topic)
	if err != nil {
		return 0, err
	}
	t, ok := td[topic]
	if !ok || t.Err != nil {
		return 0, fmt.Errorf("topic %s not found", topic)
	}
	return len(t.Partitions), nil
}

// Handler processes one batch of records from a single partition. It is
// always called from the same goroutine for a given partition.
type Handler func(partition int32, records []*kgo.Record)

// NewConsumer returns a group consumer that only commits offsets the
// partition workers have finished with (at-least-once).
func NewConsumer(brokers []string, group string, topics []string, extra ...kgo.Opt) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(time.Second),
		kgo.FetchMaxWait(50 * time.Millisecond),
		kgo.FetchMaxBytes(16 << 20),
		kgo.FetchMaxPartitionBytes(4 << 20),
	}
	return kgo.NewClient(append(opts, extra...)...)
}

// Run polls until ctx is cancelled, dispatching each partition's records to
// a dedicated goroutine created on first sight. Records are marked for commit
// only after the handler returns.
func Run(ctx context.Context, cl *kgo.Client, newHandler func(topic string, partition int32) Handler) error {
	type key struct {
		topic string
		part  int32
	}
	workers := map[key]chan []*kgo.Record{}
	var wg sync.WaitGroup
	defer func() {
		for _, ch := range workers {
			close(ch)
		}
		wg.Wait()
	}()
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		var ferr error
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				ferr = fmt.Errorf("fetch %s[%d]: %w", t, p, err)
			}
		})
		if ferr != nil {
			return ferr
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			k := key{p.Topic, p.Partition}
			ch, ok := workers[k]
			if !ok {
				ch = make(chan []*kgo.Record, 64)
				workers[k] = ch
				h := newHandler(p.Topic, p.Partition)
				wg.Add(1)
				go func(part int32) {
					defer wg.Done()
					for recs := range ch {
						h(part, recs)
						cl.MarkCommitRecords(recs...)
					}
				}(p.Partition)
			}
			ch <- p.Records
		})
	}
}
