package kafka

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// 2024 grid car numbers
var grid = []int32{1, 11, 16, 55, 4, 81, 44, 63, 14, 18, 10, 31, 23, 2, 22, 3, 27, 20, 77, 24}

func TestPartitionForIsStableAndInRange(t *testing.T) {
	used := map[int32]bool{}
	for _, dn := range grid {
		p := PartitionFor(TopicTelemetry, DriverKey(dn), TelemetryPartitions)
		if p < 0 || p >= TelemetryPartitions {
			t.Fatalf("partition %d out of range", p)
		}
		for i := 0; i < 5; i++ {
			if PartitionFor(TopicTelemetry, DriverKey(dn), TelemetryPartitions) != p {
				t.Fatalf("car %d not pinned to one partition", dn)
			}
		}
		used[p] = true
	}
	// murmur2 over 20 keys should spread across most partitions
	if len(used) < 10 {
		t.Fatalf("only %d partitions used for 20 cars", len(used))
	}
}

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

// TestKafkaPerDriverOrdering produces interleaved events for 20 cars to a
// fresh 20-partition topic on the real broker and checks that every car's
// records land on exactly one partition, in order.
func TestKafkaPerDriverOrdering(t *testing.T) {
	bl := brokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	topic := fmt.Sprintf("pitwall.test.order.%d", time.Now().UnixNano())
	Topics[topic] = TelemetryPartitions
	defer delete(Topics, topic)
	if err := EnsureTopics(ctx, bl); err != nil {
		t.Fatal(err)
	}
	if n, err := TopicPartitions(ctx, bl, topic); err != nil || n != TelemetryPartitions {
		t.Fatalf("partitions=%d err=%v", n, err)
	}
	prod, err := NewProducer(bl)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	const perCar = 200
	for seq := 0; seq < perCar; seq++ {
		for _, dn := range grid {
			prod.Produce(ctx, &kgo.Record{Topic: topic, Key: DriverKey(dn), Value: []byte(fmt.Sprintf("%d:%d", dn, seq))}, nil)
		}
	}
	if err := prod.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	cons, err := kgo.NewClient(kgo.SeedBrokers(bl...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()
	partOf := map[string]int32{}
	next := map[string]int{}
	total := 0
	for total < perCar*len(grid) {
		f := cons.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out after %d records", total)
		}
		f.EachRecord(func(r *kgo.Record) {
			var dn, seq int
			fmt.Sscanf(string(r.Value), "%d:%d", &dn, &seq)
			k := string(r.Key)
			if p, ok := partOf[k]; ok && p != r.Partition {
				t.Errorf("car %s seen on partitions %d and %d", k, p, r.Partition)
			}
			partOf[k] = r.Partition
			if r.Partition != PartitionFor(topic, r.Key, TelemetryPartitions) {
				t.Errorf("car %s on partition %d, partitioner says %d", k, r.Partition, PartitionFor(topic, r.Key, TelemetryPartitions))
			}
			if seq != next[k] {
				t.Errorf("car %s out of order: got seq %d want %d", k, seq, next[k])
			}
			next[k] = seq + 1
			total++
		})
	}
}
