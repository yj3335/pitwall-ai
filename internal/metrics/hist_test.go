package metrics

import (
	"math"
	"sync"
	"testing"
)

func TestQuantilesWithinBucketError(t *testing.T) {
	var h Hist
	for us := int64(1); us <= 100_000; us++ { // 1us .. 100ms uniform
		h.RecordMicros(us)
	}
	for q, want := range map[float64]float64{0.5: 50, 0.95: 95, 0.99: 99} {
		got := h.QuantileMs(q)
		if math.Abs(got-want)/want > 0.05 {
			t.Errorf("q%.2f = %.2fms, want ~%.0fms", q, got, want)
		}
	}
	if h.MaxMs() != 100 {
		t.Errorf("max %.2f", h.MaxMs())
	}
}

func TestConcurrentRecord(t *testing.T) {
	var h Hist
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10_000; i++ {
				h.RecordMicros(int64(i))
			}
		}()
	}
	wg.Wait()
	if h.Count() != 160_000 {
		t.Fatalf("count %d", h.Count())
	}
}

func TestIndexMonotonic(t *testing.T) {
	prev := -1
	for v := uint64(0); v < 1<<20; v += 7 {
		i := index(v)
		if i < prev {
			t.Fatalf("index not monotonic at %d", v)
		}
		if lowerBound(i) > v {
			t.Fatalf("lowerBound(%d)=%d > %d", i, lowerBound(i), v)
		}
		prev = i
	}
}
