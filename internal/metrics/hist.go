// Package metrics provides a lock-free log-linear latency histogram
// (16 sub-buckets per power of two, ~4% relative error) safe for concurrent
// recording from many goroutines.
package metrics

import (
	"math/bits"
	"sync/atomic"
	"time"
)

const subBits = 4
const subBuckets = 1 << subBits
const nBuckets = 64 * subBuckets

// Hist records durations in microseconds.
type Hist struct {
	counts [nBuckets]atomic.Int64
	n      atomic.Int64
	sum    atomic.Int64
	max    atomic.Int64
}

func index(v uint64) int {
	if v < subBuckets {
		return int(v)
	}
	e := bits.Len64(v) - 1 // position of the leading bit
	shift := e - subBits
	sub := (v >> uint(shift)) & (subBuckets - 1)
	return (e-subBits+1)*subBuckets + int(sub)
}

func lowerBound(i int) uint64 {
	if i < subBuckets {
		return uint64(i)
	}
	e := i/subBuckets + subBits - 1
	sub := uint64(i % subBuckets)
	return (1 << uint(e)) | (sub << uint(e-subBits))
}

// Record adds one observation.
func (h *Hist) Record(d time.Duration) { h.RecordMicros(d.Microseconds()) }

// RecordMicros adds one observation in microseconds (negative clamps to 0).
func (h *Hist) RecordMicros(us int64) {
	if us < 0 {
		us = 0
	}
	h.counts[index(uint64(us))].Add(1)
	h.n.Add(1)
	h.sum.Add(us)
	for {
		m := h.max.Load()
		if us <= m || h.max.CompareAndSwap(m, us) {
			break
		}
	}
}

// Count is the number of observations.
func (h *Hist) Count() int64 { return h.n.Load() }

// QuantileMs returns the q-quantile (0..1) in milliseconds, using the
// midpoint of the matching bucket.
func (h *Hist) QuantileMs(q float64) float64 {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	rank := int64(q*float64(n-1)) + 1
	var seen int64
	for i := 0; i < nBuckets; i++ {
		seen += h.counts[i].Load()
		if seen >= rank {
			lo := lowerBound(i)
			hi := lowerBound(i + 1)
			if hi > uint64(h.max.Load()) {
				hi = uint64(h.max.Load())
			}
			if hi < lo {
				hi = lo
			}
			return float64(lo+hi) / 2 / 1000
		}
	}
	return float64(h.max.Load()) / 1000
}

// MeanMs is the arithmetic mean in milliseconds.
func (h *Hist) MeanMs() float64 {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	return float64(h.sum.Load()) / float64(n) / 1000
}

// MaxMs is the largest observation in milliseconds.
func (h *Hist) MaxMs() float64 { return float64(h.max.Load()) / 1000 }

// Summary is a JSON-friendly snapshot.
type Summary struct {
	Count  int64   `json:"count"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MeanMs float64 `json:"mean_ms"`
	MaxMs  float64 `json:"max_ms"`
}

// Summarize returns the standard quantiles.
func (h *Hist) Summarize() Summary {
	return Summary{Count: h.Count(), P50Ms: h.QuantileMs(0.50), P95Ms: h.QuantileMs(0.95),
		P99Ms: h.QuantileMs(0.99), MeanMs: h.MeanMs(), MaxMs: h.MaxMs()}
}
