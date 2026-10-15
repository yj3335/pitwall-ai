// Package parquet writes immutable, hive-partitioned Apache Parquet batches
// for the lakehouse. Each batch is written to a hidden temp file in the
// partition directory, fsynced, and then atomically renamed into place, so
// concurrent readers (DuckDB globbing *.parquet) only ever see complete files
// and never contend on a lock with the writer.
package parquet

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	pq "github.com/parquet-go/parquet-go"
)

// Partitioned is implemented by row types that carry their hive partition key.
type Partitioned interface {
	PartitionKey() int32
}

// BatchWriter writes rows of T into <root>/<table>/session=<key>/batch_<seq>.parquet.
// It is owned by a single goroutine (see Sink); it holds no locks.
type BatchWriter[T Partitioned] struct {
	root   string
	table  string
	seq    atomic.Int64
	files  atomic.Int64
	rows   atomic.Int64
	prefix string
}

// NewBatchWriter creates a writer for one table. prefix distinguishes
// concurrent writer processes (e.g. a host name or run id) so that their
// batch file names never collide.
func NewBatchWriter[T Partitioned](root, table, prefix string) *BatchWriter[T] {
	return &BatchWriter[T]{root: root, table: table, prefix: prefix}
}

// Files and Rows report how much has been committed.
func (w *BatchWriter[T]) Files() int64 { return w.files.Load() }
func (w *BatchWriter[T]) Rows() int64  { return w.rows.Load() }

// Write commits rows, one file per partition present in the batch, and
// returns the committed paths.
func (w *BatchWriter[T]) Write(rows []T) ([]string, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	byKey := map[int32][]T{}
	for _, r := range rows {
		k := r.PartitionKey()
		byKey[k] = append(byKey[k], r)
	}
	var paths []string
	for key, part := range byKey {
		p, err := w.writeFile(key, part)
		if err != nil {
			return paths, err
		}
		paths = append(paths, p)
	}
	return paths, nil
}

func (w *BatchWriter[T]) writeFile(key int32, rows []T) (string, error) {
	dir := filepath.Join(w.root, w.table, fmt.Sprintf("session=%d", key))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	seq := w.seq.Add(1)
	name := fmt.Sprintf("batch_%s%012d.parquet", w.prefix, seq)
	final := filepath.Join(dir, name)
	// The temp name must not end in ".parquet": Go's filepath.Glob (and some
	// readers' globbers) match dotfiles, so a hidden ".tmp-x.parquet" would
	// still be picked up mid-write by a "*.parquet" scan.
	tmp := filepath.Join(dir, ".tmp-"+name+".inprogress")
	f, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	pw := pq.NewGenericWriter[T](f, pq.Compression(&pq.Zstd), pq.MaxRowsPerRowGroup(int64(len(rows))))
	if _, err := pw.Write(rows); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", fmt.Errorf("parquet write: %w", err)
	}
	if err := pw.Close(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", fmt.Errorf("parquet close: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil { // atomic commit
		os.Remove(tmp)
		return "", err
	}
	w.files.Add(1)
	w.rows.Add(int64(len(rows)))
	return final, nil
}

// Sink batches rows from many producer goroutines through a channel into a
// single writer goroutine, flushing by size or age.
type Sink[T Partitioned] struct {
	in      chan T
	w       *BatchWriter[T]
	max     int
	every   time.Duration
	done    chan struct{}
	errs    atomic.Int64
	lastErr atomic.Pointer[error]
	OnFlush func(paths []string, rows int)
}

// NewSink starts the writer goroutine.
func NewSink[T Partitioned](w *BatchWriter[T], maxRows int, every time.Duration) *Sink[T] {
	s := &Sink[T]{in: make(chan T, maxRows*4), w: w, max: maxRows, every: every, done: make(chan struct{})}
	go s.loop()
	return s
}

// Add enqueues a row (blocks when the writer is behind: backpressure).
func (s *Sink[T]) Add(r T) { s.in <- r }

// Close flushes the remaining rows and stops the writer.
func (s *Sink[T]) Close() error {
	close(s.in)
	<-s.done
	if p := s.lastErr.Load(); p != nil {
		return *p
	}
	return nil
}

// Errors is the number of failed flushes.
func (s *Sink[T]) Errors() int64 { return s.errs.Load() }

// Writer exposes the underlying batch writer's counters.
func (s *Sink[T]) Writer() *BatchWriter[T] { return s.w }

func (s *Sink[T]) loop() {
	defer close(s.done)
	buf := make([]T, 0, s.max)
	tick := time.NewTicker(s.every)
	defer tick.Stop()
	flush := func() {
		if len(buf) == 0 {
			return
		}
		paths, err := s.w.Write(buf)
		if err != nil {
			s.errs.Add(1)
			s.lastErr.Store(&err)
		}
		if s.OnFlush != nil {
			s.OnFlush(paths, len(buf))
		}
		buf = make([]T, 0, s.max)
	}
	for {
		select {
		case r, ok := <-s.in:
			if !ok {
				flush()
				return
			}
			buf = append(buf, r)
			if len(buf) >= s.max {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}
