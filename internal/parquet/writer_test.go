package parquet

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pq "github.com/parquet-go/parquet-go"
)

type row struct {
	Session int32   `parquet:"session_key"`
	Driver  int32   `parquet:"driver_number"`
	Lap     int32   `parquet:"lap_number"`
	Value   float64 `parquet:"value"`
	Label   string  `parquet:"label"`
}

func (r row) PartitionKey() int32 { return r.Session }

func TestBatchWriterPartitionsAndAtomicNames(t *testing.T) {
	dir := t.TempDir()
	w := NewBatchWriter[row](dir, "laps", "t_")
	paths, err := w.Write([]row{{Session: 1, Driver: 44}, {Session: 2, Driver: 1}, {Session: 1, Driver: 4}})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || w.Rows() != 3 || w.Files() != 2 {
		t.Fatalf("paths=%v rows=%d files=%d", paths, w.Rows(), w.Files())
	}
	for _, p := range paths {
		if !strings.Contains(p, "session=") || strings.HasPrefix(filepath.Base(p), ".") {
			t.Fatalf("bad committed path %s", p)
		}
		rows, err := pq.ReadFile[row](p)
		if err != nil || len(rows) == 0 {
			t.Fatalf("read %s: %v", p, err)
		}
	}
	leftovers, _ := filepath.Glob(filepath.Join(dir, "laps", "*", ".tmp-*"))
	if len(leftovers) != 0 {
		t.Fatalf("temp files left behind: %v", leftovers)
	}
}

// TestConcurrentReadersNeverSeePartialFiles hammers the sink from several
// producer goroutines while readers continuously glob and fully decode every
// visible *.parquet file. Any torn file would fail to decode.
func TestConcurrentReadersNeverSeePartialFiles(t *testing.T) {
	dir := t.TempDir()
	sink := NewSink(NewBatchWriter[row](dir, "telemetry", "c_"), 500, 5*time.Millisecond)
	const producers, perProducer = 8, 5000
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				sink.Add(row{Session: int32(p % 2), Driver: int32(p), Lap: int32(i), Value: float64(i), Label: "MEDIUM"})
			}
		}(p)
	}
	var readErrs, reads atomic.Int64
	stop := make(chan struct{})
	var rwg sync.WaitGroup
	for r := 0; r < 4; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				files, _ := filepath.Glob(filepath.Join(dir, "telemetry", "*", "*.parquet"))
				for _, f := range files {
					if _, err := pq.ReadFile[row](f); err != nil {
						if !os.IsNotExist(err) {
							if readErrs.Add(1) == 1 {
								t.Logf("first read error on %s: %v", f, err)
							}
						}
					}
					reads.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	close(stop)
	rwg.Wait()
	if readErrs.Load() != 0 {
		t.Fatalf("%d partial/corrupt reads out of %d", readErrs.Load(), reads.Load())
	}
	if got := sink.Writer().Rows(); got != producers*perProducer {
		t.Fatalf("rows committed %d, want %d", got, producers*perProducer)
	}
	var total int
	files, _ := filepath.Glob(filepath.Join(dir, "telemetry", "*", "*.parquet"))
	for _, f := range files {
		rows, err := pq.ReadFile[row](f)
		if err != nil {
			t.Fatal(err)
		}
		total += len(rows)
	}
	if total != producers*perProducer {
		t.Fatalf("read back %d rows", total)
	}
	t.Logf("%d files, %d concurrent reads, 0 errors", len(files), reads.Load())
}
