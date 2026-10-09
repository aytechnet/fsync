package benchs

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aytechnet/fsync"
)

// Bloom benchmarks. A Bloom filter answers "seen before?" with a
// bounded false-positive rate in a fraction of the memory of an exact
// set, so the references are the exact sets the same job would
// otherwise use:
//
//   - fsync.Bloom[int]  — word-blocked, one atomic OR / Load per call.
//   - fsync.Set[int]    — exact concurrent set.
//   - map[int]struct{} + sync.Mutex — naïve exact baseline.
//
// Workloads: Add (distinct keys, parallel), Contains (preloaded,
// parallel), and the memory footprint for 1M keys.

const bloomBenchKeys = 1 << 20

// ---------- Add ----------

func BenchmarkFsyncBloomAdd(b *testing.B) {
	f := fsync.NewBloom[int](bloomBenchKeys, 0.01)
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			f.Add(i)
			i++
		}
	})
}

func BenchmarkFsyncSetAddForBloom(b *testing.B) {
	var s fsync.Set[int]
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			s.Add(i)
			i++
		}
	})
}

func BenchmarkMutexMapAddForBloom(b *testing.B) {
	m := map[int]struct{}{}
	var mu sync.Mutex
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			mu.Lock()
			m[i] = struct{}{}
			mu.Unlock()
			i++
		}
	})
}

// ---------- Contains ----------

func BenchmarkFsyncBloomContains(b *testing.B) {
	f := fsync.NewBloom[int](bloomBenchKeys, 0.01)
	for i := 0; i < bloomBenchKeys; i++ {
		f.Add(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			f.Contains(i & (bloomBenchKeys - 1))
			i++
		}
	})
}

func BenchmarkFsyncSetContainsForBloom(b *testing.B) {
	s := fsync.NewSet[int]().Grow(bloomBenchKeys)
	for i := 0; i < bloomBenchKeys; i++ {
		s.Add(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Contains(i & (bloomBenchKeys - 1))
			i++
		}
	})
}

// ---------- Footprint (1M keys) ----------

func heapInUse() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse
}

// TestBloomFootprint1M reports the heap held by each structure for
// 1M distinct int keys (run with -v to read the figures).
func TestBloomFootprint1M(t *testing.T) {
	const n = 1_000_000
	measure := func(name string, build func() any) {
		before := heapInUse()
		keep := build()
		after := heapInUse()
		runtime.KeepAlive(keep)
		t.Logf(`%-24s %8.1f MB  %6.2f B/key`, name,
			float64(after-before)/1e6, float64(after-before)/n)
	}
	measure("fsync.Bloom 1%", func() any {
		f := fsync.NewBloom[int](n, 0.01)
		for i := 0; i < n; i++ {
			f.Add(i)
		}
		return f
	})
	measure("fsync.Bloom 0.1%", func() any {
		f := fsync.NewBloom[int](n, 0.001)
		for i := 0; i < n; i++ {
			f.Add(i)
		}
		return f
	})
	measure("fsync.Set", func() any {
		var s fsync.Set[int]
		for i := 0; i < n; i++ {
			s.Add(i)
		}
		return &s
	})
	measure("map[int]struct{}", func() any {
		m := map[int]struct{}{}
		for i := 0; i < n; i++ {
			m[i] = struct{}{}
		}
		return m
	})
}
