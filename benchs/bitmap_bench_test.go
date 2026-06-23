package benchs

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aytechnet/fsync"
	"github.com/puzpuzpuz/xsync/v4"
)

// Bitmap benchmarks. Compared against:
//
//   - fsync.Store[bool]      — same growth mechanics, V=bool inline.
//   - xsync.Map[int64, bool] — generic concurrent map alternative.
//   - map[int64]bool + RWMutex — naïve baseline.
//
// Workloads (mirror Set/Map suites):
//
//   - Set      — write-only, distinct indexes per goroutine.
//   - Has      — preloaded, parallel membership tests only.
//   - SetUnset — alternating Set/Unset on a rolling window.

// ---------- Set ----------

func BenchmarkFsyncBitmapSet(b *testing.B) {
	var bm fsync.Bitmap
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		// 1<<24 = 16M per worker, max ~192M indices across 12 workers
		// → ~3M buckets → 32 MB table. Bitmap's index is used as-is
		// (not hashed), so a larger per-worker offset would force a
		// multi-GB virtual table that may OOM on a machine without
		// swap. Workers stay on disjoint buckets at any realistic b.N.
		i := start.Add(1 << 24)
		for pb.Next() {
			bm.Set(i)
			i++
		}
	})
}

func BenchmarkFsyncStoreBoolSet(b *testing.B) {
	var s fsync.Store[bool]
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		// 1<<24 = 16M per worker, max ~192M indices across 12 workers
		// → ~3M buckets → 32 MB table. Bitmap's index is used as-is
		// (not hashed), so a larger per-worker offset would force a
		// multi-GB virtual table that may OOM on a machine without
		// swap. Workers stay on disjoint buckets at any realistic b.N.
		i := start.Add(1 << 24)
		for pb.Next() {
			s.Store(i, true)
			i++
		}
	})
}

func BenchmarkXsyncMapBoolSet(b *testing.B) {
	m := xsync.NewMap[int64, bool]()
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		// 1<<24 = 16M per worker, max ~192M indices across 12 workers
		// → ~3M buckets → 32 MB table. Bitmap's index is used as-is
		// (not hashed), so a larger per-worker offset would force a
		// multi-GB virtual table that may OOM on a machine without
		// swap. Workers stay on disjoint buckets at any realistic b.N.
		i := start.Add(1 << 24)
		for pb.Next() {
			m.Store(i, true)
			i++
		}
	})
}

func BenchmarkGoMapBoolMutexSet(b *testing.B) {
	m := make(map[int64]bool)
	var mu sync.Mutex
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		// 1<<24 = 16M per worker, max ~192M indices across 12 workers
		// → ~3M buckets → 32 MB table. Bitmap's index is used as-is
		// (not hashed), so a larger per-worker offset would force a
		// multi-GB virtual table that may OOM on a machine without
		// swap. Workers stay on disjoint buckets at any realistic b.N.
		i := start.Add(1 << 24)
		for pb.Next() {
			mu.Lock()
			m[i] = true
			mu.Unlock()
			i++
		}
	})
}

// ---------- Has (preloaded) ----------

func BenchmarkFsyncBitmapHas(b *testing.B) {
	var bm fsync.Bitmap
	for i := int64(0); i < preloadedKeys; i++ {
		bm.Set(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if !bm.Has(j) {
				b.Errorf("fsync.Bitmap: bit %d should be set", j)
			}
			i++
		}
	})
}

func BenchmarkFsyncStoreBoolHas(b *testing.B) {
	var s fsync.Store[bool]
	for i := int64(0); i < preloadedKeys; i++ {
		s.Store(i, true)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if v, ok := s.Load(j); !ok || !v {
				b.Errorf("fsync.Store[bool]: bit %d should be true", j)
			}
			i++
		}
	})
}

func BenchmarkXsyncMapBoolHas(b *testing.B) {
	m := xsync.NewMap[int64, bool]()
	for i := int64(0); i < preloadedKeys; i++ {
		m.Store(i, true)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if v, ok := m.Load(j); !ok || !v {
				b.Errorf("xsync.Map[bool]: bit %d should be true", j)
			}
			i++
		}
	})
}

func BenchmarkGoMapBoolRWMutexHas(b *testing.B) {
	m := make(map[int64]bool, preloadedKeys)
	for i := int64(0); i < preloadedKeys; i++ {
		m[i] = true
	}
	var mu sync.RWMutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			mu.RLock()
			v := m[j]
			mu.RUnlock()
			if !v {
				b.Errorf("go map: bit %d should be true", j)
			}
			i++
		}
	})
}

// ---------- SetUnset (rolling window) ----------

func BenchmarkFsyncBitmapSetUnset(b *testing.B) {
	var bm fsync.Bitmap
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			k := i & (churnWindow - 1)
			bm.Set(k)
			bm.Unset(k)
			i++
		}
	})
}

func BenchmarkFsyncStoreBoolSetUnset(b *testing.B) {
	var s fsync.Store[bool]
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			k := i & (churnWindow - 1)
			s.Store(k, true)
			s.Delete(k)
			i++
		}
	})
}

func BenchmarkXsyncMapBoolSetUnset(b *testing.B) {
	m := xsync.NewMap[int64, bool]()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			k := i & (churnWindow - 1)
			m.Store(k, true)
			m.Delete(k)
			i++
		}
	})
}

// ---------- Range/key (preloaded, serial) ----------

func BenchmarkFsyncBitmapRange(b *testing.B) {
	var bm fsync.Bitmap
	for i := int64(0); i < preloadedKeys; i++ {
		bm.Set(i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var n int
	for i := 0; i < b.N; i++ {
		bm.Range(func(_ int64) bool {
			n++
			return true
		})
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/key")
}

func BenchmarkFsyncStoreBoolRange(b *testing.B) {
	var s fsync.Store[bool]
	for i := int64(0); i < preloadedKeys; i++ {
		s.Store(i, true)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var n int
	for i := 0; i < b.N; i++ {
		s.Range(func(_ int64, _ bool) bool {
			n++
			return true
		})
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/key")
}

func BenchmarkXsyncMapBoolRange(b *testing.B) {
	m := xsync.NewMap[int64, bool]()
	for i := int64(0); i < preloadedKeys; i++ {
		m.Store(i, true)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var n int
	for i := 0; i < b.N; i++ {
		m.Range(func(_ int64, _ bool) bool {
			n++
			return true
		})
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/key")
}
