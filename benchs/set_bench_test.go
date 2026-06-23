package benchs

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aytechnet/fsync"
	"github.com/puzpuzpuz/xsync/v4"
)

// Set benchmarks. Four workloads, mirroring the Map[K,V] suite:
//
//   - Add        — many writers, distinct keys (insertion-heavy).
//   - Contains   — preloaded set, parallel membership tests only.
//   - AddRemove  — alternating Add/Remove on a rolling window
//                  (slot recycling under contention).
//   - Range/key  — full sweep cost amortized per element.
//
// Reference implementations:
//   - fsync.Set[int]      — the wrapper under test.
//   - fsync.Map[int, struct{}] — same data layout, raw API.
//   - xsync.Map[int, struct{}] — the v4 reference concurrent map.
//   - sync.Map (stdlib)    — stored value is `struct{}{}` (boxed).
//   - map[int]struct{} + sync.RWMutex — naïve baseline.

// ---------- Add ----------

func BenchmarkFsyncSetAdd(b *testing.B) {
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

func BenchmarkFsyncMapAdd(b *testing.B) {
	var m fsync.Map[int, struct{}]
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			m.Store(i, struct{}{})
			i++
		}
	})
}

func BenchmarkXsyncMapAdd(b *testing.B) {
	m := xsync.NewMap[int, struct{}]()
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			m.Store(i, struct{}{})
			i++
		}
	})
}

func BenchmarkSyncMapAdd(b *testing.B) {
	var m sync.Map
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			m.Store(i, struct{}{})
			i++
		}
	})
}

func BenchmarkGoMapMutexAdd(b *testing.B) {
	m := make(map[int]struct{})
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

// ---------- Contains (preloaded) ----------

func BenchmarkFsyncSetContains(b *testing.B) {
	var s fsync.Set[int]
	for i := range preloadedKeys {
		s.Add(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if !s.Contains(j) {
				b.Errorf("fsync.Set: %d should exist", j)
			}
			i++
		}
	})
}

func BenchmarkFsyncMapContains(b *testing.B) {
	var m fsync.Map[int, struct{}]
	for i := range preloadedKeys {
		m.Store(i, struct{}{})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if _, ok := m.Load(j); !ok {
				b.Errorf("fsync.Map: %d should exist", j)
			}
			i++
		}
	})
}

func BenchmarkXsyncMapContains(b *testing.B) {
	m := xsync.NewMap[int, struct{}]()
	for i := range preloadedKeys {
		m.Store(i, struct{}{})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if _, ok := m.Load(j); !ok {
				b.Errorf("xsync.Map: %d should exist", j)
			}
			i++
		}
	})
}

func BenchmarkSyncMapContains(b *testing.B) {
	var m sync.Map
	for i := range preloadedKeys {
		m.Store(i, struct{}{})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if _, ok := m.Load(j); !ok {
				b.Errorf("sync.Map: %d should exist", j)
			}
			i++
		}
	})
}

func BenchmarkGoMapRWMutexContains(b *testing.B) {
	m := make(map[int]struct{}, preloadedKeys)
	for i := range preloadedKeys {
		m[i] = struct{}{}
	}
	var mu sync.RWMutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			mu.RLock()
			_, ok := m[j]
			mu.RUnlock()
			if !ok {
				b.Errorf("go map: %d should exist", j)
			}
			i++
		}
	})
}

// ---------- AddRemove (rolling window) ----------

func BenchmarkFsyncSetAddRemove(b *testing.B) {
	var s fsync.Set[int]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (churnWindow - 1)
			s.Add(k)
			s.Remove(k)
			i++
		}
	})
}

func BenchmarkFsyncMapAddRemove(b *testing.B) {
	var m fsync.Map[int, struct{}]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (churnWindow - 1)
			m.Store(k, struct{}{})
			m.Delete(k)
			i++
		}
	})
}

func BenchmarkXsyncMapAddRemove(b *testing.B) {
	m := xsync.NewMap[int, struct{}]()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (churnWindow - 1)
			m.Store(k, struct{}{})
			m.Delete(k)
			i++
		}
	})
}

// ---------- Range/key ----------

func BenchmarkFsyncSetRange(b *testing.B) {
	var s fsync.Set[int]
	for i := range preloadedKeys {
		s.Add(i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var n int
	for i := 0; i < b.N; i++ {
		s.Range(func(_ int) bool {
			n++
			return true
		})
	}
	// Normalize per visited key.
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/key")
}

func BenchmarkXsyncMapSetRange(b *testing.B) {
	m := xsync.NewMap[int, struct{}]()
	for i := range preloadedKeys {
		m.Store(i, struct{}{})
	}
	b.ResetTimer()
	b.ReportAllocs()
	var n int
	for i := 0; i < b.N; i++ {
		m.Range(func(_ int, _ struct{}) bool {
			n++
			return true
		})
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/key")
}
