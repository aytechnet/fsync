// Package benchs compares fsync (Store, MutexStore, Map) against sync.Map,
// puzpuzpuz/xsync.Map, and the standard Go map (read-only, sync.Mutex-protected,
// sync.RWMutex-protected) on a shared set of workloads:
//
//   - Store      — many writers, distinct keys (write-heavy).
//   - ReadHeavy  — 1 write per 10 reads.
//   - ReadOnly   — preloaded, parallel reads only.
//   - Churn      — 50/50 Store then Delete on a rolling window of keys.
//   - GrowStore  — same as Store but with the table pre-sized.
//
// Run with:
//
//	go test -bench=. -benchmem ./benchs/...
package benchs

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/aytechnet/fsync"
)

// Shared constants used across implementations so each comparison row
// hits the same workload shape.
const (
	preloadedKeys = 2048
	churnWindow   = 1024
	growHint      = 1 << 18 // 256k buckets capacity for fsync.Map, ~equiv for others
)

// ---------- fsync.Map[int, int] ----------

func BenchmarkFsyncMapStore(b *testing.B) {
	var m fsync.Map[int, int]
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			m.Store(i, -i)
			i++
		}
	})
}

func BenchmarkFsyncMapGrowStore(b *testing.B) {
	var m fsync.Map[int, int]
	m.Grow(growHint)
	var start atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			m.Store(i, -i)
			i++
		}
	})
}

func BenchmarkFsyncMapReadHeavy(b *testing.B) {
	var m fsync.Map[int, int]
	const r = 10
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i / r
			if i%r == 0 {
				m.Store(j, i)
			} else {
				if v, ok := m.Load(j); !ok {
					b.Errorf("fsync.Map: item at %d should exist", j)
				} else if v/r != j {
					b.Errorf("fsync.Map: item at %d should equal %d/%d", j, i, r)
				}
			}
			i++
		}
	})
}

func BenchmarkFsyncMapReadOnly(b *testing.B) {
	var m fsync.Map[int, int]
	for i := range preloadedKeys {
		m.Store(i, -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if v, ok := m.Load(j); !ok {
				b.Errorf("fsync.Map: item at %d should exist", j)
			} else if v != -j {
				b.Errorf("fsync.Map: item at %d should equal %d", j, -j)
			}
			i++
		}
	})
}

// Churn: rolling window of keys; each pair (Store, Delete) covers the
// full lifecycle of a key. With churnWindow keys in flight, the bench
// exercises slot recycling under contention.
func BenchmarkFsyncMapChurn(b *testing.B) {
	var m fsync.Map[int, int]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := (i >> 1) & (churnWindow - 1)
			if i&1 == 0 {
				m.Store(k, i)
			} else {
				m.Delete(k)
			}
			i++
		}
	})
}

// Same as BenchmarkFsyncMapReadOnly but the table is presized via
// m.Grow(preloadedKeys) BEFORE the preload Stores happen. Comparing
// the two settles whether the rebuilds-during-warmup leave the table
// in a less favorable shape for the steady-state Load.
func BenchmarkFsyncMapReadOnlyPresized(b *testing.B) {
	var m fsync.Map[int, int]
	m.Grow(preloadedKeys)
	for i := range preloadedKeys {
		m.Store(i, -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if v, ok := m.Load(j); !ok {
				b.Errorf("fsync.Map: item at %d should exist", j)
			} else if v != -j {
				b.Errorf("fsync.Map: item at %d should equal %d", j, -j)
			}
			i++
		}
	})
}

// LoadOrStore on a pre-populated map: every call hits an existing key
// and degenerates to a Load. Measures the steady-state get-or-set cost.
func BenchmarkFsyncMapLoadOrStore(b *testing.B) {
	var m fsync.Map[int, int]
	for i := range preloadedKeys {
		m.Store(i, -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			_, _ = m.LoadOrStore(j, -j)
			i++
		}
	})
}

// Presized variant of LoadOrStore: same as above but the table is
// grown to preloadedKeys before any Store happens.
func BenchmarkFsyncMapLoadOrStorePresized(b *testing.B) {
	var m fsync.Map[int, int]
	m.Grow(preloadedKeys)
	for i := range preloadedKeys {
		m.Store(i, -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			_, _ = m.LoadOrStore(j, -j)
			i++
		}
	})
}

// Range over preloadedKeys entries: 1 op = 1 full sweep.
func BenchmarkFsyncMapRange(b *testing.B) {
	var m fsync.Map[int, int]
	for i := range preloadedKeys {
		m.Store(i, -i)
	}
	b.ResetTimer()
	for range b.N {
		sum := 0
		m.Range(func(_, v int) bool {
			sum += v
			return true
		})
		_ = sum
	}
}

func BenchmarkFsyncMapStringReadOnly(b *testing.B) {
	var m fsync.Map[string, int]
	keys := make([]string, preloadedKeys)
	for i := range preloadedKeys {
		keys[i] = strconv.Itoa(i)
		m.Store(keys[i], -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if v, ok := m.Load(keys[j]); !ok {
				b.Errorf("fsync.Map: item %q should exist", keys[j])
			} else if v != -j {
				b.Errorf("fsync.Map: item %q should equal %d", keys[j], -j)
			}
			i++
		}
	})
}

func BenchmarkFsyncMapStringLoadOrStore(b *testing.B) {
	var m fsync.Map[string, int]
	keys := make([]string, preloadedKeys)
	for i := range preloadedKeys {
		keys[i] = strconv.Itoa(i)
		m.Store(keys[i], -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			_, _ = m.LoadOrStore(keys[j], -j)
			i++
		}
	})
}

// BenchmarkFsyncMapStringStoreWithAlloc inserts new string keys each
// iteration. The strconv.Itoa allocation is *included* in the per-op
// cost: this is the realistic price of "a fresh string key from
// somewhere external", as François asked.
func BenchmarkFsyncMapStringStoreWithAlloc(b *testing.B) {
	var m fsync.Map[string, int]
	m.Grow(1 << 18)
	var seq atomic.Int64
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		base := int(seq.Add(1 << 30))
		i := 0
		for pb.Next() {
			k := strconv.Itoa(base + i)
			m.Store(k, i)
			i++
		}
	})
}

// ---------- fsync.Store[int] ----------

func BenchmarkFsyncStoreStore(b *testing.B) {
	var s fsync.Store[int]
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int64(start.Add(1_000_000))
		for pb.Next() {
			s.Store(i, -int(i))
			i++
		}
	})
}

// Pre-sized variant: Grow the bucket-pointer table once before the
// parallel Store loop. Buckets themselves are still allocated lazily
// — we just avoid the chain of intermediate table doublings.
func BenchmarkFsyncStoreGrowStore(b *testing.B) {
	var s fsync.Store[int]
	s.Grow(1 << 24) // table sized for ~16M indexes up-front
	var start atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int64(start.Add(1_000_000))
		for pb.Next() {
			s.Store(i, -int(i))
			i++
		}
	})
}

func BenchmarkFsyncStoreReadHeavy(b *testing.B) {
	var s fsync.Store[int]
	const r = 10
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i / r
			if i%r == 0 {
				s.Store(int64(j), i)
			} else {
				if v, ok := s.Load(int64(j)); !ok {
					b.Errorf("fsync.Store: item at %d should exist", j)
				} else if v/r != j {
					b.Errorf("fsync.Store: item at %d should equal %d/%d (got %d)", j, i, r, v)
				}
			}
			i++
		}
	})
}

func BenchmarkFsyncStoreReadOnly(b *testing.B) {
	var s fsync.Store[int64]
	const keys = 65536
	for i := range int64(keys) {
		s.Store(i, -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := int64(i & (keys - 1))
			if v, ok := s.Load(j); !ok {
				b.Errorf("fsync.Store: item at %d should exist", j)
			} else if v != -j {
				b.Errorf("fsync.Store: item at %d should equal %d (got %v)", j, -j, v)
			}
			i++
		}
	})
}

// Churn for Store: same shape, rolling int64 keys.
func BenchmarkFsyncStoreChurn(b *testing.B) {
	var s fsync.Store[int]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := int64((i >> 1) & (churnWindow - 1))
			if i&1 == 0 {
				s.Store(k, i)
			} else {
				s.Delete(k)
			}
			i++
		}
	})
}

func BenchmarkFsyncStoreLoadOrStore(b *testing.B) {
	var s fsync.Store[int]
	const keys = 65536
	for i := range int64(keys) {
		s.Store(i, -int(i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := int64(i & (keys - 1))
			_, _ = s.LoadOrStore(j, -int(j))
			i++
		}
	})
}

func BenchmarkFsyncStoreRange(b *testing.B) {
	var s fsync.Store[int]
	const keys = 65536
	for i := range int64(keys) {
		s.Store(i, -int(i))
	}
	b.ResetTimer()
	for range b.N {
		sum := 0
		s.Range(func(_ int64, v int) bool {
			sum += v
			return true
		})
		_ = sum
	}
}

// ---------- fsync.MutexStore[int] ----------

func BenchmarkFsyncMutexStoreStore(b *testing.B) {
	var s fsync.MutexStore[int]
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int64(start.Add(1_000_000))
		for pb.Next() {
			s.Store(i, -int(i))
			i++
		}
	})
}

func BenchmarkFsyncMutexStoreReadHeavy(b *testing.B) {
	var s fsync.MutexStore[int]
	const r = 10
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i / r
			if i%r == 0 {
				s.Store(int64(j), i)
			} else {
				if v, ok := s.Load(int64(j)); !ok {
					b.Errorf("fsync.MutexStore: item at %d should exist", j)
				} else if v/r != j {
					b.Errorf("fsync.MutexStore: item at %d should equal %d/%d (got %d)", j, i, r, v)
				}
			}
			i++
		}
	})
}

func BenchmarkFsyncMutexStoreReadOnly(b *testing.B) {
	var s fsync.MutexStore[int64]
	const keys = 65536
	for i := range int64(keys) {
		s.Store(i, -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := int64(i & (keys - 1))
			if v, ok := s.Load(j); !ok {
				b.Errorf("fsync.MutexStore: item at %d should exist", j)
			} else if v != -j {
				b.Errorf("fsync.MutexStore: item at %d should equal %d (got %v)", j, -j, v)
			}
			i++
		}
	})
}

func BenchmarkFsyncMutexStoreChurn(b *testing.B) {
	var s fsync.MutexStore[int]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := int64((i >> 1) & (churnWindow - 1))
			if i&1 == 0 {
				s.Store(k, i)
			} else {
				s.Delete(k)
			}
			i++
		}
	})
}

func BenchmarkFsyncMutexStoreLoadOrStore(b *testing.B) {
	var s fsync.MutexStore[int]
	const keys = 65536
	for i := range int64(keys) {
		s.Store(i, -int(i))
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := int64(i & (keys - 1))
			_, _ = s.LoadOrStore(j, -int(j))
			i++
		}
	})
}

func BenchmarkFsyncMutexStoreRange(b *testing.B) {
	var s fsync.MutexStore[int]
	const keys = 65536
	for i := range int64(keys) {
		s.Store(i, -int(i))
	}
	b.ResetTimer()
	for range b.N {
		sum := 0
		s.Range(func(_ int64, v int) bool {
			sum += v
			return true
		})
		_ = sum
	}
}
