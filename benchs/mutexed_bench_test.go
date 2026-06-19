package benchs

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/aytechnet/fsync"
)

// mutexedEntry is the canonical Go pattern when one needs a per-key lock:
// wrap the value in a struct that owns its own sync.Mutex, then put
// *mutexedEntry in the concurrent map. fsync.{Map,Store}.Lock /
// LockOrStore replaces this pattern with V stored inline in the bucket
// and a per-slot pin bit — no heap entry, no indirection.
type mutexedEntry struct {
	mu sync.Mutex
	v  int
}

const lockKeys = 256 // small hot set so contention shows up

// ---------- xsync.Map[int, *mutexedEntry] ----------

func BenchmarkRefXsyncMutexedEntryLockOrStore(b *testing.B) {
	m := xsync.NewMap[int, *mutexedEntry]()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (lockKeys - 1)
			e, _ := m.LoadOrStore(k, &mutexedEntry{})
			e.mu.Lock()
			e.v++
			e.mu.Unlock()
			i++
		}
	})
}

func BenchmarkRefXsyncMutexedEntryLoad(b *testing.B) {
	m := xsync.NewMap[int, *mutexedEntry]()
	for i := range lockKeys {
		m.Store(i, &mutexedEntry{})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (lockKeys - 1)
			e, _ := m.Load(k)
			e.mu.Lock()
			e.v++
			e.mu.Unlock()
			i++
		}
	})
}

// ---------- sync.Map[int, *mutexedEntry] ----------

func BenchmarkRefStdSyncMutexedEntryLoadOrStore(b *testing.B) {
	var m sync.Map
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (lockKeys - 1)
			v, _ := m.LoadOrStore(k, &mutexedEntry{})
			e := v.(*mutexedEntry)
			e.mu.Lock()
			e.v++
			e.mu.Unlock()
			i++
		}
	})
}

func BenchmarkRefStdSyncMutexedEntryLoad(b *testing.B) {
	var m sync.Map
	for i := range lockKeys {
		m.Store(i, &mutexedEntry{})
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (lockKeys - 1)
			v, _ := m.Load(k)
			e := v.(*mutexedEntry)
			e.mu.Lock()
			e.v++
			e.mu.Unlock()
			i++
		}
	})
}

// ---------- fsync.Map[int, int] — Lock / LockOrStore + inline V ----------

func BenchmarkFsyncMapLockOrStoreInc(b *testing.B) {
	var m fsync.Map[int, int]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (lockKeys - 1)
			p, cur, _ := m.LockOrStore(k, 0)
			*p++
			cur.Unlock()
			i++
		}
	})
}

func BenchmarkFsyncMapLockInc(b *testing.B) {
	var m fsync.Map[int, int]
	for i := range lockKeys {
		m.Store(i, 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (lockKeys - 1)
			p, cur, ok := m.Lock(k)
			if !ok {
				b.Fatalf("Lock(%d) failed", k)
			}
			*p++
			cur.Unlock()
			i++
		}
	})
}

// Presized variant: m.Grow(lockKeys) before the preload Stores.
func BenchmarkFsyncMapLockIncPresized(b *testing.B) {
	var m fsync.Map[int, int]
	m.Grow(lockKeys)
	for i := range lockKeys {
		m.Store(i, 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := i & (lockKeys - 1)
			p, cur, ok := m.Lock(k)
			if !ok {
				b.Fatalf("Lock(%d) failed", k)
			}
			*p++
			cur.Unlock()
			i++
		}
	})
}

// ---------- fsync.Store[int] — same Lock+inc pattern, integer key ----------

func BenchmarkFsyncStoreLockOrStoreInc(b *testing.B) {
	var s fsync.Store[int]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := int64(i & (lockKeys - 1))
			p, cur, _ := s.LockOrStore(k, 0)
			*p++
			cur.Unlock()
			i++
		}
	})
}

func BenchmarkFsyncStoreLockInc(b *testing.B) {
	var s fsync.Store[int]
	for i := range lockKeys {
		s.Store(int64(i), 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := int64(i & (lockKeys - 1))
			p, cur, ok := s.Lock(k)
			if !ok {
				b.Fatalf("Lock(%d) failed", k)
			}
			*p++
			cur.Unlock()
			i++
		}
	})
}

// ---------- fsync.MutexStore[int] — same pattern with per-slot Mutex ----------

func BenchmarkFsyncMutexStoreLockOrStoreInc(b *testing.B) {
	var s fsync.MutexStore[int]
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := int64(i & (lockKeys - 1))
			p, cur, _ := s.LockOrStore(k, 0)
			*p++
			cur.Unlock()
			i++
		}
	})
}

func BenchmarkFsyncMutexStoreLockInc(b *testing.B) {
	var s fsync.MutexStore[int]
	for i := range lockKeys {
		s.Store(int64(i), 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := int64(i & (lockKeys - 1))
			p, cur, ok := s.Lock(k)
			if !ok {
				b.Fatalf("Lock(%d) failed", k)
			}
			*p++
			cur.Unlock()
			i++
		}
	})
}

// ---------- Uncontended Lock+inc — each goroutine owns its own slot range ----------
//
// Compare these with the LockInc variants above: same code, but every
// goroutine cycles over a private range of preloadStride keys, so cache
// lines of the underlying bucket are never bounced between cores.
// This isolates the intrinsic cost of Lock+modify+Unlock from the
// contention-induced cache traffic.

const (
	uncontendedStride = 256                  // keys per goroutine
	uncontendedTotal  = uncontendedStride * 64 // enough for any GOMAXPROCS up to 64
)

func BenchmarkFsyncStoreLockIncUncontended(b *testing.B) {
	var s fsync.Store[int]
	for i := range uncontendedTotal {
		s.Store(int64(i), 0)
	}
	var seqStart atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		base := seqStart.Add(uncontendedStride) - uncontendedStride
		i := 0
		for pb.Next() {
			k := base + int64(i&(uncontendedStride-1))
			p, cur, _ := s.Lock(k)
			*p++
			cur.Unlock()
			i++
		}
	})
}

func BenchmarkFsyncMutexStoreLockIncUncontended(b *testing.B) {
	var s fsync.MutexStore[int]
	for i := range uncontendedTotal {
		s.Store(int64(i), 0)
	}
	var seqStart atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		base := seqStart.Add(uncontendedStride) - uncontendedStride
		i := 0
		for pb.Next() {
			k := base + int64(i&(uncontendedStride-1))
			p, cur, _ := s.Lock(k)
			*p++
			cur.Unlock()
			i++
		}
	})
}

func BenchmarkFsyncMapLockIncUncontended(b *testing.B) {
	var m fsync.Map[int, int]
	for i := range uncontendedTotal {
		m.Store(i, 0)
	}
	var seqStart atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		base := int(seqStart.Add(uncontendedStride) - uncontendedStride)
		i := 0
		for pb.Next() {
			k := base + (i & (uncontendedStride - 1))
			p, cur, _ := m.Lock(k)
			*p++
			cur.Unlock()
			i++
		}
	})
}

// ---------- Single-key contention — pathological worst case for Store ----------
//
// All goroutines pound the same single key. Store's bit-spin on the
// shared cacheline is maximally bounced; MutexStore's per-slot mutex
// will fall back to futex park.

func BenchmarkFsyncStoreLockIncSingleKey(b *testing.B) {
	var s fsync.Store[int]
	s.Store(0, 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p, cur, _ := s.Lock(0)
			*p++
			cur.Unlock()
		}
	})
}

func BenchmarkFsyncMutexStoreLockIncSingleKey(b *testing.B) {
	var s fsync.MutexStore[int]
	s.Store(0, 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p, cur, _ := s.Lock(0)
			*p++
			cur.Unlock()
		}
	})
}

// LockOrStore variants of the contention regimes above. Same workloads,
// but using LockOrStore on a pre-populated set: every call hits an
// existing entry, so the path degrades to "take pin, then return"
// without an init — letting us isolate the spin/CAS cost itself.

func BenchmarkFsyncStoreLockOrStoreIncUncontended(b *testing.B) {
	var s fsync.Store[int]
	for i := range uncontendedTotal {
		s.Store(int64(i), 0)
	}
	var seqStart atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		base := seqStart.Add(uncontendedStride) - uncontendedStride
		i := 0
		for pb.Next() {
			k := base + int64(i&(uncontendedStride-1))
			p, cur, _ := s.LockOrStore(k, 0)
			*p++
			cur.Unlock()
			i++
		}
	})
}

func BenchmarkFsyncStoreLockOrStoreIncSingleKey(b *testing.B) {
	var s fsync.Store[int]
	s.Store(0, 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p, cur, _ := s.LockOrStore(0, 0)
			*p++
			cur.Unlock()
		}
	})
}

// Load contention regimes — used to assess whether the Or+And pattern
// in Store.Load is bottlenecked by cacheline bouncing the way Lock was.

func BenchmarkFsyncStoreLoadUncontended(b *testing.B) {
	var s fsync.Store[int]
	for i := range uncontendedTotal {
		s.Store(int64(i), -int(i))
	}
	var seqStart atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		base := seqStart.Add(uncontendedStride) - uncontendedStride
		i := 0
		for pb.Next() {
			k := base + int64(i&(uncontendedStride-1))
			_, _ = s.Load(k)
			i++
		}
	})
}

func BenchmarkFsyncStoreLoadSingleKey(b *testing.B) {
	var s fsync.Store[int]
	s.Store(0, 42)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = s.Load(0)
		}
	})
}

