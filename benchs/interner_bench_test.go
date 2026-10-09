package benchs

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/aytechnet/fsync"
)

// Interner benchmarks: key → dense int64 id, both ways.
//
//   - fsync.Interner[string]       — LockOrStore insertion, lock-free reads.
//   - map[string]int64 + []string under sync.RWMutex — naïve baseline.
//
// Workloads: Intern of new keys (parallel), Intern of existing keys
// (the steady state: dictionary already warm), Key (id → key).

const internerBenchKeys = 1 << 16

var internerBenchStrings = func() []string {
	s := make([]string, internerBenchKeys)
	for i := range s {
		s[i] = "/categorie/produit-" + strconv.Itoa(i)
	}
	return s
}()

type mutexInterner struct {
	mu   sync.RWMutex
	ids  map[string]int64
	keys []string
}

func (in *mutexInterner) Intern(k string) int64 {
	in.mu.RLock()
	id, ok := in.ids[k]
	in.mu.RUnlock()
	if ok {
		return id
	}
	in.mu.Lock()
	defer in.mu.Unlock()
	if id, ok := in.ids[k]; ok {
		return id
	}
	in.keys = append(in.keys, k)
	id = int64(len(in.keys))
	in.ids[k] = id
	return id
}

func (in *mutexInterner) Key(id int64) string {
	in.mu.RLock()
	defer in.mu.RUnlock()
	return in.keys[id-1]
}

// ---------- Intern, new keys ----------

func BenchmarkFsyncInternerInternNew(b *testing.B) {
	var in fsync.Interner[int]
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			in.Intern(i)
			i++
		}
	})
}

func BenchmarkMutexInternerInternNew(b *testing.B) {
	in := &mutexInterner{ids: map[string]int64{}}
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			in.Intern(strconv.Itoa(i))
			i++
		}
	})
}

// ---------- Intern, existing keys (warm dictionary) ----------

func BenchmarkFsyncInternerInternExisting(b *testing.B) {
	in := fsync.NewInterner[string]().Grow(internerBenchKeys)
	for _, s := range internerBenchStrings {
		in.Intern(s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			in.Intern(internerBenchStrings[i&(internerBenchKeys-1)])
			i++
		}
	})
}

func BenchmarkMutexInternerInternExisting(b *testing.B) {
	in := &mutexInterner{ids: map[string]int64{}}
	for _, s := range internerBenchStrings {
		in.Intern(s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			in.Intern(internerBenchStrings[i&(internerBenchKeys-1)])
			i++
		}
	})
}

// ---------- Key (id → key) ----------

func BenchmarkFsyncInternerKey(b *testing.B) {
	in := fsync.NewInterner[string]().Grow(internerBenchKeys)
	for _, s := range internerBenchStrings {
		in.Intern(s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			in.Key(int64(i&(internerBenchKeys-1)) + 1)
			i++
		}
	})
}

func BenchmarkMutexInternerKey(b *testing.B) {
	in := &mutexInterner{ids: map[string]int64{}}
	for _, s := range internerBenchStrings {
		in.Intern(s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			in.Key(int64(i&(internerBenchKeys-1)) + 1)
			i++
		}
	})
}

// ---------- Lookup in a large, cold dictionary ----------

// 1M keys, looked up in a pseudo-random order: most probes miss the CPU
// caches, which is where a layout keeping keys out of the table could
// pay a second cache line per hit.
const internerLargeKeys = 1 << 20

var internerLargeStrings = func() []string {
	s := make([]string, internerLargeKeys)
	for i := range s {
		s[i] = "/categorie/produit-" + strconv.Itoa(i)
	}
	return s
}()

func BenchmarkFsyncInternerLookupLargeHit(b *testing.B) {
	in := fsync.NewInterner[string]()
	for _, s := range internerLargeStrings {
		in.Intern(s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		x := uint64(88172645463325252)
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			in.Lookup(internerLargeStrings[x&(internerLargeKeys-1)])
		}
	})
}

func BenchmarkFsyncInternerLookupLargeMiss(b *testing.B) {
	in := fsync.NewInterner[string]()
	for _, s := range internerLargeStrings {
		in.Intern(s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		x := uint64(88172645463325252)
		for pb.Next() {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			in.Lookup(internerLargeStrings[x&(internerLargeKeys-1)] + "?")
		}
	})
}
