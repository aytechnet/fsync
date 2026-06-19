package benchs

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkSyncMapStore(b *testing.B) {
	var m sync.Map
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			m.Store(i, -i)
			i++
		}
	})
}

func BenchmarkSyncMapReadHeavy(b *testing.B) {
	var m sync.Map
	const r = 10
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i / r
			if i%r == 0 {
				m.Store(j, i)
			} else {
				if v, ok := m.Load(j); !ok {
					b.Errorf("sync.Map: item at %d should exist", j)
				} else if v.(int)/r != j {
					b.Errorf("sync.Map: item at %d should equal %d/%d", j, i, r)
				}
			}
			i++
		}
	})
}

func BenchmarkSyncMapReadOnly(b *testing.B) {
	var m sync.Map
	for i := range preloadedKeys {
		m.Store(i, -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if v, ok := m.Load(j); !ok {
				b.Errorf("sync.Map: item at %d should exist", j)
			} else if v.(int) != -j {
				b.Errorf("sync.Map: item at %d should equal %d", j, -j)
			}
			i++
		}
	})
}

func BenchmarkSyncMapChurn(b *testing.B) {
	var m sync.Map
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

func BenchmarkSyncMapLoadOrStore(b *testing.B) {
	var m sync.Map
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

func BenchmarkSyncMapRange(b *testing.B) {
	var m sync.Map
	for i := range preloadedKeys {
		m.Store(i, -i)
	}
	b.ResetTimer()
	for range b.N {
		sum := 0
		m.Range(func(_, v any) bool {
			sum += v.(int)
			return true
		})
		_ = sum
	}
}

// ---------- string-keyed benches (parallel to the int-keyed ones above) ----------

func BenchmarkSyncMapStringReadOnly(b *testing.B) {
	var m sync.Map
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
			_, _ = m.Load(keys[j])
			i++
		}
	})
}

func BenchmarkSyncMapStringLoadOrStore(b *testing.B) {
	var m sync.Map
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

func BenchmarkSyncMapStringStoreWithAlloc(b *testing.B) {
	var m sync.Map
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
