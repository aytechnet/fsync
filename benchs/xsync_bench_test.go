package benchs

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
)

func BenchmarkXsyncMapStore(b *testing.B) {
	m := xsync.NewMap[int, int]()
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			m.Store(i, -i)
			i++
		}
	})
}

// GrowStore equivalent for xsync: presize the table at construction.
func BenchmarkXsyncMapGrowStore(b *testing.B) {
	m := xsync.NewMap[int, int](xsync.WithPresize(growHint))
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

func BenchmarkXsyncMapReadHeavy(b *testing.B) {
	m := xsync.NewMap[int, int]()
	const r = 10
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i / r
			if i%r == 0 {
				m.Store(j, i)
			} else {
				if v, ok := m.Load(j); !ok {
					b.Errorf("xsync.Map: item at %d should exist", j)
				} else if v/r != j {
					b.Errorf("xsync.Map: item at %d should equal %d/%d", j, i, r)
				}
			}
			i++
		}
	})
}

func BenchmarkXsyncMapReadOnly(b *testing.B) {
	m := xsync.NewMap[int, int]()
	for i := range preloadedKeys {
		m.Store(i, -i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if v, ok := m.Load(j); !ok {
				b.Errorf("xsync.Map: item at %d should exist", j)
			} else if v != -j {
				b.Errorf("xsync.Map: item at %d should equal %d", j, -j)
			}
			i++
		}
	})
}

func BenchmarkXsyncMapChurn(b *testing.B) {
	m := xsync.NewMap[int, int]()
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

func BenchmarkXsyncMapLoadOrStore(b *testing.B) {
	m := xsync.NewMap[int, int]()
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

func BenchmarkXsyncMapRange(b *testing.B) {
	m := xsync.NewMap[int, int]()
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

// ---------- string-keyed benches ----------

func BenchmarkXsyncMapStringReadOnly(b *testing.B) {
	m := xsync.NewMap[string, int]()
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

func BenchmarkXsyncMapStringLoadOrStore(b *testing.B) {
	m := xsync.NewMap[string, int]()
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

func BenchmarkXsyncMapStringStoreWithAlloc(b *testing.B) {
	m := xsync.NewMap[string, int](xsync.WithPresize(1 << 18))
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
