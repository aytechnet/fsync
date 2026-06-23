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

// ---------- Bitmap.Range by density ----------
//
// Bitmap stores 8 atomic.Uint64 per bucket (512 bits). Range uses
// bits.TrailingZeros64 on each word so the cost per *bit visited*
// drops as more bits share a word, but the cost per *bucket
// scanned* is constant (8 Loads + 8 popcounts at minimum). These
// benches keep the number of bits visited constant (rangeKeysCount)
// and vary the step between bits to span four regimes from dense
// to extreme sparse.

const rangeKeysCount = 2048

func benchBitmapRangeDensity(b *testing.B, step int64) {
	var bm fsync.Bitmap
	for i := int64(0); i < rangeKeysCount; i++ {
		bm.Set(i * step)
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

// Dense: every bit set, 2048 bits in 4 buckets, 64 bits per word.
func BenchmarkFsyncBitmapRangeDense(b *testing.B) { benchBitmapRangeDensity(b, 1) }

// Sparse 1/8: every 8th bit set, 2048 bits in 32 buckets, 8 bits per word.
func BenchmarkFsyncBitmapRangeSparse8(b *testing.B) { benchBitmapRangeDensity(b, 8) }

// Sparse 1/64: 1 bit per word, 2048 bits in 256 buckets.
func BenchmarkFsyncBitmapRangeSparse64(b *testing.B) { benchBitmapRangeDensity(b, 64) }

// Sparse 1/512: 1 bit per bucket, 2048 bits in 2048 buckets — worst case.
func BenchmarkFsyncBitmapRangeSparse512(b *testing.B) { benchBitmapRangeDensity(b, 512) }

// Sparse 1/4096: 1 bit per 8 buckets (7 empty buckets between each set
// bit), 2048 bits in 16384 buckets — pessimal scan-to-visit ratio.
func BenchmarkFsyncBitmapRangeSparse4096(b *testing.B) { benchBitmapRangeDensity(b, 4096) }

// ---------- Bitmap.Range scaling on dense input ----------

func benchBitmapRangeSize(b *testing.B, total int64) {
	var bm fsync.Bitmap
	for i := int64(0); i < total; i++ {
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

func BenchmarkFsyncBitmapRange1K(b *testing.B)   { benchBitmapRangeSize(b, 1024) }
func BenchmarkFsyncBitmapRange16K(b *testing.B)  { benchBitmapRangeSize(b, 16*1024) }
func BenchmarkFsyncBitmapRange256K(b *testing.B) { benchBitmapRangeSize(b, 256*1024) }
func BenchmarkFsyncBitmapRange1M(b *testing.B)   { benchBitmapRangeSize(b, 1024*1024) }
