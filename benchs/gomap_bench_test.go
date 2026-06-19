package benchs

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// BenchmarkGoMapMutexStore: plain Go map guarded by sync.Mutex, write-heavy.
func BenchmarkGoMapMutexStore(b *testing.B) {
	m := make(map[int]int)
	var l sync.Mutex
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			l.Lock()
			m[i] = -i
			l.Unlock()
			i++
		}
	})
}

func BenchmarkGoMapMutexReadHeavy(b *testing.B) {
	m := make(map[int]int)
	var l sync.Mutex
	const r = 10
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i / r
			if i%r == 0 {
				l.Lock()
				m[j] = i
				l.Unlock()
			} else {
				l.Lock()
				v, ok := m[j]
				l.Unlock()
				if !ok {
					b.Errorf("gomap+Mutex: item at %d should exist", j)
				} else if v/r != j {
					b.Errorf("gomap+Mutex: item at %d should equal %d/%d", j, i, r)
				}
			}
			i++
		}
	})
}

func BenchmarkGoMapMutexChurn(b *testing.B) {
	m := make(map[int]int, churnWindow)
	var l sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := (i >> 1) & (churnWindow - 1)
			l.Lock()
			if i&1 == 0 {
				m[k] = i
			} else {
				delete(m, k)
			}
			l.Unlock()
			i++
		}
	})
}

func BenchmarkGoMapRWMutexStore(b *testing.B) {
	m := make(map[int]int)
	var l sync.RWMutex
	var start atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		i := int(start.Add(1_000_000_000_000))
		for pb.Next() {
			l.Lock()
			m[i] = -i
			l.Unlock()
			i++
		}
	})
}

func BenchmarkGoMapRWMutexReadHeavy(b *testing.B) {
	m := make(map[int]int)
	var l sync.RWMutex
	const r = 10
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i / r
			if i%r == 0 {
				l.Lock()
				m[j] = i
				l.Unlock()
			} else {
				l.RLock()
				v, ok := m[j]
				l.RUnlock()
				if !ok {
					b.Errorf("gomap+RWMutex: item at %d should exist", j)
				} else if v/r != j {
					b.Errorf("gomap+RWMutex: item at %d should equal %d/%d", j, i, r)
				}
			}
			i++
		}
	})
}

func BenchmarkGoMapRWMutexChurn(b *testing.B) {
	m := make(map[int]int, churnWindow)
	var l sync.RWMutex
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			k := (i >> 1) & (churnWindow - 1)
			l.Lock()
			if i&1 == 0 {
				m[k] = i
			} else {
				delete(m, k)
			}
			l.Unlock()
			i++
		}
	})
}

// BenchmarkGoMapStringReadOnly: plain Go map[string]int, lockless reads.
func BenchmarkGoMapStringReadOnly(b *testing.B) {
	m := make(map[string]int, preloadedKeys)
	keys := make([]string, preloadedKeys)
	for i := range preloadedKeys {
		keys[i] = strconv.Itoa(i)
		m[keys[i]] = -i
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			_ = m[keys[j]]
			i++
		}
	})
}

// BenchmarkGoMapReadOnly: pre-populated plain Go map, parallel reads only.
// Safe because concurrent reads of an unmodified Go map don't race.
func BenchmarkGoMapReadOnly(b *testing.B) {
	m := make(map[int]int, preloadedKeys)
	for i := range preloadedKeys {
		m[i] = -i
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			j := i & (preloadedKeys - 1)
			if v, ok := m[j]; !ok {
				b.Errorf("gomap: item at %d should exist", j)
			} else if v != -j {
				b.Errorf("gomap: item at %d should equal %d", j, -j)
			}
			i++
		}
	})
}
