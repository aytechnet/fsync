//go:build !race

package fsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests exercise the seqlock contract on purpose: readers copy a
// multi-word V while writers overwrite or delete it. The accesses are
// data races by design (detected and retried through pins), hence the
// !race build tag, like map_load_during_lock_test.go.

type quad [4]int64

func (q quad) consistent() bool { return q[0] == q[1] && q[1] == q[2] && q[2] == q[3] }

// Regression: Store's update path wrote the value without bumping the
// seq, so Load returned torn values (≈300k in 2 s before the fix).
func TestMapSeqlockStoreUpdateNoTornRead(t *testing.T) {
	var m Map[int, quad]
	m.Store(1, quad{1, 1, 1, 1})
	var stop atomic.Bool
	var torn, reads atomic.Int64
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				runtime.Gosched()
				v, ok := m.Load(1)
				reads.Add(1)
				if !ok || !v.consistent() {
					torn.Add(1)
				}
			}
		}()
	}
	var ww sync.WaitGroup
	for w := 0; w < 2; w++ {
		ww.Add(1)
		go func(w int) {
			defer ww.Done()
			for i := int64(1); i < 500_000; i++ {
				x := i*2 + int64(w)
				m.Store(1, quad{x, x, x, x})
			}
		}(w)
	}
	ww.Wait()
	stop.Store(true)
	wg.Wait()
	if torn.Load() > 0 {
		t.Fatalf(`%d torn or missing values out of %d Loads during Store updates`, torn.Load(), reads.Load())
	}
}

// Delete zeroes the slot: a Load racing it must report either the full
// value or "absent", never a zeroed/partial value with ok=true.
func TestMapSeqlockDeleteNoZeroOrTornRead(t *testing.T) {
	var m Map[int, quad]
	var stop atomic.Bool
	var bad atomic.Int64
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				runtime.Gosched()
				for k := 0; k < 4; k++ {
					if v, ok := m.Load(k); ok && (v[0] == 0 || !v.consistent()) {
						bad.Add(1)
					}
				}
			}
		}()
	}
	var ww sync.WaitGroup
	for w := 0; w < 2; w++ {
		ww.Add(1)
		go func() {
			defer ww.Done()
			for i := int64(1); i < 300_000; i++ {
				k := int(i % 4)
				m.Store(k, quad{i, i, i, i})
				m.Delete(k)
				m.LoadAndDelete(k)
			}
		}()
	}
	ww.Wait()
	stop.Store(true)
	wg.Wait()
	if bad.Load() > 0 {
		t.Fatalf(`%d Loads returned a zeroed or torn value with ok=true`, bad.Load())
	}
}

// Lock racing Delete: Lock must never hand out a freed slot (ok=true on
// a zeroed value), and writes through *V must land in a live entry.
func TestMapLockRevalidatesAgainstDelete(t *testing.T) {
	var m Map[int, quad]
	var stop atomic.Bool
	var bad atomic.Int64
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				runtime.Gosched()
				k := 0
				if p, cur, ok := m.Lock(k); ok {
					if (*p)[0] == 0 || !p.consistent() {
						bad.Add(1)
					}
					x := (*p)[0] + 1
					*p = quad{x, x, x, x}
					cur.Unlock()
				}
			}
		}()
	}
	var ww sync.WaitGroup
	for w := 0; w < 2; w++ {
		ww.Add(1)
		go func() {
			defer ww.Done()
			for i := int64(1); i < 200_000; i++ {
				m.Store(0, quad{i, i, i, i})
				m.Delete(0)
				if i%64 == 0 {
					runtime.Gosched()
				}
			}
		}()
	}
	ww.Wait()
	stop.Store(true)
	wg.Wait()
	if bad.Load() > 0 {
		t.Fatalf(`Lock returned %d freed or torn slots`, bad.Load())
	}
}

// Range must not yield torn pairs either.
func TestMapRangeNoTornValue(t *testing.T) {
	var m Map[int, quad]
	for k := 0; k < 64; k++ {
		m.Store(k, quad{1, 1, 1, 1})
	}
	var stop atomic.Bool
	var bad atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := int64(2); !stop.Load(); i++ {
			m.Store(int(i%64), quad{i, i, i, i})
			if i%64 == 0 {
				runtime.Gosched()
			}
		}
	}()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		m.Range(func(_ int, v quad) bool {
			if !v.consistent() {
				bad.Add(1)
			}
			return true
		})
	}
	stop.Store(true)
	wg.Wait()
	if bad.Load() > 0 {
		t.Fatalf(`Range yielded %d torn values`, bad.Load())
	}
}
