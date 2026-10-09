//go:build !race

package fsync

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Set seqlock tests: readers (Contains, Range) copy keys while Remove
// rewrites them, then validate the node's seq before comparing or
// yielding. Those copies are data races by design (detected and
// retried), hence the !race build tag, like map_seqlock_test.go.

// Regression: Range yielded keys being zeroed or torn by a concurrent
// Remove (≈34k invalid string keys in 0.06 s before the per-node seq).
func TestSetSeqlockRangeNoTornStringKeys(t *testing.T) {
	var s Set[string]
	// very different lengths: a torn header would mix them visibly
	keys := []string{"a", strings.Repeat("b", 64), "cc", strings.Repeat("d", 200)}
	valid := map[string]bool{}
	for _, k := range keys {
		valid[k] = true
	}
	var stop atomic.Bool
	var bad atomic.Int64
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				runtime.Gosched()
				s.Range(func(k string) bool {
					if !valid[k] {
						bad.Add(1)
					}
					return true
				})
				for _, k := range keys {
					s.Contains(k) // must never compare a torn key
				}
			}
		}()
	}
	for i := 0; i < 200_000; i++ {
		s.Add(keys[i%4])
		s.Remove(keys[i%4])
		s.Add(keys[(i+1)%4])
		s.Remove(keys[(i+1)%4])
	}
	stop.Store(true)
	wg.Wait()
	if bad.Load() > 0 {
		t.Fatalf(`Range yielded %d keys that were never added (torn or zeroed)`, bad.Load())
	}
}

// Concurrent stress: Add / Remove / Contains / Range while the table
// grows from its first size, so every racing branch gets exercised.
func TestSetConcurrentGrowthMixedOps(t *testing.T) {
	const (
		workers = 8
		perW    = 20_000
	)
	var s Set[int]
	var wg sync.WaitGroup
	var stop atomic.Bool
	wg.Add(1)
	go func() { // a reader ranging while tables double
		defer wg.Done()
		for !stop.Load() {
			s.Range(func(int) bool { return true })
			runtime.Gosched()
		}
	}()
	var ww sync.WaitGroup
	for w := 0; w < workers; w++ {
		ww.Add(1)
		go func(w int) {
			defer ww.Done()
			base := w * perW
			for i := 0; i < perW; i++ {
				s.Add(base + i)
				if i%3 == 0 {
					s.Remove(base + i)
				}
				if !s.Contains(base+i) && i%3 != 0 {
					t.Errorf(`key %d lost`, base+i)
					return
				}
			}
		}(w)
	}
	ww.Wait()
	stop.Store(true)
	wg.Wait()
	want := 0
	for i := 0; i < perW; i++ {
		if i%3 != 0 {
			want++
		}
	}
	if s.Len() != workers*want {
		t.Errorf(`Len() = %d, want %d`, s.Len(), workers*want)
	}
}
