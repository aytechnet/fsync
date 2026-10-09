package fsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// Regression: a bucket that is pinned while the table doubles takes the
// DUPLICATE path (state back to bucketOpen). It must still be counted
// once in rebuildLeft, and the table must never be promoted while some
// of its buckets are not migrated yet — otherwise readers and writers
// dereference a nil bucket of the new table.
func TestMapGrowWhilePinnedNoNilBucket(t *testing.T) {
	const (
		keys    = 50_000
		writers = 8
	)
	var m Map[int, int64]
	var next atomic.Int64
	var wg sync.WaitGroup
	var done atomic.Bool
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !done.Load(); i++ {
				m.Load(i % keys)
			}
		}()
	}
	var ww sync.WaitGroup
	for w := 0; w < writers; w++ {
		ww.Add(1)
		go func(w int) {
			defer ww.Done()
			for i := 0; i < keys; i++ {
				k := (i + w*keys/writers) % keys
				if _, ok := m.Load(k); ok {
					continue
				}
				p, cur, created := m.LockOrStore(k, 0)
				if created {
					*p = next.Add(1)
					runtime.Gosched() // hold the pin across a possible doubling
				}
				cur.Unlock()
			}
		}(w)
	}
	ww.Wait()
	done.Store(true)
	wg.Wait()
	if m.Len() != keys {
		t.Fatalf(`Len() = %d, want %d`, m.Len(), keys)
	}
	for k := 0; k < keys; k++ {
		if _, ok := m.Load(k); !ok {
			t.Fatalf(`key %d lost after concurrent growth`, k)
		}
	}
}
