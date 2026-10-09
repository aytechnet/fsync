package fsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// mapShape inspects the current table: buckets shared between two
// positions (the old duplicate-on-pin policy), longest chain, and
// whether a rebuild is still pending.
func mapShape[K comparable, V any](m *Map[K, V]) (shared, maxChain int, pending bool) {
	t := m.table.Load()
	n := uint64(len(t.buckets))
	for i := uint64(0); i < n; i++ {
		b := t.buckets[i].Load()
		if n > 1 && t.buckets[i^(n>>1)].Load() == b {
			shared++
		}
		c := 0
		for cur := b; cur != nil; cur = cur.next.Load() {
			c++
		}
		if c > maxChain {
			maxChain = c
		}
	}
	return shared, maxChain, t.nextTable.Load() != nil
}

// Solution B: pinned buckets are skipped, never duplicated. Under heavy
// concurrent growth with pins held across doublings, no bucket ends up
// shared and chains stay short (with duplication, the same workload
// produced 22 % shared positions and chains of 2 000+ buckets).
func TestMapGrowWithPinsNoSharingShortChains(t *testing.T) {
	const (
		workers = 12
		perW    = 50_000
	)
	var m Map[int, int64]
	var next atomic.Int64
	var start atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := int(start.Add(1_000_000_000))
			for n := 0; n < perW; n++ {
				p, cur, created := m.LockOrStore(i, 0)
				if created {
					*p = next.Add(1)
					runtime.Gosched() // hold the pin across a doubling
				}
				cur.Unlock()
				i++
			}
		}()
	}
	wg.Wait()
	// a few more writes let the lazy sweep finish any skipped position
	for i := 0; i < 4096; i++ {
		m.Store(-1-i, 0)
	}
	if m.Len() != workers*perW+4096 {
		t.Fatalf(`Len() = %d, want %d`, m.Len(), workers*perW+4096)
	}
	shared, maxChain, pending := mapShape(&m)
	t.Logf(`buckets=%d shared=%d maxChain=%d pending=%v`, len(m.table.Load().buckets), shared, maxChain, pending)
	if shared != 0 {
		t.Errorf(`%d positions share a bucket, want 0 (no duplication)`, shared)
	}
	if maxChain > 8 {
		t.Errorf(`longest chain %d buckets, want ≤ 8`, maxChain)
	}
}

// A Lock held across a doubling: the pinned bucket is skipped (its *V
// stays valid and writable), the map keeps working, and the doubling
// completes once the pin is released and writes continue.
func TestMapLongLockDelaysButCompletesDoubling(t *testing.T) {
	var m Map[int, int]
	m.Store(0, 0)
	p, cur, ok := m.Lock(0)
	if !ok {
		t.Fatal(`Lock(0) failed`)
	}
	before := len(m.table.Load().buckets)
	for i := 1; i <= 2000; i++ { // several load-factor thresholds
		m.Store(i, i)
	}
	*p = 42 // the pinned slot never moved: writing through *V is safe
	for i := 1; i <= 2000; i++ {
		if v, ok := m.Load(i); !ok || v != i {
			t.Fatalf(`Load(%d) = (%d, %v) while a doubling waits on a pin`, i, v, ok)
		}
	}
	cur.Unlock()
	for i := 2001; i <= 2200; i++ { // writes drive the lazy sweep
		m.Store(i, i)
	}
	if v, ok := m.Load(0); !ok || v != 42 {
		t.Fatalf(`Load(0) = (%d, %v), want 42: the write through *V was lost`, v, ok)
	}
	// Nothing is pinned any more: Grow drives every pending doubling to
	// completion synchronously (later doublings may have started since).
	m.Grow(m.Len())
	if after := len(m.table.Load().buckets); after <= before {
		t.Errorf(`table did not grow: %d → %d buckets`, before, after)
	}
	if _, _, pending := mapShape(&m); pending {
		t.Errorf(`doubling still pending after the pin was released`)
	}
	if m.Len() != 2201 {
		t.Errorf(`Len() = %d, want 2201`, m.Len())
	}
}
