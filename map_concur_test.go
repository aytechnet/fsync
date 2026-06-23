package fsync

import (
	"sync"
	"testing"
)

// Functional sanity tests
func TestMapLoadStore(t *testing.T) {
	m := NewMap[int, int]().Grow(16)
	if _, ok := m.Load(42); ok {
		t.Errorf("Load on empty should return ok=false")
	}
	if !m.Store(1, 10) {
		t.Errorf("first Store should return true")
	}
	if m.Store(1, 100) {
		t.Errorf("second Store of same key should return false")
	}
	v, ok := m.Load(1)
	if !ok || v != 100 {
		t.Errorf("Load(1) = %d, %v; want 100, true", v, ok)
	}
	if m.Len() != 1 {
		t.Errorf("Len = %d; want 1", m.Len())
	}
}

func TestMapLockUnlock(t *testing.T) {
	m := NewMap[int, int]().Grow(16)
	m.Store(1, 10)
	p, cur, ok := m.Lock(1)
	if !ok || p == nil || *p != 10 {
		t.Fatalf("Lock(1) failed: ok=%v p=%v", ok, p)
	}
	*p = 99
	cur.Unlock()
	v, _ := m.Load(1)
	if v != 99 {
		t.Errorf("after Lock-mutate-Unlock Load = %d; want 99", v)
	}
	_, _, ok = m.Lock(999)
	if ok {
		t.Errorf("Lock on absent key should return ok=false")
	}
}

func TestMapDelete(t *testing.T) {
	m := NewMap[int, int]().Grow(16)
	if m.Delete(42) {
		t.Errorf("Delete on empty should return false")
	}
	m.Store(1, 10)
	m.Store(2, 20)
	if !m.Delete(1) {
		t.Errorf("Delete(1) should return true")
	}
	if _, ok := m.Load(1); ok {
		t.Errorf("Load(1) should fail after Delete")
	}
	if v, ok := m.Load(2); !ok || v != 20 {
		t.Errorf("Load(2) = %d, %v; want 20, true", v, ok)
	}
	if m.Len() != 1 {
		t.Errorf("Len = %d; want 1 (after Delete)", m.Len())
	}
	if m.Delete(1) {
		t.Errorf("second Delete(1) should return false")
	}
}

// TestMapDeleteAndReinsert verifies that a deleted slot is immediately
// reusable: a fresh Store after Delete should succeed and Len stays
// coherent.
func TestMapDeleteAndReinsert(t *testing.T) {
	m := NewMap[int, int]().Grow(16)
	// fill several buckets
	for i := range 100 {
		m.Store(i, i*7)
	}
	// delete every other key
	for i := 0; i < 100; i += 2 {
		if !m.Delete(i) {
			t.Errorf("Delete(%d) returned false", i)
		}
	}
	if m.Len() != 50 {
		t.Errorf("Len after 50 Deletes = %d; want 50", m.Len())
	}
	// remaining keys still reachable with old values
	for i := 1; i < 100; i += 2 {
		if v, ok := m.Load(i); !ok || v != i*7 {
			t.Errorf("Load(%d) = %d, %v; want %d", i, v, ok, i*7)
		}
	}
	// re-insert with new values (reuses the freed slots)
	for i := 0; i < 100; i += 2 {
		if !m.Store(i, i*11) {
			t.Errorf("Store(%d, %d) returned created=false on reinsert", i, i*11)
		}
	}
	if m.Len() != 100 {
		t.Errorf("Len after re-insert = %d; want 100", m.Len())
	}
	for i := range 100 {
		want := i * 7
		if i%2 == 0 {
			want = i * 11
		}
		if v, ok := m.Load(i); !ok || v != want {
			t.Errorf("Load(%d) = %d, %v; want %d", i, v, ok, want)
		}
	}
}

func TestMapLockOrStore(t *testing.T) {
	m := NewMap[int, int]().Grow(16)
	p, cur, created := m.LockOrStore(1, 10)
	if p == nil || *p != 10 {
		t.Fatalf("LockOrStore(1, 10) returned %v", p)
	}
	if !created {
		t.Errorf("LockOrStore on empty slot should report created=true")
	}
	*p = 20
	cur.Unlock()
	v, _ := m.Load(1)
	if v != 20 {
		t.Errorf("after LockOrStore-mutate Load = %d; want 20", v)
	}
	p2, cur2, created := m.LockOrStore(1, 999)
	if *p2 != 20 {
		t.Errorf("LockOrStore on existing key should return existing value, got %d", *p2)
	}
	if created {
		t.Errorf("LockOrStore on existing slot should report created=false")
	}
	cur2.Unlock()
}

func TestMapBigConcurrent(t *testing.T) {
	m := NewMap[int, int]().Grow(16384) // 16k buckets × 8 slots = 128k capacity
	var w sync.WaitGroup
	const items = 5000
	const goroutines = 12
	for g := range goroutines {
		w.Add(1)
		go func() {
			defer w.Done()
			for j := range items {
				k := g*items + j
				m.Store(k, k*3)
				if v, ok := m.Load(k); !ok || v != k*3 {
					t.Errorf("Load(%d) = %d, %v; want %d, true", k, v, ok, k*3)
				}
			}
		}()
	}
	w.Wait()
	if got := m.Len(); got != items*goroutines {
		t.Errorf("Len = %d; want %d", got, items*goroutines)
	}
}

// TestMapGrow exercises the Grow primitive on a zero-value Map and
// confirms it ends up large enough that the subsequent inserts never
// trigger a rebuild.
func TestMapGrow(t *testing.T) {
	var m Map[int, int]
	m.Grow(10_000)
	// 10000 items / 8 slots / 0.75 load factor ≈ 1667 buckets needed,
	// rounded up to next power of two = 2048.
	if got := len(m.table.Load().buckets); got < 2048 {
		t.Errorf("after Grow(10000) bucket count = %d, want >= 2048", got)
	}
	for i := range 10_000 {
		m.Store(i, i*7)
	}
	if m.table.Load().nextTable.Load() != nil {
		t.Errorf("Grow pre-sizing not enough — rebuild triggered before 10000 inserts")
	}
	for i := range 10_000 {
		if v, ok := m.Load(i); !ok || v != i*7 {
			t.Errorf("Load(%d) = %d, %v; want %d", i, v, ok, i*7)
		}
	}
}

// TestMapZeroValueUsable verifies a literal `var m Map[K,V]` works
// directly — smoke test for the lazy init path.
func TestMapZeroValueUsable(t *testing.T) {
	var m Map[int, int]
	if _, ok := m.Load(42); ok {
		t.Errorf("Load on zero-value should be ok=false")
	}
	if !m.Store(1, 10) {
		t.Errorf("Store on zero-value should return true")
	}
	v, ok := m.Load(1)
	if !ok || v != 10 {
		t.Errorf("Load(1) = %d, %v; want 10, true", v, ok)
	}
}

// TestMapRebuild forces several doublings by starting small and
// inserting beyond capacity.
func TestMapRebuild(t *testing.T) {
	m := NewMap[int, int]().Grow(16) // start very small to exercise multiple doublings
	var w sync.WaitGroup
	const items = 5000
	const goroutines = 8
	for g := range goroutines {
		w.Add(1)
		go func() {
			defer w.Done()
			for j := range items {
				k := g*items + j
				m.Store(k, k*7)
			}
		}()
	}
	w.Wait()
	// verify every key is found with the right value
	missing := 0
	for k := range items * goroutines {
		if v, ok := m.Load(k); !ok || v != k*7 {
			missing++
			if missing < 5 {
				t.Errorf("Load(%d) = %d, %v; want %d", k, v, ok, k*7)
			}
		}
	}
	if missing > 0 {
		t.Errorf("total missing: %d/%d", missing, items*goroutines)
	}
	if got := m.Len(); got != items*goroutines {
		t.Errorf("Len = %d; want %d", got, items*goroutines)
	}
}

// TestMapRebuildWithPins ensures that pinned buckets remain reachable
// after one or more rebuilds (duplication policy).
func TestMapRebuildWithPins(t *testing.T) {
	m := NewMap[int, int]().Grow(64)
	const items = 200
	// Pre-populate and pin every 8th key — this forces some buckets to be
	// duplicated rather than split when the next rebuild triggers.
	for k := range items {
		m.Store(k, k*7)
	}
	pinned := make([]Cursor[int, int], 0, items/8)
	for k := 0; k < items; k += 8 {
		p, c, ok := m.Lock(k)
		if !ok {
			t.Fatalf("Lock(%d) failed", k)
		}
		_ = p
		pinned = append(pinned, c)
	}
	// Now force several more rebuilds by inserting fresh keys.
	for k := items; k < items*40; k++ {
		m.Store(k, k*7)
	}
	// Non-pinned keys must be readable even during the pin window — the
	// pinned ones we cannot Load yet (Load blocks on pin; same goroutine
	// holding the pin would deadlock itself).
	for k := 1; k < items*40; k++ {
		if k%8 == 0 && k < items {
			continue
		}
		if v, ok := m.Load(k); !ok || v != k*7 {
			t.Errorf("during pin window Load(%d) = %d, %v; want %d", k, v, ok, k*7)
		}
	}
	// Release pins, then check the previously-pinned keys are still
	// reachable with their old values (the duplicate-on-pin policy kept
	// the bucket physically alive across the rebuild).
	for _, c := range pinned {
		c.Unlock()
	}
	for k := 0; k < items; k += 8 {
		if v, ok := m.Load(k); !ok || v != k*7 {
			t.Errorf("after unlock Load(%d) = %d, %v; want %d", k, v, ok, k*7)
		}
	}
}

// Benchmarks: fixed-size proto, so we pick bucket counts that comfortably
// hold the working set without overflow chains kicking in.

// BenchmarkProtoLoadReadOnly: 2048 keys preloaded. Each Load is a single
// bucket hit, no chain walk. This is the architecture's best case.
