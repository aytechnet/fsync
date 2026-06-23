package fsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// ---------- Store[V] — Lock / Unlock direct tests ----------

func TestStoreLockBasic(t *testing.T) {
	var s Store[int]
	s.Store(7, 42)

	p, cur, ok := s.Lock(7)
	if !ok || p == nil || *p != 42 {
		t.Fatalf("Lock(7) = (%v, _, %v); want (*42, true)", p, ok)
	}
	*p = 100
	cur.Unlock()
	if v, _ := s.Load(7); v != 100 {
		t.Errorf("after Unlock, Load = %d; want 100", v)
	}
}

func TestStoreLockMissing(t *testing.T) {
	var s Store[int]
	p, cur, ok := s.Lock(7)
	if ok || p != nil {
		t.Errorf("Lock on missing slot = (%v, _, %v); want (nil, _, false)", p, ok)
	}
	cur.Unlock() // zero cursor no-op
}

func TestStoreLockSpinsWhilePinned(t *testing.T) {
	var s Store[int]
	s.Store(0, 1)
	_, cur0, ok := s.Lock(0)
	if !ok {
		t.Fatalf("preparation Lock failed")
	}

	// second Lock from another goroutine must wait
	done := make(chan struct{})
	go func() {
		_, cur1, ok := s.Lock(0)
		if !ok {
			t.Errorf("second Lock failed")
		}
		cur1.Unlock()
		close(done)
	}()

	// give the spinner a chance to spin
	for range 100 {
		runtime.Gosched()
	}
	select {
	case <-done:
		t.Errorf("second Lock returned while first still held the pin")
	default:
	}

	cur0.Unlock()
	<-done
}

func TestStoreUnlockNotLockedPanics(t *testing.T) {
	// The panic fires when Unlock is called on a slot whose lockused
	// word is fully zero (neither used nor locked) — i.e. somebody
	// already released both bits while we held the (now stale) cursor.
	// We reproduce it with Lock → Unlock → Delete → Unlock: after the
	// Delete the used bit is cleared, so the second Unlock sees a
	// zero lockused and panics.
	var s Store[int]
	s.Store(7, 42)
	_, cur, _ := s.Lock(7)
	cur.Unlock()
	s.Delete(7)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Unlock on fully-zeroed entry should panic")
		}
	}()
	cur.Unlock() // expected panic
}

// ---------- MutexStore[V] — Lock / Unlock ----------

func TestMutexStoreLockBasic(t *testing.T) {
	var s MutexStore[int]
	s.Store(7, 42)
	p, cur, ok := s.Lock(7)
	if !ok || *p != 42 {
		t.Fatalf("Lock = (%v, _, %v); want (*42, true)", p, ok)
	}
	*p = 100
	cur.Unlock()
	if v, _ := s.Load(7); v != 100 {
		t.Errorf("Load after Unlock = %d; want 100", v)
	}
}

func TestMutexStoreLockMissing(t *testing.T) {
	var s MutexStore[int]
	p, cur, ok := s.Lock(7)
	if ok || p != nil {
		t.Errorf("Lock on missing slot = (%v, _, %v); want (nil, _, false)", p, ok)
	}
	cur.Unlock() // zero cursor no-op
}

// ---------- Store[V] ----------

func TestStoreGrow(t *testing.T) {
	// Grow on zero-value Store: allocates the table at the target
	// size without allocating any bucket. Confirm via Len() == 0.
	var s Store[int]
	s.Grow(10000) // covers indexes 0..10000 = 313 buckets → 512 (next pow2)
	if l := s.Len(); l != 0 {
		t.Errorf("Grow should not populate any entry, Len = %d", l)
	}
	// Store a few entries; they must work and Len reflect them.
	for i := int64(0); i < 50; i++ {
		s.Store(i, int(i)*2)
	}
	if l := s.Len(); l != 50 {
		t.Errorf("after 50 Store, Len = %d; want 50", l)
	}
	for i := int64(0); i < 50; i++ {
		if v, ok := s.Load(i); !ok || v != int(i)*2 {
			t.Errorf("Load(%d) = (%d, %v); want (%d, true)", i, v, ok, int(i)*2)
		}
	}

	// Grow on an already-large-enough table is a no-op.
	s.Grow(100) // smaller than current — should not shrink
	if l := s.Len(); l != 50 {
		t.Errorf("Grow with smaller maxIndex should be no-op, Len = %d", l)
	}

	// Grow below s.start is also a no-op.
	s2 := NewStore[int](100)
	s2.Grow(50) // 50 < start=100
	if l := s2.Len(); l != 0 {
		t.Errorf("Grow below start should be no-op, Len = %d", l)
	}
}

func TestMutexStoreGrow(t *testing.T) {
	// Same shape as TestStoreGrow but exercises MutexStore (64-slot
	// buckets, min table size 16).
	var s MutexStore[int]
	s.Grow(10000) // 10000/64 ≈ 156 buckets → 256 (next pow2)
	if l := s.Len(); l != 0 {
		t.Errorf("Grow should not populate any entry, Len = %d", l)
	}
	for i := int64(0); i < 50; i++ {
		s.Store(i, int(i)*3)
	}
	if l := s.Len(); l != 50 {
		t.Errorf("after 50 Store, Len = %d; want 50", l)
	}
	for i := int64(0); i < 50; i++ {
		if v, ok := s.Load(i); !ok || v != int(i)*3 {
			t.Errorf("Load(%d) = (%d, %v); want (%d, true)", i, v, ok, int(i)*3)
		}
	}
	s.Grow(100) // no-op, already larger
	if l := s.Len(); l != 50 {
		t.Errorf("Grow no-op should not change Len, got %d", l)
	}
	s2 := NewMutexStore[int](100)
	s2.Grow(50) // below start
	if l := s2.Len(); l != 0 {
		t.Errorf("Grow below start should be no-op, Len = %d", l)
	}
}

func TestStoreLoadOrStore(t *testing.T) {
	var s Store[int]

	v, loaded := s.LoadOrStore(7, 42)
	if loaded {
		t.Errorf("LoadOrStore on empty slot should report loaded=false")
	}
	if v != 42 {
		t.Errorf("LoadOrStore should return 42, got %d", v)
	}

	v, loaded = s.LoadOrStore(7, 999)
	if !loaded {
		t.Errorf("LoadOrStore on existing slot should report loaded=true")
	}
	if v != 42 {
		t.Errorf("LoadOrStore should return existing 42, got %d", v)
	}

	// concurrency: exactly one loaded=false
	var s2 Store[int]
	var inserts atomic.Int64
	var w sync.WaitGroup
	for range 32 {
		w.Add(1)
		go func() {
			defer w.Done()
			_, l := s2.LoadOrStore(99, 1)
			if !l {
				inserts.Add(1)
			}
		}()
	}
	w.Wait()
	if inserts.Load() != 1 {
		t.Errorf("exactly one concurrent LoadOrStore should insert, got %d", inserts.Load())
	}
}

func TestStoreRange(t *testing.T) {
	var s Store[int]
	want := map[int64]int{1: 10, 2: 20, 5: 50, 100: -100}
	for k, v := range want {
		s.Store(k, v)
	}
	got := make(map[int64]int)
	s.Range(func(i int64, v int) bool {
		got[i] = v
		return true
	})
	if len(got) != len(want) {
		t.Errorf("Range visited %d entries; want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Range key %d: got %d, want %d", k, got[k], v)
		}
	}

	// early-stop
	count := 0
	s.Range(func(_ int64, _ int) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Errorf("Range should have stopped at 2 visits, got %d", count)
	}
}

func TestStoreClear(t *testing.T) {
	var s Store[int]
	for i := int64(0); i < 100; i++ {
		s.Store(i, int(i))
	}
	if s.Len() != 100 {
		t.Fatalf("pre-Clear Len = %d; want 100", s.Len())
	}
	s.Clear()
	if s.Len() != 0 {
		t.Errorf("post-Clear Len = %d; want 0", s.Len())
	}
	if _, ok := s.Load(0); ok {
		t.Errorf("post-Clear Load(0) should return ok=false")
	}
	// Store still usable after Clear
	s.Store(7, 70)
	if v, ok := s.Load(7); !ok || v != 70 {
		t.Errorf("post-Clear Store/Load should work, got (%d, %v)", v, ok)
	}
}

func TestStoreLoadAndDelete(t *testing.T) {
	var s Store[int]
	s.Store(1, 11)

	v, loaded := s.LoadAndDelete(1)
	if !loaded || v != 11 {
		t.Errorf("LoadAndDelete(1) = (%d, %v); want (11, true)", v, loaded)
	}
	if _, ok := s.Load(1); ok {
		t.Errorf("entry should be gone after LoadAndDelete")
	}

	v, loaded = s.LoadAndDelete(99)
	if loaded || v != 0 {
		t.Errorf("LoadAndDelete on missing key = (%d, %v); want (0, false)", v, loaded)
	}
}

func TestStoreSwap(t *testing.T) {
	var s Store[int]

	prev, loaded := s.Swap(1, 100)
	if loaded || prev != 0 {
		t.Errorf("first Swap = (%d, %v); want (0, false)", prev, loaded)
	}
	prev, loaded = s.Swap(1, 200)
	if !loaded || prev != 100 {
		t.Errorf("second Swap = (%d, %v); want (100, true)", prev, loaded)
	}
	if v, _ := s.Load(1); v != 200 {
		t.Errorf("after Swap, Load = %d; want 200", v)
	}
}

func TestStoreCompareAndSwap(t *testing.T) {
	var s Store[int]
	s.Store(1, 10)

	if !s.CompareAndSwap(1, 10, 20) {
		t.Errorf("CompareAndSwap(1, 10, 20) should succeed")
	}
	if v, _ := s.Load(1); v != 20 {
		t.Errorf("after CAS, Load = %d; want 20", v)
	}
	if s.CompareAndSwap(1, 10, 30) {
		t.Errorf("CompareAndSwap with wrong old should fail")
	}
	if s.CompareAndSwap(99, 0, 1) {
		t.Errorf("CompareAndSwap on missing key should fail")
	}
}

func TestStoreCompareAndDelete(t *testing.T) {
	var s Store[int]
	s.Store(1, 10)

	if s.CompareAndDelete(1, 99) {
		t.Errorf("CompareAndDelete with wrong old should fail")
	}
	if _, ok := s.Load(1); !ok {
		t.Errorf("after failed CAD, entry should remain")
	}
	if !s.CompareAndDelete(1, 10) {
		t.Errorf("CompareAndDelete(1, 10) should succeed")
	}
	if _, ok := s.Load(1); ok {
		t.Errorf("entry should be gone after successful CAD")
	}
}

// ---------- MutexStore[V] ----------

func TestMutexStoreLoadOrStore(t *testing.T) {
	var s MutexStore[int]
	v, loaded := s.LoadOrStore(7, 42)
	if loaded || v != 42 {
		t.Errorf("LoadOrStore on empty = (%d, %v); want (42, false)", v, loaded)
	}
	v, loaded = s.LoadOrStore(7, 999)
	if !loaded || v != 42 {
		t.Errorf("LoadOrStore on existing = (%d, %v); want (42, true)", v, loaded)
	}
}

func TestMutexStoreRangeClear(t *testing.T) {
	var s MutexStore[int]
	for i := int64(0); i < 5; i++ {
		s.Store(i, int(i)*10)
	}
	sum := 0
	s.Range(func(_ int64, v int) bool {
		sum += v
		return true
	})
	if sum != 0+10+20+30+40 {
		t.Errorf("Range sum = %d; want 100", sum)
	}
	s.Clear()
	if s.Len() != 0 {
		t.Errorf("post-Clear Len = %d; want 0", s.Len())
	}
}

func TestMutexStoreLoadAndDeleteSwapCASCAD(t *testing.T) {
	var s MutexStore[int]
	s.Store(1, 11)

	if v, loaded := s.LoadAndDelete(1); !loaded || v != 11 {
		t.Errorf("LoadAndDelete = (%d, %v); want (11, true)", v, loaded)
	}
	if prev, loaded := s.Swap(2, 22); loaded || prev != 0 {
		t.Errorf("first Swap = (%d, %v); want (0, false)", prev, loaded)
	}
	if prev, loaded := s.Swap(2, 33); !loaded || prev != 22 {
		t.Errorf("second Swap = (%d, %v); want (22, true)", prev, loaded)
	}
	if !s.CompareAndSwap(2, 33, 44) {
		t.Errorf("CAS should succeed")
	}
	if s.CompareAndSwap(2, 33, 55) {
		t.Errorf("CAS with stale old should fail")
	}
	if !s.CompareAndDelete(2, 44) {
		t.Errorf("CAD should succeed")
	}
	if _, ok := s.Load(2); ok {
		t.Errorf("entry should be gone after CAD")
	}
}

// ---------- Map.hash multi-type ----------
//
// The hash() type-switch has explicit cases for int / int64 / uint /
// uint64 / uintptr; everything else falls through to maphash.Comparable.
// Cover all of them.

type hashStruct struct {
	a int
	b string
}

func TestMapHashTypes(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		m := NewMap[int, int]().Grow(16)
		m.Store(7, 70)
		if v, _ := m.Load(7); v != 70 {
			t.Errorf("int: got %d", v)
		}
	})
	t.Run("int64", func(t *testing.T) {
		m := NewMap[int64, int]().Grow(16)
		m.Store(int64(7), 70)
		if v, _ := m.Load(7); v != 70 {
			t.Errorf("int64: got %d", v)
		}
	})
	t.Run("uint", func(t *testing.T) {
		m := NewMap[uint, int]().Grow(16)
		m.Store(uint(7), 70)
		if v, _ := m.Load(7); v != 70 {
			t.Errorf("uint: got %d", v)
		}
	})
	t.Run("uint64", func(t *testing.T) {
		m := NewMap[uint64, int]().Grow(16)
		m.Store(uint64(7), 70)
		if v, _ := m.Load(7); v != 70 {
			t.Errorf("uint64: got %d", v)
		}
	})
	t.Run("uintptr", func(t *testing.T) {
		m := NewMap[uintptr, int]().Grow(16)
		m.Store(uintptr(7), 70)
		if v, _ := m.Load(7); v != 70 {
			t.Errorf("uintptr: got %d", v)
		}
	})
	t.Run("string", func(t *testing.T) {
		m := NewMap[string, int]().Grow(16)
		m.Store("alpha", 1)
		m.Store("beta", 2)
		if v, _ := m.Load("alpha"); v != 1 {
			t.Errorf("string alpha: got %d", v)
		}
		if v, _ := m.Load("beta"); v != 2 {
			t.Errorf("string beta: got %d", v)
		}
		if _, ok := m.Load("gamma"); ok {
			t.Errorf("string gamma should be absent")
		}
	})
	t.Run("struct", func(t *testing.T) {
		m := NewMap[hashStruct, int]().Grow(16)
		m.Store(hashStruct{1, "x"}, 100)
		m.Store(hashStruct{2, "y"}, 200)
		if v, _ := m.Load(hashStruct{1, "x"}); v != 100 {
			t.Errorf("struct {1,x}: got %d", v)
		}
		if v, _ := m.Load(hashStruct{2, "y"}); v != 200 {
			t.Errorf("struct {2,y}: got %d", v)
		}
	})
}

// ---------- Map[K, V] — overflow chain and Range during rebuild ----------

// Build a map small enough that 50 keys overflow into chained buckets.
// Exercises the next-bucket walk in LockOrStore / Lock / Delete /
// LoadAndDelete / Range.
func TestMapOverflowChain(t *testing.T) {
	m := NewMap[int, int]().Grow(1) // smallest table: firstSize buckets
	const N = 200
	for i := range N {
		m.Store(i, i*3)
	}
	if l := m.Len(); l != N {
		t.Errorf("Len = %d; want %d", l, N)
	}
	for i := range N {
		if v, ok := m.Load(i); !ok || v != i*3 {
			t.Errorf("Load(%d) = (%d, %v); want (%d, true)", i, v, ok, i*3)
		}
	}
	// Range visits all entries
	seen := make(map[int]int)
	m.Range(func(k, v int) bool {
		seen[k] = v
		return true
	})
	if len(seen) != N {
		t.Errorf("Range saw %d entries; want %d", len(seen), N)
	}
}

// TestMapStressConcurrent fires every "mutator" op kind in parallel
// (Store, Lock, LockOrStore, Delete, LoadAndDelete, CompareAndSwap,
// Swap, Grow) so the bucket states bucketOpen / bucketFrozen /
// bucketMoved are all exercised by retry paths in Lock / LockOrStore /
// Delete / deleteIf. Range is intentionally excluded — its weakly-
// consistent value read trips the race detector on a concurrent
// Store, which is by design (see Range's godoc and the README's
// Design history section).
func TestMapStressConcurrent(t *testing.T) {
	var m Map[int, int]
	const goroutines = 16
	const opsPerG = 500
	const keyspace = 256

	var w sync.WaitGroup
	for g := range goroutines {
		w.Add(1)
		go func() {
			defer w.Done()
			for i := range opsPerG {
				k := (g*opsPerG + i) % keyspace
				switch i % 8 {
				case 0:
					m.Store(k, i)
				case 1:
					p, cur, ok := m.Lock(k)
					if ok {
						*p++
						cur.Unlock()
					}
				case 2:
					p, cur, _ := m.LockOrStore(k, i)
					*p = i + 1
					cur.Unlock()
				case 3:
					m.Delete(k)
				case 4:
					m.LoadAndDelete(k)
				case 5:
					m.CompareAndSwap(k, i-1, i)
				case 6:
					m.Swap(k, i)
				case 7:
					if i%64 == 0 {
						m.Grow(keyspace * 4)
					}
				}
			}
		}()
	}
	w.Wait()
}

// Drive Grow concurrently with Range so Range hits the bucketMoved /
// nextTable follow-up path that's otherwise hard to trigger.
func TestMapRangeDuringGrow(t *testing.T) {
	m := NewMap[int, int]().Grow(64)
	const N = 1000
	for i := range N {
		m.Store(i, -i)
	}

	var w sync.WaitGroup
	w.Add(1)
	go func() {
		defer w.Done()
		// trigger several doublings while Range scans
		m.Grow(N * 8)
	}()

	got := 0
	m.Range(func(_, _ int) bool {
		got++
		return true
	})
	w.Wait()
	if got < N/2 {
		t.Errorf("Range during Grow visited only %d entries; want at least %d", got, N/2)
	}
	// post-Grow consistency
	if l := m.Len(); l != N {
		t.Errorf("post-Grow Len = %d; want %d", l, N)
	}
}

// ---------- Map[K, V] ----------

func TestMapLoadOrStore(t *testing.T) {
	m := NewMap[int, int]().Grow(16)

	v, loaded := m.LoadOrStore(1, 10)
	if loaded || v != 10 {
		t.Errorf("LoadOrStore on empty = (%d, %v); want (10, false)", v, loaded)
	}
	v, loaded = m.LoadOrStore(1, 999)
	if !loaded || v != 10 {
		t.Errorf("LoadOrStore on existing = (%d, %v); want (10, true)", v, loaded)
	}

	// concurrency
	var m2 Map[int, int]
	var inserts atomic.Int64
	var w sync.WaitGroup
	for range 32 {
		w.Add(1)
		go func() {
			defer w.Done()
			_, l := m2.LoadOrStore(7, 1)
			if !l {
				inserts.Add(1)
			}
		}()
	}
	w.Wait()
	if inserts.Load() != 1 {
		t.Errorf("exactly one concurrent LoadOrStore should insert, got %d", inserts.Load())
	}
}

func TestMapRange(t *testing.T) {
	m := NewMap[int, int]().Grow(16)
	want := map[int]int{1: 10, 2: 20, 5: 50, 100: -100}
	for k, v := range want {
		m.Store(k, v)
	}
	got := make(map[int]int)
	m.Range(func(k, v int) bool {
		got[k] = v
		return true
	})
	if len(got) != len(want) {
		t.Errorf("Range visited %d entries; want %d", len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Range key %d: got %d, want %d", k, got[k], v)
		}
	}

	count := 0
	m.Range(func(_, _ int) bool {
		count++
		return count < 2
	})
	if count != 2 {
		t.Errorf("Range should have stopped at 2 visits, got %d", count)
	}
}

func TestMapClear(t *testing.T) {
	m := NewMap[int, int]().Grow(16)
	for i := 0; i < 100; i++ {
		m.Store(i, i)
	}
	if m.Len() != 100 {
		t.Fatalf("pre-Clear Len = %d; want 100", m.Len())
	}
	m.Clear()
	if m.Len() != 0 {
		t.Errorf("post-Clear Len = %d; want 0", m.Len())
	}
	if _, ok := m.Load(0); ok {
		t.Errorf("post-Clear Load(0) should be ok=false")
	}
	m.Store(7, 70)
	if v, ok := m.Load(7); !ok || v != 70 {
		t.Errorf("post-Clear Store/Load should work, got (%d, %v)", v, ok)
	}
}

func TestMapLoadAndDeleteSwapCASCAD(t *testing.T) {
	m := NewMap[int, int]().Grow(16)

	m.Store(1, 11)
	if v, loaded := m.LoadAndDelete(1); !loaded || v != 11 {
		t.Errorf("LoadAndDelete = (%d, %v); want (11, true)", v, loaded)
	}
	if v, loaded := m.LoadAndDelete(99); loaded || v != 0 {
		t.Errorf("LoadAndDelete missing = (%d, %v); want (0, false)", v, loaded)
	}

	if prev, loaded := m.Swap(2, 22); loaded || prev != 0 {
		t.Errorf("first Swap = (%d, %v); want (0, false)", prev, loaded)
	}
	if prev, loaded := m.Swap(2, 33); !loaded || prev != 22 {
		t.Errorf("second Swap = (%d, %v); want (22, true)", prev, loaded)
	}

	if !m.CompareAndSwap(2, 33, 44) {
		t.Errorf("CAS should succeed")
	}
	if m.CompareAndSwap(2, 33, 55) {
		t.Errorf("CAS with stale old should fail")
	}
	if m.CompareAndSwap(99, 0, 1) {
		t.Errorf("CAS on missing key should fail")
	}

	if m.CompareAndDelete(2, 99) {
		t.Errorf("CAD with wrong old should fail")
	}
	if !m.CompareAndDelete(2, 44) {
		t.Errorf("CAD should succeed")
	}
	if _, ok := m.Load(2); ok {
		t.Errorf("entry should be gone after CAD")
	}
}
