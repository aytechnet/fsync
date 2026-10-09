package fsync

import (
	"testing"
	"time"
)

// Coverage tests for Set: key types with their own hash path, and the
// rebuild-in-flight branches of Contains / Remove / Range / Grow, driven
// deterministically by starting a rebuild and migrating chosen buckets
// from the test (white-box, same package).

func TestSetKeyTypes(t *testing.T) {
	var su Set[uint64]
	var sn Set[uint]
	var sp Set[uintptr]
	type pair struct{ a, b int }
	var ss Set[pair] // default branch: maphash.Comparable
	for i := 0; i < 300; i++ {
		su.Add(uint64(i))
		sn.Add(uint(i))
		sp.Add(uintptr(i))
		ss.Add(pair{i, -i})
	}
	for i := 0; i < 300; i++ {
		if !su.Contains(uint64(i)) || !sn.Contains(uint(i)) || !sp.Contains(uintptr(i)) || !ss.Contains(pair{i, -i}) {
			t.Fatalf(`key %d missing in one of the typed sets`, i)
		}
	}
	if ss.Contains(pair{1, 1}) || su.Len() != 300 || ss.Len() != 300 {
		t.Errorf(`unexpected content: Contains(pair{1,1})=%v Len=%d/%d`, ss.Contains(pair{1, 1}), su.Len(), ss.Len())
	}
}

func TestSetEmptyZeroValue(t *testing.T) {
	var s Set[int]
	calls := 0
	s.Range(func(int) bool { calls++; return true }) // nil table
	if calls != 0 || s.Contains(1) || s.Remove(1) {
		t.Errorf(`empty Set: Range calls=%d Contains=%v Remove=%v`, calls, s.Contains(1), s.Remove(1))
	}
	// helpMigrateBucket with no rebuild in flight is a no-op
	s.helpMigrateBucket(s.loadOrInitTable(), 0)
}

// filledSetWithRebuild returns a Set holding 0..n-1 whose table has a
// rebuild started (nextTable installed) but no bucket migrated yet.
func filledSetWithRebuild(t *testing.T, n int) (*Set[int], *tableSet[int]) {
	t.Helper()
	s := NewSet[int]().Grow(n) // presized: no automatic rebuild while filling
	for i := 0; i < n; i++ {
		s.Add(i)
	}
	tb := s.table.Load()
	if tb.nextTable.Load() != nil {
		t.Fatal(`unexpected rebuild already in flight`)
	}
	s.maybeStartRebuild(tb)
	if nt := tb.nextTable.Load(); nt == nil || len(nt.buckets) == 0 {
		t.Fatal(`rebuild did not start`)
	}
	return s, tb
}

func TestSetOperationsDuringRebuild(t *testing.T) {
	const n = 400
	s, tb := filledSetWithRebuild(t, n)
	half := uint64(len(tb.buckets)) / 2
	for idx := uint64(0); idx < half; idx++ { // migrate the first half only
		s.helpMigrateBucket(tb, idx)
	}
	if s.table.Load() != tb {
		t.Fatal(`table promoted before every bucket was migrated`)
	}
	// Contains and Remove follow moved buckets into nextTable.
	for i := 0; i < n; i++ {
		if !s.Contains(i) {
			t.Fatalf(`Contains(%d) false during rebuild`, i)
		}
	}
	// Range visits moved buckets through nextTable, each key once.
	seen := map[int]int{}
	s.Range(func(k int) bool { seen[k]++; return true })
	if len(seen) != n {
		t.Fatalf(`Range during rebuild visited %d keys, want %d`, len(seen), n)
	}
	for k, c := range seen {
		if c != 1 {
			t.Fatalf(`Range visited key %d %d times`, k, c)
		}
	}
	// Early stop inside the moved (nextTable) branch of Range.
	stops := 0
	s.Range(func(int) bool { stops++; return false })
	if stops != 1 {
		t.Errorf(`Range must stop after f returns false, got %d calls`, stops)
	}
	for i := 0; i < n; i += 2 {
		if !s.Remove(i) {
			t.Fatalf(`Remove(%d) false during rebuild`, i)
		}
	}
	// Grow with a rebuild in flight helps finish it before doubling again.
	s.Grow(8 * n)
	if tb2 := s.table.Load(); tb2 == tb || len(tb2.buckets) < bucketsFor(8*n) {
		t.Fatalf(`Grow did not finish the pending rebuild and grow: %d buckets`, len(s.table.Load().buckets))
	}
	for i := 0; i < n; i++ {
		if s.Contains(i) != (i%2 == 1) {
			t.Fatalf(`after Grow: Contains(%d) = %v`, i, s.Contains(i))
		}
	}
}

// A goroutine holding a stale table whose rebuild has completed (every
// bucket moved, rebuildLeft ≤ 0) promotes nextTable itself.
func TestSetStaleTablePromotesNext(t *testing.T) {
	s, tb := filledSetWithRebuild(t, 300)
	for idx := uint64(0); idx < uint64(len(tb.buckets)); idx++ {
		s.helpMigrateBucket(tb, idx)
	}
	nt := s.table.Load()
	if nt == tb {
		t.Fatal(`rebuild did not promote`)
	}
	s.table.Store(tb) // simulate a reader that still sees the old table
	if !s.Contains(7) || s.table.Load() != nt {
		t.Fatalf(`Contains on a stale table must find the key and promote nextTable`)
	}
	s.table.Store(tb)
	if !s.Remove(8) || s.table.Load() != nt {
		t.Fatalf(`Remove on a stale table must remove the key and promote nextTable`)
	}
	if s.Contains(8) {
		t.Errorf(`key 8 still present after Remove`)
	}
}

// Remove on a frozen bucket helps the migration (waiting for the
// migrator), then follows the bucket into nextTable.
func TestSetRemoveOnFrozenBucket(t *testing.T) {
	s, tb := filledSetWithRebuild(t, 300)
	key := 42
	idx := s.hash(key) & tb.mask
	b := tb.buckets[idx].Load()
	b.state.Store(bucketFrozen) // a migrator "owns" the bucket
	done := make(chan bool)
	go func() { done <- s.Remove(key) }()
	time.Sleep(10 * time.Millisecond)
	b.state.Store(bucketOpen) // the "migrator" backs off
	select {
	case ok := <-done:
		if !ok {
			t.Fatal(`Remove after the freeze must still find the key`)
		}
	case <-time.After(5 * time.Second):
		t.Fatal(`Remove stuck on a frozen bucket`)
	}
	if s.Contains(key) {
		t.Error(`key still present after Remove`)
	}
}
