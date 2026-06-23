package fsync

import (
	"hash/maphash"
	"runtime"
	"sync"
	"sync/atomic"
)

// Set is a generic concurrent set of comparable keys. Each bucket
// packs 8 inline `K` slots and a single 64-bit `meta` word (8 h7
// tags); Contains is one atomic Load on `meta`, one tag scan, and
// one key compare — no seqlock, no values array, no pin pattern.
// Per-bit footprint is the same as `Map[K, struct{}]` (Go's
// `struct{}` is zero-sized) but Contains is faster because Set does
// not carry the seqlock the generic Map needs to support Lock(*V).
//
// The zero value is usable: the first Add lazy-allocates a small
// table. NewSet is provided for symmetry with NewMap and to skip
// that lazy-init branch on the very first call.
type Set[K comparable] struct {
	seed  uint64
	live  atomic.Int64
	table atomic.Pointer[tableSet[K]]
}

// bucketSet is a Set bucket: 8 inline `K` slots, a `meta` word
// packing the 8 h7 tags, an open/frozen/moved state used by the
// lazy rebuild, a writer mutex, and an overflow pointer. No `pins`
// (Set has no Lock(*V) API) and no `values` (the key alone IS the
// entry).
type bucketSet[K comparable] struct {
	meta  atomic.Uint64 // 8 h7 tags, occupiedBit | top 7 hash bits per slot
	state atomic.Uint32 // bucketOpen / bucketFrozen / bucketMoved
	mu    sync.Mutex
	keys  [slotCount]K
	next  atomic.Pointer[bucketSet[K]]
}

type tableSet[K comparable] struct {
	buckets     []atomic.Pointer[bucketSet[K]]
	mask        uint64
	rebuildLeft atomic.Int64
	rebuildIdx  atomic.Uint64
	nextTable   atomic.Pointer[tableSet[K]]
}

// NewSet returns an empty Set[K]. The zero-value Set is equally
// usable (`var s Set[K]`); NewSet is provided for symmetry with
// NewMap and pre-allocates the initial bucket table.
//
// To pre-size for a known steady-state count, chain Grow:
//
//	s := fsync.NewSet[string]().Grow(100_000)
func NewSet[K comparable]() *Set[K] {
	s := &Set[K]{}
	s.table.Store(newTableSet[K](firstSize))
	return s
}

// newTableSet creates a Set bucket table of size n (must be a
// power of two) with every slot pre-allocated to an empty bucket.
func newTableSet[K comparable](n int) *tableSet[K] {
	t := &tableSet[K]{
		buckets: make([]atomic.Pointer[bucketSet[K]], n),
		mask:    uint64(n - 1),
	}
	for i := range t.buckets {
		t.buckets[i].Store(&bucketSet[K]{})
	}
	return t
}

// loadOrInitTable returns the current table, allocating the first
// one lazily so a zero-value Set is usable.
func (s *Set[K]) loadOrInitTable() *tableSet[K] {
	if t := s.table.Load(); t != nil {
		return t
	}
	nt := newTableSet[K](firstSize)
	if s.table.CompareAndSwap(nil, nt) {
		return nt
	}
	return s.table.Load()
}

// hash mirrors Map.hash: wyhash on integer keys (fast), maphash on
// strings (also fast, anti-flooding), maphash.Comparable on
// everything else.
func (s *Set[K]) hash(key K) uint64 {
	switch v := any(key).(type) {
	case int:
		return hashUint64(s.seed, uint64(v))
	case int64:
		return hashUint64(s.seed, uint64(v))
	case uint64:
		return hashUint64(s.seed, v)
	case uint:
		return hashUint64(s.seed, uint64(v))
	case uintptr:
		return hashUint64(s.seed, uint64(v))
	case string:
		return maphash.String(mapSeed, v)
	default:
		return maphash.Comparable(mapSeed, key)
	}
}

// Grow expands the bucket table so the set can hold at least
// estimatedItems without triggering a rebuild. Returns the
// receiver, so calls can be chained.
func (s *Set[K]) Grow(estimatedItems int) *Set[K] {
	target := bucketsFor(estimatedItems)
	for {
		t := s.loadOrInitTable()
		if len(t.buckets) >= target {
			return s
		}
		if t.nextTable.Load() != nil {
			n := uint64(len(t.buckets))
			for idx := uint64(0); idx < n; idx++ {
				s.helpMigrateBucket(t, idx)
			}
			continue
		}
		s.maybeStartRebuild(t)
		n := uint64(len(t.buckets))
		for idx := uint64(0); idx < n; idx++ {
			s.helpMigrateBucket(t, idx)
		}
	}
}

// Contains reports whether key is in the set. Lock-free: one atomic
// `meta.Load()` + tag scan + key compare per bucket in the overflow
// chain. No seqlock, no value read.
func (s *Set[K]) Contains(key K) bool {
	t := s.table.Load()
	if t == nil {
		return false
	}
	h := s.hash(key)
	tag := uint64(slotTag(h))

	for {
		b := t.buckets[h&t.mask].Load()
		if b.state.Load() == bucketMoved {
			nt := t.nextTable.Load()
			if nt == nil || len(nt.buckets) == 0 {
				t = s.table.Load()
				continue
			}
			if t.rebuildLeft.Load() <= 0 {
				s.table.CompareAndSwap(t, nt)
			}
			t = nt
			continue
		}
		for cur := b; cur != nil; cur = cur.next.Load() {
			meta := cur.meta.Load()
			for j := 0; j < slotCount; j++ {
				if uint8(meta>>(8*j)) == byte(tag) && cur.keys[j] == key {
					return true
				}
			}
		}
		return false
	}
}

// Add inserts key into the set. Returns true if the key was newly
// inserted, false if it was already present.
func (s *Set[K]) Add(key K) (added bool) {
	h := s.hash(key)
	tag := uint64(slotTag(h))

	t := s.loadOrInitTable()
Retry:
	b := t.buckets[h&t.mask].Load()

	switch b.state.Load() {
	case bucketMoved:
		nt := t.nextTable.Load()
		if nt == nil || len(nt.buckets) == 0 {
			t = s.table.Load()
			goto Retry
		}
		if t.rebuildLeft.Load() <= 0 {
			s.table.CompareAndSwap(t, nt)
		}
		t = nt
		goto Retry
	case bucketFrozen:
		s.helpMigrateBucket(t, h&t.mask)
		goto Retry
	}

	b.mu.Lock()
	if b.state.Load() != bucketOpen {
		b.mu.Unlock()
		goto Retry
	}

	// update path: key already present?
	for cur := b; cur != nil; cur = cur.next.Load() {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			if uint8(meta>>(8*j)) == byte(tag) && cur.keys[j] == key {
				b.mu.Unlock()
				return false
			}
		}
	}

	// insert path
	cur := b
	for {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			if uint8(meta>>(8*j))&occupiedBit == 0 {
				cur.keys[j] = key
				cur.meta.Store(meta&^(uint64(0xff)<<(8*j)) | (tag << (8 * j)))
				b.mu.Unlock()
				s.afterInsert(t)
				return true
			}
		}
		next := cur.next.Load()
		if next == nil {
			nb := &bucketSet[K]{}
			nb.keys[0] = key
			nb.meta.Store(tag)
			cur.next.Store(nb)
			b.mu.Unlock()
			s.afterInsert(t)
			return true
		}
		cur = next
	}
}

// afterInsert increments live, triggers rebuild if needed, and
// helps an in-flight rebuild make progress.
func (s *Set[K]) afterInsert(t *tableSet[K]) {
	if s.live.Add(1) > int64(loadFactor)*int64(slotCount)*int64(len(t.buckets))/4 {
		s.maybeStartRebuild(t)
	}
	s.helpRebuildProgress(t, sweepBatch)
}

// Remove deletes key from the set. Returns true if the key was
// present (and has been removed), false otherwise.
func (s *Set[K]) Remove(key K) bool {
	h := s.hash(key)
	tag := uint64(slotTag(h))

	t := s.loadOrInitTable()
Retry:
	b := t.buckets[h&t.mask].Load()
	switch b.state.Load() {
	case bucketMoved:
		nt := t.nextTable.Load()
		if nt == nil || len(nt.buckets) == 0 {
			t = s.table.Load()
			goto Retry
		}
		if t.rebuildLeft.Load() <= 0 {
			s.table.CompareAndSwap(t, nt)
		}
		t = nt
		goto Retry
	case bucketFrozen:
		s.helpMigrateBucket(t, h&t.mask)
		goto Retry
	}

	b.mu.Lock()
	if b.state.Load() != bucketOpen {
		b.mu.Unlock()
		goto Retry
	}

	for cur := b; cur != nil; cur = cur.next.Load() {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			if uint8(meta>>(8*j)) == byte(tag) && cur.keys[j] == key {
				var zeroK K
				cur.keys[j] = zeroK
				cur.meta.Store(meta &^ (uint64(0xff) << (8 * j)))
				b.mu.Unlock()
				s.live.Add(-1)
				return true
			}
		}
	}
	b.mu.Unlock()
	return false
}

// Len returns the live element count. Same caveat as Map.Len: not
// a linearization point under concurrency.
func (s *Set[K]) Len() int {
	return int(s.live.Load())
}

// Range calls f sequentially for each key in the set. Iteration is
// weakly consistent (sync.Map semantics): keys present throughout
// the iteration are visited; keys added or removed during the scan
// may or may not be observed. Iteration stops if f returns false.
// A rebuild in progress is followed transparently.
func (s *Set[K]) Range(f func(key K) bool) {
	t := s.table.Load()
	if t == nil {
		return
	}
	for i := range t.buckets {
		b := t.buckets[i].Load()
		if b.state.Load() == bucketMoved {
			nt := t.nextTable.Load()
			if nt != nil && len(nt.buckets) > 0 {
				newMask := nt.mask
				oldMask := t.mask
				for nidx := uint64(0); nidx <= newMask; nidx++ {
					if nidx&oldMask != uint64(i) {
						continue
					}
					nb := nt.buckets[nidx].Load()
					if !rangeBucketChainSet(nb, f) {
						return
					}
				}
				continue
			}
		}
		if !rangeBucketChainSet(b, f) {
			return
		}
	}
}

// rangeBucketChainSet walks one Set bucket's overflow chain, calling
// f on every occupied slot. Returns false if f asked to stop.
func rangeBucketChainSet[K comparable](b *bucketSet[K], f func(key K) bool) bool {
	for cur := b; cur != nil; cur = cur.next.Load() {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			if uint8(meta>>(8*j))&occupiedBit == 0 {
				continue
			}
			if !f(cur.keys[j]) {
				return false
			}
		}
	}
	return true
}

// Clear drops every key. The freshly initialized table replaces
// the old one — concurrent Range may still see entries from the
// old table briefly.
func (s *Set[K]) Clear() {
	s.table.Store(newTableSet[K](firstSize))
	s.live.Store(0)
}

// ---------- Rebuild plumbing (split-only; no pin to preserve) ----------

func (s *Set[K]) maybeStartRebuild(t *tableSet[K]) {
	if s.table.Load() != t {
		return
	}
	if t.nextTable.Load() != nil {
		return
	}
	if !t.nextTable.CompareAndSwap(nil, &tableSet[K]{}) {
		return
	}
	nt := &tableSet[K]{
		buckets: make([]atomic.Pointer[bucketSet[K]], 2*len(t.buckets)),
		mask:    2*uint64(len(t.buckets)) - 1,
	}
	t.rebuildLeft.Store(int64(len(t.buckets)))
	t.rebuildIdx.Store(0)
	t.nextTable.Store(nt)
}

func (s *Set[K]) helpRebuildProgress(t *tableSet[K], n int) {
	nt := t.nextTable.Load()
	if nt == nil || len(nt.buckets) == 0 {
		return
	}
	heads := uint64(len(t.buckets))
	for i := 0; i < n; i++ {
		idx := t.rebuildIdx.Add(1) - 1
		if idx >= heads {
			return
		}
		s.helpMigrateBucket(t, idx)
	}
}

func (s *Set[K]) helpMigrateBucket(t *tableSet[K], idx uint64) {
	nt := t.nextTable.Load()
	if nt == nil || len(nt.buckets) == 0 {
		return
	}
	b := t.buckets[idx].Load()
	for {
		switch b.state.Load() {
		case bucketMoved:
			return
		case bucketFrozen:
			runtime.Gosched()
			continue
		}
		if !b.state.CompareAndSwap(bucketOpen, bucketFrozen) {
			continue
		}
		s.migrateBucket(t, nt, idx, b)
		b.state.Store(bucketMoved)
		if t.rebuildLeft.Add(-1) <= 0 {
			s.table.CompareAndSwap(t, nt)
		}
		return
	}
}

// migrateBucket always splits (Set has no pins to preserve, so we
// never need the duplicate-on-pin path Map uses).
func (s *Set[K]) migrateBucket(t, nt *tableSet[K], idx uint64, b *bucketSet[K]) {
	oldSize := uint64(len(t.buckets))

	// drain in-flight writers: any writer that already passed the
	// state==Open check must finish its in-bucket work (under b.mu)
	// before we read its tags below.
	b.mu.Lock()
	//lint:ignore SA2001 the Lock/Unlock pair is a barrier, not a real critical section
	b.mu.Unlock()

	b0 := &bucketSet[K]{}
	b1 := &bucketSet[K]{}
	for cur := b; cur != nil; cur = cur.next.Load() {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			tb := uint8(meta >> (8 * j))
			if tb&occupiedBit == 0 {
				continue
			}
			k := cur.keys[j]
			h := s.hash(k)
			tag := uint64(slotTag(h))
			if h&oldSize == 0 {
				insertIntoBucketSet(b0, k, tag)
			} else {
				insertIntoBucketSet(b1, k, tag)
			}
		}
	}
	nt.buckets[idx].Store(b0)
	nt.buckets[idx+oldSize].Store(b1)
}

// insertIntoBucketSet places key in the first free slot of bucket
// or its overflow chain, creating overflow as needed. Caller is
// the only writer (migrator owns the chain), so no locking needed.
func insertIntoBucketSet[K comparable](b *bucketSet[K], key K, tag uint64) {
	cur := b
	for {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			if uint8(meta>>(8*j))&occupiedBit == 0 {
				cur.keys[j] = key
				cur.meta.Store(meta&^(uint64(0xff)<<(8*j)) | (tag << (8 * j)))
				return
			}
		}
		next := cur.next.Load()
		if next == nil {
			nb := &bucketSet[K]{}
			nb.keys[0] = key
			nb.meta.Store(tag)
			cur.next.Store(nb)
			return
		}
		cur = next
	}
}
