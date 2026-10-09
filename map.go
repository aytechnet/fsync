// Package fsync provides concurrent data structures faster than sync.Map
// and puzpuzpuz xsync, built for the DyaPi iPaaS platform and the
// Aytechnet monitoring stack (agent statistics, dictionaries, caches).
//
// Map[K, V] is an xsync-style bucket-direct hash map with the unique
// feature of an integrated per-entry pin. Each bucket carries up to 8
// (K, V) slots inline plus a packed metadata word (8 h7 tags) and a
// pins/seq word. The hot path of Load is a tag scan against one cacheline,
// roughly on par with xsync.Map. The pin word makes it possible to expose
// a stable *V via Lock and LockOrStore, replacing the canonical Go pattern
// of `map[K]*Entry where Entry has its own mutex` — without the per-entry
// allocation and without the per-entry sync.Mutex.
//
// API at a glance:
//
//	m.Load(k)              (V, bool)             // lock-free read
//	m.Store(k, v)          bool                  // blocks on pinned key
//	m.Delete(k)            bool                  // blocks on pinned key
//	m.Lock(k)              (*V, Cursor, bool)    // pin existing entry
//	m.LockOrStore(k, v)    (*V, Cursor)          // pin existing OR insert+pin
//	cursor.Unlock()                              // release pin (method on Cursor)
//	m.Len()                int
//
// Concurrency contract — same as fsync.Store[V]:
//   - Load, Store and Delete ALL BLOCK on a key whose slot is currently
//     Locked. Once the Lock holder calls Unlock, the waiting operations
//     resume. The blocking is a Gosched spin (no kernel futex) outside
//     any bucket mutex, so other writers on the bucket are not stuck
//     behind it.
//   - Critical consequence: a caller of Lock MUST eventually call
//     Unlock. A Lock held forever starves every concurrent Load, Store
//     and Delete of the same key. Use `defer cur.Unlock()` right after
//     a successful Lock to make this hard to get wrong.
//   - The *V returned by Lock / LockOrStore is valid only until the
//     matching Unlock. After Unlock the pointer must not be used (a
//     concurrent rebuild or Delete may then move or invalidate the slot).
//
// Value-type considerations for Load:
//   - For word-sized V (int, uint, pointer, *T) the read is
//     hardware-atomic on amd64/arm64 and the seqlock retry covers the
//     small window between observing pins clear and re-reading them.
//   - For multi-word V (struct, string, slice header), every in-place
//     writer (Lock holder, Store update, Delete) pins the slot and bumps
//     the seq, and Load validates the whole match+read window against
//     it: a torn V is retried, never returned (since v0.5.0; before,
//     Store's update path wrote without the seq).
//   - For V containing INTERNAL STATE referenced through a pointer
//     (V = map[X]Y, V = *Sub, V = struct{slice []T}, …) the seqlock
//     protects ONLY the V header copied by Load — it does NOT protect
//     the inner object. A Lock holder mutating *(*p)[k] races with any
//     Load that subsequently dereferences the returned V's pointers.
//     Either restrict access to the inner state to Lock holders (which,
//     because Load blocks on Lock, is the natural and recommended
//     pattern), or wrap as Map[K, *Sub] where Sub is itself
//     concurrency-safe.
//
// Rebuild policy (split, never duplicate):
//   - When the live count crosses the load-factor threshold the bucket
//     table is doubled; each bucket is SPLIT into two fresh buckets
//     according to the new bit of the mask.
//   - A bucket with a pinned slot is SKIPPED, not migrated: moving it
//     would invalidate the *V handed out by Lock. It stays open in the
//     old table (reads and writes keep working on it) and the lazy sweep
//     comes back to it once its pins are released; the new table is
//     promoted only when every position has been migrated. A long-held
//     Lock therefore delays the end of a doubling, never correctness.
//   - Every write of a slot's key/value (Store update, Delete, migration)
//     runs under the same pin+seq protocol as Lock/Unlock, so Load never
//     returns a torn multi-word V nor a value being deleted, and Lock
//     re-validates the slot (key, occupancy, bucket state) after pinning.
//
// Zero-value Map: `var m Map[K, V]` is usable directly; the first write
// lazily allocates the bucket table. Use NewMap(minBuckets) to pre-size.
package fsync

import (
	"hash/maphash"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

var mapSeed = maphash.MakeSeed()

// hashUint64 is the wyhash-style mixer used by puzpuzpuz/xsync — a 128-bit
// multiply-xor with the xxHash PRIME64_1 constant. Much faster than
// maphash for integer keys.
func hashUint64(seed, v uint64) uint64 {
	hi, lo := bits.Mul64(v^seed, 0x9E3779B185EBCA87)
	return hi ^ lo
}

const (
	slotCount   = 8
	occupiedBit = 0x80
	loadFactor  = 3 // trigger rebuild when live > slotCount * len(buckets) * loadFactor / 4
	firstSize   = 64
	sweepBatch  = 2 // buckets to migrate per Store, lazy convergence

	// pin/seq layout of bucketMap.pins:
	//   bits  0-7 : 8 pin bits (one per slot)
	//   bits 8-63 : 56-bit monotonic counter (the "seq")
	// Lock and Unlock both INCREMENT the seq by pinsSeqUnit in addition to
	// toggling the pin bit. The seq lets Load detect that a Lock/Unlock
	// cycle happened on the bucket while it was reading values[j], and
	// retry. Without it, a multi-word V could be torn-read when a Lock
	// holder modifies it via *V concurrently with Load.
	//
	// We tried fusing the rebuild state into this word (bits 8-9, seq
	// pushed to bits 10-63). It regressed ReadOnly by 11 % and
	// LoadOrStore by 14 % — the inline [8]K [8]V cacheline layout
	// shifted, and the extra `& pinsStateMask` mask on the hot Load
	// path was net negative. Kept separate state field.
	pinsPinMask = uint64(0xff)
	pinsSeqUnit = uint64(1) << 8

	bucketOpen   uint32 = 0 // accepts inserts / updates
	bucketFrozen uint32 = 1 // claimed by a migrator
	bucketMoved  uint32 = 2 // already migrated; readers must follow nextTable
)

type bucketMap[K comparable, V any] struct {
	meta   atomic.Uint64 // 8 tag bytes (1 byte per slot); byte j == 0 => slot j empty
	pins   atomic.Uint64 // 8 pin bits (one per slot)
	state  atomic.Uint32 // bucketOpen / bucketFrozen / bucketMoved
	mu     sync.Mutex
	keys   [slotCount]K
	values [slotCount]V
	next   atomic.Pointer[bucketMap[K, V]] // overflow chain
}

type tableMap[K comparable, V any] struct {
	buckets     []atomic.Pointer[bucketMap[K, V]]
	mask        uint64
	rebuildLeft atomic.Int64                   // buckets still to migrate; promotion when ≤ 0
	rebuildIdx  atomic.Uint64                  // next bucket index to claim for the lazy sweep
	nextTable   atomic.Pointer[tableMap[K, V]] // != nil iff a rebuild is in progress
}

// Map is a generic concurrent hash map of K → V, lock-free on the
// read path. Slots live inline in fixed-size 8-slot buckets so Lock
// returns a stable *V into the bucket itself, with no per-entry
// heap allocation. The zero value is usable: the first write
// lazy-allocates a small table. See the package README for the full
// concurrency contract and design rationale.
type Map[K comparable, V any] struct {
	seed  uint64
	live  atomic.Int64
	table atomic.Pointer[tableMap[K, V]]
}

// Cursor is a handle on a pinned slot returned by Map.Lock /
// Map.LockOrStore. slotIdx is stored as a plain uint and always masked
// with `& 7` at the access sites; this lets the compiler prove the
// index is in [0, slotCount) and skip the slice bounds check on
// values[]/keys[]/pins-bit shifts.
type Cursor[K comparable, V any] struct {
	bucket  *bucketMap[K, V]
	slotIdx uint
}

// bucketsFor returns the smallest power-of-two bucket count whose total
// slot capacity (8 slots per bucket) lets the map hold `estimatedItems`
// without immediately triggering a rebuild (i.e., kept under the load
// factor threshold loadFactor/4 = 3/4 = 0.75).
func bucketsFor(estimatedItems int) int {
	if estimatedItems <= 0 {
		return firstSize
	}
	// items <= slotCount * buckets * loadFactor / 4
	// → buckets >= items * 4 / (slotCount * loadFactor)
	need := (estimatedItems*4 + slotCount*loadFactor - 1) / (slotCount * loadFactor)
	n := 1
	for n < need {
		n <<= 1
	}
	if n < firstSize {
		n = firstSize
	}
	return n
}

// NewMap allocates a map pre-sized to comfortably hold estimatedItems
// without an immediate rebuild. The zero-value Map is also usable: the
// first write lazily allocates a small initial bucket table; call Grow on
// it later if you discover the expected size.
// NewMap returns an empty Map[K, V]. The zero-value Map is equally
// usable (`var m Map[K, V]`); NewMap is provided for symmetry with
// NewQueue / NewSet and pre-allocates the initial bucket table so
// the first Load / Store / Lock skips the lazy-init branch.
//
// To pre-size for a known steady-state count, chain Grow:
//
//	m := fsync.NewMap[int, string]().Grow(100_000)
func NewMap[K comparable, V any]() *Map[K, V] {
	m := &Map[K, V]{}
	m.table.Store(newTable[K, V](firstSize))
	return m
}

// Grow expands the bucket table so the map can hold at least
// estimatedItems without triggering a rebuild. If the current capacity
// already meets the target, Grow is a no-op. Grow is safe to call
// concurrently with Load / Store / Lock / Delete.
//
// Use Grow on a zero-value Map once you know its expected steady-state
// size, to avoid a chain of small doublings during the warmup. Each
// doubling here is finished synchronously by the caller, so by the time
// Grow returns the new table is fully promoted and Load/Store hit it
// directly with no rebuild bookkeeping.
//
// Returns the receiver, so calls can be chained:
//
//	m := fsync.NewMap[int, string]().Grow(100_000)
func (m *Map[K, V]) Grow(estimatedItems int) *Map[K, V] {
	target := bucketsFor(estimatedItems)
	for {
		t := m.loadOrInitTable()
		if len(t.buckets) >= target {
			return m
		}
		if t.nextTable.Load() != nil {
			// a rebuild started by someone else is in flight — help
			// drive it to completion, then re-check.
			n := uint64(len(t.buckets))
			for idx := uint64(0); idx < n; idx++ {
				m.helpMigrateBucket(t, idx)
			}
			if t.rebuildLeft.Load() > 0 {
				runtime.Gosched() // a pinned bucket holds the doubling back
			}
			continue
		}
		m.maybeStartRebuild(t)
		// finish the doubling synchronously so Grow's contract holds.
		n := uint64(len(t.buckets))
		for idx := uint64(0); idx < n; idx++ {
			m.helpMigrateBucket(t, idx)
		}
	}
}

// newTable creates a bucket table of size n (must be a power of two) with
// every slot pre-allocated to an empty bucket.
func newTable[K comparable, V any](n int) *tableMap[K, V] {
	t := &tableMap[K, V]{
		buckets: make([]atomic.Pointer[bucketMap[K, V]], n),
		mask:    uint64(n - 1),
	}
	for i := range t.buckets {
		t.buckets[i].Store(&bucketMap[K, V]{})
	}
	return t
}

// loadOrInitTable returns the current table, allocating the first one
// lazily so a zero-value Map is usable.
func (m *Map[K, V]) loadOrInitTable() *tableMap[K, V] {
	if t := m.table.Load(); t != nil {
		return t
	}
	nt := newTable[K, V](firstSize)
	if m.table.CompareAndSwap(nil, nt) {
		return nt
	}
	return m.table.Load()
}

func (m *Map[K, V]) hash(key K) uint64 {
	switch v := any(key).(type) {
	case int:
		return hashUint64(m.seed, uint64(v))
	case int64:
		return hashUint64(m.seed, uint64(v))
	case uint64:
		return hashUint64(m.seed, v)
	case uint:
		return hashUint64(m.seed, uint64(v))
	case uintptr:
		return hashUint64(m.seed, uint64(v))
	case string:
		// maphash.String is specialized for string and skips the
		// reflection path that maphash.Comparable goes through for a
		// generic comparable T. Cuts about half the cost on string
		// keys in practice.
		return maphash.String(mapSeed, v)
	default:
		return maphash.Comparable(mapSeed, key)
	}
}

func slotTag(h uint64) byte {
	return occupiedBit | byte(h>>57)
}

// Load returns value and ok=true if key is present.
//
// Concurrency contract (matches fsync.Store[V]): Load BLOCKS on a key
// whose slot is currently held by Lock. The blocking is a Gosched spin
// outside any mutex, so other writers on the bucket are not stuck waiting
// behind it. The caller of Lock is therefore RESPONSIBLE for invoking
// Unlock so that Load (and Store, Delete) on the same key can make
// progress; a Lock held forever starves every concurrent Load of the
// same key.
//
// Once the lock window is clear, the read uses a seqlock retry to skip
// observations that straddled a Lock+Unlock cycle that started after we
// had observed the pin as clear. For word-sized V (int, uint, pointer,
// *T) the value read between pin-clear and seq-stable is hardware-atomic
// on amd64 / arm64. For multi-word V, the value is read between two
// known-stable pin observations so a torn read only happens if a Lock
// holder takes the pin AND writes AND releases inside a single Load
// — possible in theory but rare; same trade-off as sync.Map storing
// interface{}.
//
// V containing INTERNAL STATE referenced through a pointer (map, *Sub,
// slice header into a backing array, …): the seqlock protects only the
// V header copied out by Load. Mutating the inner object under Lock
// without serializing the readers races with any code that dereferences
// the returned V's pointers. Either restrict access to the inner state
// to Lock holders, or wrap as Map[K, *Sub] where Sub is itself
// concurrency-safe.
func (m *Map[K, V]) Load(key K) (value V, ok bool) {
	t := m.table.Load()
	if t == nil {
		return // zero-value Map → never any entry
	}
	h := m.hash(key)
	tag := uint64(slotTag(h))

	for {
		b := t.buckets[h&t.mask].Load()
		if b.state.Load() == bucketMoved {
			// chain migrated → local-switch to nextTable; promote globally
			// only when every chain has finished migrating.
			nt := t.nextTable.Load()
			if nt == nil || len(nt.buckets) == 0 {
				t = m.table.Load()
				continue
			}
			if t.rebuildLeft.Load() <= 0 {
				m.table.CompareAndSwap(t, nt)
			}
			t = nt
			continue
		}
	Chain:
		for cur := b; cur != nil; cur = cur.next.Load() {
			// Seqlock window: pins (pin bits + seq) is read BEFORE the
			// tag scan and the key compare, and again after the value
			// read. Every writer of a slot's key/value pins the slot and
			// bumps the seq, so an unchanged pins word proves that the
			// match AND the value were read with no write in between.
		Win:
			for {
				psStart := cur.pins.Load()
				meta := cur.meta.Load()
				for j := 0; j < slotCount; j++ {
					if uint8(meta>>(8*j)) != byte(tag) {
						continue
					}
					// copy, validate, THEN compare: a multi-word key
					// being rewritten must never be dereferenced torn
					k := cur.keys[j]
					if cur.pins.Load() != psStart {
						continue Win
					}
					if k != key {
						continue
					}
					if psStart&(uint64(1)<<j) != 0 {
						// pinned by a Lock holder or a writer: wait, then
						// redo the whole window (the slot may be gone).
						runtime.Gosched()
						continue Win
					}
					v := cur.values[j]
					if cur.pins.Load() == psStart {
						return v, true
					}
					continue Win // a write overlapped the read: retry
				}
				if cur.pins.Load() != psStart {
					continue // the scan raced a write: redo this node
				}
				continue Chain // not in this node
			}
		}
		return
	}
}

// Store inserts key→value if not present or overwrites it. Returns true
// if a new entry was created. Blocks on Lock-pinned keys (see contract).
func (m *Map[K, V]) Store(key K, value V) (created bool) {
	h := m.hash(key)
	tag := uint64(slotTag(h))

	t := m.loadOrInitTable()
Retry:
	b := t.buckets[h&t.mask].Load()

	switch b.state.Load() {
	case bucketMoved:
		nt := t.nextTable.Load()
		if nt == nil || len(nt.buckets) == 0 {
			t = m.table.Load()
			goto Retry
		}
		if t.rebuildLeft.Load() <= 0 {
			m.table.CompareAndSwap(t, nt)
		}
		t = nt
		goto Retry
	case bucketFrozen:
		m.helpMigrateBucket(t, h&t.mask)
		goto Retry
	}

	b.mu.Lock()
	if b.state.Load() != bucketOpen {
		b.mu.Unlock()
		goto Retry
	}

	// update path
	for cur := b; cur != nil; cur = cur.next.Load() {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			if uint8(meta>>(8*j)) == byte(tag) && cur.keys[j] == key {
				bit := uint64(1) << j
				if !pinSlot(cur, bit) {
					b.mu.Unlock()
					for cur.pins.Load()&bit != 0 {
						runtime.Gosched()
					}
					goto Retry
				}
				cur.values[j] = value // under pin+seq: Load cannot tear it
				unpinSlot(cur, bit)
				b.mu.Unlock()
				return false
			}
		}
	}

	// insert path
	cur := b
	for {
		meta := cur.meta.Load()
		pins := cur.pins.Load()
		for j := 0; j < slotCount; j++ {
			// a free slot still pinned belongs to a Lock that raced a
			// Delete and is about to back off: leave it alone
			if uint8(meta>>(8*j))&occupiedBit == 0 && pins&(uint64(1)<<j) == 0 {
				cur.keys[j] = key
				cur.values[j] = value
				cur.meta.Store(meta&^(uint64(0xff)<<(8*j)) | (tag << (8 * j)))
				b.mu.Unlock()
				m.afterInsert(t)
				return true
			}
		}
		next := cur.next.Load()
		if next == nil {
			nb := &bucketMap[K, V]{}
			nb.keys[0] = key
			nb.values[0] = value
			nb.meta.Store(tag)
			cur.next.Store(nb)
			b.mu.Unlock()
			m.afterInsert(t)
			return true
		}
		cur = next
	}
}

// afterInsert increments live, triggers rebuild if needed, and helps
// progress an in-flight rebuild.
func (m *Map[K, V]) afterInsert(t *tableMap[K, V]) {
	if m.live.Add(1) > int64(loadFactor)*int64(slotCount)*int64(len(t.buckets))/4 {
		m.maybeStartRebuild(t)
	}
	// Help the rebuild of the CURRENT table, not only of t: once most
	// buckets are migrated, inserts land in the next table (t is then
	// nt), and a position skipped because it was pinned would never be
	// revisited — the doubling would stay pending forever.
	if cur := m.table.Load(); cur != t && cur.nextTable.Load() != nil {
		m.helpRebuildProgress(cur, sweepBatch)
	}
	m.helpRebuildProgress(t, sweepBatch)
}

// Lock pins the entry for key, returning &V (stable until Unlock).
func (m *Map[K, V]) Lock(key K) (*V, Cursor[K, V], bool) {
	h := m.hash(key)
	tag := uint64(slotTag(h))

	t := m.loadOrInitTable()
Retry:
	for {
		b := t.buckets[h&t.mask].Load()
		if b.state.Load() == bucketMoved {
			nt := t.nextTable.Load()
			if nt == nil || len(nt.buckets) == 0 {
				t = m.table.Load()
				continue
			}
			if t.rebuildLeft.Load() <= 0 {
				m.table.CompareAndSwap(t, nt)
			}
			t = nt
			continue
		}

		for cur := b; cur != nil; cur = cur.next.Load() {
			ps0 := cur.pins.Load()
			meta := cur.meta.Load()
			for j := 0; j < slotCount; j++ {
				if uint8(meta>>(8*j)) != byte(tag) {
					continue
				}
				k := cur.keys[j] // copy, validate, then compare (see Load)
				if cur.pins.Load() != ps0 {
					runtime.Gosched()
					goto Retry
				}
				if k == key {
					bit := uint64(1) << j
					p := cur.pins.Load()
					if p&bit != 0 {
						// key is pinned by another Lock holder; release CPU
						// and restart the search (bucket might have been
						// migrated meanwhile).
						runtime.Gosched()
						goto Retry
					}
					// Acquire: set the pin bit AND increment the seq in one
					// CAS, so Load observes a coherent before/after window.
					if !cur.pins.CompareAndSwap(p, (p|bit)+pinsSeqUnit) {
						goto Retry
					}
					// Re-validate under the pin. Lock takes no bucket mutex,
					// so between the key match and the CAS a Delete may have
					// freed the slot (and an insert reused it), or a migrator
					// may have frozen the bucket to copy it. Writers and
					// migrators all respect a pinned slot from here on, so a
					// slot that still holds key in an open bucket is ours.
					if b.state.Load() == bucketOpen &&
						uint8(cur.meta.Load()>>(8*j)) == byte(tag) && cur.keys[j] == key {
						return &cur.values[j], Cursor[K, V]{bucket: cur, slotIdx: uint(j)}, true
					}
					unpinSlot(cur, bit)
					if b.state.Load() == bucketFrozen {
						m.helpMigrateBucket(t, h&t.mask)
					}
					runtime.Gosched()
					goto Retry
				}
			}
		}
		return nil, Cursor[K, V]{}, false
	}
}

// Unlock releases the pin acquired by Lock or LockOrStore.
//
// The clear of the pin bit and the seq increment are done in a single CAS
// so a Load that observes a stable pins snapshot across its value read is
// guaranteed that no Lock/Unlock cycle occurred during the read.
//
// Idiomatic use: `defer cur.Unlock()` immediately after a successful Lock
// or LockOrStore.
func (cur Cursor[K, V]) Unlock() {
	bit := uint64(1) << (cur.slotIdx & 7)
	for {
		p := cur.bucket.pins.Load()
		if cur.bucket.pins.CompareAndSwap(p, (p&^bit)+pinsSeqUnit) {
			return
		}
	}
}

// pinSlot pins slot bit of cur for an in-place write (Store update,
// Delete) and bumps the seq, like Lock. It fails if the slot is already
// pinned. Callers hold the bucket mutex.
func pinSlot[K comparable, V any](cur *bucketMap[K, V], bit uint64) bool {
	for {
		p := cur.pins.Load()
		if p&bit != 0 {
			return false
		}
		if cur.pins.CompareAndSwap(p, (p|bit)+pinsSeqUnit) {
			return true
		}
	}
}

// unpinSlot releases a pin taken by pinSlot or Lock, bumping the seq.
func unpinSlot[K comparable, V any](cur *bucketMap[K, V], bit uint64) {
	for {
		p := cur.pins.Load()
		if cur.pins.CompareAndSwap(p, (p&^bit)+pinsSeqUnit) {
			return
		}
	}
}

// LockOrStore atomically locks the slot for key, returning a stable
// *V along with a cursor for Unlock. If the key is already present,
// behaves like Lock and returns the existing slot's *V with
// created=false. Otherwise inserts (key, value), pins the new slot,
// and returns &value with created=true.
func (m *Map[K, V]) LockOrStore(key K, value V) (*V, Cursor[K, V], bool) {
	h := m.hash(key)
	tag := uint64(slotTag(h))

	t := m.loadOrInitTable()
Retry:
	b := t.buckets[h&t.mask].Load()
	switch b.state.Load() {
	case bucketMoved:
		nt := t.nextTable.Load()
		if nt == nil || len(nt.buckets) == 0 {
			t = m.table.Load()
			goto Retry
		}
		if t.rebuildLeft.Load() <= 0 {
			m.table.CompareAndSwap(t, nt)
		}
		t = nt
		goto Retry
	case bucketFrozen:
		m.helpMigrateBucket(t, h&t.mask)
		goto Retry
	}

	b.mu.Lock()
	if b.state.Load() != bucketOpen {
		b.mu.Unlock()
		goto Retry
	}

	// existing key?
	for cur := b; cur != nil; cur = cur.next.Load() {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			if uint8(meta>>(8*j)) == byte(tag) && cur.keys[j] == key {
				bit := uint64(1) << j
				for {
					p := cur.pins.Load()
					if p&bit != 0 {
						b.mu.Unlock()
						for cur.pins.Load()&bit != 0 {
							runtime.Gosched()
						}
						goto Retry
					}
					if cur.pins.CompareAndSwap(p, (p|bit)+pinsSeqUnit) {
						b.mu.Unlock()
						return &cur.values[j], Cursor[K, V]{bucket: cur, slotIdx: uint(j)}, false
					}
				}
			}
		}
	}

	// insert + pin in one shot
	cur := b
	for {
		meta := cur.meta.Load()
		pins := cur.pins.Load()
		for j := 0; j < slotCount; j++ {
			if uint8(meta>>(8*j))&occupiedBit == 0 && pins&(uint64(1)<<j) == 0 {
				cur.keys[j] = key
				cur.values[j] = value
				// pin + seq increment atomically
				for {
					p := cur.pins.Load()
					if cur.pins.CompareAndSwap(p, (p|(uint64(1)<<j))+pinsSeqUnit) {
						break
					}
				}
				cur.meta.Store(meta&^(uint64(0xff)<<(8*j)) | (tag << (8 * j)))
				b.mu.Unlock()
				m.afterInsert(t)
				return &cur.values[j], Cursor[K, V]{bucket: cur, slotIdx: uint(j)}, true
			}
		}
		next := cur.next.Load()
		if next == nil {
			nb := &bucketMap[K, V]{}
			nb.keys[0] = key
			nb.values[0] = value
			nb.pins.Store(1 + pinsSeqUnit) // pin slot 0 + initial seq tick
			nb.meta.Store(tag)
			cur.next.Store(nb)
			b.mu.Unlock()
			m.afterInsert(t)
			return &nb.values[0], Cursor[K, V]{bucket: nb, slotIdx: 0}, true
		}
		cur = next
	}
}

// LoadOrStore returns the existing value for key with loaded=true, or
// otherwise inserts (key, value) and returns value with loaded=false. The
// pin is released before returning — call Lock / LockOrStore if you need
// a stable *V. Sémantique sync.Map.LoadOrStore.
func (m *Map[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	if v, ok := m.Load(key); ok {
		return v, true
	}
	p, cur, created := m.LockOrStore(key, value)
	actual = *p
	cur.Unlock()
	return actual, !created
}

// Delete removes the entry for key. Returns true if an entry was actually
// removed, false if the key was not present.
//
// Concurrency contract (matches Store): Delete BLOCKS on a key currently
// pinned by a Lock holder — silently removing it under the holder would
// invalidate the *V they are mutating. The wait spins outside b.mu so
// other writers on the bucket are not penalized.
//
// Free-list: the slot's meta tag byte is cleared, so the next Store / Lock
// / LockOrStore insert path naturally reuses the slot (it scans for the
// first slot with the occupancy bit unset). No external free-list is
// needed. Key and value fields are zeroed so the GC can reclaim any
// pointers they held.
func (m *Map[K, V]) Delete(key K) bool {
	h := m.hash(key)
	tag := uint64(slotTag(h))

	t := m.loadOrInitTable()
Retry:
	b := t.buckets[h&t.mask].Load()
	switch b.state.Load() {
	case bucketMoved:
		nt := t.nextTable.Load()
		if nt == nil || len(nt.buckets) == 0 {
			t = m.table.Load()
			goto Retry
		}
		if t.rebuildLeft.Load() <= 0 {
			m.table.CompareAndSwap(t, nt)
		}
		t = nt
		goto Retry
	case bucketFrozen:
		m.helpMigrateBucket(t, h&t.mask)
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
				bit := uint64(1) << j
				if !pinSlot(cur, bit) {
					// pinned by a Lock holder — release mu and wait outside
					b.mu.Unlock()
					for cur.pins.Load()&bit != 0 {
						runtime.Gosched()
					}
					goto Retry
				}
				// Under pin+seq: free the slot in meta, then zero the
				// key/value so the GC can reclaim their pointers. A Load
				// that matched the key before sees the seq move and retries.
				cur.meta.Store(meta &^ (uint64(0xff) << (8 * j)))
				var zeroK K
				var zeroV V
				cur.keys[j] = zeroK
				cur.values[j] = zeroV
				unpinSlot(cur, bit)
				b.mu.Unlock()
				m.live.Add(-1)
				return true
			}
		}
	}
	b.mu.Unlock()
	return false
}

// LoadAndDelete atomically reads and removes the entry for key. Returns
// the previous value and loaded=true, or zero V and loaded=false if key
// was not present. Blocks while another goroutine holds the pin (same
// contract as Delete). Sémantique sync.Map.LoadAndDelete.
func (m *Map[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	v, ok, _ := m.deleteIf(key, nil)
	return v, ok
}

// CompareAndDelete deletes the entry for key iff its current value
// equals old (Go interface equality on V). Returns true iff the deletion
// happened. Same comparability requirement on V as CompareAndSwap.
// Sémantique sync.Map.CompareAndDelete.
func (m *Map[K, V]) CompareAndDelete(key K, old V) (deleted bool) {
	_, _, deleted = m.deleteIf(key, func(cur V) bool {
		return any(cur) == any(old)
	})
	return
}

// Swap atomically stores value at key and returns the previous value with
// loaded=true. If the entry did not exist, the new value is inserted,
// previous is zero V and loaded=false. Sémantique sync.Map.Swap.
func (m *Map[K, V]) Swap(key K, value V) (previous V, loaded bool) {
	p, cur, created := m.LockOrStore(key, value)
	if !created {
		previous = *p
		loaded = true
		*p = value
	}
	cur.Unlock()
	return
}

// CompareAndSwap atomically replaces the value at key with new iff the
// current value equals old (Go interface equality on V). Returns true
// iff the swap happened. Sémantique sync.Map.CompareAndSwap.
func (m *Map[K, V]) CompareAndSwap(key K, old, new V) (swapped bool) {
	p, cur, ok := m.Lock(key)
	if !ok {
		return false
	}
	if any(*p) == any(old) {
		*p = new
		swapped = true
	}
	cur.Unlock()
	return
}

// deleteIf is the shared implementation of Delete / LoadAndDelete /
// CompareAndDelete. If predicate is nil, the slot is deleted
// unconditionally; otherwise it is deleted only when predicate(value)
// returns true. Returns the value found (zero if not found), ok = key was
// present, deleted = removal actually happened.
func (m *Map[K, V]) deleteIf(key K, predicate func(V) bool) (value V, ok, deleted bool) {
	h := m.hash(key)
	tag := uint64(slotTag(h))

	t := m.loadOrInitTable()
Retry:
	b := t.buckets[h&t.mask].Load()
	switch b.state.Load() {
	case bucketMoved:
		nt := t.nextTable.Load()
		if nt == nil || len(nt.buckets) == 0 {
			t = m.table.Load()
			goto Retry
		}
		if t.rebuildLeft.Load() <= 0 {
			m.table.CompareAndSwap(t, nt)
		}
		t = nt
		goto Retry
	case bucketFrozen:
		m.helpMigrateBucket(t, h&t.mask)
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
				bit := uint64(1) << j
				if !pinSlot(cur, bit) {
					b.mu.Unlock()
					for cur.pins.Load()&bit != 0 {
						runtime.Gosched()
					}
					goto Retry
				}
				value = cur.values[j] // under our pin: no Lock holder can write it
				ok = true
				if predicate != nil && !predicate(value) {
					unpinSlot(cur, bit)
					b.mu.Unlock()
					return
				}
				cur.meta.Store(meta &^ (uint64(0xff) << (8 * j)))
				var zeroK K
				var zeroV V
				cur.keys[j] = zeroK
				cur.values[j] = zeroV
				unpinSlot(cur, bit)
				b.mu.Unlock()
				m.live.Add(-1)
				deleted = true
				return
			}
		}
	}
	b.mu.Unlock()
	return
}

// Len returns the live entry count (excluding overflow orphans).
func (m *Map[K, V]) Len() int {
	return int(m.live.Load())
}

// Range calls f(k, v) for each live entry. Iteration is weakly consistent:
// a slot pinned by a concurrent Lock holder is skipped (it will reappear
// on a later Range). Slots inserted or deleted during iteration may be
// visited zero or one time. f is called outside any internal lock so it
// can itself call Load/Store/Delete/Lock on m. If f returns false, Range
// stops. A rebuild in progress is followed transparently.
func (m *Map[K, V]) Range(f func(key K, value V) bool) {
	t := m.table.Load()
	if t == nil {
		return
	}
	for i := range t.buckets {
		b := t.buckets[i].Load()
		// follow nextTable transparently when buckets are migrated mid-Range
		if b.state.Load() == bucketMoved {
			nt := t.nextTable.Load()
			if nt != nil && len(nt.buckets) > 0 {
				// scan both new-table halves that map back from this index:
				// together they hold every key originally here.
				newMask := nt.mask
				oldMask := t.mask
				for nidx := uint64(0); nidx <= newMask; nidx++ {
					if nidx&oldMask != uint64(i) {
						continue
					}
					nb := nt.buckets[nidx].Load()
					if !rangeBucketChain(nb, f) {
						return
					}
				}
				continue
			}
		}
		if !rangeBucketChain(b, f) {
			return
		}
	}
}

// rangeBucketChain walks one bucket's overflow chain, calling f on every
// live, unpinned slot, each (key, value) pair read under the seqlock.
// Returns false if f asked to stop.
func rangeBucketChain[K comparable, V any](b *bucketMap[K, V], f func(key K, value V) bool) bool {
	var ks [slotCount]K
	var vs [slotCount]V
	for cur := b; cur != nil; cur = cur.next.Load() {
		// One seqlock window per node: snapshot every live, unpinned
		// slot, then check that no write happened meanwhile (one pins
		// re-read for 8 slots instead of one per slot).
		n := 0
		for {
			ps := cur.pins.Load()
			meta := cur.meta.Load()
			n = 0
			for j := 0; j < slotCount; j++ {
				if uint8(meta>>(8*j))&occupiedBit == 0 || ps&(uint64(1)<<j) != 0 {
					continue // free, or pinned: skip, weakly consistent
				}
				ks[n], vs[n] = cur.keys[j], cur.values[j]
				n++
			}
			if cur.pins.Load() == ps {
				break
			}
		}
		for i := 0; i < n; i++ {
			if !f(ks[i], vs[i]) {
				return false
			}
		}
	}
	return true
}

// Clear drops every entry, returning the map to a freshly initialized
// state. Pin holders of *V via Lock / LockOrStore still see a valid
// pointer (the old bucket lives as long as those pins do); new
// Load/Store/Lock operations see an empty map.
func (m *Map[K, V]) Clear() {
	m.table.Store(newTable[K, V](firstSize))
	m.live.Store(0)
}

// ---------- Rebuild plumbing ----------

func (m *Map[K, V]) maybeStartRebuild(t *tableMap[K, V]) {
	if m.table.Load() != t {
		return
	}
	if t.nextTable.Load() != nil {
		return
	}
	if !t.nextTable.CompareAndSwap(nil, &tableMap[K, V]{}) {
		return
	}
	nt := &tableMap[K, V]{
		buckets: make([]atomic.Pointer[bucketMap[K, V]], 2*len(t.buckets)),
		mask:    2*uint64(len(t.buckets)) - 1,
	}
	t.rebuildLeft.Store(int64(len(t.buckets)))
	t.rebuildIdx.Store(0)
	t.nextTable.Store(nt)
}

func (m *Map[K, V]) helpRebuildProgress(t *tableMap[K, V], n int) {
	nt := t.nextTable.Load()
	if nt == nil || nt == m.table.Load() {
		// Either no rebuild started, or the "lock" pointer is still the
		// placeholder (rebuild being installed). Try again later.
		// nextTable == nil → no rebuild.
		// We also bail on a sentinel that's neither nil nor a real new table.
		if nt == nil || len(nt.buckets) == 0 {
			return
		}
	}
	if nt == nil || len(nt.buckets) == 0 {
		return
	}
	heads := uint64(len(t.buckets))
	for i := 0; i < n; i++ {
		if t.rebuildLeft.Load() <= 0 {
			return
		}
		// The sweep cycles over the positions until every one is
		// migrated: a pinned bucket is skipped, so the first pass may
		// leave some behind. Already-migrated positions cost one atomic
		// load (nt slot != nil) in helpMigrateBucket.
		idx := (t.rebuildIdx.Add(1) - 1) % heads
		m.helpMigrateBucket(t, idx)
	}
}

func (m *Map[K, V]) helpMigrateBucket(t *tableMap[K, V], idx uint64) {
	nt := t.nextTable.Load()
	if nt == nil || len(nt.buckets) == 0 {
		return
	}
	b := t.buckets[idx].Load()
	for {
		// A position is migrated — and counted in rebuildLeft — exactly
		// once: its new-table slots are set by the migration itself.
		if nt.buckets[idx].Load() != nil {
			return
		}
		switch b.state.Load() {
		case bucketMoved:
			return
		case bucketFrozen:
			runtime.Gosched()
			continue
		}
		if chainPinned(b) {
			return // skip: a pinned slot must not move; the sweep comes back
		}
		if !b.state.CompareAndSwap(bucketOpen, bucketFrozen) {
			continue
		}
		if nt.buckets[idx].Load() != nil { // migrated between the check and the claim
			b.state.Store(bucketOpen)
			return
		}
		if !m.migrateBucket(t, nt, idx, b) {
			b.state.Store(bucketOpen) // pinned meanwhile: skipped, retried later
			return
		}
		b.state.Store(bucketMoved)
		if t.rebuildLeft.Add(-1) <= 0 {
			m.table.CompareAndSwap(t, nt)
		}
		return
	}
}

// chainPinned reports whether any slot of b's chain is pinned (only the
// low 8 bits of pins are pin bits; the rest is the seq).
func chainPinned[K comparable, V any](b *bucketMap[K, V]) bool {
	for cur := b; cur != nil; cur = cur.next.Load() {
		if cur.pins.Load()&pinsPinMask != 0 {
			return true
		}
	}
	return false
}

// migrateBucket SPLITS b into two fresh buckets of nt, or returns false
// without touching nt if a slot is pinned. The caller has set
// b.state = bucketFrozen: every writer taking b.mu re-checks the state
// and backs off, and Lock (which takes no mutex) re-validates the state
// after pinning. We take b.mu once to drain writers that passed the
// state check before the freeze; after that, any pin either is visible
// here (skip) or belongs to a Lock that will see bucketFrozen and back
// off before touching the slot (the freeze and the pin are both
// sequentially consistent atomics, so one of the two sides sees the
// other).
func (m *Map[K, V]) migrateBucket(t, nt *tableMap[K, V], idx uint64, b *bucketMap[K, V]) (split bool) {
	oldSize := uint64(len(t.buckets))

	b.mu.Lock()
	b.mu.Unlock() //nolint:staticcheck // SA2001: the Lock/Unlock pair is a barrier, not a real critical section

	if chainPinned(b) {
		return false
	}

	b0 := &bucketMap[K, V]{}
	b1 := &bucketMap[K, V]{}
	for cur := b; cur != nil; cur = cur.next.Load() {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			tb := uint8(meta >> (8 * j))
			if tb&occupiedBit == 0 {
				continue
			}
			k := cur.keys[j]
			v := cur.values[j]
			h := m.hash(k)
			tag := uint64(slotTag(h))
			if h&oldSize == 0 {
				insertIntoBucket(b0, k, v, tag)
			} else {
				insertIntoBucket(b1, k, v, tag)
			}
		}
	}
	nt.buckets[idx].Store(b0)
	nt.buckets[idx+oldSize].Store(b1)
	return true
}

// insertIntoBucket places (key, value) in the first free slot of bucket
// or its overflow chain, creating overflow as needed. Caller is the only
// writer (migrator owns the chain), so no locking needed.
func insertIntoBucket[K comparable, V any](b *bucketMap[K, V], key K, value V, tag uint64) {
	cur := b
	for {
		meta := cur.meta.Load()
		for j := 0; j < slotCount; j++ {
			if uint8(meta>>(8*j))&occupiedBit == 0 {
				cur.keys[j] = key
				cur.values[j] = value
				cur.meta.Store(meta&^(uint64(0xff)<<(8*j)) | (tag << (8 * j)))
				return
			}
		}
		next := cur.next.Load()
		if next == nil {
			nb := &bucketMap[K, V]{}
			nb.keys[0] = key
			nb.values[0] = value
			nb.meta.Store(tag)
			cur.next.Store(nb)
			return
		}
		cur = next
	}
}
