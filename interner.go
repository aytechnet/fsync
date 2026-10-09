package fsync

import (
	"hash/maphash"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

// Interner assigns each distinct key a dense, strictly positive int64
// identifier (1, 2, 3…) and translates both ways, concurrently. It is
// the "dictionary encoding" / symbol table of columnar stores and
// compilers (Rust's lasso::ThreadedRodeo is the closest equivalent);
// Go's unique package interns to canonical handles, not to integers.
//
// Guarantees, for any number of goroutines:
//
//   - one key, one id: concurrent Interns of the same key all get the
//     same id, and exactly one of them reports created = true;
//   - dense ids: an id is drawn only by the goroutine that reserved the
//     key's slot, so no id is burnt by a lost race — ids are exactly
//     1..Len();
//   - both directions consistent: the id → key entry is published
//     before the id, so Key(id) succeeds for any id obtained from
//     Intern or Lookup;
//   - ids are never reassigned: there is no Delete, so an id stored
//     elsewhere (a Store or Bitmap index, a serialized record) can never
//     come to name another key.
//
// 0 is never an id: Lookup returns 0 for an unknown key.
//
// Layout, tailored to an append-only dictionary (no Map underneath):
//
//   - an open-addressing table of 64-bit words, each packing a 24-bit
//     hash fingerprint and a 40-bit id — no key in the table;
//   - the keys, stored ONCE, in a segmented dense array indexed by id,
//     which is also the id → key direction.
//
// A published word never changes, so lookups need no seqlock: probe,
// compare the fingerprint, then the key behind the id. An insert
// reserves an empty word with one CAS, draws the id, writes the key and
// publishes the id: inserts of different keys proceed in parallel. Only
// a table doubling (rare, amortized) excludes inserts, through an
// RWMutex that lookups never touch.
//
// The zero value is usable.
type Interner[K comparable] struct {
	table atomic.Pointer[internTable]
	segs  [internSegCount]atomic.Pointer[internSeg[K]]
	next  atomic.Int64 // last id handed out
	grow  sync.RWMutex // RLock: inserts; Lock: table doubling
	seed  uint64
}

// Slot word layout: id in the low 40 bits, fingerprint in the high 24.
// 0 = empty; fingerprint with id 0 = reserved (id being assigned).
const (
	internIDBits     = 40
	internIDMask     = uint64(1)<<internIDBits - 1
	internFirstSlots = 64     // first table size (power of two)
	internMaxLoad    = 7      // grow beyond 7/10 occupancy (linear probing)
	internSegShift   = 6      // first key segment holds 1<<6 = 64 ids
	internSegCount   = 40 - 6 // segments double: enough for 2^40 ids
)

type internTable struct {
	slots []atomic.Uint64
	mask  uint64
	limit int64 // ids beyond which the table must double
}

// internSeg holds the keys of a contiguous id range, and one
// "published" bit per id (set after the key is written).
type internSeg[K comparable] struct {
	keys []K
	pub  []atomic.Uint64
}

func newInternTable(slots int) *internTable {
	n := internFirstSlots
	for n < slots {
		n <<= 1
	}
	return &internTable{
		slots: make([]atomic.Uint64, n),
		mask:  uint64(n - 1),
		limit: int64(n) * internMaxLoad / 10,
	}
}

// NewInterner returns an empty Interner. The zero value is equally
// usable; NewInterner exists for symmetry, chain Grow to pre-size.
func NewInterner[K comparable]() *Interner[K] {
	return &Interner[K]{}
}

// Grow pre-sizes the table for estimatedItems keys and returns the
// receiver.
func (in *Interner[K]) Grow(estimatedItems int) *Interner[K] {
	in.grow.Lock()
	defer in.grow.Unlock()
	t := in.table.Load()
	need := estimatedItems*10/internMaxLoad + 1
	if t == nil || len(t.slots) < need {
		in.rehash(t, need)
	}
	return in
}

func (in *Interner[K]) hash(key K) uint64 {
	switch v := any(key).(type) {
	case int:
		return hashUint64(in.seed, uint64(v))
	case int64:
		return hashUint64(in.seed, uint64(v))
	case uint64:
		return hashUint64(in.seed, v)
	case uint:
		return hashUint64(in.seed, uint64(v))
	case uint32:
		return hashUint64(in.seed, uint64(v))
	case uintptr:
		return hashUint64(in.seed, uint64(v))
	case string:
		return maphash.String(mapSeed, v)
	default:
		return maphash.Comparable(mapSeed, key)
	}
}

// fingerprint is the high 24 bits of h, forced non-zero so that a
// reserved word (fingerprint, id 0) differs from an empty one.
func fingerprint(h uint64) uint64 {
	return (h>>internIDBits | 1) << internIDBits
}

// locate maps an id to its segment and offset: segment s holds ids
// [64·(2^s − 1) + 1, 64·(2^(s+1) − 1)], i.e. 64, 128, 256… ids.
func locate(id int64) (s int, off uint64) {
	x := uint64(id-1) + 1<<internSegShift
	s = bits.Len64(x) - 1 - internSegShift
	return s, x - uint64(1)<<(s+internSegShift)
}

func (in *Interner[K]) segment(s int) *internSeg[K] {
	if sg := in.segs[s].Load(); sg != nil {
		return sg
	}
	n := 1 << (s + internSegShift)
	sg := &internSeg[K]{keys: make([]K, n), pub: make([]atomic.Uint64, (n+63)/64)}
	if in.segs[s].CompareAndSwap(nil, sg) {
		return sg
	}
	return in.segs[s].Load()
}

// Key returns the key behind id, and false for 0, a negative id or an
// id not handed out (or not yet published).
func (in *Interner[K]) Key(id int64) (key K, ok bool) {
	if id <= 0 || id > in.next.Load() {
		return key, false
	}
	s, off := locate(id)
	sg := in.segs[s].Load()
	if sg == nil || sg.pub[off>>6].Load()&(uint64(1)<<(off&63)) == 0 {
		return key, false
	}
	return sg.keys[off], true
}

// keyOf reads the key of a published id (known to be set).
func (in *Interner[K]) keyOf(id int64) K {
	s, off := locate(id)
	return in.segs[s].Load().keys[off]
}

// find probes t for key. It returns the id when found, or 0 and the
// index of the first empty word on the probe path. A reserved word with
// a matching fingerprint may be this very key being assigned: wait for
// its id rather than skip it.
func (in *Interner[K]) find(t *internTable, h, fp uint64, key K) (id int64, empty uint64) {
	i := h & t.mask
	for {
		w := t.slots[i].Load()
		if w == 0 {
			return 0, i
		}
		if w&^internIDMask == fp {
			id := int64(w & internIDMask)
			if id == 0 {
				runtime.Gosched()
				continue // reserved: re-read the same word
			}
			if in.keyOf(id) == key {
				return id, i
			}
		}
		i = (i + 1) & t.mask
	}
}

// Lookup returns the id of key, or 0 if key was never interned. It
// never assigns an id.
func (in *Interner[K]) Lookup(key K) int64 {
	t := in.table.Load()
	if t == nil {
		return 0
	}
	h := in.hash(key)
	id, _ := in.find(t, h, fingerprint(h), key)
	return id
}

// Intern returns the id of key, assigning the next one if key is new.
// created reports whether this call assigned it.
func (in *Interner[K]) Intern(key K) (id int64, created bool) {
	h := in.hash(key)
	fp := fingerprint(h)
	if t := in.table.Load(); t != nil {
		if id, _ := in.find(t, h, fp, key); id != 0 {
			return id, false
		}
	}
	for {
		in.grow.RLock()
		t := in.table.Load()
		if t == nil || in.next.Load() >= t.limit {
			in.grow.RUnlock()
			in.double(t)
			continue
		}
		id, i := in.find(t, h, fp, key)
		if id != 0 {
			in.grow.RUnlock()
			return id, false
		}
		// Reserve the empty word. Same key ⇒ same probe path, and words
		// are never emptied, so concurrent Interns of one key meet on
		// this word: one CAS wins, the others see the reservation (and
		// wait for the id) when they retry.
		if !t.slots[i].CompareAndSwap(0, fp) {
			in.grow.RUnlock()
			continue
		}
		id = in.next.Add(1)
		s, off := locate(id)
		sg := in.segment(s)
		sg.keys[off] = key
		sg.pub[off>>6].Or(uint64(1) << (off & 63)) // key published…
		t.slots[i].Store(fp | uint64(id))          // …then the id
		in.grow.RUnlock()
		return id, true
	}
}

// double replaces t by a table twice as large, unless another goroutine
// already did. Inserts are excluded (grow.Lock waits for every reserved
// word to be published); lookups keep reading t until the swap.
func (in *Interner[K]) double(t *internTable) {
	in.grow.Lock()
	defer in.grow.Unlock()
	if in.table.Load() != t {
		return
	}
	n := internFirstSlots
	if t != nil {
		n = 2 * len(t.slots)
	}
	in.rehash(t, n)
}

// rehash builds a table of at least n words from t and installs it.
// The caller holds grow.Lock: no word of t is reserved.
func (in *Interner[K]) rehash(t *internTable, n int) {
	nt := newInternTable(n)
	if t != nil {
		for i := range t.slots {
			w := t.slots[i].Load()
			if w == 0 {
				continue
			}
			j := in.hash(in.keyOf(int64(w&internIDMask))) & nt.mask
			for nt.slots[j].Load() != 0 {
				j = (j + 1) & nt.mask
			}
			nt.slots[j].Store(w)
		}
	}
	in.table.Store(nt)
}

// Len returns the number of ids handed out (= the largest id).
func (in *Interner[K]) Len() int {
	return int(in.next.Load())
}

// Range calls f for each (id, key) in id order until f returns false.
// Keys interned concurrently with Range may or may not be visited.
func (in *Interner[K]) Range(f func(id int64, key K) bool) {
	last := in.next.Load()
	for id := int64(1); id <= last; id++ {
		if k, ok := in.Key(id); ok && !f(id, k) {
			return
		}
	}
}
