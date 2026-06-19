package fsync

import (
	//"log"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

type (
	bucketStore[V any] struct {
		lockused atomic.Uint64
		values   [32]V
	}
	tableStore struct {
		buckets []unsafe.Pointer
	}
	Store[V any] struct {
		start    int64
		table    atomic.Pointer[tableStore]
		newTable atomic.Pointer[tableStore]
	}

	bucketMutexStore[V any] struct {
		used    atomic.Uint64
		mutexes [64]sync.Mutex
		values  [64]V
	}
	tableMutexStore struct {
		buckets []unsafe.Pointer
	}
	MutexStore[V any] struct {
		start    int64
		table    atomic.Pointer[tableMutexStore]
		newTable atomic.Pointer[tableMutexStore]
	}

	// StoreCursor[V] is a handle on a pinned slot returned by Store.Lock /
	// Store.LockOrStore. It carries a direct pointer to the bucket (which
	// is stable — buckets are never relocated) plus the slot index inside
	// it, so Unlock is one atomic And on the bucket's lockused word with
	// no table / bucket index re-lookup. bi is a plain uint always
	// masked with `& 31` at use sites; this lets the compiler skip
	// bounds checks on values[bi] / mutexes[bi]. Idiomatic use:
	// `defer cur.Unlock()` immediately after a successful Lock /
	// LockOrStore.
	StoreCursor[V any] struct {
		bucket *bucketStore[V]
		bi     uint
	}

	// MutexStoreCursor[V] is a handle on a pinned slot returned by
	// MutexStore.Lock / MutexStore.LockOrStore. Same direct-pointer
	// optimization as StoreCursor: Unlock is one mutex.Unlock with no
	// table lookup. bi is masked with `& 63` at use sites.
	MutexStoreCursor[V any] struct {
		bucket *bucketMutexStore[V]
		bi     uint
	}
)

// bucket returns the bucket associated to i and the absolute index normalized from i
func (s *Store[V]) bucket(i int64) (b *bucketStore[V], index uint) {
	if i < s.start {
		return
	}

	index = uint(i - s.start)

	bucketIndex := index >> 5

	// optimize read path as table is the current table
	// a new table is allocated for locking and updating buckets
	if table := s.table.Load(); table != nil {
		if bucketIndex < uint(len(table.buckets)) {
			b = (*bucketStore[V])(atomic.LoadPointer(&table.buckets[bucketIndex]))
		}
	}

	return
}

// bucket returns the bucket associated to i and the absolute index normalized from i
func (s *Store[V]) bucketAlloc(i int64) (b *bucketStore[V], index uint) {
	if i < s.start {
		return
	}

	index = uint(i - s.start)

	bucketIndex := index >> 5

Retry:

	// table is never nil unless for empty buckets
	if table := s.table.Load(); table != nil {
		if bucketIndex < uint(len(table.buckets)) {
			b = (*bucketStore[V])(atomic.LoadPointer(&table.buckets[bucketIndex]))
		}

		// optimize write path on already allocated buckets only
		if b == nil {
			// a new table may need to be created
			if size := 1 << bits.Len64(uint64(bucketIndex)); size > len(table.buckets) {
				if newTable := s.newTable.Load(); newTable == table {
					newTable = &tableStore{
						buckets: make([]unsafe.Pointer, size),
					}

					if s.newTable.CompareAndSwap(table, newTable) {
						// copy existing table buckets
						copy(newTable.buckets, table.buckets)

						// in order to fix a potential race on allocated new buckets in table after a nil pointer has been copied above
						// the store must fix new table as soon as possible
						if !s.table.CompareAndSwap(table, newTable) {
							// Invariant: the s.newTable CAS above guards this window;
							// reaching this branch would mean another goroutine modified
							// s.table inside it, which the lock discipline forbids. Was
							// a panic — silenced for lib publication, never reproduced.
						}

						table = newTable
					} else {
						// a new table has been allocated meanwhile so retry
						//log.Printf("fsync(store): race allocation of new table, dropping buckets of length %d", size)
						goto Retry
					}
				} else {
					// new table is not equal to table so retry
					//log.Printf("fsync(store): race new table not equal to table (bucketIndex=%d)", bucketIndex)
					goto Retry
				}
			}

			pb := &table.buckets[bucketIndex]
			if b = (*bucketStore[V])(atomic.LoadPointer(pb)); b == nil {
				// allocating a new bucket is only possible safely if no new table already exists
				// or a race may occurs without a CAS
				if s.newTable.CompareAndSwap(table, nil) {
					// allocate a new bucket
					b = &bucketStore[V]{}

					if !atomic.CompareAndSwapPointer(pb, nil, unsafe.Pointer(b)) {
						//log.Printf("fsync(store): race access on table bucket %d, dropped allocated bucket", bucketIndex)
						b = (*bucketStore[V])(atomic.LoadPointer(pb))
					}

					if !s.newTable.CompareAndSwap(nil, table) {
						// Invariant: we reserved the table by CAS-ing s.newTable to nil
						// before allocating the bucket; restoring it must succeed. Was
						// a panic — silenced for lib publication, never reproduced.
					}
				} else {
					//log.Printf("fsync(store): race a new table already exists")
					goto Retry
				}
			}
		}
	} else {
		// the first table must be created
		size := 1 << bits.Len64(uint64(bucketIndex))
		if size < 32 {
			size = 32
		}

		newTable := &tableStore{
			buckets: make([]unsafe.Pointer, size),
		}

		if s.table.CompareAndSwap(nil, newTable) {
			// only this goroutine is able to update table once new table will be complete
			pb := &newTable.buckets[bucketIndex]

			if b = (*bucketStore[V])(atomic.LoadPointer(pb)); b == nil {
				// allocate a new bucket
				b = &bucketStore[V]{}

				if !atomic.CompareAndSwapPointer(pb, nil, unsafe.Pointer(b)) {
					//log.Printf("fsync(store): race access on first table bucket %d, dropped allocated bucket", bucketIndex)
					b = (*bucketStore[V])(atomic.LoadPointer(pb))
				}
			}

			if !s.newTable.CompareAndSwap(nil, newTable) {
				// Invariant: only the goroutine that CAS-ed s.table from nil to
				// newTable owns this window; nobody else should publish into
				// s.newTable here. Was a panic — silenced for lib publication.
			}
		} else {
			//log.Printf("fsync(store): dropped first allocated table of size %d", size)
			// another go routine has already made the first table, so use it instead
			goto Retry
		}
	}

	return
}

// bucket returns the bucket associated to i and the absolute index normalized from i
func (s *MutexStore[V]) bucket(i int64) (b *bucketMutexStore[V], index uint) {
	if i < s.start {
		return
	}

	index = uint(i - s.start)

	bucketIndex := index >> 6

	// optimize read path as table is the current table
	// a new table is allocated for locking and updating buckets
	if table := s.table.Load(); table != nil {
		if bucketIndex < uint(len(table.buckets)) {
			b = (*bucketMutexStore[V])(atomic.LoadPointer(&table.buckets[bucketIndex]))
		}
	}

	return
}

// bucket returns the bucket associated to i and the absolute index normalized from i
func (s *MutexStore[V]) bucketAlloc(i int64) (b *bucketMutexStore[V], index uint) {
	if i < s.start {
		return
	}

	index = uint(i - s.start)

	bucketIndex := index >> 6

Retry:

	// table is never nil unless for empty buckets
	if table := s.table.Load(); table != nil {
		if bucketIndex < uint(len(table.buckets)) {
			b = (*bucketMutexStore[V])(atomic.LoadPointer(&table.buckets[bucketIndex]))
		}

		// optimize write path on already allocated buckets only
		if b == nil {
			// a new table may need to be created
			if size := 1 << bits.Len64(uint64(bucketIndex)); size > len(table.buckets) {
				if newTable := s.newTable.Load(); newTable == table {
					newTable = &tableMutexStore{
						buckets: make([]unsafe.Pointer, size),
					}

					if s.newTable.CompareAndSwap(table, newTable) {
						// copy existing table buckets
						copy(newTable.buckets, table.buckets)

						// in order to fix a potential race on allocated new buckets in table after a nil pointer has been copied above
						// the store must fix new table as soon as possible
						if !s.table.CompareAndSwap(table, newTable) {
							// Invariant: the s.newTable CAS above guards this window;
							// reaching this branch would mean another goroutine modified
							// s.table inside it, which the lock discipline forbids. Was
							// a panic — silenced for lib publication, never reproduced.
						}

						table = newTable
					} else {
						// a new table has been allocated meanwhile so retry
						//log.Printf("fsync(store): race allocation of new table, dropping buckets of length %d", size)
						goto Retry
					}
				} else {
					// new table is not equal to table so retry
					//log.Printf("fsync(store): race new table not equal to table (bucketIndex=%d)", bucketIndex)
					goto Retry
				}
			}

			pb := &table.buckets[bucketIndex]
			if b = (*bucketMutexStore[V])(atomic.LoadPointer(pb)); b == nil {
				// allocating a new bucket is only possible safely if no new table already exists
				// or a race may occurs without a CAS
				if s.newTable.CompareAndSwap(table, nil) {
					// allocate a new bucket
					b = &bucketMutexStore[V]{}

					if !atomic.CompareAndSwapPointer(pb, nil, unsafe.Pointer(b)) {
						//log.Printf("fsync(store): race access on table bucket %d, dropped allocated bucket", bucketIndex)
						b = (*bucketMutexStore[V])(atomic.LoadPointer(pb))
					}

					if !s.newTable.CompareAndSwap(nil, table) {
						// Invariant: we reserved the table by CAS-ing s.newTable to nil
						// before allocating the bucket; restoring it must succeed. Was
						// a panic — silenced for lib publication, never reproduced.
					}
				} else {
					//log.Printf("fsync(store): race a new table already exists")
					goto Retry
				}
			}
		}
	} else {
		// the first table must be created
		size := 1 << bits.Len64(uint64(bucketIndex))
		if size < 16 {
			size = 16
		}

		newTable := &tableMutexStore{
			buckets: make([]unsafe.Pointer, size),
		}

		if s.table.CompareAndSwap(nil, newTable) {
			// only this goroutine is able to update table once new table will be complete
			pb := &newTable.buckets[bucketIndex]

			if b = (*bucketMutexStore[V])(atomic.LoadPointer(pb)); b == nil {
				// allocate a new bucket
				b = &bucketMutexStore[V]{}

				if !atomic.CompareAndSwapPointer(pb, nil, unsafe.Pointer(b)) {
					//log.Printf("fsync(store): race access on first table bucket %d, dropped allocated bucket", bucketIndex)
					b = (*bucketMutexStore[V])(atomic.LoadPointer(pb))
				}
			}

			if !s.newTable.CompareAndSwap(nil, newTable) {
				// Invariant: only the goroutine that CAS-ed s.table from nil to
				// newTable owns this window; nobody else should publish into
				// s.newTable here. Was a panic — silenced for lib publication.
			}
		} else {
			//log.Printf("fsync(store): dropped first allocated table of size %d", size)
			// another go routine has already made the first table, so use it instead
			goto Retry
		}
	}

	return
}

// NewStore returns an empty Store[V] whose keys are interpreted as
// offsets from start. A Lock/Load/Store at index i maps to absolute
// slot (i - start); calls with i < start are no-ops. The zero-value
// Store is equally usable (start defaults to 0).
func NewStore[V any](start int64) *Store[V] {
	return &Store[V]{
		start: start,
	}
}

// Grow ensures the bucket-pointer table has room for up to
// `maxIndex` (inclusive), without allocating any of the underlying
// buckets themselves. Buckets are still allocated lazily on the
// first Store / Lock / LockOrStore touching each slot. Useful when
// the maximum expected index is known up-front: a single Grow call
// avoids the chain of intermediate table doublings (and the
// transient memory churn of allocating then dropping each doubled
// table) that would happen if Store discovered the size
// incrementally. Calls with maxIndex < s.start are a no-op; calls
// where the existing table is already large enough are a no-op.
func (s *Store[V]) Grow(maxIndex int64) {
	if maxIndex < s.start {
		return
	}
	bucketIndex := uint(maxIndex-s.start) >> 5
	targetSize := 1 << bits.Len64(uint64(bucketIndex))
	if targetSize < 32 {
		targetSize = 32
	}

	for {
		table := s.table.Load()
		if table != nil && len(table.buckets) >= targetSize {
			return
		}
		if table == nil {
			// First table: allocate at the target size in one shot.
			nt := &tableStore{buckets: make([]unsafe.Pointer, targetSize)}
			if s.table.CompareAndSwap(nil, nt) {
				if !s.newTable.CompareAndSwap(nil, nt) {
					// Invariant: see bucketAlloc — restoring newTable
					// must succeed if no one else owns this window.
				}
				return
			}
			continue // another goroutine made the first table; retry
		}
		// Existing table is too small. Reserve the resize via the
		// same s.table/s.newTable CAS protocol bucketAlloc uses.
		if nt := s.newTable.Load(); nt == table {
			bigger := &tableStore{buckets: make([]unsafe.Pointer, targetSize)}
			if s.newTable.CompareAndSwap(table, bigger) {
				copy(bigger.buckets, table.buckets)
				if !s.table.CompareAndSwap(table, bigger) {
					// Invariant: same as bucketAlloc.
				}
				return
			}
		}
		// Someone else holds the resize window; retry.
	}
}

// Len returns the live entry count. The result is consistent under
// concurrency only with respect to the bucket scan order; Store /
// Delete that happen during the scan may or may not be reflected.
func (s *Store[V]) Len() (count int) {
	if table := s.table.Load(); table != nil {
		for i := range table.buckets {
			if b := (*bucketStore[V])(atomic.LoadPointer(&table.buckets[i])); b != nil {
				count += bits.OnesCount32(uint32(b.lockused.Load()))
			}
		}
	}

	return
}

// Range calls f(i, v) for each used entry. Iteration is weakly consistent
// (a slot pinned by a concurrent Lock holder is skipped — it will reappear
// on a later Range), and f is called outside any internal lock so it can
// itself call Load/Store/Delete on s. If f returns false, Range stops.
func (s *Store[V]) Range(f func(i int64, value V) bool) {
	table := s.table.Load()
	if table == nil {
		return
	}
	for bucketIdx := range table.buckets {
		b := (*bucketStore[V])(atomic.LoadPointer(&table.buckets[bucketIdx]))
		if b == nil {
			continue
		}
		baseIdx := int64(bucketIdx) << 5
		lu := b.lockused.Load()
		used := uint32(lu)
		locked := uint32(lu >> 32)
		// only visit entries that are used AND not currently pinned
		for live := used & ^locked; live != 0; live &= live - 1 {
			bi := bits.TrailingZeros32(live)
			if !f(s.start+baseIdx+int64(bi), b.values[bi]) {
				return
			}
		}
	}
}

// Clear drops every entry. Pin holders of *V via Lock / LockOrStore still
// see a valid pointer (the bucket lives as long as those pins do); new
// Load/Store/Lock operations see an empty Store. Both table and the
// shadow newTable used for resize locking are reset so the next
// bucketAlloc starts from a clean state.
func (s *Store[V]) Clear() {
	s.table.Store(nil)
	s.newTable.Store(nil)
}

// Load returns the value at i if it exists. The implementation briefly
// pins the slot via b.lockused.Or(lockbit) during the read so that
// multi-word V cannot be torn by a concurrent Lock holder writing
// mid-cycle (see README "Design history" section for why this
// trumps a Load-then-CAS variant).
func (s *Store[V]) Load(i int64) (value V, ok bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 31
		usebit := uint64(1) << bi
		lockbit := usebit << 32
		bits := lockbit | usebit

		for {
			//for range 16 {
			if oldbits := b.lockused.Or(lockbit) & bits; oldbits == usebit {
				// entry was used and unlocked
				value = b.values[bi]
				ok = true

				b.lockused.And(^lockbit)

				return
			} else if oldbits == 0 {
				// entry was not used and unlocked so unlock it before return
				b.lockused.And(^lockbit)

				return
			} else if oldbits == lockbit {
				// entry was not used but locked so return
				return
			} // retry if entry was used and locked
			//}

			//runtime.Gosched()
		}
	}

	return
}

// Lock pins the entry at i and returns &V along with a cursor. ok=false
// means the entry does not exist (and no pin is held). Caller must call
// cur.Unlock() to release the pin.
//
// Implementation: Load-then-CAS instead of unconditional Or(lockbit).
// On contention, the spin loop only does Load on the shared cacheline
// (kept in the M(O)ESI Shared state across cores, no invalidation
// traffic). The cacheline is only invalidated when a goroutine
// actually wins the CAS and takes ownership, instead of on every
// retry attempt. Substantial speedup under heavy pin contention; tiny
// overhead in the uncontended path (Load + CAS vs single Or).
func (s *Store[V]) Lock(i int64) (value *V, cursor StoreCursor[V], ok bool) {
	if b, idx := s.bucket(i); b != nil {
		bi := idx & 31
		usebit := uint64(1) << bi
		lockbit := usebit << 32

		for {
			cur := b.lockused.Load()
			if cur&usebit == 0 {
				// entry not used → no pin to release, just bail out
				return
			}
			if cur&lockbit != 0 {
				// pinned by another goroutine; spin on Load only
				runtime.Gosched()
				continue
			}
			// used && not locked → try to take the pin
			if b.lockused.CompareAndSwap(cur, cur|lockbit) {
				value = &b.values[bi]
				cursor = StoreCursor[V]{bucket: b, bi: bi}
				ok = true

				return
			}
			// CAS lost a race with a concurrent Lock / Delete; retry
		}
	}

	return
}

// Unlock releases the pin acquired by Store.Lock or Store.LockOrStore.
// Calling Unlock on the zero cursor is a no-op.
func (cur StoreCursor[V]) Unlock() {
	if cur.bucket == nil {
		return
	}
	lockbit := uint64(1) << (32 + (cur.bi & 31))

	if cur.bucket.lockused.And(^lockbit) == 0 {
		panic("fsync(Store): unlock of unlocked entry")
	}
}

// Store writes value at index i, allocating the bucket if needed.
// Returns true if the slot was previously empty (a new entry was
// created), false if the call overwrote an existing value. Store
// blocks (Gosched spin) while a Lock holder owns the slot.
func (s *Store[V]) Store(i int64, value V) (created bool) {
	if b, i := s.bucketAlloc(i); b != nil {
		bi := i & 31
		usebit := uint64(1) << bi
		lockbit := usebit << 32
		bits := lockbit | usebit

		for {
			if oldbits := b.lockused.Or(bits) & bits; oldbits&lockbit == 0 {
				// entry was unlocked
				b.values[bi] = value

				b.lockused.And(^lockbit)

				return oldbits&usebit == 0
			} // retry if entry was locked

			//runtime.Gosched()
		}
	}

	return
}

// LockOrStore atomically returns a stable *V to the entry at i with a
// cursor, pinned for exclusive access (caller MUST call cur.Unlock()).
// If the entry did not exist, it is initialized with value and
// created is true; otherwise the existing value is returned unchanged
// and created is false. Like Lock, the call spins while another
// goroutine holds the pin on the same entry.
//
// Implementation: Load-then-CAS spin (same as Lock). The CAS sets
// both the pin AND usebit in one atomic op, eliminating the second
// Or(usebit) that the previous version did on the create path. On
// the hold path, Load on the shared cacheline replaces the wasteful
// Or(lockbit) retry of the old version.
func (s *Store[V]) LockOrStore(i int64, value V) (p *V, cursor StoreCursor[V], created bool) {
	if b, idx := s.bucketAlloc(i); b != nil {
		bi := idx & 31
		usebit := uint64(1) << bi
		lockbit := usebit << 32

		for {
			cur := b.lockused.Load()
			if cur&lockbit != 0 {
				// pinned by another goroutine; spin on Load only
				runtime.Gosched()
				continue
			}
			// not pinned; take pin and set used in one CAS
			if b.lockused.CompareAndSwap(cur, cur|lockbit|usebit) {
				if cur&usebit == 0 {
					b.values[bi] = value
					created = true
				}
				p = &b.values[bi]
				cursor = StoreCursor[V]{bucket: b, bi: bi}

				return
			}
			// CAS lost a race; retry
		}
	}

	return
}

// LoadOrStore returns the value at i if it already exists (loaded=true,
// value untouched) or initializes it with the supplied value (loaded=false,
// actual=value). Unlike LockOrStore it does NOT return a pinned *V — the
// pin is released before returning. Sémantique sync.Map.LoadOrStore.
func (s *Store[V]) LoadOrStore(i int64, value V) (actual V, loaded bool) {
	if b, i := s.bucketAlloc(i); b != nil {
		bi := i & 31
		usebit := uint64(1) << bi
		lockbit := usebit << 32
		bits := lockbit | usebit

		for {
			if oldbits := b.lockused.Or(lockbit) & bits; oldbits&lockbit == 0 {
				if oldbits&usebit != 0 {
					actual = b.values[bi]
					loaded = true
				} else {
					b.values[bi] = value
					b.lockused.Or(usebit)
					actual = value
				}
				b.lockused.And(^lockbit)
				return
			}
		}
	}

	return
}

// Delete clears the entry at i. Returns true if an entry was actually
// removed, false if the slot was already empty. Unlike Load/Store/Lock,
// Delete does NOT take the pin: a concurrent Lock holder continues to
// see a valid *V into the bucket, but the slot is marked free and the
// caller's next Lock(i) / Load(i) will return ok=false until a new
// Store happens.
func (s *Store[V]) Delete(i int64) (deleted bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 31
		usebit := uint64(1) << bi

		if b.lockused.And(^usebit)&usebit != 0 {
			return true
		}
	}

	return false
}

// LoadAndDelete atomically reads and removes the entry at i. Returns the
// previous value with loaded=true, or zero V with loaded=false if the
// entry did not exist. Blocks while another goroutine holds the pin.
// Sémantique sync.Map.LoadAndDelete.
func (s *Store[V]) LoadAndDelete(i int64) (value V, loaded bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 31
		usebit := uint64(1) << bi
		lockbit := usebit << 32
		bits := lockbit | usebit

		for {
			if oldbits := b.lockused.Or(lockbit) & bits; oldbits&lockbit == 0 {
				if oldbits&usebit != 0 {
					value = b.values[bi]
					loaded = true
					// clear both used and lock atomically
					b.lockused.And(^bits)
				} else {
					b.lockused.And(^lockbit)
				}
				return
			}
		}
	}
	return
}

// Swap atomically writes value at i and returns the previous value with
// loaded=true; if the entry did not exist, loaded=false and previous is
// the zero V. Blocks while another goroutine holds the pin. Sémantique
// sync.Map.Swap.
func (s *Store[V]) Swap(i int64, value V) (previous V, loaded bool) {
	if b, i := s.bucketAlloc(i); b != nil {
		bi := i & 31
		usebit := uint64(1) << bi
		lockbit := usebit << 32
		bits := lockbit | usebit

		for {
			if oldbits := b.lockused.Or(lockbit) & bits; oldbits&lockbit == 0 {
				if oldbits&usebit != 0 {
					previous = b.values[bi]
					loaded = true
				}
				b.values[bi] = value
				// publish used=1 if it wasn't, then drop the lock
				if oldbits&usebit == 0 {
					b.lockused.Or(usebit)
				}
				b.lockused.And(^lockbit)
				return
			}
		}
	}
	return
}

// CompareAndSwap swaps the entry's value with new if and only if the
// current value equals old. Comparison uses Go's interface equality on V,
// so V must be runtime-comparable. Returns true iff the swap happened.
// Blocks while another goroutine holds the pin. Sémantique
// sync.Map.CompareAndSwap.
func (s *Store[V]) CompareAndSwap(i int64, old, new V) (swapped bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 31
		usebit := uint64(1) << bi
		lockbit := usebit << 32
		bits := lockbit | usebit

		for {
			if oldbits := b.lockused.Or(lockbit) & bits; oldbits&lockbit == 0 {
				if oldbits&usebit != 0 && any(b.values[bi]) == any(old) {
					b.values[bi] = new
					swapped = true
				}
				b.lockused.And(^lockbit)
				return
			}
		}
	}
	return
}

// CompareAndDelete deletes the entry if and only if its current value
// equals old. Returns true iff the deletion happened. Same comparability
// requirement on V as CompareAndSwap. Sémantique sync.Map.CompareAndDelete.
func (s *Store[V]) CompareAndDelete(i int64, old V) (deleted bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 31
		usebit := uint64(1) << bi
		lockbit := usebit << 32
		bits := lockbit | usebit

		for {
			if oldbits := b.lockused.Or(lockbit) & bits; oldbits&lockbit == 0 {
				if oldbits&usebit != 0 && any(b.values[bi]) == any(old) {
					deleted = true
					b.lockused.And(^bits)
				} else {
					b.lockused.And(^lockbit)
				}
				return
			}
		}
	}
	return
}

// NewMutexStore returns an empty MutexStore[V]. Same start-offset
// semantics as NewStore; zero-value is usable.
func NewMutexStore[V any](start int64) *MutexStore[V] {
	return &MutexStore[V]{
		start: start,
	}
}

// Grow ensures the bucket-pointer table has room for up to
// `maxIndex` (inclusive), without allocating any of the underlying
// buckets themselves. Same semantics as Store.Grow but with
// MutexStore's 64-slot bucket size: the bucket-index shift is `>> 6`
// instead of `>> 5`, and the first-table minimum is 16 instead of 32.
func (s *MutexStore[V]) Grow(maxIndex int64) {
	if maxIndex < s.start {
		return
	}
	bucketIndex := uint(maxIndex-s.start) >> 6
	targetSize := 1 << bits.Len64(uint64(bucketIndex))
	if targetSize < 16 {
		targetSize = 16
	}

	for {
		table := s.table.Load()
		if table != nil && len(table.buckets) >= targetSize {
			return
		}
		if table == nil {
			nt := &tableMutexStore{buckets: make([]unsafe.Pointer, targetSize)}
			if s.table.CompareAndSwap(nil, nt) {
				if !s.newTable.CompareAndSwap(nil, nt) {
					// Invariant: see bucketAlloc.
				}
				return
			}
			continue
		}
		if nt := s.newTable.Load(); nt == table {
			bigger := &tableMutexStore{buckets: make([]unsafe.Pointer, targetSize)}
			if s.newTable.CompareAndSwap(table, bigger) {
				copy(bigger.buckets, table.buckets)
				if !s.table.CompareAndSwap(table, bigger) {
					// Invariant: see bucketAlloc.
				}
				return
			}
		}
	}
}

// Len returns the live entry count. Same caveat as Store.Len: not a
// linearization point under concurrency.
func (s *MutexStore[V]) Len() (count int) {
	if table := s.table.Load(); table != nil {
		for i := range table.buckets {
			if b := (*bucketMutexStore[V])(atomic.LoadPointer(&table.buckets[i])); b != nil {
				count += bits.OnesCount64(b.used.Load())
			}
		}
	}

	return
}

// Range calls f(i, v) for each used entry. The per-slot mutex is taken
// briefly to read each value; f is called with the mutex released so it
// can itself call Load/Store/Delete on s. If f returns false, Range stops.
func (s *MutexStore[V]) Range(f func(i int64, value V) bool) {
	table := s.table.Load()
	if table == nil {
		return
	}
	for bucketIdx := range table.buckets {
		b := (*bucketMutexStore[V])(atomic.LoadPointer(&table.buckets[bucketIdx]))
		if b == nil {
			continue
		}
		baseIdx := int64(bucketIdx) << 6
		used := b.used.Load()
		for live := used; live != 0; live &= live - 1 {
			bi := bits.TrailingZeros64(live)
			b.mutexes[bi].Lock()
			v := b.values[bi]
			b.mutexes[bi].Unlock()
			if !f(s.start+baseIdx+int64(bi), v) {
				return
			}
		}
	}
}

// Clear drops every entry. Pin holders of *V via Lock / LockOrStore still
// see a valid pointer; new operations see an empty store. Both table and
// the shadow newTable used for resize locking are reset.
func (s *MutexStore[V]) Clear() {
	s.table.Store(nil)
	s.newTable.Store(nil)
}

// Load returns the value at i if it exists. The per-slot mutex is
// briefly taken to read the value (multi-word V is safe from torn
// reads). Blocks while a Lock holder owns the slot's mutex.
func (s *MutexStore[V]) Load(i int64) (value V, ok bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 63
		usebit := uint64(1) << bi

		if ok = b.used.Load()&usebit != 0; ok {
			b.mutexes[bi].Lock()
			value = b.values[bi]
			b.mutexes[bi].Unlock()
		}
	}

	return
}

// Lock pins the entry at i (takes its per-slot mutex) and returns &V
// with a cursor. ok=false means the entry does not exist (and no mutex
// is held). Caller must call cur.Unlock() to release.
func (s *MutexStore[V]) Lock(i int64) (value *V, cursor MutexStoreCursor[V], ok bool) {
	if b, idx := s.bucket(i); b != nil {
		bi := idx & 63
		usebit := uint64(1) << bi

		if b.used.Load()&usebit != 0 {
			b.mutexes[bi].Lock()
			value = &b.values[bi]
			cursor = MutexStoreCursor[V]{bucket: b, bi: bi}
			ok = true
		}
	}

	return
}

// Unlock releases the per-slot mutex acquired by MutexStore.Lock or
// MutexStore.LockOrStore. No-op on the zero cursor.
func (cur MutexStoreCursor[V]) Unlock() {
	if cur.bucket == nil {
		return
	}
	cur.bucket.mutexes[cur.bi&63].Unlock()
}

// Store writes value at index i. Returns created=true if the slot was
// previously empty. The per-slot mutex is taken while the value is
// written; used is published AFTER the mutex is released (cheaper
// hot-path at the cost of a brief window where the slot is observable
// by a Load taking the mutex but not yet marked used — LockOrStore
// handles this race by checking used while holding the mutex).
func (s *MutexStore[V]) Store(i int64, value V) (created bool) {
	if b, i := s.bucketAlloc(i); b != nil {
		bi := i & 63
		usebit := uint64(1 << bi)

		b.mutexes[bi].Lock()
		b.values[bi] = value
		b.mutexes[bi].Unlock()

		created = b.used.Or(usebit)&usebit == 0
	}

	return
}

// LockOrStore atomically returns a stable *V to the entry at i, with
// the per-slot mutex held (caller MUST eventually call Unlock(i)). If
// the entry did not exist, it is initialized with value and created is
// true; otherwise the existing value is returned unchanged and
// created is false.
// LockOrStore atomically returns a stable *V to the entry at i with a
// cursor and the per-slot mutex held (caller MUST call cur.Unlock()).
// If the entry did not exist, it is initialized with value and
// created=true; otherwise the existing value is returned and created=false.
func (s *MutexStore[V]) LockOrStore(i int64, value V) (p *V, cursor MutexStoreCursor[V], created bool) {
	if b, idx := s.bucketAlloc(i); b != nil {
		bi := idx & 63
		usebit := uint64(1 << bi)

		b.mutexes[bi].Lock()
		// Test used under our mutex so we don't race with Store, which
		// publishes used AFTER releasing the mutex.
		if b.used.Load()&usebit == 0 {
			b.values[bi] = value
			b.used.Or(usebit)
			created = true
		}
		p = &b.values[bi]
		cursor = MutexStoreCursor[V]{bucket: b, bi: bi}
	}

	return
}

// LoadOrStore returns the value at i if it already exists (loaded=true,
// value untouched) or initializes it with the supplied value (loaded=false,
// actual=value). Sémantique sync.Map.LoadOrStore.
func (s *MutexStore[V]) LoadOrStore(i int64, value V) (actual V, loaded bool) {
	if b, i := s.bucketAlloc(i); b != nil {
		bi := i & 63
		usebit := uint64(1 << bi)

		b.mutexes[bi].Lock()
		if b.used.Load()&usebit != 0 {
			actual = b.values[bi]
			loaded = true
		} else {
			b.values[bi] = value
			b.used.Or(usebit)
			actual = value
		}
		b.mutexes[bi].Unlock()
	}

	return
}

// Delete clears the entry at i. Returns true if an entry was actually
// removed. Same concurrency semantics as Store.Delete (no pin taken).
func (s *MutexStore[V]) Delete(i int64) (deleted bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 63
		usebit := uint64(1 << bi)

		if b.used.And(^usebit)&usebit != 0 {
			return true
		}
	}

	return false
}

// LoadAndDelete atomically reads and removes the entry at i. Returns the
// previous value with loaded=true, or zero V with loaded=false.
// Sémantique sync.Map.LoadAndDelete.
func (s *MutexStore[V]) LoadAndDelete(i int64) (value V, loaded bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 63
		usebit := uint64(1 << bi)

		b.mutexes[bi].Lock()
		if b.used.Load()&usebit != 0 {
			value = b.values[bi]
			loaded = true
			b.used.And(^usebit)
		}
		b.mutexes[bi].Unlock()
	}
	return
}

// Swap atomically writes value at i and returns the previous value with
// loaded=true; if the entry did not exist, loaded=false. Sémantique
// sync.Map.Swap.
func (s *MutexStore[V]) Swap(i int64, value V) (previous V, loaded bool) {
	if b, i := s.bucketAlloc(i); b != nil {
		bi := i & 63
		usebit := uint64(1 << bi)

		b.mutexes[bi].Lock()
		if b.used.Load()&usebit != 0 {
			previous = b.values[bi]
			loaded = true
		} else {
			b.used.Or(usebit)
		}
		b.values[bi] = value
		b.mutexes[bi].Unlock()
	}
	return
}

// CompareAndSwap swaps the entry's value with new if and only if the
// current value equals old (Go interface equality). Returns true iff the
// swap happened. Sémantique sync.Map.CompareAndSwap.
func (s *MutexStore[V]) CompareAndSwap(i int64, old, new V) (swapped bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 63
		usebit := uint64(1 << bi)

		b.mutexes[bi].Lock()
		if b.used.Load()&usebit != 0 && any(b.values[bi]) == any(old) {
			b.values[bi] = new
			swapped = true
		}
		b.mutexes[bi].Unlock()
	}
	return
}

// CompareAndDelete deletes the entry iff its current value equals old.
// Sémantique sync.Map.CompareAndDelete.
func (s *MutexStore[V]) CompareAndDelete(i int64, old V) (deleted bool) {
	if b, i := s.bucket(i); b != nil {
		bi := i & 63
		usebit := uint64(1 << bi)

		b.mutexes[bi].Lock()
		if b.used.Load()&usebit != 0 && any(b.values[bi]) == any(old) {
			b.used.And(^usebit)
			deleted = true
		}
		b.mutexes[bi].Unlock()
	}
	return
}
