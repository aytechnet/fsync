package fsync

import (
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// Bitmap is a lock-free concurrent bit set indexed by int64. Each
// bucket packs 64 bits in a single atomic.Uint64 word, so the per-
// entry memory cost is 1 bit (0.125 byte) plus the bucket pointer
// table overhead amortized across 64 entries. By comparison Store[bool]
// would carry ~1.25 bytes per entry (`[32]bool` + a `lockused` word
// per 32 entries), so Bitmap is roughly **10× smaller** at steady
// state.
//
// Indexes are interpreted as offsets from `start` (Bitmap.Set(i)
// targets absolute slot `i - start`); calls with `i < start` are
// no-ops, mirroring Store. The zero value is usable (start defaults
// to 0).
//
// Bitmap has no Lock(*V) API: a pointer to a single bit does not
// exist, and Set / Unset / Has are already lock-free single-atomic
// operations against the bucket word.
type Bitmap struct {
	start    int64
	table    atomic.Pointer[tableBitmap]
	newTable atomic.Pointer[tableBitmap]
}

type bucketBitmap struct {
	used atomic.Uint64
}

type tableBitmap struct {
	buckets []unsafe.Pointer
}

// NewBitmap returns an empty Bitmap whose indexes are interpreted as
// offsets from start. The zero-value Bitmap is equally usable.
func NewBitmap(start int64) *Bitmap {
	return &Bitmap{start: start}
}

// bucket returns the bucket for index i (read path) and the
// normalized absolute index. b is nil if the bucket has not been
// allocated yet or i < s.start.
func (s *Bitmap) bucket(i int64) (b *bucketBitmap, index uint) {
	if i < s.start {
		return
	}
	index = uint(i - s.start)
	bucketIndex := index >> 6
	if table := s.table.Load(); table != nil {
		if bucketIndex < uint(len(table.buckets)) {
			b = (*bucketBitmap)(atomic.LoadPointer(&table.buckets[bucketIndex]))
		}
	}
	return
}

// bucketAlloc is the write-path bucket lookup: same as bucket but
// allocates the bucket (and grows the table) when missing. The
// growth protocol matches Store.bucketAlloc one-for-one (table /
// newTable CAS pair to serialize resizes).
func (s *Bitmap) bucketAlloc(i int64) (b *bucketBitmap, index uint) {
	if i < s.start {
		return
	}
	index = uint(i - s.start)
	bucketIndex := index >> 6

Retry:

	if table := s.table.Load(); table != nil {
		if bucketIndex < uint(len(table.buckets)) {
			b = (*bucketBitmap)(atomic.LoadPointer(&table.buckets[bucketIndex]))
		}

		if b == nil {
			if size := 1 << bits.Len64(uint64(bucketIndex)); size > len(table.buckets) {
				if newTable := s.newTable.Load(); newTable == table {
					newTable = &tableBitmap{buckets: make([]unsafe.Pointer, size)}
					if s.newTable.CompareAndSwap(table, newTable) {
						copy(newTable.buckets, table.buckets)
						if !s.table.CompareAndSwap(table, newTable) {
							// Invariant: see Store.bucketAlloc.
						}
						table = newTable
					} else {
						goto Retry
					}
				} else {
					goto Retry
				}
			}

			pb := &table.buckets[bucketIndex]
			if b = (*bucketBitmap)(atomic.LoadPointer(pb)); b == nil {
				if s.newTable.CompareAndSwap(table, nil) {
					b = &bucketBitmap{}
					if !atomic.CompareAndSwapPointer(pb, nil, unsafe.Pointer(b)) {
						b = (*bucketBitmap)(atomic.LoadPointer(pb))
					}
					if !s.newTable.CompareAndSwap(nil, table) {
						// Invariant: see Store.bucketAlloc.
					}
				} else {
					goto Retry
				}
			}
		}
	} else {
		size := 1 << bits.Len64(uint64(bucketIndex))
		if size < 32 {
			size = 32
		}
		newTable := &tableBitmap{buckets: make([]unsafe.Pointer, size)}
		if s.table.CompareAndSwap(nil, newTable) {
			pb := &newTable.buckets[bucketIndex]
			if b = (*bucketBitmap)(atomic.LoadPointer(pb)); b == nil {
				b = &bucketBitmap{}
				if !atomic.CompareAndSwapPointer(pb, nil, unsafe.Pointer(b)) {
					b = (*bucketBitmap)(atomic.LoadPointer(pb))
				}
			}
			if !s.newTable.CompareAndSwap(nil, newTable) {
				// Invariant: see Store.bucketAlloc.
			}
		} else {
			goto Retry
		}
	}

	return
}

// Grow ensures the bucket-pointer table has room for up to maxIndex
// (inclusive), without allocating any of the underlying bucket
// words themselves. Same semantics as Store.Grow but with a
// 64-bits-per-bucket shift. Calls with maxIndex < s.start are a
// no-op.
func (s *Bitmap) Grow(maxIndex int64) {
	if maxIndex < s.start {
		return
	}
	bucketIndex := uint(maxIndex-s.start) >> 6
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
			nt := &tableBitmap{buckets: make([]unsafe.Pointer, targetSize)}
			if s.table.CompareAndSwap(nil, nt) {
				if !s.newTable.CompareAndSwap(nil, nt) {
					// Invariant: see Store.Grow.
				}
				return
			}
			continue
		}
		if nt := s.newTable.Load(); nt == table {
			bigger := &tableBitmap{buckets: make([]unsafe.Pointer, targetSize)}
			if s.newTable.CompareAndSwap(table, bigger) {
				copy(bigger.buckets, table.buckets)
				if !s.table.CompareAndSwap(table, bigger) {
					// Invariant: see Store.Grow.
				}
				return
			}
		}
	}
}

// Has reports whether the bit at index i is set.
func (s *Bitmap) Has(i int64) bool {
	if b, idx := s.bucket(i); b != nil {
		return b.used.Load()&(uint64(1)<<(idx&63)) != 0
	}
	return false
}

// Set sets the bit at index i. Returns true if the bit transitioned
// from 0 to 1 (newly set), false if it was already 1.
func (s *Bitmap) Set(i int64) (added bool) {
	if b, idx := s.bucketAlloc(i); b != nil {
		mask := uint64(1) << (idx & 63)
		return b.used.Or(mask)&mask == 0
	}
	return false
}

// Unset clears the bit at index i. Returns true if the bit
// transitioned from 1 to 0, false if it was already 0 (or i < start).
func (s *Bitmap) Unset(i int64) (removed bool) {
	if b, idx := s.bucket(i); b != nil {
		mask := uint64(1) << (idx & 63)
		return b.used.And(^mask)&mask != 0
	}
	return false
}

// Toggle flips the bit at index i and returns the new state (true =
// set after the call). The bucket is allocated if missing.
//
// Implementation: a tiny CAS loop on the bucket word (atomic.Uint64
// has Or/And but not Xor as of Go 1.25). The retry path runs only
// on a lost CAS, which is rare in practice — a single concurrent
// Set/Unset/Toggle on this 64-bit window has to win the same word.
func (s *Bitmap) Toggle(i int64) (nowSet bool) {
	if b, idx := s.bucketAlloc(i); b != nil {
		mask := uint64(1) << (idx & 63)
		for {
			cur := b.used.Load()
			if b.used.CompareAndSwap(cur, cur^mask) {
				return cur&mask == 0
			}
		}
	}
	return false
}

// Len returns the live bit count (popcount over every allocated
// bucket word). The result is a snapshot: concurrent Set / Unset
// during the scan may or may not be reflected.
func (s *Bitmap) Len() (count int) {
	if table := s.table.Load(); table != nil {
		for i := range table.buckets {
			if b := (*bucketBitmap)(atomic.LoadPointer(&table.buckets[i])); b != nil {
				count += bits.OnesCount64(b.used.Load())
			}
		}
	}
	return
}

// Range calls f sequentially for each index whose bit is currently
// set, in ascending order. Iteration stops if f returns false.
// Iteration is weakly consistent: concurrent Set / Unset during the
// scan may or may not be observed.
func (s *Bitmap) Range(f func(i int64) bool) {
	table := s.table.Load()
	if table == nil {
		return
	}
	for bIdx := range table.buckets {
		b := (*bucketBitmap)(atomic.LoadPointer(&table.buckets[bIdx]))
		if b == nil {
			continue
		}
		w := b.used.Load()
		base := s.start + int64(bIdx)<<6
		for w != 0 {
			j := bits.TrailingZeros64(w)
			if !f(base + int64(j)) {
				return
			}
			w &= w - 1
		}
	}
}

// Clear unsets every bit. Buckets remain allocated (the underlying
// table is not shrunk).
func (s *Bitmap) Clear() {
	if table := s.table.Load(); table != nil {
		for i := range table.buckets {
			if b := (*bucketBitmap)(atomic.LoadPointer(&table.buckets[i])); b != nil {
				b.used.Store(0)
			}
		}
	}
}
