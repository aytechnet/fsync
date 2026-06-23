package fsync

import (
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// Bitmap is a lock-free concurrent bit set indexed by int64. Each
// bucket packs **8 atomic.Uint64 words = 512 bits = exactly one
// cacheline** (64 bytes). Compared to a one-word-per-bucket layout,
// this consolidates 8× fewer heap objects for the same index range,
// which both lightens GC pressure (fewer pointers in the bucket
// table, fewer objects to scan) and improves locality (neighbouring
// bits share the same cacheline). The per-bit memory cost stays
// around 0.13 byte at steady state; by comparison Store[bool] would
// carry ~1.25 bytes per entry, so Bitmap is roughly **10× smaller**.
//
// Indexes are interpreted as offsets from `start` (Bitmap.Set(i)
// targets absolute slot `i - start`); calls with `i < start` are
// no-ops, mirroring Store. The zero value is usable (start defaults
// to 0).
//
// Bitmap has no Lock(*V) API: a pointer to a single bit does not
// exist, and Set / Unset / Has / Toggle are already lock-free
// single-atomic operations against one of the bucket's 8 words.
type Bitmap struct {
	start    int64
	table    atomic.Pointer[tableBitmap]
	newTable atomic.Pointer[tableBitmap]
}

// bucketBitmap packs 8 atomic.Uint64 words (512 bits) into a single
// heap object. The 8 words also fit a single 64-byte cacheline on
// every mainstream CPU.
type bucketBitmap struct {
	words [8]atomic.Uint64
}

type tableBitmap struct {
	buckets []unsafe.Pointer
}

// bitsPerBitmapBucket is 512 (8 words × 64 bits). It is the unit
// the bucket-table shift uses (>> 9). The intra-bucket selectors
// derive from the low 9 bits of the normalized index: bits [6..8]
// pick the word (0..7), bits [0..5] pick the bit (0..63).
const bitsPerBitmapBucketShift = 9

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
	bucketIndex := index >> bitsPerBitmapBucketShift
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
	bucketIndex := index >> bitsPerBitmapBucketShift

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
// 512-bits-per-bucket shift (8 words × 64 bits). Calls with
// maxIndex < s.start are a no-op.
//
// Returns the receiver, so calls can be chained.
func (s *Bitmap) Grow(maxIndex int64) *Bitmap {
	if maxIndex < s.start {
		return s
	}
	bucketIndex := uint(maxIndex-s.start) >> bitsPerBitmapBucketShift
	targetSize := 1 << bits.Len64(uint64(bucketIndex))
	if targetSize < 32 {
		targetSize = 32
	}

	for {
		table := s.table.Load()
		if table != nil && len(table.buckets) >= targetSize {
			return s
		}
		if table == nil {
			nt := &tableBitmap{buckets: make([]unsafe.Pointer, targetSize)}
			if s.table.CompareAndSwap(nil, nt) {
				if !s.newTable.CompareAndSwap(nil, nt) {
					// Invariant: see Store.Grow.
				}
				return s
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
				return s
			}
		}
	}
}

// Has reports whether the bit at index i is set.
func (s *Bitmap) Has(i int64) bool {
	if b, idx := s.bucket(i); b != nil {
		return b.words[(idx>>6)&7].Load()&(uint64(1)<<(idx&63)) != 0
	}
	return false
}

// Set sets the bit at index i. Returns true if the bit transitioned
// from 0 to 1 (newly set), false if it was already 1.
func (s *Bitmap) Set(i int64) (added bool) {
	if b, idx := s.bucketAlloc(i); b != nil {
		mask := uint64(1) << (idx & 63)
		return b.words[(idx>>6)&7].Or(mask)&mask == 0
	}
	return false
}

// Unset clears the bit at index i. Returns true if the bit
// transitioned from 1 to 0, false if it was already 0 (or i < start).
func (s *Bitmap) Unset(i int64) (removed bool) {
	if b, idx := s.bucket(i); b != nil {
		mask := uint64(1) << (idx & 63)
		return b.words[(idx>>6)&7].And(^mask)&mask != 0
	}
	return false
}

// Toggle flips the bit at index i and returns the new state (true =
// set after the call). The bucket is allocated if missing.
//
// Implementation: a tiny CAS loop on the target word (atomic.Uint64
// has Or/And but not Xor as of Go 1.25). The retry path runs only
// on a lost CAS, which is rare in practice — a single concurrent
// Set/Unset/Toggle on the same word has to win the race.
func (s *Bitmap) Toggle(i int64) (nowSet bool) {
	if b, idx := s.bucketAlloc(i); b != nil {
		w := &b.words[(idx>>6)&7]
		mask := uint64(1) << (idx & 63)
		for {
			cur := w.Load()
			if w.CompareAndSwap(cur, cur^mask) {
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
				for w := range b.words {
					count += bits.OnesCount64(b.words[w].Load())
				}
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
		bucketBase := s.start + int64(bIdx)<<bitsPerBitmapBucketShift
		for w := range b.words {
			word := b.words[w].Load()
			wordBase := bucketBase + int64(w)<<6
			for word != 0 {
				j := bits.TrailingZeros64(word)
				if !f(wordBase + int64(j)) {
					return
				}
				word &= word - 1
			}
		}
	}
}

// Clear unsets every bit. Buckets remain allocated (the underlying
// table is not shrunk).
func (s *Bitmap) Clear() {
	if table := s.table.Load(); table != nil {
		for i := range table.buckets {
			if b := (*bucketBitmap)(atomic.LoadPointer(&table.buckets[i])); b != nil {
				for w := range b.words {
					b.words[w].Store(0)
				}
			}
		}
	}
}
