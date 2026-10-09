package fsync

import (
	"hash/maphash"
	"math"
	"math/bits"
	"sync/atomic"
)

// Bloom is a lock-free concurrent Bloom filter of fixed capacity.
//
// Layout: "word-blocked" — every bit of a key lives in ONE 64-bit
// word, chosen by the key's hash. Add is therefore a single atomic OR
// and Contains a single atomic Load: one cache line touched per call,
// no lock, no allocation. It costs ~20 % more memory than a classic
// (unblocked) filter for the same false-positive rate, and buys an
// exact concurrent contract: among concurrent Adds of the same key,
// at most one returns added=true (the OR is atomic on the whole key
// mask), so Add doubles as a "first time we see this?" test.
//
// As any Bloom filter: Contains has no false negatives (a bit once
// set is never cleared outside Reset) and a bounded false-positive
// rate while the filter stays within its capacity. A false positive
// makes Add return false for a key never added. The filter cannot
// grow: size it with NewBloom(expectedItems, fpRate), watch
// Saturation / EstimatedLen, and Reset it when its period ends.
//
// The zero value is usable: the first Add lazily allocates a filter
// sized for bloomDefaultItems keys at 1 % false positives. Prefer
// NewBloom with the real expected count.
type Bloom[K comparable] struct {
	table atomic.Pointer[tableBloom]
}

type tableBloom struct {
	words    []atomic.Uint64
	k        uint32 // bits set per key, all within one word
	capacity int    // expected items this table was sized for
	fpRate   float64
}

const (
	bloomDefaultItems = 4096
	bloomDefaultFP    = 0.01
	bloomMaxK         = 10 // 10 × 6-bit positions fit in one 64-bit hash
	bloomSeed2        = 0xC2B2AE3D27D4EB4F
)

// NewBloom returns a Bloom filter sized so that, with expectedItems
// keys inserted, the false-positive rate stays at or below fpRate.
// expectedItems < 1 is treated as 1; fpRate is clamped to
// [1e-6, 0.5].
func NewBloom[K comparable](expectedItems int, fpRate float64) *Bloom[K] {
	b := &Bloom[K]{}
	b.table.Store(newTableBloom(expectedItems, fpRate))
	return b
}

// newTableBloom picks the smallest word count (and the best k for it)
// meeting fpRate at expectedItems, using the word-blocked model.
func newTableBloom(n int, p float64) *tableBloom {
	if n < 1 {
		n = 1
	}
	p = math.Max(1e-6, math.Min(0.5, p))
	// Start from the classic optimum (−ln p / ln²2 bits per key) and
	// grow by 1/16 until the blocked model meets the target.
	words := int(math.Ceil(float64(n) * -math.Log(p) / (math.Ln2 * math.Ln2) / 64))
	if words < 1 {
		words = 1
	}
	for {
		k, fp := bloomBestK(n, words)
		if fp <= p {
			return &tableBloom{words: make([]atomic.Uint64, words), k: k, capacity: n, fpRate: p}
		}
		words += words/16 + 1
	}
}

// bloomBestK returns the k (1..bloomMaxK) minimizing the word-blocked
// false-positive rate for n keys in `words` words, and that rate.
func bloomBestK(n, words int) (uint32, float64) {
	best, bestFP := uint32(1), 1.0
	for k := uint32(1); k <= bloomMaxK; k++ {
		if fp := bloomBlockedFP(float64(n)/float64(words), k); fp < bestFP {
			best, bestFP = k, fp
		}
	}
	return best, bestFP
}

// bloomBlockedFP is the false-positive rate of a word-blocked filter
// whose words receive λ keys on average (Poisson), k bits per key out
// of 64: Σ_j P(j; λ) · (1 − (63/64)^{k·j})^k.
func bloomBlockedFP(lambda float64, k uint32) float64 {
	kf := float64(k)
	limit := int(lambda + 10*math.Sqrt(lambda) + 20)
	fp, pj := 0.0, math.Exp(-lambda) // P(0)
	for j := 0; j <= limit; j++ {
		if j > 0 {
			pj *= lambda / float64(j)
		}
		fill := 1 - math.Pow(63.0/64.0, kf*float64(j))
		fp += pj * math.Pow(fill, kf)
	}
	return fp
}

func (b *Bloom[K]) loadOrInitTable() *tableBloom {
	if t := b.table.Load(); t != nil {
		return t
	}
	nt := newTableBloom(bloomDefaultItems, bloomDefaultFP)
	if b.table.CompareAndSwap(nil, nt) {
		return nt
	}
	return b.table.Load()
}

// hash mirrors Set.hash: wyhash on integer keys, maphash on strings,
// maphash.Comparable on everything else.
func (b *Bloom[K]) hash(key K) uint64 {
	switch v := any(key).(type) {
	case int:
		return hashUint64(0, uint64(v))
	case int64:
		return hashUint64(0, uint64(v))
	case uint64:
		return hashUint64(0, v)
	case uint:
		return hashUint64(0, uint64(v))
	case uint32:
		return hashUint64(0, uint64(v))
	case uintptr:
		return hashUint64(0, uint64(v))
	case string:
		return maphash.String(mapSeed, v)
	default:
		return maphash.Comparable(mapSeed, key)
	}
}

// locate returns the word a key maps to and its k-bit mask. The word
// index uses the high half of h (Lemire's fastrange, so any word
// count works, not only powers of two); the k bit positions come
// from 6-bit slices of a second, independent mix of h.
func (t *tableBloom) locate(h uint64) (*atomic.Uint64, uint64) {
	idx, _ := bits.Mul64(h, uint64(len(t.words)))
	h2 := hashUint64(bloomSeed2, h)
	var mask uint64
	for i := uint32(0); i < t.k; i++ {
		mask |= uint64(1) << (h2 & 63)
		h2 >>= 6
	}
	return &t.words[idx], mask
}

// Add inserts key. It returns true if the key was (probably) absent —
// at least one of its bits was still clear — and false if it was
// already present or is a false positive. Among concurrent Adds of
// the same key, at most one returns true.
func (b *Bloom[K]) Add(key K) (added bool) {
	w, mask := b.loadOrInitTable().locate(b.hash(key))
	return w.Or(mask)&mask != mask
}

// Contains reports whether key may have been added: false means
// certainly absent, true means present or a false positive.
func (b *Bloom[K]) Contains(key K) bool {
	t := b.table.Load()
	if t == nil {
		return false
	}
	w, mask := t.locate(b.hash(key))
	return w.Load()&mask == mask
}

// Reset empties the filter by swapping in a fresh table of the same
// size: an Add racing with Reset lands either in the old table
// (discarded) or the new one, never half in each.
func (b *Bloom[K]) Reset() {
	if t := b.table.Load(); t != nil {
		b.table.Store(newTableBloom(t.capacity, t.fpRate))
	}
}

// Saturation returns the fraction of bits set (0..1). Past roughly
// 0.5 the filter is beyond the capacity it was sized for and its
// false-positive rate climbs: time to Reset or size up.
func (b *Bloom[K]) Saturation() float64 {
	t := b.table.Load()
	if t == nil {
		return 0
	}
	return float64(t.setBits()) / float64(len(t.words)*64)
}

// EstimatedLen estimates the number of distinct keys added, from the
// fill of each word (inverting the expected fill (1 − (63/64)^{k·j})
// per word). Approximate; exact counting is not a Bloom filter's job.
func (b *Bloom[K]) EstimatedLen() int {
	t := b.table.Load()
	if t == nil {
		return 0
	}
	perKey := float64(t.k) * -math.Log1p(-1.0/64)
	n := 0.0
	for i := range t.words {
		set := bits.OnesCount64(t.words[i].Load())
		if set == 64 {
			set = 63 // a saturated word carries no more information
		}
		n += -math.Log1p(-float64(set)/64) / perKey
	}
	return int(math.Round(n))
}

// Cap returns the number of keys the filter was sized for.
func (b *Bloom[K]) Cap() int {
	if t := b.table.Load(); t != nil {
		return t.capacity
	}
	return bloomDefaultItems
}

// Bytes returns the memory held by the bit array.
func (b *Bloom[K]) Bytes() int {
	if t := b.table.Load(); t != nil {
		return len(t.words) * 8
	}
	return 0
}

func (t *tableBloom) setBits() int {
	n := 0
	for i := range t.words {
		n += bits.OnesCount64(t.words[i].Load())
	}
	return n
}
