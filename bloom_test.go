package fsync

import (
	"math"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBloomBasic(t *testing.T) {
	var b Bloom[uint64] // zero value usable

	if b.Contains(42) {
		t.Errorf(`Contains on empty Bloom should be false`)
	}
	if b.Saturation() != 0 || b.EstimatedLen() != 0 {
		t.Errorf(`empty Bloom: Saturation=%v EstimatedLen=%d, want 0`, b.Saturation(), b.EstimatedLen())
	}
	if !b.Add(42) {
		t.Errorf(`first Add(42) should return true`)
	}
	if b.Add(42) {
		t.Errorf(`second Add(42) should return false`)
	}
	if !b.Contains(42) {
		t.Errorf(`Contains(42) should be true after Add`)
	}
	if b.Cap() != bloomDefaultItems {
		t.Errorf(`zero-value Cap() = %d, want %d`, b.Cap(), bloomDefaultItems)
	}

	b.Reset()
	if b.Contains(42) {
		t.Errorf(`Contains(42) should be false after Reset`)
	}
	if !b.Add(42) {
		t.Errorf(`Add(42) after Reset should return true`)
	}
}

func TestBloomNoFalseNegativeAndFPRate(t *testing.T) {
	for _, tc := range []struct {
		n  int
		fp float64
	}{{1000, 0.01}, {50_000, 0.01}, {50_000, 0.001}} {
		b := NewBloom[uint64](tc.n, tc.fp)
		for i := 0; i < tc.n; i++ {
			b.Add(hashUint64(7, uint64(i)))
		}
		for i := 0; i < tc.n; i++ {
			if !b.Contains(hashUint64(7, uint64(i))) {
				t.Fatalf(`n=%d: false negative on key %d`, tc.n, i)
			}
		}
		const probes = 200_000
		fps := 0
		for i := 0; i < probes; i++ {
			if b.Contains(hashUint64(7, uint64(tc.n+1+i))) {
				fps++
			}
		}
		rate := float64(fps) / probes
		// allow 50 % over the target: sampling noise and model rounding
		if rate > tc.fp*1.5 {
			t.Errorf(`n=%d target %.3f%%: measured FP %.3f%% (%d words, k=%d)`,
				tc.n, tc.fp*100, rate*100, b.Bytes()/8, b.table.Load().k)
		}
		t.Logf(`n=%d target %.3f%%: FP %.3f%%, %.2f bytes/key, k=%d, saturation %.2f, estimated len %d`,
			tc.n, tc.fp*100, rate*100, float64(b.Bytes())/float64(tc.n), b.table.Load().k,
			b.Saturation(), b.EstimatedLen())
	}
}

func TestBloomEstimatedLen(t *testing.T) {
	const n = 20_000
	b := NewBloom[int](n, 0.01)
	for i := 0; i < n; i++ {
		b.Add(i)
	}
	if got := b.EstimatedLen(); math.Abs(float64(got-n)) > n*0.05 {
		t.Errorf(`EstimatedLen() = %d, want %d ± 5 %%`, got, n)
	}
	if s := b.Saturation(); s <= 0 || s >= 0.6 {
		t.Errorf(`Saturation() = %.3f at capacity, want within (0, 0.6)`, s)
	}
}

func TestBloomStringKeys(t *testing.T) {
	b := NewBloom[string](1000, 0.01)
	for i := 0; i < 1000; i++ {
		b.Add("/produit/" + strconv.Itoa(i))
	}
	for i := 0; i < 1000; i++ {
		if !b.Contains("/produit/" + strconv.Itoa(i)) {
			t.Fatalf(`false negative on string key %d`, i)
		}
	}
}

func TestBloomSizingMonotonic(t *testing.T) {
	// a stricter rate or more items never yields a smaller filter
	small := NewBloom[int](10_000, 0.01).Bytes()
	if NewBloom[int](10_000, 0.001).Bytes() <= small {
		t.Errorf(`fp 0.1%% should need more memory than fp 1%%`)
	}
	if NewBloom[int](20_000, 0.01).Bytes() <= small {
		t.Errorf(`20k items should need more memory than 10k`)
	}
}

// Concurrent Adds of the same keys: exactly one Add per key may
// report added=true (word-blocked layout, single atomic OR), and no
// key may be lost.
func TestBloomConcurrentAddOncePerKey(t *testing.T) {
	const (
		keys    = 20_000
		workers = 8
	)
	b := NewBloom[uint64](keys, 0.001)
	var firsts [keys]atomic.Int32
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < keys; i++ {
				k := (i + w*keys/workers) % keys // each worker starts elsewhere
				if b.Add(uint64(k)) {
					firsts[k].Add(1)
				}
				if i%1024 == 0 {
					runtime.Gosched()
				}
			}
		}(w)
	}
	wg.Wait()

	falsePos := 0
	for k := 0; k < keys; k++ {
		switch firsts[k].Load() {
		case 0:
			falsePos++ // its bits were already set by other keys
		case 1:
		default:
			t.Fatalf(`key %d: %d Adds returned true, want at most 1`, k, firsts[k].Load())
		}
		if !b.Contains(uint64(k)) {
			t.Fatalf(`false negative on key %d after concurrent Adds`, k)
		}
	}
	if falsePos > keys/100 {
		t.Errorf(`%d keys never reported as new (> 1 %%)`, falsePos)
	}
}

func TestBloomConcurrentResetNoPanic(t *testing.T) {
	b := NewBloom[int](1000, 0.01)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				b.Add(i)
				b.Contains(i)
			}
		}()
	}
	for i := 0; i < 100; i++ {
		b.Reset()
		runtime.Gosched()
	}
	close(stop)
	wg.Wait()
	if b.Cap() != 1000 {
		t.Errorf(`Reset must keep the capacity: Cap() = %d`, b.Cap())
	}
}

func TestBloomKeyTypes(t *testing.T) {
	type pair struct{ a, b int }
	b64 := NewBloom[int64](500, 0.01)
	bu := NewBloom[uint](500, 0.01)
	b32 := NewBloom[uint32](500, 0.01)
	bp := NewBloom[uintptr](500, 0.01)
	bs := NewBloom[pair](500, 0.01) // default branch: maphash.Comparable
	for i := 0; i < 500; i++ {
		b64.Add(int64(i))
		bu.Add(uint(i))
		b32.Add(uint32(i))
		bp.Add(uintptr(i))
		bs.Add(pair{i, -i})
	}
	for i := 0; i < 500; i++ {
		if !b64.Contains(int64(i)) || !bu.Contains(uint(i)) || !b32.Contains(uint32(i)) ||
			!bp.Contains(uintptr(i)) || !bs.Contains(pair{i, -i}) {
			t.Fatalf(`false negative on key %d in a typed filter`, i)
		}
	}
}

func TestBloomEdgeSizes(t *testing.T) {
	var z Bloom[int] // zero value, before any Add
	if z.Cap() != bloomDefaultItems || z.Bytes() != 0 {
		t.Errorf(`zero value: Cap=%d Bytes=%d, want %d and 0`, z.Cap(), z.Bytes(), bloomDefaultItems)
	}
	tiny := NewBloom[int](0, 0.01) // expectedItems < 1 is treated as 1
	if tiny.Cap() != 1 || tiny.Bytes() < 8 {
		t.Errorf(`NewBloom(0): Cap=%d Bytes=%d`, tiny.Cap(), tiny.Bytes())
	}
	if !tiny.Add(7) || !tiny.Contains(7) {
		t.Errorf(`a 1-key filter must hold its key`)
	}
	// fpRate is clamped to [1e-6, 0.5]
	if loose, strict := NewBloom[int](1000, 0.9).Bytes(), NewBloom[int](1000, 0).Bytes(); loose >= strict {
		t.Errorf(`clamped rates: fp 0.5 filter (%d B) should be smaller than fp 1e-6 (%d B)`, loose, strict)
	}
}

// Far beyond capacity, words saturate (64 bits set): EstimatedLen stays
// finite and Saturation reports the overload.
func TestBloomOverCapacity(t *testing.T) {
	b := NewBloom[int](10, 0.5)
	for i := 0; i < 5000; i++ {
		b.Add(i)
	}
	if s := b.Saturation(); s < 0.9 {
		t.Errorf(`Saturation() = %.2f far over capacity, want ≥ 0.9`, s)
	}
	if n := b.EstimatedLen(); n <= 0 {
		t.Errorf(`EstimatedLen() = %d on a saturated filter, want a positive finite estimate`, n)
	}
}

// Concurrent first Adds on a zero value race to allocate the table:
// exactly one table wins and no key is lost.
func TestBloomZeroValueConcurrentInit(t *testing.T) {
	for round := 0; round < 100; round++ {
		var b Bloom[int]
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				b.Add(w)
			}(w)
		}
		wg.Wait()
		for w := 0; w < 8; w++ {
			if !b.Contains(w) {
				t.Fatalf(`round %d: key %d lost in concurrent first Adds`, round, w)
			}
		}
	}
}
