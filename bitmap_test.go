package fsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestBitmapBasic(t *testing.T) {
	var b Bitmap

	if b.Len() != 0 {
		t.Errorf(`empty Bitmap must have Len()=0, got %d`, b.Len())
	}
	if b.Has(0) {
		t.Errorf(`Has on empty Bitmap should be false`)
	}
	if b.Unset(0) {
		t.Errorf(`Unset on empty Bitmap should be false`)
	}

	if !b.Set(5) {
		t.Errorf(`first Set(5) should return true (transition 0→1)`)
	}
	if b.Set(5) {
		t.Errorf(`second Set(5) should return false (was already 1)`)
	}
	if !b.Has(5) {
		t.Errorf(`Has(5) should be true after Set`)
	}
	if b.Len() != 1 {
		t.Errorf(`Len() should be 1, got %d`, b.Len())
	}

	if !b.Unset(5) {
		t.Errorf(`Unset(5) should return true`)
	}
	if b.Unset(5) {
		t.Errorf(`Unset(5) again should return false`)
	}
	if b.Has(5) {
		t.Errorf(`Has(5) should be false after Unset`)
	}
	if b.Len() != 0 {
		t.Errorf(`Len() should be 0 after Unset, got %d`, b.Len())
	}
}

func TestBitmapStartOffset(t *testing.T) {
	b := NewBitmap(1000)

	// Below start is a no-op on every op.
	if b.Set(500) {
		t.Errorf(`Set below start should return false`)
	}
	if b.Has(500) {
		t.Errorf(`Has below start should be false`)
	}
	if b.Unset(500) {
		t.Errorf(`Unset below start should be false`)
	}
	if b.Toggle(500) {
		t.Errorf(`Toggle below start should return false`)
	}
	if b.Len() != 0 {
		t.Errorf(`no allocation should happen below start; Len=%d`, b.Len())
	}

	// Indexes ≥ start work normally.
	if !b.Set(1000) {
		t.Errorf(`Set(1000) on bitmap starting at 1000 should return true`)
	}
	if !b.Set(1500) {
		t.Errorf(`Set(1500) should return true`)
	}
	if !b.Has(1000) || !b.Has(1500) {
		t.Errorf(`Has must see Set bits at start and start+offset`)
	}
	if b.Has(999) {
		t.Errorf(`Has(999) (below start) must still be false`)
	}
	if b.Len() != 2 {
		t.Errorf(`Len() should be 2, got %d`, b.Len())
	}
}

func TestBitmapCrossBucket(t *testing.T) {
	// Indexes 0 and 64 are in different buckets (>>6 differs).
	var b Bitmap
	b.Set(0)
	b.Set(63)
	b.Set(64) // 2nd bucket
	b.Set(127)
	b.Set(128) // 3rd bucket
	if l := b.Len(); l != 5 {
		t.Errorf(`Len should be 5, got %d`, l)
	}
	for _, i := range []int64{0, 63, 64, 127, 128} {
		if !b.Has(i) {
			t.Errorf(`Has(%d) should be true`, i)
		}
	}
	for _, i := range []int64{1, 62, 65, 126, 129} {
		if b.Has(i) {
			t.Errorf(`Has(%d) should be false`, i)
		}
	}
}

func TestBitmapToggle(t *testing.T) {
	var b Bitmap

	if !b.Toggle(10) {
		t.Errorf(`Toggle on cleared bit should return true (now set)`)
	}
	if !b.Has(10) {
		t.Errorf(`Has(10) should be true after Toggle`)
	}
	if b.Toggle(10) {
		t.Errorf(`Toggle on set bit should return false (now cleared)`)
	}
	if b.Has(10) {
		t.Errorf(`Has(10) should be false after second Toggle`)
	}
}

func TestBitmapRange(t *testing.T) {
	b := NewBitmap(100)
	indexes := []int64{100, 200, 300, 400, 500}
	for _, i := range indexes {
		b.Set(i)
	}
	seen := make(map[int64]bool)
	b.Range(func(i int64) bool {
		seen[i] = true
		return true
	})
	if len(seen) != len(indexes) {
		t.Errorf(`Range should visit all %d indexes, saw %d`, len(indexes), len(seen))
	}
	for _, i := range indexes {
		if !seen[i] {
			t.Errorf(`Range missed index %d`, i)
		}
	}

	// Ascending order check.
	var prev int64 = -1
	b.Range(func(i int64) bool {
		if i <= prev {
			t.Errorf(`Range must be ascending; saw %d after %d`, i, prev)
		}
		prev = i
		return true
	})

	// Early termination.
	count := 0
	b.Range(func(_ int64) bool {
		count++
		return count < 3
	})
	if count != 3 {
		t.Errorf(`Range should stop after f returns false, saw %d calls`, count)
	}
}

func TestBitmapClear(t *testing.T) {
	var b Bitmap
	for i := int64(0); i < 200; i++ {
		b.Set(i)
	}
	b.Clear()
	if b.Len() != 0 {
		t.Errorf(`Len after Clear should be 0, got %d`, b.Len())
	}
	for i := int64(0); i < 200; i++ {
		if b.Has(i) {
			t.Errorf(`Has(%d) after Clear should be false`, i)
			break
		}
	}
	// Bitmap must remain usable.
	b.Set(42)
	if !b.Has(42) {
		t.Errorf(`Set after Clear must work`)
	}
}

func TestBitmapGrow(t *testing.T) {
	var b Bitmap
	// Force a tiny initial table.
	b.Set(0)

	// Grow to a much higher max — must resize the table in place.
	b.Grow(64 * 256)
	if !b.Has(0) {
		t.Errorf(`existing bit must survive Grow`)
	}
	b.Set(64 * 200)
	if !b.Has(64 * 200) {
		t.Errorf(`Set after Grow must work at high index`)
	}

	// Grow below start is a no-op.
	b2 := NewBitmap(1000)
	b2.Grow(500)
	if b2.Len() != 0 {
		t.Errorf(`Grow below start must not allocate; Len=%d`, b2.Len())
	}

	// Grow from nil table directly to large size.
	var b3 Bitmap
	b3.Grow(64 * 256)
	b3.Set(64 * 100)
	if !b3.Has(64 * 100) {
		t.Errorf(`Grow on nil table then Set/Has must work`)
	}
}

func TestBigBitmap(t *testing.T) {
	var b Bitmap
	var w sync.WaitGroup
	cpus := int64(runtime.GOMAXPROCS(0))
	count := int64(100000)

	for g := range cpus {
		w.Add(1)
		go func() {
			defer w.Done()
			for j := range count {
				b.Set(g*count + j)
			}
		}()
	}
	w.Wait()

	if l := int64(b.Len()); l != count*cpus {
		t.Errorf(`concurrent Set: expected Len()=%d, got %d`, count*cpus, l)
	}
	for i := int64(0); i < count*cpus; i++ {
		if !b.Has(i) {
			t.Errorf(`missing bit %d after concurrent Set`, i)
			break
		}
	}
}

func TestBitmapConcurrentSameBit(t *testing.T) {
	// All goroutines race on the same bit; exactly one Set must
	// observe added=true.
	var b Bitmap
	const goroutines = 64
	var addedTrue atomic.Int64
	var w sync.WaitGroup

	for range goroutines {
		w.Add(1)
		go func() {
			defer w.Done()
			if b.Set(42) {
				addedTrue.Add(1)
			}
		}()
	}
	w.Wait()

	if addedTrue.Load() != 1 {
		t.Errorf(`exactly one concurrent Set should report added=true, got %d`, addedTrue.Load())
	}
	if !b.Has(42) {
		t.Errorf(`bit 42 should be set after race`)
	}
}
