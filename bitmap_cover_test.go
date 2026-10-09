package fsync

import (
	"sync"
	"testing"
	"time"
)

// Coverage tests for Bitmap: table growth on Set beyond the initial 32
// buckets, the retry branches taken while another goroutine owns the
// growth token, Grow on every starting state, Range on an empty bitmap.

const bitsPerBucket = 1 << bitsPerBitmapBucketShift // 512

func TestBitmapSetGrowsTable(t *testing.T) {
	var b Bitmap
	b.Set(0)
	far := int64(100 * bitsPerBucket) // bucket 100, beyond the initial 32
	if !b.Set(far) || !b.Has(far) || !b.Has(0) {
		t.Fatalf(`Set beyond the initial table must grow it and keep old bits`)
	}
	if n := len(b.table.Load().buckets); n < 101 {
		t.Errorf(`table has %d buckets, want ≥ 101`, n)
	}
	if b.Len() != 2 {
		t.Errorf(`Len() = %d, want 2`, b.Len())
	}
}

// While another goroutine holds the growth / allocation token (newTable
// not pointing to the current table), Set retries until it is released.
func TestBitmapSetWaitsForGrowthToken(t *testing.T) {
	for _, tc := range []struct {
		name string
		i    int64
	}{
		{"grow", int64(200 * bitsPerBucket)}, // needs a bigger table
		{"alloc", int64(5 * bitsPerBucket)},  // needs a new bucket in place
	} {
		var b Bitmap
		b.Set(0)
		table := b.table.Load()
		b.newTable.Store(nil) // token taken by "someone else"
		done := make(chan struct{})
		go func() { b.Set(tc.i); close(done) }()
		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			t.Fatalf(`%s: Set completed while the token was held`, tc.name)
		default:
		}
		b.newTable.Store(table) // token released
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf(`%s: Set stuck after the token was released`, tc.name)
		}
		if !b.Has(tc.i) || !b.Has(0) {
			t.Fatalf(`%s: bits lost`, tc.name)
		}
	}
}

func TestBitmapGrowAllStates(t *testing.T) {
	var b Bitmap
	b.Grow(10) // zero value: small target is rounded up to 32 buckets
	if n := len(b.table.Load().buckets); n != 32 {
		t.Errorf(`Grow(10) on empty bitmap: %d buckets, want 32`, n)
	}
	b.Set(3)
	b.Grow(int64(1000 * bitsPerBucket)) // bigger: copies existing buckets
	if n := len(b.table.Load().buckets); n < 1001 || !b.Has(3) {
		t.Errorf(`Grow bigger: %d buckets, Has(3)=%v`, n, b.Has(3))
	}
	before := b.table.Load()
	b.Grow(5) // already large enough: no-op
	if b.table.Load() != before {
		t.Errorf(`Grow to a smaller size must be a no-op`)
	}
	s := NewBitmap(100)
	s.Grow(5) // maxIndex < start: no-op
	if s.table.Load() != nil {
		t.Errorf(`Grow below start must not allocate`)
	}
}

func TestBitmapRangeEmpty(t *testing.T) {
	var b Bitmap
	calls := 0
	b.Range(func(int64) bool { calls++; return true })
	if calls != 0 {
		t.Errorf(`Range on an empty bitmap called f %d times`, calls)
	}
}

// Many goroutines grow the same bitmap concurrently from its zero value
// (Grow and Set racing for the table / token CAS pairs).
func TestBitmapConcurrentGrowth(t *testing.T) {
	for round := 0; round < 50; round++ {
		var b Bitmap
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				if w%2 == 0 {
					b.Grow(int64((w + 1) * 64 * bitsPerBucket))
				}
				for k := 0; k < 16; k++ {
					b.Set(int64((w*16+k)*37*bitsPerBucket + w))
				}
			}(w)
		}
		wg.Wait()
		for w := 0; w < 8; w++ {
			for k := 0; k < 16; k++ {
				if i := int64((w*16+k)*37*bitsPerBucket + w); !b.Has(i) {
					t.Fatalf(`round %d: bit %d lost under concurrent growth`, round, i)
				}
			}
		}
	}
}
