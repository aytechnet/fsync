package fsync

import (
	"testing"

	"runtime"
	"sync"
	"sync/atomic"
)

func TestNewStore(t *testing.T) {
	s := NewStore[string](1)

	if s.Len() != 0 {
		t.Errorf(`Empty new store must have a size of 0`)
	}

	s.Store(1, "one")
	if s.Len() != 1 {
		t.Errorf(`Store size must be incremented on storing, size = %d`, s.Len())
	}

	s.Store(1, "un")
	if s.Len() != 1 {
		t.Errorf(`Store size must be incremented on storing, size = %d`, s.Len())
	}

	if !s.Delete(1) {
		t.Errorf(`Deleting an existing entry from a store must be true`)
	}
	if s.Len() != 0 {
		t.Errorf(`Emptied store must have a size of 0, size = %d`, s.Len())
	}

	s.Store(100, "hundred")
	if s.Len() != 1 {
		t.Errorf(`Store size must be incremented on storing, size = %d`, s.Len())
	}

	s.Store(1000, "thousand")
	if s.Len() != 2 {
		t.Errorf(`Store size must be incremented on storing, size = %d`, s.Len())
	}

	if v, ok := s.Load(99); ok {
		t.Errorf(`Store should not have an element at 99, value = %s`, v)
	}

	if _, ok := s.Load(100); !ok {
		t.Errorf(`Store should have an element at 100`)
	}
}

func TestNewMutexStore(t *testing.T) {
	s := NewMutexStore[string](1)

	if s.Len() != 0 {
		t.Errorf(`Empty new store must have a size of 0`)
	}

	s.Store(1, "one")
	if s.Len() != 1 {
		t.Errorf(`Store size must be incremented on storing, size = %d`, s.Len())
	}

	s.Store(1, "un")
	if s.Len() != 1 {
		t.Errorf(`Store size must be incremented on storing, size = %d`, s.Len())
	}

	if !s.Delete(1) {
		t.Errorf(`Deleting an existing entry from a store must be true`)
	}
	if s.Len() != 0 {
		t.Errorf(`Emptied store must have a size of 0, size = %d`, s.Len())
	}

	s.Store(100, "hundred")
	if s.Len() != 1 {
		t.Errorf(`Store size must be incremented on storing, size = %d`, s.Len())
	}

	s.Store(1000, "thousand")
	if s.Len() != 2 {
		t.Errorf(`Store size must be incremented on storing, size = %d`, s.Len())
	}

	if v, ok := s.Load(99); ok {
		t.Errorf(`Store should not have an element at 99, value = %s`, v)
	}

	if _, ok := s.Load(100); !ok {
		t.Errorf(`Store should have an element at 100`)
	}
}

func TestBigStore(t *testing.T) {
	var s Store[int64]
	var w sync.WaitGroup

	cpus := int64(runtime.GOMAXPROCS(0))
	count := int64(100000)

	for i := range cpus {
		w.Add(1)
		go func() {
			for j := range count {
				s.Store(i*count+j, (i*count+j)*3)
			}
			w.Done()
		}()
	}
	w.Wait()

	if l := s.Len(); l != int(count*cpus) {
		t.Errorf("Store length should be %d, got %d", count*cpus, l)
	}

	for i := range count * cpus {
		if v, ok := s.Load(i); !ok {
			t.Errorf("Store item at %d should exists", i)
		} else if v != i*3 {
			t.Errorf(`Store item at %d should equal to %d`, i, i*3)
		}
	}
}

func TestBigMutexStore(t *testing.T) {
	var s MutexStore[int64]
	var w sync.WaitGroup

	cpus := int64(runtime.GOMAXPROCS(0))
	count := int64(100000)

	for i := range cpus {
		w.Add(1)
		go func() {
			for j := range count {
				s.Store(i*count+j, (i*count+j)*3)
			}
			w.Done()
		}()
	}
	w.Wait()

	if l := s.Len(); l != int(count*cpus) {
		t.Errorf("Store length should be %d, got %d", count*cpus, l)
	}

	for i := range count * cpus {
		if v, ok := s.Load(i); !ok {
			t.Errorf("Store item at %d should exists", i)
		} else if v != i*3 {
			t.Errorf(`Store item at %d should equal to %d`, i, i*3)
		}
	}
}

func TestStoreLockOrStore(t *testing.T) {
	var s Store[int]

	// First call on an empty slot: created=true, pin held.
	p, cur, created := s.LockOrStore(42, 7)
	if !created {
		t.Errorf(`LockOrStore on empty slot should report created=true`)
	}
	if *p != 7 {
		t.Errorf(`LockOrStore should initialize *p=7, got %d`, *p)
	}
	*p = 11 // mutate under pin
	cur.Unlock()

	// Second call on an existing slot: created=false, existing value
	// returned, ignoring the passed value.
	p2, cur2, created := s.LockOrStore(42, 999)
	if created {
		t.Errorf(`LockOrStore on existing slot should report created=false`)
	}
	if *p2 != 11 {
		t.Errorf(`LockOrStore should return existing value 11, got %d`, *p2)
	}
	cur2.Unlock()

	// Concurrent LockOrStore on the same key: exactly one created=true.
	const goroutines = 32
	var s2 Store[int]
	var creates atomic.Int64
	var w sync.WaitGroup
	for range goroutines {
		w.Add(1)
		go func() {
			defer w.Done()
			_, c, created := s2.LockOrStore(100, 1)
			if created {
				creates.Add(1)
			}
			c.Unlock()
		}()
	}
	w.Wait()
	if creates.Load() != 1 {
		t.Errorf(`Exactly one concurrent LockOrStore should report created=true, got %d`, creates.Load())
	}
}

func TestMutexStoreLockOrStore(t *testing.T) {
	var s MutexStore[int]

	p, cur, created := s.LockOrStore(42, 7)
	if !created {
		t.Errorf(`LockOrStore on empty slot should report created=true`)
	}
	if *p != 7 {
		t.Errorf(`LockOrStore should initialize *p=7, got %d`, *p)
	}
	*p = 11
	cur.Unlock()

	p2, cur2, created := s.LockOrStore(42, 999)
	if created {
		t.Errorf(`LockOrStore on existing slot should report created=false`)
	}
	if *p2 != 11 {
		t.Errorf(`LockOrStore should return existing value 11, got %d`, *p2)
	}
	cur2.Unlock()

	const goroutines = 32
	var s2 MutexStore[int]
	var creates atomic.Int64
	var w sync.WaitGroup
	for range goroutines {
		w.Add(1)
		go func() {
			defer w.Done()
			_, c, created := s2.LockOrStore(100, 1)
			if created {
				creates.Add(1)
			}
			c.Unlock()
		}()
	}
	w.Wait()
	if creates.Load() != 1 {
		t.Errorf(`Exactly one concurrent LockOrStore should report created=true, got %d`, creates.Load())
	}
}

// TestStoreBelowStart exercises every Store[V] operation with an
// index strictly below start; each must take the i<s.start
// early-return in bucket / bucketAlloc and behave as a no-op (no
// panic, no bucket allocation, sentinel return values).
func TestStoreBelowStart(t *testing.T) {
	s := NewStore[int](100)

	if v, ok := s.Load(50); ok || v != 0 {
		t.Errorf(`Load below start should return (0, false), got (%d, %v)`, v, ok)
	}
	if created := s.Store(50, 7); created {
		t.Errorf(`Store below start should report created=false, got true`)
	}
	if s.Len() != 0 {
		t.Errorf(`Store below start must not allocate a bucket; Len=%d`, s.Len())
	}
	if deleted := s.Delete(50); deleted {
		t.Errorf(`Delete below start should return false, got true`)
	}
	if _, _, ok := s.Lock(50); ok {
		t.Errorf(`Lock below start should return ok=false`)
	}
	if p, cur, created := s.LockOrStore(50, 7); p != nil || created {
		cur.Unlock()
		t.Errorf(`LockOrStore below start should return (nil, _, false), got (%v, _, %v)`, p, created)
	}
	if actual, loaded := s.LoadOrStore(50, 7); loaded || actual != 0 {
		t.Errorf(`LoadOrStore below start should return (0, false), got (%d, %v)`, actual, loaded)
	}
	if v, loaded := s.LoadAndDelete(50); loaded || v != 0 {
		t.Errorf(`LoadAndDelete below start should return (0, false), got (%d, %v)`, v, loaded)
	}
	if prev, loaded := s.Swap(50, 7); loaded || prev != 0 {
		t.Errorf(`Swap below start should return (0, false), got (%d, %v)`, prev, loaded)
	}
	if s.CompareAndSwap(50, 0, 7) {
		t.Errorf(`CompareAndSwap below start should return false`)
	}
	if s.CompareAndDelete(50, 7) {
		t.Errorf(`CompareAndDelete below start should return false`)
	}
}

// TestMutexStoreBelowStart is the MutexStore[V] counterpart of
// TestStoreBelowStart.
func TestMutexStoreBelowStart(t *testing.T) {
	s := NewMutexStore[int](100)

	if v, ok := s.Load(50); ok || v != 0 {
		t.Errorf(`Load below start should return (0, false), got (%d, %v)`, v, ok)
	}
	if created := s.Store(50, 7); created {
		t.Errorf(`Store below start should report created=false, got true`)
	}
	if s.Len() != 0 {
		t.Errorf(`Store below start must not allocate a bucket; Len=%d`, s.Len())
	}
	if deleted := s.Delete(50); deleted {
		t.Errorf(`Delete below start should return false, got true`)
	}
	if _, _, ok := s.Lock(50); ok {
		t.Errorf(`Lock below start should return ok=false`)
	}
	if p, cur, created := s.LockOrStore(50, 7); p != nil || created {
		cur.Unlock()
		t.Errorf(`LockOrStore below start should return (nil, _, false), got (%v, _, %v)`, p, created)
	}
	if actual, loaded := s.LoadOrStore(50, 7); loaded || actual != 0 {
		t.Errorf(`LoadOrStore below start should return (0, false), got (%d, %v)`, actual, loaded)
	}
	if v, loaded := s.LoadAndDelete(50); loaded || v != 0 {
		t.Errorf(`LoadAndDelete below start should return (0, false), got (%d, %v)`, v, loaded)
	}
	if prev, loaded := s.Swap(50, 7); loaded || prev != 0 {
		t.Errorf(`Swap below start should return (0, false), got (%d, %v)`, prev, loaded)
	}
	if s.CompareAndSwap(50, 0, 7) {
		t.Errorf(`CompareAndSwap below start should return false`)
	}
	if s.CompareAndDelete(50, 7) {
		t.Errorf(`CompareAndDelete below start should return false`)
	}
}

// TestStoreLockOnEmptySlot covers the `cur&usebit == 0` early-return
// in Store.Lock: locking an index whose bucket exists but whose slot
// has never been Stored returns ok=false without acquiring any pin.
func TestStoreLockOnEmptySlot(t *testing.T) {
	var s Store[int]
	// Allocate the bucket containing slot 0 by storing then deleting it.
	s.Store(0, 1)
	s.Delete(0)
	if _, _, ok := s.Lock(0); ok {
		t.Errorf(`Lock on deleted slot should return ok=false`)
	}
	// Other slot in the same bucket that was never Stored.
	if _, _, ok := s.Lock(5); ok {
		t.Errorf(`Lock on never-stored slot should return ok=false`)
	}
}

// TestStoreGrowExpandsExistingTable covers the "existing table too
// small" path in Store.Grow: an initial Store allocates the 32-bucket
// table, then Grow at a much higher maxIndex resizes the table in
// place via the s.table/s.newTable CAS protocol. Also exercises the
// nil-table and below-start branches of Grow.
func TestStoreGrowExpandsExistingTable(t *testing.T) {
	var s Store[int]

	// First Store allocates the initial 32-bucket table.
	s.Store(0, 1)

	// Grow up to bucket index 64 → targetSize = 128, forces resize.
	s.Grow(32 * 64)

	// Pre-existing value must still be readable through the new table.
	if v, ok := s.Load(0); !ok || v != 1 {
		t.Errorf(`After Grow, Load(0) should return (1, true), got (%d, %v)`, v, ok)
	}

	// Writes at high indices land in the resized table.
	s.Store(32*50, 42)
	if v, ok := s.Load(32 * 50); !ok || v != 42 {
		t.Errorf(`After Grow, Store/Load(32*50) should return (42, true), got (%d, %v)`, v, ok)
	}

	// Grow with maxIndex below start is a no-op.
	s2 := NewStore[int](100)
	s2.Grow(50)
	if s2.Len() != 0 {
		t.Errorf(`Grow below start must not allocate; Len=%d`, s2.Len())
	}

	// Grow from a nil table directly to a large size.
	var s3 Store[int]
	s3.Grow(32 * 64)
	s3.Store(32*60, 99)
	if v, ok := s3.Load(32 * 60); !ok || v != 99 {
		t.Errorf(`Grow on nil table then Store/Load should work, got (%d, %v)`, v, ok)
	}
}

// TestMutexStoreGrowExpandsExistingTable mirrors
// TestStoreGrowExpandsExistingTable for MutexStore (64 slots per
// bucket, >> 6 shift, first-table minimum 16).
func TestMutexStoreGrowExpandsExistingTable(t *testing.T) {
	var s MutexStore[int]

	s.Store(0, 1)

	// Force resize: bucketIndex = 64*64/64 = 64 → targetSize = 128.
	s.Grow(64 * 64)

	if v, ok := s.Load(0); !ok || v != 1 {
		t.Errorf(`After Grow, Load(0) should return (1, true), got (%d, %v)`, v, ok)
	}
	s.Store(64*50, 42)
	if v, ok := s.Load(64 * 50); !ok || v != 42 {
		t.Errorf(`After Grow, Store/Load(64*50) should return (42, true), got (%d, %v)`, v, ok)
	}

	// Below-start Grow is a no-op.
	s2 := NewMutexStore[int](100)
	s2.Grow(50)
	if s2.Len() != 0 {
		t.Errorf(`Grow below start must not allocate; Len=%d`, s2.Len())
	}

	// Grow from a nil table directly to a large size.
	var s3 MutexStore[int]
	s3.Grow(64 * 64)
	s3.Store(64*60, 99)
	if v, ok := s3.Load(64 * 60); !ok || v != 99 {
		t.Errorf(`Grow on nil table then Store/Load should work, got (%d, %v)`, v, ok)
	}
}
