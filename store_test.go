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
