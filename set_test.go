package fsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSetBasic(t *testing.T) {
	var s Set[string]

	if s.Len() != 0 {
		t.Errorf(`empty Set must have Len()=0, got %d`, s.Len())
	}
	if s.Contains("absent") {
		t.Errorf(`Contains on empty Set should be false`)
	}
	if s.Remove("absent") {
		t.Errorf(`Remove on empty Set should be false`)
	}

	if !s.Add("alpha") {
		t.Errorf(`first Add of "alpha" should return true`)
	}
	if s.Add("alpha") {
		t.Errorf(`second Add of "alpha" should return false`)
	}
	if !s.Contains("alpha") {
		t.Errorf(`Contains("alpha") should be true after Add`)
	}
	if s.Len() != 1 {
		t.Errorf(`Len() should be 1, got %d`, s.Len())
	}

	if !s.Remove("alpha") {
		t.Errorf(`Remove("alpha") should return true`)
	}
	if s.Contains("alpha") {
		t.Errorf(`Contains("alpha") should be false after Remove`)
	}
	if s.Len() != 0 {
		t.Errorf(`Len() should be 0 after Remove, got %d`, s.Len())
	}
}

func TestSetNewSetWithCapacity(t *testing.T) {
	s := NewSet[int]().Grow(1000)
	for i := range 500 {
		if !s.Add(i) {
			t.Errorf(`Add(%d) on fresh set should return true`, i)
		}
	}
	if s.Len() != 500 {
		t.Errorf(`Len() should be 500, got %d`, s.Len())
	}
	// Grow at runtime.
	s.Grow(10000)
	if !s.Contains(0) || !s.Contains(499) {
		t.Errorf(`Grow must preserve existing elements`)
	}
}

func TestSetRange(t *testing.T) {
	var s Set[int]
	const n = 100
	for i := range n {
		s.Add(i)
	}
	seen := make(map[int]bool, n)
	s.Range(func(k int) bool {
		if seen[k] {
			t.Errorf(`Range visited %d twice`, k)
		}
		seen[k] = true
		return true
	})
	if len(seen) != n {
		t.Errorf(`Range should visit all %d elements, saw %d`, n, len(seen))
	}

	// Early termination.
	count := 0
	s.Range(func(_ int) bool {
		count++
		return count < 10
	})
	if count != 10 {
		t.Errorf(`Range should stop after f returns false, saw %d calls`, count)
	}
}

func TestSetClear(t *testing.T) {
	var s Set[int]
	for i := range 50 {
		s.Add(i)
	}
	s.Clear()
	if s.Len() != 0 {
		t.Errorf(`Len() after Clear should be 0, got %d`, s.Len())
	}
	if s.Contains(0) {
		t.Errorf(`Contains after Clear should be false`)
	}
	// Set must remain usable after Clear.
	s.Add(42)
	if !s.Contains(42) {
		t.Errorf(`Add after Clear must work`)
	}
}

func TestSetConcurrentAdd(t *testing.T) {
	var s Set[int64]
	var w sync.WaitGroup
	cpus := int64(runtime.GOMAXPROCS(0))
	count := int64(10000)

	for g := range cpus {
		w.Add(1)
		go func() {
			defer w.Done()
			for j := range count {
				s.Add(g*count + j)
			}
		}()
	}
	w.Wait()

	if l := int64(s.Len()); l != count*cpus {
		t.Errorf(`concurrent Add: expected Len()=%d, got %d`, count*cpus, l)
	}
	for i := int64(0); i < count*cpus; i++ {
		if !s.Contains(i) {
			t.Errorf(`missing element %d after concurrent Add`, i)
		}
	}
}

func TestSetConcurrentAddSameKey(t *testing.T) {
	// All goroutines race on the same key; exactly one Add must
	// observe added=true.
	var s Set[int]
	const goroutines = 64
	var addedTrue atomic.Int64
	var w sync.WaitGroup

	for range goroutines {
		w.Add(1)
		go func() {
			defer w.Done()
			if s.Add(42) {
				addedTrue.Add(1)
			}
		}()
	}
	w.Wait()

	if addedTrue.Load() != 1 {
		t.Errorf(`exactly one concurrent Add should report added=true, got %d`, addedTrue.Load())
	}
	if !s.Contains(42) {
		t.Errorf(`element 42 should be present after race`)
	}
	if s.Len() != 1 {
		t.Errorf(`Len() should be 1 after race, got %d`, s.Len())
	}
}
