package fsync

import (
	"runtime"
	"sync"
	"testing"
)

func TestNewMap(t *testing.T) {
	var m Map[int, string]

	m.Store(1, "one")
	if m.Len() != 1 {
		t.Errorf(`Map size must be incremented on storing, size = %d`, m.Len())
	}

	m.Store(100, "hundred")
	if m.Len() != 2 {
		t.Errorf(`Map size must be incremented on storing, size = %d`, m.Len())
	}

	m.Store(1000, "thousand")
	if m.Len() != 3 {
		t.Errorf(`Map size must be incremented on storing, size = %d`, m.Len())
	}

	if v, ok := m.Load(99); ok {
		t.Errorf(`Map should not have an element at 99, value = %s`, v)
	}

	if v, ok := m.Load(100); !ok {
		t.Errorf(`Map should have an element at 100`)
	} else if v != "hundred" {
		t.Errorf(`Map should have element 'hundred' at 100, got %s`, v)
	}
}

func TestStoreMap(t *testing.T) {
	var m Map[int, int]

	n := 10
	for i := range n {
		m.Store(i+1, i*7)
	}

	if l := m.Len(); l != n {
		t.Errorf("Map length should be %d, got %d", n, l)
	}

	for i := range n {
		if v, ok := m.Load(i + 1); !ok {
			t.Errorf("Map item at %d should exists", i)
		} else if v != i*7 {
			t.Errorf(`Map item at %d should equal to %d (value is %d)`, i, v, i*7)
		}
	}
}

func TestBigMap(t *testing.T) {
	var m Map[int, int]
	var w sync.WaitGroup

	count := 100000
	cpus := runtime.GOMAXPROCS(0)

	for i := range cpus {
		w.Add(1)
		go func() {
			for j := range count {
				m.Store(i*count+j, (i*count+j)*3)
			}
			w.Done()
		}()
	}
	w.Wait()

	if l := m.Len(); l != count*cpus {
		t.Errorf("Map length should be %d, got %d", count*cpus, l)
	}

	for i := range count * cpus {
		if v, ok := m.Load(i); !ok {
			t.Errorf("Map item at %d should exists", i)
		} else if v != i*3 {
			t.Errorf(`Map item at %d should equal to %d`, i, i*3)
		}
	}
}
