package fsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestConcurSmall(t *testing.T) {
	var m Map[int, int]
	var w sync.WaitGroup
	const count = 5000
	cpus := runtime.GOMAXPROCS(0)
	var lost atomic.Int64

	for i := range cpus {
		w.Add(1)
		go func() {
			defer w.Done()
			for j := range count {
				k := i*count + j
				m.Store(k, k*3)
				if v, ok := m.Load(k); !ok || v != k*3 {
					lost.Add(1)
				}
			}
		}()
	}
	w.Wait()

	if n := lost.Load(); n > 0 {
		t.Errorf("self-load lost %d items out of %d", n, count*cpus)
	}
	t.Logf("Len=%d (expected %d)", m.Len(), count*cpus)
}
