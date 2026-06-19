//go:build !race

package fsync

// This file exercises a deliberate-by-design data race: the V returned by
// Lock is a raw *V; the Lock holder writes to it non-atomically, and a
// concurrent Load reads values[j] non-atomically. On amd64 / arm64 with
// word-sized V (int, uint, pointer, *T), both the write and the read are
// hardware-atomic, so the observation is always coherent — but the Go
// memory model does not technically guarantee it and -race correctly
// flags the access pattern. We gate the test behind the !race build tag
// so it runs in default `go test` runs but is skipped under `go test
// -race`. The contract is documented on ProtoMap.Load.

import (
	"sync"
	"testing"
)

// TestMapLoadDuringLock checks that a Load running concurrently with a
// Lock holder that mutates *V observes one of the valid written values
// (never a torn intermediate). The seqlock retry in Load catches any
// observation that straddled a Lock+Unlock pair.
func TestMapLoadDuringLock(t *testing.T) {
	m := NewMap[int, int](64)
	const key = 42
	m.Store(key, 0)

	validValues := []int{1, 100, 10000, 1000000}
	stop := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			for _, v := range validValues {
				p, c, ok := m.Lock(key)
				if !ok {
					t.Errorf("Lock(%d) failed", key)
					return
				}
				*p = v
				c.Unlock()
			}
		}
	}()

	const readers = 8
	var rg sync.WaitGroup
	for range readers {
		rg.Add(1)
		go func() {
			defer rg.Done()
			for range 100_000 {
				v, ok := m.Load(key)
				if !ok {
					t.Errorf("Load(%d) returned ok=false", key)
					return
				}
				valid := v == 0
				for _, vv := range validValues {
					if v == vv {
						valid = true
						break
					}
				}
				if !valid {
					t.Errorf("Load returned unexpected value %d (not 0 or one of the valid writes)", v)
					return
				}
			}
		}()
	}

	rg.Wait()
	close(stop)
	wg.Wait()
}
