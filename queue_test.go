package fsync

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// ---------- Queue[T] ----------

func TestQueueEmpty(t *testing.T) {
	var q Queue[int]
	if _, ok := q.Dequeue(); ok {
		t.Errorf("Dequeue on empty queue should return ok=false")
	}
	if l := q.Len(); l != 0 {
		t.Errorf("empty queue Len = %d; want 0", l)
	}
}

func TestQueueBasic(t *testing.T) {
	var q Queue[int]
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)
	if l := q.Len(); l != 3 {
		t.Errorf("Len = %d; want 3", l)
	}
	for i, want := range []int{1, 2, 3} {
		v, ok := q.Dequeue()
		if !ok || v != want {
			t.Errorf("step %d: Dequeue = (%d, %v); want (%d, true)", i, v, ok, want)
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Errorf("Dequeue past last should return ok=false")
	}
}

func TestQueueCrossBlock(t *testing.T) {
	var q Queue[int]
	// fill 2.5 blocks (160 items) to exercise the segment-chain path
	const N = 160
	for i := range N {
		q.Enqueue(i)
	}
	if l := q.Len(); l != N {
		t.Errorf("Len = %d; want %d", l, N)
	}
	for i := range N {
		v, ok := q.Dequeue()
		if !ok || v != i {
			t.Errorf("at %d: Dequeue = (%d, %v); want (%d, true)", i, v, ok, i)
		}
	}
}

func TestQueueEnqueueBatch(t *testing.T) {
	var q Queue[int]
	q.EnqueueBatch([]int{10, 20, 30, 40, 50})
	if l := q.Len(); l != 5 {
		t.Errorf("Len = %d; want 5", l)
	}
	for i, want := range []int{10, 20, 30, 40, 50} {
		v, _ := q.Dequeue()
		if v != want {
			t.Errorf("at %d: got %d; want %d", i, v, want)
		}
	}
}

func TestQueueConcurrent(t *testing.T) {
	concurrentQueueTest(t, func() interface {
		Enqueue(int)
		Dequeue() (int, bool)
	} {
		return &Queue[int]{}
	})
}

// NewQueue() should be equivalent to the zero value.
func TestNewQueue(t *testing.T) {
	q := NewQueue[string]()
	q.Enqueue("a")
	q.Enqueue("b")
	v, ok := q.Dequeue()
	if !ok || v != "a" {
		t.Errorf("Dequeue = (%q, %v); want (a, true)", v, ok)
	}
}

// ---------- MutexQueue[T] ----------

func TestMutexQueueEmpty(t *testing.T) {
	var q MutexQueue[int]
	if _, ok := q.Dequeue(); ok {
		t.Errorf("Dequeue on empty MutexQueue should return ok=false")
	}
	if l := q.Len(); l != 0 {
		t.Errorf("empty MutexQueue Len = %d; want 0", l)
	}
}

func TestMutexQueueBasic(t *testing.T) {
	var q MutexQueue[int]
	q.Enqueue(1)
	q.Enqueue(2)
	q.Enqueue(3)
	if l := q.Len(); l != 3 {
		t.Errorf("Len = %d; want 3", l)
	}
	for i, want := range []int{1, 2, 3} {
		v, ok := q.Dequeue()
		if !ok || v != want {
			t.Errorf("step %d: Dequeue = (%d, %v); want (%d, true)", i, v, ok, want)
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Errorf("Dequeue past last should return ok=false")
	}
}

func TestMutexQueueCrossBlock(t *testing.T) {
	var q MutexQueue[int]
	const N = 160
	for i := range N {
		q.Enqueue(i)
	}
	if l := q.Len(); l != N {
		t.Errorf("Len = %d; want %d", l, N)
	}
	for i := range N {
		v, ok := q.Dequeue()
		if !ok || v != i {
			t.Errorf("at %d: Dequeue = (%d, %v); want (%d, true)", i, v, ok, i)
		}
	}
}

func TestMutexQueueEnqueueBatch(t *testing.T) {
	var q MutexQueue[string]
	q.EnqueueBatch([]string{"alpha", "beta", "gamma"})
	if l := q.Len(); l != 3 {
		t.Errorf("Len = %d; want 3", l)
	}
	for i, want := range []string{"alpha", "beta", "gamma"} {
		v, _ := q.Dequeue()
		if v != want {
			t.Errorf("at %d: got %q; want %q", i, v, want)
		}
	}
}

func TestMutexQueueLenIntermediateBlocks(t *testing.T) {
	// Force three blocks in flight: enqueue 192 (3 blocks), then Dequeue
	// 70 (drains the first block + partway into second). Len must visit
	// the default arm (intermediate full block) as well as head and tail.
	var q MutexQueue[int]
	const enq = 192
	for i := range enq {
		q.Enqueue(i)
	}
	const deq = 70
	for range deq {
		q.Dequeue()
	}
	if l := q.Len(); l != enq-deq {
		t.Errorf("Len = %d; want %d", l, enq-deq)
	}
}

func TestMutexQueueConcurrent(t *testing.T) {
	concurrentQueueTest(t, func() interface {
		Enqueue(int)
		Dequeue() (int, bool)
	} {
		return &MutexQueue[int]{}
	})
}

// concurrentQueueTest runs producers in parallel with consumers; once
// the producers signal they are done, consumers keep draining until the
// queue reports empty. The shared shape means TestQueueConcurrent and
// TestMutexQueueConcurrent agree on the semantic checked.
func concurrentQueueTest(t *testing.T, mk func() interface {
	Enqueue(int)
	Dequeue() (int, bool)
}) {
	q := mk()
	const producers = 4
	const consumers = 4
	const perProducer = 1000
	const total = producers * perProducer
	var consumed atomic.Int64
	var producersDone atomic.Bool

	var wp sync.WaitGroup
	for p := range producers {
		wp.Add(1)
		go func() {
			defer wp.Done()
			base := p * perProducer
			for i := range perProducer {
				q.Enqueue(base + i)
			}
		}()
	}
	// signaller: flip the producersDone flag when every producer is done
	go func() {
		wp.Wait()
		producersDone.Store(true)
	}()

	var wc sync.WaitGroup
	for range consumers {
		wc.Add(1)
		go func() {
			defer wc.Done()
			for {
				if _, ok := q.Dequeue(); ok {
					consumed.Add(1)
					continue
				}
				// queue empty; if producers are done AND the queue stays
				// empty, we are done — otherwise yield and retry.
				if producersDone.Load() {
					if _, ok := q.Dequeue(); ok {
						consumed.Add(1)
						continue
					}
					return
				}
				runtime.Gosched()
			}
		}()
	}
	wc.Wait()
	if consumed.Load() != int64(total) {
		t.Errorf("consumed %d; want %d", consumed.Load(), total)
	}
}

func TestNewMutexQueue(t *testing.T) {
	q := NewMutexQueue[int]()
	q.Enqueue(42)
	v, ok := q.Dequeue()
	if !ok || v != 42 {
		t.Errorf("Dequeue = (%d, %v); want (42, true)", v, ok)
	}
}
