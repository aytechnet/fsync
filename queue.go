package fsync

import (
	"sync"
	"sync/atomic"
	"runtime"
)

const queueBlockSize = 64

// segmentQueue is a block of 64 values, the building block of Queue.
// A segment is filled once (enqPos 0->64) then drained once (deqPos 0->64) and
// never recycled: enqueue and dequeue only ever look at the current tail/head
// segment and advance forward, so no value is ever addressed by an absolute
// index and there is never a need to walk the chain backward.
type segmentQueue[T any] struct {
	enqPos    atomic.Uint64 // next slot to write; producers fetch-add, may overshoot 64
	_         [56]byte      // keep producer and consumer cursors on distinct cache lines
	deqPos    atomic.Uint64 // next slot to read; consumers CAS, capped at 64
	_         [56]byte
	published atomic.Uint64 // bit i set once items[i] is fully written and readable
	next      atomic.Pointer[segmentQueue[T]]
	items     [queueBlockSize]T
}

// Queue is a lock-free multi-producer/multi-consumer FIFO of T, made of chained
// blocks of 64 values (same spirit as Store/Map: atomic cursors + bitmask, blocks
// never moved so values keep stable addresses). It is unbounded and grows one
// block at a time; fully consumed blocks become unreachable and are reclaimed by
// the GC. The zero value is ready to use.
type Queue[T any] struct {
	tailSeg atomic.Pointer[segmentQueue[T]]
	_       [56]byte
	headSeg atomic.Pointer[segmentQueue[T]]
}

// NewQueue returns an empty Queue[T]. The zero-value Queue is equally
// usable; NewQueue is provided for symmetry with NewStore / NewMap.
func NewQueue[T any]() *Queue[T] {
	return &Queue[T]{}
}

// Enqueue appends v. It never blocks and never drops.
func (q *Queue[T]) Enqueue(v T) {
	for {
		t := q.tailSeg.Load()
		if t == nil {
			// lazy allocation of the first block
			ns := &segmentQueue[T]{}
			if q.tailSeg.CompareAndSwap(nil, ns) {
				q.headSeg.CompareAndSwap(nil, ns)
			}
			continue
		}

		if e := t.enqPos.Add(1) - 1; e < queueBlockSize {
			t.items[e] = v
			t.published.Or(uint64(1) << e)
			return
		}

		// current block is full so link and move to the next one then retry
		nxt := t.next.Load()
		if nxt == nil {
			ns := &segmentQueue[T]{}
			if t.next.CompareAndSwap(nil, ns) {
				nxt = ns
			} else {
				nxt = t.next.Load()
			}
		}
		q.tailSeg.CompareAndSwap(t, nxt)
	}
}

// EnqueueBatch appends every element of vs in order.
func (q *Queue[T]) EnqueueBatch(vs []T) {
	for _, v := range vs {
		q.Enqueue(v)
	}
}

// Dequeue removes and returns the oldest element. ok is false if the queue is empty.
func (q *Queue[T]) Dequeue() (v T, ok bool) {
	for {
		h := q.headSeg.Load()
		if h == nil {
			return
		}

		d := h.deqPos.Load()
		if d >= queueBlockSize {
			// block fully consumed so advance to the next one if any
			nxt := h.next.Load()
			if nxt == nil {
				return
			}
			q.headSeg.CompareAndSwap(h, nxt)
			continue
		}

		// only slots actually claimed by a producer are available
		e := h.enqPos.Load()
		if e > queueBlockSize {
			e = queueBlockSize
		}
		if d >= e {
			return
		}

		if h.deqPos.CompareAndSwap(d, d+1) {
			mask := uint64(1) << d
			// the producer that claimed slot d will publish it, so this spins briefly
			for h.published.Load()&mask == 0 {
				runtime.Gosched()
			}
			v = h.items[d]
			ok = true
			return
		}
	}
}

// Len returns an approximate number of queued elements.
func (q *Queue[T]) Len() (n int) {
	for s := q.headSeg.Load(); s != nil; s = s.next.Load() {
		e := s.enqPos.Load()
		if e > queueBlockSize {
			e = queueBlockSize
		}
		if d := s.deqPos.Load(); e > d {
			n += int(e - d)
		}
	}

	return
}

// blockMutexQueue is the 64-value block backing MutexQueue.
type blockMutexQueue[T any] struct {
	items [queueBlockSize]T
	next  *blockMutexQueue[T]
}

// MutexQueue is a mutex-guarded FIFO of T in blocks of 64, equivalent in behaviour
// to Queue. It exists only as a baseline to benchmark Queue against (same role as
// MutexStore versus Store) and is not part of the intended public API yet.
type MutexQueue[T any] struct {
	mu   sync.Mutex
	head *blockMutexQueue[T]
	tail *blockMutexQueue[T]
	deq  int // read cursor in head block
	enq  int // write cursor in tail block
}

// NewMutexQueue returns an empty MutexQueue[T]. The zero-value is also
// usable; NewMutexQueue is provided for symmetry with NewQueue.
func NewMutexQueue[T any]() *MutexQueue[T] {
	return &MutexQueue[T]{}
}

// Enqueue appends v under the queue's mutex. It never blocks beyond
// mutex contention (no I/O, no allocation when the tail block has
// room).
func (q *MutexQueue[T]) Enqueue(v T) {
	q.mu.Lock()

	if q.tail == nil {
		b := &blockMutexQueue[T]{}
		q.head = b
		q.tail = b
		q.enq = 0
		q.deq = 0
	} else if q.enq == queueBlockSize {
		b := &blockMutexQueue[T]{}
		q.tail.next = b
		q.tail = b
		q.enq = 0
	}

	q.tail.items[q.enq] = v
	q.enq++

	q.mu.Unlock()
}

// EnqueueBatch appends every element of vs in order. Each element is
// enqueued under its own mutex acquire; there is no batch-level lock.
func (q *MutexQueue[T]) EnqueueBatch(vs []T) {
	for _, v := range vs {
		q.Enqueue(v)
	}
}

// Dequeue removes and returns the oldest element. ok is false if the
// queue is empty.
func (q *MutexQueue[T]) Dequeue() (v T, ok bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.head == nil {
		return
	}

	// Advance head past a fully drained block before computing limit.
	// Without this, the first Dequeue right after a producer Enqueue
	// that filled and chained a new block would observe head==full,
	// deq==64, and bail out as if empty.
	if q.deq == queueBlockSize && q.head.next != nil {
		q.head = q.head.next
		q.deq = 0
	}

	limit := queueBlockSize
	if q.head == q.tail {
		limit = q.enq
	}
	if q.deq >= limit {
		return
	}

	v = q.head.items[q.deq]
	ok = true
	q.deq++

	return
}

// Len returns the exact number of queued elements (the mutex is held
// during the count).
func (q *MutexQueue[T]) Len() (n int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for b := q.head; b != nil; b = b.next {
		switch {
		case b == q.head && b == q.tail:
			n += q.enq - q.deq
		case b == q.head:
			n += queueBlockSize - q.deq
		case b == q.tail:
			n += q.enq
		default:
			n += queueBlockSize
		}
	}

	return
}
