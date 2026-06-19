package benchs

import (
	"sync"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/aytechnet/fsync"
)

// Queue benchmarks. Three workloads:
//
//   - SerialPingPong: 1 goroutine alternates Enqueue then Dequeue on the
//     same queue. Pure per-op overhead, zero contention. Establishes the
//     baseline cost of a producer step and a consumer step combined.
//
//   - MPMC (4P + 4C): four producers each enqueue while four consumers
//     each dequeue, until N total ops have run. Mixed contention on both
//     ends of the queue; representative of a worker dispatch pattern.
//
//   - SPSC (1P + 1C): single producer paired with single consumer, the
//     workload xsync.SPSCQueue is specifically tuned for.
//
// All bounded queues are sized at queueCapacity slots so the bounded
// flavors do not block in steady state. Tests pre-fill half the
// capacity for the parallel workloads so producers and consumers find
// items / room from the very first iteration.

const queueCapacity = 1024

// ---------- SerialPingPong: Enqueue → Dequeue, one op = one round-trip ----------

func BenchmarkFsyncQueueSerialPingPong(b *testing.B) {
	var q fsync.Queue[int]
	for i := 0; i < b.N; i++ {
		q.Enqueue(i)
		q.Dequeue()
	}
}

func BenchmarkFsyncMutexQueueSerialPingPong(b *testing.B) {
	var q fsync.MutexQueue[int]
	for i := 0; i < b.N; i++ {
		q.Enqueue(i)
		q.Dequeue()
	}
}

func BenchmarkXsyncMPMCQueueSerialPingPong(b *testing.B) {
	q := xsync.NewMPMCQueue[int](queueCapacity)
	for i := 0; i < b.N; i++ {
		q.TryEnqueue(i)
		q.TryDequeue()
	}
}

func BenchmarkXsyncSPSCQueueSerialPingPong(b *testing.B) {
	q := xsync.NewSPSCQueue[int](queueCapacity)
	for i := 0; i < b.N; i++ {
		q.TryEnqueue(i)
		q.TryDequeue()
	}
}

func BenchmarkChanSerialPingPong(b *testing.B) {
	c := make(chan int, queueCapacity)
	for i := 0; i < b.N; i++ {
		c <- i
		<-c
	}
}

// ---------- MPMC: 4 producers + 4 consumers ----------

const (
	mpmcProducers = 4
	mpmcConsumers = 4
	mpmcPerSide   = 100000 // ops per producer / per consumer per bench round
)

func runMPMC(b *testing.B, enqueue func(int), dequeue func()) {
	const totalPerRound = mpmcProducers * mpmcPerSide
	rounds := b.N / totalPerRound
	if rounds == 0 {
		rounds = 1
	}
	b.ResetTimer()
	for r := 0; r < rounds; r++ {
		var w sync.WaitGroup
		for p := 0; p < mpmcProducers; p++ {
			w.Add(1)
			go func() {
				defer w.Done()
				for i := 0; i < mpmcPerSide; i++ {
					enqueue(i)
				}
			}()
		}
		for c := 0; c < mpmcConsumers; c++ {
			w.Add(1)
			go func() {
				defer w.Done()
				for i := 0; i < mpmcPerSide; i++ {
					dequeue()
				}
			}()
		}
		w.Wait()
	}
}

func BenchmarkFsyncQueueMPMC(b *testing.B) {
	var q fsync.Queue[int]
	runMPMC(b,
		func(i int) { q.Enqueue(i) },
		func() {
			for {
				if _, ok := q.Dequeue(); ok {
					return
				}
			}
		},
	)
}

func BenchmarkFsyncMutexQueueMPMC(b *testing.B) {
	var q fsync.MutexQueue[int]
	runMPMC(b,
		func(i int) { q.Enqueue(i) },
		func() {
			for {
				if _, ok := q.Dequeue(); ok {
					return
				}
			}
		},
	)
}

func BenchmarkXsyncMPMCQueueMPMC(b *testing.B) {
	q := xsync.NewMPMCQueue[int](queueCapacity)
	runMPMC(b,
		func(i int) {
			for !q.TryEnqueue(i) {
			}
		},
		func() {
			for {
				if _, ok := q.TryDequeue(); ok {
					return
				}
			}
		},
	)
}

func BenchmarkChanMPMC(b *testing.B) {
	c := make(chan int, queueCapacity)
	runMPMC(b,
		func(i int) { c <- i },
		func() { <-c },
	)
}

// ---------- MPMC Balanced: 12 goroutines alternate Enqueue/Dequeue ----------
//
// Variant of MPMC built to give bounded queues a fair shot: every
// goroutine does both Enqueue and Dequeue (1:1 ratio), and the queue
// is pre-filled to half its capacity, so the steady state stays
// around half-full. The producer-side TryEnqueue should not see a
// full queue and the consumer-side TryDequeue should not see an
// empty one — meaning a bounded MPMC queue gets to skip the retry
// loop that killed it on the producer-vs-consumer benchmark.
//
// Capacity raised to 8192 to give xsync's slot array plenty of
// room (each cell carries its own turn counter; less cell
// contention than the small 1024 capacity).

const balancedCapacity = 8192

func BenchmarkFsyncQueueMPMCBalanced(b *testing.B) {
	var q fsync.Queue[int]
	for i := 0; i < balancedCapacity/2; i++ {
		q.Enqueue(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.Enqueue(i)
			for {
				if _, ok := q.Dequeue(); ok {
					break
				}
			}
			i++
		}
	})
}

func BenchmarkFsyncMutexQueueMPMCBalanced(b *testing.B) {
	var q fsync.MutexQueue[int]
	for i := 0; i < balancedCapacity/2; i++ {
		q.Enqueue(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			q.Enqueue(i)
			for {
				if _, ok := q.Dequeue(); ok {
					break
				}
			}
			i++
		}
	})
}

func BenchmarkXsyncMPMCQueueMPMCBalanced(b *testing.B) {
	q := xsync.NewMPMCQueue[int](balancedCapacity)
	for i := 0; i < balancedCapacity/2; i++ {
		q.TryEnqueue(i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			for !q.TryEnqueue(i) {
			}
			for {
				if _, ok := q.TryDequeue(); ok {
					break
				}
			}
			i++
		}
	})
}

func BenchmarkChanMPMCBalanced(b *testing.B) {
	c := make(chan int, balancedCapacity)
	for i := 0; i < balancedCapacity/2; i++ {
		c <- i
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c <- i
			<-c
			i++
		}
	})
}

// ---------- SPSC: 1 producer + 1 consumer ----------

const spscPerSide = 1000000

func runSPSC(b *testing.B, enqueue func(int), dequeue func()) {
	rounds := b.N / spscPerSide
	if rounds == 0 {
		rounds = 1
	}
	b.ResetTimer()
	for r := 0; r < rounds; r++ {
		var w sync.WaitGroup
		w.Add(2)
		go func() {
			defer w.Done()
			for i := 0; i < spscPerSide; i++ {
				enqueue(i)
			}
		}()
		go func() {
			defer w.Done()
			for i := 0; i < spscPerSide; i++ {
				dequeue()
			}
		}()
		w.Wait()
	}
}

func BenchmarkFsyncQueueSPSC(b *testing.B) {
	var q fsync.Queue[int]
	runSPSC(b,
		func(i int) { q.Enqueue(i) },
		func() {
			for {
				if _, ok := q.Dequeue(); ok {
					return
				}
			}
		},
	)
}

func BenchmarkFsyncMutexQueueSPSC(b *testing.B) {
	var q fsync.MutexQueue[int]
	runSPSC(b,
		func(i int) { q.Enqueue(i) },
		func() {
			for {
				if _, ok := q.Dequeue(); ok {
					return
				}
			}
		},
	)
}

func BenchmarkXsyncSPSCQueueSPSC(b *testing.B) {
	q := xsync.NewSPSCQueue[int](queueCapacity)
	runSPSC(b,
		func(i int) {
			for !q.TryEnqueue(i) {
			}
		},
		func() {
			for {
				if _, ok := q.TryDequeue(); ok {
					return
				}
			}
		},
	)
}

func BenchmarkXsyncUMPSCQueueSPSC(b *testing.B) {
	q := xsync.NewUMPSCQueue[int]()
	runSPSC(b,
		func(i int) { q.Enqueue(i) },
		func() { q.Dequeue() },
	)
}

func BenchmarkChanSPSC(b *testing.B) {
	c := make(chan int, queueCapacity)
	runSPSC(b,
		func(i int) { c <- i },
		func() { <-c },
	)
}
