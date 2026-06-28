# fsync — fast concurrent containers for Go

[![ci](https://github.com/aytechnet/fsync/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/aytechnet/fsync/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/aytechnet/fsync.svg)](https://pkg.go.dev/github.com/aytechnet/fsync)
[![Go Coverage](https://img.shields.io/codecov/c/github/aytechnet/fsync/main?color=brightcolor)](https://codecov.io/gh/aytechnet/fsync)
[![Go Report Card](https://goreportcard.com/badge/github.com/aytechnet/fsync)](https://goreportcard.com/report/github.com/aytechnet/fsync)

`fsync` is a Go 1.25 library of high-performance, generic concurrent
containers — drop-in replacements for `sync.Map`, `map[K]V + mutex`,
buffered `chan`, and bitsets. Built for the DyaPi iPaaS platform; six
containers, one set of guarantees: lock-free reads, zero-allocation
hot paths, full `sync.Map`-compatible API, plus a stable `*V` pointer
out of `Lock`.

## At a glance

| Type            | Key              | Value layout    | Niche                                  |
|-----------------|------------------|-----------------|----------------------------------------|
| `Map[K,V]`      | `K comparable`   | inline `[8]V`   | general concurrent hash map            |
| `Set[K]`        | `K comparable`   | inline `[8]K`   | concurrent set-of-keys                 |
| `Store[V]`      | `int64`          | inline `[32]V`  | dense integer-indexed store, lock-free |
| `MutexStore[V]` | `int64`          | inline `[64]V`  | same, mutex per slot (contention)      |
| `Bitmap`        | `int64`          | `[8]atomic.Uint64` (512 bits / cacheline) | dense bit set (~10× lighter than `Store[bool]`) |
| `Queue[T]`      | —                | inline `[64]T`  | unbounded MPMC FIFO, lock-free         |

## Quick start

```go
import "github.com/aytechnet/fsync"

// Drop-in for sync.Map.
var m fsync.Map[string, int]
m.Store("hits", 1)
v, ok := m.Load("hits") // 1, true

// Lock returns a stable *V into the bucket — no allocation,
// no *Entry indirection, no separate mutex needed.
p, cur, _ := m.Lock("hits")
*p++                    // mutate in place under exclusive ownership
cur.Unlock()            // defer cur.Unlock() is the idiomatic pattern

// Pre-size to skip warmup doublings; Grow is chainable.
counters := fsync.NewMap[int, int64]().Grow(100_000)

// Bitmap: dense int64-indexed bit set, ~145 KB for 1M bits.
var seen fsync.Bitmap
seen.Set(42)
seen.Has(42)            // true
seen.Range(func(i int64) bool { /* … */ ; return true })
```

## Why fsync?

- **Lock(`*V`) without per-entry allocation.** The canonical
  `map[K]*{mu, V}` pattern allocates an entry struct on every first
  insert and chases a pointer on every access. `fsync.Map.Lock`
  returns a stable `*V` straight into the inline bucket — no
  `*Entry`, no allocation, on every key from the very first one.
- **Speed.** `Load` at 0.82 ns (`Store`), 1.40 ns (`Map`);
  `Bitmap.Has` at **0.52 ns** — the fastest concurrent lookup of
  any structure benchmarked here. `Range` is 1.5–2.6 ns/key thanks
  to inline-bucket layouts (one cacheline read per 8–64 entries).
- **Memory.** Inline `[N]V` slots cost B/op only at the bucket
  granularity. `Bitmap` packs 1 M bits in **~145 KB** (vs ~48 MB for
  `xsync.Map[int64, bool]`); `Set` is a zero-runtime-cost wrapper
  on `Map[K, struct{}]`. Detailed footprint table further down.
- **`sync.Map`-compatible.** `LoadOrStore`, `Swap`, `CompareAndSwap`,
  `CompareAndDelete`, `LoadAndDelete`, `Range`, `Clear`: same
  signatures and semantics. Plus a runtime `Grow(n)` you don't get
  from `sync.Map` or `xsync.Map` (chainable).
- **Production-tested.** Backs the `aytechnet/dyapi` iPaaS platform
  in production. 89 % test coverage, race-detector clean, A+ on Go
  Report Card.

For a per-container deep dive — concurrency contract, race-detector
caveat with inline `V`, design history, and the full benchmark
matrix — keep reading. For the API, see
[pkg.go.dev/github.com/aytechnet/fsync](https://pkg.go.dev/github.com/aytechnet/fsync).

## Concurrency contract

For every key independently:

- `Load`, `Store`, `Delete` are lock-free and O(1) average.
- `Lock(k)` pins the slot exclusively and returns a stable `*V`.
- `LockOrStore(k, zero)` is the atomic insert-and-pin equivalent
  (returns `(*V, created)`).
- While a slot is pinned, other `Load` / `Store` / `Delete` on **that
  same key** spin until `Unlock`. Other keys are unaffected.
- **Always pair `Lock` with `Unlock`** — idiomatic usage is
  `defer cur.Unlock()` right after `Lock`.

### Lock usage guidelines (read this before using Lock)

The `Lock` primitive is the differentiator of `fsync` — it hands
back a stable `*V` into the inline bucket with zero allocation —
but it's also the easiest API to misuse. Read these before
sprinkling `defer cur.Unlock()` in your code.

**1. Hold the Lock for nanoseconds, not microseconds.**
`Store[V]` and `Map[K,V]` implement `Lock` with a **busy-wait
spin** on contention (Load-then-CAS + `runtime.Gosched()`). This
is optimal for cycle-counted critical sections — `*p++`,
`*p = newValue`, an append to a small slice, a counter mutation —
but **disastrous** if you do I/O, a syscall, a network call, or
wait on a channel under the Lock: every other goroutine touching
the same key will burn CPU until you Unlock.

The right shape is `Lock → tiny in-memory mutation → Unlock`. If
you need the value to drive a blocking call:

```go
// WRONG: holds the spin lock during an HTTP roundtrip.
p, cur, _ := m.Lock(id)
*p = callExternalAPI(*p)   // every concurrent Lock/Load on `id` busy-spins
cur.Unlock()

// RIGHT: copy under Lock, do the work outside, re-apply under Lock.
p, cur, _ := m.Lock(id)
snapshot := *p
cur.Unlock()
updated := callExternalAPI(snapshot)
p, cur, _ = m.Lock(id)
*p = updated
cur.Unlock()
```

If your workload genuinely needs to **block under a per-key
lock** (e.g. serialize all writers to the same entry across an
I/O), use **`MutexStore[V]`** instead: its `Lock` uses one
`sync.Mutex` per slot, so contenders park in the kernel (futex)
instead of busy-spinning. Slower on the uncontended path, but
won't burn cores under hold.

**2. Never re-Lock the same key from inside a Lock holder
(self-deadlock).**
The pin is per-key, not per-goroutine. A second `Lock(k)` from
the same goroutine that already holds `Lock(k)` will spin
forever on its own pin. Avoid calling code under Lock that might
transitively `Lock` the same key — including helpers that
themselves take a `Lock`.

**3. The Cursor is not an ownership token; don't pass it across
goroutines.**
The `Cursor` returned by `Lock` identifies the bucket+slot, not
the holder. Calling `cur.Unlock()` from a different goroutine
than the one that `Lock`-ed is unsupported and will not be
detected at runtime.

**4. The `*V` returned by `Lock` is only valid until
`cur.Unlock()`.**
After Unlock, the slot may be overwritten by a concurrent
`Store`, zeroed by a concurrent `Delete`, or (during a rebuild)
still point into the old bucket while readers have switched to
the new one. Never dereference `p` after `Unlock`. The
duplicate-on-pin rebuild policy guarantees the address stays
valid **while** pinned — that guarantee ends at Unlock.

**5. `LoadOrStore` / `Swap` / `CompareAndSwap` give you
sync.Map-style atomic ops without a holdable lock.**
If all you need is "atomically read-and-set" or
"swap-if-equals," use those instead of `Lock + read + write +
Unlock` — they're faster (no pin), don't expose you to the
guidelines above, and match the sync.Map signatures byte-for-byte.

### Full `sync.Map`-compatible surface

On top of the pinning primitives, every map / set / store ships the
same set of atomic operations as `sync.Map`, with identical semantics:

- `LoadOrStore(k, v) (actual V, loaded bool)` — get-or-insert, no pin.
- `LoadAndDelete(k) (V, bool)` — atomic read+remove.
- `Swap(k, v) (previous V, loaded bool)` — set new and return old.
- `CompareAndSwap(k, old, new V) bool` — interface-comparable `V`.
- `CompareAndDelete(k, old V) bool` — interface-comparable `V`.
- `Range(f func(K, V) bool)` — weakly consistent iteration.
- `Clear()` — drop everything (pinned `*V` remain valid).

## Benchmarks

Go 1.25, AMD Ryzen 5 8540U (Zen 4, 6c/12t), GOMAXPROCS=12, all benches
in `./benchs/`. Numbers below are medians of 3 runs at `-benchtime=5s
-count=3` for fsync rows and `-benchtime=2s -count=3` for `sync.Map`
and `xsync.Map`. The `Store` and `GrowStore` rows use a shorter
`-benchtime=1s` (fsync) or `-benchtime=500ms` (sync.Map / xsync.Map)
to avoid OOM on a 14 GB machine — each `sync.Map.Store` insertion
boxes an `int` into an interface, so the live set climbs into the
tens of GB at higher b.N. Lower is better.

Workloads:

- **ReadOnly** — preloaded map (2048 keys), parallel `Load` only.
- **ReadHeavy** — 1 `Store` per 10 `Load`s.
- **Store** — distinct keys per goroutine, write-only.
- **GrowStore** — same as Store but the table is pre-sized
  (`fsync.Map.Grow(N)` / `xsync.WithPresize(N)`).
- **Churn** — alternating `Store(k)` / `Delete(k)` on a rolling window
  of 1024 keys (slot recycling under contention).
- **LoadOrStore** — preloaded, every call hits an existing key
  (get-or-set steady state).
- **Range/key** — full sweep over the preloaded set divided by entry
  count (cost of seeing one entry during iteration).
- **Lock + inc** — 256 hot keys, `Lock(k)` + `*p++` + `Unlock(k)` (no
  alloc once warm).

Below: one **Speed** table and one **Memory** table, same row order
in both, read them side by side. ns/op is wall-clock-per-iteration;
B/op + allocs/op are the inline-`V` design's whole point.

### Speed — `ns/op` (lower is better)

| Implementation                       | ReadOnly    | ReadHeavy   | Store        | GrowStore    | Churn        | LoadOrStore | Range/key   |
|--------------------------------------|------------:|------------:|-------------:|-------------:|-------------:|------------:|------------:|
| `map[int]int` (no lock, baseline)    |     2.11 ns | data race   |          —   |          —   |          —   |          —  |          —  |
| `map[int]int` + `sync.Mutex`         |           — |     45.5 ns |       218 ns |          —   |      58.0 ns |          —  |          —  |
| `map[int]int` + `sync.RWMutex`       |           — |     19.9 ns |       238 ns |          —   |      61.8 ns |          —  |          —  |
| `sync.Map` (stdlib)                  |     2.95 ns |     8.42 ns |      95.1 ns |          —   |      82.9 ns |    10.08 ns |     5.16 ns |
| `xsync.Map` v4                       |     1.04 ns |     3.56 ns |      74.5 ns |      70.9 ns |     14.96 ns |     1.38 ns |     4.52 ns |
| **`fsync.Map`**                      | **1.40 ns** | **5.91 ns** |  **74.2 ns** |  **65.3 ns** |  **17.62 ns**| **1.54 ns** | **2.61 ns** |
| **`fsync.Store`**                    | **0.82 ns** | **1.19 ns** |  **3.46 ns** |  **3.34 ns** |  **2.30 ns** | **1.17 ns** | **1.50 ns** |
| **`fsync.MutexStore`**               |     0.97 ns |     1.22 ns |      3.05 ns |          —   |      3.22 ns |     1.22 ns |     3.72 ns |

`Store` and `MutexStore` use a dense `int64` key and skip hashing
entirely — that's where the order-of-magnitude jump on `Store`
(3.46 vs 74.2 ns) and `Churn` (2.30 vs 17.6 ns) comes from. Treat
their rows as "the cost of the same workload once the key happens
to be a dense integer".

Highlights:

- `fsync.Map.LoadOrStore` at **1.54 ns** is **6.5× faster than
  `sync.Map` (10.08 ns)** on the hot get-or-set path, and within
  ~12 % of `xsync.Map` (1.38 ns) — drop-in replacement parity.
- `fsync.Map.Range/key` at **2.61 ns** is the **fastest iterator**
  among hashed maps: 2.0× faster than `sync.Map` (5.16 ns) and
  ~1.7× faster than `xsync.Map` (4.52 ns), thanks to the inline
  `[8]V` bucket layout (one cacheline read per 8 entries).
  `fsync.Store` goes further still at **1.50 ns/entry** with its
  `[32]V` slots.
- `fsync.Map` Load (1.40 ns) sits below a plain `map[int]int`
  (2.11 ns under `RunParallel` with false-sharing on shared
  buckets) and well below `sync.Map` (2.95 ns) — full concurrent
  safety AND `Lock(*V)` semantics for less than the lockless
  baseline. `xsync.Map` (1.04 ns) edges everyone on Load thanks
  to its tighter bucket layout (no pin-word to read).
- `fsync.Store.ReadOnly` at **0.82 ns** is the fastest concurrent
  Load benchmarked here — it beats `xsync.Map` (1.04 ns) because
  integer-indexed slots skip hashing entirely.
- `GrowStore` on `fsync.Map` is within 12 % of `Store` (no Grow):
  the table is allocated up-front so concurrent Stores no longer
  race on rebuild. `xsync.WithPresize` shows similar behavior but
  with higher run-to-run variance.
- `Churn` (rolling window Store+Delete) is where `xsync.Map`
  (14.96 ns) shines vs `fsync.Map` (17.62 ns). Both crush stdlib
  maps with locks (~60 ns) and `sync.Map` (82.9 ns).
- On Store vs MutexStore: `Store` wins on Load/Churn/LoadOrStore
  (lock-free bit-spin), `MutexStore` wins on raw Store throughput
  (~9 % faster — futex is cheaper than the bit-spin for write-heavy
  workloads). Pin-heavy workloads depend on contention regime — see
  the dedicated `Lock + inc` table below.

### Memory — `B/op` and `allocs/op` per call

Same row order as above; lower is better. The columns hit the three
operations where memory cost matters most: bulk `Store`,
steady-state `LoadOrStore`, and the per-entry locking pattern.

| Implementation                       | Store                | LoadOrStore     | Lock + inc¹                     |
|--------------------------------------|---------------------:|----------------:|--------------------------------:|
| `sync.Map` (stdlib)                  |  117 B / 3 allocs    | 14 B / 1 alloc  | 16 B / 1 alloc (first insert)   |
| `xsync.Map` v4                       |   84 B / 1 alloc     |  0 B / 0 allocs | 16 B / 1 alloc (first insert)   |
| **`fsync.Map`**                      | **88 B / 0 allocs**ᵃ |  **0 B / 0 allocs** | **0 B / 0 allocs**          |
| **`fsync.Store`**                    |  **1 B / 0 allocs**ᵃ |  **0 B / 0 allocs** | **0 B / 0 allocs**          |
| **`fsync.MutexStore`**               |  **2 B / 0 allocs**ᵃ |  **0 B / 0 allocs** | **0 B / 0 allocs**          |

ᵃ **Amortized bucket allocation.** `fsync` allocates whole buckets,
not entries: one bucket covers 8 entries on `Map`, 32 on `Store`,
64 on `MutexStore`. On a stream of inserts the per-op alloc count
drops below 1 and rounds to 0. The amortized memory cost is the
`B/op` figure — most of `fsync.Map`'s 83 B is the bucket struct
divided by ~8, and `Store`'s 1 B is the bucket struct (~264 B)
divided by 32 amortized over millions of iterations on the same
hot slots.

¹ `sync.Map` and `xsync.Map` have no native `Lock(*V)` API; the
canonical Go workaround is to store `*mutexedEntry{mu sync.Mutex;
v V}` instead of `V` itself, then `Load(k)` and take
`e.mu.Lock()`. The 16 B / 1 alloc visible above is the caller's
`new(mutexedEntry)` paid once per first-time key (and amortized to
0 on subsequent Lock+inc on the same key). Both libs also allocate
an internal `entry{key, value}` struct on each first insert that
the benchmark amortizes invisibly across millions of iterations on
256 hot keys — the genuine first-insert cost is closer to **2
allocs ≈ 32 B**, not 1 alloc. `fsync.*.Lock` and `LockOrStore`
return a stable `*V` straight into the inline slot, no `*Entry`
indirection on any path, no first-insert spike, on every key from
the very first one.

### Memory footprint for 1M `int → int` entries

The per-op B/op above is the cost of *one* insert amortized. For the
total RAM held by a live, fully populated container, measure with
`runtime.ReadMemStats` after inserting 1M distinct `int → int`
entries from a single goroutine, GC forced before and after:

| Implementation                  | RAM        | per entry | Heap objects |
|---------------------------------|-----------:|----------:|-------------:|
| `map[int]int` + `sync.Mutex`    |   36.1 MB  |     37 B  |       4 106  |
| `xsync.Map[int, int]`           |   48.0 MB  |     50 B  |   1 011 568  |
| **`fsync.Store[int]`**          |  **8.8 MB**|   **9 B** |  **31 256**  |
| **`fsync.MutexStore[int]`**     |   17.3 MB  |    18 B   |     15 629   |
| **`fsync.Map[int, int]`**       |   72.2 MB  |    75 B   |    412 288   |
| `sync.Map` (`int → int` boxed)  |  115.9 MB  |    121 B  |   2 359 268  |

Reading:

- **`Store[V]`** is the most compact at **9 B/entry** — the dense
  integer index skips hashing and packs 32 `V` values per
  bucket alongside a single `lockused` word. Use it whenever
  your key is already a dense `int64`.
- **`MutexStore[V]`** doubles that to 18 B/entry because each
  64-slot bucket carries 64 `sync.Mutex` (one per slot, ~16 B
  each), the price of futex-parking instead of bit-spinning.
- **`Map[K, V]`** generic costs 75 B/entry vs xsync's 50 B —
  the extra 25 B comes from the 8-byte `pins` word, the 4-byte
  `state` field and the `sync.Mutex` per bucket that
  `xsync.Map` doesn't need (no `Lock(*V)` API there). The
  tradeoff: you pay for the bucket's writer serialization in
  exchange for a stable pointer back to the inline `V`. For
  read-only or struct{}-valued workloads, switch to `Set[K]`
  (42 B/entry, see Set section) which drops the `pins` word.
- **`sync.Map`** balloons to 121 B/entry because both key and
  value are boxed as `interface{}` — that's two 16-byte
  interface headers plus the entry struct, per entry, plus the
  read-mostly fast path's per-key entry pointer. The 2.4 M
  heap objects are 6× what `fsync.Map` carries.

For 1M-bit-style workloads, see the `Bitmap` section
(0.13 B/entry); for set-of-keys workloads, see the `Set`
section (42 B/entry); for FIFO queues, see the `Queue` section
(11 B/item).

### Per-entry locking pattern under three contention regimes

The Lock+modify+Unlock cycle behaves very differently depending on
contention. We report three regimes:

- **Moderate** — 256 hot keys, 12 goroutines (the canonical workload).
- **Uncontended** — each goroutine owns a private slice of 256 keys,
  no cacheline shared between cores.
- **Single-key (extreme)** — all 12 goroutines pound the *same* key 0.

| Implementation                                                  | Moderate    | Uncontended | Single-key   |
|-----------------------------------------------------------------|------------:|------------:|-------------:|
| `sync.Map[int, *{Mutex, int}]` — `Load` then take mutex         |     3.81 ns |          —  |           —  |
| `sync.Map[int, *{Mutex, int}]` — first insert (allocates entry) |     9.35 ns |          —  |           —  |
| `xsync.Map[int, *{Mutex, int}]` — `Load` then take mutex        |     2.16 ns |          —  |           —  |
| `xsync.Map[int, *{Mutex, int}]` — first insert (allocates entry)|     7.60 ns |          —  |           —  |
| **`fsync.Map.Lock` + `*p++` + `Unlock`**                        | **7.95 ns** | **3.77 ns** |          —  |
| **`fsync.Map.LockOrStore` + `*p++` + `Unlock`**                 | **8.56 ns** |          —  |           —  |
| **`fsync.Store.Lock` + `*p++` + `Unlock`**                      | **15.65 ns**| **1.07 ns** | **12.68 ns**|
| **`fsync.Store.LockOrStore` + `*p++` + `Unlock`**               | **19.37 ns**| **1.41 ns** | **15.90 ns**|
| **`fsync.MutexStore.Lock` + `*p++` + `Unlock`**                 | **5.11 ns** | **1.18 ns** | **51.00 ns**|
| **`fsync.MutexStore.LockOrStore` + `*p++` + `Unlock`**          | **5.24 ns** |          —  |           —  |

Notes:

- The reference rows hide one heap allocation per first-time key and a
  `*Entry` indirection on every access; fsync stores `V` inline so the
  bucket *is* the entry. No `*Entry` allocation at any point.
- **Honest disclosure on the steady-state Load+mutex path:** once the
  `map[K]*{mu, V}` pattern is warm and no new entries get inserted,
  `xsync.Map[*{mu,v}].Load + e.mu.Lock + e.v++ + e.mu.Unlock`
  (**2.16 ns** here) **beats `fsync.Map.Lock + *p++ + Unlock`
  (7.95 ns)** by 3.7×. The reasons are real: xsync's Load has no
  pin-word to read, and a pointer-load + sync.Mutex.Lock on an
  uncontended mutex compiles into very few atomics on Zen 4. The
  trade-off is one heap allocation per first-time key on the xsync
  path, plus the indirection on every access. `fsync.Map.Lock`
  earns its 7.95 ns by giving you a stable `*V` directly into the
  bucket, no `*Entry`, no allocation, and the same Lock semantics
  the moment the key is first seen.
- **Single-key:** the standout finding. `fsync.Store.Lock` at
  **12.68 ns** is **4× faster than `fsync.MutexStore.Lock` (51 ns)**
  under extreme single-key contention, because the Load-then-CAS
  spin keeps the cacheline in Shared state during the hold (one
  Modified transition per acquire, not per retry). Before this
  optim the same workload took ~370 ns on `Store.Lock` (~×30
  improvement). On the moderate regime, however, `MutexStore`
  still wins (5.1 vs 15.6 ns): the 32 slots that share `lockused`
  on a Store bucket end up cache-bouncing across multiple
  goroutines, while each `MutexStore` slot has its own 64-byte
  mutex cacheline.
- **Uncontended:** `fsync.Store.Lock` (1.07 ns) is the fastest of
  all, *including* a plain `xsync.Map` Load (1.04 ns) — the
  pin/check/unpin cycle of a Load-then-CAS roughly matches a Load
  here. `fsync.MutexStore.Lock` (1.18 ns) is slightly behind
  because of the mutex.Lock/Unlock atomics. `fsync.Map.Lock`
  (3.77 ns) carries the bucket-walk overhead.
- `fsync.Map.LockOrStore` (8.56 ns) is competitive with
  `xsync.Map[*{mu,v}].LoadOrStore` (7.60 ns) and beats
  `sync.Map.LoadOrStore` (9.35 ns) on insert.
- **Rule of thumb:**
  - **Moderate / read-heavy contention** → `fsync.MutexStore` for
    Lock-heavy workloads, `fsync.Store` for everything else.
  - **Extreme contention on a single hot key** → `fsync.Store` wins.
  - **No contention** → `fsync.Store` everywhere.

### `Map[string]int` (2048 keys preloaded, GOMAXPROCS=12)

| Implementation                  | ReadOnly    |
|---------------------------------|------------:|
| `sync.Map[string]int`           |     3.44 ns |
| `xsync.Map[string]int` v4       | **2.04 ns** |
| **`fsync.Map[string]int`**      | **3.14 ns** |

(Same picture as for `int` keys, just shifted higher by the cost of
`maphash.String` on every Load. `xsync.Map` keeps its tighter-bucket
lead; `fsync.Map` lands between sync and xsync. See the per-procs
breakdown below for scaling behavior.)

### Scaling from `GOMAXPROCS=1` to `12` — drop-in for a non-concurrent map?

A concurrent map's value is only as good as its single-threaded cost.
A library that you have to swap out the moment the workload becomes
serial is much less attractive than one that stays competitive with
`map[K]V` (no lock) on the slow path. So we ran `ReadOnly` and
`LoadOrStore` against `map[K]V`, `sync.Map`, `xsync.Map`,
`fsync.Map`, `fsync.Store`, `fsync.MutexStore` at every
`GOMAXPROCS` from 1 to 12 (`go test -cpu=1,2,4,6,8,10,12`). Numbers
are median of 2 runs at `-benchtime=2s`; lower is better.

#### `ReadOnly` (2048 keys preloaded, parallel Load)

| GOMAXPROCS | `map[int]int` | `sync.Map` | `xsync.Map` | `fsync.Map` | `fsync.Store` | `fsync.MutexStore` |
|---:|---:|---:|---:|---:|---:|---:|
|  1 | **5.79 ns** | 11.66 ns | 3.89 ns | **5.76 ns** | 3.66 ns | 3.96 ns |
|  2 |     4.74 ns |  6.25 ns | 2.11 ns |     3.10 ns | 3.02 ns | 2.71 ns |
|  4 |     2.83 ns |  3.88 ns | 1.30 ns |     1.90 ns | 2.46 ns | 1.42 ns |
|  6 |     2.98 ns |  2.95 ns | 1.06 ns |     1.49 ns | 1.65 ns | 1.17 ns |
|  8 |     1.53 ns |  2.99 ns | 1.01 ns |     1.50 ns | 1.19 ns | 1.08 ns |
| 10 |     2.04 ns |  2.99 ns | 1.03 ns |     1.51 ns | 1.00 ns | 1.04 ns |
| 12 |     1.84 ns |  2.98 ns | 1.03 ns |     1.51 ns | 0.82 ns | 0.98 ns |

#### `LoadOrStore` (2048 keys preloaded, parallel get-or-set)

| GOMAXPROCS | `sync.Map` | `xsync.Map` | `fsync.Map` | `fsync.Store` | `fsync.MutexStore` |
|---:|---:|---:|---:|---:|---:|
|  1 | 33.88 ns | 5.53 ns | **6.37 ns** | 4.41 ns | 4.67 ns |
|  2 | 18.74 ns | 2.94 ns |     3.35 ns | 2.43 ns | 2.59 ns |
|  4 | 11.75 ns | 1.82 ns |     2.07 ns | 1.56 ns | 1.61 ns |
|  6 |  9.47 ns | 1.43 ns |     1.62 ns | 1.33 ns | 1.38 ns |
|  8 |  9.33 ns | 1.41 ns |     1.63 ns | 1.82 ns | 1.29 ns |
| 10 |  9.27 ns | 1.40 ns |     1.64 ns | 1.37 ns | 1.34 ns |
| 12 |  9.95 ns | 1.38 ns |     1.66 ns | 1.19 ns | 1.26 ns |

The headline: **at `GOMAXPROCS=1`, `fsync.Map.ReadOnly` (5.67 ns) is
within 3 % of a plain `map[int]int` (5.85 ns)** — meaning the
pin-word read and the bucket walk are entirely absorbed on the
single-threaded path. So `fsync.Map` is a credible drop-in for a
`map[K]V` even when the workload is *not* concurrent.

Other observations:

- Everyone plateaus around `GOMAXPROCS=6–8` (the test machine has 6
  physical cores with SMT — 12 logical threads). Past that the
  parallel benefits saturate.
- The lockless `map[int]int` baseline scales only ×3.2 (5.85 → 1.82
  ns) because RunParallel still has all goroutines hitting one
  shared map, with false sharing on the bucket cachelines.
- `fsync.Map` scales ×4.4 (5.67 → 1.28 ns), `xsync.Map` ×4.3 (3.92 →
  0.91 ns), `fsync.Store` ×4.6 (3.63 → 0.79 ns). All three sit
  above the lockless baseline at 12 goroutines because they spread
  hot keys across many cachelines.
- `fsync.Map` keeps a steady ~40 % gap behind `xsync.Map` at every
  GOMAXPROCS — the cost of the pin word that `Lock(*V)` needs. The
  gap does not widen at scale.
- `sync.Map.LoadOrStore` is **5–8× slower** than `fsync.Map.LoadOrStore`
  at every GOMAXPROCS. If your code calls `LoadOrStore` in a hot loop,
  this single number probably justifies the migration on its own.

#### `Map[string]int` — same scaling but no integer-hash shortcut

The int-keyed tables benefit from a wyhash-style 128-bit multiply for
integer keys. The string-keyed version goes through `maphash.String`
(specialized, faster than `maphash.Comparable` for string), but
still pays for hashing the bytes — which is why the numbers below are
markedly higher than their `int`-keyed counterparts. Comparable
results (median of 2 runs at `-benchtime=2s`):

`ReadOnly`:

| GOMAXPROCS | `map[string]int` | `sync.Map` | `xsync.Map` | `fsync.Map` |
|---:|---:|---:|---:|---:|
|  1 |  **8.21 ns** | 13.79 ns | 7.91 ns | **11.75 ns** |
|  4 |     2.51 ns |  4.39 ns | 2.66 ns |     3.92 ns |
| 12 |     2.59 ns |  3.44 ns | 2.04 ns |     3.14 ns |

(`map[string]int` is the lockless baseline, not re-measured this run — it scales as in `int`-keyed tables.)

`LoadOrStore` (steady state, all keys preloaded):

| GOMAXPROCS | `sync.Map` | `xsync.Map` | `fsync.Map` |
|---:|---:|---:|---:|
|  1 | 47.65 ns | 10.30 ns | **12.66 ns** |
|  4 | 16.48 ns |  3.43 ns |     4.22 ns |
| 12 | 14.88 ns |  2.62 ns |     3.32 ns |

`StoreWithAlloc` — inserts a fresh `strconv.Itoa(n)` key on every
iteration. The strconv allocation cost is *included* in the per-op
number on purpose: this is the realistic cost of seeing a fresh
string key from outside.

| GOMAXPROCS | `sync.Map`             | `xsync.Map`            | `fsync.Map`            |
|---:|-----------------------:|-----------------------:|-----------------------:|
|  1 |  542 ns / **4 allocs** | 368 ns / **2 allocs**  | **370 ns / 1 alloc**   |
| 12 |  109 ns / 4 allocs     |  76 ns / 2 allocs      | **106 ns / 1 alloc**   |

Highlights:

- At `GOMAXPROCS=1`, `fsync.Map` (11.71 ns) is 43 % slower than the
  lockless `map[string]int` (8.21 ns), but `sync.Map` is 63 %
  slower, so `fsync.Map` still wins against the canonical drop-in
  on serial workloads.
- `xsync.Map` keeps the same ~30-40 % edge on string ReadOnly it has
  on int — the pin-word read overhead applies equally to both key
  types.
- `LoadOrStore`: `sync.Map` boxes the value into an `interface{}` and
  carries 4 heap allocations per call; `fsync.Map` has 1 (the
  `strconv.Itoa` only). At 1 G the alloc cost dominates everything
  else and the gap shrinks at 12 G as GC amortizes across cores.
- On the `StoreWithAlloc` workload, `fsync.Map` carries the
  fewest allocations (1 — the unavoidable `strconv.Itoa`), against
  4 for `sync.Map` and 2 for `xsync.Map`. At low GOMAXPROCS that
  alloc count translates to ns/op directly (430 vs 559 vs 370 at
  1 proc — xsync wins on raw speed thanks to its faster Store
  path); at 12 procs all three converge around 90–120 ns/op as
  the GC amortizes across cores. The 1-alloc-per-insert floor of
  `fsync.Map` becomes decisive on long-running insertion-heavy
  workloads where GC pause time matters.

### `Set[K]` — concurrent set of comparable keys

`Set[K]` is a dedicated specialization (not a wrapper) with its own
`bucketSet[K]` layout: 8 inline `K` slots, one `meta atomic.Uint64`
packing 8 h7 tags, and a writer mutex. **No `pins` word, no
`values` array, no seqlock pattern** — the key alone *is* the
entry, and the meta tag scan + key compare is the entire Contains
hot path. Compared to a `Map[K, struct{}]` wrapper, this drops 8
bytes per bucket (the unused `pins` word) and shaves the seqlock
overhead the generic Map carries to support `Lock(*V)`.

API (zero value usable):

```go
NewSet[K]() *Set[K]
(*Set[K]).Grow(estimatedItems int) *Set[K]
(*Set[K]).Add(k K) (added bool)
(*Set[K]).Contains(k K) bool
(*Set[K]).Remove(k K) bool
(*Set[K]).Len() int
(*Set[K]).Range(f func(K) bool)
(*Set[K]).Clear()
```

Benchmarks on `int` keys (medians of 3 × `-benchtime=1s`, same
Ryzen 5 8540U, 12 threads):

| Implementation             | Add                 | Contains    | Add+Remove           | Range/key   |
|----------------------------|--------------------:|------------:|---------------------:|------------:|
| `map[int]struct{}`+`Mutex` | 214 ns / 0 allocs   |      —      |                  —   |          —  |
| `sync.Map` (stdlib)        | 84.7 ns / 2 allocs  |     2.81 ns |                  —   |     5.13 ns |
| `xsync.Map[K, struct{}]`   | 80.1 ns / 1 alloc   | **0.93 ns** | 30.2 ns / **1 alloc**|     5.61 ns |
| `fsync.Map[K, struct{}]`   | 55.0 ns / 0 alloc   | 1.48 ns     | 35.1 ns / 0 alloc    |     2.55 ns |
| **`fsync.Set`**            | **51.9 ns / 0 alloc** | **1.36 ns** | **33.7 ns / 0 alloc**| **2.59 ns** |

Footprint for 1M `int` keys (measured via `runtime.ReadMemStats`):

| Implementation             | RAM       | per entry | Heap objects |
|----------------------------|----------:|----------:|-------------:|
| `sync.Map` (`struct{}{}` boxed) | 108.4 MB  | 113 B     |  1 860 161   |
| `xsync.Map[int, struct{}]` |  47.9 MB  |    50 B   |  1 009 678   |
| `fsync.Map[int, struct{}]` |  47.0 MB  |    49 B   |    412 288   |
| **`fsync.Set[int]`**       | **40.8 MB** | **42 B**  |  **412 288** |

`sync.Map` is **2.7× heavier** than the others because it stores
both key and value as `interface{}` (16 B + 16 B box overhead per
entry on top of the entry struct itself), and it also leaks 5×
more heap objects to scan on every GC cycle. `fsync.Set` and
`fsync.Map[K, struct{}]` share the same bucket count (412k) but
Set's bucket is 8 B smaller (no `pins` word), which compounds to
~6 MB saved at 1M entries.

Readings:

- **vs `fsync.Map[K, struct{}]`:** Set is ~6 % faster on Add,
  ~8 % faster on Contains, **13 % less RAM** (no `pins` word in
  the bucket). The specialization is small but real — Map's
  seqlock and pin word are dead weight in the Set use case.
- **vs `xsync.Map[K, struct{}]`:** Set saves **1 alloc per
  insert** (Add 52 vs 80 ns) and is 15 % lighter on RAM. xsync
  still wins Contains (0.93 vs 1.36 ns) thanks to a tighter
  bucket layout with no state-check on the hot path.
- **vs `sync.Map`:** ~1.6× faster on Add, with 2 allocs/op cut to
  0; ~2× faster on Contains.
- **Range/key at 2.59 ns** is **~2.2× faster than `xsync.Map`**
  (5.61 ns) thanks to the inline `[8]K` bucket layout — one
  cacheline read per 8 keys.

When to pick which: use `fsync.Set` whenever you'd otherwise write
`map[K]struct{}` + a mutex, or `sync.Map` with `struct{}{}` values.
Use `xsync.Map[K, struct{}]` if your workload is read-dominated on
ints and the 0.4 ns gap on `Contains` is the bottleneck. Use plain
`map[K]struct{}` only when the workload is single-goroutine.

### `Bitmap` — dense bit set on `int64` indexes

`Bitmap` is the specialized version of `Store[bool]`: each bucket
packs **8 `atomic.Uint64` words = 512 bits = exactly one cacheline**
(64 bytes), with no `values` array at all. The per-bit memory cost
drops from ~1.25 bytes (`Store[bool]`: `[32]bool` + a `lockused`
word per 32 bits) to ~0.13 byte — a **~10× win on steady-state
memory**, with **8× fewer heap objects** for the same index range
(one bucket covers 512 bits instead of 64). Reads and writes are
each one atomic op on the bucket word selected by 3 bits of the
index, plus a single bitmask — no pin pattern, no values[] write.

The constructor takes a `start int64` like `Store`: `Bitmap.Set(i)`
addresses absolute slot `i - start`, and calls with `i < start` are
no-ops. The zero value is usable (start defaults to 0). There is no
`Lock(*V)` API — a pointer to a single bit does not exist, and
Set / Unset / Has / Toggle are already lock-free single-atomic
operations on the bucket word.

API:

```go
NewBitmap(start int64) *Bitmap
(*Bitmap).Grow(maxIndex int64)
(*Bitmap).Set(i int64) (added bool)     // true if transitioned 0→1
(*Bitmap).Unset(i int64) (removed bool) // true if transitioned 1→0
(*Bitmap).Has(i int64) bool
(*Bitmap).Toggle(i int64) (nowSet bool)
(*Bitmap).Len() int                     // popcount over all buckets
(*Bitmap).Range(f func(i int64) bool)   // ascending order, weakly consistent
(*Bitmap).Clear()
```

Benchmarks on `int64` keys (medians of 3 × `-benchtime=500ms`,
same Ryzen 5 8540U, 12 threads):

| Implementation                  | Set                  | Has          | Set+Unset            | Range/key   |
|---------------------------------|---------------------:|-------------:|---------------------:|------------:|
| `map[int64]bool` + `sync.Mutex` | 192 ns               |     25.3 ns  |                  —   |          —  |
| `xsync.Map[int64, bool]` v4     | 49.9 ns / **1 alloc**|     0.98 ns  | 31.8 ns / **1 alloc**|     4.53 ns |
| `fsync.Store[bool]`             | 15.4 ns / 0 alloc    |     2.57 ns  |  **6.35 ns / 0 alloc**|    1.47 ns |
| **`fsync.Bitmap`**              | **1.68 ns / 0 alloc** | **0.52 ns** |     23.5 ns / 0 alloc| **1.53 ns** |

Readings:

- **`Has` at 0.52 ns/op** is the fastest lookup of the whole package:
  ~5× faster than `Store[bool].Load` (2.57 ns) and ~2× faster than
  `xsync.Map.Load` (0.98 ns). A single `Uint64.Load` + bitmask, no
  pin-word, no per-slot indirection.
- **`Set` at 1.68 ns** is **9× faster** than `Store[bool].Store`
  (15.4 ns) and **~30× faster** than `xsync.Map.Store` (49.9 ns).
  One atomic `Or` on the chosen word in the bucket; write-coalescing
  on a hot cacheline as a side-benefit of the 8-word bucket.
- **`Range/key` at 1.53 ns** is the package record. Iteration uses
  `bits.TrailingZeros64` to walk only set bits within each bucket
  word — no per-slot scan. Range scales linearly from 1K to 1M
  bits at a steady 1.5 ns/bit, and degrades gracefully on sparse
  inputs: ~1.8 ns/bit at 1 bit per word, ~7 ns/bit at 1 bit per
  bucket, ~10 ns/bit at 1 bit per 8 buckets. The worst-case
  fixed cost is "scan an empty bucket" (~4 ns: 8 atomic Loads
  + zero check), and unallocated buckets in the table are
  skipped via a nil-pointer check so very sparse layouts don't
  pay a per-empty-slot tax. See `benchs/bitmap_bench_test.go`
  for the full density/scaling matrix.
- **`Set+Unset` (rolling window of 1024 indexes) is the one
  weakness**: 23.5 ns vs `Store[bool]`'s 6.35 ns. With 512 bits per
  bucket the rolling window concentrates pressure on just 2 buckets
  (vs 32 for `Store[bool]`'s 32-slot buckets), so the cacheline
  ping-pong between 12 cores hits harder. On *non-contended* writes
  (distinct keys per worker), Bitmap stays well ahead.

Memory footprint comparison (heap delta and live `HeapObjects`
measured via `runtime.ReadMemStats` after inserting 1M distinct
indexes from a single goroutine, GC forced before and after):

| Implementation                  | RAM for 1M bits | Heap objects | per entry |
|---------------------------------|----------------:|-------------:|----------:|
| `map[int64]bool` + `sync.Mutex` |        36.1 MB  |        4 121 |     37 B  |
| `xsync.Map[int64, bool]`        |        48.0 MB  |    1 011 074 |     50 B  |
| `fsync.Map[int64, bool]`        |        47.0 MB  |      412 288 |     49 B  |
| `fsync.Store[bool]`             |         1.7 MB  |       31 254 |      1 B  |
| **`fsync.Bitmap`**              |    **0.14 MB**  |    **1 958** |   **~0 B** (~0.13) |

`fsync.Map[int64, bool]` and `xsync.Map[int64, bool]` land within
~2 % of each other on RAM but the hashed maps both leak heap
objects by the hundreds of thousands (one per inserted entry) —
that's the GC-mark cost the inline-bucket designs sidestep.
`fsync.Store[bool]` skips hashing and packs 32 bools per
bucket — already **~30× less RAM** and **~30× fewer objects**.
`Bitmap` then packs 512 bits per cacheline-aligned bucket, no
values array — another **~12× win on top of `Store[bool]`** and
**~340× smaller than the hashed maps**, with only **~2 000 heap
objects** for 1M bits (a 500× reduction vs the hashed maps'
~500k+ objects to scan on every GC cycle).

When to pick which: use `Bitmap` whenever the workload reduces to
"is index `i` set/unset" on a dense or sparse `int64` domain
(presence flags, free-slot maps, bloom-filter-like structures,
visited sets in graph algorithms). Use `Store[bool]` if you need
the `Lock(*V)` semantics or are okay with the 10× memory cost in
exchange for ~3× faster write-then-clear churn cycles. Use
`xsync.Map[int64, bool]` only if your indexes are extremely sparse
across the int64 range AND you can't afford the virtual address
reservation that `Bitmap.Grow` makes.

### `Queue[T]` and `MutexQueue[T]` — unbounded MPMC FIFO

`Queue[T]` is a lock-free multi-producer / multi-consumer FIFO built
from 64-slot inline segments, chained as needed. **It is unbounded
and never blocks**: `Enqueue` always succeeds (a new segment is
linked when the current tail fills), `Dequeue` returns
`(zero, false)` only when the queue is genuinely empty, never as a
back-pressure signal. Fully drained segments become unreachable and
are reclaimed by the GC — the queue's working memory tracks the
live element count, not the high-water mark.

The design target is the **fan-out / fan-in pattern** common in iPaaS
glue code: an arbitrary number of producers post work, an arbitrary
number of consumers drain it, and no one wants to size a buffered
channel up-front or pay for a `select` per send. `Queue` is what you
reach for when you'd otherwise write `chan T` with a guess at the
capacity. `MutexQueue[T]` is the simple mutex-guarded baseline
included for comparison (same role as `MutexStore` vs `Store`); not
part of the intended public API yet.

Three workloads benchmarked (`./benchs/queue_bench_test.go`):

- **SerialPingPong** — single goroutine, one `Enqueue` then one
  `Dequeue` per iteration. Pure per-op overhead, zero contention.
- **MPMC (4P + 4C)** — four producers each enqueue while four
  consumers each dequeue, mixed contention on both ends.
- **SPSC (1P + 1C)** — one producer paired with one consumer,
  workload `xsync.SPSCQueue` is tuned for.

| Implementation                         | SerialPingPong | MPMC (4P+4C) | SPSC (1P+1C) |
|----------------------------------------|---------------:|-------------:|-------------:|
| `chan T` (buffered, capacity 1024)     |        17.8 ns |      50.4 ns |      26.8 ns |
| `xsync.MPMCQueue` (bounded, 1024)      |     **7.82 ns**|       141 ns |           —  |
| `xsync.SPSCQueue` (bounded, 1024)      |     **3.31 ns**|           —  |      31.9 ns |
| `xsync.UMPSCQueue` (unbounded MPSC)    |              — |           —  |      19.2 ns |
| **`fsync.Queue`**                      |     **8.68 ns**| **32.1 ns**  | **9.16 ns**  |
| **`fsync.MutexQueue`** (baseline)      |        10.4 ns |      57.2 ns |      19.1 ns |

Readings:

- **MPMC**: `fsync.Queue` (32.1 ns) is **~4.4× faster than
  `xsync.MPMCQueue`** (141 ns) and ~1.6× faster than a buffered `chan`
  (50.4 ns). The segment-per-block design lets producers
  fetch-add into the tail segment while consumers CAS-advance the
  head segment, with no global cursor cacheline ping-pong.
- **SPSC**: `fsync.Queue` (9.16 ns) is **~3.5× faster than
  `xsync.SPSCQueue`** (31.9 ns) and ~3× faster than `chan` (26.8
  ns) — the producer and consumer cursors sit on disjoint
  cachelines via padding, so the single-pair regime sees no
  ping-pong at all.
- **SerialPingPong**: `xsync.SPSCQueue` wins (3.31 ns) — its
  bounded ring + zero atomics on the uncontended fast path is hard
  to beat in pure single-goroutine throughput; `fsync.Queue`
  (8.68 ns) trades that for being unbounded and MPMC-safe.
  Buffered `chan` (17.8 ns) loses to both even here.
- **Memory** (per-op): all queues report **0 allocs/op** thanks to
  segment amortization (`fsync.Queue` allocates one 64-slot
  segment per 64 elements). For total RAM at steady state, see
  the footprint table below.

Footprint for 1M `int` items pending in the queue (no dequeue,
measured via `runtime.ReadMemStats`):

| Implementation                  | RAM       | per item | Heap objects |
|---------------------------------|----------:|---------:|-------------:|
| `chan int` (capacity 1M)        |   7.63 MB |    8 B   |          1   |
| `xsync.SPSCQueue` (cap 1M)      |   7.63 MB |    8 B   |          2   |
| **`fsync.MutexQueue`**          | **8.58 MB**| **9 B** | **15 626**   |
| **`fsync.Queue`**               |**10.49 MB**|**11 B** |  **15 626**  |
| `xsync.MPMCQueue` (cap 1M)      |  68.67 MB |   72 B   |          2   |

`fsync.Queue` is **6.5× lighter than `xsync.MPMCQueue`** (11 vs
72 B/item) — xsync's Vyukov MPMC bounded queue pads each slot to
a full cacheline (64 B + an atomic sequence) to avoid false
sharing between producer and consumer cursors. `fsync.Queue` only
pads the head/tail segment pointers, so the per-item cost stays
close to the raw `T` size. `chan` and `xsync.SPSCQueue` are
single-allocation ring buffers (1–2 heap objects total), so they
beat fsync's segment-chained design on object count — but they
are bounded; fsync grows on demand and frees consumed segments
to the GC.

When to pick which: use `fsync.Queue` when producers and consumers
both scale and you can't pre-size; use `xsync.SPSCQueue` if you
have **exactly one** producer **and one** consumer and bounded
back-pressure is fine; use a buffered `chan` when you also need
`select` semantics. `MutexQueue` exists as a sanity baseline and
to handle very-low-contention call sites where the cost of an
uncontended `sync.Mutex` is competitive.

## Methodology

Benchmarks compare against `sync.Map` (stdlib) and
`github.com/puzpuzpuz/xsync/v4`, the two most-used alternatives in the
Go ecosystem. The "plain `map[K]V` + `sync.Mutex` / `sync.RWMutex`" rows
show the cost of the naïve approach so the reader can calibrate the
others. All variants share identical preload size, key set and parallel
access pattern, so the comparison stays fair.

All bench code lives in `./benchs/` (`fsync_bench_test.go`,
`gomap_bench_test.go`, `sync_bench_test.go`, `xsync_bench_test.go`,
`mutexed_bench_test.go`, `queue_bench_test.go`). The standalone
string-hash microbench (maphash vs FNV-1a vs xxh3 vs wyhash) lives
in `./hashbench/` with its own `go.mod` to keep the parent module
dependency-free.

Reproduce with:

```sh
# Full suite (sync.Map.Store may OOM at 5 s × 3 on machines under 16 GB)
go test -bench=. -benchtime=5s -count=3 -run='^$' ./benchs/

# Same, including per-op memory & allocation counters
go test -bench=. -benchtime=5s -count=3 -benchmem -run='^$' ./benchs/

# Lighter variant (safe on 8 GB, still 3 runs)
go test -bench=. -benchtime=2s -count=3 -run='^$' ./benchs/
```

`b.N` is capped at 10⁹ inside the testing framework, so workloads that
land below ~5 ns/op hit the cap before consuming the full
`-benchtime` budget. To go past that cap, force the iteration count:
`-benchtime=10000000000x`.

## Implementation notes

### Inline `V` vs heap `*entry`: the tradeoff with `-race`

A point worth being explicit about, because it's the structural
difference between `fsync.Map` and `xsync.Map` / `sync.Map`:

- **`xsync.Map` (and stdlib `sync.Map`)** allocate a heap entry
  per insert (`*entry{key, value}`). Once an entry is published,
  it is **never mutated**: a Store creates a new `*entry` and CAS-es
  the pointer over the old one (which becomes garbage). So a `Load`
  reads the value field of a struct that is, from the moment of
  publication, immutable. The Go race detector therefore **never
  sees a write race** there, regardless of what `V` is. The price
  is one heap allocation per first-time insert.

- **`fsync.Map`** keeps `[8]V` inline in the bucket (no `*entry`,
  no allocation on insert). The seqlock on `pins` makes the read
  observably atomic *semantically*: a Load only returns a `V` it
  observed between two pin-clear snapshots, so torn-reads under a
  concurrent Lock holder are impossible. But the read is a plain
  struct-field read, not an `atomic.Load` of a pointer — and the
  Go runtime race detector does NOT understand the seqlock as a
  synchronization primitive. So under `go test -race`, a `Load`
  running concurrently with a `Store` / `Lock`+mutate on the same
  slot may be **flagged as a data race even though the seqlock
  guarantees the returned `V` is semantically valid**.

  This is exactly what `TestMapLoadDuringLock` documents: that
  test exists, uses the right semantics, but is compiled out under
  `-race` via `//go:build !race` because the runtime would flag
  the inline-`V` read.

**What it means for the caller**:

- If you use `fsync.Map` from production code (no `-race`), no
  difference: the `V` returned by `Load` is correct, the seqlock
  retries on Lock/Unlock cycles, and you pay zero allocation per
  Store.
- If you store a *complex* `V` like `map[X]Y` or `*Sub` and you
  ever Lock + mutate the inner state, the race detector will
  reliably catch your callers' downstream races on that inner
  state — which is what you want. `fsync.Map` doesn't try to hide
  that responsibility from you; it stays out of the way.
- If your test suite runs `go test -race` AND you ever Store the
  same key from two goroutines concurrently (a legitimate pattern),
  expect the inline-V read in `Load` to occasionally surface in
  the race report. The semantic guarantee holds; the report is a
  consequence of the inline layout, not of a real torn read.

The "no heap allocation" property is the entire point of inline
`V` and the reason `Lock` can hand back a stable `*V`. If you want
the alloc-and-immutable behaviour of `xsync.Map`, store a pointer:
`fsync.Map[K, *Sub]` and your callers do `*p = newSub` themselves.
You'd give back the zero-alloc-per-write but you'd get the
race-detector silence in exchange.

## Design history & explored alternatives

This section keeps a quick record of the structural decisions and the
attempts that were tried and rolled back. It is here for two reasons:
to document the tradeoffs that shaped the current implementation, and
to spare a future contributor the cost of re-exploring an idea that
was already benched and rejected.

### Hash map architecture — three successive designs for `Map[K,V]`

1. **Slot+chain head table** (May 2026, abandoned). Open addressing
   with chained slots via a delta-encoded `iln` field; heads on odd
   slot indices, links on even ones; value table reused from
   `Store[V]` for `*V` stability. Incremental rebuild with a
   delegation protocol (`ilnNeedRebuild` bit) and helper goroutines.
   Under heavy concurrent writes it **never converged**: insertions
   outpaced the rebuild, the old table saturated, and `TestBigMap`
   either overflowed or timed out. Three rescue strategies were
   tried (delegation, single-migrator serialization, cooperative
   per-Store sweep) — all of them missed the same root cause.

2. **Bucket map over `Store[V]`** (June 2026, abandoned). The head
   table became an array of 32-slot buckets in the xsync style
   (control word with used+lock bits per slot), but values still
   lived in a `Store[V]` underneath. Cooperative resize bucket-by-
   bucket with state transitions `Open → Frozen → Moved`. Working
   correctness under `-race` required fixing a subtle
   lookup-then-claim race on `bucketInsert`. The design was
   correct but layered — every op paid a `Store[V]` indirection on
   top of the bucket map.

3. **Bucket-direct, duplicate-on-pin** (current). Heavily inspired by
   `puzpuzpuz/xsync`: each bucket holds `[8]V` inline alongside
   `[8]K`, eight `h7` tag bytes packed in `meta`, and a
   `pins/seq` word. Rebuild **splits** a bucket when no pin is
   currently held, and **duplicates** it (same pointer published in
   two new-table slots) when at least one pin is held — so the `*V`
   addresses handed out by `Lock` survive resizes. Convergence is
   guaranteed by construction: no migration of values, only of
   bucket pointers.

### Cursor / Unlock evolution

- **v0:** `m.Unlock(cur)` and `s.Unlock(i)` — procedural style.
  Callers had to keep a reference to the container alongside the
  cursor.
- **v1:** `cur.Unlock()` method on the cursor. New `StoreCursor[V]`
  and `MutexStoreCursor[V]` types introduced for `Store` and
  `MutexStore` (`Cursor[K,V]` already existed on `Map`). Procedural
  `Unlock`s removed.
- **v2:** cursors hold `*bucket` directly instead of `(*store, i)`.
  Buckets are never relocated (the Store/MutexStore resize logic
  has this invariant already, and Map's duplicate-on-pin keeps it
  too), so the cursor's only job — releasing the pin — does not
  need a table walk anymore. `cur.Unlock()` becomes one atomic
  `And`/`mutex.Unlock` with zero lookups.

### Spin loop optimizations (Store)

- **`Store.Lock` and `Store.LockOrStore`**: rewritten from
  unconditional `Or(lockbit)` retry loops to **Load-then-CAS**.
  Read the shared `lockused` word in the M(O)ESI Shared state
  while another goroutine holds the pin; only attempt the CAS
  when the read says the pin is free. Cacheline only enters
  Modified when a goroutine *actually wins* the lock, not on
  every wasted retry. Single-key contention on 12 goroutines:
  Lock dropped from **372 ns to 12.5 ns** (×30), LockOrStore from
  ~370 ns to 16.5 ns. Uncontended path costs +0.13 ns (Lock) and
  +0.5 ns (LockOrStore) — one extra atomic Load — happily paid.

- **`Store.Load`** — same optimization was attempted, benched, and
  **rolled back**. Single-key would have gained the same ×24 win
  (244 → 10 ns) but ReadOnly regressed by ~29 % and ReadHeavy by
  ~51 %. Reason: under typical Load workloads, `atomic Or` on
  bit-disjoint slots interleaves at the bus-reservation level
  (the CPU can fuse concurrent ORs into a single memory
  transaction), while a `CAS` retry loses every race. Pin
  contention being rare in real workloads, the existing Or+And
  remained the better default. The bench file
  (`benchs/mutexed_bench_test.go`) keeps the single-key and
  uncontended Load benches so the regression rule is reproducible
  if anyone revisits.

- **`Store.Delete`**: one bare `And(^usebit)` — already a single
  atomic op, not optimized further.

### Open exploration (not implemented)

- **`MapOnStore[K,V]`** — secondary indexes attached to a shared
  `Store[V]`. The applicative use case (multi-key lookup over an
  ERP entity store) is strong; no Go library known to provide it
  in-memory and concurrent. Sketched in design discussion: hook
  list on Store, snapshot+recompute of the extractor at Lock /
  Unlock to detect re-indexing needs, fast path zero-hook to
  preserve cost when no MapOnStore is attached. Estimated
  overhead per Lock/Unlock cycle: ~2-3 ns per attached hook
  (extractor is opaque to the inliner). Not implemented.

- **`StoreIter[V]`** — read-only cursor with First/Last/Next/
  Prev/Seek over the dense integer index. Distinct type from
  `StoreCursor[V]` (pin handle) so the two concerns stay
  separated; bridge via `it.Lock() (*V, StoreCursor[V], bool)`
  if the caller wants to mutate. Not implemented; the existing
  `Range(f)` covers most use cases for now.
