# fsync — fast concurrent containers for Go

[![ci](https://github.com/aytechnet/fsync/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/aytechnet/fsync/actions/workflows/test.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/aytechnet/fsync.svg)](https://pkg.go.dev/github.com/aytechnet/fsync)
[![Go Coverage](https://img.shields.io/codecov/c/github/aytechnet/fsync/main?color=brightcolor)](https://codecov.io/gh/aytechnet/fsync)
[![Go Report Card](https://goreportcard.com/badge/github.com/aytechnet/fsync)](https://goreportcard.com/report/github.com/aytechnet/fsync)

`fsync` is a Go 1.25 library of high-performance concurrent containers
built for the DyaPi iPaaS platform. It offers three primitives, each
targeted at a different niche:

| Type            | Key              | Value layout    | Niche                                  |
|-----------------|------------------|-----------------|----------------------------------------|
| `Store[V]`      | `int64`          | inline `[32]V`  | dense integer-indexed store, lock-free |
| `MutexStore[V]` | `int64`          | inline `[64]V`  | same, mutex per slot (contention)      |
| `Map[K,V]`      | `K comparable`   | inline `[8]V`   | general concurrent hash map            |

## What makes it different — `Lock` returns a stable `*V`

The signature feature is that `Lock` pins a single entry and returns a
**stable `*V` pointer** to the value stored *inline* in the container:

```go
p, cur, ok := m.Lock(key) // p is a pinned *V into the bucket itself
*p++                       // mutate in place under exclusive ownership
cur.Unlock()               // release the pin
```

This replaces the canonical Go pattern of "map of mutexed entries":

```go
e, _ := m.LoadOrStore(key, &Entry{}) // one heap alloc per new key
e.mu.Lock()
e.v++                                 // chases a pointer
e.mu.Unlock()
```

In `fsync.Map` / `fsync.Store` / `fsync.MutexStore`, `V` lives in the
bucket: no per-entry allocation, no `*Entry` indirection, and the
pinned address survives rebuilds (the duplicate-on-pin policy keeps
pinned buckets addressable in both old and new tables until released).

### Concurrency contract

For every key independently:

- `Load`, `Store`, `Delete` are lock-free and O(1) average.
- `Lock(k)` pins the slot exclusively and returns a stable `*V`.
- `LockOrStore(k, zero)` is the atomic insert-and-pin equivalent
  (returns `(*V, created)`).
- While a slot is pinned, other `Load` / `Store` / `Delete` on **that
  same key** spin until `Unlock`. Other keys are unaffected.
- **Always pair `Lock` with `Unlock`** — idiomatic usage is
  `defer cur.Unlock()` right after `Lock`.

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

### Full `sync.Map`-compatible surface

On top of the pinning primitives, every structure ships the same set of
atomic operations as `sync.Map`, with identical semantics:

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
-count=3`, **except `sync.Map` and `xsync.Map` rows which use
`-benchtime=2s -count=3`** (avoids OOM on `BenchmarkSyncMapStore` —
each insertion boxes an `int` interface, so at 5 s × 3 × 12 goroutines
the live set tops 10 GB). Lower is better.

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

All `ns/op` numbers below are wall-clock-per-iteration; the
`Allocations per operation` table further down complements them with
the `B/op` and `allocs/op` view (which the structures' inline-`V`
design is mainly about).

### `Map[K,V]` — generic hash map (int → int)

| Implementation                       | ReadOnly    | ReadHeavy   | Store        | GrowStore    | Churn        | LoadOrStore | Range/key   |
|--------------------------------------|------------:|------------:|-------------:|-------------:|-------------:|------------:|------------:|
| `map[int]int` (no lock, baseline)    |     1.39 ns | data race   |          —   |          —   |          —   |          —  |          —  |
| `map[int]int` + `sync.Mutex`         |           — |     45.0 ns |       222 ns |          —   |      57.4 ns |          —  |          —  |
| `map[int]int` + `sync.RWMutex`       |           — |     20.9 ns |       246 ns |          —   |      60.6 ns |          —  |          —  |
| `sync.Map` (stdlib)                  |     3.09 ns |     8.97 ns |       118 ns |          —   |      88.3 ns |    10.33 ns |     4.99 ns |
| `xsync.Map` v4                       |     1.03 ns |     3.38 ns |      89.2 ns |      95.2 ns |      15.4 ns |     1.35 ns |     4.37 ns |
| **`fsync.Map`**                      | **1.56 ns** | **6.06 ns** |  **74.4 ns** |  **73.0 ns** |  **18.0 ns** | **1.59 ns** | **2.21 ns** |

Highlights:

- `fsync.Map.LoadOrStore` at **1.59 ns** is **6.5× faster than
  `sync.Map` (10.33 ns)** on the hot get-or-set path, and within ~18 %
  of `xsync.Map` (1.35 ns) — drop-in replacement parity.
- `fsync.Map.Range/key` at **2.21 ns** is the **fastest iterator** of
  the three: 2.3× faster than `sync.Map` (4.99 ns) and 2.0× faster than
  `xsync.Map` (4.37 ns), thanks to the inline `[8]V` bucket layout
  (one cacheline read per 8 entries).
- `fsync.Map` Load (1.56 ns) sits between a plain `map[int]int` (1.39
  ns) and `sync.Map` (3.09 ns) — ~12 % overhead vs the lockless
  baseline buys full concurrent safety AND `Lock(*V)` semantics.
- `xsync.Map` (1.03 ns) edges everyone on Load thanks to its tighter
  bucket layout — it has no pin-word to read.
- `GrowStore` on `fsync.Map` is within 2 % of `Store` (no Grow): the
  table is allocated up-front so concurrent Stores no longer race on
  rebuild. `xsync.WithPresize` shows similar behavior but with higher
  run-to-run variance.
- `Churn` (rolling window Store+Delete) is where `xsync.Map` (15.4 ns)
  shines vs `fsync.Map` (18.0 ns). Both crush stdlib maps with locks
  (~60 ns) and `sync.Map` (88.3 ns).

### Allocations per operation

A defining feature of `fsync` is **zero heap allocation on the hot
path**. The `[8]V` slots inline in each bucket mean no per-entry
heap object: a `Load` or steady-state `LoadOrStore` allocates
nothing, a `Lock(k)` hands back a `*V` straight into the bucket
(no `*Entry`, no boxing), and `Store` allocates only the bucket
itself, which gets amortized over many inserts. Headline numbers
from `go test -benchmem` (median of 3 runs at `-benchtime=2s`):

| Operation                                            | `fsync.Map`         | `xsync.Map`            | `sync.Map`              |
|------------------------------------------------------|--------------------:|-----------------------:|------------------------:|
| `LoadOrStore` (preloaded, every call a hit)          | **0 B / 0 allocs**  | 0 B / 0 allocs         | 14 B / **1 alloc**      |
| `Store` (insert distinct keys, write-only)           | 83 B / **0 allocs** | 84 B / **1 alloc**     | 126 B / **3 allocs**    |
| `Lock + inc` (`Lock(k)` + `*p++` + `Unlock`)¹        | **0 B / 0 allocs**  | 16 B / 1 alloc (first) | 16 B / 1 alloc (first)  |
| `LockOrStore + inc` (atomic insert-and-pin + `*p++`)¹| **0 B / 0 allocs**  | 16 B / 1 alloc         | 16 B / 1 alloc          |

¹ `xsync.Map` and `sync.Map` have no native `Lock(*V)` API; the
canonical Go workaround is to store `*mutexedEntry{mu sync.Mutex;
v V}` instead of `V` itself, then `Load(k)` and take `e.mu.Lock()`.
The 16 B / 1 alloc visible above is the **caller's pre-allocated
`&mutexedEntry{}`** passed to `LoadOrStore` on each iteration.
Subsequent Loads on the same key are 0 alloc once the entry is in
place. `fsync.Map.Lock` and `LockOrStore` return a stable `*V`
*into the bucket itself* — zero `*Entry`, zero allocation, on every
key including the first insert. This is the structural win behind
`fsync.Map`'s pin-based API.

**Caveat on the "1 alloc" reading.** On top of the 16 B / 1 alloc
the caller pays, `xsync.Map` and `sync.Map` ALSO allocate an
internal `entry{key, value}` struct on each FIRST insert
(`new(entry[K, V])` in xsync, see `map.go:433`; similar in
stdlib `sync.Map`). On `xsync.Map[int, *mutexedEntry]` that's
roughly 16 B (int + ptr) of internal allocation. The benchmark
loops over 256 hot keys for millions of iterations, so those 256
internal allocations get amortized to ~0 per op and stay
invisible in the `B/op` / `allocs/op` figure. The true cost of a
genuine first insert is therefore closer to **2 allocs ≈ 32 B**
(the caller's `*mutexedEntry` AND the lib's internal entry), not
1 alloc. Same correction applies on the `Store` (insert int → int)
row: the 84 B / 1 alloc of `xsync.Map.Store` IS the internal
entry alloc (no caller boxing involved with a pure-int V); the
126 B / 3 allocs of `sync.Map.Store` is the internal entry + the
two `interface{}` box allocs around key and value.

`fsync.Map`, in contrast, has **no `*entry` indirection at all**:
the `[8]V` inline bucket IS the entry. The 83 B / 0 allocs on
`fsync.Map.Store` is the amortized bucket allocation — and no
internal per-entry struct exists to allocate. So even on a stream
of strictly first-insert calls there is no hidden second
allocation lurking behind the headline number.

The `fsync.Map.Store` row reports **0 allocs/op** even though
83 B/op got allocated: each bucket alloc covers up to 8 inserts, so
the per-op count is below 1 and rounds to zero. The amortized
memory cost is the 83 B/op figure.

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
| `sync.Map[int, *{Mutex, int}]` — first insert (allocates entry) |    10.06 ns |          —  |           —  |
| `xsync.Map[int, *{Mutex, int}]` — `Load` then take mutex        |     2.16 ns |          —  |           —  |
| `xsync.Map[int, *{Mutex, int}]` — first insert (allocates entry)|     7.99 ns |          —  |           —  |
| **`fsync.Map.Lock` + `*p++` + `Unlock`**                        | **7.65 ns** | **3.62 ns** |          —  |
| **`fsync.Map.LockOrStore` + `*p++` + `Unlock`**                 | **8.80 ns** |          —  |           —  |
| **`fsync.Store.Lock` + `*p++` + `Unlock`**                      | **15.1 ns** | **1.05 ns** | **12.9 ns** |
| **`fsync.Store.LockOrStore` + `*p++` + `Unlock`**               | **19.5 ns** | **1.44 ns** | **16.3 ns** |
| **`fsync.MutexStore.Lock` + `*p++` + `Unlock`**                 | **5.54 ns** | **1.20 ns** | **51.5 ns** |
| **`fsync.MutexStore.LockOrStore` + `*p++` + `Unlock`**          | **5.81 ns** |          —  |           —  |

Notes:

- The reference rows hide one heap allocation per first-time key and a
  `*Entry` indirection on every access; fsync stores `V` inline so the
  bucket *is* the entry. No `*Entry` allocation at any point.
- **Honest disclosure on the steady-state Load+mutex path:** once the
  `map[K]*{mu, V}` pattern is warm and no new entries get inserted,
  `xsync.Map[*{mu,v}].Load + e.mu.Lock + e.v++ + e.mu.Unlock`
  (**2.16 ns** here) **beats `fsync.Map.Lock + *p++ + Unlock`
  (7.65 ns)** by 3.5×. The reasons are real: xsync's Load has no
  pin-word to read, and a pointer-load + sync.Mutex.Lock on an
  uncontended mutex compiles into very few atomics on Zen 4. The
  trade-off is one heap allocation per first-time key on the xsync
  path, plus the indirection on every access. `fsync.Map.Lock`
  earns its 7.65 ns by giving you a stable `*V` directly into the
  bucket, no `*Entry`, no allocation, and the same Lock semantics
  the moment the key is first seen.
- **Single-key:** the standout finding. `fsync.Store.Lock` at **12.9
  ns** is **4× faster than `fsync.MutexStore.Lock` (51.5 ns)** under
  extreme single-key contention, because the Load-then-CAS spin keeps
  the cacheline in Shared state during the hold (one Modified
  transition per acquire, not per retry). Before this optim the same
  workload took ~370 ns on `Store.Lock` (~×30 improvement). On the
  moderate regime, however, `MutexStore` still wins (5.5 vs 15 ns):
  the 32 slots that share `lockused` on a Store bucket end up
  cache-bouncing across multiple goroutines, while each `MutexStore`
  slot has its own 64-byte mutex cacheline.
- **Uncontended:** `fsync.Store.Lock` (1.05 ns) is the fastest of all,
  *including* a plain `xsync.Map` Load (1.03 ns) — the
  pin/check/unpin cycle of a Load-then-CAS roughly matches a Load
  here. `fsync.MutexStore.Lock` (1.20 ns) is slightly behind because
  of the mutex.Lock/Unlock atomics. `fsync.Map.Lock` (3.62 ns)
  carries the bucket-walk overhead.
- `fsync.Map.LockOrStore` (8.80 ns) is competitive with
  `xsync.Map[*{mu,v}].LoadOrStore` (7.99 ns) and beats
  `sync.Map.LoadOrStore` (10.06 ns) on insert.
- **Rule of thumb:**
  - **Moderate / read-heavy contention** → `fsync.MutexStore` for
    Lock-heavy workloads, `fsync.Store` for everything else.
  - **Extreme contention on a single hot key** → `fsync.Store` wins.
  - **No contention** → `fsync.Store` everywhere.

### `Map[string]int` (2048 keys preloaded)

| Implementation                  | ReadOnly    |
|---------------------------------|------------:|
| `sync.Map[string]int`           |     3.09 ns |
| **`fsync.Map[string]int`**      | **2.79 ns** |

### `Store[V]` and `MutexStore[V]` — dense integer-indexed (int64 → V)

Integer keys mean no hashing, no chaining; slot lookup is one shift and
one bit test. `Store` is fully lock-free (bit-based pin/used);
`MutexStore` keeps a `sync.Mutex` per slot.

| Implementation        | ReadOnly    | ReadHeavy   | Store       | Churn       | LoadOrStore | Range/key   |
|-----------------------|------------:|------------:|------------:|------------:|------------:|------------:|
| **`fsync.Store`**     | **0.75 ns** | **0.95 ns** | **3.47 ns** | **2.18 ns** | **1.10 ns** | **1.42 ns** |
| `fsync.MutexStore`    |     1.04 ns |     1.14 ns |     3.15 ns |     3.12 ns |     1.19 ns |     3.69 ns |

Reading: `Store` wins on Load (~28 % faster), Churn (~30 % faster) and
LoadOrStore (~8 % faster); `MutexStore` wins on raw Store throughput
(~9 % faster, the mutex is cheaper than the bit-spin for write-heavy
workloads) and on the moderate Lock+inc workload (see Lock+inc table
above). The Lock+inc winner depends on the contention regime — see
the Lock+inc section.

`fsync.Store.ReadOnly` at **0.75 ns/op** is the fastest concurrent map
Load benchmarked here — it beats `xsync.Map` (1.03 ns) because
integer-indexed slots skip hashing entirely.

`fsync.Store.LoadOrStore` at **1.10 ns/op** matches its raw Load cost
within ~50 % (the pin acquire+release on a guaranteed-hit), and the
equivalent `Range/key` at **1.42 ns** is the fastest iteration
benchmarked here.

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
|  1 | **5.85 ns** | 11.72 ns | 3.92 ns | **5.67 ns** | 3.63 ns | 3.98 ns |
|  2 |     4.58 ns |  6.32 ns | 2.10 ns |     3.01 ns | 3.17 ns | 3.00 ns |
|  4 |     2.66 ns |  3.57 ns | 1.26 ns |     1.80 ns | 2.11 ns | 1.47 ns |
|  6 |     2.68 ns |  2.55 ns | 0.89 ns |     1.30 ns | 1.32 ns | 1.02 ns |
|  8 |     2.33 ns |  2.54 ns | 0.91 ns |     1.25 ns | 0.97 ns | 1.00 ns |
| 10 |     2.16 ns |  2.57 ns | 0.91 ns |     1.27 ns | 0.81 ns | 0.95 ns |
| 12 |     1.82 ns |  2.56 ns | 0.91 ns |     1.28 ns | 0.79 ns | 1.02 ns |

#### `LoadOrStore` (2048 keys preloaded, parallel get-or-set)

| GOMAXPROCS | `sync.Map` | `xsync.Map` | `fsync.Map` | `fsync.Store` | `fsync.MutexStore` |
|---:|---:|---:|---:|---:|---:|
|  1 | 34.11 ns | 5.57 ns | **6.68 ns** | 4.53 ns | 4.82 ns |
|  2 | 18.55 ns | 2.92 ns |     3.56 ns | 2.44 ns | 2.56 ns |
|  4 | 10.77 ns | 1.78 ns |     2.09 ns | 1.50 ns | 1.60 ns |
|  6 |  8.34 ns | 1.30 ns |     1.46 ns | 1.14 ns | 1.15 ns |
|  8 |  8.23 ns | 1.26 ns |     1.46 ns | 1.26 ns | 1.12 ns |
| 10 |  8.20 ns | 1.26 ns |     1.44 ns | 1.23 ns | 1.11 ns |
| 12 |  8.95 ns | 1.23 ns |     1.42 ns | 1.11 ns | 1.09 ns |

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
|  1 |  **8.21 ns** | 13.42 ns | 7.89 ns | **11.71 ns** |
|  4 |     2.51 ns |  4.25 ns | 2.46 ns |     3.52 ns |
| 12 |     2.59 ns |  2.91 ns | 1.75 ns |     2.60 ns |

`LoadOrStore` (steady state, all keys preloaded):

| GOMAXPROCS | `sync.Map` | `xsync.Map` | `fsync.Map` |
|---:|---:|---:|---:|
|  1 | 59.29 ns | 10.32 ns | **12.72 ns** |
|  4 | 15.57 ns |  3.30 ns |     3.83 ns |
| 12 | 13.43 ns |  2.22 ns |     2.74 ns |

`StoreWithAlloc` — inserts a fresh `strconv.Itoa(n)` key on every
iteration. The strconv allocation cost is *included* in the per-op
number on purpose: this is the realistic cost of seeing a fresh
string key from outside.

| GOMAXPROCS | `sync.Map`          | `fsync.Map`        |
|---:|--------------------:|-------------------:|
|  1 | 759 ns / **4 allocs** | 548 ns / **1 alloc** |
| 12 | 121 ns / 4 allocs   | 124 ns / 1 alloc    |

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
- On the `StoreWithAlloc` workload, `fsync.Map` is ~30 % faster at
  1 G thanks to its 4× fewer allocations; at 12 G they converge.
  This is the realistic insertion-rate ceiling, allocations
  included.

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
