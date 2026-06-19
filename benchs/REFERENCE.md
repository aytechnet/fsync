# fsync — benchmark reference snapshot

Raw benchmark output captured on a quiet machine after the
inline-`V` / pin-bit cursor design settled. This file is the
**reference** the README tables are derived from: anything more than
~10 % off these numbers on the same hardware suggests a real
regression rather than run-to-run jitter. Commit it together with
the matching code so a future contributor can spot the actual day
something changed.

## Setup

| Field      | Value                                |
|------------|--------------------------------------|
| Date       | 2026-06-19                            |
| Hardware   | AMD Ryzen 5 8540U (Zen 4, 6c/12t)    |
| Go         | 1.25                                  |
| GOMAXPROCS | 12 (default, machine has 12 threads) |

## How to reproduce

```sh
# Full suite with per-op memory & allocation counters
go test -bench=. -benchtime=5s -count=3 -benchmem -run='^$' ./benchs/

# If 5 s × 3 makes sync.Map.Store OOM (live set tops ~12 GB on
# under-16-GB machines), drop the benchtime:
go test -bench=. -benchtime=2s -count=3 -benchmem -run='^$' ./benchs/

# Standalone string-hash microbench (maphash vs FNV-1a vs xxh3 vs wyhash)
cd hashbench && go test -bench=. -benchtime=3s -count=3 ./...
```

## Reference output — allocation-focused subset (machine idle)

`go test -bench='^Benchmark(FsyncMapStore|FsyncMapLoadOrStore|FsyncMapLockInc|FsyncMapLockOrStoreInc|FsyncStoreReadOnly|FsyncStoreLoadOrStore|XsyncMapStore|XsyncMapLoadOrStore|SyncMapStore|SyncMapLoadOrStore)$ -benchtime=3s -count=3 -benchmem`

```
BenchmarkFsyncMapStore-12          52144279     78.53 ns/op    71 B/op    0 allocs/op
BenchmarkFsyncMapStore-12          67463131     80.76 ns/op   100 B/op    0 allocs/op
BenchmarkFsyncMapStore-12          69460927     79.34 ns/op   100 B/op    0 allocs/op
BenchmarkFsyncMapLoadOrStore-12  1000000000      1.732 ns/op     0 B/op    0 allocs/op
BenchmarkFsyncMapLoadOrStore-12  1000000000      1.730 ns/op     0 B/op    0 allocs/op
BenchmarkFsyncMapLoadOrStore-12  1000000000      1.717 ns/op     0 B/op    0 allocs/op
BenchmarkFsyncStoreReadOnly-12   1000000000      0.8405 ns/op    0 B/op    0 allocs/op
BenchmarkFsyncStoreReadOnly-12   1000000000      0.8321 ns/op    0 B/op    0 allocs/op
BenchmarkFsyncStoreReadOnly-12   1000000000      0.8445 ns/op    0 B/op    0 allocs/op
BenchmarkFsyncStoreLoadOrStore-12 1000000000      1.209 ns/op     0 B/op    0 allocs/op
BenchmarkFsyncStoreLoadOrStore-12 1000000000      1.184 ns/op     0 B/op    0 allocs/op
BenchmarkFsyncStoreLoadOrStore-12 1000000000      1.198 ns/op     0 B/op    0 allocs/op
BenchmarkFsyncMapLockOrStoreInc-12 420802322      9.017 ns/op    0 B/op    0 allocs/op
BenchmarkFsyncMapLockOrStoreInc-12 403810542      8.278 ns/op    0 B/op    0 allocs/op
BenchmarkFsyncMapLockOrStoreInc-12 446819718      9.069 ns/op    0 B/op    0 allocs/op
BenchmarkFsyncMapLockInc-12       436026255      8.476 ns/op    0 B/op    0 allocs/op
BenchmarkFsyncMapLockInc-12       444898630      8.160 ns/op    0 B/op    0 allocs/op
BenchmarkFsyncMapLockInc-12       497934030      7.739 ns/op    0 B/op    0 allocs/op
BenchmarkSyncMapStore-12          50169225    100.2 ns/op   124 B/op    3 allocs/op
BenchmarkSyncMapStore-12          59338314     96.41 ns/op  122 B/op    3 allocs/op
BenchmarkSyncMapStore-12          56195347     93.64 ns/op  123 B/op    3 allocs/op
BenchmarkSyncMapLoadOrStore-12   296754823     10.55 ns/op    14 B/op    1 allocs/op
BenchmarkSyncMapLoadOrStore-12   349484580     10.39 ns/op    14 B/op    1 allocs/op
BenchmarkSyncMapLoadOrStore-12   347437849     10.35 ns/op    14 B/op    1 allocs/op
BenchmarkXsyncMapStore-12         49224710     82.88 ns/op    65 B/op    1 allocs/op
BenchmarkXsyncMapStore-12         91697888     74.12 ns/op    68 B/op    1 allocs/op
BenchmarkXsyncMapStore-12         97569838     64.00 ns/op    65 B/op    1 allocs/op
BenchmarkXsyncMapLoadOrStore-12  1000000000      1.378 ns/op    0 B/op    0 allocs/op
BenchmarkXsyncMapLoadOrStore-12  1000000000      1.370 ns/op    0 B/op    0 allocs/op
BenchmarkXsyncMapLoadOrStore-12  1000000000      1.426 ns/op    0 B/op    0 allocs/op
```

**Read this table as the source of truth for the "Allocations per
operation" section of the README.** Headline:

- `fsync.Map`'s LoadOrStore, LockInc and LockOrStoreInc are
  **strictly 0 B / 0 allocs**.
- `fsync.Map.Store` reports 0 allocs/op because the bucket
  allocation amortizes over up to 8 inserts; the ~100 B/op figure
  captures the amortized memory cost. There is no per-entry struct
  to allocate on top of the bucket.
- `sync.Map` allocates 3 times per Store (entry + boxing key +
  boxing value into `interface{}`) and 1 time per LoadOrStore
  (boxing the supplied value).
- `xsync.Map` allocates exactly one internal `entry{key, value}`
  per first insert (16-24 B depending on K and V), nothing on
  steady-state LoadOrStore. The 65 B/op figure includes bucket
  amortization on top of the entry.
- All `fsync.Map.Lock` / `LockOrStore`-flavoured operations,
  including the Lock+inc workload pattern that the README
  benchmarks, are **0 alloc on every call** because the `*V` lives
  in the bucket — no `*Entry` to allocate, on every key including
  the first insert.

## Reference output — full ns/op suite (Fsync/GoMap/Ref at 5 s × 3)

Refer to the README's tables directly; the raw numbers are in the
shape of the README rows and the methodology section documents
the variance. A representative snapshot of the Fsync/GoMap/Ref
portion captured on the same day:

- `FsyncMapReadOnly`         : 1.55 ns/op
- `FsyncMapStore`            : 71.9 ns/op (median, 3-run spread 68–83 ns)
- `FsyncMapGrowStore`        : 73.0 ns/op
- `FsyncMapReadHeavy`        : 6.30 ns/op
- `FsyncMapChurn`            : 19.2 ns/op (3-run spread 16.7–22.4 ns)
- `FsyncMapLoadOrStore`      : 1.74 ns/op (steady-state hit)
- `FsyncMapRange`            : 4 403 ns/op (≈ 2.15 ns/key over 2048)
- `FsyncMapStringReadOnly`   : 3.16 ns/op
- `FsyncMapStringLoadOrStore`: 3.37 ns/op
- `FsyncMapStringStoreWithAlloc` : 119 ns/op / 1 alloc *
  (* OOMs at 5 s × 3 on under-16-GB; rerun at 2 s × 3)

- `FsyncStoreStore`          : 3.43 ns/op
- `FsyncStoreGrowStore`      : 3.04 ns/op
- `FsyncStoreReadHeavy`      : 1.16 ns/op
- `FsyncStoreReadOnly`       : 0.84 ns/op (idle machine; 1.1–2.3 ns when contended)
- `FsyncStoreLoadOrStore`    : 1.20 ns/op
- `FsyncStoreChurn`          : 3.95 ns/op
- `FsyncStoreRange/key`      : 112 706 ns/op / 65 536 ≈ 1.72 ns/key
- `FsyncStoreLockInc 256/12` : 15.8 ns/op
- `FsyncStoreLockInc Single` : 12.6 ns/op
- `FsyncStoreLockInc Uncont` : 1.07 ns/op
- `FsyncStoreLockOrStoreInc` : 20.3 ns/op
- `FsyncStoreLoadUncontended`: 0.70 ns/op
- `FsyncStoreLoadSingleKey`  : 251 ns/op

- `FsyncMutexStoreStore`     : 3.05 ns/op
- `FsyncMutexStoreReadHeavy` : 1.23 ns/op
- `FsyncMutexStoreReadOnly`  : 0.99 ns/op
- `FsyncMutexStoreChurn`     : 3.24 ns/op
- `FsyncMutexStoreLoadOrStore`: 1.25 ns/op
- `FsyncMutexStoreRange/key` : 240 378 / 65 536 ≈ 3.67 ns/key
- `FsyncMutexStoreLockInc`   : 5.56 ns/op
- `FsyncMutexStoreLockOrStoreInc`: 6.08 ns/op
- `FsyncMutexStoreLockInc Single`: 51.2 ns/op
- `FsyncMutexStoreLockInc Uncont`: 1.27 ns/op

- `GoMapMutexStore`          : 223 ns/op
- `GoMapMutexReadHeavy`      : 44.5 ns/op
- `GoMapMutexChurn`          : 57.2 ns/op
- `GoMapRWMutexStore`        : 266 ns/op
- `GoMapRWMutexReadHeavy`    : 24.4 ns/op
- `GoMapRWMutexChurn`        : 61.8 ns/op
- `GoMapReadOnly`            : 1.40 ns/op
- `GoMapStringReadOnly`      : 2.68 ns/op

- `RefXsyncMutexedEntryLockOrStore` : 8.02 ns/op (16 B / 1 alloc)
- `RefXsyncMutexedEntryLoad`        : 2.14 ns/op (0 B / 0 alloc)
- `RefStdSyncMutexedEntryLoadOrStore`: 9.99 ns/op (16 B / 1 alloc)
- `RefStdSyncMutexedEntryLoad`      : 3.87 ns/op (0 B / 0 alloc)

## Reference output — Sync/Xsync at 2 s × 3 (avoids OOM)

- `SyncMapStore`             : 96-100 ns/op (variance from GC, 3 allocs)
- `SyncMapReadHeavy`         : reuse README number (machine variance)
- `SyncMapReadOnly`          : reuse README number
- `SyncMapChurn`             : 84 ns/op
- `SyncMapLoadOrStore`       : 10.4 ns/op
- `SyncMapRange`             : 10 364 / 2048 ≈ 5.06 ns/key
- `SyncMapStringReadOnly`    : 3.38 ns/op
- `SyncMapStringLoadOrStore` : 15.7 ns/op
- `SyncMapStringStoreWithAlloc`: 120 ns / 4 allocs

- `XsyncMapStore`            : 58-94 ns/op (high variance; 1 alloc)
- `XsyncMapGrowStore`        : 92 ns/op
- `XsyncMapReadHeavy`        : 3.39 ns/op
- `XsyncMapReadOnly`         : 1.02 ns/op
- `XsyncMapChurn`            : 15.8 ns/op
- `XsyncMapLoadOrStore`      : 1.49 ns/op
- `XsyncMapRange`            : 8 931 / 2048 ≈ 4.36 ns/key
- `XsyncMapStringReadOnly`   : 2.10 ns/op
- `XsyncMapStringLoadOrStore`: 2.61 ns/op
- `XsyncMapStringStoreWithAlloc`: 109 ns / 2 allocs

## Variance disclosure

- The **allocation counters (`B/op` and `allocs/op`) are
  deterministic** modulo GC noise on `B/op`: a run on a freshly
  warmed binary will always report the same allocs/op shape. The
  allocation table in the README is therefore precise.
- The **`ns/op` numbers fluctuate ±5–15 %** depending on machine
  load (other processes, browser tabs, IDE indexers, parallel
  `go test` runs from sibling sessions). The numbers in the README
  are medians of the lowest-variance runs we captured during
  development. A run-to-run difference up to ~15 % is normal noise;
  anything beyond that on the same hardware is worth investigating.
- The lower the `ns/op`, the more sensitive it is to noise: a Load
  at 0.8 ns can swing to 2 ns under shared CPU, while a Lock+inc at
  10 ns barely moves.
