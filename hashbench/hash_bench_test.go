// Package hashbench is a standalone Go module dedicated to the
// string-hash microbench. It lives in its own go.mod so its xxh3 /
// wyhash dependencies do not leak into fsync's parent module (which
// is the iPaaS DyaPi project today and will become the public
// github.com/aytechnet/fsync module tomorrow). To run:
//
//	cd hashbench && go test -bench=. -benchtime=3s -count=3 ./...
package hashbench

import (
	"hash/fnv"
	"hash/maphash"
	"strings"
	"testing"

	"github.com/orisano/wyhash"
	"github.com/zeebo/xxh3"
)

// Hash microbenchmarks. Goal: measure the per-call cost of the four
// reasonable string-hash candidates on Zen 4 (AMD Ryzen 5 8540U),
// across four representative key lengths, so we can decide whether
// adding an external dep is worth it for fsync.Map[string].
//
// Candidates:
//   - maphash.String  — stdlib, calls runtime.memhash, AES-NI when available.
//   - fnv.New64a      — stdlib, FNV-1a, byte-by-byte, no SIMD.
//   - xxh3            — zeebo/xxh3, modern, AVX2 when available.
//   - wyhash          — orisano/wyhash, multiply-only, no SIMD.
//
// Sizes: 8, 32, 128, 512 bytes. 8 is "typical short key" (uuid-ish or
// integer-stringified), 32 is "user identifier or path segment", 128
// covers most URL paths and email addresses, 512 captures the long-
// payload regime where SIMD really matters.

var hashSeed = maphash.MakeSeed()

func makeStr(n int) string { return strings.Repeat("a", n) }

func runHashBench(b *testing.B, fn func(string) uint64) {
	sizes := []int{8, 32, 128, 512}
	for _, n := range sizes {
		s := makeStr(n)
		b.Run(strconvItoa(n), func(b *testing.B) {
			var sink uint64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = fn(s)
			}
			_ = sink
		})
	}
}

// Local strconv.Itoa replacement to keep this file self-contained.
func strconvItoa(n int) string {
	switch n {
	case 8:
		return "8"
	case 32:
		return "32"
	case 128:
		return "128"
	case 512:
		return "512"
	}
	return "?"
}

func BenchmarkHashMaphashString(b *testing.B) {
	runHashBench(b, func(s string) uint64 { return maphash.String(hashSeed, s) })
}

func BenchmarkHashFNV1a(b *testing.B) {
	runHashBench(b, func(s string) uint64 {
		h := fnv.New64a()
		h.Write([]byte(s))
		return h.Sum64()
	})
}

func BenchmarkHashXXH3(b *testing.B) {
	runHashBench(b, func(s string) uint64 { return xxh3.HashString(s) })
}

func BenchmarkHashWyhash(b *testing.B) {
	runHashBench(b, func(s string) uint64 { return wyhash.Sum64(0, []byte(s)) })
}
