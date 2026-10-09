package fsync

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

func TestInternerBasic(t *testing.T) {
	var in Interner[string] // zero value usable

	if in.Len() != 0 || in.Lookup("a") != 0 {
		t.Fatalf(`empty Interner: Len=%d Lookup("a")=%d, want 0`, in.Len(), in.Lookup("a"))
	}
	if _, ok := in.Key(0); ok {
		t.Errorf(`Key(0) must be false: 0 is never an id`)
	}
	if _, ok := in.Key(1); ok {
		t.Errorf(`Key(1) on empty Interner must be false`)
	}

	id, created := in.Intern("/chaussures")
	if id != 1 || !created {
		t.Fatalf(`first Intern = (%d, %v), want (1, true)`, id, created)
	}
	id, created = in.Intern("/chaussures")
	if id != 1 || created {
		t.Fatalf(`second Intern = (%d, %v), want (1, false)`, id, created)
	}
	if id, _ := in.Intern("/sacs"); id != 2 {
		t.Errorf(`second key got id %d, want 2`, id)
	}
	if in.Lookup("/sacs") != 2 || in.Lookup("/inconnu") != 0 {
		t.Errorf(`Lookup: /sacs=%d (want 2), /inconnu=%d (want 0)`, in.Lookup("/sacs"), in.Lookup("/inconnu"))
	}
	if in.Lookup("/inconnu") != 0 || in.Len() != 2 {
		t.Errorf(`Lookup must not assign: Len=%d, want 2`, in.Len())
	}
	if k, ok := in.Key(2); !ok || k != "/sacs" {
		t.Errorf(`Key(2) = (%q, %v), want ("/sacs", true)`, k, ok)
	}
	if _, ok := in.Key(-1); ok {
		t.Errorf(`Key(-1) must be false`)
	}
	if _, ok := in.Key(3); ok {
		t.Errorf(`Key(3) not handed out yet must be false`)
	}
}

func TestInternerDenseAndRange(t *testing.T) {
	in := NewInterner[int]().Grow(10_000)
	for i := 0; i < 10_000; i++ {
		if id, _ := in.Intern(i * 7); id != int64(i+1) {
			t.Fatalf(`key %d got id %d, want %d (dense, in order)`, i*7, id, i+1)
		}
	}
	seen := 0
	in.Range(func(id int64, key int) bool {
		if key != int(id-1)*7 {
			t.Fatalf(`Range: id %d → key %d, want %d`, id, key, (id-1)*7)
		}
		seen++
		return true
	})
	if seen != 10_000 {
		t.Errorf(`Range visited %d entries, want 10000`, seen)
	}
	stop := 0
	in.Range(func(int64, int) bool { stop++; return stop < 3 })
	if stop != 3 {
		t.Errorf(`Range must stop when f returns false: %d calls`, stop)
	}
}

// Many goroutines intern overlapping key sets: every key gets exactly
// one id, exactly one Intern per key reports created, and the ids are
// exactly 1..N with no gap.
func TestInternerConcurrentOneIDPerKey(t *testing.T) {
	const (
		keys    = 20_000
		workers = 8
	)
	var in Interner[string]
	var creations [keys]atomic.Int32
	got := make([][]int64, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		got[w] = make([]int64, keys)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < keys; i++ {
				k := (i + w*keys/workers) % keys // each worker starts elsewhere
				id, created := in.Intern("k" + strconv.Itoa(k))
				if created {
					creations[k].Add(1)
				}
				got[w][k] = id
				if i%512 == 0 {
					runtime.Gosched()
				}
			}
		}(w)
	}
	wg.Wait()

	if in.Len() != keys {
		t.Fatalf(`Len() = %d, want %d (no burnt id)`, in.Len(), keys)
	}
	used := make([]bool, keys+1)
	for k := 0; k < keys; k++ {
		if c := creations[k].Load(); c != 1 {
			t.Fatalf(`key %d: %d Interns reported created, want exactly 1`, k, c)
		}
		id := got[0][k]
		for w := 1; w < workers; w++ {
			if got[w][k] != id {
				t.Fatalf(`key %d: worker %d got id %d, worker 0 got %d`, k, w, got[w][k], id)
			}
		}
		if id < 1 || id > keys || used[id] {
			t.Fatalf(`key %d: id %d out of 1..%d or used twice`, k, id, keys)
		}
		used[id] = true
		if key, ok := in.Key(id); !ok || key != "k"+strconv.Itoa(k) {
			t.Fatalf(`Key(%d) = (%q, %v), want k%d`, id, key, ok, k)
		}
	}
}

// Readers racing with writers: any id seen through Lookup must already
// resolve through Key (reverse entry written before publication).
func TestInternerConcurrentReadersSeeConsistentIDs(t *testing.T) {
	const keys = 20_000
	var in Interner[int]
	var wg sync.WaitGroup
	var done atomic.Bool
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !done.Load() {
				for k := 0; k < keys; k += 97 {
					if id := in.Lookup(k); id != 0 {
						if key, ok := in.Key(id); !ok || key != k {
							t.Errorf(`Lookup(%d) = %d but Key(%d) = (%d, %v)`, k, id, id, key, ok)
							return
						}
					}
				}
			}
		}()
	}
	var ww sync.WaitGroup
	for w := 0; w < 4; w++ {
		ww.Add(1)
		go func(w int) {
			defer ww.Done()
			for k := w; k < keys; k += 4 {
				in.Intern(k)
			}
		}(w)
	}
	ww.Wait()
	done.Store(true)
	wg.Wait()
	if in.Len() != keys {
		t.Errorf(`Len() = %d, want %d`, in.Len(), keys)
	}
}

// Stress for the open-addressing layout: the table starts at 64 words
// and doubles many times while 12 writers insert overlapping string
// keys and readers check that any id seen through Lookup resolves
// through Key to the same key. At the end ids are exactly 1..N.
func TestInternerStressGrowthUnderReadersAndWriters(t *testing.T) {
	const (
		keys    = 120_000
		writers = 12
	)
	var in Interner[string]
	name := func(k int) string { return "/p/" + strconv.Itoa(k) }
	var wg sync.WaitGroup
	var done atomic.Bool
	var bad atomic.Int64
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			x := uint64(r*7919 + 1)
			for !done.Load() {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				k := int(x % keys)
				if id := in.Lookup(name(k)); id != 0 {
					if got, ok := in.Key(id); !ok || got != name(k) {
						bad.Add(1)
					}
				}
				if x%256 == 0 {
					runtime.Gosched()
				}
			}
		}(r)
	}
	var ww sync.WaitGroup
	var created atomic.Int64
	for w := 0; w < writers; w++ {
		ww.Add(1)
		go func(w int) {
			defer ww.Done()
			for i := 0; i < keys; i++ {
				k := (i*7 + w*keys/writers) % keys // overlapping, scattered
				if _, c := in.Intern(name(k)); c {
					created.Add(1)
				}
			}
		}(w)
	}
	ww.Wait()
	done.Store(true)
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf(`%d Lookup ids did not resolve to their key through Key`, bad.Load())
	}
	if created.Load() != keys || in.Len() != keys {
		t.Fatalf(`created=%d Len=%d, want %d (one creation per key, dense ids)`, created.Load(), in.Len(), keys)
	}
	seen := make([]bool, keys+1)
	for k := 0; k < keys; k++ {
		id := in.Lookup(name(k))
		if id < 1 || id > keys || seen[id] {
			t.Fatalf(`key %d: id %d out of range or duplicated`, k, id)
		}
		seen[id] = true
	}
}

func TestInternerGrowPresized(t *testing.T) {
	in := NewInterner[int]().Grow(100_000)
	words := len(in.table.Load().slots)
	for i := 0; i < 100_000; i++ {
		in.Intern(i)
	}
	if got := len(in.table.Load().slots); got != words {
		t.Errorf(`presized table doubled anyway: %d → %d words`, words, got)
	}
	if _, ok := in.Key(100_001); ok {
		t.Errorf(`Key beyond Len must be false`)
	}
}
