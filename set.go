package fsync

// Set is a generic concurrent set of comparable keys, implemented as a
// thin typed wrapper over Map[K, struct{}]. The wrapper costs nothing
// at runtime: `struct{}` is zero-sized in Go, so the underlying
// bucket's `[8]V` slot array is also zero-sized and the per-entry
// memory footprint is exactly the same as a dedicated bitmap-of-keys
// implementation. Method calls inline directly into the equivalent
// Map[K, struct{}] operation.
//
// Set offers the canonical set-of-keys API (Add / Contains / Remove
// / Len / Range / Clear) without exposing the `struct{}` value type
// to callers. The zero value is usable: the first Add lazy-allocates
// the underlying table.
type Set[K comparable] struct {
	m Map[K, struct{}]
}

// NewSet returns an empty Set[K] pre-sized to comfortably hold
// estimatedItems without an immediate rebuild. Pass 0 if you have no
// estimate; the zero-value Set is equally usable.
func NewSet[K comparable](estimatedItems int) *Set[K] {
	s := &Set[K]{}
	s.m.table.Store(newTable[K, struct{}](bucketsFor(estimatedItems)))
	return s
}

// Grow expands the underlying table so the set can hold at least
// estimatedItems without triggering a rebuild. Same semantics as
// Map.Grow. Returns the receiver, so calls can be chained.
func (s *Set[K]) Grow(estimatedItems int) *Set[K] {
	s.m.Grow(estimatedItems)
	return s
}

// Contains reports whether key is in the set.
func (s *Set[K]) Contains(key K) bool {
	_, ok := s.m.Load(key)
	return ok
}

// Add inserts key into the set. Returns true if the key was newly
// inserted, false if it was already present.
func (s *Set[K]) Add(key K) (added bool) {
	return s.m.Store(key, struct{}{})
}

// Remove deletes key from the set. Returns true if the key was
// present (and has been removed), false otherwise.
func (s *Set[K]) Remove(key K) bool { return s.m.Delete(key) }

// Len returns the current number of elements. The result is a
// snapshot; concurrent Add / Remove may not be reflected.
func (s *Set[K]) Len() int { return s.m.Len() }

// Range calls f sequentially for each key in the set. The iteration
// is weakly consistent (sync.Map semantics): keys present throughout
// the iteration are guaranteed to be visited; keys inserted or
// removed during iteration may or may not be observed. Iteration
// stops if f returns false.
func (s *Set[K]) Range(f func(key K) bool) {
	s.m.Range(func(k K, _ struct{}) bool { return f(k) })
}

// Clear removes every element from the set.
func (s *Set[K]) Clear() { s.m.Clear() }
