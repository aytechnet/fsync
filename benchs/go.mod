module github.com/aytechnet/fsync/benchs

go 1.25

require (
	github.com/aytechnet/fsync v0.0.0-00010101000000-000000000000
	github.com/puzpuzpuz/xsync/v4 v4.4.0
)

// During development the parent module lives in the parent
// directory. Once fsync has a tagged release, this can be dropped
// and a version pinned in the require block above.
replace github.com/aytechnet/fsync => ../
