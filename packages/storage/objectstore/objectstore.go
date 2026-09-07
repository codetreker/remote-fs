// Package objectstore holds a namespace in an object store: the bytes of every file live
// under an opaque key in a container of blobs, and the tree that gives those bytes names —
// with the permissions, the times, and the shape of the directories — lives in a metastore.
//
// The division is not an implementation detail that could have gone the other way. An
// object store offers no directories, no attributes worth the name, and no way to change
// two things at once; a database offers all three and holds no bytes cheaply. Each half
// here does what it is good at, and the seam between them is the only place where a crash
// can be observed, which is why the write path is arranged so that a crash at the seam
// leaves litter rather than a lie.
//
// # The write path
//
// A write reserves a key, puts the bytes under it, and then asks the metastore to point the
// path at it. Objects are never modified once written, so a reader either finds the key the
// tree pointed at when it looked, whole, or finds the tree pointing somewhere else — never
// a file half replaced. The reservation is recorded before the bytes exist so that a
// sweeper can always tell an object a writer is still working on from an object nobody will
// ever reference; without it, the only way to tell them apart is to wait long enough that
// no write could still be in flight, which makes correctness a guess about latency.
//
// A crash between the put and the commit leaves an object nothing references. That costs
// storage until it is swept and it costs nothing else: no name points at it, so nothing can
// read it, and the namespace is exactly what it was before the write started.
package objectstore

import "context"

// Objects is a container of immutable blobs addressed by opaque keys.
//
// Every method must distinguish "this object is not there" from "I could not find out".
// The distinction is the whole reason this interface exists rather than the calls being
// made inline: a network failure that returns the same answer as a missing object turns
// R-ERR-2 from a rule into a suggestion, and the place that mistake is easiest to make is
// the place a REST client's 404 is converted into an error.
type Objects interface {
	// Put stores content under key with create-only semantics. A nil error positively proves
	// that this invocation created the immutable object under key; it must never mean that an
	// existing object was accepted or overwritten. Any error leaves ownership unresolved to
	// the caller, including EEXIST and a response lost after the request may have landed.
	//
	// The returned digest is whatever the store reports for the bytes it stored, or nil if
	// it reports nothing. It is a fact about the stored object, not a checksum computed here
	// from the same buffer that was sent — one of those detects a corrupted transfer and the
	// other cannot.
	Put(ctx context.Context, key string, content []byte) (digest []byte, err error)

	// Get returns the whole contents of the object under key.
	Get(ctx context.Context, key string) ([]byte, error)

	// Delete removes the object under key. An object that is not there is not an error:
	// deletion is driven by a sweeper that may have been interrupted after the delete and
	// before the record of it, so running it again must converge rather than fail.
	Delete(ctx context.Context, key string) error

	// Available reports how many more bytes the storage beneath this container currently
	// offers, independently of any workspace allowance. A store that has no finite physical
	// capacity it can measure answers syscall.ENOSYS; that is a standing property of the
	// implementation, not a transient measurement failure. Every other error is a failure to
	// determine whether bytes fit and must be preserved as such by callers.
	Available(ctx context.Context) (int64, error)

	// Close releases resources owned by the object store. It must wait for operations it has
	// admitted before releasing storage ownership, so a composite can close its metastore
	// while the backing store is still exclusively held.
	Close() error
}

// BoundedObjects retrieves an object under a caller-owned payload limit. Implementations
// maxBytes must be positive. Implementations must determine that the object exceeds it
// before allocating the complete payload; a larger object is syscall.EFBIG. The separate capability keeps existing Objects users
// source-compatible while allowing an embedded server to refuse an object backend that
// cannot uphold storage.BoundedStorage.
type BoundedObjects interface {
	Objects
	GetBounded(ctx context.Context, key string, maxBytes int64) ([]byte, error)
}
