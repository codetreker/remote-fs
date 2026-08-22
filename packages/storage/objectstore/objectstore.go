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
	// Put stores content under key. The key was reserved by a metastore and has never been
	// used before, so an implementation may assume it is writing rather than overwriting;
	// where the store offers a way to say so, it should, since a key collision would mean
	// something has gone wrong upstream and silence is the wrong response to that.
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
}
