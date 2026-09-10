// Package memory implements objectstore.Objects in the process's own heap, so that a
// metastore-backed volume can be assembled and tested without a blob service.
//
// The layers above the object store — the tree, the change log, replication, the mount —
// are not testing blob storage when they run. Making each of them stand up a container to
// prove something about itself costs the time of every such run and points the failure at
// the wrong layer on the day the container is what broke.
//
// It holds every object it is given for as long as it is alive: no bound, no way to
// configure one, no eviction, and nothing that outlives the process. That is why it is not
// something to serve a volume from — R-INT-3 requires a configurable ceiling on
// anything that accumulates, and there is none here.
//
// It is not a way to run azblob's tests without a blob endpoint, and nothing here should be
// read as one. That package runs against a real endpoint and fails rather than skips when it
// cannot reach one, because the wire version, the conditional write and the service's error
// codes have no other proof — and none of it is proof this package is able to supply.
package memory

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

// Objects is a set of immutable objects held in a map.
type Objects struct {
	// mu guards content. Get takes it for reading only, which is sound because a stored
	// object is never modified in place: what Get hands back is a copy, so a reader is
	// left holding nothing that a later Delete could pull out from under it.
	mu      sync.RWMutex
	content map[string][]byte
}

var _ objectstore.Objects = (*Objects)(nil)
var _ objectstore.BoundedObjects = (*Objects)(nil)

// New opens an empty set of objects.
func New() *Objects {
	return &Objects{content: make(map[string][]byte)}
}

// Put stores content under key, and returns the MD5 of the bytes it stored.
//
// A key that has already been written is refused with EEXIST rather than overwritten, for
// the reason the real store refuses it: keys are reserved once and used once, so a second
// Put means a reservation was handed out twice, and replacing the object would change what
// a reader still holding the old key goes on to read.
//
// The digest is the value Blob Storage reports for the same content: azblob returns the
// service's Content-MD5, which is an MD5 of the blob. What it is not is worth what the
// interface asks a digest to be worth — there is no transfer here to be corrupted, so a sum
// taken over the copy we retained attests to that copy and to nothing beyond it. Reporting
// nil is the contract's other honest answer and is not the one taken, because it would
// quietly empty out every assertion above this layer that a digest was recorded.
func (o *Objects) Put(ctx context.Context, key string, content []byte) ([]byte, error) {
	if err := withdrawn(ctx, "put", key); err != nil {
		return nil, err
	}

	stored := copyOf(content)
	sum := md5.Sum(stored)

	o.mu.Lock()
	defer o.mu.Unlock()
	if _, taken := o.content[key]; taken {
		return nil, fmt.Errorf("put %q: an object is already stored under this key: %w", key, syscall.EEXIST)
	}
	o.content[key] = stored
	return sum[:], nil
}

// Get returns the whole object under key.
//
// A key nothing was stored under is ENOENT, and here that answer carries the weight
// R-ERR-2 wants it to: there is no endpoint to be unreachable, so absence is the only
// thing a lookup that comes back empty can mean.
func (o *Objects) Get(ctx context.Context, key string) ([]byte, error) {
	return o.get(ctx, key, 0, false)
}

// GetBounded checks the retained object's length while holding the map lock and before
// making the caller-owned copy.
func (o *Objects) GetBounded(ctx context.Context, key string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("get %q: the byte limit %d is not positive: %w", key, maxBytes, syscall.EINVAL)
	}
	return o.get(ctx, key, maxBytes, true)
}

func (o *Objects) get(ctx context.Context, key string, maxBytes int64, bounded bool) ([]byte, error) {
	if err := withdrawn(ctx, "get", key); err != nil {
		return nil, err
	}

	o.mu.RLock()
	defer o.mu.RUnlock()
	stored, ok := o.content[key]
	if !ok {
		return nil, fmt.Errorf("get %q: nothing is stored under this key: %w", key, syscall.ENOENT)
	}
	if bounded && int64(len(stored)) > maxBytes {
		return nil, fmt.Errorf("get %q: the object contains %d bytes, above the result limit of %d: %w",
			key, len(stored), maxBytes, syscall.EFBIG)
	}
	return copyOf(stored), nil
}

// Delete removes the object under key, and takes an object that is not there as done: the
// sweeper that drives deletion may have been interrupted after removing an object and
// before recording that it had, so running it again has to converge rather than fail.
func (o *Objects) Delete(ctx context.Context, key string) error {
	if err := withdrawn(ctx, "delete", key); err != nil {
		return err
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.content, key)
	return nil
}

// Available refuses a physical-capacity figure. Heap capacity is neither a stable limit nor
// a measurement of room reserved for this object store, so reporting one would invent a
// constraint the process does not own.
func (o *Objects) Available(context.Context) (int64, error) {
	return 0, fmt.Errorf("the in-memory object store has no physical capacity to report: %w", syscall.ENOSYS)
}

// Close releases no resources. Objects retained here belong to this value and become
// collectible with it.
func (o *Objects) Close() error { return nil }

// withdrawn reports the context's own failure, if it has one, before anything is stored or
// read.
//
// azblob answers a cancelled request with EINTR and a failure it cannot name with EIO. A
// store the layers above are tested against has to answer the same way, or their
// cancellation paths pass here and are left to be found out against the real service.
func withdrawn(ctx context.Context, op, key string) error {
	switch err := ctx.Err(); {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("%s %q: the request was withdrawn: %w", op, key, syscall.EINTR)
	default:
		return fmt.Errorf("%s %q: %s: %w", op, key, err, syscall.EIO)
	}
}

// copyOf is what stands in for the wire.
//
// The caller's buffer and the stored object have to be separate arrays in both directions:
// a caller that reuses its buffer after a Put would otherwise rewrite an object that is
// meant to be immutable, and a caller that writes into what Get returned would rewrite it
// for everyone. Serialising the bytes makes that sharing impossible against a real
// service, so a store that skipped the copy here would let the layers above pass against
// it and corrupt objects against the real one.
//
// Zero bytes copy to an empty slice rather than to nil, which is what reading an empty
// body yields.
func copyOf(content []byte) []byte {
	copied := make([]byte, len(content))
	copy(copied, content)
	return copied
}
