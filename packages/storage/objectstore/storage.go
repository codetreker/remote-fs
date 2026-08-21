package objectstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// sweepBatch is how many objects a mutation clears out of the way before returning.
//
// Bounded rather than exhaustive: the caller is waiting, and a namespace that has
// accumulated garbage faster than its writes clear it is one where the alternative is a
// write that takes as long as the backlog. What this batch does not reach stays recorded,
// and Sweep exists for a caller that wants to drain it.
const sweepBatch = 8

// reserveGrace is how long an object may sit reserved before a sweep may take it as
// abandoned. It bounds the time between reserving a key and committing it, which is one
// put of one file's contents; a value this far above that is a value no live write reaches.
const reserveGrace = time.Hour

// readAttempts is how many times a read will start over because the file was replaced
// while it was being fetched. Each retry means another writer got between the two steps of
// this read, so a reader only exhausts these against a writer that never pauses — and a
// reader that gave up quietly, or answered from an object it could not fetch, would be the
// worse outcome by far.
const readAttempts = 4

// Storage is one namespace, its tree in a metastore and its bytes in an object store.
type Storage struct {
	objects Objects
	meta    metastore.Store
	now     func() time.Time
}

var _ storage.Storage = (*Storage)(nil)

// New assembles a namespace from the two halves that hold it.
func New(objects Objects, meta metastore.Store) *Storage {
	return &Storage{objects: objects, meta: meta, now: time.Now}
}

// Close releases the metastore. The object store holds nothing that outlives a request.
func (s *Storage) Close() error { return s.meta.Close() }

func (s *Storage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return storage.Attr{}, &os.PathError{Op: "stat", Path: path, Err: err}
	}
	node, err := s.meta.Stat(ctx, cleaned)
	if err != nil {
		return storage.Attr{}, err
	}
	return node.Attr(), nil
}

func (s *Storage) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "setattr", Path: path, Err: err}
	}
	if err := change.Check(); err != nil {
		return &os.PathError{Op: "setattr", Path: path, Err: err}
	}
	return s.meta.SetAttr(ctx, cleaned, change)
}

func (s *Storage) List(ctx context.Context, path string) ([]storage.Entry, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return nil, &os.PathError{Op: "list", Path: path, Err: err}
	}
	children, err := s.meta.List(ctx, cleaned)
	if err != nil {
		return nil, err
	}
	entries := make([]storage.Entry, len(children))
	for i, c := range children {
		entries[i] = storage.Entry{Name: string(c.Name), Attr: c.Node.Attr()}
	}
	return entries, nil
}

// Read returns the whole contents of the file at path.
//
// Nothing here answers for a symbolic link, and nothing needs to: the storage contract
// offers no operation that makes one, so a namespace reachable only through it never comes
// to hold one. A local directory is different because something outside this system can
// make a link in it; a metastore has no outside.
//
// Reading is two steps — ask the tree which object, then ask for that object — and a write
// can land between them. When it does, the object the tree named a moment ago has already
// been swept, and the read must tell that apart from the one thing it looks exactly like:
// a namespace that has lost bytes it still claims to hold. The difference is whether the
// tree still names the object that is missing. If it names a different one, the file was
// replaced and the new contents are as valid an answer as the old ones would have been. If
// it names the same one, something that should exist does not, and that is reported rather
// than retried, because no number of retries will make it appear.
func (s *Storage) Read(ctx context.Context, path string) ([]byte, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return nil, &os.PathError{Op: "read", Path: path, Err: err}
	}
	missing := metastore.Key("")
	for attempt := 0; ; attempt++ {
		node, err := s.meta.Stat(ctx, cleaned)
		if err != nil {
			return nil, err
		}
		switch {
		case node.IsDir():
			return nil, &os.PathError{Op: "read", Path: path, Err: syscall.EISDIR}
		case node.Content == "":
			return nil, nil
		case node.Content == missing:
			return nil, fmt.Errorf("the contents of %s are recorded under an object the store does not have: %w", path, syscall.EIO)
		}

		content, err := s.objects.Get(ctx, string(node.Content))
		switch {
		case err == nil:
			return content, nil
		case !errors.Is(err, syscall.ENOENT):
			// The tree named an object and the object store could not produce it. That is not
			// "the file is not there" — the file is there, and its bytes are what could not be
			// reached. Reporting it as absence is the fabricated answer R-ERR-2 forbids, and
			// R-ERR-6 says a storage assembled from parts that fail separately answers for the
			// part that failed.
			return nil, fmt.Errorf("reading the contents of %s: %w", path, err)
		case attempt == readAttempts:
			// Every attempt lost the same race to a different write. Answering with a report
			// that the file could not be read is worse than useless here — the file is there
			// and is being written to — but it is what is true, and inventing an answer from
			// bytes nobody could fetch is the one thing that must not happen.
			return nil, fmt.Errorf("the contents of %s were replaced under every one of %d attempts to read them: %w",
				path, attempt+1, syscall.EAGAIN)
		}
		missing = node.Content
	}
}

// Write replaces the contents of the file at path.
//
// The order is: reserve a key, put the bytes under it, point the tree at it. Objects are
// never modified once written, so a concurrent reader holding an earlier key reads that
// object whole; the replacement becomes visible when the tree changes, in one step, which
// is the atomicity the contract asks for and which no sequence of writes to a single blob
// could offer.
//
// A failure after the put and before the commit leaves an object nothing references. It
// costs storage until a sweep reaches it and costs nothing else — no name points at it, so
// nothing can read it, and the namespace is what it was before the write began.
func (s *Storage) Write(ctx context.Context, path string, content []byte) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "write", Path: path, Err: err}
	}

	object := metastore.Object{Size: int64(len(content)), ModTime: s.now()}
	if len(content) > 0 {
		key, err := s.meta.Reserve(ctx)
		if err != nil {
			return err
		}
		digest, err := s.objects.Put(ctx, string(key), content)
		if err != nil {
			return fmt.Errorf("storing the contents of %s: %w", path, err)
		}
		object.Key, object.Digest = key, digest
	}

	if err := s.meta.Commit(ctx, cleaned, object); err != nil {
		return err
	}
	s.sweep(ctx, sweepBatch)
	return nil
}

func (s *Storage) Create(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "create", Path: path, Err: err}
	}
	return s.meta.Create(ctx, cleaned)
}

func (s *Storage) Mkdir(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "mkdir", Path: path, Err: err}
	}
	return s.meta.Mkdir(ctx, cleaned)
}

func (s *Storage) Remove(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "remove", Path: path, Err: err}
	}
	if err := s.meta.Remove(ctx, cleaned); err != nil {
		return err
	}
	s.sweep(ctx, sweepBatch)
	return nil
}

func (s *Storage) RemoveDir(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "removedir", Path: path, Err: err}
	}
	return s.meta.RemoveDir(ctx, cleaned)
}

func (s *Storage) Rename(ctx context.Context, from, to string) error {
	cleanFrom, err := storage.CleanPath(from)
	if err != nil {
		return &os.PathError{Op: "rename", Path: from, Err: err}
	}
	cleanTo, err := storage.CleanPath(to)
	if err != nil {
		return &os.PathError{Op: "rename", Path: to, Err: err}
	}
	if err := s.meta.Rename(ctx, cleanFrom, cleanTo); err != nil {
		return err
	}
	s.sweep(ctx, sweepBatch)
	return nil
}

func (s *Storage) Space(ctx context.Context) (storage.Space, error) {
	return s.meta.Space(ctx)
}

// Sweep deletes objects nothing references and forgets them, until it runs out or reaches
// limit. It returns how many it removed.
//
// A caller that wants a namespace tidied rather than merely kept from growing calls this;
// the mutations clear only a batch each, so a namespace that was written to by a process
// that then died has a backlog nobody is walking.
func (s *Storage) Sweep(ctx context.Context, limit int) (int, error) {
	removed := 0
	for removed < limit {
		batch := min(limit-removed, sweepBatch)
		keys, err := s.meta.Garbage(ctx, batch, reserveGrace)
		if err != nil {
			return removed, err
		}
		if len(keys) == 0 {
			return removed, nil
		}
		gone, err := s.discard(ctx, keys)
		removed += gone
		if err != nil {
			return removed, err
		}
		if gone < len(keys) {
			// Everything reachable this round is gone and something is still recorded, so
			// another round would ask for the same keys and fail the same way.
			return removed, nil
		}
	}
	return removed, nil
}

// sweep clears what it can and reports nothing.
//
// It is called after a mutation has already succeeded, where a failure to delete an object
// is not a failure of the operation the caller made: the write happened, the name points
// where it should, and the only casualty is that some bytes nobody references are still
// being paid for. That failure is not lost — the record that named them is still there, so
// the next mutation or the next Sweep tries again. State carries the retry, which is why
// there is nothing here to return.
func (s *Storage) sweep(ctx context.Context, limit int) {
	keys, err := s.meta.Garbage(ctx, limit, reserveGrace)
	if err != nil || len(keys) == 0 {
		return
	}
	s.discard(ctx, keys)
}

// discard deletes the objects behind keys and forgets the ones that are gone, returning how
// many were removed. Keys whose objects could not be deleted keep their records, so nothing
// is forgotten while its bytes are still being paid for.
func (s *Storage) discard(ctx context.Context, keys []metastore.Key) (int, error) {
	gone := make([]metastore.Key, 0, len(keys))
	var failure error
	for _, key := range keys {
		if err := s.objects.Delete(ctx, string(key)); err != nil {
			failure = fmt.Errorf("deleting an unreferenced object: %w", err)
			break
		}
		gone = append(gone, key)
	}
	if len(gone) > 0 {
		if err := s.meta.Forget(ctx, gone); err != nil && failure == nil {
			failure = err
		}
	}
	return len(gone), failure
}
