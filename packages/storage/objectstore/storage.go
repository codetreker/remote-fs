package objectstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// sweepBatch is the most garbage one metastore query hands to the object store. Bounded
// queries keep one maintenance step from monopolising the worker; a full background pass
// queues another, while an explicit Sweep may run several batches up to its caller-supplied
// limit.
const sweepBatch = 8

const (
	// DefaultSweepInterval bounds how long background maintenance normally waits before
	// retrying recorded garbage.
	DefaultSweepInterval = time.Minute

	// DefaultSweepBatch bounds the object-store and metastore work performed by one
	// background maintenance attempt.
	DefaultSweepBatch = 64

	// MaxSweepBatch prevents one maintenance attempt from becoming effectively unbounded.
	MaxSweepBatch = 1 << 20
)

// readAttempts is how many times a read will start over because the file was replaced
// while it was being fetched. Each retry means another writer got between the two steps of
// this read, so a reader only exhausts these against a writer that never pauses — and a
// reader that gave up quietly, or answered from an object it could not fetch, would be the
// worse outcome by far.
const readAttempts = 4

// Storage is one volume, its tree in a metastore and its bytes in an object store.
type Storage struct {
	objects Objects
	meta    metastore.Store
	now     func() time.Time

	maintenanceStop    context.CancelFunc
	maintenanceDone    chan struct{}
	maintenanceTrigger chan struct{}
	cleanupContext     context.Context
	stopCleanup        context.CancelFunc
	sweepPermit        chan struct{}
	statusMu           sync.Mutex
	maintenanceStatus  MaintenanceStatus
	operations         sync.RWMutex
	closeMu            sync.Mutex
	closeDone          chan struct{}
	closeErr           error
	fileMu             sync.Mutex
	fileSessions       map[*fileSession]struct{}
	filesClosing       bool
	fileCloseMu        sync.Mutex
	fileCloseRetry     bool
}

var _ storage.Storage = (*Storage)(nil)
var _ storage.BoundedStorage = (*Storage)(nil)

// LockService returns the authority bound to this metastore's native publication
// guards. Nil means the metastore has not enabled explicit file locks.
func (s *Storage) LockService() locking.Service {
	if native, ok := s.meta.(interface{ LockService() locking.Service }); ok {
		return native.LockService()
	}
	return nil
}

// CheckPublicationAccounting reports whether quota wrappers can settle against the
// actual targets resolved by the final metastore publication.
func (s *Storage) CheckPublicationAccounting() error {
	if native, ok := s.meta.(interface{ CheckPublicationAccounting() error }); ok {
		return native.CheckPublicationAccounting()
	}
	return fmt.Errorf("the metastore cannot account for final publication: %w", syscall.ENOSYS)
}

// Options configures storage-owned background maintenance.
//
// Both fields are required. Background maintenance is explicit because it owns a goroutine
// and outlives every request made through the Storage.
type Options struct {
	SweepInterval time.Duration
	SweepBatch    int
}

// DefaultOptions returns the bounded background-maintenance defaults. Callers may replace
// either value before passing the result to NewWithOptions.
func DefaultOptions() Options {
	return Options{SweepInterval: DefaultSweepInterval, SweepBatch: DefaultSweepBatch}
}

// Check validates that one maintenance attempt remains finite and can make progress.
func (o Options) Check() error {
	if o.SweepInterval <= 0 {
		return fmt.Errorf("the sweep interval %v is not positive: %w", o.SweepInterval, syscall.EINVAL)
	}
	if o.SweepBatch <= 0 {
		return fmt.Errorf("the sweep batch %d is not positive: %w", o.SweepBatch, syscall.EINVAL)
	}
	if o.SweepBatch > MaxSweepBatch {
		return fmt.Errorf("the sweep batch %d exceeds the finite maximum %d: %w", o.SweepBatch, MaxSweepBatch, syscall.EINVAL)
	}
	return nil
}

// MaintenanceStatus is the result of the most recent sweep attempt. A zero LastSweepTime
// means no attempt has completed yet. LastSweepError is cleared by a later successful sweep,
// so it describes the current maintenance outcome rather than an historical error log.
type MaintenanceStatus struct {
	LastSweepTime    time.Time
	LastSweepRemoved int
	LastSweepError   error
}

// New assembles a volume from the two halves that hold it and takes ownership of both.
// It uses DefaultOptions, so committed mutations trigger prompt bounded cleanup and the
// periodic pass retries retained garbage after transient failures or a quiet restart.
func New(objects Objects, meta metastore.Store) *Storage {
	options := DefaultOptions()
	return newStorage(objects, meta, options.SweepInterval, options.SweepBatch)
}

// NewWithOptions assembles a volume whose sweeper is owned by the returned Storage. A
// successful call takes ownership of objects and meta; a failed call leaves both with the
// caller. The sweeper uses a storage-lifetime context, so cancellation of the request which
// caused garbage does not cancel its later cleanup.
func NewWithOptions(objects Objects, meta metastore.Store, options Options) (*Storage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}

	return newStorage(objects, meta, options.SweepInterval, options.SweepBatch), nil
}

func newStorage(objects Objects, meta metastore.Store, interval time.Duration, batch int) *Storage {
	lifetime, stop := context.WithCancel(context.Background())
	cleanupContext, stopCleanup := context.WithCancel(context.Background())
	s := &Storage{
		objects:            objects,
		meta:               meta,
		now:                time.Now,
		maintenanceStop:    stop,
		maintenanceDone:    make(chan struct{}),
		maintenanceTrigger: make(chan struct{}, 1),
		cleanupContext:     cleanupContext,
		stopCleanup:        stopCleanup,
		sweepPermit:        make(chan struct{}, 1),
	}
	s.sweepPermit <- struct{}{}
	go s.maintain(lifetime, interval, batch)
	s.sweepAfterMutation()
	return s
}

// Close stops storage-owned maintenance, waits for every sweep already in progress, and
// releases both durable halves. The metastore closes first while object-store ownership is
// still held; both close failures are returned. Concurrent callers receive the same result.
// A retained-file cleanup refusal preserves both halves and can be retried.
func (s *Storage) Close() error {
	s.closeMu.Lock()
	if s.closeDone != nil {
		done := s.closeDone
		select {
		case <-done:
			if !s.fileCloseRetry {
				err := s.closeErr
				s.closeMu.Unlock()
				return err
			}
		default:
			s.closeMu.Unlock()
			<-done
			s.closeMu.Lock()
			err := s.closeErr
			s.closeMu.Unlock()
			return err
		}
	}
	s.fileCloseRetry = false
	s.closeDone = make(chan struct{})
	done := s.closeDone
	s.closeMu.Unlock()

	s.stopCleanup()
	if err := s.CloseFileSessions(); err != nil {
		s.closeMu.Lock()
		s.closeErr = err
		s.fileCloseRetry = true
		close(done)
		s.closeMu.Unlock()
		return err
	}

	s.maintenanceStop()
	<-s.maintenanceDone
	// Every public operation holds this gate across all metastore and object-store steps.
	// Marking the storage closed above refuses new entrants; the write lock waits for those
	// already admitted before either durable half is released.
	s.operations.Lock()

	metaErr := s.meta.Close()
	objectsErr := s.objects.Close()
	err := errors.Join(wrapClose("metastore", metaErr), wrapClose("object store", objectsErr))
	s.operations.Unlock()

	s.closeMu.Lock()
	s.closeErr = err
	close(done)
	s.closeMu.Unlock()
	return err
}

func wrapClose(what string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("closing the %s: %w", what, err)
}

func (s *Storage) maintain(ctx context.Context, interval time.Duration, batch int) {
	defer close(s.maintenanceDone)
	var ticks <-chan time.Time
	var ticker *time.Ticker
	if interval > 0 {
		ticker = time.NewTicker(interval)
		ticks = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case <-s.maintenanceTrigger:
			s.maintainBatch(ctx, batch)
		case <-ticks:
			s.maintainBatch(ctx, batch)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Storage) maintainBatch(ctx context.Context, batch int) {
	removed, err := s.runBackgroundSweep(ctx, batch)
	if err == nil && removed == batch {
		s.sweepAfterMutation()
	}
}

// MaintenanceStatus returns a snapshot of the most recent sweep outcome.
func (s *Storage) MaintenanceStatus() MaintenanceStatus {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	return s.maintenanceStatus
}

func (s *Storage) beginOperation() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closeDone != nil {
		return fmt.Errorf("the object-store volume is closed: %w", syscall.EIO)
	}
	s.operations.RLock()
	return nil
}

func (s *Storage) endOperation() { s.operations.RUnlock() }

func (s *Storage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return storage.Attr{}, &os.PathError{Op: "stat", Path: path, Err: err}
	}
	if err := s.beginOperation(); err != nil {
		return storage.Attr{}, &os.PathError{Op: "stat", Path: path, Err: err}
	}
	defer s.endOperation()
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
	if err := s.beginOperation(); err != nil {
		return &os.PathError{Op: "setattr", Path: path, Err: err}
	}
	defer s.endOperation()
	return s.meta.SetAttr(ctx, cleaned, change)
}

// CheckBounded verifies that neither durable half requires an unbounded intermediate for
// ReadBounded or ListBounded. A handler calls this before accepting requests.
func (s *Storage) CheckBounded() error {
	if _, ok := s.objects.(BoundedObjects); !ok {
		return errors.New("the object backend does not implement bounded reads")
	}
	if _, ok := s.meta.(metastore.BoundedLister); !ok {
		return errors.New("the metastore does not implement bounded listings")
	}
	return nil
}

func (s *Storage) List(ctx context.Context, path string) ([]storage.Entry, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return nil, &os.PathError{Op: "list", Path: path, Err: err}
	}
	if err := s.beginOperation(); err != nil {
		return nil, &os.PathError{Op: "list", Path: path, Err: err}
	}
	defer s.endOperation()
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

// ListBounded converts children directly from one metastore query into the caller's
// bounded result. No complete []metastore.Child exists beside the returned entries.
func (s *Storage) ListBounded(ctx context.Context, path string, result *storage.ListResult) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "list", Path: path, Err: err}
	}
	if result == nil {
		return &os.PathError{Op: "list", Path: path, Err: syscall.EINVAL}
	}
	lister, ok := s.meta.(metastore.BoundedLister)
	if !ok {
		return &os.PathError{Op: "list", Path: path, Err: syscall.ENOSYS}
	}
	if err := s.beginOperation(); err != nil {
		return &os.PathError{Op: "list", Path: path, Err: err}
	}
	defer s.endOperation()
	return lister.ListBounded(ctx, cleaned, result)
}

// Read returns the whole contents of the file at path.
//
// Nothing here answers for a symbolic link, and nothing needs to: the storage contract
// offers no operation that makes one, so a volume reachable only through it never comes
// to hold one. A local directory is different because something outside this system can
// make a link in it; a metastore has no outside.
//
// Reading is two steps — ask the tree which object, then ask for that object — and a write
// can land between them. When it does, the object the tree named a moment ago has already
// been swept, and the read must tell that apart from the one thing it looks exactly like:
// a volume that has lost bytes it still claims to hold. The difference is whether the
// tree still names the object that is missing. If it names a different one, the file was
// replaced and the new contents are as valid an answer as the old ones would have been. If
// it names the same one, something that should exist does not, and that is reported rather
// than retried, because no number of retries will make it appear.
func (s *Storage) Read(ctx context.Context, path string) ([]byte, error) {
	return s.read(ctx, path, nil)
}

// ReadBounded refuses a file from its metastore size before asking the object store for
// bytes, then gives the same bound to the object backend so corrupted or inconsistent
// object metadata cannot trigger a larger allocation below this layer.
func (s *Storage) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, &os.PathError{Op: "read", Path: path, Err: syscall.EINVAL}
	}
	return s.read(ctx, path, &maxBytes)
}

func (s *Storage) read(ctx context.Context, path string, maxBytes *int64) ([]byte, error) {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return nil, &os.PathError{Op: "read", Path: path, Err: err}
	}
	if err := s.beginOperation(); err != nil {
		return nil, &os.PathError{Op: "read", Path: path, Err: err}
	}
	defer s.endOperation()
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
			if node.Size != 0 {
				return nil, fmt.Errorf("the contents of %s have size %d but no object: %w", path, node.Size, syscall.EIO)
			}
			return nil, nil
		case node.Content == missing:
			return nil, fmt.Errorf("the contents of %s are recorded under an object the store does not have: %w", path, syscall.EIO)
		}
		if maxBytes != nil && node.Size > *maxBytes {
			return nil, fmt.Errorf("the file contains %d bytes, above the result limit of %d: %w", node.Size, *maxBytes, syscall.EFBIG)
		}

		var content []byte
		if maxBytes == nil {
			content, err = s.objects.Get(ctx, string(node.Content))
		} else if bounded, ok := s.objects.(BoundedObjects); ok {
			content, err = bounded.GetBounded(ctx, string(node.Content), *maxBytes)
		} else {
			return nil, fmt.Errorf("the object backend cannot enforce a bounded read: %w", syscall.ENOSYS)
		}
		switch {
		case err == nil:
			if int64(len(content)) != node.Size {
				return nil, fmt.Errorf("the contents of %s are %d bytes but the volume records %d: %w",
					path, len(content), node.Size, syscall.EIO)
			}
			return content, nil
		case !isOnly(err, syscall.ENOENT):
			// The tree named an object and the object store could not produce it. That is not
			// "the file is not there" — the file is there, and its bytes are what could not be
			// reached. Reporting it as absence is the fabricated answer R-ERR-2 forbids, and
			// R-ERR-6 says a storage assembled from parts that fail separately answers for the
			// part that failed.
			return nil, objectFailure("reading", path, err)
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
// The reservation is told what it is for, so a write with nowhere to land — no such
// directory, a directory at the name, no room under the allowance — is refused before its
// bytes are sent rather than after. The commit refuses it again, and that one is the
// authority; this one only keeps a caller from paying to upload what will not be kept.
//
// Put success is the proof that this reservation created the object under its key. A Put
// error provides no such proof: the key is quarantined as unresolved and is never offered
// to deletion, including when the error is EEXIST or the response was lost after the
// request may have landed. Once Put has succeeded, a failed commit may safely abandon the
// object for collection. An ambiguously successful commit refuses abandonment as
// referenced, preserving the object the tree may already name.
func (s *Storage) Write(ctx context.Context, path string, content []byte) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "write", Path: path, Err: err}
	}
	if err := s.beginOperation(); err != nil {
		return &os.PathError{Op: "write", Path: path, Err: err}
	}
	defer s.endOperation()

	object := metastore.Object{Size: int64(len(content)), ModTime: s.now()}
	if len(content) > 0 {
		key, err := s.meta.Reserve(ctx, cleaned, object.Size)
		if err != nil {
			return err
		}
		digest, err := s.objects.Put(ctx, string(key), content)
		if err != nil {
			return s.quarantine(path, key, objectFailure("storing", path, err))
		}
		object.Key, object.Digest = key, digest
	}

	if err := s.meta.Commit(ctx, cleaned, object); err != nil {
		if object.Key != "" {
			return s.abandon(path, object.Key, err)
		}
		return err
	}
	s.sweepAfterMutation()
	return nil
}

func (s *Storage) abandon(path string, key metastore.Key, operationErr error) error {
	abandonErr := s.meta.Abandon(s.cleanupContext, key)
	s.sweepAfterMutation()
	if abandonErr == nil {
		return operationErr
	}
	if isVolumeFact(abandonErr) {
		operationErr = ambiguousCommitFailure(path, operationErr)
	}
	return errors.Join(operationErr,
		internalFailure("abandoning", path, abandonErr))
}

func (s *Storage) quarantine(path string, key metastore.Key, operationErr error) error {
	quarantineErr := s.meta.Quarantine(s.cleanupContext, key)
	if quarantineErr == nil {
		return operationErr
	}
	return errors.Join(operationErr,
		internalFailure("quarantining", path, quarantineErr))
}

func objectFailure(action, path string, err error) error {
	if isVolumeFact(err) {
		return sanitizeFailure(
			fmt.Sprintf("%s the contents of %s failed with an object-key result", action, path),
			err, isVolumeFact,
		)
	}
	return fmt.Errorf("%s the contents of %s: %w", action, path, err)
}

func internalFailure(action, path string, err error) error {
	if isVolumeFact(err) {
		return sanitizeFailure(
			fmt.Sprintf("%s the object reserved for %s reached an internal state", action, path),
			err, isVolumeFact,
		)
	}
	return fmt.Errorf("%s the object reserved for %s: %w", action, path, err)
}

func ambiguousCommitFailure(path string, err error) error {
	return sanitizeFailure(
		fmt.Sprintf("the commit outcome for %s is unknown", path),
		err,
		func(err error) bool {
			var errno syscall.Errno
			return errors.As(err, &errno)
		},
	)
}

func sanitizeFailure(description string, err error, reject func(error) bool) error {
	return &sanitizedFailureError{
		message:     fmt.Sprintf("%s (%v): %v", description, err, syscall.EIO),
		retained:    acceptedSubtrees(err, reject),
		diagnostics: classifiedSubtrees(err, reject),
	}
}

type sanitizedFailureError struct {
	message     string
	retained    []error
	diagnostics []error
}

func (e *sanitizedFailureError) Error() string { return e.message }

func (e *sanitizedFailureError) Unwrap() []error {
	return append([]error{syscall.EIO}, e.retained...)
}

func (e *sanitizedFailureError) As(target any) bool {
	if _, asksForErrno := target.(*syscall.Errno); asksForErrno {
		return false
	}
	for _, diagnostic := range e.diagnostics {
		if errors.As(diagnostic, target) {
			return true
		}
	}
	return false
}

func acceptedSubtrees(err error, reject func(error) bool) []error {
	if err == nil {
		return nil
	}
	if !reject(err) {
		return []error{err}
	}
	// A classified error deliberately separates its safe public rendering from diagnostic
	// causes retained for errors.As. Once its classification is rejected, unwrapping those
	// causes here would bypass that rendering and expose transport dumps through errors.Join.
	if _, ok := err.(interface{ Classification() error }); ok {
		return nil
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var accepted []error
		for _, child := range joined.Unwrap() {
			accepted = append(accepted, acceptedSubtrees(child, reject)...)
		}
		return accepted
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return acceptedSubtrees(wrapped.Unwrap(), reject)
	}
	return nil
}

func classifiedSubtrees(err error, reject func(error) bool) []error {
	if err == nil || !reject(err) {
		return nil
	}
	if _, ok := err.(interface{ Classification() error }); ok {
		return []error{err}
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var classified []error
		for _, child := range joined.Unwrap() {
			classified = append(classified, classifiedSubtrees(child, reject)...)
		}
		return classified
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return classifiedSubtrees(wrapped.Unwrap(), reject)
	}
	return nil
}

func isVolumeFact(err error) bool {
	for _, errno := range []syscall.Errno{
		syscall.ENOENT,
		syscall.EEXIST,
		syscall.EISDIR,
		syscall.ENOTDIR,
		syscall.ENOTEMPTY,
		syscall.EINVAL,
	} {
		if errors.Is(err, errno) {
			return true
		}
	}
	return false
}

func (s *Storage) Create(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "create", Path: path, Err: err}
	}
	if err := s.beginOperation(); err != nil {
		return &os.PathError{Op: "create", Path: path, Err: err}
	}
	defer s.endOperation()
	return s.meta.Create(ctx, cleaned)
}

func (s *Storage) Mkdir(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "mkdir", Path: path, Err: err}
	}
	if err := s.beginOperation(); err != nil {
		return &os.PathError{Op: "mkdir", Path: path, Err: err}
	}
	defer s.endOperation()
	return s.meta.Mkdir(ctx, cleaned)
}

func (s *Storage) Remove(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "remove", Path: path, Err: err}
	}
	if err := s.beginOperation(); err != nil {
		return &os.PathError{Op: "remove", Path: path, Err: err}
	}
	defer s.endOperation()
	if err := s.meta.Remove(ctx, cleaned); err != nil {
		return err
	}
	s.sweepAfterMutation()
	return nil
}

func (s *Storage) RemoveDir(ctx context.Context, path string) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return &os.PathError{Op: "removedir", Path: path, Err: err}
	}
	if err := s.beginOperation(); err != nil {
		return &os.PathError{Op: "removedir", Path: path, Err: err}
	}
	defer s.endOperation()
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
	if err := s.beginOperation(); err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	defer s.endOperation()
	if err := s.meta.Rename(ctx, cleanFrom, cleanTo); err != nil {
		return err
	}
	s.sweepAfterMutation()
	return nil
}

func (s *Storage) Space(ctx context.Context) (storage.Space, error) {
	if err := s.beginOperation(); err != nil {
		return storage.Space{}, err
	}
	defer s.endOperation()
	space, err := s.meta.Space(ctx)
	if err != nil {
		return storage.Space{}, err
	}
	if !space.Coherent() {
		return storage.Space{}, fmt.Errorf("the metastore reports a total of %d bytes with %d used and %d available, which cannot be true of anything: %w",
			space.Total, space.Used, space.Avail, syscall.EIO)
	}

	available, err := s.objects.Available(ctx)
	switch {
	case isOnly(err, syscall.ENOSYS):
		return space, nil
	case err != nil:
		return storage.Space{}, fmt.Errorf("measuring the object store's available space: %w", err)
	case available < 0:
		return storage.Space{}, fmt.Errorf("the object store reports %d available bytes, which cannot be true of anything: %w",
			available, syscall.EIO)
	default:
		space.Avail = min(space.Avail, available)
		return space, nil
	}
}

// isOnly reports whether every leaf in err is target. errors.Is alone is not enough for an
// unsupported-capability decision: a joined ENOSYS and I/O failure carries ENOSYS, but it
// also carries the failure that must not be hidden behind the quota figure.
func isOnly(err, target error) bool {
	return onlyLeaves(err, target)
}

func onlyLeaves(err error, targets ...error) bool {
	if err == nil {
		return false
	}
	if classified, ok := err.(interface{ Classification() error }); ok {
		return onlyLeaves(classified.Classification(), targets...)
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !onlyLeaves(child, targets...) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyLeaves(wrapped.Unwrap(), targets...)
	}
	for _, target := range targets {
		if err == target {
			return true
		}
	}
	return false
}

// Sweep deletes objects nothing references and forgets them, until it runs out or reaches
// limit. It returns how many it removed.
//
// A caller that wants a volume tidied rather than merely kept from growing calls this;
// the mutations clear only a batch each, so a volume that was written to by a process
// that then died has a backlog nobody is walking.
func (s *Storage) Sweep(ctx context.Context, limit int) (int, error) {
	if limit < 0 {
		return 0, fmt.Errorf("the sweep limit %d is negative: %w", limit, syscall.EINVAL)
	}
	if err := s.beginOperation(); err != nil {
		return 0, err
	}
	defer s.endOperation()
	if limit == 0 {
		return 0, nil
	}
	if err := s.acquireSweep(ctx); err != nil {
		return 0, err
	}
	defer s.releaseSweep()
	return s.runSweepLocked(ctx, limit)
}

func (s *Storage) runBackgroundSweep(ctx context.Context, limit int) (int, error) {
	if err := s.acquireSweep(ctx); err != nil {
		return 0, err
	}
	defer s.releaseSweep()
	return s.runSweepLocked(ctx, limit)
}

func (s *Storage) acquireSweep(ctx context.Context) error {
	select {
	case <-s.sweepPermit:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Storage) releaseSweep() { s.sweepPermit <- struct{}{} }

func (s *Storage) runSweepLocked(ctx context.Context, limit int) (int, error) {
	removed, err := s.sweep(ctx, limit)
	s.statusMu.Lock()
	s.maintenanceStatus = MaintenanceStatus{
		LastSweepTime:    s.now(),
		LastSweepRemoved: removed,
		LastSweepError:   err,
	}
	s.statusMu.Unlock()
	return removed, err
}

func (s *Storage) sweep(ctx context.Context, limit int) (int, error) {
	removed := 0
	for removed < limit {
		batch := min(limit-removed, sweepBatch)
		keys, err := s.meta.Garbage(ctx, batch)
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

// sweepAfterMutation asks the storage-owned worker to clear a bounded batch.
//
// It is called whenever a successful volume edit or a failed write can have produced
// garbage, so neither outcome waits for unrelated object deletion. The one-place buffer
// coalesces a burst, and a signal arriving while a sweep runs remains buffered for the next
// batch. Failures are retained in MaintenanceStatus, while the metastore record keeps the
// garbage eligible for later retry.
func (s *Storage) sweepAfterMutation() {
	select {
	case s.maintenanceTrigger <- struct{}{}:
	default:
	}
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
		if err := s.meta.Forget(ctx, gone); err != nil {
			failure = errors.Join(failure, err)
		}
	}
	return len(gone), failure
}
