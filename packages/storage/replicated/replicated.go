// Package replicated answers what a volume's tree looks like from a copy held here, and
// sends everything else to the server that holds the volume itself.
//
// A mount that keeps nothing locally turns every kernel call into a request. That is one
// round trip per stat, per lookup, and per name a path search asks about and does not find —
// and a toolchain asks about far more names than exist, so a volume on a 20 ms link
// spends its time almost entirely on being told that files are not there. The copy here is
// what makes those answers local: a stat becomes a query against a SQLite database in the
// same process, microseconds rather than milliseconds.
//
// # What makes a copy worth believing
//
// Keeping metadata is easy; knowing when it has become a lie is the whole problem, and
// R-CON-2 rules out the usual answer. Visibility may not depend on an interval elapsing, so
// no expiry, however short, is available — a one-second cache would satisfy R-CON-1 to the
// letter and fail R-CON-2. What is left is a copy fed by a stream: the copy is worth
// believing exactly while the change stream behind it is being observed without a break, and
// worth nothing the moment it is not. There is no third state and no interval anywhere.
//
// So this is not a cache. Nothing here expires, nothing is revalidated, and no operation
// checks whether what it holds is still current: either the stream is alive, in which case
// everything here is what the server had as of a position it can name, or the stream is not,
// in which case every volume operation fails with EIO (R-ERR-1, R-ERR-2). A copy that answered from
// a broken stream would report "no such file" for files that are there and an empty listing
// for directories that are not, which is the answer that makes whatever runs on top delete,
// regenerate and overwrite.
//
// # What is answered from where
//
// Path-based Stat and List are answered here. Everything else — the bytes of a file, every change to
// the volume, and how much room it has — goes to the server, because none of it is
// metadata this copy holds and none of it is a question a copy may answer.
// Retained file and node-identity queries also go to the authority: detached objects have
// no entry in this tree. Their mutations confirm the authority's returned log position,
// including an unchanged position when the object has no name.
// Explicit lease control also goes directly to the authority. It remains available when
// the metadata stream fails, so a caller can reconcile or release an outstanding grant.
//
// The kernel is told to cache nothing: packages/fuse leaves all three of its timeouts at
// zero, so every lookup, every stat and every listing still arrives here. That is deliberate
// and it is what keeps this layer able to be correct at all — the kernel answering from its
// own cache is the one place a stale answer could be given without this code ever running.
package replicated

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// Storage is one volume: a copy of its tree, and the server that holds it.
//
// It takes the transport's own client rather than a storage.Storage, because the two halves
// it needs are one volume. Reads and writes travel over the storage contract, and the
// stream and the picture the copy is built from are that transport's own operations; a
// second transport (R-INT-9 asks for two more) is when an interface between them earns
// itself, and building one now would be an abstraction with a single implementation.
type Storage struct {
	remote *httprest.Storage
	local  *sqlite.Replica

	// lifetime ends when this storage is closed, which is what returns the subscription and
	// stops the goroutine following it. It is not any caller's context: the stream outlives
	// the call that opened it and every call made through here afterwards.
	lifetime context.Context
	stop     context.CancelFunc
	stopped  chan struct{}

	// incarnation names the run of history the copy was built against, and is offered back
	// when reattaching.
	incarnation metastore.Incarnation
	generation  uint64

	options Options

	mu sync.Mutex

	// failure remains set through snapshot replay or resumption, and whenever the
	// stream stops. While it is set the copy may not be answered from at all.
	failure error

	// at is how far the copy has been brought, mirrored here so mutation confirmation reads
	// it in the same critical section that records what has arrived.
	at metastore.Position

	// behind is the fixed replay target for a snapshot gate or resumed stream.
	// failure also covers a snapshot gate at zero and its cancellation handoff;
	// reaching behind alone cannot make a newly installed snapshot usable.
	behind metastore.Position

	// notify is closed and replaced whenever a waiter's answer may have changed: a change
	// applied, the stream broken, the stream alive again.
	notify chan struct{}

	// confirmationCapacity is separate from notify so an unrelated change flood does not wake every
	// caller waiting only for confirmation capacity.
	confirmationCapacity chan struct{}

	// activeConfirmations counts fixed-size records whose server result or local barrier
	// confirmation is still in flight. Callers waiting to enter are bounded separately.
	activeConfirmations int
	confirmationWaiters int
	closing             bool
	closeMu             sync.Mutex
	fileSessions        map[*fileSession]struct{}
	fileSessionOpening  int
}

var _ storage.Storage = (*Storage)(nil)
var _ storage.BoundedStorage = (*Storage)(nil)

// New builds a copy of the volume at remote in local, and returns once it is usable.
//
// It blocks until the captured tree has been installed and replayed through a checkpoint
// obtained after snapshot delivery. The copy cannot answer queries before that point.
// A failure names the step that failed, because "the mount did not come up" is not something
// an operator can act on.
//
// The order is subscribe, then take the picture, and it is not interchangeable. The reverse
// — take the picture, then subscribe from the position it was taken at — does not converge:
// the scan takes time, changes accumulate while it runs, and once enough of them accumulate
// to push that position out of the server's retention window the copy has to be built again,
// at a cost proportional to the size of the tree. The larger the volume, the less likely
// it is ever to finish, which is the worst direction that coupling can run in. Subscribing
// first takes the window off this path entirely: the stream is already attached at a
// position no later than the picture's, so however long the picture takes, nothing that
// happened meanwhile has been missed.
//
// local is closed by Close, and where it lives is the caller's decision.
func New(ctx context.Context, local *sqlite.Replica, remote *httprest.Storage) (*Storage, error) {
	return NewWithOptions(ctx, local, remote, DefaultOptions())
}

// NewWithConfirmationGrace is New with the bound on how long a successful mutation waits for
// the copy to reach its returned barrier. What that bound is for is on DefaultConfirmationGrace.
func NewWithConfirmationGrace(ctx context.Context, local *sqlite.Replica, remote *httprest.Storage, grace time.Duration) (*Storage, error) {
	options := DefaultOptions()
	options.ConfirmationGrace = grace
	return NewWithOptions(ctx, local, remote, options)
}

// NewWithOptions is New with explicit bounds for snapshot replay, mutation
// confirmation resources and file-session ownership.
func NewWithOptions(ctx context.Context, local *sqlite.Replica, remote *httprest.Storage, options Options) (*Storage, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	if options.MaxFileSessions == 0 {
		options.MaxFileSessions = DefaultMaxFileSessions
	}
	lifetime, stop := context.WithCancel(context.Background())
	s := &Storage{
		remote:               remote,
		local:                local,
		lifetime:             lifetime,
		stop:                 stop,
		stopped:              make(chan struct{}),
		notify:               make(chan struct{}),
		confirmationCapacity: make(chan struct{}),
		options:              options,
		failure:              errors.New("the copy of this volume has not been built yet"),
	}

	sub, release, err := s.build(ctx)
	if err != nil {
		stop()
		return nil, err
	}
	go s.follow(sub, release)
	return s, nil
}

// Close stops following the volume, closes owned file sessions, and releases
// the copy. The remote client's lifetime remains owned by its caller.
func (s *Storage) Close() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	s.mu.Lock()
	s.closing = true
	s.wake()
	s.wakeConfirmationCapacity()
	s.mu.Unlock()
	s.stop()
	<-s.stopped

	s.mu.Lock()
	for s.activeConfirmations != 0 || s.confirmationWaiters != 0 || s.fileSessionOpening != 0 {
		notify := s.confirmationCapacity
		s.mu.Unlock()
		<-notify
		s.mu.Lock()
	}
	s.mu.Unlock()
	return errors.Join(s.closeFileSessions(), s.local.Close())
}

// Stat reports the node at path from the copy.
func (s *Storage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	if err := s.usable("stat", path); err != nil {
		return storage.Attr{}, err
	}
	node, err := s.local.Stat(ctx, path)
	if err != nil {
		return storage.Attr{}, err
	}
	return node.Attr(), nil
}

func (s *Storage) CheckBounded() error { return s.remote.CheckBounded() }

// List returns the entries of the directory at path from the copy.
func (s *Storage) List(ctx context.Context, path string) ([]storage.Entry, error) {
	if err := s.usable("list", path); err != nil {
		return nil, err
	}
	children, err := s.local.List(ctx, path)
	if err != nil {
		return nil, err
	}
	entries := make([]storage.Entry, len(children))
	for i, child := range children {
		entries[i] = storage.Entry{Name: string(child.Name), Attr: child.Node.Attr()}
	}
	return entries, nil
}

// ListBounded holds the same replica position for the whole ordered query and transfers
// each child directly into the caller's bounded result.
func (s *Storage) ListBounded(ctx context.Context, path string, result *storage.ListResult) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	if err := s.usable("list", path); err != nil {
		return err
	}
	return s.local.ListBounded(ctx, path, result)
}

// Read returns the contents of the file at path, from the server.
//
// Contents are not replicated and are not this copy's to answer. Only the tree is here.
func (s *Storage) Read(ctx context.Context, path string) ([]byte, error) {
	if err := s.usable("read", path); err != nil {
		return nil, err
	}
	return s.remote.Read(ctx, path)
}

func (s *Storage) ReadBounded(ctx context.Context, path string, maxBytes int64) ([]byte, error) {
	if err := s.usable("read", path); err != nil {
		return nil, err
	}
	return s.remote.ReadBounded(ctx, path, maxBytes)
}

// Space reports the room the volume has, from the server. A volume's allowance and
// what is taken of it are facts about the volume rather than about its tree, and nothing
// in the change log carries them.
func (s *Storage) Space(ctx context.Context) (storage.Space, error) {
	if err := s.usable("space", ""); err != nil {
		return storage.Space{}, err
	}
	return s.remote.Space(ctx)
}

// --- changing the volume --------------------------------------------------------------

// The mutations. Each is performed by the server and returns only once the copy has applied
// through the atomic log barrier returned with the successful response.
//
// The barrier makes R-CON-4 hold: a program that writes a file and then stats it must see what
// it wrote, including the size and modification time. Returning before the copy reaches the
// barrier could expose the previous contents. The position also avoids guessing which event a
// mutation produced when other writers change the same name concurrently.
//
// What that costs, stated here rather than left to be found: every mutation now takes as long
// as the server needs plus as long as its barrier needs to be applied. No second request is made
// — the changes are already travelling on a connection that is open — but one more crossing of
// the network is in the latency of every write, every create, every rename, and on a link
// where a round trip is 20 ms a write costs about half of one again.
//
// The wait has a ceiling, DefaultConfirmationGrace. A copy that has not reached the barrier by
// then reports EIO even though the change did happen, and it is that one call that gives up:
// the stream carries on, because a stream working through a backlog looks exactly like one
// that has stopped, and whether it has stopped is answered by the bound the transport keeps
// on a stream that has gone quiet. A caller that sees it should read rather than write again
// — the failure is this side's inability to confirm, not a statement that nothing happened.

func (s *Storage) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	if change.Empty() {
		if err := s.usable("setattr", path); err != nil {
			return err
		}
		return s.remote.SetAttr(ctx, path, change)
	}
	return s.change(ctx, "setattr", path, func(sendCtx context.Context) (httprest.MutationBarrier, error) {
		return s.remote.SetAttrWithBarrier(sendCtx, path, change)
	})
}

func (s *Storage) Write(ctx context.Context, path string, content []byte) error {
	return s.change(ctx, "write", path,
		func(sendCtx context.Context) (httprest.MutationBarrier, error) {
			return s.remote.WriteWithBarrier(sendCtx, path, content)
		})
}

func (s *Storage) Create(ctx context.Context, path string) error {
	return s.change(ctx, "create", path,
		func(sendCtx context.Context) (httprest.MutationBarrier, error) {
			return s.remote.CreateWithBarrier(sendCtx, path)
		})
}

func (s *Storage) Mkdir(ctx context.Context, path string) error {
	return s.change(ctx, "mkdir", path,
		func(sendCtx context.Context) (httprest.MutationBarrier, error) {
			return s.remote.MkdirWithBarrier(sendCtx, path)
		})
}

func (s *Storage) Remove(ctx context.Context, path string) error {
	return s.change(ctx, "unlink", path,
		func(sendCtx context.Context) (httprest.MutationBarrier, error) {
			return s.remote.RemoveWithBarrier(sendCtx, path)
		})
}

func (s *Storage) RemoveDir(ctx context.Context, path string) error {
	return s.change(ctx, "rmdir", path,
		func(sendCtx context.Context) (httprest.MutationBarrier, error) {
			return s.remote.RemoveDirWithBarrier(sendCtx, path)
		})
}

// Renaming a name onto itself is the exception. POSIX has rename(2) "return successfully and
// perform no other action" when both names resolve to one entry, so nothing is recorded and
// no barrier confirmation is required. The operation is still sent because whether the name
// exists, and whether it is the root, are the server's answers.
func (s *Storage) Rename(ctx context.Context, from, to string) error {
	if err := s.usable("rename", from); err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err.Err}
	}
	if cleanFrom, err := storage.CleanPath(from); err == nil {
		if cleanTo, err := storage.CleanPath(to); err == nil && cleanFrom == cleanTo {
			return s.remote.Rename(ctx, from, to)
		}
	}
	return s.change(ctx, "rename", to, func(sendCtx context.Context) (httprest.MutationBarrier, error) {
		return s.remote.RenameWithBarrier(sendCtx, from, to)
	})
}

// change performs one mutation and returns once the copy holds it.
//
// The wait is registered before the mutation is sent. The event can arrive before the
// response to the request that caused it — they travel on different connections — and a
// caller that started watching afterwards would be watching for something that had already
// happened.
func (s *Storage) change(ctx context.Context, op, path string, send func(context.Context) (httprest.MutationBarrier, error)) error {
	if err := s.usable(op, path); err != nil {
		return err
	}
	if _, err := storage.CleanPath(path); err != nil {
		return &os.PathError{Op: op, Path: path, Err: err}
	}
	confirmation, err := s.expect(ctx, op, path)
	if err != nil {
		return err
	}
	defer s.forget(confirmation)

	if err := ctx.Err(); err != nil {
		return confirmationContextError(op, path, ctx)
	}
	s.mu.Lock()
	closing := s.closing
	s.mu.Unlock()
	if closing {
		return confirmationAdmissionError(op, path, errors.New("the replicated storage started closing before the mutation was sent"))
	}

	sendCtx, cancel := context.WithCancelCause(ctx)
	stopCancellation := context.AfterFunc(s.lifetime, func() {
		cancel(errors.New("the replicated storage was closed"))
	})
	defer func() {
		stopCancellation()
		cancel(nil)
	}()

	barrier, err := send(sendCtx)
	if err != nil {
		return err
	}
	if err := s.setBarrier(confirmation, barrier); err != nil {
		return &os.PathError{Op: op, Path: path, Err: err}
	}
	return s.await(ctx, op, path, confirmation)
}

func confirmationAdmissionError(op, path string, cause error) error {
	return &os.PathError{Op: op, Path: path, Err: &confirmationRefusal{errno: syscall.EAGAIN, cause: cause}}
}

// The caller's context ended before dispatch. Its reason owns the result; a custom
// cancellation cause remains diagnostic and cannot turn an interrupted call into EIO.
func confirmationContextError(op, path string, ctx context.Context) error {
	ended, cause := ctx.Err(), context.Cause(ctx)
	if cause != ended {
		cause = errors.Join(ended, cause)
	}
	return &os.PathError{Op: op, Path: path, Err: &confirmationRefusal{errno: storage.ErrnoOf(ended), cause: cause}}
}

type confirmationRefusal struct {
	errno syscall.Errno
	cause error
}

func (e *confirmationRefusal) Error() string {
	return fmt.Sprintf("mutation confirmation was not admitted before the mutation was sent: %v: %v", e.cause, e.errno)
}

func (e *confirmationRefusal) Unwrap() []error       { return []error{e.errno, e.cause} }
func (e *confirmationRefusal) Classification() error { return e.errno }
