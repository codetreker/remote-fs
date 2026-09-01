// Package replicated answers what a namespace's tree looks like from a copy held here, and
// sends everything else to the server that holds the namespace itself.
//
// A mount that keeps nothing locally turns every kernel call into a request. That is one
// round trip per stat, per lookup, and per name a path search asks about and does not find —
// and a toolchain asks about far more names than exist, so a workspace on a 20 ms link
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
// in which case every operation fails with EIO (R-ERR-1, R-ERR-2). A copy that answered from
// a broken stream would report "no such file" for files that are there and an empty listing
// for directories that are not, which is the answer that makes whatever runs on top delete,
// regenerate and overwrite.
//
// # What is answered from where
//
// Stat and List are answered here. Everything else — the bytes of a file, every change to
// the namespace, and how much room it has — goes to the server, because none of it is
// metadata this copy holds and none of it is a question a copy may answer.
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

// Storage is one namespace: a copy of its tree, and the server that holds it.
//
// It takes the transport's own client rather than a storage.Storage, because the two halves
// it needs are one namespace. Reads and writes travel over the storage contract, and the
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

	// grace is how long one caller waits for the change it made to come back. What it is
	// for, and why running out of it is one operation's failure rather than the stream's,
	// is on DefaultEchoGrace.
	grace time.Duration

	mu sync.Mutex

	// failure is what ended the stream, and is nil for as long as the stream is alive. While
	// it is set the copy may not be answered from at all.
	failure error

	// at is how far the copy has been brought, mirrored here so that a caller waiting for
	// its own change to come back reads it in the same critical section that records what
	// has arrived.
	at metastore.Position

	// behind is the position a resumed stream said the log had reached, while the copy has
	// not reached it yet. It is zero when the copy is current. Between the two the copy is
	// knowingly missing changes and may not be answered from, which is what separates being
	// behind by what is in flight — ordinary, and true at every moment — from being behind
	// by an outage the log has just described.
	behind metastore.Position

	// notify is closed and replaced whenever a waiter's answer may have changed: a change
	// applied, the stream broken, the stream alive again.
	notify chan struct{}

	// waiting holds the position each caller watching for its own change started from, and
	// touched is what has changed at a name since the earliest of them. Nothing is recorded
	// while nobody is waiting, and what no remaining waiter could be released by is dropped
	// as each one leaves — so the map holds the changes of the longest mutation still in
	// flight rather than of the namespace's life (R-INT-3).
	waiting []metastore.Position
	touched map[location]touch
}

var _ storage.Storage = (*Storage)(nil)

// New builds a copy of the namespace at remote in local, and returns once it is usable.
//
// It blocks. There is no pass-through mode here and no degraded one: until the copy has been
// filled there is nothing to answer from, and a mount that answered anyway would be
// answering from an empty tree. A failure names the step that failed, because "the mount did
// not come up" is not something an operator can act on.
//
// The order is subscribe, then take the picture, and it is not interchangeable. The reverse
// — take the picture, then subscribe from the position it was taken at — does not converge:
// the scan takes time, changes accumulate while it runs, and once enough of them accumulate
// to push that position out of the server's retention window the copy has to be built again,
// at a cost proportional to the size of the tree. The larger the namespace, the less likely
// it is ever to finish, which is the worst direction that coupling can run in. Subscribing
// first takes the window off this path entirely: the stream is already attached at a
// position no later than the picture's, so however long the picture takes, nothing that
// happened meanwhile has been missed.
//
// local is closed by Close, and where it lives is the caller's decision.
func New(ctx context.Context, local *sqlite.Replica, remote *httprest.Storage) (*Storage, error) {
	return NewWithEchoGrace(ctx, local, remote, DefaultEchoGrace)
}

// NewWithEchoGrace is New with the bound on how long a caller waits for its own change given
// rather than defaulted. What that bound is for is on DefaultEchoGrace.
func NewWithEchoGrace(ctx context.Context, local *sqlite.Replica, remote *httprest.Storage, grace time.Duration) (*Storage, error) {
	if grace <= 0 {
		return nil, fmt.Errorf("a caller allowed %v to see the change it made is one that cannot be told it happened: %w", grace, syscall.EINVAL)
	}
	lifetime, stop := context.WithCancel(context.Background())
	s := &Storage{
		remote:   remote,
		local:    local,
		lifetime: lifetime,
		stop:     stop,
		stopped:  make(chan struct{}),
		notify:   make(chan struct{}),
		grace:    grace,
		touched:  map[location]touch{},
		failure:  errors.New("the copy of this namespace has not been built yet"),
	}

	sub, err := s.build(ctx)
	if err != nil {
		stop()
		return nil, err
	}
	go s.follow(sub)
	return s, nil
}

// Close stops following the namespace and releases the copy.
func (s *Storage) Close() error {
	s.stop()
	<-s.stopped
	return s.local.Close()
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

// Read returns the contents of the file at path, from the server.
//
// Contents are not replicated and are not this copy's to answer. Only the tree is here.
func (s *Storage) Read(ctx context.Context, path string) ([]byte, error) {
	if err := s.usable("read", path); err != nil {
		return nil, err
	}
	return s.remote.Read(ctx, path)
}

// Space reports the room the namespace has, from the server. A namespace's allowance and
// what is taken of it are facts about the namespace rather than about its tree, and nothing
// in the change log carries them.
func (s *Storage) Space(ctx context.Context) (storage.Space, error) {
	if err := s.usable("space", ""); err != nil {
		return storage.Space{}, err
	}
	return s.remote.Space(ctx)
}

// --- changing the namespace --------------------------------------------------------------

// The mutations. Each is performed by the server and each returns only once the change it
// made has come back on the stream and been applied here.
//
// Waiting for the echo is what makes R-CON-4 hold: a program that writes a file and then
// stats it must see what it wrote, including the size and the modification time. Between the
// server committing a change and its event arriving, the copy still holds what the name held
// before — so a mutation that returned at the commit would leave the caller one stat away
// from the previous contents, which is what a build tool comparing a target against its
// sources reads. There is no version of this that reports a locally invented size or time
// instead: what a mutation waits for is the server's own record of what it did.
//
// What that costs, stated here rather than left to be found: every mutation now takes as long
// as the server needs plus as long as its event needs to come back. No second request is made
// — the event is already travelling on a connection that is open — but one more crossing of
// the network is in the latency of every write, every create, every rename, and on a link
// where a round trip is 20 ms a write costs about half of one again.
//
// And the wait has a ceiling, DefaultEchoGrace. A mutation whose event has not arrived by
// then reports EIO even though the change did happen, and it is that one call that gives up:
// the stream carries on, because a stream working through a backlog looks exactly like one
// that has stopped, and whether it has stopped is answered by the bound the transport keeps
// on a stream that has gone quiet. A caller that sees it should read rather than write again
// — the failure is this side's inability to confirm, not a statement that nothing happened.

func (s *Storage) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	// A change that names no attribute changes nothing, and a namespace records nothing for
	// it — so there is no echo to wait for, and waiting for one would hang until the grace
	// below ran out. It is still sent, because whether the node is there at all is the
	// server's answer rather than this copy's.
	echo := &echoed{path: path, holds: true}
	if change.Empty() {
		echo = nil
	}
	return s.change(ctx, "setattr", path, echo, func() error { return s.remote.SetAttr(ctx, path, change) })
}

func (s *Storage) Write(ctx context.Context, path string, content []byte) error {
	return s.change(ctx, "write", path, &echoed{path: path, holds: true},
		func() error { return s.remote.Write(ctx, path, content) })
}

func (s *Storage) Create(ctx context.Context, path string) error {
	return s.change(ctx, "create", path, &echoed{path: path, holds: true},
		func() error { return s.remote.Create(ctx, path) })
}

func (s *Storage) Mkdir(ctx context.Context, path string) error {
	return s.change(ctx, "mkdir", path, &echoed{path: path, holds: true},
		func() error { return s.remote.Mkdir(ctx, path) })
}

func (s *Storage) Remove(ctx context.Context, path string) error {
	return s.change(ctx, "unlink", path, &echoed{path: path, holds: false},
		func() error { return s.remote.Remove(ctx, path) })
}

func (s *Storage) RemoveDir(ctx context.Context, path string) error {
	return s.change(ctx, "rmdir", path, &echoed{path: path, holds: false},
		func() error { return s.remote.RemoveDir(ctx, path) })
}

// Rename waits for the destination, which is the one name a rename changes: applying that
// one change empties the source and fills the destination in a single step, because a rename
// is one row in the log and one row here.
//
// Renaming a name onto itself is the exception. POSIX has rename(2) "return successfully and
// perform no other action" when both names resolve to one entry, so nothing is recorded and
// there is no echo — but it is still sent, because whether the name is there at all, and
// whether it is the root, are the server's answers.
func (s *Storage) Rename(ctx context.Context, from, to string) error {
	if err := s.usable("rename", from); err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err.Err}
	}
	var echo *echoed
	if cleanFrom, err := storage.CleanPath(from); err == nil {
		if cleanTo, err := storage.CleanPath(to); err == nil && cleanFrom != cleanTo {
			echo = &echoed{path: to, holds: true}
		}
	}
	return s.change(ctx, "rename", to, echo, func() error { return s.remote.Rename(ctx, from, to) })
}

// echoed is the change a mutation waits to see come back: the name it acted on, and whether
// that name holds a node afterwards.
//
// The direction matters, and the case that shows why is a rename onto an occupied name. That
// records two changes — the destination emptied, then the node arriving there — and a caller
// released by the first of them would stat the destination and be told there is nothing
// there, which is the one answer this system exists not to give.
type echoed struct {
	path  string
	holds bool
}

// change performs one mutation and returns once the copy holds it.
//
// The wait is registered before the mutation is sent. The event can arrive before the
// response to the request that caused it — they travel on different connections — and a
// caller that started watching afterwards would be watching for something that had already
// happened.
func (s *Storage) change(ctx context.Context, op, path string, echo *echoed, send func() error) error {
	if err := s.usable(op, path); err != nil {
		return err
	}
	after := s.expect()
	defer s.forget(after)

	if err := send(); err != nil {
		return err
	}
	if echo == nil {
		return nil
	}
	return s.await(ctx, op, after, *echo)
}
