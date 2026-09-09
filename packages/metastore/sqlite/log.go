package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
)

// Window is how much of a namespace's change log is kept.
//
// The three numbers answer three different questions and none of them subsumes the others.
// Floor is what makes a brief disconnection resumable for a namespace that is barely written
// to; Cap is the resource ceiling; Age is what keeps a database from filling with the logs of
// namespaces nobody is watching, which matters because many namespaces exist and few are
// mounted at any moment. That last reason is easy to lose once the log is on disk rather than
// in memory — "it is only disk" is exactly the argument that would remove it.
//
// Floor wins over Age. A namespace that has been quiet for longer than Age keeps its last
// Floor entries anyway, because discarding them would turn every brief absence into a full
// rebuild for a namespace where nothing had happened at all.
type Window struct {
	// Floor is the fewest entries a namespace's log keeps, however old they are.
	Floor int

	// Cap is the most entries a namespace's log keeps.
	Cap int

	// Age is how long an entry is kept, subject to Floor.
	Age time.Duration
}

// DefaultWindow is the window a log is held to when a caller has no reason of its own to
// choose. The numbers are chosen rather than measured, and the note that introduced them says
// so: ten thousand entries will not survive a branch switch that touches two thousand files,
// and the cost of falling out of the window grows with the size of the tree.
func DefaultWindow() Window {
	return Window(changes.DefaultWindow())
}

// --- recording -------------------------------------------------------------------------

// recordCreated records that a name that held nothing now holds a node.
//
// The node is read back rather than assembled from what was just inserted. The two would say
// the same thing today, and a replica is exactly as correct as this record is: reading the row
// the transaction wrote is the version of that statement which cannot drift.
func (s *Store) recordCreated(ctx context.Context, tx *sql.Tx, at metastore.Location, id int64) error {
	node, err := nodeByID(ctx, tx, id)
	if err != nil {
		return err
	}
	return changes.Record(ctx, tx, s.namespace, metastore.Change{
		Kind: metastore.Created, Parent: at.Parent, Name: at.Name, Node: &node,
	})
}

// recordChanged records that a node is no longer what it was, reading back what it now is.
func (s *Store) recordChanged(ctx context.Context, tx *sql.Tx, id int64) error {
	at, err := s.locate(ctx, tx, id)
	if err != nil {
		return err
	}
	node, err := nodeByID(ctx, tx, id)
	if err != nil {
		return err
	}
	return changes.Record(ctx, tx, s.namespace, metastore.Change{
		Kind: metastore.Modified, Parent: at.Parent, Name: at.Name, Node: &node,
	})
}

// recordRemoved records that a name that held a node now holds nothing. It carries no node,
// because there is none to carry.
func (s *Store) recordRemoved(ctx context.Context, tx *sql.Tx, at metastore.Location) error {
	return changes.Record(ctx, tx, s.namespace, metastore.Change{Kind: metastore.Removed, Parent: at.Parent, Name: at.Name})
}

// recordRenamed records a node arriving at a name from another one.
//
// One row for a whole subtree: everything beneath a renamed directory keeps the parent it
// always had, so nothing beneath it has changed and nothing beneath it gets an event. A
// replica applies this by moving one entry, which is the property that made carrying the node
// in the event worth its cost — an invalidation would force it to discard the subtree and walk
// it again, and renaming directories is what build tools and version control do constantly.
//
// A rename that lands on an occupied name destroys what was there, and that destruction is
// recorded as its own Removed rather than left for a replica to infer from this row. The
// inference is available — whatever was at the name is gone by definition — but it would be
// the one place where a node stops existing without the log saying so, and a replica that got
// the rule wrong would keep the replaced node with nothing to correct it. Every node that
// ceases to exist has exactly one Removed.
func (s *Store) recordRenamed(ctx context.Context, tx *sql.Tx, at, from metastore.Location, node metastore.Node) error {
	return changes.Record(ctx, tx, s.namespace, metastore.Change{
		Kind: metastore.Renamed, Parent: at.Parent, Name: at.Name, From: &from, Node: &node,
	})
}

// locate reports where a node sits: the directory holding it, and the name it has there.
//
// The root sits nowhere and comes back as the zero Location, which is how metastore.Row and
// metastore.Change name it. Any other node without an entry is this package having lost the
// tree rather than a location to report, so it is refused instead of being reported as the
// root — a change recorded against the wrong parent is applied by a replica without complaint.
func (s *Store) locate(ctx context.Context, tx *sql.Tx, id int64) (metastore.Location, error) {
	var (
		parent int64
		name   []byte
	)
	switch err := tx.QueryRowContext(ctx,
		`SELECT parent, name FROM entries WHERE node = ? AND namespace = ?`,
		id, s.namespace).Scan(&parent, &name); {
	case errors.Is(err, sql.ErrNoRows):
		if id != s.root {
			return metastore.Location{}, fmt.Errorf("%w: node %d holds no name in this namespace", syscall.EIO, id)
		}
		return metastore.Location{}, nil
	case err != nil:
		return metastore.Location{}, err
	}
	return metastore.Location{Parent: parent, Name: name}, nil
}

// nodeByID reads one node by its id, which is how a change reports what a name holds after
// the statement that changed it.
func nodeByID(ctx context.Context, tx *sql.Tx, id int64) (metastore.Node, error) {
	return scanNode(tx.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes n WHERE n.id = ?`, id))
}

// --- retention -------------------------------------------------------------------------

// --- reading ---------------------------------------------------------------------------

// Since returns the changes recorded after a position, along with what the log still holds.
//
// The two travel together and are read in one transaction, because a caller decides between
// three different actions by comparing them — caught up, resume from here, rebuild — and a
// retention read a moment after the changes could describe a window the changes never came
// from.
//
// A caller that has fallen out of the window still gets whatever the log holds. The contract
// puts the comparison on the caller rather than having this decide for it: an implementation
// that returned nothing to a caller it judged too far behind would be answering a question
// about retention with an empty page, which reads exactly like being caught up.
func (s *Store) Since(
	ctx context.Context,
	after metastore.Position,
	limit int,
	result *metastore.ChangeResult,
) (retention metastore.Retention, returnErr error) {
	if result == nil {
		return metastore.Retention{}, fmt.Errorf("reading changes needs a bounded result: %w", syscall.EINVAL)
	}
	if after < 0 {
		return metastore.Retention{}, result.Fail(fmt.Errorf("%d is not a position: %w", after, syscall.EINVAL))
	}
	if limit < 0 {
		return metastore.Retention{}, result.Fail(fmt.Errorf("a limit of %d changes is not a count: %w", limit, syscall.EINVAL))
	}
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		retention, err = changes.ReadPage(ctx, tx, s.namespace, s.maxIntegrityRecords, after, limit, result)
		return err
	}); err != nil {
		return metastore.Retention{}, result.Fail(fmt.Errorf("reading the changes after position %d: %w", after, sqlerr.Failure(err)))
	}
	return retention, nil
}

// Incarnation names this log as a continuation of itself.
func (s *Store) Incarnation(ctx context.Context, maxBytes int64) (metastore.Incarnation, error) {
	barrier, err := s.Barrier(ctx, maxBytes)
	return barrier.Incarnation, err
}

func (s *Store) Barrier(ctx context.Context, maxBytes int64) (metastore.LogBarrier, error) {
	if maxBytes < 1 {
		return metastore.LogBarrier{}, fmt.Errorf("an incarnation byte bound of %d cannot hold an identity: %w", maxBytes, syscall.EINVAL)
	}
	var barrier metastore.LogBarrier
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		barrier, err = changes.ReadLogBarrier(ctx, tx, s.namespace, maxBytes, true)
		return err
	}); err != nil {
		return metastore.LogBarrier{}, fmt.Errorf("reading the log's barrier: %w", sqlerr.Failure(err))
	}
	return barrier, nil
}

// CommittedPosition is the newest position the tree was changed at.
//
// Here that is also the log's tail, because the two are written in one transaction. It is a
// separate question in the contract for the implementations where they can drift apart.
func (s *Store) CommittedPosition(ctx context.Context) (metastore.Position, error) {
	var committed metastore.Position
	if err := s.inspect(ctx, func(tx *sql.Tx) error {
		barrier, err := changes.ReadLogBarrier(ctx, tx, s.namespace, 0, false)
		committed = barrier.Position
		return err
	}); err != nil {
		return 0, fmt.Errorf("reading the position the tree was last changed at: %w", sqlerr.Failure(err))
	}
	return committed, nil
}
