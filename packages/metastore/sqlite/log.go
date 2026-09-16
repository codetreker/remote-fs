package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// Window is how much of a volume's change log is kept.
//
// The three numbers answer three different questions and none of them subsumes the others.
// Floor is what makes a brief disconnection resumable for a volume that is barely written
// to; Cap is the resource ceiling; Age is what keeps a database from filling with the logs of
// volumes nobody is watching, which matters because many volumes exist and few are
// mounted at any moment. That last reason is easy to lose once the log is on disk rather than
// in memory — "it is only disk" is exactly the argument that would remove it.
//
// Floor wins over Age. A volume that has been quiet for longer than Age keeps its last
// Floor entries anyway, because discarding them would turn every brief absence into a full
// rebuild for a volume where nothing had happened at all.
type Window struct {
	// Floor is the fewest entries a volume's log keeps, however old they are.
	Floor int

	// Cap is the most entries a volume's log keeps.
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
	after, err := s.captureEventImage(ctx, tx, node)
	if err != nil {
		return err
	}
	return changes.Record(ctx, tx, s.volume, metastore.Change{Kind: metastore.Created, Parent: at.Parent, Name: at.Name, Node: &node,
		Notification: &metastore.Notification{SubjectID: node.ID, SubjectKind: node.Kind, ChangeMask: metastore.ChangeName, After: after}})
}

func (s *Store) recordChanged(ctx context.Context, tx *sql.Tx, before metastore.Node) error {
	after, err := nodeByID(ctx, tx, before.ID)
	if err != nil {
		return err
	}
	if after.MetadataRevision == before.MetadataRevision {
		if _, err := s.updateChangeTime(ctx, tx, before.ID); err != nil {
			return err
		}
	}
	return s.recordChangedMask(ctx, tx, before, 0)
}

func (s *Store) recordChangedMask(ctx context.Context, tx *sql.Tx, before metastore.Node, extra metastore.ChangeMask) error {
	node, err := nodeByID(ctx, tx, before.ID)
	if err != nil {
		return err
	}
	beforeImage, err := s.captureEventImage(ctx, tx, before)
	if err != nil {
		return err
	}
	afterImage, err := s.captureEventImage(ctx, tx, node)
	if err != nil {
		return err
	}
	at := eventLocation(afterImage)
	return changes.Record(ctx, tx, s.volume, metastore.Change{Kind: metastore.Modified, Parent: at.Parent, Name: at.Name, Node: &node,
		Notification: &metastore.Notification{SubjectID: node.ID, SubjectKind: node.Kind, ChangeMask: metastore.NotificationMask(before, node) | extra, Before: beforeImage, After: afterImage}})
}

// Removal records the old entry before unlink or retirement makes its ancestry
// unavailable. Its replication Node stays nil; the image retains the old facts.
func (s *Store) recordRemoved(ctx context.Context, tx *sql.Tx, at metastore.Location, before metastore.Node) error {
	image, err := s.captureEventImage(ctx, tx, before)
	if err != nil {
		return err
	}
	return changes.Record(ctx, tx, s.volume, metastore.Change{Kind: metastore.Removed, Parent: at.Parent, Name: at.Name,
		Notification: &metastore.Notification{SubjectID: before.ID, SubjectKind: before.Kind, ChangeMask: metastore.ChangeName, Before: image}})
}

// The source image is captured before the entry moves; the destination image
// observes the updated parent revisions in the same transaction.
func (s *Store) recordRenamed(ctx context.Context, tx *sql.Tx, before *metastore.EventImage, nodeID int64) error {
	extra, err := s.updateChangeTime(ctx, tx, nodeID)
	if err != nil {
		return err
	}
	node, err := nodeByID(ctx, tx, nodeID)
	if err != nil {
		return err
	}
	after, err := s.captureEventImage(ctx, tx, node)
	if err != nil {
		return err
	}
	from, at := eventLocation(before), eventLocation(after)
	return changes.Record(ctx, tx, s.volume, metastore.Change{Kind: metastore.Renamed, Parent: at.Parent, Name: at.Name, From: &from, Node: &node,
		Notification: &metastore.Notification{SubjectID: node.ID, SubjectKind: node.Kind, ChangeMask: metastore.ChangeName | extra, Before: before, After: after}})
}

func (s *Store) updateChangeTime(ctx context.Context, tx *sql.Tx, id int64) (metastore.ChangeMask, error) {
	before, err := nodeByID(ctx, tx, id)
	if err != nil {
		return 0, err
	}
	instant := time.Now()
	sec, nsec := sqlvalue.StoredTime(instant)
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET change_sec=?,change_nsec=?,metadata_revision=metadata_revision+1 WHERE volume=? AND id=? AND metadata_revision<?`, sec, nsec, s.volume, id, int64(math.MaxInt64))
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if count != 1 {
		return 0, fmt.Errorf("node metadata revision cannot advance: %w", syscall.EOVERFLOW)
	}
	if before.ChangeTime != nil && before.ChangeTime.Equal(instant) {
		return 0, nil
	}
	return metastore.ChangeTime, nil
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
		retention, err = changes.ReadPage(ctx, tx, s.volume, s.maxIntegrityRecords, after, limit, result)
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
		barrier, err = changes.ReadLogBarrier(ctx, tx, s.volume, maxBytes, true)
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
		barrier, err := changes.ReadLogBarrier(ctx, tx, s.volume, 0, false)
		committed = barrier.Position
		return err
	}); err != nil {
		return 0, fmt.Errorf("reading the position the tree was last changed at: %w", sqlerr.Failure(err))
	}
	return committed, nil
}
