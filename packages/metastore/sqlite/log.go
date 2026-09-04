package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
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
	return Window{Floor: 100, Cap: 10000, Age: 10 * time.Minute}
}

// check refuses a window that describes nothing a log could be held to. There is no repair
// here and no substitution of a default: a caller that asked for a log holding no entries
// asked for something this package will not do quietly, and the trim below reads these
// numbers as facts.
func (w Window) check() error {
	switch {
	case w.Floor < 1:
		return fmt.Errorf("a log keeping at least %d entries would have nothing to resume from: %w",
			w.Floor, syscall.EINVAL)
	case w.Cap < w.Floor:
		return fmt.Errorf("a log capped at %d entries cannot keep the %d it is required to: %w",
			w.Cap, w.Floor, syscall.EINVAL)
	case w.Age <= 0:
		return fmt.Errorf("entries older than %v is every entry there will ever be: %w",
			w.Age, syscall.EINVAL)
	}
	return nil
}

// The numbers the log stores for metastore.ChangeKind.
//
// They are written down here rather than taken from the constants' own values, because those
// come from an iota in another package: reordering it is an edit nobody would think to check
// this file for, and the damage would be a stored log whose Created rows read back as Removed
// against replicas that had already applied them.
const (
	kindCreated  = 0
	kindRemoved  = 1
	kindModified = 2
	kindRenamed  = 3
)

func storedKind(kind metastore.ChangeKind) (int64, error) {
	switch kind {
	case metastore.Created:
		return kindCreated, nil
	case metastore.Removed:
		return kindRemoved, nil
	case metastore.Modified:
		return kindModified, nil
	case metastore.Renamed:
		return kindRenamed, nil
	}
	return 0, fmt.Errorf("%w: %d is not a kind of change", syscall.EIO, kind)
}

func loadedKind(stored int64) (metastore.ChangeKind, error) {
	switch stored {
	case kindCreated:
		return metastore.Created, nil
	case kindRemoved:
		return metastore.Removed, nil
	case kindModified:
		return metastore.Modified, nil
	case kindRenamed:
		return metastore.Renamed, nil
	}
	return 0, fmt.Errorf("%w: the log holds a change of kind %d, which this build has no meaning for",
		syscall.EIO, stored)
}

// --- recording -------------------------------------------------------------------------

// record appends one change to the namespace's log and moves the committed position onto it,
// inside the caller's transaction.
//
// Being inside that transaction is the whole reason the log lives in this database. A log
// appended to after the tree change committed would leave a window in which the change
// happened and its event did not, and once the log survives a restart that window stops
// healing itself: the incarnation is unchanged, so a replica resuming at the position before
// the lost event is told it is caught up, and the node that changed stays wrong in every
// replica forever.
//
// The position is whatever the insert allocated. Nothing returns it to the caller: the log is
// the record of what happened, and an operation handing its position back would invite a
// second path by which somebody could learn about a change.
func (s *Store) record(ctx context.Context, tx *sql.Tx, change metastore.Change) error {
	kind, err := storedKind(change.Kind)
	if err != nil {
		return err
	}

	var fromParent, fromName any
	if change.From != nil {
		fromParent, fromName = change.From.Parent, change.From.Name
	}
	var node, mode, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec, content any
	if change.Node != nil {
		accessSec, accessNsec := storedTime(change.Node.AccessTime)
		changeSec, changeNsec := storedTime(change.Node.ModTime)
		node, mode, size = change.Node.ID, int64(change.Node.Mode), change.Node.Size
		atimeSec, atimeNsec = accessSec, accessNsec
		mtimeSec, mtimeNsec = changeSec, changeNsec
		content = storedKey(change.Node.Content)
	}

	sec, nsec := storedTime(time.Now())
	result, err := tx.ExecContext(ctx, `
		INSERT INTO changes (namespace, kind, parent, name, from_parent, from_name,
		                     node, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content,
		                     recorded_sec, recorded_nsec)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		s.namespace, kind, change.Parent, change.Name, fromParent, fromName,
		node, mode, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec, content,
		sec, nsec)
	if err != nil {
		return err
	}
	position, err := result.LastInsertId()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE logs SET committed_position = ? WHERE namespace = ?`,
		position, s.namespace)
	return err
}

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
	return s.record(ctx, tx, metastore.Change{
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
	return s.record(ctx, tx, metastore.Change{
		Kind: metastore.Modified, Parent: at.Parent, Name: at.Name, Node: &node,
	})
}

// recordRemoved records that a name that held a node now holds nothing. It carries no node,
// because there is none to carry.
func (s *Store) recordRemoved(ctx context.Context, tx *sql.Tx, at metastore.Location) error {
	return s.record(ctx, tx, metastore.Change{Kind: metastore.Removed, Parent: at.Parent, Name: at.Name})
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
	return s.record(ctx, tx, metastore.Change{
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

// trim discards what the window no longer covers, inside the caller's transaction.
//
// It rides on every write transaction rather than on a goroutine of its own, and Open runs it
// once more. A background sweeper would need a lifetime and a stop path for what is one
// DELETE; trimming lazily inside Since would leave a namespace nobody subscribes to untrimmed
// forever, which is precisely the case the age rule exists for.
//
// Known gap, recorded rather than fixed: a namespace written to once and then quiet, on a
// server that does not restart, sits at whatever it reached and never shrinks. Nothing here
// runs without a transaction to ride on. Ten thousand entries is roughly two megabytes, so a
// thousand such namespaces is a couple of gigabytes; the fix is a background trim or one at
// mount and unmount, and neither is worth its lifetime yet.
func trim(ctx context.Context, tx *sql.Tx, namespace int64, window Window) error {
	oldest, oldestAt, err := oldestEntry(ctx, tx, namespace)
	if err != nil || oldest == 0 {
		return err
	}
	newest, err := newestEntry(ctx, tx, namespace)
	if err != nil {
		return err
	}

	// Two index lookups decide the common case, which is that there is nothing to do. The span
	// between the oldest and the newest position overstates the count, because the sequence is
	// shared with the other namespaces in this database — an overstatement only ever costs the
	// count below, never a wrong answer.
	span := int64(newest - oldest + 1)
	overCap := span > int64(window.Cap)
	tooOld := oldestAt.Before(time.Now().Add(-window.Age))
	if !overCap && !tooOld {
		return nil
	}

	var held int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM changes WHERE namespace = ?`, namespace).Scan(&held); err != nil {
		return err
	}

	// byVolume is the cut that leaves exactly Cap entries, byFloor the cut that leaves exactly
	// Floor. Age may cut no deeper than byFloor, which is how the floor wins over the age rule.
	byVolume, err := nthOldest(ctx, tx, namespace, held-int64(window.Cap))
	if err != nil {
		return err
	}
	byFloor, err := nthOldest(ctx, tx, namespace, held-int64(window.Floor))
	if err != nil {
		return err
	}
	byAge, err := lastBefore(ctx, tx, namespace, time.Now().Add(-window.Age))
	if err != nil {
		return err
	}
	byAge = min(byAge, byFloor)

	cut := max(byVolume, byAge)
	if cut == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM changes WHERE namespace = ? AND position <= ?`, namespace, cut); err != nil {
		return err
	}
	// The cut itself is recorded, because it is the only thing that can decide whether a
	// returning replica has missed anything: it has missed nothing exactly when it has already
	// seen everything at or below this. Working that out from the oldest surviving entry
	// instead would be working it out from a distance that the other namespaces in this
	// database set, which is a fact about them rather than about this replica.
	//
	// What makes it usable is that it is a position this namespace actually held and no longer
	// does, rather than a bound the DELETE was phrased with. Both candidates above are read out
	// of the table — nthOldest returns the nth entry's own position, and lastBefore returns the
	// newest position among the entries older than an instant rather than the instant — so the
	// larger of them is a row this statement is about to remove. That is what lets a replica
	// below it be told, truthfully, that something it needed is gone.
	//
	// Alongside it, which dimension pushed the oldest entries out, for whoever is told they
	// fell out of the window: age says that caller was away too long, volume says the
	// namespace changes faster than the log was configured to hold. When both would have cut
	// to the same place, volume did it — the entries were over the cap whether or not anybody
	// had been away.
	_, err = tx.ExecContext(ctx,
		`UPDATE logs SET trimmed_through = ?, trimmed_by_age = ? WHERE namespace = ?`,
		int64(cut), byAge > byVolume, namespace)
	return err
}

// oldestEntry reports the oldest position still held and the instant it was recorded, or zero
// for a log holding nothing.
func oldestEntry(ctx context.Context, tx *sql.Tx, namespace int64) (metastore.Position, time.Time, error) {
	var position, sec, nsec int64
	switch err := tx.QueryRowContext(ctx,
		`SELECT position, recorded_sec, recorded_nsec FROM changes
		 WHERE namespace = ? ORDER BY position LIMIT 1`, namespace).Scan(&position, &sec, &nsec); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, time.Time{}, nil
	case err != nil:
		return 0, time.Time{}, err
	}
	return metastore.Position(position), loadedTime(sec, int32(nsec)), nil
}

// newestEntry reports the newest position still held, or zero for a log holding nothing. It
// is not the log's tail: the tail is the newest position ever recorded, which outlives the
// entry at it.
func newestEntry(ctx context.Context, tx *sql.Tx, namespace int64) (metastore.Position, error) {
	var newest int64
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(position), 0) FROM changes WHERE namespace = ?`, namespace).Scan(&newest)
	return metastore.Position(newest), err
}

// nthOldest returns the position of the nth oldest entry, which is the cut that discards
// exactly n of them. A count of zero or below discards nothing.
func nthOldest(ctx context.Context, tx *sql.Tx, namespace, n int64) (metastore.Position, error) {
	if n <= 0 {
		return 0, nil
	}
	var position int64
	err := tx.QueryRowContext(ctx,
		`SELECT position FROM changes WHERE namespace = ? ORDER BY position LIMIT 1 OFFSET ?`,
		namespace, n-1).Scan(&position)
	if err != nil {
		return 0, err
	}
	return metastore.Position(position), nil
}

// lastBefore returns the newest position recorded before an instant, which is the cut that
// discards everything older than it.
//
// It is the newest match rather than a scan for a boundary, which is the same answer as long
// as a namespace's entries are recorded in the order their positions were allocated. A wall
// clock that stepped backwards would break that ordering and make this cut slightly deeper
// than the age asked for; the floor is what bounds how much that can cost.
func lastBefore(ctx context.Context, tx *sql.Tx, namespace int64, cutoff time.Time) (metastore.Position, error) {
	sec, nsec := storedTime(cutoff)
	var position int64
	err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(position), 0) FROM changes
		WHERE namespace = ?
		  AND (recorded_sec < ? OR (recorded_sec = ? AND recorded_nsec < ?))`,
		namespace, sec, sec, nsec).Scan(&position)
	return metastore.Position(position), err
}

// newIncarnation mints a name for a run of history.
//
// Random rather than a counter. A counter would go backwards when this database is restored
// from a backup, and two logs that have nothing to do with each other both sitting at 1 is
// entirely possible; either way a replica would be told it may resume against history it has
// never seen. A random value leaves one branch — it matches, or everything is rebuilt.
func newIncarnation() (metastore.Incarnation, error) {
	value, err := randomHex(16)
	if err != nil {
		return "", fmt.Errorf("%w: minting an incarnation: %w", syscall.EIO, err)
	}
	return metastore.Incarnation(value), nil
}

// --- reading ---------------------------------------------------------------------------

// changeMetadataColumns carries every scalar beside its SQLite storage class and only the
// lengths of variable payloads. Since validates those facts before asking SQLite to copy a
// BLOB or TEXT value into Go memory.
const changeMetadataColumns = `
	position, typeof(position), namespace, typeof(namespace), kind, typeof(kind),
	parent, typeof(parent), COALESCE(length(CAST(name AS BLOB)), 0), typeof(name),
	from_parent, typeof(from_parent), COALESCE(length(CAST(from_name AS BLOB)), 0), typeof(from_name),
	node, typeof(node), mode, typeof(mode), size, typeof(size),
	atime_sec, typeof(atime_sec), atime_nsec, typeof(atime_nsec),
	mtime_sec, typeof(mtime_sec), mtime_nsec, typeof(mtime_nsec),
	COALESCE(length(CAST(content AS BLOB)), 0), typeof(content),
	recorded_sec, typeof(recorded_sec), recorded_nsec, typeof(recorded_nsec)`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanChangeMetadata(row rowScanner, expectedNamespace int64) (metastore.Change, metastore.ChangePayloadLengths, error) {
	var (
		positionRaw, namespaceRaw, kindRaw, parentRaw            any
		fromParentRaw, idRaw, modeRaw, sizeRaw                   any
		atimeSecRaw, atimeNsecRaw, mtimeSecRaw, mtimeNsecRaw     any
		recordedSecRaw, recordedNsecRaw                          any
		positionType, namespaceType, kindType, parentType        string
		nameType, fromParentType, fromNameType                   string
		idType, modeType, sizeType                               string
		atimeSecType, atimeNsecType, mtimeSecType, mtimeNsecType string
		contentType, recordedSecType, recordedNsecType           string
		lengths                                                  metastore.ChangePayloadLengths
	)
	if err := row.Scan(
		&positionRaw, &positionType, &namespaceRaw, &namespaceType, &kindRaw, &kindType,
		&parentRaw, &parentType, &lengths.Name, &nameType,
		&fromParentRaw, &fromParentType, &lengths.FromName, &fromNameType,
		&idRaw, &idType, &modeRaw, &modeType, &sizeRaw, &sizeType,
		&atimeSecRaw, &atimeSecType, &atimeNsecRaw, &atimeNsecType,
		&mtimeSecRaw, &mtimeSecType, &mtimeNsecRaw, &mtimeNsecType,
		&lengths.Content, &contentType,
		&recordedSecRaw, &recordedSecType, &recordedNsecRaw, &recordedNsecType,
	); err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, err
	}
	position, err := requiredStoredInteger("position", positionRaw, positionType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, err
	}
	namespace, err := requiredStoredInteger("namespace", namespaceRaw, namespaceType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, err
	}
	kindValue, err := requiredStoredInteger("kind", kindRaw, kindType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, err
	}
	parent, err := requiredStoredInteger("parent", parentRaw, parentType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, err
	}
	fromParent, ok := nullableStoredInteger(fromParentRaw, fromParentType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, invalidStoredChangeScalar(position, "from_parent", fromParentType)
	}
	id, ok := nullableStoredInteger(idRaw, idType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, invalidStoredChangeScalar(position, "node", idType)
	}
	mode, ok := nullableStoredInteger(modeRaw, modeType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, invalidStoredChangeScalar(position, "mode", modeType)
	}
	size, ok := nullableStoredInteger(sizeRaw, sizeType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, invalidStoredChangeScalar(position, "size", sizeType)
	}
	atimeSec, ok := nullableStoredInteger(atimeSecRaw, atimeSecType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, invalidStoredChangeScalar(position, "atime_sec", atimeSecType)
	}
	atimeNsec, ok := nullableStoredInteger(atimeNsecRaw, atimeNsecType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, invalidStoredChangeScalar(position, "atime_nsec", atimeNsecType)
	}
	mtimeSec, ok := nullableStoredInteger(mtimeSecRaw, mtimeSecType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, invalidStoredChangeScalar(position, "mtime_sec", mtimeSecType)
	}
	mtimeNsec, ok := nullableStoredInteger(mtimeNsecRaw, mtimeNsecType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, invalidStoredChangeScalar(position, "mtime_nsec", mtimeNsecType)
	}
	recordedSec, err := requiredStoredInteger("recorded_sec", recordedSecRaw, recordedSecType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, err
	}
	recordedNsec, err := requiredStoredInteger("recorded_nsec", recordedNsecRaw, recordedNsecType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, err
	}
	loaded, err := loadedKind(kindValue)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, err
	}
	change := metastore.Change{Position: metastore.Position(position), Kind: loaded, Parent: parent}
	if nameType == "blob" {
		change.Name = []byte{}
	}
	if fromParent.Valid {
		change.From = &metastore.Location{Parent: fromParent.Int64}
		if fromNameType == "blob" {
			change.From.Name = []byte{}
		}
	}
	if id.Valid {
		change.Node = &metastore.Node{
			ID:         id.Int64,
			Mode:       fs.FileMode(mode.Int64),
			Size:       size.Int64,
			AccessTime: loadedTime(atimeSec.Int64, int32(atimeNsec.Int64)),
			ModTime:    loadedTime(mtimeSec.Int64, int32(mtimeNsec.Int64)),
		}
	}
	if err := validateChangeMetadata(change, lengths, namespace, expectedNamespace,
		nameType, fromNameType, contentType, id, mode, size, atimeSec, atimeNsec,
		mtimeSec, mtimeNsec, recordedSec, recordedNsec); err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, err
	}
	return change, lengths, nil
}

func requiredStoredInteger(column string, value any, storageClass string) (int64, error) {
	integer, ok := storedInteger(value, storageClass)
	if !ok {
		return 0, invalidStoredChangeScalar(0, column, storageClass)
	}
	return integer, nil
}

func nullableStoredInteger(value any, storageClass string) (sql.NullInt64, bool) {
	if storageClass == "null" && value == nil {
		return sql.NullInt64{}, true
	}
	integer, ok := storedInteger(value, storageClass)
	return sql.NullInt64{Int64: integer, Valid: ok}, ok
}

func invalidStoredChangeScalar(position int64, column, storageClass string) error {
	if position > 0 {
		return fmt.Errorf("%w: change %d stores %s as %s", syscall.EIO, position, column, storageClass)
	}
	return fmt.Errorf("%w: a change stores %s as %s", syscall.EIO, column, storageClass)
}

func validateChangeMetadata(
	change metastore.Change,
	lengths metastore.ChangePayloadLengths,
	namespace, expectedNamespace int64,
	nameType, fromNameType, contentType string,
	id, mode, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec sql.NullInt64,
	recordedSec, recordedNsec int64,
) error {
	if change.Position <= 0 || namespace <= 0 || namespace != expectedNamespace || change.Parent < 0 ||
		recordedNsec < 0 || recordedNsec >= int64(time.Second) {
		return fmt.Errorf("%w: change position %d has invalid identity, parent, or recorded time", syscall.EIO, change.Position)
	}
	if nameType != "null" && nameType != "blob" {
		return fmt.Errorf("%w: change %d stores its name as %s", syscall.EIO, change.Position, nameType)
	}
	if fromNameType != "null" && fromNameType != "blob" {
		return fmt.Errorf("%w: change %d stores its source name as %s", syscall.EIO, change.Position, fromNameType)
	}
	if contentType != "null" && contentType != "text" {
		return fmt.Errorf("%w: change %d stores its content key as %s", syscall.EIO, change.Position, contentType)
	}
	if lengths.Name < 0 || lengths.FromName < 0 || lengths.Content < 0 {
		return fmt.Errorf("%w: change %d has a negative payload length", syscall.EIO, change.Position)
	}
	wantFrom := change.Kind == metastore.Renamed
	if wantFrom != (change.From != nil) {
		return fmt.Errorf("%w: change %d has an incomplete source location", syscall.EIO, change.Position)
	}
	wantNode := change.Kind != metastore.Removed
	nodeFields := []sql.NullInt64{id, mode, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec}
	for _, field := range nodeFields {
		if field.Valid != wantNode {
			return fmt.Errorf("%w: change %d has an incomplete node", syscall.EIO, change.Position)
		}
	}
	if !wantNode {
		if contentType != "null" {
			return fmt.Errorf("%w: removed change %d carries content", syscall.EIO, change.Position)
		}
	} else {
		if contentType == "text" && lengths.Content == 0 {
			return fmt.Errorf("%w: change %d carries an empty content key", syscall.EIO, change.Position)
		}
		if id.Int64 <= 0 || size.Int64 < 0 || mode.Int64 < 0 || mode.Int64 > math.MaxUint32 ||
			atimeNsec.Int64 < 0 || atimeNsec.Int64 >= int64(time.Second) ||
			mtimeNsec.Int64 < 0 || mtimeNsec.Int64 >= int64(time.Second) {
			return fmt.Errorf("%w: change %d carries invalid node metadata", syscall.EIO, change.Position)
		}
		if nodeType := fs.FileMode(mode.Int64).Type(); nodeType != 0 && nodeType != fs.ModeDir {
			return fmt.Errorf("%w: change %d carries unsupported node type %v", syscall.EIO, change.Position, fs.FileMode(mode.Int64).Type())
		}
		if fs.FileMode(mode.Int64).IsDir() && (size.Int64 != 0 || contentType != "null") {
			return fmt.Errorf("%w: change %d carries bytes for a directory", syscall.EIO, change.Position)
		}
		if fs.FileMode(mode.Int64).Type() == 0 && contentType == "null" && size.Int64 != 0 {
			return fmt.Errorf("%w: change %d carries file bytes without a content key", syscall.EIO, change.Position)
		}
	}
	switch change.Kind {
	case metastore.Created, metastore.Removed:
		if change.Parent <= 0 || nameType != "blob" || lengths.Name <= 0 {
			return fmt.Errorf("%w: change %d has no addressable destination", syscall.EIO, change.Position)
		}
	case metastore.Modified:
		root := change.Parent == 0 && nameType == "null" && lengths.Name == 0
		named := change.Parent > 0 && nameType == "blob" && lengths.Name > 0
		if !root && !named {
			return fmt.Errorf("%w: modified change %d has no valid location", syscall.EIO, change.Position)
		}
	case metastore.Renamed:
		if change.Parent <= 0 || nameType != "blob" || lengths.Name <= 0 ||
			change.From == nil || change.From.Parent <= 0 || fromNameType != "blob" || lengths.FromName <= 0 {
			return fmt.Errorf("%w: renamed change %d has an incomplete location", syscall.EIO, change.Position)
		}
	}
	return nil
}

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
		if retention, err = s.retention(ctx, tx); err != nil {
			return err
		}
		cursor := after
		for produced := 0; produced < limit; produced++ {
			change, lengths, err := scanChangeMetadata(tx.QueryRowContext(ctx, `
				SELECT `+changeMetadataColumns+` FROM changes
				WHERE namespace = ? AND position > ?
				ORDER BY position
				LIMIT 1`, s.namespace, int64(cursor)), s.namespace)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			reservation, fits, err := result.Reserve(change, lengths)
			if err != nil {
				return err
			}
			if !fits {
				return nil
			}
			var name, fromName []byte
			var content sql.NullString
			if err := tx.QueryRowContext(ctx, `
				SELECT name, from_name, content FROM changes
				WHERE namespace = ? AND position = ?`, s.namespace, int64(change.Position)).Scan(
				&name, &fromName, &content,
			); err != nil {
				return err
			}
			if err := validateChangePayload(change, name, fromName, content); err != nil {
				return err
			}
			if err := reservation.Commit(name, fromName, metastore.Key(content.String)); err != nil {
				return err
			}
			cursor = change.Position
		}
		return nil
	}); err != nil {
		return metastore.Retention{}, result.Fail(fmt.Errorf("reading the changes after position %d: %w", after, failure(err)))
	}
	return retention, nil
}

func validateChangePayload(change metastore.Change, name, fromName []byte, content sql.NullString) error {
	if change.Name != nil && !validStoredComponent(name) {
		return fmt.Errorf("%w: change %d carries an invalid destination name", syscall.EIO, change.Position)
	}
	if change.From != nil && !validStoredComponent(fromName) {
		return fmt.Errorf("%w: change %d carries an invalid source name", syscall.EIO, change.Position)
	}
	if content.Valid && content.String == "" {
		return fmt.Errorf("%w: change %d carries an empty content key", syscall.EIO, change.Position)
	}
	return nil
}

func validStoredComponent(name []byte) bool {
	return len(name) != 0 && !bytes.Equal(name, []byte(".")) && !bytes.Equal(name, []byte("..")) &&
		bytes.IndexByte(name, '/') < 0 && bytes.IndexByte(name, 0) < 0
}

// retention reports what the log holds, inside the caller's transaction.
//
// Tail comes from the committed position rather than from the newest entry, and that is the
// distinction the whole structure turns on: a log that has discarded everything can otherwise
// not tell "you are caught up" from "you missed everything", and those two answers differ by
// a full rebuild of the tree.
func (s *Store) retention(ctx context.Context, tx *sql.Tx) (metastore.Retention, error) {
	var (
		tail           int64
		trimmedThrough int64
		trimmedByAge   bool
	)
	if err := tx.QueryRowContext(ctx,
		`SELECT committed_position, trimmed_through, trimmed_by_age FROM logs WHERE namespace = ?`,
		s.namespace).Scan(&tail, &trimmedThrough, &trimmedByAge); err != nil {
		return metastore.Retention{}, err
	}
	oldest, _, err := oldestEntry(ctx, tx, s.namespace)
	if err != nil {
		return metastore.Retention{}, err
	}
	return metastore.Retention{
		Oldest:         oldest,
		Tail:           metastore.Position(tail),
		TrimmedThrough: metastore.Position(trimmedThrough),
		TrimmedByAge:   trimmedByAge,
	}, nil
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
		var length int64
		if err := tx.QueryRowContext(ctx,
			`SELECT length(CAST(incarnation AS BLOB)) FROM logs WHERE namespace = ?`, s.namespace,
		).Scan(&length); err != nil {
			return err
		}
		if length > maxBytes {
			return fmt.Errorf("the log incarnation has %d bytes under a %d-byte bound: %w", length, maxBytes, syscall.EFBIG)
		}
		var incarnation string
		var position int64
		if err := tx.QueryRowContext(ctx,
			`SELECT incarnation, committed_position FROM logs WHERE namespace = ?`, s.namespace,
		).Scan(&incarnation, &position); err != nil {
			return err
		}
		if incarnation == "" || position < 0 {
			return fmt.Errorf("the log carries invalid incarnation %q or position %d: %w", incarnation, position, syscall.EIO)
		}
		barrier = metastore.LogBarrier{Incarnation: metastore.Incarnation(incarnation), Position: metastore.Position(position)}
		return nil
	}); err != nil {
		return metastore.LogBarrier{}, fmt.Errorf("reading the log's barrier: %w", failure(err))
	}
	return barrier, nil
}

// CommittedPosition is the newest position the tree was changed at.
//
// Here that is also the log's tail, because the two are written in one transaction. It is a
// separate question in the contract for the implementations where they can drift apart.
func (s *Store) CommittedPosition(ctx context.Context) (metastore.Position, error) {
	var committed int64
	if err := s.read.QueryRowContext(ctx,
		`SELECT committed_position FROM logs WHERE namespace = ?`, s.namespace).Scan(&committed); err != nil {
		return 0, fmt.Errorf("reading the position the tree was last changed at: %w", failure(err))
	}
	return metastore.Position(committed), nil
}
