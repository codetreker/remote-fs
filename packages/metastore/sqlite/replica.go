package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

// replicaVolume is the name a copy's volume carries in its own database. One file
// holds one copy, so the name distinguishes nothing and is fixed rather than asked for.
const replicaVolume = "replica"

// Replica is a local copy of another volume's tree: filled from one consistent picture of
// it, and kept current by applying the changes recorded after that picture was taken.
//
// It offers the reading half of metastore.Store and nothing that changes a tree of its own
// accord. That is the point of the type. Every difference between this copy and the
// volume it copies has to arrive as a change somebody else recorded, so an operation here
// that made a node would be a second author of this tree, and the copy would then hold
// something the log never said.
//
// The ids are the source's own. A change names the directory it landed in by node id, so a
// copy that allocated ids of its own would have to keep a translation beside the tree and get
// it right forever; copying them verbatim removes the question. It is why the rows below are
// inserted with the ids they arrive carrying.
//
// A file's content key is not kept. Reading a file goes to the server — this copy is of the
// metadata alone — so the key would name bytes no object store here has ever heard of, which
// is what the schema's reference from nodes.content to objects refuses. Everything a caller
// of Stat or List reads is kept; nothing that is kept is a key to somewhere else.
type Replica struct {
	store *Store

	// admission holds the tree and its position together across each applied change
	// and the complete lifetime of a reseed transaction.
	admission replicaGate

	// readSlots limits phase grants to the work the SQLite reader pool can execute.
	// Callers waiting for a slot must not extend the current reader phase.
	readSlots chan struct{}

	// Scan backlog waits outside SQL and phase admission so it cannot consume a
	// point read's deadline. Both classes still share the same total reader bound.
	listingSlots chan struct{}

	// at is how far this copy has been brought. It is held here rather than in the database
	// because nothing ever reads it back: the copy does not outlive the mount that made it,
	// and a position on disk would offer a resume this version does not do.
	at metastore.Position
}

// OpenReplica makes an empty copy in the SQLite database at path, ready to be filled.
//
// The database is the copy's own. Nothing else writes it, and it is expected to be a file
// that lives and dies with whatever is holding the copy — so it is created if it is not
// there, and what it held before is what a previous run of the same mount left, not a
// volume anybody is serving.
//
// The window and the allowance a Store is opened with mean nothing here: this copy records no
// changes of its own, so its log stays empty, and the room the volume has is the server's
// answer rather than anything this database knows.
func OpenReplica(ctx context.Context, path string) (*Replica, error) {
	options := DefaultOptions()
	store, err := OpenWithOptions(ctx, path, replicaVolume, 0, options)
	if err != nil {
		return nil, err
	}
	return &Replica{
		store: store, admission: newReplicaGate(),
		readSlots:    make(chan struct{}, options.MaxReaderConnections),
		listingSlots: make(chan struct{}, max(1, options.MaxReaderConnections-1)),
	}, nil
}

func (r *Replica) acquireRead(ctx context.Context) error {
	select {
	case r.readSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := r.admission.acquireRead(ctx); err != nil {
		<-r.readSlots
		return err
	}
	return nil
}

func (r *Replica) releaseRead() {
	r.admission.releaseRead()
	<-r.readSlots
}

func (r *Replica) acquireListing(ctx context.Context) error {
	select {
	case r.listingSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := r.acquireRead(ctx); err != nil {
		<-r.listingSlots
		return err
	}
	return nil
}

func (r *Replica) releaseListing() {
	r.releaseRead()
	<-r.listingSlots
}

// Stat reports the node at path, as the volume held it at Position.
func (r *Replica) Stat(ctx context.Context, path string) (metastore.Node, error) {
	if err := r.acquireRead(ctx); err != nil {
		return metastore.Node{}, pathError("stat", path, sqlerr.Failure(err))
	}
	defer r.releaseRead()
	return r.store.Stat(ctx, path)
}

// List returns the children of the directory at path, as the volume held them at Position.
func (r *Replica) List(ctx context.Context, path string) ([]metastore.Child, error) {
	if err := r.acquireListing(ctx); err != nil {
		return nil, pathError("list", path, sqlerr.Failure(err))
	}
	defer r.releaseListing()
	var children []metastore.Child
	err := r.inspectDirectory(ctx, path, func(tx *sql.Tx, parent int64) error {
		var err error
		children, err = r.store.listChildren(ctx, tx, parent)
		return err
	})
	if err != nil {
		return nil, err
	}
	return children, nil
}

// ListBounded holds the replica read lock while one ordered database observation is
// enumerated, so applying a concurrent change cannot splice two replica positions into a
// successful listing.
func (r *Replica) ListBounded(ctx context.Context, path string, result *storage.ListResult) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	if err := r.acquireListing(ctx); err != nil {
		return pathError("list", path, sqlerr.Failure(err))
	}
	defer r.releaseListing()
	if result == nil {
		return pathError("list", path, syscall.EINVAL)
	}
	return r.inspectDirectory(ctx, path, func(tx *sql.Tx, parent int64) error {
		return r.store.listChildrenBounded(ctx, tx, parent, result)
	})
}

// Replica readers observe only copied facts. Their phase gate owns consistency;
// native access claims belong to the authority that serves the file contents.
func (r *Replica) inspectDirectory(ctx context.Context, path string, read func(*sql.Tx, int64) error) error {
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return pathError("list", path, err)
	}
	err = r.store.inspect(ctx, func(tx *sql.Tx) error {
		directory, err := r.store.resolve(ctx, tx, cleaned)
		if err != nil {
			return err
		}
		if !directory.IsDir() {
			return syscall.ENOTDIR
		}
		return read(tx, directory.ID)
	})
	if err != nil {
		return pathError("list", path, sqlerr.Failure(err))
	}
	return nil
}

// Position is how far this copy has been brought: everything the source recorded up to and
// including it is here, and nothing later is.
func (r *Replica) Position() metastore.Position {
	_ = r.admission.acquireRead(context.Background())
	defer r.admission.releaseRead()
	return r.at
}

// Close releases the database the copy is held in. What is on disk is not removed: where the
// file lives is the caller's decision, and so is when it stops existing.
func (r *Replica) Close() error { return r.store.Close() }

// Apply brings the copy forward by one change, and reports whether the change was new to it.
//
// A change at a position this copy already holds is discarded rather than applied again. This
// occurs when the stream attached before a snapshot later replays changes already covered by
// that snapshot.
//
// Which of the two it was is reported rather than left to be inferred, because the caller
// keeps its own account of what the copy holds. A caller that took every change it delivered
// as applied would move that account onto a position this copy never reached — backwards,
// where a picture had already carried it further — and would then answer questions about
// changes the copy discarded.
//
// Nothing here tolerates a change that does not find what it describes. A rename whose source
// is missing, a modification of a node this copy does not hold and a removal of a name that
// is not there are each reported rather than skipped, because the copy has no way back from
// having quietly not applied something: there is no revalidation behind these changes and no
// timeout that repairs one. The consistent picture is what makes the strictness safe — every
// change after it acts on something that picture already contained.
func (r *Replica) Apply(ctx context.Context, change metastore.Change) (applied bool, returnErr error) {
	if err := r.admission.acquireWrite(ctx); err != nil {
		return false, err
	}
	defer r.admission.releaseWrite()
	if err := r.store.coordinator.commit.acquire(ctx); err != nil {
		return false, err
	}
	defer r.store.coordinator.commit.release()
	if err := r.store.coordinator.healthy(); err != nil {
		return false, err
	}
	if change.Position <= r.at {
		return false, nil
	}
	tx, err := beginOwnedTransaction(ctx, r.store.write, nil)
	if err != nil {
		return false, fmt.Errorf("applying the change at position %d: %w", change.Position, sqlerr.Failure(err))
	}
	observationHeld := false
	defer func() { returnErr = r.store.finishMutationTransaction(tx, returnErr, observationHeld) }()

	if err := r.apply(ctx, tx.Tx, change); err != nil {
		return false, fmt.Errorf("applying the change at position %d: %w", change.Position, sqlerr.Failure(err))
	}
	if err := r.store.checkReplicaMetadataBudget(ctx, tx.Tx); err != nil {
		return false, err
	}
	state, err := dbstate.AdvanceGeneration(ctx, tx.Tx)
	if err != nil {
		return false, fmt.Errorf("applying the change at position %d: %w", change.Position, sqlerr.Failure(err))
	}
	r.store.coordinator.health.Lock()
	observationHeld = true
	if err := r.store.coordinator.healthErrorLocked(); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		r.store.coordinator.poisonLocked(sqlerr.NewUncertainCommit(sqlerr.Failure(err)))
		return false, fmt.Errorf("applying the change at position %d: %w", change.Position, r.store.coordinator.healthErrorLocked())
	}
	if err := r.store.acceptLocked(DurableState(state)); err != nil {
		return false, fmt.Errorf("applying the change at position %d: %w", change.Position, err)
	}
	r.at = change.Position
	return true, nil
}

func (r *Replica) apply(ctx context.Context, tx *sql.Tx, change metastore.Change) error {
	// A change that does not describe an outcome is refused here as well as where it was
	// decoded. What arrives is not always something a transport checked — a log read directly
	// reaches this too — and the alternative to a refusal is a nil dereference that takes the
	// mount down, or worse, a zero node recorded as a regular file of no length.
	if wantNode := change.Kind != metastore.Removed; wantNode && change.Node == nil {
		return fmt.Errorf("%w: the change carries no node, and its kind says what the name holds afterwards", syscall.EIO)
	}
	if change.Kind == metastore.Renamed && change.From == nil {
		return fmt.Errorf("%w: the rename does not say where the node came from", syscall.EIO)
	}

	switch change.Kind {
	case metastore.Created:
		if err := dbstate.ObserveNewNodeID(ctx, tx, change.Node.ID); err != nil {
			return err
		}
		if err := r.store.insertReplicaNode(ctx, tx, *change.Node); err != nil {
			return err
		}
		return insertEntry(ctx, tx, r.store.volume, change.Parent, change.Name, change.Node.ID)

	case metastore.Modified:
		return r.store.updateReplicaNode(ctx, tx, *change.Node)

	case metastore.Removed:
		id, err := entryNode(ctx, tx, r.store.volume, change.Parent, change.Name)
		if err != nil {
			return err
		}
		if err := removeEntry(ctx, tx, r.store.volume, change.Parent, change.Name); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM nodes WHERE id = ?`, id)
		return err

	case metastore.Renamed:
		// The entry is moved rather than rewritten in place, so that a directory arriving here
		// carries everything beneath it: the nodes under it keep the parent they always had and
		// none of their rows is touched. That is the property that made carrying the node in the
		// change worth its cost, and it is the one a replica would give up if it discarded the
		// subtree and asked for it again.
		if err := removeEntry(ctx, tx, r.store.volume, change.From.Parent, change.From.Name); err != nil {
			return err
		}
		if err := insertEntry(ctx, tx, r.store.volume, change.Parent, change.Name, change.Node.ID); err != nil {
			return err
		}
		return r.store.updateReplicaNode(ctx, tx, *change.Node)
	}
	return fmt.Errorf("%w: the change is of kind %d, which this build has no meaning for",
		syscall.EIO, change.Kind)
}

// --- the rows a copy is made of ---------------------------------------------------------

// insertReplicaNode records a node under the id it arrived with. Its content key is dropped: see
// the type's own comment for why a copy holds no keys.
func (s *Store) insertReplicaNode(ctx context.Context, tx *sql.Tx, node metastore.Node) error {
	values, err := replicaNodeValues(node)
	if err != nil {
		return err
	}
	if err := s.admitReplicaMetadata(ctx, tx, 0, values.bytes); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
  INSERT INTO nodes (id,volume,kind,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,
                     birth_sec,birth_nsec,change_sec,change_nsec,metadata,link_target,directory_revision,content)
  VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)`, append([]any{node.ID, s.volume}, values.fields...)...)
	return err
}

// updateReplicaNode replaces source facts without introducing a replica content key.
func (s *Store) updateReplicaNode(ctx context.Context, tx *sql.Tx, node metastore.Node) error {
	values, err := replicaNodeValues(node)
	if err != nil {
		return err
	}
	if err := s.admitReplicaMetadata(ctx, tx, node.ID, values.bytes); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
  UPDATE nodes SET kind=?,size=?,atime_sec=?,atime_nsec=?,mtime_sec=?,mtime_nsec=?,
                   birth_sec=?,birth_nsec=?,change_sec=?,change_nsec=?,metadata=?,link_target=?,directory_revision=?
  WHERE id=?`, append(values.fields, node.ID)...)
	if err != nil {
		return err
	}
	return sqlvalue.ExactlyOne(result, fmt.Sprintf("node %d, which this copy does not hold", node.ID))
}

type replicaNodePayload struct {
	fields []any
	bytes  int64
}

func replicaNodeValues(node metastore.Node) (replicaNodePayload, error) {
	if node.ID <= 0 || node.Kind.Check() != nil || node.Size < 0 || len(node.DirectoryRevision) > storage.MaxObservationTokenBytes || len(node.LinkTarget) > storage.MaxLinkTargetBytes {
		return replicaNodePayload{}, syscall.EIO
	}
	if node.Kind == storage.NodeDirectory && node.Size != 0 || node.Kind != storage.NodeDirectory && len(node.DirectoryRevision) != 0 {
		return replicaNodePayload{}, syscall.EIO
	}
	if node.Kind == storage.NodeSymlink {
		if len(node.LinkTarget) == 0 || int64(len(node.LinkTarget)) != node.Size {
			return replicaNodePayload{}, syscall.EIO
		}
	} else if len(node.LinkTarget) != 0 {
		return replicaNodePayload{}, syscall.EIO
	}
	metadata, err := storage.EncodeMetadata(node.Metadata)
	if err != nil {
		return replicaNodePayload{}, err
	}
	atimeSec, atimeNsec := sqlvalue.StoredTime(node.AccessTime)
	mtimeSec, mtimeNsec := sqlvalue.StoredTime(node.ModTime)
	var birthSec, birthNsec, changeSec, changeNsec any
	if node.BirthTime != nil {
		birthSec, birthNsec = sqlvalue.StoredTime(*node.BirthTime)
	}
	if node.ChangeTime != nil {
		changeSec, changeNsec = sqlvalue.StoredTime(*node.ChangeTime)
	}
	return replicaNodePayload{fields: []any{int64(node.Kind), node.Size, atimeSec, atimeNsec, mtimeSec, mtimeNsec, birthSec, birthNsec, changeSec, changeNsec, metadata, append([]byte{}, node.LinkTarget...), append([]byte{}, node.DirectoryRevision...)}, bytes: int64(len(metadata) + len(node.LinkTarget))}, nil
}

func insertEntry(ctx context.Context, tx *sql.Tx, volume, parent int64, name []byte, node int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entries (volume, parent, name, node) VALUES (?, ?, ?, ?)`,
		volume, parent, name, node)
	if err != nil && sqlerr.IsUniqueViolation(err) {
		return fmt.Errorf("%w: %q under node %d is already taken in this copy", syscall.EIO, name, parent)
	}
	return err
}

func removeEntry(ctx context.Context, tx *sql.Tx, volume, parent int64, name []byte) error {
	result, err := tx.ExecContext(ctx,
		`DELETE FROM entries WHERE volume = ? AND parent = ? AND name = ?`, volume, parent, name)
	if err != nil {
		return err
	}
	return sqlvalue.ExactlyOne(result, fmt.Sprintf("%q under node %d, which this copy does not hold", name, parent))
}

func entryNode(ctx context.Context, tx *sql.Tx, volume, parent int64, name []byte) (int64, error) {
	var id int64
	switch err := tx.QueryRowContext(ctx,
		`SELECT node FROM entries WHERE volume = ? AND parent = ? AND name = ?`,
		volume, parent, name).Scan(&id); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, fmt.Errorf("%w: the change names %q under node %d, which this copy does not hold",
			syscall.EIO, name, parent)
	case err != nil:
		return 0, err
	}
	return id, nil
}

// --- filling a copy from a picture -------------------------------------------------------

// Reseed empties the copy and begins filling it from a fresh picture of the source.
//
// Nothing may read the copy between the two, so this holds the copy exclusively until the
// Seeding is completed or closed — which is to say for as long as the picture takes to cross
// the network. That is deliberate: a reader let in halfway would be told that names which
// exist are not there, and nothing downstream can tell that answer from the truth.
//
// The wipe and the rows are one transaction, so a picture that fails to arrive leaves the
// copy as it was rather than empty. What the caller does with a copy that is still the old
// one is its own decision — it is a copy of a log that said to start over, so the answer is
// to try again rather than to serve it.
func (r *Replica) Reseed(ctx context.Context) (*Seeding, error) {
	if err := r.admission.acquireWrite(ctx); err != nil {
		return nil, err
	}
	if err := r.store.coordinator.commit.acquire(ctx); err != nil {
		r.admission.releaseWrite()
		return nil, err
	}
	if err := r.store.coordinator.healthy(); err != nil {
		r.store.coordinator.commit.release()
		r.admission.releaseWrite()
		return nil, err
	}
	tx, err := beginOwnedTransaction(ctx, r.store.write, nil)
	if err != nil {
		r.store.coordinator.commit.release()
		r.admission.releaseWrite()
		return nil, fmt.Errorf("beginning to fill the copy: %w", sqlerr.Failure(err))
	}
	state, err := dbstate.Validate(ctx, tx)
	if err != nil {
		err = r.store.finishMutationTransaction(tx, err, false)
		r.store.coordinator.commit.release()
		r.admission.releaseWrite()
		return nil, fmt.Errorf("validating the copy's identity allocator: %w", sqlerr.Failure(err))
	}
	seeding := &Seeding{replica: r, tx: tx, nodeHighWater: state.NodeHighWater}
	// Rows arrive in whatever order the picture yields them, and a child may therefore reach
	// this before its parent. Deferring the references to the commit is what lets that be the
	// picture's business rather than a rule the two sides have to agree on and keep agreeing on;
	// the references are still checked, in full, before this transaction is allowed to commit.
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		return nil, seeding.finish(fmt.Errorf("beginning to fill the copy: %w", sqlerr.Failure(err)), false)
	}
	if err := seeding.empty(ctx); err != nil {
		return nil, seeding.finish(err, false)
	}
	return seeding, nil
}

// Seeding is a copy being filled from one picture of its source.
type Seeding struct {
	replica *Replica
	tx      *ownedTransaction

	// root is the id of the row that had no parent, and 0 until one has arrived. A tree with
	// no root is not a tree, so completing without one is refused.
	root int64

	// done marks the transaction as settled, so that closing after completing releases
	// nothing twice.
	done bool

	// nodeHighWater is validated once when the reseed begins. maxNodeID accumulates the
	// unordered source identities so Complete can move both allocator witnesses once.
	nodeHighWater int64
	maxNodeID     int64
}

// empty drops what the copy held. Entries first: a node another row still names cannot be
// deleted while it names it, and that reference is one this schema means.
func (s *Seeding) empty(ctx context.Context) error {
	for _, statement := range []string{
		`DELETE FROM entries WHERE volume = ?`,
		`DELETE FROM nodes WHERE volume = ?`,
	} {
		if _, err := s.tx.ExecContext(ctx, statement, s.replica.store.volume); err != nil {
			return fmt.Errorf("emptying the copy: %w", sqlerr.Failure(err))
		}
	}
	return nil
}

// Add records one page of the picture.
func (s *Seeding) Add(ctx context.Context, rows []metastore.Row) error {
	for _, row := range rows {
		if err := s.add(ctx, row); err != nil {
			return fmt.Errorf("filling the copy: %w", sqlerr.Failure(err))
		}
	}
	return nil
}

func (s *Seeding) add(ctx context.Context, row metastore.Row) error {
	if row.Node.ID <= 0 {
		return fmt.Errorf("node identity %d is not positive: %w", row.Node.ID, syscall.EIO)
	}
	s.maxNodeID = max(s.maxNodeID, row.Node.ID)
	if err := s.replica.store.insertReplicaNode(ctx, s.tx.Tx, row.Node); err != nil {
		return err
	}
	// Parent 0 and no name is how a picture names the one node that has neither. Nothing else
	// can carry parent 0, since every other row names a node, and ids begin at one.
	if row.Parent == 0 && row.Name == nil {
		if s.root != 0 {
			return fmt.Errorf("%w: the picture carries two nodes with no parent, %d and %d", syscall.EIO, s.root, row.Node.ID)
		}
		s.root = row.Node.ID
		return nil
	}
	return insertEntry(ctx, s.tx.Tx, s.replica.store.volume, row.Parent, row.Name, row.Node.ID)
}

// Complete records that the picture was whole and that the copy stands at the position it was
// taken at. Every node in the copy is exactly as the source held it at that position, so the
// copy has one applied position rather than one per node — which is what makes replaying the
// changes that arrived meanwhile a comparison against a single number.
func (s *Seeding) Complete(ctx context.Context, at metastore.Position) error {
	if s.done {
		return fmt.Errorf("this picture has already been settled: %w", syscall.EINVAL)
	}
	if s.root == 0 {
		return fmt.Errorf("%w: the picture carried no node without a parent, so it is not a tree", syscall.EIO)
	}
	if err := s.replica.store.checkReplicaMetadataBudget(ctx, s.tx.Tx); err != nil {
		return err
	}
	if _, err := s.tx.ExecContext(ctx, `UPDATE volumes SET root = ? WHERE id = ?`,
		s.root, s.replica.store.volume); err != nil {
		return fmt.Errorf("completing the copy: %w", sqlerr.Failure(err))
	}
	highWater := max(s.nodeHighWater, s.maxNodeID)
	if _, err := s.tx.ExecContext(ctx,
		`UPDATE database_state SET node_high_water = ? WHERE singleton = 1`, highWater,
	); err != nil {
		return fmt.Errorf("completing the copy: %w", sqlerr.Failure(err))
	}
	sequence, err := dbstate.SequenceValue(ctx, s.tx, "nodes")
	if err != nil {
		return fmt.Errorf("completing the copy: %w", sqlerr.Failure(err))
	}
	if sequence != highWater {
		return fmt.Errorf("completing the copy: SQLite node sequence %d does not match observed high-water %d: %w",
			sequence, highWater, syscall.EIO)
	}
	state, err := dbstate.AdvanceGeneration(ctx, s.tx.Tx)
	if err != nil {
		return fmt.Errorf("completing the copy: %w", sqlerr.Failure(err))
	}
	s.replica.store.coordinator.health.Lock()
	if err := s.replica.store.coordinator.healthErrorLocked(); err != nil {
		s.replica.store.coordinator.health.Unlock()
		return fmt.Errorf("completing the copy: %w", err)
	}
	if err := s.tx.Commit(); err != nil {
		s.replica.store.coordinator.poisonLocked(sqlerr.NewUncertainCommit(sqlerr.Failure(err)))
		healthErr := s.replica.store.coordinator.healthErrorLocked()
		return s.finish(fmt.Errorf("completing the copy: %w", healthErr), true)
	}
	if err := s.replica.store.acceptLocked(DurableState(state)); err != nil {
		return s.finish(fmt.Errorf("completing the copy: %w", err), true)
	}
	// The root of the copy is the source's root, arrived with the picture. It is fixed for the
	// life of a volume, so it is read once rather than joined for on every path resolution,
	// and this is the one moment at which it changes. Both are written before readers are let
	// back in, which is what the exclusion this holds is for.
	s.replica.store.root = s.root
	s.replica.at = at
	return s.finish(nil, true)
}

// Close releases what the filling holds, and discards it if it was never completed. A caller
// that stops part way through calls it, and so does the one that completed.
func (s *Seeding) Close() error {
	if s.done {
		return nil
	}
	s.replica.store.coordinator.health.Lock()
	rollbackErr := s.tx.Rollback()
	err := s.replica.store.mutationCleanupFailure(rollbackErr, nil)
	s.replica.store.coordinator.health.Unlock()
	s.settle()
	if err == nil && rollbackErr != nil {
		err = sqlerr.Failure(rollbackErr)
	}
	if err != nil {
		return fmt.Errorf("discarding a picture that was not completed: %w", sqlerr.Failure(err))
	}
	return nil
}

func (s *Seeding) finish(primary error, observationHeld bool) error {
	err := s.replica.store.finishMutationTransaction(s.tx, primary, observationHeld)
	s.settle()
	return err
}

// settle marks the transaction finished and lets readers back in.
func (s *Seeding) settle() {
	s.done = true
	s.replica.store.coordinator.commit.release()
	s.replica.admission.releaseWrite()
}

func (s *Store) admitReplicaMetadata(ctx context.Context, tx *sql.Tx, previous, nextBytes int64) error {
	used, err := s.metadataUsage(ctx, tx)
	if err != nil {
		return err
	}
	var oldBytes int64
	if previous != 0 {
		if err := tx.QueryRowContext(ctx, `SELECT length(metadata)+length(link_target) FROM nodes WHERE volume=? AND id=?`, s.volume, previous).Scan(&oldBytes); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("node %d is absent from the copy: %w", previous, syscall.EIO)
			}
			return err
		}
	}
	if oldBytes < 0 || oldBytes > used {
		return syscall.EIO
	}
	if nextBytes > s.maxMetadataBytes-(used-oldBytes) {
		return fmt.Errorf("replica metadata exceeds its %d-byte bound: %w", s.maxMetadataBytes, syscall.EFBIG)
	}
	return nil
}

func (s *Store) checkReplicaMetadataBudget(ctx context.Context, tx *sql.Tx) error {
	used, err := s.metadataUsage(ctx, tx)
	if err != nil {
		return err
	}
	if used > s.maxMetadataBytes {
		return fmt.Errorf("replica metadata exceeds its %d-byte bound: %w", s.maxMetadataBytes, syscall.EFBIG)
	}
	return nil
}
