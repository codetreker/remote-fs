package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// replicaNamespace is the name a copy's namespace carries in its own database. One file
// holds one copy, so the name distinguishes nothing and is fixed rather than asked for.
const replicaNamespace = "replica"

// Replica is a local copy of another namespace's tree: filled from one consistent picture of
// it, and kept current by applying the changes recorded after that picture was taken.
//
// It offers the reading half of metastore.Store and nothing that changes a tree of its own
// accord. That is the point of the type. Every difference between this copy and the
// namespace it copies has to arrive as a change somebody else recorded, so an operation here
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

	// mu keeps a reader out of a tree that is being rebuilt. A reader that arrived between
	// the wipe and the rows would be told the namespace is empty, which is the fabricated
	// answer R-ERR-2 forbids above all others, so a rebuild holds this for its whole length —
	// the network's length — and an applied change holds it for one transaction.
	mu sync.RWMutex

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
// namespace anybody is serving.
//
// The window and the allowance a Store is opened with mean nothing here: this copy records no
// changes of its own, so its log stays empty, and the room the namespace has is the server's
// answer rather than anything this database knows.
func OpenReplica(ctx context.Context, path string) (*Replica, error) {
	store, err := Open(ctx, path, replicaNamespace, 0, DefaultWindow())
	if err != nil {
		return nil, err
	}
	return &Replica{store: store}, nil
}

// Stat reports the node at path, as the namespace held it at Position.
func (r *Replica) Stat(ctx context.Context, path string) (metastore.Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.store.Stat(ctx, path)
}

// List returns the children of the directory at path, as the namespace held them at Position.
func (r *Replica) List(ctx context.Context, path string) ([]metastore.Child, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.store.List(ctx, path)
}

// Position is how far this copy has been brought: everything the source recorded up to and
// including it is here, and nothing later is.
func (r *Replica) Position() metastore.Position {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.at
}

// Close releases the database the copy is held in. What is on disk is not removed: where the
// file lives is the caller's decision, and so is when it stops existing.
func (r *Replica) Close() error { return r.store.Close() }

// Apply brings the copy forward by one change.
//
// A change at a position this copy already holds is discarded rather than applied again, and
// that one rule is what makes a replica's own echo free of charge: the change it caused
// arrives on the stream like any other, and either it is new to this copy or it is not.
//
// Nothing here tolerates a change that does not find what it describes. A rename whose source
// is missing, a modification of a node this copy does not hold and a removal of a name that
// is not there are each reported rather than skipped, because the copy has no way back from
// having quietly not applied something: there is no revalidation behind these changes and no
// timeout that repairs one. The consistent picture is what makes the strictness safe — every
// change after it acts on something that picture already contained.
func (r *Replica) Apply(ctx context.Context, change metastore.Change) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if change.Position <= r.at {
		return nil
	}
	tx, err := r.store.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("applying the change at position %d: %w", change.Position, failure(err))
	}
	defer tx.Rollback()

	if err := r.apply(ctx, tx, change); err != nil {
		return fmt.Errorf("applying the change at position %d: %w", change.Position, failure(err))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("applying the change at position %d: %w", change.Position, failure(err))
	}
	r.at = change.Position
	return nil
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
		if err := insertNode(ctx, tx, r.store.namespace, *change.Node); err != nil {
			return err
		}
		return insertEntry(ctx, tx, change.Parent, change.Name, change.Node.ID)

	case metastore.Modified:
		return updateNode(ctx, tx, *change.Node)

	case metastore.Removed:
		id, err := entryNode(ctx, tx, change.Parent, change.Name)
		if err != nil {
			return err
		}
		if err := removeEntry(ctx, tx, change.Parent, change.Name); err != nil {
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
		if err := removeEntry(ctx, tx, change.From.Parent, change.From.Name); err != nil {
			return err
		}
		if err := insertEntry(ctx, tx, change.Parent, change.Name, change.Node.ID); err != nil {
			return err
		}
		return updateNode(ctx, tx, *change.Node)
	}
	return fmt.Errorf("%w: the change is of kind %d, which this build has no meaning for",
		syscall.EIO, change.Kind)
}

// --- the rows a copy is made of ---------------------------------------------------------

// insertNode records a node under the id it arrived with. Its content key is dropped: see
// the type's own comment for why a copy holds no keys.
func insertNode(ctx context.Context, tx *sql.Tx, namespace int64, node metastore.Node) error {
	accessSec, accessNsec := storedTime(node.AccessTime)
	changeSec, changeNsec := storedTime(node.ModTime)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO nodes (id, namespace, mode, size, atime_sec, atime_nsec, mtime_sec, mtime_nsec, content)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		node.ID, namespace, int64(node.Mode), node.Size, accessSec, accessNsec, changeSec, changeNsec)
	return err
}

// updateNode replaces what a copy holds about a node it already has, and refuses to be a
// statement about a node it does not.
func updateNode(ctx context.Context, tx *sql.Tx, node metastore.Node) error {
	accessSec, accessNsec := storedTime(node.AccessTime)
	changeSec, changeNsec := storedTime(node.ModTime)
	result, err := tx.ExecContext(ctx, `
		UPDATE nodes SET mode = ?, size = ?, atime_sec = ?, atime_nsec = ?, mtime_sec = ?, mtime_nsec = ?
		WHERE id = ?`,
		int64(node.Mode), node.Size, accessSec, accessNsec, changeSec, changeNsec, node.ID)
	if err != nil {
		return err
	}
	return exactlyOne(result, fmt.Sprintf("node %d, which this copy does not hold", node.ID))
}

func insertEntry(ctx context.Context, tx *sql.Tx, parent int64, name []byte, node int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO entries (parent, name, node) VALUES (?, ?, ?)`,
		parent, name, node)
	if err != nil && isUniqueViolation(err) {
		return fmt.Errorf("%w: %q under node %d is already taken in this copy", syscall.EIO, name, parent)
	}
	return err
}

func removeEntry(ctx context.Context, tx *sql.Tx, parent int64, name []byte) error {
	result, err := tx.ExecContext(ctx, `DELETE FROM entries WHERE parent = ? AND name = ?`, parent, name)
	if err != nil {
		return err
	}
	return exactlyOne(result, fmt.Sprintf("%q under node %d, which this copy does not hold", name, parent))
}

func entryNode(ctx context.Context, tx *sql.Tx, parent int64, name []byte) (int64, error) {
	var id int64
	switch err := tx.QueryRowContext(ctx,
		`SELECT node FROM entries WHERE parent = ? AND name = ?`, parent, name).Scan(&id); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, fmt.Errorf("%w: the change names %q under node %d, which this copy does not hold",
			syscall.EIO, name, parent)
	case err != nil:
		return 0, err
	}
	return id, nil
}

// exactlyOne refuses a statement that did not act on the one row it describes.
//
// The count is the whole check. A copy is exactly as correct as the changes it applies, and
// a statement that matched nothing is the shape that failure takes here: no error from the
// database, no difference in the tree, and nothing afterwards that would notice.
func exactlyOne(result sql.Result, subject string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return fmt.Errorf("%w: the change acts on %s", syscall.EIO, subject)
	}
	return nil
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
	r.mu.Lock()
	tx, err := r.store.write.BeginTx(ctx, nil)
	if err != nil {
		r.mu.Unlock()
		return nil, fmt.Errorf("beginning to fill the copy: %w", failure(err))
	}
	seeding := &Seeding{replica: r, tx: tx}
	// Rows arrive in whatever order the picture yields them, and a child may therefore reach
	// this before its parent. Deferring the references to the commit is what lets that be the
	// picture's business rather than a rule the two sides have to agree on and keep agreeing on;
	// the references are still checked, in full, before this transaction is allowed to commit.
	if _, err := tx.ExecContext(ctx, `PRAGMA defer_foreign_keys = ON`); err != nil {
		seeding.Close()
		return nil, fmt.Errorf("beginning to fill the copy: %w", failure(err))
	}
	if err := seeding.empty(ctx); err != nil {
		seeding.Close()
		return nil, err
	}
	return seeding, nil
}

// Seeding is a copy being filled from one picture of its source.
type Seeding struct {
	replica *Replica
	tx      *sql.Tx

	// root is the id of the row that had no parent, and 0 until one has arrived. A tree with
	// no root is not a tree, so completing without one is refused.
	root int64

	// done marks the transaction as settled, so that closing after completing releases
	// nothing twice.
	done bool
}

// empty drops what the copy held. Entries first: a node another row still names cannot be
// deleted while it names it, and that reference is one this schema means.
func (s *Seeding) empty(ctx context.Context) error {
	for _, statement := range []string{
		`DELETE FROM entries WHERE node IN (SELECT id FROM nodes WHERE namespace = ?)`,
		`DELETE FROM nodes WHERE namespace = ?`,
	} {
		if _, err := s.tx.ExecContext(ctx, statement, s.replica.store.namespace); err != nil {
			return fmt.Errorf("emptying the copy: %w", failure(err))
		}
	}
	return nil
}

// Add records one page of the picture.
func (s *Seeding) Add(ctx context.Context, rows []metastore.Row) error {
	for _, row := range rows {
		if err := s.add(ctx, row); err != nil {
			return fmt.Errorf("filling the copy: %w", failure(err))
		}
	}
	return nil
}

func (s *Seeding) add(ctx context.Context, row metastore.Row) error {
	if err := insertNode(ctx, s.tx, s.replica.store.namespace, row.Node); err != nil {
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
	return insertEntry(ctx, s.tx, row.Parent, row.Name, row.Node.ID)
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
	if _, err := s.tx.ExecContext(ctx, `UPDATE namespaces SET root = ? WHERE id = ?`,
		s.root, s.replica.store.namespace); err != nil {
		return fmt.Errorf("completing the copy: %w", failure(err))
	}
	if err := s.tx.Commit(); err != nil {
		return fmt.Errorf("completing the copy: %w", failure(err))
	}
	// The root of the copy is the source's root, arrived with the picture. It is fixed for the
	// life of a namespace, so it is read once rather than joined for on every path resolution,
	// and this is the one moment at which it changes. Both are written before readers are let
	// back in, which is what the exclusion this holds is for.
	s.replica.store.root = s.root
	s.replica.at = at
	s.settle()
	return nil
}

// Close releases what the filling holds, and discards it if it was never completed. A caller
// that stops part way through calls it, and so does the one that completed.
func (s *Seeding) Close() error {
	if s.done {
		return nil
	}
	err := s.tx.Rollback()
	s.settle()
	if err != nil {
		return fmt.Errorf("discarding a picture that was not completed: %w", failure(err))
	}
	return nil
}

// settle marks the transaction finished and lets readers back in.
func (s *Seeding) settle() {
	s.done = true
	s.replica.mu.Unlock()
}
