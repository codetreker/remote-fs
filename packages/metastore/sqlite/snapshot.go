package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// Snapshot opens a consistent picture of the whole tree and reports the position it is taken
// at.
//
// The picture is a read transaction against the dedicated snapshot pool, and the first
// statement issued in it is the one that reads the position. That order is what makes the
// position and the rows one instant rather than two: SQLite takes its read snapshot when a
// deferred transaction first reads, so a position sampled before the transaction had touched
// anything could describe a moment the rows do not come from. Sampling it afterwards is the worse
// mistake and the easier one to write — a picture stamped with a position newer than itself
// makes a replica discard the very events that would have corrected the rows it scanned
// early, and nothing afterwards ever corrects them.
//
// A snapshot holds that read transaction for its whole life, which on SQLite pins the WAL:
// checkpointing cannot pass a live reader, so the write-ahead log grows for as long as the
// picture is open. The picture is streamed to somewhere far away, so its lifetime is decided
// by the network — a caller closes it as soon as it is done with it, and the worst moment is
// the one where every replica rebuilds at once.
func (s *Store) Snapshot(ctx context.Context) (metastore.Snap, metastore.Position, error) {
	tx, err := s.snapshotRead.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, fmt.Errorf("opening a picture of the tree: %w", failure(err))
	}
	var committed int64
	if err := tx.QueryRowContext(ctx,
		`SELECT committed_position FROM logs WHERE namespace = ?`, s.namespace).Scan(&committed); err != nil {
		primary := fmt.Errorf("opening a picture of the tree: %w", failure(err))
		return nil, 0, finishReadTransaction("snapshot transaction", tx, primary)
	}
	if err := validateNamespaceIntegrity(ctx, tx, s.namespace, s.maxIntegrityRecords); err != nil {
		primary := fmt.Errorf("validating the picture of the tree: %w", failure(err))
		return nil, 0, finishReadTransaction("snapshot transaction", tx, primary)
	}
	// The cursor starts before every entry there is. The name is an empty blob rather than
	// nothing at all, so that the comparison in page is over two values rather than over a NULL.
	return &snapshot{store: s, tx: tx, name: []byte{}}, metastore.Position(committed), nil
}

// snapshot is one consistent picture, delivered in pages.
//
// Paging is a cursor over (parent, name) rather than a *sql.Rows held open across calls. Both
// stay inside the one transaction, so both are consistent; the cursor is what lets each page
// carry its own deadline, since a Rows opened by an earlier call would answer to that call's
// context long after it returned.
type snapshot struct {
	store *Store

	// tx is the read transaction the picture is taken in, and nil once it is closed.
	tx *sql.Tx

	// The root has no name and no parent, so it is not an entry and is delivered on its own.
	sentRoot bool

	// parent and name are the last entry delivered, which is where the next page starts.
	parent int64
	name   []byte

	done   bool
	failed error
}

func (p *snapshot) Next(ctx context.Context, limit int, result *metastore.RowResult) (done bool, returnErr error) {
	if result == nil {
		return false, fmt.Errorf("reading a snapshot needs a bounded result: %w", syscall.EINVAL)
	}
	if p.failed != nil {
		return false, result.Fail(p.failed)
	}
	defer func() {
		if returnErr != nil {
			p.failed = returnErr
		}
	}()
	if p.tx == nil {
		return false, result.Fail(fmt.Errorf("this picture of the tree has been closed: %w", syscall.EINVAL))
	}
	if limit < 1 {
		return false, result.Fail(fmt.Errorf("a page of %d rows is not a page: %w", limit, syscall.EINVAL))
	}
	if p.done {
		return true, nil
	}

	produced := 0
	if !p.sentRoot {
		root, contentBytes, err := p.store.rootMetadata(ctx, p.tx)
		if err != nil {
			return false, result.Fail(fmt.Errorf("reading the root of the picture: %w", failure(err)))
		}
		reservation, fits, err := result.Reserve(
			metastore.Row{Node: root}, metastore.RowPayloadLengths{Content: contentBytes},
		)
		if err != nil {
			return false, err
		}
		if !fits {
			return false, result.Fail(fmt.Errorf("the root did not fit an empty snapshot page: %w", syscall.EIO))
		}
		content, err := p.store.nodeContent(ctx, p.tx, root.ID)
		if err != nil {
			return false, result.Fail(fmt.Errorf("reading the root content key: %w", failure(err)))
		}
		if err := reservation.Commit(nil, content); err != nil {
			return false, err
		}
		p.sentRoot = true
		produced++
		if produced == limit {
			return false, nil
		}
	}

	for produced < limit {
		row, lengths, found, err := p.nextMetadata(ctx)
		if err != nil {
			return false, result.Fail(fmt.Errorf("reading a page of the picture: %w", failure(err)))
		}
		if !found {
			p.done = true
			return true, nil
		}
		reservation, fits, err := result.Reserve(row, lengths)
		if err != nil {
			return false, err
		}
		if !fits {
			return false, nil
		}
		name, content, err := p.payload(ctx, row.Parent, row.Node.ID)
		if err != nil {
			return false, result.Fail(fmt.Errorf("reading a snapshot row payload: %w", failure(err)))
		}
		if err := reservation.Commit(name, content); err != nil {
			return false, err
		}
		p.parent, p.name = row.Parent, name
		produced++
	}
	return false, nil
}

// pageQuery reads the fixed-width metadata and payload lengths of the next entry after a
// cursor. The name and content key are deliberately absent: the result reserves their wire
// representation before a second query is allowed to load them.
//
// Every clause is chosen so that one plan is the only plan, and the plan is what is under
// test rather than the rows — what went wrong here before was never the rows this returns,
// only what it cost to return them.
//
// The namespace equality and the cursor range both address the entry table's primary key, so
// a page is a seek to the cursor and a read forward over this namespace's rows and no
// others. The ordering falls out of the same key, so no page sorts anything.
//
// The cursor is a row value rather than the `parent > ? OR (parent = ? AND name > ?)` that
// says the same thing, and what separates them is not syntax but what the planner can prove.
// Three placeholders are three unrelated values as far as it knows, so nothing ties the
// second to the first and nothing constrains parent at all. Write the same predicate with one
// parameter referenced twice and it can prove them equal, and constrains parent but not name.
// A row value constrains both. Measured against modernc.org/sqlite v1.57.0 over a database of
// eighty entries and one of four hundred thousand, with and without table statistics, the
// four spellings give three different plans:
//
//	(parent, name) > (?, ?)                      PRIMARY KEY (namespace=? AND (parent,name)>(?,?))
//	parent > ?  OR (parent = ?  AND name > ?)    PRIMARY KEY (namespace=?)
//	parent > ?2 OR (parent = ?2 AND name > ?3)   PRIMARY KEY (namespace=? AND parent>?)
//	parent > ?2 OR (parent = ?3 AND name > ?4)   PRIMARY KEY (namespace=?)
//
// The second line is what a page costs without the row value: the namespace still bounds the
// search, so no foreign row is read, but every page starts at this namespace's first entry
// and tests the cursor row by row. The fourth line is the control that says which thing is
// doing the work — numbering the parameters changes nothing on its own.
//
// Anyone rewriting this predicate should check which line they landed on rather than which
// one they meant. An equivalent-looking rewrite is a different query here, and it is a
// difference no result distinguishes.
//
// TestAPictureIsPagedByRangeRatherThanByScanningAndSorting asserts the plan this produces,
// with and without table statistics.
const pageQuery = `
	SELECT e.parent, length(CAST(e.name AS BLOB)), n.id, n.mode, n.size,
	       n.atime_sec, n.atime_nsec, n.mtime_sec, n.mtime_nsec,
	       COALESCE(length(CAST(n.content AS BLOB)), 0)
	FROM entries e JOIN nodes n ON n.id = e.node
	WHERE e.namespace = ? AND (e.parent, e.name) > (?, ?)
	ORDER BY e.parent, e.name
	LIMIT ?`

type nodeMetadataScan struct {
	id                 int64
	mode               int64
	size               int64
	atimeSec, mtimeSec int64
	atimeNsec          int32
	mtimeNsec          int32
	contentBytes       int64
}

func (s *nodeMetadataScan) fields() []any {
	return []any{&s.id, &s.mode, &s.size, &s.atimeSec, &s.atimeNsec, &s.mtimeSec, &s.mtimeNsec, &s.contentBytes}
}

func (s nodeMetadataScan) node() metastore.Node {
	return metastore.Node{
		ID: s.id, Mode: fs.FileMode(s.mode), Size: s.size,
		AccessTime: loadedTime(s.atimeSec, s.atimeNsec),
		ModTime:    loadedTime(s.mtimeSec, s.mtimeNsec),
	}
}

func (s *Store) rootMetadata(ctx context.Context, tx *sql.Tx) (metastore.Node, int64, error) {
	var node nodeMetadataScan
	err := tx.QueryRowContext(ctx, `
		SELECT n.id, n.mode, n.size, n.atime_sec, n.atime_nsec, n.mtime_sec, n.mtime_nsec,
		       COALESCE(length(CAST(n.content AS BLOB)), 0)
		FROM nodes n WHERE n.id = ?`, s.root).Scan(node.fields()...)
	return node.node(), node.contentBytes, err
}

func (s *Store) nodeContent(ctx context.Context, tx *sql.Tx, id int64) (metastore.Key, error) {
	var content sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT content FROM nodes WHERE id = ?`, id).Scan(&content)
	return metastore.Key(content.String), err
}

func (p *snapshot) nextMetadata(ctx context.Context) (metastore.Row, metastore.RowPayloadLengths, bool, error) {
	var (
		parent    int64
		nameBytes int64
		node      nodeMetadataScan
	)
	err := p.tx.QueryRowContext(ctx, pageQuery, p.store.namespace, p.parent, p.name, 1).Scan(
		append([]any{&parent, &nameBytes}, node.fields()...)...,
	)
	if err == sql.ErrNoRows {
		return metastore.Row{}, metastore.RowPayloadLengths{}, false, nil
	}
	if err != nil {
		return metastore.Row{}, metastore.RowPayloadLengths{}, false, err
	}
	return metastore.Row{Parent: parent, Name: []byte{}, Node: node.node()}, metastore.RowPayloadLengths{
		Name: nameBytes, Content: node.contentBytes,
	}, true, nil
}

func (p *snapshot) payload(ctx context.Context, parent, node int64) ([]byte, metastore.Key, error) {
	var (
		name    []byte
		content sql.NullString
	)
	err := p.tx.QueryRowContext(ctx, `
		SELECT e.name, n.content
		FROM entries e JOIN nodes n ON n.id = e.node
		WHERE e.namespace = ? AND e.parent = ? AND e.node = ?`,
		p.store.namespace, parent, node).Scan(&name, &content)
	return name, metastore.Key(content.String), err
}

// Close releases the read transaction the picture is taken in, and with it the WAL that
// transaction was pinning. Closing twice is not an error: a caller that stops early closes,
// and so does the one that read to the end.
func (p *snapshot) Close() error {
	if p.tx == nil {
		return nil
	}
	tx := p.tx
	p.tx = nil
	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("releasing a picture of the tree: %w", failure(err))
	}
	return nil
}
