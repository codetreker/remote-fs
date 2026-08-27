package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// Snapshot opens a consistent picture of the whole tree and reports the position it is taken
// at.
//
// The picture is a read transaction against the reader pool, and the first statement issued
// in it is the one that reads the position. That order is what makes the position and the
// rows one instant rather than two: SQLite takes its read snapshot when a deferred
// transaction first reads, so a position sampled before the transaction had touched anything
// could describe a moment the rows do not come from. Sampling it afterwards is the worse
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
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, fmt.Errorf("opening a picture of the tree: %w", failure(err))
	}
	var committed int64
	if err := tx.QueryRowContext(ctx,
		`SELECT committed_position FROM logs WHERE namespace = ?`, s.namespace).Scan(&committed); err != nil {
		tx.Rollback()
		return nil, 0, fmt.Errorf("opening a picture of the tree: %w", failure(err))
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

	done bool
}

func (p *snapshot) Next(ctx context.Context, limit int) ([]metastore.Row, bool, error) {
	if p.tx == nil {
		return nil, false, fmt.Errorf("this picture of the tree has been closed: %w", syscall.EINVAL)
	}
	if limit < 1 {
		return nil, false, fmt.Errorf("a page of %d rows is not a page: %w", limit, syscall.EINVAL)
	}
	if p.done {
		return nil, true, nil
	}

	rows := make([]metastore.Row, 0, limit)
	if !p.sentRoot {
		root, err := p.store.rootNode(ctx, p.tx)
		if err != nil {
			return nil, false, fmt.Errorf("reading the root of the picture: %w", failure(err))
		}
		// Parent 0 and no name, which is how metastore.Row names the node that has neither.
		rows = append(rows, metastore.Row{Node: root})
		p.sentRoot = true
		if len(rows) == limit {
			return rows, false, nil
		}
	}

	budget := limit - len(rows)
	page, err := p.page(ctx, budget)
	if err != nil {
		return nil, false, fmt.Errorf("reading a page of the picture: %w", failure(err))
	}
	// A page short of what was asked for is the end of the tree: the cursor advances strictly,
	// so there is nothing after the last row it returned.
	p.done = len(page) < budget
	return append(rows, page...), p.done, nil
}

// page reads the next want entries in (parent, name) order and advances the cursor.
//
// The namespace filter is on the node rather than on the entry, because an entry belongs to
// whichever namespace the node it names does. Node ids are unique across the database, so
// (parent, name) remains a total order over one namespace's entries.
func (p *snapshot) page(ctx context.Context, want int) ([]metastore.Row, error) {
	rows, err := p.tx.QueryContext(ctx, `
		SELECT e.parent, e.name, `+nodeColumns+`
		FROM entries e JOIN nodes n ON n.id = e.node
		WHERE n.namespace = ? AND (e.parent > ? OR (e.parent = ? AND e.name > ?))
		ORDER BY e.parent, e.name
		LIMIT ?`, p.store.namespace, p.parent, p.parent, p.name, want)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	page := make([]metastore.Row, 0, want)
	for rows.Next() {
		var (
			parent int64
			name   []byte
			node   nodeScan
		)
		if err := rows.Scan(append([]any{&parent, &name}, node.fields()...)...); err != nil {
			return nil, err
		}
		page = append(page, metastore.Row{Parent: parent, Name: name, Node: node.node()})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(page) > 0 {
		last := page[len(page)-1]
		p.parent, p.name = last.Parent, last.Name
	}
	return page, nil
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
