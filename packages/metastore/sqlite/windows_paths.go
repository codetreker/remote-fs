package sqlite

import (
	"context"
	"database/sql"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type windowsPathSize struct{ units, bytes, depth int }

type windowsPathNode struct {
	parent int64
	part   windowsPathSize
	full   windowsPathSize
	state  uint8
}

func windowsPathPart(name []byte) windowsPathSize {
	units := 0
	for _, r := range string(name) {
		units++
		if r > 0xffff {
			units++
		}
	}
	return windowsPathSize{units: units, bytes: len(name), depth: 1}
}

func (base windowsPathSize) child(part windowsPathSize) (windowsPathSize, error) {
	separator := 0
	if base.depth != 0 {
		separator = 1
	}
	if part.units > storage.WindowsMaxPathUTF16Units-base.units-separator ||
		part.bytes > metastore.MaxNotificationNameBytes-base.bytes ||
		part.depth > metastore.MaxNotificationAncestors-base.depth {
		return windowsPathSize{}, syscall.ENAMETOOLONG
	}
	return windowsPathSize{units: base.units + separator + part.units, bytes: base.bytes + part.bytes, depth: base.depth + part.depth}, nil
}

// Every graph vertex is resolved once. Depth is bounded before recursion, and
// no expanded path strings are retained for nested directories.
func validateWindowsPaths(ctx context.Context, root int64, graph map[int64]*windowsPathNode) error {
	var resolve func(int64, int) (windowsPathSize, error)
	resolve = func(id int64, depth int) (windowsPathSize, error) {
		if err := ctx.Err(); err != nil {
			return windowsPathSize{}, err
		}
		if id == root {
			return windowsPathSize{}, nil
		}
		node, ok := graph[id]
		if !ok || node.state == 1 {
			return windowsPathSize{}, syscall.EIO
		}
		if node.state == 2 {
			return node.full, nil
		}
		if depth >= metastore.MaxNotificationAncestors {
			return windowsPathSize{}, syscall.ENAMETOOLONG
		}
		node.state = 1
		parent, err := resolve(node.parent, depth+1)
		if err != nil {
			return windowsPathSize{}, err
		}
		node.full, err = parent.child(node.part)
		if err != nil {
			return windowsPathSize{}, err
		}
		node.state = 2
		return node.full, nil
	}
	for id := range graph {
		if _, err := resolve(id, 0); err != nil {
			return err
		}
	}
	return nil
}

type windowsPathBudget struct{ records, bytes int64 }

func (b *windowsPathBudget) take(length int64) error {
	if length <= 0 || length > 1020 {
		return syscall.EIO
	}
	cost := int64(256) + 4*length
	if b.records < 1 || b.bytes < cost {
		return syscall.EFBIG
	}
	b.records--
	b.bytes -= cost
	return nil
}

func (s *Store) windowsPathSize(ctx context.Context, tx *sql.Tx, id int64, budget *windowsPathBudget) (windowsPathSize, error) {
	var parts []windowsPathSize
	seen := make(map[int64]bool)
	for id != s.root {
		if err := ctx.Err(); err != nil {
			return windowsPathSize{}, err
		}
		if id <= 0 || seen[id] {
			return windowsPathSize{}, syscall.EIO
		}
		seen[id] = true
		if len(parts) >= metastore.MaxNotificationAncestors {
			return windowsPathSize{}, syscall.ENAMETOOLONG
		}
		var parent, length int64
		var name []byte
		if err := tx.QueryRowContext(ctx, `SELECT parent,length(name),CASE WHEN length(name)<=1020 THEN name ELSE NULL END FROM entries WHERE volume=? AND node=?`, s.volume, id).Scan(&parent, &length, &name); err != nil {
			return windowsPathSize{}, err
		}
		if err := budget.take(length); err != nil {
			return windowsPathSize{}, err
		}
		if _, err := windowsNameKey(name); err != nil {
			return windowsPathSize{}, err
		}
		parts = append(parts, windowsPathPart(name))
		id = parent
	}
	var full windowsPathSize
	for i := len(parts) - 1; i >= 0; i-- {
		var err error
		full, err = full.child(parts[i])
		if err != nil {
			return windowsPathSize{}, err
		}
	}
	return full, nil
}

func (s *Store) windowsPathAllowed(ctx context.Context, tx *sql.Tx, parent int64, name []byte, moving int64) error {
	budget := windowsPathBudget{records: s.maxIntegrityRecords, bytes: s.maxIntegrityBytes}
	base, err := s.windowsPathSize(ctx, tx, parent, &budget)
	if err != nil {
		return err
	}
	prospective, err := base.child(windowsPathPart(name))
	if err != nil {
		return err
	}
	if moving == 0 {
		return nil
	}
	old, err := s.windowsPathSize(ctx, tx, moving, &budget)
	if err != nil {
		return err
	}
	if prospective.units <= old.units && prospective.bytes <= old.bytes && prospective.depth <= old.depth {
		return nil
	}
	type pending struct {
		id   int64
		size windowsPathSize
	}
	queue := []pending{{moving, prospective}}
	seen := map[int64]bool{moving: true}
	for head := 0; head < len(queue); head++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		current := queue[head]
		err := func() error {
			rows, err := tx.QueryContext(ctx, `SELECT node,length(name),CASE WHEN length(name)<=1020 THEN name ELSE NULL END FROM entries WHERE volume=? AND parent=?`, s.volume, current.id)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				var id, length int64
				var childName []byte
				if err := rows.Scan(&id, &length, &childName); err != nil {
					return err
				}
				if err := budget.take(length); err != nil {
					return err
				}
				if id <= 0 || seen[id] {
					return syscall.EIO
				}
				if _, err := windowsNameKey(childName); err != nil {
					return err
				}
				full, err := current.size.child(windowsPathPart(childName))
				if err != nil {
					return err
				}
				seen[id] = true
				queue = append(queue, pending{id, full})
			}
			return rows.Err()
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
