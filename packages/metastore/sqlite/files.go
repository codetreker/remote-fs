package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type retainedNode struct{ namespace, id int64 }

// All fields are protected by the database commit gate. Physical retention is
// distinct from active publication authority, which is revoked before I/O drains.
type retainedFile struct {
	store          *Store
	id             int64
	read, write    bool
	active, closed bool
	closeErr       error
}

var _ metastore.FileStore = (*Store)(nil)
var _ metastore.File = (*retainedFile)(nil)

func (s *Store) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (metastore.File, error) {
	if err := options.Check(); err != nil {
		return nil, err
	}
	cleaned, err := storage.CleanPath(path)
	if err != nil {
		return nil, err
	}
	return s.openFile(ctx, cleaned, 0, options)
}

func (s *Store) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (metastore.File, error) {
	if err := options.CheckNode(id); err != nil {
		return nil, err
	}
	if id > math.MaxInt64 {
		return nil, syscall.ESTALE
	}
	return s.openFile(ctx, "", int64(id), options)
}

func (s *Store) openFile(ctx context.Context, path string, id int64, options storage.FileOpenOptions) (metastore.File, error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return nil, err
	}
	if err := s.coordinator.healthy(); err != nil {
		return nil, err
	}
	if s.files == nil {
		return nil, syscall.ESTALE
	}
	if s.fileDomain.files >= s.fileDomain.maxFiles {
		return nil, syscall.EAGAIN
	}
	var state metastore.FileState
	resolve := func(tx *sql.Tx) error {
		var node metastore.Node
		var err error
		if id != 0 {
			node, err = s.nodeByID(ctx, tx, id)
		} else {
			node, err = s.resolve(ctx, tx, path)
		}
		if err != nil {
			if errors.Is(err, syscall.ENOENT) && options.ExpectedID != 0 {
				return syscall.ESTALE
			}
			if !errors.Is(err, syscall.ENOENT) || !options.Create {
				return err
			}
			if options.ExpectedID != 0 {
				return syscall.ESTALE
			}
			parent, name, err := s.resolveParent(ctx, tx, path)
			if err != nil {
				return err
			}
			node, err = s.createOpenNode(ctx, tx, parent, name, options)
			if err != nil {
				return err
			}
		} else {
			if options.Create && options.Exclusive {
				return syscall.EEXIST
			}
			if options.ExpectedID != 0 && options.ExpectedID != uint64(node.ID) {
				return syscall.ESTALE
			}
			if !node.Mode.IsRegular() {
				return syscall.EISDIR
			}
			if options.Truncate {
				if err := s.replaceNodeContent(ctx, tx, node, metastore.Object{ModTime: time.Now()}); err != nil {
					return err
				}
			}
		}
		state, err = s.fileState(ctx, tx, node.ID)
		return err
	}
	modify := options.Create || options.Truncate
	if options.Create && !options.Truncate {
		err := s.inspect(ctx, func(tx *sql.Tx) error {
			_, err := s.resolve(ctx, tx, path)
			if err == nil {
				modify = false
			}
			if errors.Is(err, syscall.ENOENT) {
				return nil
			}
			return err
		})
		if err != nil {
			return nil, failure(err)
		}
	}
	if modify {
		kind := locking.CreateMutation
		if options.Truncate {
			kind = locking.WriteMutation
		}
		intent := &namespaceIntent{kind: kind, node: id}
		if id == 0 {
			intent.paths = []string{path}
		}
		if err := s.mutateTransactionLocked(ctx, ctx, intent, resolve); err != nil {
			return nil, failure(err)
		}
	} else if err := s.inspect(ctx, resolve); err != nil {
		return nil, failure(err)
	}
	f := &retainedFile{store: s, id: state.ID, read: options.Read, write: options.Write, active: true}
	s.files[f] = struct{}{}
	s.fileDomain.files++
	s.coordinator.pins[retainedNode{s.namespace, f.id}]++
	return f, nil
}

func (s *Store) createOpenNode(ctx context.Context, tx *sql.Tx, parent metastore.Node, name []byte, options storage.FileOpenOptions) (metastore.Node, error) {
	now := time.Now()
	sec, nsec := storedTime(now)
	id, err := allocateNodeID(ctx, tx)
	if err != nil {
		return metastore.Node{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO nodes (id,namespace,mode,size,atime_sec,atime_nsec,mtime_sec,mtime_nsec,content) VALUES (?,?,?,0,?,?,?,?,NULL)`, id, s.namespace, int64(options.Mode), sec, nsec, sec, nsec); err != nil {
		return metastore.Node{}, err
	}
	if err := s.link(ctx, tx, parent.ID, name, id); err != nil {
		return metastore.Node{}, err
	}
	if err := s.recordCreated(ctx, tx, metastore.Location{Parent: parent.ID, Name: name}, id); err != nil {
		return metastore.Node{}, err
	}
	if err := s.touch(ctx, tx, parent.ID, now); err != nil {
		return metastore.Node{}, err
	}
	return s.nodeByID(ctx, tx, id)
}

func (s *Store) nodeByID(ctx context.Context, tx *sql.Tx, id int64) (metastore.Node, error) {
	node, err := scanNode(tx.QueryRowContext(ctx, `SELECT `+nodeColumns+` FROM nodes n WHERE n.namespace=? AND n.id=?`, s.namespace, id))
	if errors.Is(err, sql.ErrNoRows) {
		return metastore.Node{}, syscall.ESTALE
	}
	return node, err
}

func (s *Store) fileState(ctx context.Context, tx *sql.Tx, id int64) (metastore.FileState, error) {
	var scan nodeScan
	var revision int64
	var detached bool
	err := tx.QueryRowContext(ctx, `SELECT `+nodeColumns+`,n.content_revision,n.detached FROM nodes n WHERE n.namespace=? AND n.id=?`, s.namespace, id).Scan(append(scan.fields(), &revision, &detached)...)
	if errors.Is(err, sql.ErrNoRows) {
		return metastore.FileState{}, syscall.ESTALE
	}
	if err != nil {
		return metastore.FileState{}, err
	}
	if revision < 1 {
		return metastore.FileState{}, syscall.EIO
	}
	return metastore.FileState{Node: scan.node(), Revision: uint64(revision), Detached: detached}, nil
}

func (f *retainedFile) check() error {
	if !f.active {
		return syscall.ESTALE
	}
	return f.store.coordinator.healthy()
}

func (f *retainedFile) Node(ctx context.Context) (metastore.FileState, error) {
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return metastore.FileState{}, err
	}
	defer f.store.coordinator.commit.release()
	if err := f.check(); err != nil {
		return metastore.FileState{}, err
	}
	var state metastore.FileState
	err := f.store.inspect(ctx, func(tx *sql.Tx) error { var err error; state, err = f.store.fileState(ctx, tx, f.id); return err })
	return state, failure(err)
}

func (f *retainedFile) Reserve(ctx context.Context, size int64) (metastore.Key, error) {
	if !f.write {
		return "", syscall.EBADF
	}
	if size < 0 {
		return "", syscall.EINVAL
	}
	if size > f.store.objectLimits.MaxPendingBytes {
		return "", syscall.EFBIG
	}
	key, err := newKey()
	if err != nil {
		return "", err
	}
	sec, nsec := storedTime(time.Now())
	err = f.store.mutate(ctx, func(tx *sql.Tx) error {
		if err := f.check(); err != nil {
			return err
		}
		node, err := f.store.nodeByID(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if err := f.store.roomFor(ctx, tx, size-node.Size); err != nil {
			return err
		}
		return f.store.reserveObject(ctx, tx, key, size, sec, nsec)
	})
	if err != nil {
		return "", failure(err)
	}
	return key, nil
}

func (f *retainedFile) Commit(ctx context.Context, expected uint64, object metastore.Object) (metastore.FileState, error) {
	if !f.write {
		return metastore.FileState{}, syscall.EBADF
	}
	if object.Size < 0 || expected == 0 {
		return metastore.FileState{}, syscall.EINVAL
	}
	var state metastore.FileState
	err := f.store.mutatePublication(ctx, &namespaceIntent{kind: locking.WriteMutation, node: f.id}, func(tx *sql.Tx) error {
		if err := f.check(); err != nil {
			return err
		}
		before, err := f.store.fileState(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if before.Revision != expected {
			return syscall.EAGAIN
		}
		if err := f.store.replaceNodeContent(ctx, tx, before.Node, object); err != nil {
			return err
		}
		state, err = f.store.fileState(ctx, tx, f.id)
		return err
	})
	return state, failure(err)
}

func (s *Store) advanceContentRevision(ctx context.Context, tx *sql.Tx, id int64) error {
	result, err := tx.ExecContext(ctx, `UPDATE nodes SET content_revision=content_revision+1 WHERE id=? AND namespace=? AND content_revision < ?`, id, s.namespace, int64(math.MaxInt64))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("file content revision cannot advance: %w", syscall.EOVERFLOW)
	}
	return nil
}

func (s *Store) replaceNodeContent(ctx context.Context, tx *sql.Tx, node metastore.Node, object metastore.Object) error {
	if object.Key == "" {
		if object.Size != 0 {
			return syscall.EINVAL
		}
	} else {
		var state int
		if err := tx.QueryRowContext(ctx, `SELECT state FROM objects WHERE key=? AND namespace=?`, string(object.Key), s.namespace).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return syscall.EINVAL
			}
			return err
		}
		if state != stateReserved {
			return syscall.EINVAL
		}
	}
	if err := s.advanceContentRevision(ctx, tx, node.ID); err != nil {
		return err
	}
	if err := s.account(ctx, tx, object.Size-node.Size); err != nil {
		return err
	}
	sec, nsec := storedTime(object.ModTime)
	if _, err := tx.ExecContext(ctx, `UPDATE nodes SET size=?,mtime_sec=?,mtime_nsec=?,content=? WHERE namespace=? AND id=?`, object.Size, sec, nsec, storedKey(object.Key), s.namespace, node.ID); err != nil {
		return err
	}
	if object.Key != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET state=?,size=?,digest=? WHERE key=? AND namespace=?`, stateReferenced, object.Size, object.Digest, string(object.Key), s.namespace); err != nil {
			return err
		}
	}
	if node.Content != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET state=? WHERE key=? AND namespace=?`, stateGarbage, string(node.Content), s.namespace); err != nil {
			return err
		}
	}
	return s.recordNamedChanged(ctx, tx, node.ID)
}

func (s *Store) recordNamedChanged(ctx context.Context, tx *sql.Tx, id int64) error {
	var detached bool
	if err := tx.QueryRowContext(ctx, `SELECT detached FROM nodes WHERE namespace=? AND id=?`, s.namespace, id).Scan(&detached); err != nil {
		return err
	}
	if detached {
		return nil
	}
	return s.recordChanged(ctx, tx, id)
}

func (f *retainedFile) SetAttr(ctx context.Context, change storage.AttrChange) (metastore.FileState, error) {
	if err := change.Check(); err != nil {
		return metastore.FileState{}, err
	}
	var state metastore.FileState
	err := f.store.mutatePublication(ctx, &namespaceIntent{kind: locking.SetAttrMutation, node: f.id}, func(tx *sql.Tx) error {
		if err := f.check(); err != nil {
			return err
		}
		if err := f.store.setNodeAttr(ctx, tx, f.id, change); err != nil {
			return err
		}
		var err error
		state, err = f.store.fileState(ctx, tx, f.id)
		return err
	})
	return state, failure(err)
}

func (s *Store) setNodeAttr(ctx context.Context, tx *sql.Tx, id int64, change storage.AttrChange) error {
	node, err := s.nodeByID(ctx, tx, id)
	if err != nil {
		return err
	}
	if change.Empty() {
		return nil
	}
	if err := applyChange(ctx, tx, node, change); err != nil {
		return err
	}
	return s.recordNamedChanged(ctx, tx, id)
}

func (s *Store) StatNode(ctx context.Context, id uint64) (metastore.Node, error) {
	if id == 0 || id > math.MaxInt64 {
		return metastore.Node{}, syscall.ESTALE
	}
	var node metastore.Node
	err := s.inspect(ctx, func(tx *sql.Tx) error { var err error; node, err = s.nodeByID(ctx, tx, int64(id)); return err })
	return node, failure(err)
}

func (s *Store) SetNodeAttr(ctx context.Context, id uint64, change storage.AttrChange) (metastore.Node, error) {
	if id == 0 || id > math.MaxInt64 {
		return metastore.Node{}, syscall.ESTALE
	}
	if err := change.Check(); err != nil {
		return metastore.Node{}, err
	}
	var node metastore.Node
	err := s.mutatePublication(ctx, &namespaceIntent{kind: locking.SetAttrMutation, node: int64(id)}, func(tx *sql.Tx) error {
		if err := s.setNodeAttr(ctx, tx, int64(id), change); err != nil {
			return err
		}
		var err error
		node, err = s.nodeByID(ctx, tx, int64(id))
		return err
	})
	return node, failure(err)
}

func (s *Store) Usage(ctx context.Context) (int64, error) {
	var used int64
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT used FROM namespaces WHERE id=?`, s.namespace).Scan(&used)
	})
	if err == nil && used < 0 {
		err = syscall.EIO
	}
	return used, failure(err)
}

func (f *retainedFile) Retire(ctx context.Context) error {
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer f.store.coordinator.commit.release()
	f.active = false
	return nil
}

func (f *retainedFile) Close(ctx context.Context) error {
	if err := f.store.coordinator.commit.acquire(ctx); err != nil {
		return err
	}
	defer f.store.coordinator.commit.release()
	f.active = false
	if f.closed || f.closeErr != nil {
		return f.closeErr
	}
	s := f.store
	key := retainedNode{s.namespace, f.id}
	count := s.coordinator.pins[key]
	if count < 1 {
		return syscall.EIO
	}
	if count == 1 {
		var detached bool
		err := s.inspect(ctx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT detached FROM nodes WHERE namespace=? AND id=?`, s.namespace, f.id).Scan(&detached)
		})
		if err != nil {
			return failure(err)
		}
		if detached {
			err = s.mutateTransactionLocked(ctx, ctx, &namespaceIntent{kind: locking.RemoveMutation, node: f.id, cleanup: true}, func(tx *sql.Tx) error {
				node, err := s.nodeByID(ctx, tx, f.id)
				if err != nil {
					return err
				}
				return s.discardNode(ctx, tx, node)
			})
			if err != nil {
				if s.coordinator.healthy() != nil || storage.IsPublicationAccountingUncertain(err) {
					f.closeErr = failure(err)
					s.coordinator.poisonWith(f.closeErr)
					if s.locks != nil {
						s.locks.Fence(f.closeErr)
					}
					return f.closeErr
				}
				return failure(err)
			}
		}
		delete(s.coordinator.pins, key)
	} else {
		s.coordinator.pins[key] = count - 1
	}
	delete(s.files, f)
	s.fileDomain.files--
	f.closed = true
	return nil
}
