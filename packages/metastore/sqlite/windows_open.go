package sqlite

import (
	"context"
	"database/sql"
	"io/fs"
	"math"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/windowsaccess"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *windowsSession) nativeContext(ctx context.Context, f *windowsFile) context.Context {
	if f != nil {
		ctx = withWindowsActor(ctx, f.handle)
	}
	return metastore.WithFilePublicationGuard(ctx, func() error {
		if !s.active || !time.Now().Before(s.expires) {
			return syscall.ESTALE
		}
		return nil
	})
}

func (s *windowsSession) resolveLookup(ctx context.Context, tx *sql.Tx, lookup storage.WindowsLookup) (metastore.Node, metastore.Node, []byte, bool, error) {
	if lookup.Name == "" {
		n, err := s.store.nodeByID(ctx, tx, s.store.root)
		return n, metastore.Node{}, nil, err == nil, err
	}
	if lookup.ParentID > math.MaxInt64 {
		return metastore.Node{}, metastore.Node{}, nil, false, syscall.ESTALE
	}
	parent, err := s.store.nodeByID(ctx, tx, int64(lookup.ParentID))
	if err != nil {
		return metastore.Node{}, parent, nil, false, err
	}
	if !parent.IsDir() {
		return metastore.Node{}, parent, nil, false, syscall.ENOTDIR
	}
	if lookup.ParentReference != "" {
		f := s.files[lookup.ParentReference]
		if f == nil || !f.active || f.id != parent.ID {
			return metastore.Node{}, parent, nil, false, syscall.ESTALE
		}
	}
	if s.store.fileDomain.windows.access.DeletePending(uint64(parent.ID)) {
		return metastore.Node{}, parent, nil, false, windowsError(windowsaccess.ErrDeletePending)
	}
	state, err := s.store.fileState(ctx, tx, parent.ID)
	if err != nil {
		return metastore.Node{}, parent, nil, false, err
	}
	if state.Detached {
		return metastore.Node{}, parent, nil, false, syscall.ESTALE
	}
	node, found, err := s.store.windowsLookup(ctx, tx, parent.ID, []byte(lookup.Name))
	if err != nil {
		return node, parent, nil, false, err
	}
	name := []byte(lookup.Name)
	if found {
		if lookup.ExpectedID != 0 && lookup.ExpectedID != uint64(node.ID) {
			return node, parent, nil, false, syscall.ESTALE
		}
		at, err := s.store.locate(ctx, tx, node.ID)
		if err != nil {
			return node, parent, nil, false, err
		}
		name = at.Name
	} else if lookup.ExpectedID != 0 {
		return node, parent, nil, false, syscall.ESTALE
	}
	return node, parent, name, found, nil
}

func (s *windowsSession) Open(ctx context.Context, request storage.WindowsOpenRequest, id storage.WindowsActionID) (metastore.WindowsResult, error) {
	if err := request.Check(); err != nil {
		return metastore.WindowsResult{}, err
	}
	fingerprint, err := windowsFingerprint(request)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	done, err := s.acquire(ctx)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	defer done()
	a, fresh, err := s.admit(id, fingerprint, nil)
	if err != nil {
		return metastore.WindowsResult{}, err
	}
	if !fresh {
		return s.result(a)
	}
	if len(s.files) >= s.options.MaxFiles {
		return s.finish(a, syscall.EMFILE)
	}
	ctx = s.nativeContext(ctx, nil)
	var node, parent metastore.Node
	var name []byte
	var found bool
	err = s.store.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		node, parent, name, found, err = s.resolveLookup(ctx, tx, request.Lookup)
		return err
	})
	if err != nil {
		return s.finish(a, err)
	}
	if !found && (request.Disposition == storage.WindowsOpen || request.Disposition == storage.WindowsOverwrite) {
		return s.finish(a, syscall.ENOENT)
	}
	if found && request.Disposition == storage.WindowsCreate {
		return s.finish(a, syscall.EEXIST)
	}
	if found && node.Mode&fs.ModeSymlink != 0 && !request.OpenReparsePoint {
		probe := &windowsFile{session: s, id: node.ID}
		var target string
		var at storage.WindowsNameInfo
		err := s.store.inspect(ctx, func(tx *sql.Tx) error { var err error; target, at, err = probe.linkTarget(ctx, tx); return err })
		if err != nil {
			return s.finish(a, err)
		}
		info := storage.WindowsSymlinkInfo{Target: target, Location: at}
		a.result.Symlink = &info
		return s.finish(a, &storage.WindowsSymlinkError{WindowsSymlinkInfo: info, Err: syscall.ELOOP})
	}
	if found && !node.IsDir() && (request.Access&(storage.WindowsWriteData|storage.WindowsAppendData) != 0 || request.DeleteOnClose) {
		if err := s.store.inspect(ctx, func(tx *sql.Tx) error { return s.store.checkWindowsReadonly(ctx, tx, node.ID) }); err != nil {
			return s.finish(a, err)
		}
	}
	if !found && request.Kind != storage.WindowsDirectory && request.DeleteOnClose && request.DOSAttributes&storage.WindowsDOSReadOnly != 0 {
		return s.finish(a, syscall.EACCES)
	}
	directory := node.IsDir()
	if found && node.Mode&fs.ModeSymlink != 0 {
		var attrs uint32
		if err := s.store.inspect(ctx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT windows_attributes FROM nodes WHERE volume=? AND id=?`, s.store.volume, node.ID).Scan(&attrs)
		}); err != nil {
			return s.finish(a, err)
		}
		directory = attrs&storage.WindowsDOSDirectory != 0
	}
	if found && request.Kind == storage.WindowsDirectory && !directory {
		return s.finish(a, syscall.ENOTDIR)
	}
	if found && request.Kind == storage.WindowsRegularFile && directory {
		return s.finish(a, syscall.EISDIR)
	}
	supersede := found && request.Disposition == storage.WindowsSupersede
	overwrite := found && (request.Disposition == storage.WindowsOverwrite || request.Disposition == storage.WindowsOverwriteIf)
	if found && node.Mode&fs.ModeSymlink != 0 && overwrite {
		return s.finish(a, syscall.EINVAL)
	}
	if found && directory && (supersede || overwrite) {
		return s.finish(a, syscall.EISDIR)
	}
	desiredAttributes := request.DOSAttributes
	if overwrite || supersede {
		var existing uint32
		if err := s.store.inspect(ctx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT windows_attributes FROM nodes WHERE volume=? AND id=?`, s.store.volume, node.ID).Scan(&existing)
		}); err != nil {
			return s.finish(a, err)
		}
		if existing&(storage.WindowsDOSHidden|storage.WindowsDOSSystem)&^desiredAttributes != 0 {
			return s.finish(a, syscall.EACCES)
		}
		desiredAttributes = (desiredAttributes &^ storage.WindowsDOSNormal) | storage.WindowsDOSArchive
	}
	handle, err := s.store.fileDomain.windows.allocateHandle()
	if err != nil {
		return s.finish(a, err)
	}
	f := &windowsFile{session: s, handle: handle, access: request.Access, active: true, reference: s.epoch + ":" + strconv.FormatUint(handle, 10)}
	registered := false
	defer func() {
		if registered {
			if request.DeleteOnClose {
				_ = s.store.fileDomain.windows.access.SetDeletePending(handle, false)
			}
			_, _ = s.store.fileDomain.windows.access.Close(handle)
		}
	}()
	register := func() error {
		if err := s.check(ctx); err != nil {
			return err
		}
		f.id = node.ID
		if err := s.store.registerWindowsOpenLocked(ctx, handle, node.ID, request.Access, request.Share); err != nil {
			return err
		}
		registered = true
		if request.DeleteOnClose {
			return s.store.fileDomain.windows.access.SetDeletePending(handle, true)
		}
		return nil
	}
	created := !found || supersede
	a.result.CreateAction = storage.WindowsOpened
	if overwrite {
		ctx = metastore.WithFileIO(ctx, metastore.WindowsIO{Write: true, Truncate: true, Size: 0})
	}
	if created || overwrite {
		kind := locking.WriteMutation
		if created {
			kind = locking.CreateMutation
		}
		if supersede {
			kind = locking.RemoveMutation
		}
		intent := &volumeIntent{kind: kind, totalUsage: true}
		if found {
			intent.nodes = []int64{node.ID}
		}
		err = s.store.mutateTransactionLocked(ctx, ctx, intent, func(tx *sql.Tx) error {
			if supersede {
				if err := s.store.recordRemoved(ctx, tx, metastore.Location{Parent: parent.ID, Name: name}, node); err != nil {
					return err
				}
				if err := s.store.unlink(ctx, tx, parent.ID, name); err != nil {
					return err
				}
				if err := s.store.discard(ctx, tx, node); err != nil {
					return err
				}
			}
			if created {
				mode := request.Mode
				if request.Kind == storage.WindowsDirectory {
					mode |= fs.ModeDir
				}
				var err error
				node, err = s.store.createOpenNode(ctx, tx, parent, []byte(request.Lookup.Name), storage.FileOpenOptions{Mode: mode})
				if err != nil {
					return err
				}
				sec, nsec := sqlvalue.StoredTime(time.Now())
				if _, err := tx.ExecContext(ctx, `UPDATE nodes SET windows_creation_sec=?,windows_creation_nsec=?,windows_change_sec=?,windows_change_nsec=?,windows_attributes=? WHERE volume=? AND id=?`, sec, nsec, sec, nsec, desiredAttributes, s.store.volume, node.ID); err != nil {
					return err
				}
				a.result.CreateAction = storage.WindowsCreated
				if supersede {
					a.result.CreateAction = storage.WindowsSuperseded
				}
			} else {
				if err := s.store.replaceNodeContent(ctx, tx, node, metastore.Object{ModTime: time.Now()}, &desiredAttributes); err != nil {
					return err
				}
				a.result.CreateAction = storage.WindowsOverwritten
			}
			if err := register(); err != nil {
				return err
			}
			var err error
			a.result.Attr, err = f.attr(ctx, tx)
			return err
		})
	} else {
		err = register()
		if err == nil {
			err = s.store.inspect(ctx, func(tx *sql.Tx) error { var err error; a.result.Attr, err = f.attr(ctx, tx); return err })
		}
	}
	if err != nil {
		return s.finish(a, err)
	}
	registered = false
	s.files[f.reference] = f
	s.store.fileDomain.files++
	s.store.coordinator.pins[retainedNode{s.store.volume, node.ID}]++
	a.result.Reference = f
	return s.finish(a, nil)
}

func (f *windowsFile) Reference() string { return f.reference }

func (f *windowsFile) check(access storage.WindowsAccess) error {
	if !f.active {
		return syscall.EBADF
	}
	if access&^f.access != 0 {
		return syscall.EACCES
	}
	return nil
}

func (f *windowsFile) attr(ctx context.Context, tx *sql.Tx) (storage.WindowsAttr, error) {
	node, err := f.session.store.nodeByID(ctx, tx, f.id)
	if err != nil {
		return storage.WindowsAttr{}, err
	}
	var createdSec, changedSec int64
	var createdNsec, changedNsec int32
	var attributes uint32
	err = tx.QueryRowContext(ctx, `SELECT windows_creation_sec,windows_creation_nsec,windows_change_sec,windows_change_nsec,windows_attributes FROM nodes WHERE volume=? AND id=?`, f.session.store.volume, f.id).Scan(&createdSec, &createdNsec, &changedSec, &changedNsec, &attributes)
	if err != nil {
		return storage.WindowsAttr{}, err
	}
	if createdNsec < 0 || createdNsec >= 1e9 || changedNsec < 0 || changedNsec >= 1e9 || attributes&^(storage.WindowsSettableDOSAttributes|storage.WindowsDOSDirectory) != 0 {
		return storage.WindowsAttr{}, syscall.EIO
	}
	if err := checkWindowsKindAttributes(node.Mode, attributes); err != nil {
		return storage.WindowsAttr{}, err
	}
	if node.IsDir() {
		attributes |= storage.WindowsDOSDirectory
	}
	nameInfo := storage.WindowsNameInfo{State: storage.WindowsNameLinked}
	if f.id == f.session.store.root {
		nameInfo.State = storage.WindowsNameRoot
	} else {
		state, err := f.session.store.fileState(ctx, tx, f.id)
		if err != nil {
			return storage.WindowsAttr{}, err
		}
		if state.Detached {
			nameInfo.State = storage.WindowsNameDetached
		} else {
			at, err := f.session.store.locate(ctx, tx, f.id)
			if err != nil {
				return storage.WindowsAttr{}, err
			}
			facts, err := f.session.store.locationFacts(ctx, tx, at)
			if err != nil {
				return storage.WindowsAttr{}, err
			}
			parts := make([]string, 0, len(facts.Ancestors))
			for _, ancestor := range facts.Ancestors {
				if len(ancestor.Name) != 0 {
					parts = append(parts, string(ancestor.Name))
				}
			}
			parts = append(parts, string(facts.LeafName))
			nameInfo.Path = strings.Join(parts, "/")
		}
	}
	if err := nameInfo.Check(); err != nil {
		return storage.WindowsAttr{}, err
	}
	return storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: node.Attr(), CreationTime: time.Unix(createdSec, int64(createdNsec)).UTC(), ChangeTime: time.Unix(changedSec, int64(changedNsec)).UTC(), DOSAttributes: attributes, DeletePending: f.session.store.fileDomain.windows.access.DeletePending(uint64(f.id))}, NameInfo: nameInfo}, nil
}

func (f *windowsFile) Stat(ctx context.Context) (storage.WindowsAttr, error) {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return storage.WindowsAttr{}, err
	}
	defer done()
	if err := f.check(storage.WindowsReadAttributes); err != nil {
		return storage.WindowsAttr{}, err
	}
	var attr storage.WindowsAttr
	err = f.session.store.inspect(ctx, func(tx *sql.Tx) error { var err error; attr, err = f.attr(ctx, tx); return err })
	return attr, err
}

func (f *windowsFile) ListBounded(ctx context.Context, result *storage.WindowsListResult) (err error) {
	defer func() {
		if err != nil {
			result.Fail(err)
		}
	}()
	done, err := f.session.acquire(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := f.check(storage.WindowsReadData); err != nil {
		return err
	}
	return f.session.store.inspect(ctx, func(tx *sql.Tx) error {
		node, err := f.session.store.nodeByID(ctx, tx, f.id)
		if err != nil {
			return err
		}
		if !node.IsDir() {
			return syscall.ENOTDIR
		}
		return f.listWindowsChildren(ctx, tx, result)
	})
}

func (f *windowsFile) Sync(ctx context.Context) error {
	done, err := f.session.acquire(ctx)
	if err != nil {
		return err
	}
	defer done()
	if err := f.check(0); err != nil {
		return err
	}
	return f.session.store.coordinator.healthy()
}

var _ metastore.WindowsStore = (*Store)(nil)
var _ metastore.WindowsSession = (*windowsSession)(nil)
var _ metastore.WindowsFile = (*windowsFile)(nil)
