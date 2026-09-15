package windows

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// This bounded in-memory authority lets native acceptance exercise HTTP, SMB,
// SSPI and Windows mapping without depending on a Linux-only database driver.
// Its unsupported operations fail explicitly; storage durability is not measured.
const nativeMaxFileBytes = 1 << 20

type nativeAuthority struct {
	identity          string
	mu                sync.Mutex
	nodes             map[uint64]*nativeNode
	sessions          []*nativeSession
	changes           []metastore.Change
	nextNode, nextRef uint64
	actionCount       int
	activations       map[storage.WindowsActionID]storage.WindowsActivation
}
type nativeNode struct {
	parent   uint64
	name     string
	attr     storage.WindowsBasicAttr
	data     []byte
	detached bool
}
type nativeSession struct {
	authority *nativeAuthority
	options   storage.FileSessionOptions
	epoch     string
	expires   time.Time
	revision  uint64
	closed    bool
	files     map[string]*nativeFile
	actions   map[storage.WindowsActionID]nativeReceipt
}

// Receipts remain for the authority lifetime. Admission stops at the global cap;
// the single action epoch never evicts a receipt that could be replayed.
type nativeReceipt struct {
	fingerprint string
	result      storage.WindowsActionResult
}
type nativeFile struct {
	session   *nativeSession
	node      *nativeNode
	reference string
	intent    storage.WindowsOpenIntent
	closed    bool
}

var _ storage.WindowsStorage = (*nativeAuthority)(nil)
var _ metastore.Log = (*nativeAuthority)(nil)
var _ storage.WindowsSession = (*nativeSession)(nil)
var _ storage.WindowsFile = (*nativeFile)(nil)

func newNativeAuthority() *nativeAuthority {
	now := time.Now().UTC()
	var identity [16]byte
	rand.Read(identity[:])
	root := &nativeNode{attr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir | 0755, AccessTime: now, ModTime: now}, CreationTime: now, ChangeTime: now, DOSAttributes: storage.WindowsDOSDirectory}}
	return &nativeAuthority{identity: fmt.Sprintf("%x", identity), nodes: map[uint64]*nativeNode{1: root}, nextNode: 1, activations: make(map[storage.WindowsActionID]storage.WindowsActivation)}
}
func (a *nativeAuthority) CheckBounded() error        { return nil }
func (a *nativeAuthority) CheckWindowsStorage() error { return nil }
func (a *nativeAuthority) WindowsState(ctx context.Context) (storage.WindowsState, error) {
	return storage.WindowsState{Enabled: true, ActionEpoch: 1, MaxEventBytes: metastore.MaxNotificationBytes, VolumeIdentity: a.identity, VolumeSerial: 0x524653}, ctx.Err()
}
func (a *nativeAuthority) EnableWindows(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.WindowsActivation{}, err
	}
	epoch, err := id.Epoch()
	if err != nil {
		return storage.WindowsActivation{}, err
	}
	if epoch != 1 {
		return storage.WindowsActivation{}, syscall.ESTALE
	}
	if r, ok := a.activations[id]; ok {
		return r, nil
	}
	if len(a.activations) >= 32 {
		return storage.WindowsActivation{}, syscall.ENOSPC
	}
	r := storage.WindowsActivation{Action: id, State: storage.WindowsActionCompleted, Enabled: true, HistoryRemaining: time.Hour}
	a.activations[id] = r
	return r, nil
}
func (a *nativeAuthority) QueryWindowsActivation(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.WindowsActivation{}, err
	}
	r, ok := a.activations[id]
	if !ok {
		return r, syscall.ESTALE
	}
	return r, nil
}
func (a *nativeAuthority) NewWindowsSession(ctx context.Context, o storage.FileSessionOptions) (storage.WindowsSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := o.Check(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.sessions) >= 32 {
		return nil, syscall.EMFILE
	}
	s := &nativeSession{authority: a, options: o, epoch: fmt.Sprintf("native-%d", len(a.sessions)+1), expires: time.Now().Add(o.Lease), revision: 1, files: make(map[string]*nativeFile), actions: make(map[storage.WindowsActionID]nativeReceipt)}
	a.sessions = append(a.sessions, s)
	return s, nil
}
func (a *nativeAuthority) lookup(p string) (*nativeNode, error) {
	clean, err := storage.CleanPath(p)
	if err != nil {
		return nil, err
	}
	p = clean
	n := a.nodes[1]
	if p == "" {
		return n, nil
	}
	for _, name := range strings.Split(p, "/") {
		if !n.attr.IsDir() {
			return nil, syscall.ENOTDIR
		}
		n = a.child(n.attr.ID, name)
		if n == nil {
			return nil, syscall.ENOENT
		}
	}
	return n, nil
}
func (a *nativeAuthority) child(parent uint64, name string) *nativeNode {
	for _, n := range a.nodes {
		if !n.detached && n.parent == parent && strings.EqualFold(n.name, name) {
			return n
		}
	}
	return nil
}
func (a *nativeAuthority) path(n *nativeNode) string {
	if n.attr.ID == 1 || n.detached {
		return ""
	}
	parts := []string{n.name}
	for p := n.parent; p != 1; {
		parent := a.nodes[p]
		parts = append(parts, parent.name)
		p = parent.parent
	}
	slices.Reverse(parts)
	return strings.Join(parts, "/")
}
func (a *nativeAuthority) attrs(n *nativeNode) storage.WindowsAttr {
	state := storage.WindowsNameLinked
	if n.attr.ID == 1 {
		state = storage.WindowsNameRoot
	} else if n.detached {
		state = storage.WindowsNameDetached
	}
	return storage.WindowsAttr{WindowsBasicAttr: n.attr, NameInfo: storage.WindowsNameInfo{State: state, Path: a.path(n)}}
}
func (a *nativeAuthority) Stat(ctx context.Context, p string) (storage.Attr, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.Attr{}, err
	}
	n, err := a.lookup(p)
	if err != nil {
		return storage.Attr{}, err
	}
	return n.attr.Attr, nil
}
func (a *nativeAuthority) Read(ctx context.Context, p string) ([]byte, error) {
	return a.ReadBounded(ctx, p, nativeMaxFileBytes)
}
func (a *nativeAuthority) ReadBounded(ctx context.Context, p string, max int64) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if max <= 0 {
		return nil, syscall.EINVAL
	}
	n, err := a.lookup(p)
	if err != nil {
		return nil, err
	}
	if n.attr.IsDir() {
		return nil, syscall.EISDIR
	}
	if int64(len(n.data)) > max {
		return nil, syscall.EFBIG
	}
	return slices.Clone(n.data), nil
}
func (a *nativeAuthority) List(ctx context.Context, p string) ([]storage.Entry, error) {
	r, err := storage.NewListResult(1<<20, 0, func(_ int, n int64, _ storage.Attr) (int64, error) { return n + 128, nil })
	if err != nil {
		return nil, err
	}
	if err = a.ListBounded(ctx, p, r); err != nil {
		return nil, err
	}
	return r.Entries()
}
func (a *nativeAuthority) ListBounded(ctx context.Context, p string, r *storage.ListResult) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return r.Fail(err)
	}
	n, err := a.lookup(p)
	if err != nil {
		return r.Fail(err)
	}
	if !n.attr.IsDir() {
		return r.Fail(syscall.ENOTDIR)
	}
	for _, child := range a.nodes {
		if !child.detached && child.parent == n.attr.ID {
			if err := r.Add(storage.Entry{Name: child.name, Attr: child.attr.Attr}); err != nil {
				return r.Fail(err)
			}
		}
	}
	return nil
}
func (a *nativeAuthority) Space(ctx context.Context) (storage.Space, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var used int64
	for _, n := range a.nodes {
		used += int64(len(n.data))
	}
	return storage.Space{Total: 64 * nativeMaxFileBytes, Used: used, Avail: 64*nativeMaxFileBytes - used}, ctx.Err()
}
func (*nativeAuthority) SetAttr(context.Context, string, storage.AttrChange) error {
	return syscall.EOPNOTSUPP
}
func (*nativeAuthority) Write(context.Context, string, []byte) error  { return syscall.EOPNOTSUPP }
func (*nativeAuthority) Create(context.Context, string) error         { return syscall.EOPNOTSUPP }
func (*nativeAuthority) Mkdir(context.Context, string) error          { return syscall.EOPNOTSUPP }
func (*nativeAuthority) Remove(context.Context, string) error         { return syscall.EOPNOTSUPP }
func (*nativeAuthority) RemoveDir(context.Context, string) error      { return syscall.EOPNOTSUPP }
func (*nativeAuthority) Rename(context.Context, string, string) error { return syscall.EOPNOTSUPP }
func (a *nativeAuthority) fileData(p string) ([]byte, error)          { return a.Read(context.Background(), p) }
func (a *nativeAuthority) activeState() (sessions, refs int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, s := range a.sessions {
		if !s.closed {
			sessions++
		}
		for _, f := range s.files {
			if !f.closed {
				refs++
			}
		}
	}
	return sessions, refs
}
func (a *nativeAuthority) openReferences() int {
	_, refs := a.activeState()
	return refs
}
func (a *nativeAuthority) expireSessions() {
	for _, s := range a.sessions {
		s.expire()
	}
}

func (s *nativeSession) expire() {
	if !s.closed && time.Now().After(s.expires) {
		s.closed = true
		for _, f := range s.files {
			f.release()
		}
	}
}
func (s *nativeSession) health(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.expire()
	if s.closed {
		return syscall.ESTALE
	}
	return nil
}
func (s *nativeSession) status() storage.FileSessionStatus {
	remaining := time.Until(s.expires)
	if remaining < 0 {
		remaining = 0
	}
	if s.closed {
		remaining = 0
	}
	return storage.FileSessionStatus{Epoch: s.epoch, Remaining: remaining, Revision: s.revision, ActionEpoch: 1, HistoryRemaining: s.options.History, Retired: s.closed, Fenced: s.closed}
}
func (s *nativeSession) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.FileSessionStatus{}, err
	}
	s.expire()
	return s.status(), nil
}
func (s *nativeSession) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if err := s.health(ctx); err != nil {
		return s.status(), err
	}
	s.expires = time.Now().Add(s.options.Lease)
	s.revision++
	return s.status(), nil
}
func (s *nativeSession) Close(ctx context.Context) error {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if s.closed {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.closed = true
	for _, f := range s.files {
		f.release()
	}
	return nil
}
func nativeActionError(r storage.WindowsActionResult) error {
	if r.Errno == 0 {
		return nil
	}
	if r.Failure != "" {
		return &storage.WindowsError{Failure: r.Failure, Err: r.Errno}
	}
	return r.Errno
}
func (s *nativeSession) action(ctx context.Context, id storage.WindowsActionID, kind string, input any, run func() (storage.WindowsActionResult, error)) (storage.WindowsActionResult, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	fingerprint := fmt.Sprintf("%s:%x", kind, sha256.Sum256(encoded))
	if previous, ok := s.actions[id]; ok {
		if previous.fingerprint != fingerprint {
			return storage.WindowsActionResult{}, syscall.EINVAL
		}
		return previous.result, nativeActionError(previous.result)
	}
	if err := s.health(ctx); err != nil {
		return storage.WindowsActionResult{}, err
	}
	epoch, err := id.Epoch()
	if err != nil {
		return storage.WindowsActionResult{}, err
	}
	if epoch != 1 {
		return storage.WindowsActionResult{}, syscall.ESTALE
	}
	if s.authority.actionCount >= 1024 {
		return storage.WindowsActionResult{}, syscall.ENOSPC
	}
	r, err := run()
	r.Action = id
	r.State = storage.WindowsActionCompleted
	r.HistoryRemaining = s.options.History
	if err != nil {
		r.State = storage.WindowsActionRejected
		r.Errno = storage.ErrnoOf(err)
		r.Failure = storage.WindowsFailureOf(err)
	}
	s.actions[id] = nativeReceipt{fingerprint: fingerprint, result: r}
	s.authority.actionCount++
	return r, err
}
func (s *nativeSession) QueryAction(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	s.authority.mu.Lock()
	defer s.authority.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.WindowsActionResult{}, err
	}
	r, ok := s.actions[id]
	if !ok {
		return storage.WindowsActionResult{}, syscall.ESTALE
	}
	return r.result, nativeActionError(r.result)
}
func (s *nativeSession) CancelAction(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return s.QueryAction(ctx, id)
}
func nativeSharing(access storage.WindowsAccess, share storage.WindowsShare) bool {
	return (access&storage.WindowsReadData == 0 || share&storage.WindowsShareRead != 0) && (access&(storage.WindowsWriteData|storage.WindowsAppendData) == 0 || share&storage.WindowsShareWrite != 0) && (access&storage.WindowsDelete == 0 || share&storage.WindowsShareDelete != 0)
}
func (s *nativeSession) resolve(l storage.WindowsLookup) (*nativeNode, error) {
	a := s.authority
	if l.Name == "" {
		return a.nodes[1], nil
	}
	p := a.nodes[l.ParentID]
	if p == nil || p.detached {
		return nil, syscall.ESTALE
	}
	if !p.attr.IsDir() {
		return nil, syscall.ENOTDIR
	}
	if l.ParentReference != "" {
		f := s.files[l.ParentReference]
		if f == nil || f.closed || f.node != p {
			return nil, syscall.EBADF
		}
	}
	n := a.child(l.ParentID, l.Name)
	if l.ExpectedID != 0 && (n == nil || n.attr.ID != l.ExpectedID) {
		return nil, syscall.ESTALE
	}
	return n, nil
}
func (s *nativeSession) Open(ctx context.Context, request storage.WindowsOpenRequest, id storage.WindowsActionID) (storage.WindowsOpenResult, error) {
	a := s.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	r, err := s.action(ctx, id, "open", request, func() (storage.WindowsActionResult, error) {
		if err := request.Check(); err != nil {
			return storage.WindowsActionResult{}, err
		}
		a.expireSessions()
		n, err := s.resolve(request.Lookup)
		if err != nil {
			return storage.WindowsActionResult{}, err
		}
		if n == nil && (request.Disposition == storage.WindowsOpen || request.Disposition == storage.WindowsOverwrite) {
			return storage.WindowsActionResult{}, syscall.ENOENT
		}
		if n != nil && request.Disposition == storage.WindowsCreate {
			return storage.WindowsActionResult{}, syscall.EEXIST
		}
		if a.nextRef >= 256 || len(s.files) >= s.options.MaxFiles {
			return storage.WindowsActionResult{}, syscall.EMFILE
		}
		createAction := storage.WindowsOpened
		if n != nil {
			if n.attr.DeletePending {
				return storage.WindowsActionResult{}, &storage.WindowsError{Failure: storage.WindowsDeletePending, Err: syscall.EACCES}
			}
			if request.Kind == storage.WindowsDirectory && !n.attr.IsDir() {
				return storage.WindowsActionResult{}, syscall.ENOTDIR
			}
			if request.Kind == storage.WindowsRegularFile && n.attr.IsDir() {
				return storage.WindowsActionResult{}, syscall.EISDIR
			}
			for _, other := range a.sessions {
				for _, f := range other.files {
					if !f.closed && f.node == n && (!nativeSharing(request.Access, f.intent.Share) || !nativeSharing(f.intent.Access, request.Share)) {
						return storage.WindowsActionResult{}, &storage.WindowsError{Failure: storage.WindowsSharingViolation, Err: syscall.EACCES}
					}
				}
			}
			if request.Disposition == storage.WindowsSupersede || request.Disposition == storage.WindowsOverwrite || request.Disposition == storage.WindowsOverwriteIf {
				if n.attr.IsDir() {
					return storage.WindowsActionResult{}, syscall.EISDIR
				}
				n.data = nil
				n.attr.Size = 0
				n.attr.ModTime = time.Now().UTC()
				n.attr.ChangeTime = n.attr.ModTime
				n.attr.DOSAttributes = request.DOSAttributes
				createAction = storage.WindowsOverwritten
				if request.Disposition == storage.WindowsSupersede {
					createAction = storage.WindowsSuperseded
				}
				a.record(n, metastore.Modified, metastore.ChangeSize|metastore.ChangeContent|metastore.ChangeModTime, nil)
			}
		} else {
			if len(a.nodes) >= 64 {
				return storage.WindowsActionResult{}, syscall.ENOSPC
			}
			a.nextNode++
			now := time.Now().UTC()
			mode := request.Mode
			dos := request.DOSAttributes
			if request.Kind == storage.WindowsDirectory {
				mode |= fs.ModeDir
				dos |= storage.WindowsDOSDirectory
			}
			n = &nativeNode{parent: request.Lookup.ParentID, name: request.Lookup.Name, attr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: a.nextNode, Mode: mode, AccessTime: now, ModTime: now}, CreationTime: now, ChangeTime: now, DOSAttributes: dos}}
			a.nodes[n.attr.ID] = n
			createAction = storage.WindowsCreated
			a.record(n, metastore.Created, metastore.ChangeName, nil)
		}
		a.nextRef++
		f := &nativeFile{session: s, node: n, reference: fmt.Sprintf("native-ref-%d", a.nextRef), intent: request.WindowsOpenIntent}
		s.files[f.reference] = f
		if request.DeleteOnClose {
			n.attr.DeletePending = true
		}
		return storage.WindowsActionResult{File: f, Attr: a.attrs(n), CreateAction: createAction}, nil
	})
	return storage.WindowsOpenResult{File: r.File, Attr: r.Attr, CreateAction: r.CreateAction}, err
}
func (f *nativeFile) Reference() string { return f.reference }
func (f *nativeFile) health(ctx context.Context, access storage.WindowsAccess) error {
	if err := f.session.health(ctx); err != nil {
		return err
	}
	if f.closed || access != 0 && f.intent.Access&access == 0 {
		return syscall.EBADF
	}
	return nil
}
func (f *nativeFile) Stat(ctx context.Context) (storage.WindowsAttr, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.health(ctx, 0); err != nil {
		return storage.WindowsAttr{}, err
	}
	return a.attrs(f.node), nil
}
func (f *nativeFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.health(ctx, storage.WindowsReadData); err != nil {
		return storage.FileRead{}, err
	}
	if err := storage.CheckWindowsRange(offset, int64(length)); err != nil {
		return storage.FileRead{}, err
	}
	if f.node.attr.IsDir() {
		return storage.FileRead{}, syscall.EISDIR
	}
	if int64(len(f.node.data)) > f.session.options.MaxFileSize {
		return storage.FileRead{}, syscall.EFBIG
	}
	start := min(offset, int64(len(f.node.data)))
	end := min(offset+int64(length), int64(len(f.node.data)))
	return storage.FileRead{Attr: f.node.attr.Attr, Data: slices.Clone(f.node.data[start:end])}, nil
}
func (f *nativeFile) mutate(ctx context.Context, id storage.WindowsActionID, kind string, input any, access storage.WindowsAccess, run func() error) (storage.WindowsActionResult, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	return f.session.action(ctx, id, f.reference+":"+kind, input, func() (storage.WindowsActionResult, error) {
		if err := f.health(ctx, access); err != nil {
			return storage.WindowsActionResult{}, err
		}
		err := run()
		return storage.WindowsActionResult{Attr: a.attrs(f.node)}, err
	})
}
func (f *nativeFile) WriteAt(ctx context.Context, offset int64, data []byte, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, id, "write", struct {
		Offset int64
		Data   []byte
	}{offset, data}, storage.WindowsWriteData|storage.WindowsAppendData, func() error {
		if err := storage.CheckWindowsRange(offset, int64(len(data))); err != nil {
			return err
		}
		if f.node.attr.IsDir() {
			return syscall.EISDIR
		}
		if f.intent.Access&storage.WindowsWriteData == 0 {
			offset = int64(len(f.node.data))
		}
		size := max(int64(len(f.node.data)), offset+int64(len(data)))
		if size > nativeMaxFileBytes || size > f.session.options.MaxFileSize {
			return syscall.EFBIG
		}
		if size > int64(len(f.node.data)) {
			f.node.data = append(f.node.data, make([]byte, int(size)-len(f.node.data))...)
		}
		copy(f.node.data[offset:], data)
		f.contentChanged()
		return nil
	})
}
func (f *nativeFile) contentChanged() {
	n := f.node
	n.attr.Size = int64(len(n.data))
	n.attr.ModTime = time.Now().UTC()
	n.attr.ChangeTime = n.attr.ModTime
	f.session.authority.record(n, metastore.Modified, metastore.ChangeSize|metastore.ChangeContent|metastore.ChangeModTime|metastore.ChangeTime, nil)
}
func (f *nativeFile) Truncate(ctx context.Context, size int64, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, id, "truncate", size, storage.WindowsWriteData, func() error {
		if size < 0 {
			return syscall.EINVAL
		}
		if f.node.attr.IsDir() {
			return syscall.EISDIR
		}
		if size > nativeMaxFileBytes || size > f.session.options.MaxFileSize {
			return syscall.EFBIG
		}
		data := make([]byte, int(size))
		copy(data, f.node.data)
		f.node.data = data
		f.contentChanged()
		return nil
	})
}
func (f *nativeFile) SetAttr(ctx context.Context, c storage.WindowsAttrChange, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, id, "setattr", c, storage.WindowsWriteAttributes, func() error {
		if err := c.Check(); err != nil {
			return err
		}
		n := f.node
		mask := metastore.ChangeMask(0)
		if c.Mode != nil {
			n.attr.Mode = n.attr.Mode.Type() | *c.Mode
			mask |= metastore.ChangeAttributes
		}
		if c.AccessTime != nil {
			n.attr.AccessTime = *c.AccessTime
			mask |= metastore.ChangeAccessTime
		}
		if c.ModTime != nil {
			n.attr.ModTime = *c.ModTime
			mask |= metastore.ChangeModTime
		}
		if c.CreationTime != nil {
			n.attr.CreationTime = *c.CreationTime
			mask |= metastore.ChangeCreationTime
		}
		if c.ChangeTime != nil {
			n.attr.ChangeTime = *c.ChangeTime
			mask |= metastore.ChangeTime
		}
		if c.DOSAttributes != nil {
			n.attr.DOSAttributes = *c.DOSAttributes
			if n.attr.IsDir() {
				n.attr.DOSAttributes |= storage.WindowsDOSDirectory
			}
			mask |= metastore.ChangeAttributes
		}
		f.session.authority.record(n, metastore.Modified, mask, nil)
		return nil
	})
}
func (f *nativeFile) ListBounded(ctx context.Context, r *storage.WindowsListResult) error {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.health(ctx, storage.WindowsReadData); err != nil {
		return r.Fail(err)
	}
	if !f.node.attr.IsDir() {
		return r.Fail(syscall.ENOTDIR)
	}
	for _, n := range a.nodes {
		if !n.detached && n.parent == f.node.attr.ID {
			if err := r.Add(storage.WindowsEntry{Name: n.name, Attr: n.attr}); err != nil {
				return r.Fail(err)
			}
		}
	}
	return nil
}

func (f *nativeFile) SetDeletePending(ctx context.Context, pending bool, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, id, "delete-pending", pending, storage.WindowsDelete, func() error {
		a := f.session.authority
		if f.node.attr.ID == 1 {
			return syscall.EBUSY
		}
		if pending {
			for _, s := range a.sessions {
				for _, other := range s.files {
					if other != f && !other.closed && other.node == f.node && other.intent.Share&storage.WindowsShareDelete == 0 {
						return &storage.WindowsError{Failure: storage.WindowsSharingViolation, Err: syscall.EACCES}
					}
				}
			}
			if f.node.attr.IsDir() {
				for _, n := range a.nodes {
					if !n.detached && n.parent == f.node.attr.ID {
						return syscall.ENOTEMPTY
					}
				}
			}
		}
		f.node.attr.DeletePending = pending
		return nil
	})
}
func (f *nativeFile) Rename(ctx context.Context, r storage.WindowsRenameRequest, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, id, "rename", r, storage.WindowsDelete, func() error {
		if err := r.Check(); err != nil {
			return err
		}
		a := f.session.authority
		source, err := f.session.resolve(r.Source)
		if err != nil {
			return err
		}
		if source != f.node {
			return syscall.ESTALE
		}
		destination, err := f.session.resolve(r.Destination)
		if err != nil {
			return err
		}
		if destination != nil && destination != source {
			if !r.Replace || r.Destination.ExpectedID == 0 {
				return syscall.EEXIST
			}
			if destination.attr.IsDir() {
				return syscall.EISDIR
			}
		}
		for p := r.Destination.ParentID; p != 0; p = a.nodes[p].parent {
			if p == source.attr.ID {
				return syscall.EINVAL
			}
		}
		for _, s := range a.sessions {
			for _, other := range s.files {
				if !other.closed && other != f && (other.node == source || other.node == destination) && other.intent.Share&storage.WindowsShareDelete == 0 {
					return &storage.WindowsError{Failure: storage.WindowsSharingViolation, Err: syscall.EACCES}
				}
			}
		}
		if destination != nil && destination != source {
			a.record(destination, metastore.Removed, metastore.ChangeName, nil)
			destination.detached = true
		}
		before := a.location(source)
		source.parent = r.Destination.ParentID
		source.name = r.Destination.Name
		source.attr.ChangeTime = time.Now().UTC()
		a.record(source, metastore.Renamed, metastore.ChangeName|metastore.ChangeTime, before)
		return nil
	})
}
func (f *nativeFile) ReadLink(ctx context.Context) (storage.WindowsSymlinkInfo, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := f.health(ctx, 0); err != nil {
		return storage.WindowsSymlinkInfo{}, err
	}
	return storage.WindowsSymlinkInfo{}, &storage.WindowsError{Failure: storage.WindowsNotReparsePoint, Err: syscall.EINVAL}
}
func (f *nativeFile) SetLink(ctx context.Context, target string, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, id, "setlink", target, storage.WindowsWriteData, func() error { return syscall.EOPNOTSUPP })
}
func (f *nativeFile) LockBatch(ctx context.Context, b storage.WindowsLockBatch, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.mutate(ctx, id, "lock", b, 0, func() error { return syscall.EOPNOTSUPP })
}
func (f *nativeFile) Sync(ctx context.Context) error {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	return f.health(ctx, 0)
}
func (f *nativeFile) release() {
	if f.closed {
		return
	}
	f.closed = true
	n := f.node
	if !n.attr.DeletePending || n.detached {
		return
	}
	a := f.session.authority
	for _, s := range a.sessions {
		for _, other := range s.files {
			if !other.closed && other.node == n {
				return
			}
		}
	}
	a.record(n, metastore.Removed, metastore.ChangeName, nil)
	n.detached = true
}
func (f *nativeFile) Close(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	a := f.session.authority
	a.mu.Lock()
	defer a.mu.Unlock()
	return f.session.action(ctx, id, f.reference+":close", nil, func() (storage.WindowsActionResult, error) {
		f.release()
		return storage.WindowsActionResult{Attr: a.attrs(f.node)}, nil
	})
}
func (a *nativeAuthority) location(n *nativeNode) *metastore.LocationFacts {
	if n.attr.ID == 1 || n.detached {
		return nil
	}
	var ancestors []metastore.DirectoryAncestor
	for p := n.parent; p != 0; {
		parent := a.nodes[p]
		ancestors = append(ancestors, metastore.DirectoryAncestor{DirectoryID: int64(p), Name: []byte(parent.name)})
		p = parent.parent
	}
	slices.Reverse(ancestors)
	return &metastore.LocationFacts{Ancestors: ancestors, LeafName: []byte(n.name)}
}
func nativeMeta(n *nativeNode) metastore.Node {
	return metastore.Node{ID: int64(n.attr.ID), Mode: n.attr.Mode, Size: n.attr.Size, AccessTime: n.attr.AccessTime, ModTime: n.attr.ModTime}
}
func (a *nativeAuthority) position() metastore.Position {
	if len(a.changes) == 0 {
		return 0
	}
	return a.changes[len(a.changes)-1].Position
}
func (a *nativeAuthority) record(n *nativeNode, kind metastore.ChangeKind, mask metastore.ChangeMask, before *metastore.LocationFacts) {
	node := nativeMeta(n)
	at := a.location(n)
	notification := &metastore.Notification{SubjectID: int64(n.attr.ID), SubjectKind: n.attr.Mode.Type(), Directory: n.attr.IsDir(), ChangeMask: mask}
	c := metastore.Change{Position: a.position() + 1, Kind: kind, Parent: int64(n.parent), Name: []byte(n.name), Node: &node, Notification: notification}
	if n.detached {
		c.Parent = 0
		c.Name = nil
	}
	switch kind {
	case metastore.Created:
		notification.After = at
	case metastore.Removed:
		notification.Before = at
		c.Node = nil
	case metastore.Modified:
		notification.Before = at
		notification.After = at
	case metastore.Renamed:
		notification.Before = before
		notification.After = at
		c.From = &metastore.Location{Parent: before.Ancestors[len(before.Ancestors)-1].DirectoryID, Name: slices.Clone(before.LeafName)}
	}
	if err := metastore.ValidateNotification(c); err != nil {
		panic(err)
	}
	if len(a.changes) == 1024 {
		copy(a.changes, a.changes[1:])
		a.changes[len(a.changes)-1] = c
	} else {
		a.changes = append(a.changes, c)
	}
}
func (a *nativeAuthority) Incarnation(ctx context.Context, maxBytes int64) (metastore.Incarnation, error) {
	identity := a.identity
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if int64(len(identity)) > maxBytes {
		return "", syscall.EFBIG
	}
	return metastore.Incarnation(identity), nil
}
func (a *nativeAuthority) Barrier(ctx context.Context, maxBytes int64) (metastore.LogBarrier, error) {
	identity, err := a.Incarnation(ctx, maxBytes)
	if err != nil {
		return metastore.LogBarrier{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return metastore.LogBarrier{Incarnation: identity, Position: a.position()}, nil
}
func (a *nativeAuthority) CommittedPosition(ctx context.Context) (metastore.Position, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.position(), ctx.Err()
}
func (a *nativeAuthority) Since(ctx context.Context, after metastore.Position, limit int, result *metastore.ChangeResult) (metastore.Retention, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	retention := metastore.Retention{Tail: a.position()}
	if len(a.changes) > 0 {
		retention.Oldest = a.changes[0].Position
		retention.TrimmedThrough = retention.Oldest - 1
	}
	if err := ctx.Err(); err != nil {
		return retention, result.Fail(err)
	}
	if limit <= 0 || after < 0 {
		return retention, result.Fail(syscall.EINVAL)
	}
	count := 0
	for _, c := range a.changes {
		if c.Position <= after {
			continue
		}
		if count >= limit {
			break
		}
		encoded, err := metastore.EncodeNotification(c)
		if err != nil {
			return retention, result.Fail(err)
		}
		lengths := metastore.ChangePayloadLengths{Name: int64(len(c.Name)), Notification: int64(len(encoded))}
		var from []byte
		var content metastore.Key
		if c.From != nil {
			from = c.From.Name
			lengths.FromName = int64(len(from))
		}
		if c.Node != nil {
			content = c.Node.Content
			lengths.Content = int64(len(content))
		}
		reservation, fits, err := result.Reserve(c, lengths)
		if err != nil {
			return retention, result.Fail(err)
		}
		if !fits {
			break
		}
		if err = reservation.Commit(c.Name, from, content, encoded); err != nil {
			return retention, result.Fail(err)
		}
		count++
	}
	return retention, nil
}

type nativeSnapshot struct {
	mu     sync.Mutex
	rows   []metastore.Row
	index  int
	closed bool
}

func (a *nativeAuthority) Snapshot(ctx context.Context) (metastore.Snap, metastore.Position, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	rows := make([]metastore.Row, 0, len(a.nodes))
	for _, n := range a.nodes {
		if !n.detached {
			rows = append(rows, metastore.Row{Parent: int64(n.parent), Name: []byte(n.name), Node: nativeMeta(n)})
		}
	}
	slices.SortFunc(rows, func(a, b metastore.Row) int {
		if a.Node.ID < b.Node.ID {
			return -1
		}
		if a.Node.ID > b.Node.ID {
			return 1
		}
		return 0
	})
	return &nativeSnapshot{rows: rows}, a.position(), nil
}
func (s *nativeSnapshot) Next(ctx context.Context, limit int, result *metastore.RowResult) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, result.Fail(err)
	}
	if s.closed {
		return false, result.Fail(syscall.EBADF)
	}
	if limit <= 0 {
		return false, result.Fail(syscall.EINVAL)
	}
	for count := 0; s.index < len(s.rows) && count < limit; count++ {
		row := s.rows[s.index]
		reservation, fits, err := result.Reserve(row, metastore.RowPayloadLengths{Name: int64(len(row.Name)), Content: int64(len(row.Node.Content))})
		if err != nil {
			return false, result.Fail(err)
		}
		if !fits {
			break
		}
		if err = reservation.Commit(row.Name, row.Node.Content); err != nil {
			return false, result.Fail(err)
		}
		s.index++
	}
	return s.index == len(s.rows), nil
}
func (s *nativeSnapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.rows = nil
	return nil
}

type nativeUnsupportedLocks struct{}

func (*nativeAuthority) LockService() locking.Service { return nativeUnsupportedLocks{} }
func (nativeUnsupportedLocks) BeginEnrollment(context.Context) (locking.EnrollmentTicket, error) {
	return "", syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) OpenSession(context.Context, locking.EnrollmentTicket) (locking.Session, error) {
	return locking.Session{}, syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) CreateOwner(context.Context, locking.SessionID, locking.RequestID) (locking.Owner, error) {
	return locking.Owner{}, syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) RetireOwner(context.Context, locking.OwnerRef) error {
	return syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) CloseSession(context.Context, locking.SessionID) error {
	return syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) Resolve(context.Context, locking.OwnerRef, string) (locking.ResourceRef, error) {
	return locking.ResourceRef{}, syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) Acquire(context.Context, locking.AcquireRequest) (locking.ActionResult, error) {
	return locking.ActionResult{}, syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) Renew(context.Context, locking.RenewRequest) (locking.ActionResult, error) {
	return locking.ActionResult{}, syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) Release(context.Context, locking.OwnerRef, locking.GrantRef) (locking.ReleaseResult, error) {
	return locking.ReleaseResult{}, syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) Cancel(context.Context, locking.OwnerRef, locking.RequestID) (locking.CancelResult, error) {
	return locking.CancelResult{}, syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) QueryAction(context.Context, locking.OwnerRef, locking.RequestID) (locking.ActionResult, error) {
	return locking.ActionResult{}, syscall.EOPNOTSUPP
}
func (nativeUnsupportedLocks) QueryGrant(context.Context, locking.OwnerRef, locking.GrantRef) (locking.GrantStatus, error) {
	return locking.GrantStatus{}, syscall.EOPNOTSUPP
}
