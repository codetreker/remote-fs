package windows

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// This authority bounds the native kernel/HTTP acceptance workload. It stores
// generic metadata and exact names; platform interpretation belongs to SMB.
const nativeMaxFileBytes = 1 << 20

type nativeAuthority struct {
	mu                                                  sync.Mutex
	identity                                            string
	nodes                                               map[uint64]*nativeNode
	nextNode, nextEntry, nextRef, nextSession, nextWait uint64
	sessions                                            []*nativeSession
	changes                                             []metastore.Change
	access                                              *fileaccess.Coordinator
}
type nativeNode struct {
	parent         uint64
	name           string
	entry          storage.EntryID
	attr           storage.Attr
	data, link     []byte
	detached       bool
	state          storage.EntryState
	generation     storage.DrainGeneration
	drainCondition storage.RemovalCondition
}

var _ storage.FileStorage = (*nativeAuthority)(nil)
var _ metastore.Log = (*nativeAuthority)(nil)

func newNativeAuthority() *nativeAuthority {
	now := time.Now().UTC()
	var identity [16]byte
	rand.Read(identity[:])
	limits := fileaccess.DefaultLimits()
	limits.MaxClaims = 256
	limits.MaxOwners = 256
	limits.MaxRanges = 1024
	limits.MaxOwnerRanges = 1024
	limits.MaxSetRanges = 1024
	access, err := fileaccess.New(limits)
	if err != nil {
		panic(err)
	}
	root := &nativeNode{attr: storage.Attr{ID: 1, Kind: storage.NodeDirectory, AccessTime: now, ModTime: now, CreationTime: &now, ChangeTime: &now, MetadataRevision: 1, DirectoryRevision: 1}, state: storage.EntryActive}
	return &nativeAuthority{identity: fmt.Sprintf("%x", identity), nodes: map[uint64]*nativeNode{1: root}, nextNode: 1, access: access}
}
func (*nativeAuthority) CheckBounded() error     { return nil }
func (*nativeAuthority) CheckFileStorage() error { return nil }
func (a *nativeAuthority) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	return storage.FileVolumeState{VolumeIdentity: a.identity, RootID: 1, MaxEventBytes: metastore.MaxNotificationBytes}, ctx.Err()
}
func nativeAttr(n *nativeNode) storage.Attr {
	a := n.attr
	a.Metadata = a.Metadata.Clone()
	if a.CreationTime != nil {
		v := *a.CreationTime
		a.CreationTime = &v
	}
	if a.ChangeTime != nil {
		v := *a.ChangeTime
		a.ChangeTime = &v
	}
	return a
}
func (a *nativeAuthority) child(parent uint64, name string) *nativeNode {
	for _, n := range a.nodes {
		if !n.detached && n.parent == parent && n.name == name {
			return n
		}
	}
	return nil
}
func (a *nativeAuthority) lookup(p string) (*nativeNode, error) {
	clean, err := storage.CleanPath(p)
	if err != nil {
		return nil, err
	}
	n := a.nodes[1]
	if clean == "" {
		return n, nil
	}
	for _, name := range strings.Split(clean, "/") {
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
func (a *nativeAuthority) location(n *nativeNode) storage.EntryLocation {
	location := storage.EntryLocation{RootNodeID: 1, NodeID: n.attr.ID, State: storage.LocationLinked}
	if n.attr.ID == 1 {
		location.State = storage.LocationRoot
		return location
	}
	if n.detached {
		location.State = storage.LocationDetached
		return location
	}
	for current := n; current.attr.ID != 1; current = a.nodes[current.parent] {
		parent := a.nodes[current.parent]
		location.Ancestors = append(location.Ancestors, storage.EntryCondition{ParentID: parent.attr.ID, DirectoryRevision: parent.attr.DirectoryRevision, EntryID: current.entry, NodeID: current.attr.ID, Name: []byte(current.name)})
	}
	slices.Reverse(location.Ancestors)
	return location
}
func (a *nativeAuthority) removal(n *nativeNode) storage.RemovalStatus {
	r := storage.RemovalStatus{EntryID: n.entry, State: n.state, Generation: n.generation}
	if n.state == storage.EntryDraining {
		r.DrainCondition = n.drainCondition
	}
	return r
}
func (a *nativeAuthority) observation(n *nativeNode) storage.FileObservation {
	return a.observe(n, storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
}
func (a *nativeAuthority) observe(n *nativeNode, o storage.ObservationOptions) storage.FileObservation {
	v := storage.FileObservation{Attr: nativeAttr(n), Removal: a.removal(n)}
	if o.IncludeLocation {
		location := a.location(n)
		v.Location = &location
	}
	if o.IncludeLinkTarget {
		v.LinkTarget = bytes.Clone(n.link)
	}
	return v
}
func nativeMeta(n *nativeNode) metastore.Node {
	a := nativeAttr(n)
	return metastore.Node{ID: int64(a.ID), Kind: a.Kind, Size: a.Size, AccessTime: a.AccessTime, ModTime: a.ModTime, CreationTime: a.CreationTime, ChangeTime: a.ChangeTime, MetadataRevision: a.MetadataRevision, DirectoryRevision: a.DirectoryRevision, Metadata: a.Metadata, LinkTarget: bytes.Clone(n.link)}
}
func (a *nativeAuthority) image(n *nativeNode) *metastore.EventImage {
	return &metastore.EventImage{Attr: nativeAttr(n), Location: a.location(n), LinkTarget: bytes.Clone(n.link)}
}
func (a *nativeAuthority) position() metastore.Position {
	if len(a.changes) == 0 {
		return 0
	}
	return a.changes[len(a.changes)-1].Position
}
func (a *nativeAuthority) record(n *nativeNode, kind metastore.ChangeKind, mask metastore.ChangeMask, before *metastore.EventImage) {
	node := nativeMeta(n)
	if kind == metastore.Modified {
		old := nativeMeta(&nativeNode{attr: before.Attr, link: before.LinkTarget})
		mask = metastore.NotificationMask(old, node) | (mask & metastore.ChangeContent)
	}
	after := a.image(n)
	c := metastore.Change{Position: a.position() + 1, Kind: kind, Parent: int64(n.parent), Name: []byte(n.name), Node: &node, Notification: &metastore.Notification{SubjectID: int64(n.attr.ID), SubjectKind: n.attr.Kind, ChangeMask: mask, Before: before, After: after}}
	if n.detached {
		c.Parent = 0
		c.Name = nil
	}
	if kind == metastore.Removed {
		c.Node = nil
		c.Notification.After = nil
	}
	if kind == metastore.Renamed {
		at := before.Location.Ancestors[len(before.Location.Ancestors)-1]
		c.From = &metastore.Location{Parent: int64(at.ParentID), Name: bytes.Clone(at.Name)}
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
	return nativeAttr(n), nil
}
func (a *nativeAuthority) Read(ctx context.Context, p string) ([]byte, error) {
	return a.ReadBounded(ctx, p, nativeMaxFileBytes)
}
func (a *nativeAuthority) ReadBounded(ctx context.Context, p string, maximum int64) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := a.expireSessions(); err != nil {
		return nil, err
	}
	n, err := a.lookup(p)
	if err != nil {
		return nil, err
	}
	if n.attr.Kind != storage.NodeRegular {
		return nil, syscall.EISDIR
	}
	if int64(len(n.data)) > maximum {
		return nil, syscall.EFBIG
	}
	if err := a.use(n, 0, storage.ReadContent); err != nil {
		return nil, err
	}
	if err := a.access.CheckIO(n.attr.ID, nil, fileaccess.Span{Start: 0, End: uint64(max(len(n.data), 1) - 1), Boundary: len(n.data) == 0}, false); err != nil {
		return nil, nativeRangeError(err)
	}
	return bytes.Clone(n.data), nil
}
func (a *nativeAuthority) List(ctx context.Context, p string) ([]storage.Entry, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	n, err := a.lookup(p)
	if err != nil {
		return nil, err
	}
	if !n.attr.IsDir() {
		return nil, syscall.ENOTDIR
	}
	entries := []storage.Entry{}
	for _, child := range a.nodes {
		if !child.detached && child.parent == n.attr.ID {
			entries = append(entries, storage.Entry{Name: child.name, Attr: nativeAttr(child)})
		}
	}
	slices.SortFunc(entries, func(a, b storage.Entry) int { return strings.Compare(a.Name, b.Name) })
	return entries, nil
}
func (a *nativeAuthority) ListBounded(ctx context.Context, p string, result *storage.ListResult) error {
	entries, err := a.List(ctx, p)
	if err != nil {
		return result.Fail(err)
	}
	for _, entry := range entries {
		if err := result.Add(entry); err != nil {
			return result.Fail(err)
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
	return
}
func (a *nativeAuthority) openReferences() int { _, refs := a.activeState(); return refs }

func (a *nativeAuthority) Incarnation(ctx context.Context, max int64) (metastore.Incarnation, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if int64(len(a.identity)) > max {
		return "", syscall.EFBIG
	}
	return metastore.Incarnation(a.identity), nil
}
func (a *nativeAuthority) Barrier(ctx context.Context, max int64) (metastore.LogBarrier, error) {
	id, err := a.Incarnation(ctx, max)
	if err != nil {
		return metastore.LogBarrier{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return metastore.LogBarrier{Incarnation: id, Position: a.position()}, nil
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
	if limit < 0 || after < 0 {
		return retention, result.Fail(syscall.EINVAL)
	}
	count := 0
	for _, change := range a.changes {
		if change.Position <= after {
			continue
		}
		if count >= limit {
			break
		}
		encoded, err := metastore.EncodeNotification(change)
		if err != nil {
			return retention, result.Fail(err)
		}
		lengths := metastore.ChangePayloadLengths{Name: int64(len(change.Name)), Notification: int64(len(encoded))}
		meta := change
		meta.Name = nil
		meta.Notification = nil
		var from, metadata, target []byte
		var content metastore.Key
		if change.From != nil {
			from = change.From.Name
			lengths.FromName = int64(len(from))
			copy := *change.From
			copy.Name = nil
			meta.From = &copy
		}
		if change.Node != nil {
			node := change.Node.Clone()
			content = node.Content
			target = node.LinkTarget
			metadata, err = storage.EncodeMetadata(node.Metadata)
			if err != nil {
				return retention, result.Fail(err)
			}
			lengths.Content = int64(len(content))
			lengths.Target = int64(len(target))
			lengths.Metadata = int64(len(metadata))
			node.Content = ""
			node.LinkTarget = nil
			node.Metadata = nil
			meta.Node = &node
		}
		reservation, fits, err := result.Reserve(meta, lengths)
		if err != nil {
			return retention, result.Fail(err)
		}
		if !fits {
			break
		}
		if err := reservation.Commit(change.Name, from, content, metadata, target, encoded); err != nil {
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
	rows := []metastore.Row{}
	for _, n := range a.nodes {
		if !n.detached {
			rows = append(rows, metastore.Row{EntryID: n.entry, Parent: int64(n.parent), Name: []byte(n.name), Node: nativeMeta(n)})
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
		metadata, err := storage.EncodeMetadata(row.Node.Metadata)
		if err != nil {
			return false, result.Fail(err)
		}
		meta := row
		meta.Name = nil
		meta.Node.Content = ""
		meta.Node.Metadata = nil
		meta.Node.LinkTarget = nil
		reservation, fits, err := result.Reserve(meta, metastore.RowPayloadLengths{Name: int64(len(row.Name)), Content: int64(len(row.Node.Content)), Metadata: int64(len(metadata)), Target: int64(len(row.Node.LinkTarget))})
		if err != nil {
			return false, result.Fail(err)
		}
		if !fits {
			break
		}
		if err := reservation.Commit(row.Name, row.Node.Content, metadata, row.Node.LinkTarget); err != nil {
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

func (a *nativeAuthority) expireSessions() error {
	for _, s := range a.sessions {
		if !s.closed && !time.Now().Before(s.expires) {
			if err := s.retire(); err != nil {
				return err
			}
		}
	}
	return nil
}
