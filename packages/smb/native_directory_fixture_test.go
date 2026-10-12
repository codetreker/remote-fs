package smb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// NewNativeDirectoryProbeBackend supplies a controlled, read-only authority for
// the native redirector probe. It intentionally uses no platform disk backend.
func NewNativeDirectoryProbeBackend() storage.FileStorage {
	instant := time.Unix(1700000000, 0).UTC()
	attr := func(id uint64, kind storage.NodeKind, size int64) storage.Attr {
		return storage.Attr{ID: id, Kind: kind, Size: size, AllocationKnown: true,
			BirthTime: &instant, ChangeTime: &instant, AccessTime: instant, ModTime: instant}
	}
	return &nativeDirectoryBackend{views: map[uint64][]storage.Entry{
		1: {
			{Name: "alpha.txt", Attr: attr(3, storage.NodeRegular, 0)},
			{Name: "beta.bin", Attr: attr(4, storage.NodeRegular, 0)},
			{Name: "folder", Attr: attr(2, storage.NodeDirectory, 0)},
			{Name: "long-😀-name.txt", Attr: attr(5, storage.NodeRegular, 0)},
		},
		2: {{Name: "nested.txt", Attr: attr(6, storage.NodeRegular, 0)}},
	}, root: attr(1, storage.NodeDirectory, 0)}
}

type nativeDirectoryBackend struct {
	endpointStorage
	views map[uint64][]storage.Entry
	root  storage.Attr
}

func (b *nativeDirectoryBackend) Stat(ctx context.Context, path string) (storage.Attr, error) {
	if err := ctx.Err(); err != nil {
		return storage.Attr{}, err
	}
	if path != "" {
		return storage.Attr{}, syscall.ENOENT
	}
	if err := storage.CheckAttrResultBudget(ctx, b.root, 6); err != nil {
		return storage.Attr{}, err
	}
	return b.root.Clone(), nil
}

func (b *nativeDirectoryBackend) NewFileSession(ctx context.Context, _ storage.FileSessionOptions) (storage.FileSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session := newEndpointFileSession()
	session.status.Epoch = fmt.Sprintf("native-directory-%d", b.sessionOpens.Add(1))
	return &nativeDirectorySession{endpointFileSession: session, backend: b,
		references: make(map[string]*nativeDirectoryReference)}, nil
}

type nativeDirectorySession struct {
	*endpointFileSession
	backend    *nativeDirectoryBackend
	refMu      sync.Mutex
	references map[string]*nativeDirectoryReference
	sequence   uint64
}

func (*nativeDirectorySession) CheckDirectoryRead() error { return nil }

func (s *nativeDirectorySession) directory(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (out storage.DirectoryObservation, returned error) {
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if err := target.Check(); err != nil {
		return out, err
	}
	if target.Scope != nil {
		s.refMu.Lock()
		ref := s.references[target.Scope.Token]
		valid := ref != nil && ref.id == target.NodeID && !ref.closed
		allowed := valid && ref.use.Uses&storage.ReadEntries != 0
		s.refMu.Unlock()
		if !valid {
			return out, storage.ErrInvalidScope
		}
		if !allowed {
			return out, syscall.EACCES
		}
	}
	entries, ok := s.backend.views[target.NodeID]
	if !ok {
		return out, syscall.ENOTDIR
	}
	for _, entry := range entries {
		reserved, err := result.Reserve(int64(len(entry.Name)), 6, entry.Attr)
		if err != nil {
			return out, err
		}
		if err := reserved.Commit(entry.Name, nil); err != nil {
			return out, err
		}
	}
	return storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte("native-probe-one")}, nil
}

func (s *nativeDirectorySession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	return s.directory(ctx, target, result)
}

func (s *nativeDirectorySession) ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	return storage.ObservedDirectory{}, syscall.ENOSYS
}

func (s *nativeDirectorySession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	if err := options.Check(); err != nil {
		result.Fail(err)
		return storage.DirectoryMetadataObservation{}, err
	}
	if options.IncludeName {
		result.Fail(syscall.ENOSYS)
		return storage.DirectoryMetadataObservation{}, syscall.ENOSYS
	}
	if err := s.checkGuards(options.Guards); err != nil {
		result.Fail(err)
		return storage.DirectoryMetadataObservation{}, err
	}
	observation, err := s.directory(ctx, target, result)
	return storage.DirectoryMetadataObservation{Observation: observation}, err
}

func (s *nativeDirectorySession) checkGuards(guards *storage.NamespaceGuards) error {
	if guards == nil {
		return nil
	}
	if guards.RootID != 0 && guards.RootID != 1 {
		return storage.ErrConditionConflict
	}
	for _, guard := range guards.Directories {
		if _, ok := s.backend.views[guard.ParentID]; !ok || string(guard.Revision) != "native-probe-one" {
			return storage.ErrConditionConflict
		}
	}
	for _, edge := range guards.Edges {
		found := false
		for _, entry := range s.backend.views[edge.ParentID] {
			if entry.Name == string(edge.RawLeaf) && entry.Attr.ID == edge.ChildID {
				found = true
			}
		}
		if !found {
			return storage.ErrConditionConflict
		}
	}
	return nil
}

func (s *nativeDirectorySession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if options.Create || options.Kind != storage.NodeDirectory || options.Target.State != storage.SameNode || options.Target.NodeID != id {
		return storage.NodeOpenResult{}, syscall.EACCES
	}
	attr := s.backend.root
	if id != attr.ID {
		found := false
		for _, entries := range s.backend.views {
			for _, entry := range entries {
				if entry.Attr.ID == id {
					attr, found = entry.Attr, true
				}
			}
		}
		if !found {
			return storage.NodeOpenResult{}, syscall.ENOENT
		}
	}
	if err := storage.CheckAttrResultBudget(ctx, attr, 6); err != nil {
		return storage.NodeOpenResult{}, err
	}
	if err := options.Action.Check(); err != nil {
		return storage.NodeOpenResult{}, err
	}
	s.refMu.Lock()
	defer s.refMu.Unlock()
	s.sequence++
	token := fmt.Sprintf("native-probe-%d", s.sequence)
	ref := &nativeDirectoryReference{session: s, id: id, attr: attr, token: token, metadata: options.MetadataAccess, use: options.Use}
	s.references[token] = ref
	return storage.NodeOpenResult{Reference: ref, Attr: attr.Clone(), Outcome: storage.Opened}, nil
}

func (s *nativeDirectorySession) OpenChildRef(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if err := s.checkGuards(selection.Guards); err != nil {
		return storage.NodeOpenResult{}, err
	}
	for _, entry := range s.backend.views[selection.Name.Parent.NodeID] {
		if entry.Name == string(selection.Name.RawLeaf) && entry.Attr.ID == options.Target.NodeID {
			return s.OpenNodeRef(ctx, entry.Attr.ID, options)
		}
	}
	return storage.NodeOpenResult{}, syscall.ENOENT
}

type nativeDirectoryReference struct {
	session  *nativeDirectorySession
	id       uint64
	attr     storage.Attr
	token    string
	metadata storage.MetadataPermissions
	use      storage.UseClaim
	closed   bool
	attempt  *storage.CloseAttempt
}

func (*nativeDirectoryReference) CheckReferenceState() error         { return nil }
func (*nativeDirectoryReference) CheckScopedReference() error        { return nil }
func (r *nativeDirectoryReference) ReferenceNodeID() (uint64, error) { return r.id, nil }
func (r *nativeDirectoryReference) Scope(ctx context.Context) (storage.UseScope, error) {
	r.session.refMu.Lock()
	defer r.session.refMu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.UseScope{}, err
	}
	if r.closed {
		return storage.UseScope{}, syscall.EBADF
	}
	return storage.UseScope{Token: r.token}, nil
}
func (r *nativeDirectoryReference) State(ctx context.Context, projection storage.StateProjection) (storage.ReferenceState, error) {
	r.session.refMu.Lock()
	defer r.session.refMu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.ReferenceState{}, err
	}
	if r.closed {
		return storage.ReferenceState{}, syscall.EBADF
	}
	if projection != storage.StateScalars && (projection != storage.StateFull || r.metadata&storage.ReadMetadata == 0) {
		return storage.ReferenceState{}, syscall.EACCES
	}
	attr := r.attr.Clone()
	if projection == storage.StateScalars {
		attr.BirthTime, attr.ChangeTime = nil, nil
		attr.AccessTime, attr.ModTime = time.Time{}, time.Time{}
	}
	return storage.ReferenceState{Attr: attr}, nil
}
func (r *nativeDirectoryReference) Stat(ctx context.Context) (storage.Attr, error) {
	state, err := r.State(ctx, storage.StateFull)
	return state.Attr, err
}
func (*nativeDirectoryReference) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, syscall.EACCES
}
func (r *nativeDirectoryReference) Close(ctx context.Context) error {
	_, err := r.CloseWithResult(ctx)
	return err
}
func (r *nativeDirectoryReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	r.session.refMu.Lock()
	defer r.session.refMu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	r.closed = true
	delete(r.session.references, r.token)
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}
func (r *nativeDirectoryReference) CloseOwnerStatus(ctx context.Context) (storage.CloseOwnerStatus, error) {
	r.session.refMu.Lock()
	defer r.session.refMu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.CloseOwnerStatus{}, err
	}
	return storage.CloseOwnerStatus{Ready: !r.closed, Released: r.closed, NextGeneration: 1, CurrentEpoch: 1}, nil
}
func (r *nativeDirectoryReference) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	if err := attempt.Check(); err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	r.session.refMu.Lock()
	if r.attempt != nil && *r.attempt != attempt {
		r.session.refMu.Unlock()
		return storage.ReferenceCloseResult{}, syscall.EINVAL
	}
	copy := attempt
	r.attempt = &copy
	r.session.refMu.Unlock()
	return r.CloseWithResult(ctx)
}
func (r *nativeDirectoryReference) QueryCloseAttempt(ctx context.Context, attempt storage.CloseAttempt) (storage.FileActionReceipt, error) {
	r.session.refMu.Lock()
	defer r.session.refMu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if r.attempt == nil || *r.attempt != attempt || !r.closed {
		return storage.FileActionReceipt{}, syscall.EINVAL
	}
	return storage.FileActionReceipt{Action: attempt.Action, Operation: storage.OpFileClose, Outcome: storage.FileActionCompleted}, nil
}

func TestNativeDirectoryProbeFixture(t *testing.T) {
	backend := NewNativeDirectoryProbeBackend()
	raw, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := raw.(*nativeDirectorySession)
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.OpenNodeRef(t.Context(), 1, storage.NodeRefOptions{Kind: storage.NodeDirectory,
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: 1}, Action: action, Use: storage.UseClaim{Uses: storage.ReadEntries}})
	if err != nil {
		t.Fatal(err)
	}
	ref := opened.Reference
	state, err := ref.State(t.Context(), storage.StateScalars)
	if err != nil || state.Attr.ID != 1 || state.Attr.BirthTime != nil || !state.Attr.ModTime.IsZero() {
		t.Fatalf("fixture scalar state=%+v error=%v", state, err)
	}
	if _, err := ref.Stat(t.Context()); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("fixture granted metadata read: %v", err)
	}
	scope, err := ref.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	collector := func() *storage.ListResult {
		t.Helper()
		result, err := storage.NewListResult(65536, 0, func(_ int, name, metadata int64, _ storage.Attr) (int64, error) {
			return storage.ObservedEntryBytes(name, metadata)
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	result := collector()
	if _, err := session.ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{NodeID: 1, Scope: &scope}, result); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 4 {
		t.Fatalf("fixture entries=%+v error=%v", entries, err)
	}
	result = collector()
	if _, err := session.ObserveDirectoryMetadata(t.Context(), storage.DirectoryTarget{NodeID: 1}, storage.DirectoryMetadataOptions{}, result); err != nil {
		t.Fatal(err)
	}
	if err := ref.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	result = collector()
	if _, err := session.ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{NodeID: 1, Scope: &scope}, result); !errors.Is(err, storage.ErrInvalidScope) {
		t.Fatalf("closed scope survived: %v", err)
	}
	if _, err := result.Entries(); err == nil {
		t.Fatal("failed capture retained a usable directory")
	}
}
