package fuse

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse/posix"
	"github.com/codetreker/remote-fs/packages/storage"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type directoryReferenceFixture struct {
	attr                   storage.Attr
	scope                  storage.UseScope
	closes, stats, updates int
	clean                  bool
	scopeErr               error
	statErr                error
	onStat                 func()
}

func (r *directoryReferenceFixture) Stat(context.Context) (storage.Attr, error) {
	if r.onStat != nil {
		r.onStat()
	}
	r.stats++
	if r.statErr != nil {
		return storage.Attr{}, r.statErr
	}
	if r.closes != 0 {
		return storage.Attr{}, syscall.EBADF
	}
	return r.attr, nil
}
func (r *directoryReferenceFixture) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (r *directoryReferenceFixture) Close(ctx context.Context) error {
	r.closes++
	_, deadline := ctx.Deadline()
	r.clean = ctx.Err() == nil && deadline
	return nil
}
func (r *directoryReferenceFixture) CheckScopedReference() error { return nil }
func (r *directoryReferenceFixture) Scope(context.Context) (storage.UseScope, error) {
	return r.scope, r.scopeErr
}
func (r *directoryReferenceFixture) CheckMetadataAccess() error { return nil }
func (r *directoryReferenceFixture) SetMetadata(_ context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	return storage.OpaquePayload{}, syscall.EBADF
}

func (s *directorySessionFixture) SetMetadata(_ context.Context, id uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	r := s.reference
	if id != r.attr.ID {
		return storage.OpaquePayload{}, syscall.ESTALE
	}
	s.mutations++
	if s.onMutation != nil {
		s.onMutation()
	}
	if namespace != posix.Namespace || len(version) != 0 || r.updates != 0 {
		return storage.OpaquePayload{}, syscall.EINVAL
	}
	r.updates++
	payload := storage.OpaquePayload{Version: []byte{1}, Data: append([]byte(nil), data...)}
	r.attr.Metadata = map[string]storage.OpaquePayload{namespace: payload}
	return payload, nil
}

type directorySessionFixture struct {
	*namespaceFixture
	reference    *directoryReferenceFixture
	opens, reads int
	mutations    int
	onMutation   func()
	metadataErr  error
}

func (s *directorySessionFixture) CheckMetadataAccess() error { return s.metadataErr }
func (s *directorySessionFixture) SetNodeAttr(_ context.Context, id uint64, c storage.AttrChange) (storage.Attr, error) {
	if id != s.reference.attr.ID {
		return storage.Attr{}, syscall.ESTALE
	}
	s.mutations++
	if s.onMutation != nil {
		s.onMutation()
	}
	if c.ModTime != nil {
		s.reference.attr.ModTime = *c.ModTime
	}
	return s.reference.attr, nil
}

func (s *directorySessionFixture) CheckNodeReferences() error { return nil }
func (s *directorySessionFixture) OpenNodeRef(_ context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if id != 7 || options.Kind != storage.NodeDirectory || options.Target.State != storage.SameNode || options.Target.NodeID != 7 || options.Use.Uses != storage.ReadEntries || options.Use.Deny != 0 || options.MetadataAccess != storage.ReadMetadata {
		return storage.NodeOpenResult{}, syscall.EINVAL
	}
	s.opens++
	return storage.NodeOpenResult{Reference: s.reference, Attr: s.reference.attr, Outcome: storage.Opened}, nil
}
func (s *directorySessionFixture) OpenChildRef(context.Context, storage.ChildName, storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	panic("directory open used a stale path")
}
func (s *directorySessionFixture) ReadDirNode(_ context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	if target.NodeID != 7 || target.Scope == nil || *target.Scope != s.reference.scope || s.reference.closes != 0 {
		return storage.ObservedDirectory{}, syscall.EBADF
	}
	s.reads++
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: 7, Revision: []byte{1}}, Entries: []storage.ObservedEntry{{RawLeaf: []byte("child"), Attr: storage.Attr{ID: 8, Kind: storage.NodeRegular}}}}, nil
}

func TestDirectoryHandleKeepsReferenceScopeThroughReadsAttributesAndClose(t *testing.T) {
	namespace := &namespaceFixture{}
	root := namespaceRoot(namespace)
	reference := &directoryReferenceFixture{attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "opened-directory"}}
	session := &directorySessionFixture{namespaceFixture: namespace, reference: reference}
	root.volume.files = session
	namespace.lookup = func(name storage.ChildName) (storage.Attr, error) {
		if name.Parent.NodeID != 7 || name.Parent.Scope == nil || *name.Parent.Scope != reference.scope {
			return storage.Attr{}, syscall.EBADF
		}
		return storage.Attr{ID: 8, Kind: storage.NodeRegular}, nil
	}
	opened, _, errno := root.OpendirHandle(t.Context(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatal(errno)
	}
	handle := opened.(*directoryHandle)
	entry, errno := handle.Readdirent(t.Context())
	if errno != 0 || entry == nil || entry.Name != "child" || session.reads != 1 {
		t.Fatalf("readdir=%+v %v reads=%d", entry, errno, session.reads)
	}
	if _, errno := handle.Lookup(t.Context(), "child", &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
	if errno := handle.Seekdir(t.Context(), 0); errno != 0 {
		t.Fatal(errno)
	}
	if entry, errno := handle.Readdirent(t.Context()); errno != 0 || entry == nil || session.reads != 1 {
		t.Fatal("seek lost captured directory snapshot")
	}
	var out gofuse.AttrOut
	if errno := root.Getattr(t.Context(), handle, &out); errno != 0 || reference.stats != 1 {
		t.Fatalf("directory getattr=%v stats=%d", errno, reference.stats)
	}
	if errno := root.Setattr(t.Context(), handle, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: gofuse.FATTR_MODE, Mode: 0700}}, &out); errno != 0 || reference.updates != 1 || out.Mode&0777 != 0700 {
		t.Fatalf("directory chmod=%v updates=%d mode=%o", errno, reference.updates, out.Mode)
	}
	modified := time.Unix(123, 0)
	if _, err := handle.setAttr(t.Context(), storage.AttrChange{ModTime: &modified}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	handle.Releasedir(ctx, 0)
	handle.Releasedir(ctx, 0)
	if reference.closes != 1 || !reference.clean || session.opens != 1 {
		t.Fatalf("release closes=%d clean=%v opens=%d", reference.closes, reference.clean, session.opens)
	}
	if _, errno := handle.Readdirent(t.Context()); errno != syscall.EBADF {
		t.Fatal(errno)
	}
	if errno := root.Getattr(t.Context(), handle, &out); errno != syscall.EBADF {
		t.Fatal(errno)
	}
}

func directoryMutationFixture(t *testing.T) (*directoryHandle, *directorySessionFixture) {
	t.Helper()
	reference := &directoryReferenceFixture{attr: storage.Attr{ID: 7, Kind: storage.NodeDirectory}, scope: storage.UseScope{Token: "directory"}}
	session := &directorySessionFixture{namespaceFixture: &namespaceFixture{}, reference: reference}
	root := namespaceRoot(session.namespaceFixture)
	root.volume.files = session
	opened, _, errno := root.OpendirHandle(t.Context(), syscall.O_RDONLY)
	if errno != 0 {
		t.Fatal(errno)
	}
	return opened.(*directoryHandle), session
}

func TestDirectoryMetadataRejectsInactiveScopeBeforeIdentityMutation(t *testing.T) {
	for _, mutation := range []string{"attributes", "permissions"} {
		for _, state := range []string{"closed", "expired", "scope error", "scope mismatch", "cancelled"} {
			t.Run(mutation+"/"+state, func(t *testing.T) {
				d, session := directoryMutationFixture(t)
				ctx := t.Context()
				want := error(syscall.EBADF)
				switch state {
				case "closed":
					d.Releasedir(ctx, 0)
				case "expired":
					d.node.volume.deadline = time.Now().Add(-time.Second)
					want = syscall.ESTALE
				case "scope error":
					session.reference.scopeErr = storage.ErrInvalidScope
					want = storage.ErrInvalidScope
				case "scope mismatch":
					session.reference.scope.Token = "other reference"
					want = storage.ErrInvalidScope
				case "cancelled":
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
					want = context.Canceled
				}
				var err error
				if mutation == "attributes" {
					stamp := time.Unix(123, 0)
					_, err = d.setAttr(ctx, storage.AttrChange{ModTime: &stamp})
				} else {
					err = d.node.setPermissions(ctx, d, 0700)
				}
				if !errors.Is(err, want) || session.mutations != 0 {
					t.Fatalf("mutation=%v want=%v dispatched=%d", err, want, session.mutations)
				}
			})
		}
	}
}

func TestDirectoryMetadataHoldsReferenceThroughMutation(t *testing.T) {
	for _, mutation := range []string{"attributes", "permissions"} {
		t.Run(mutation, func(t *testing.T) {
			d, session := directoryMutationFixture(t)
			entered, release := make(chan struct{}), make(chan struct{})
			unlocked := make(chan string, 2)
			checkLocked := func(stage string) {
				if d.mu.TryLock() {
					d.mu.Unlock()
					unlocked <- stage
				}
			}
			session.reference.onStat = func() { checkLocked("metadata version read") }
			session.onMutation = func() {
				checkLocked("identity mutation")
				close(entered)
				<-release
			}
			result := make(chan error, 1)
			go func() {
				if mutation == "permissions" {
					result <- d.node.setPermissions(t.Context(), d, 0700)
				} else {
					stamp := time.Unix(123, 0)
					_, err := d.setAttr(t.Context(), storage.AttrChange{ModTime: &stamp})
					result <- err
				}
			}()
			<-entered
			closing, closed := make(chan struct{}), make(chan struct{})
			go func() {
				close(closing)
				d.Releasedir(t.Context(), 0)
				close(closed)
			}()
			<-closing
			close(release)
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			<-closed
			select {
			case stage := <-unlocked:
				t.Fatalf("directory release mutex was not held across %s", stage)
			default:
			}
			if session.reference.closes != 1 || session.mutations != 1 {
				t.Fatalf("closes=%d mutations=%d", session.reference.closes, session.mutations)
			}
		})
	}
}

func TestDirectoryMetadataRequiresSessionCapabilityAndCheckedAttributes(t *testing.T) {
	for _, failure := range []string{"unsupported", "capability failure", "stat failure", "wrong identity"} {
		t.Run(failure, func(t *testing.T) {
			d, session := directoryMutationFixture(t)
			want := error(syscall.EIO)
			switch failure {
			case "unsupported":
				d.node.volume.files = &struct{ storage.FileSession }{session}
				want = syscall.EOPNOTSUPP
			case "capability failure":
				session.metadataErr = errors.New("metadata authority unavailable")
				want = session.metadataErr
			case "stat failure":
				session.reference.statErr = errors.New("reference attributes unavailable")
				want = session.reference.statErr
			case "wrong identity":
				session.reference.attr.ID++
			}
			if err := d.node.setPermissions(t.Context(), d, 0700); !errors.Is(err, want) || session.mutations != 0 {
				t.Fatalf("chmod=%v want=%v mutations=%d", err, want, session.mutations)
			}
		})
	}
}
