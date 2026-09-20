package fuse

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"strings"
	"syscall"
	"testing"
	"time"

	fsbridge "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/fuse/posix"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

type mutationStorage struct {
	storage.FileStorage
	before func(context.Context, string) error
	after  func(string)
}

func (s *mutationStorage) enter(ctx context.Context, op string) error {
	if s.before != nil {
		return s.before(ctx, op)
	}
	return nil
}

func (s *mutationStorage) finish(op string, err error) error {
	if err == nil && s.after != nil {
		s.after(op)
	}
	return err
}

func (s *mutationStorage) Mkdir(ctx context.Context, path string) error {
	if err := s.enter(ctx, "mkdir"); err != nil {
		return err
	}
	return s.finish("mkdir", s.FileStorage.Mkdir(ctx, path))
}

func (s *mutationStorage) SetAttr(ctx context.Context, path string, change storage.AttrChange) error {
	if err := s.enter(ctx, "setattr"); err != nil {
		return err
	}
	return s.finish("setattr", s.FileStorage.SetAttr(ctx, path, change))
}

func (s *mutationStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	if err := s.enter(ctx, "stat"); err != nil {
		return storage.Attr{}, err
	}
	attr, err := s.FileStorage.Stat(ctx, path)
	return attr, s.finish("stat", err)
}

func (s *mutationStorage) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	session, err := s.FileStorage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &mutationSession{capableTestSession: testSessionCapabilities(session), owner: s}, nil
}

type mutationSession struct {
	capableTestSession
	owner *mutationStorage
}

func (s *mutationSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	if err := s.owner.enter(ctx, "stat"); err != nil {
		return storage.Attr{}, err
	}
	attr, err := s.NamespaceAccess.LookupAt(ctx, name)
	return attr, s.owner.finish("stat", err)
}

func (s *mutationSession) OpenAt(ctx context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	if err := s.owner.enter(ctx, "open"); err != nil {
		return storage.OpenResult{}, err
	}
	result, err := s.AtomicFileOpener.OpenAt(ctx, name, options)
	if err := s.owner.finish("open", err); err != nil {
		return result, err
	}
	if result.File != nil {
		result.File = &mutationFile{File: result.File, owner: s.owner}
	}
	if err := s.owner.enter(ctx, "open-result"); err != nil {
		return result, err
	}
	s.owner.finish("stat", nil)
	return result, nil
}

func (s *mutationSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	op := map[storage.NameOperation]string{
		storage.NameMkdir: "mkdir", storage.NameSymlink: "symlink", storage.NameRemove: "remove",
		storage.NameRemoveDir: "rmdir", storage.NameRename: "rename",
	}[command.Kind]
	if err := s.owner.enter(ctx, op); err != nil {
		return storage.NameResult{}, err
	}
	result, err := s.NamespaceAccess.MutateName(ctx, command)
	if err := s.owner.finish(op, err); err != nil {
		return result, err
	}
	if err := s.owner.enter(ctx, op+"-result"); err != nil {
		return result, err
	}
	s.owner.finish("stat", nil)
	return result, nil
}

func (s *mutationSession) OpenFile(ctx context.Context, path string, options storage.FileOpenOptions) (storage.File, error) {
	if err := s.owner.enter(ctx, "open"); err != nil {
		return nil, err
	}
	file, err := s.FileSession.OpenFile(ctx, path, options)
	if err := s.owner.finish("open", err); err != nil {
		return nil, err
	}
	return &mutationFile{File: file, owner: s.owner}, nil
}

func (s *mutationSession) OpenNode(ctx context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	if err := s.owner.enter(ctx, "open-node"); err != nil {
		return nil, err
	}
	file, err := s.FileSession.OpenNode(ctx, id, options)
	if err := s.owner.finish("open-node", err); err != nil {
		return nil, err
	}
	return &mutationFile{File: file, owner: s.owner}, nil
}

func (s *mutationSession) StatNode(ctx context.Context, id uint64) (storage.Attr, error) {
	if err := s.owner.enter(ctx, "stat"); err != nil {
		return storage.Attr{}, err
	}
	attr, err := s.FileSession.StatNode(ctx, id)
	return attr, s.owner.finish("stat", err)
}

func (s *mutationSession) SetNodeAttr(ctx context.Context, id uint64, change storage.AttrChange) (storage.Attr, error) {
	if err := s.owner.enter(ctx, "setattr"); err != nil {
		return storage.Attr{}, err
	}
	attr, err := s.FileSession.SetNodeAttr(ctx, id, change)
	return attr, s.owner.finish("setattr", err)
}

func (s *mutationSession) SetMetadata(ctx context.Context, id uint64, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	if err := s.owner.enter(ctx, "setattr"); err != nil {
		return storage.OpaquePayload{}, err
	}
	result, err := s.MetadataAccess.SetMetadata(ctx, id, namespace, version, data)
	return result, s.owner.finish("setattr", err)
}

type mutationFile struct {
	storage.File
	owner *mutationStorage
}

func (f *mutationFile) CheckScopedReference() error {
	return f.File.(storage.ScopedReference).CheckScopedReference()
}

func (f *mutationFile) Scope(ctx context.Context) (storage.UseScope, error) {
	return f.File.(storage.ScopedReference).Scope(ctx)
}

func (f *mutationFile) CheckMetadataAccess() error {
	return f.File.(storage.ReferenceMetadataAccess).CheckMetadataAccess()
}

func (f *mutationFile) SetMetadata(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	if err := f.owner.enter(ctx, "setattr"); err != nil {
		return storage.OpaquePayload{}, err
	}
	result, err := f.File.(storage.ReferenceMetadataAccess).SetMetadata(ctx, namespace, version, data)
	return result, f.owner.finish("setattr", err)
}

func (f *mutationFile) Stat(ctx context.Context) (storage.Attr, error) {
	if err := f.owner.enter(ctx, "stat"); err != nil {
		return storage.Attr{}, err
	}
	attr, err := f.File.Stat(ctx)
	return attr, f.owner.finish("stat", err)
}

func (f *mutationFile) Truncate(ctx context.Context, size int64) (storage.Attr, error) {
	if err := f.owner.enter(ctx, "truncate"); err != nil {
		return storage.Attr{}, err
	}
	attr, err := f.File.Truncate(ctx, size)
	return attr, f.owner.finish("truncate", err)
}

func (f *mutationFile) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	if err := f.owner.enter(ctx, "write"); err != nil {
		return storage.Attr{}, err
	}
	attr, err := f.File.WriteAt(ctx, offset, data)
	return attr, f.owner.finish("write", err)
}

func (f *mutationFile) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	if err := f.owner.enter(ctx, "setattr"); err != nil {
		return storage.Attr{}, err
	}
	attr, err := f.File.SetAttr(ctx, change)
	return attr, f.owner.finish("setattr", err)
}

func (f *mutationFile) Close(ctx context.Context) error {
	if err := f.owner.enter(ctx, "close"); err != nil {
		return err
	}
	return f.owner.finish("close", f.File.Close(ctx))
}

func mutationTree(t *testing.T) (*node, *node, *mutationStorage) {
	t.Helper()
	_, local := memoryfixture.New(t, "mutation", 0, locking.DefaultOptions())
	if err := local.Write(t.Context(), "f", []byte("contents")); err != nil {
		t.Fatal(err)
	}
	initial, err := local.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	original, err := local.Stat(t.Context(), "f")
	if err != nil {
		t.Fatal(err)
	}
	data, err := posix.Encode(0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := initial.(storage.MetadataAccess).SetMetadata(t.Context(), original.ID, posix.Namespace, original.Metadata[posix.Namespace].Version, data); err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	downstream := &mutationStorage{FileStorage: local}
	session, err := downstream.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Errorf("close mutation session: %v", err)
		}
	})
	attr, err := local.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	v := activeTestVolume(downstream, 1024)
	v.files = session
	root := &node{volume: v, id: rootIdentity(attr.ID)}
	fsbridge.NewNodeFS(root, &fsbridge.Options{})
	child, errno := root.Lookup(t.Context(), "f", &gofuse.EntryOut{})
	if errno != 0 {
		t.Fatal(errno)
	}
	if !root.AddChild("f", child, false) {
		t.Fatal("attaching the file inode")
	}
	return root, child.Operations().(*node), downstream
}

func mutationHandle(t *testing.T, n *node) *handle {
	t.Helper()
	file, err := n.volume.files.OpenNode(t.Context(), n.id.node, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	return newHandle(n, file, true, true)
}

func TestCreationCancellationAccountsForCompletedStages(t *testing.T) {
	for _, test := range []struct {
		name, failedOp     string
		directory, changed bool
	}{
		{"atomic file open", "open", false, false},
		{"captured file result", "open-result", false, true},
		{"directory creation", "mkdir", true, false},
		{"captured directory result", "mkdir-result", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, _, downstream := mutationTree(t)
			downstream.before = func(_ context.Context, op string) error {
				if op == test.failedOp {
					return context.Canceled
				}
				return nil
			}
			var errno syscall.Errno
			if test.directory {
				_, errno = root.Mkdir(t.Context(), "new", 0700, &gofuse.EntryOut{})
			} else {
				_, _, _, errno = root.Create(t.Context(), "new", syscall.O_RDWR, 0600, &gofuse.EntryOut{})
			}
			want := syscall.EINTR
			if test.changed {
				want = syscall.EIO
			}
			if errno != want {
				t.Fatalf("creation returned %v, want %v", errno, want)
			}
			attr, err := downstream.FileStorage.Stat(t.Context(), "new")
			if test.changed && err != nil || !test.changed && !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("volume after interrupted creation: %v", err)
			}
			if test.changed {
				mode, modeErr := permissions(attr)
				want := iofs.FileMode(0600)
				if test.directory {
					want = 0700
				}
				if modeErr != nil || mode.Perm() != want {
					t.Fatalf("atomic create left metadata %v instead of requested %04o: %v", attr.Metadata, want, modeErr)
				}
			}
		})
	}
}

func TestSetattrCancellationAccountsForCompletedStages(t *testing.T) {
	for _, withHandle := range []bool{false, true} {
		for _, test := range []struct {
			name, failedOp string
			valid          uint32
			want           syscall.Errno
			truncates      int
		}{
			{"truncate refused", "truncate", gofuse.FATTR_SIZE, syscall.EINTR, 0},
			{"truncate then attributes", "stat", gofuse.FATTR_SIZE, syscall.EIO, 1},
			{"truncate then mode", "setattr", gofuse.FATTR_SIZE | gofuse.FATTR_MODE, syscall.EIO, 1},
			{"mode refused", "setattr", gofuse.FATTR_MODE, syscall.EIO, 0},
			{"mode then attributes", "stat", gofuse.FATTR_MODE, syscall.EIO, 0},
		} {
			t.Run(fmt.Sprintf("%s/handle=%v", test.name, withHandle), func(t *testing.T) {
				_, n, downstream := mutationTree(t)
				var file fsbridge.FileHandle
				if withHandle {
					file = mutationHandle(t, n)
				}
				truncates, setters := 0, 0
				downstream.before = func(_ context.Context, op string) error {
					if op == test.failedOp && (op != "stat" || test.valid&gofuse.FATTR_SIZE != 0 && truncates != 0 || test.valid&gofuse.FATTR_SIZE == 0 && setters != 0) {
						return context.Canceled
					}
					return nil
				}
				downstream.after = func(op string) {
					if op == "truncate" {
						truncates++
					}
					if op == "setattr" {
						setters++
					}
				}
				err := n.setattr(t.Context(), file, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
					Valid: test.valid, Size: 3, Mode: 0640,
				}}, &gofuse.AttrOut{})
				if errnoOf(err) != test.want || !errors.Is(err, context.Canceled) || truncates != test.truncates {
					t.Fatalf("setattr returned %v, truncates=%d; want %v and %d", err, truncates, test.want, test.truncates)
				}
				body, err := downstream.FileStorage.Read(t.Context(), "f")
				want := "contents"
				if test.truncates != 0 {
					want = "con"
				}
				if err != nil || string(body) != want {
					t.Fatalf("completed stages left %q, %v; want %q", body, err, want)
				}
			})
		}
	}
}

func TestSetattrCancellationBetweenSuccessfulMutationStages(t *testing.T) {
	for _, stage := range []string{"truncate", "setattr"} {
		t.Run(stage, func(t *testing.T) {
			_, n, downstream := mutationTree(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			setters := 0
			downstream.before = func(ctx context.Context, op string) error {
				if op == "setattr" {
					setters++
				}
				return ctx.Err()
			}
			downstream.after = func(op string) {
				if op == stage {
					cancel()
				}
			}
			in := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid: gofuse.FATTR_SIZE | gofuse.FATTR_MODE, Size: 3, Mode: 0640,
			}}
			if errno := n.Setattr(ctx, nil, in, &gofuse.AttrOut{}); errno != syscall.EIO {
				t.Fatalf("setattr canceled after %s returned %v, want EIO", stage, errno)
			}
			if stage == "truncate" && setters != 0 || stage == "setattr" && setters != 1 {
				t.Fatalf("setattr canceled after %s attempted %d attribute changes", stage, setters)
			}
		})
	}
}

func TestWriteFailureIsReportedBeforeAnAttributeChange(t *testing.T) {
	_, n, downstream := mutationTree(t)
	first, second := mutationHandle(t, n), mutationHandle(t, n)
	writes := 0
	downstream.after = func(op string) {
		if op == "write" {
			writes++
		}
	}
	if n, errno := first.Write(t.Context(), []byte("complete"), 0); errno != 0 || n != 8 {
		t.Fatalf("first write: %d, %v", n, errno)
	}
	downstream.before = func(_ context.Context, op string) error {
		if op == "write" {
			return context.Canceled
		}
		return nil
	}
	if n, errno := second.Write(t.Context(), []byte("rejected"), 0); errno != syscall.EINTR || n != 0 {
		t.Fatalf("refused write: %d, %v", n, errno)
	}
	if errno := n.Setattr(t.Context(), first, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		Valid: gofuse.FATTR_MTIME, Mtime: 123456789,
	}}, &gofuse.AttrOut{}); errno != 0 {
		t.Fatal(errno)
	}
	if writes != 1 {
		t.Fatalf("attribute change submitted another write: %d writes", writes)
	}
	if body, err := downstream.FileStorage.Read(t.Context(), "f"); err != nil || string(body) != "complete" {
		t.Fatalf("write outcomes left %q, %v", body, err)
	}
}

func TestMutationClassificationPreservesIndependentFailures(t *testing.T) {
	fault := errors.New("volume unavailable")
	for _, cause := range []error{syscall.EACCES, fault, errors.Join(context.Canceled, fault), errors.Join(fault, context.Canceled), context.DeadlineExceeded} {
		if got := afterMutation(true, cause); got != cause {
			t.Errorf("mutation changed independent failure %v to %v", cause, got)
		}
	}
	cause := fmt.Errorf("reading attributes after resize: %w", context.Canceled)
	err := afterMutation(true, cause)
	if errnoOf(err) != syscall.EIO || !errors.Is(err, cause) || !errors.Is(err, context.Canceled) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("partial mutation lost its cause or classification: %v", err)
	}
	for _, detail := range []string{"after a change", cause.Error(), syscall.EIO.Error()} {
		if !strings.Contains(err.Error(), detail) {
			t.Errorf("partial mutation diagnostic omits %q: %q", detail, err.Error())
		}
	}
	conflict := afterMutation(true, storage.ErrConditionConflict)
	if errnoOf(conflict) != syscall.EIO || !errors.Is(conflict, storage.ErrConditionConflict) {
		t.Fatalf("partial condition conflict remained retryable: %v", conflict)
	}
}

func TestMutationSuccessIgnoresLateCancellation(t *testing.T) {
	for _, operation := range []string{"create", "mkdir", "setattr"} {
		t.Run(operation, func(t *testing.T) {
			root, n, downstream := mutationTree(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			observations := 0
			downstream.after = func(op string) {
				if op == "stat" {
					observations++
					if operation == "setattr" && observations == 1 {
						return
					}
					cancel()
				}
			}
			var errno syscall.Errno
			switch operation {
			case "create":
				_, _, _, errno = root.Create(ctx, "new", syscall.O_RDWR, 0600, &gofuse.EntryOut{})
			case "mkdir":
				_, errno = root.Mkdir(ctx, "new", 0700, &gofuse.EntryOut{})
			case "setattr":
				errno = n.Setattr(ctx, nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
					Valid: gofuse.FATTR_MODE, Mode: 0600,
				}}, &gofuse.AttrOut{})
			}
			if errno != 0 || ctx.Err() != context.Canceled {
				t.Fatalf("completed %s returned %v, context %v", operation, errno, ctx.Err())
			}
		})
	}
}

func TestSetattrCanceledBeforeStartingHasNoEffects(t *testing.T) {
	_, n, downstream := mutationTree(t)
	h := mutationHandle(t, n)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	downstream.before = func(context.Context, string) error { t.Fatal("a canceled setattr reached storage"); return nil }
	errno := n.Setattr(ctx, h, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		Valid: gofuse.FATTR_SIZE | gofuse.FATTR_MODE, Size: 3, Mode: 0600,
	}}, &gofuse.AttrOut{})
	if errno != syscall.EINTR {
		t.Fatalf("pre-canceled setattr returned %v", errno)
	}
	if body, err := downstream.FileStorage.Read(t.Context(), "f"); err != nil || string(body) != "contents" {
		t.Fatalf("pre-canceled setattr changed %q, %v", body, err)
	}
}
