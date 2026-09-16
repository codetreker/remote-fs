package fuse

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse/posix"
	"github.com/codetreker/remote-fs/packages/storage"
	fsbridge "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type namespaceFixture struct {
	storage.FileSession
	lookup func(storage.ChildName) (storage.Attr, error)
	open   func(storage.ChildName, storage.OpenAtOptions) (storage.OpenResult, error)
	mutate func(storage.NameCommand) (storage.NameResult, error)
}

func (s *namespaceFixture) CheckNamespaceAccess() error { return nil }
func (s *namespaceFixture) CheckAtomicFileOpen() error  { return nil }
func (s *namespaceFixture) LookupAt(_ context.Context, name storage.ChildName) (storage.Attr, error) {
	return s.lookup(name)
}
func (s *namespaceFixture) OpenAt(_ context.Context, name storage.ChildName, o storage.OpenAtOptions) (storage.OpenResult, error) {
	return s.open(name, o)
}
func (s *namespaceFixture) MutateName(_ context.Context, c storage.NameCommand) (storage.NameResult, error) {
	return s.mutate(c)
}
func (s *namespaceFixture) ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	panic("exact-name operation enumerated sibling entries")
}
func (s *namespaceFixture) Status(context.Context) (storage.FileSessionStatus, error) {
	panic("ordinary exact-name operation added an action epoch query")
}

type retainedNamespaceFile struct {
	storage.File
	closes int
	clean  bool
}

func (f *retainedNamespaceFile) Stat(context.Context) (storage.Attr, error) {
	panic("open discarded the atomically captured attributes")
}
func (f *retainedNamespaceFile) Close(ctx context.Context) error {
	f.closes++
	_, deadline := ctx.Deadline()
	f.clean = ctx.Err() == nil && deadline
	return nil
}

func namespaceRoot(session *namespaceFixture) *node {
	v := &volume{files: session, maxFileSize: 1024, flushTimeout: time.Second, deadline: time.Now().Add(time.Minute), stop: make(chan struct{}), done: make(chan struct{})}
	root := &node{volume: v, id: rootIdentity(7)}
	fsbridge.NewNodeFS(root, &fsbridge.Options{})
	return root
}

func TestExactLookupAndCreateKeepRawNameAndCapturedOpenResult(t *testing.T) {
	session := &namespaceFixture{}
	root := namespaceRoot(session)
	name := "raw-\xff"
	session.lookup = func(target storage.ChildName) (storage.Attr, error) {
		if target.Parent.NodeID != 7 || target.Parent.Scope != nil || string(target.RawLeaf) != name {
			t.Fatal(target)
		}
		return storage.Attr{ID: 11, Kind: storage.NodeRegular}, nil
	}
	if _, errno := root.Lookup(t.Context(), name, &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
	file := &retainedNamespaceFile{}
	session.open = func(target storage.ChildName, o storage.OpenAtOptions) (storage.OpenResult, error) {
		if target.Parent.NodeID != 7 || string(target.RawLeaf) != name || o.Guards != nil || o.Target.State != storage.Any || !o.Create || !o.Read || !o.Write || o.Use.Uses != storage.ReadData|storage.WriteData || o.Use.Deny != 0 {
			t.Fatalf("open target=%+v options=%+v", target, o)
		}
		mode, err := posix.Decode(o.Initial.OnCreate.Metadata[posix.Namespace])
		if err != nil || mode != 0600 {
			t.Fatalf("creation mode=%v %v", mode, err)
		}
		return storage.OpenResult{File: file, Attr: storage.Attr{ID: 12, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{posix.Namespace: {Version: []byte{1}, Data: o.Initial.OnCreate.Metadata[posix.Namespace]}}}, Outcome: storage.Created}, nil
	}
	var out gofuse.EntryOut
	_, opened, _, errno := root.Create(t.Context(), name, syscall.O_RDWR, 0600, &out)
	if errno != 0 || opened == nil || out.Mode&0777 != 0600 || file.closes != 0 {
		t.Fatalf("create result=%v mode=%o closes=%d", errno, out.Mode, file.closes)
	}
}

func TestOpenFailureClosesExactReturnedReferenceAndPreservesEffects(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "retention only", true: "creation committed"}[changed], func(t *testing.T) {
			session := &namespaceFixture{}
			root := namespaceRoot(session)
			file := &retainedNamespaceFile{}
			session.open = func(storage.ChildName, storage.OpenAtOptions) (storage.OpenResult, error) {
				outcome := storage.Opened
				if changed {
					outcome = storage.Created
				}
				return storage.OpenResult{File: file, Outcome: outcome}, context.Canceled
			}
			_, _, err := root.openFile(t.Context(), root.childName("file"), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Create: true}})
			want := syscall.EINTR
			if changed {
				want = syscall.EIO
			}
			if errnoOf(err) != want || !errors.Is(err, context.Canceled) || file.closes != 1 || !file.clean {
				t.Fatalf("cleanup result=%v closes=%d clean=%v", err, file.closes, file.clean)
			}
		})
	}
}

func TestUnknownOpenOutcomeCannotInviteMutationRetry(t *testing.T) {
	session := &namespaceFixture{}
	root := namespaceRoot(session)
	file := &retainedNamespaceFile{}
	session.open = func(storage.ChildName, storage.OpenAtOptions) (storage.OpenResult, error) {
		return storage.OpenResult{File: file}, context.Canceled
	}
	_, _, err := root.openFile(t.Context(), root.childName("file"), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Create: true}})
	if errnoOf(err) != syscall.EIO || !errors.Is(err, context.Canceled) || file.closes != 1 || !file.clean {
		t.Fatalf("unknown result=%v closes=%d clean=%v", err, file.closes, file.clean)
	}
}

func TestDirectoryMutationCancellationPreservesPartialResult(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "not applied", true: "effect possible"}[partial], func(t *testing.T) {
			session := &namespaceFixture{}
			root := namespaceRoot(session)
			session.mutate = func(command storage.NameCommand) (storage.NameResult, error) {
				if command.Kind != storage.NameMkdir || command.Name.Parent.NodeID != 7 || command.Guards != nil {
					t.Fatal(command)
				}
				if partial {
					return storage.NameResult{Attr: &storage.Attr{ID: 8, Kind: storage.NodeDirectory}}, context.Canceled
				}
				return storage.NameResult{}, context.Canceled
			}
			_, errno := root.Mkdir(t.Context(), "new", 0700, &gofuse.EntryOut{})
			want := syscall.EINTR
			if partial {
				want = syscall.EIO
			}
			if errno != want {
				t.Fatalf("directory cancellation=%v want=%v", errno, want)
			}
		})
	}
}

func (s *namespaceFixture) ReadDirNodeBounded(context.Context, storage.DirectoryTarget, *storage.ListResult) (storage.DirectoryObservation, error) {
	panic("fixture received unexpected bounded-read method")
}

func TestDirectoryCreationCarriesPermissionsInItsAtomicMutation(t *testing.T) {
	session := &namespaceFixture{}
	root := namespaceRoot(session)
	calls := 0
	session.mutate = func(command storage.NameCommand) (storage.NameResult, error) {
		calls++
		mode, err := posix.Decode(command.Initial.Metadata[posix.Namespace])
		if err != nil || mode != 0700 || command.Kind != storage.NameMkdir || command.Name.Parent.NodeID != 7 || command.Target.State != storage.Absent || command.Guards != nil {
			t.Fatalf("directory creation=%+v permissions=%v err=%v", command, mode, err)
		}
		attr := storage.Attr{ID: 8, Kind: storage.NodeDirectory, Metadata: map[string]storage.OpaquePayload{posix.Namespace: {Version: []byte{1}, Data: command.Initial.Metadata[posix.Namespace]}}}
		return storage.NameResult{Attr: &attr}, nil
	}
	var out gofuse.EntryOut
	if _, errno := root.Mkdir(t.Context(), "directory", 0700, &out); errno != 0 || calls != 1 || out.Mode&0777 != 0700 {
		t.Fatalf("mkdir=%v calls=%d mode=%o", errno, calls, out.Mode)
	}
}
