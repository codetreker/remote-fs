package fuse

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/fuse/posix"
	"github.com/codetreker/remote-fs/packages/storage"
)

type namespaceFixture struct {
	storage.FileSession
	lookup         func(storage.ChildName) (storage.Attr, error)
	readDir        func(storage.DirectoryTarget) (storage.ObservedDirectory, error)
	readDirBounded func(storage.DirectoryTarget, *storage.ListResult) (storage.DirectoryObservation, error)
	open           func(storage.ChildName, storage.OpenAtOptions) (storage.OpenResult, error)
	mutate         func(storage.NameCommand) (storage.NameResult, error)
}

func (s *namespaceFixture) CheckNamespaceAccess() error { return nil }
func (s *namespaceFixture) CheckAtomicFileOpen() error  { return nil }
func (s *namespaceFixture) LookupAt(_ context.Context, name storage.ChildName) (storage.Attr, error) {
	return s.lookup(name)
}
func (s *namespaceFixture) ReadDirNode(_ context.Context, target storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	if s.readDir == nil {
		panic("unexpected unbounded directory observation")
	}
	return s.readDir(target)
}
func (s *namespaceFixture) ReadDirNodeBounded(_ context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	if s.readDirBounded == nil {
		panic("unexpected bounded directory observation")
	}
	return s.readDirBounded(target, result)
}
func (s *namespaceFixture) OpenAt(_ context.Context, name storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
	return s.open(name, options)
}
func (s *namespaceFixture) MutateName(_ context.Context, command storage.NameCommand) (storage.NameResult, error) {
	return s.mutate(command)
}
func (s *namespaceFixture) Status(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{
		Epoch: "namespace-fixture", Revision: 1, ActionEpoch: 7,
		Remaining: time.Minute, HistoryRemaining: time.Minute,
	}, nil
}

type retainedNamespaceFile struct {
	storage.File
	closes int
	clean  bool
}

func (f *retainedNamespaceFile) Stat(context.Context) (storage.Attr, error) {
	panic("atomic open result attributes were discarded")
}
func (f *retainedNamespaceFile) Close(ctx context.Context) error {
	f.closes++
	_, deadline := ctx.Deadline()
	f.clean = ctx.Err() == nil && deadline
	return nil
}

func namespaceRoot(session storage.FileSession) *node {
	v := &volume{
		files: session, maxFileSize: 1024, flushTimeout: time.Second,
		deadline: time.Now().Add(time.Minute), stop: make(chan struct{}), done: make(chan struct{}),
	}
	root := &node{volume: v, id: rootIdentity(7)}
	fs.NewNodeFS(root, &fs.Options{})
	return root
}

func TestExactLookupAndCreateUseParentIdentityAndCapturedOpenResult(t *testing.T) {
	session := &namespaceFixture{}
	root := namespaceRoot(session)
	name := "raw-\xff"
	session.lookup = func(target storage.ChildName) (storage.Attr, error) {
		if target.Parent.NodeID != 7 || target.Parent.Scope != nil || string(target.RawLeaf) != name {
			t.Fatalf("lookup target = %+v", target)
		}
		return storage.Attr{ID: 11, Kind: storage.NodeRegular}, nil
	}
	if _, errno := root.Lookup(t.Context(), name, &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}

	file := &retainedNamespaceFile{}
	session.open = func(target storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
		if target.Parent.NodeID != 7 || target.Parent.Scope != nil || string(target.RawLeaf) != name {
			t.Fatalf("open target = %+v", target)
		}
		if !options.Read || !options.Write || !options.Create || options.Exclusive || options.Target.State != storage.Any || options.Existing != storage.Keep || options.Use.Uses != storage.ReadData|storage.WriteData {
			t.Fatalf("open options = %+v", options)
		}
		if epoch, err := options.Action.Epoch(); err != nil || epoch != 7 {
			t.Fatalf("action = %q epoch=%d err=%v", options.Action, epoch, err)
		}
		mode, err := posix.Decode(options.Initial.OnCreate.Metadata[posix.Namespace])
		if err != nil || mode != 0600 {
			t.Fatalf("creation mode = %v, %v", mode, err)
		}
		return storage.OpenResult{
			File: file,
			Attr: storage.Attr{ID: 12, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{
				posix.Namespace: {Version: []byte{1}, Data: options.Initial.OnCreate.Metadata[posix.Namespace]},
			}},
			Outcome: storage.Created,
		}, nil
	}
	var out gofuse.EntryOut
	_, opened, _, errno := root.Create(t.Context(), name, syscall.O_RDWR|syscall.O_CREAT, 0600, &out)
	if errno != 0 || opened == nil || out.Mode&0777 != 0600 || file.closes != 0 {
		t.Fatalf("create = %v mode=%o closes=%d", errno, out.Mode, file.closes)
	}
}

func TestAtomicOpenFailureClosesReturnedFileAndPreservesUnknownOutcome(t *testing.T) {
	for _, outcome := range []storage.OpenOutcome{storage.Opened, 0, storage.Created} {
		t.Run(map[storage.OpenOutcome]string{storage.Opened: "retention only", 0: "unknown", storage.Created: "created"}[outcome], func(t *testing.T) {
			session := &namespaceFixture{}
			root := namespaceRoot(session)
			file := &retainedNamespaceFile{}
			session.open = func(storage.ChildName, storage.OpenAtOptions) (storage.OpenResult, error) {
				return storage.OpenResult{File: file, Outcome: outcome}, context.Canceled
			}
			_, _, err := root.openFile(t.Context(), root.childName("file", nil), storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Create: true}})
			want := syscall.EINTR
			if outcome != storage.Opened {
				want = syscall.EIO
			}
			if errnoOf(err) != want || !errors.Is(err, context.Canceled) || file.closes != 1 || !file.clean {
				t.Fatalf("result=%v closes=%d clean=%v", err, file.closes, file.clean)
			}
		})
	}
}

type replayingNamespaceFixture struct {
	*namespaceFixture
	receipt storage.FileActionReceipt
	queries int
}

func (s *replayingNamespaceFixture) CheckFileActions() error { return nil }
func (s *replayingNamespaceFixture) QueryFileAction(_ context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	s.queries++
	s.receipt.Action = action
	return s.receipt, nil
}
func (s *replayingNamespaceFixture) QueryDeleteIntent(context.Context, storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	panic("file open queried a delete intent")
}
func (s *replayingNamespaceFixture) AcknowledgeDeleteIntent(context.Context, storage.AcknowledgeDeleteIntentCommand) error {
	panic("file open acknowledged a delete intent")
}

func TestInterruptedAtomicOpenReplaysTheSameCompletedAction(t *testing.T) {
	base := &namespaceFixture{}
	session := &replayingNamespaceFixture{
		namespaceFixture: base,
		receipt:          storage.FileActionReceipt{Operation: storage.OpFileOpenAt, Outcome: storage.FileActionCompleted},
	}
	root := namespaceRoot(session)
	file := &retainedNamespaceFile{}
	var first storage.FileActionID
	calls := 0
	base.open = func(_ storage.ChildName, options storage.OpenAtOptions) (storage.OpenResult, error) {
		calls++
		if calls == 1 {
			first = options.Action
			return storage.OpenResult{}, context.Canceled
		}
		if options.Action != first {
			t.Fatalf("replay changed action %q to %q", first, options.Action)
		}
		return storage.OpenResult{File: file, Attr: storage.Attr{ID: 12, Kind: storage.NodeRegular, Metadata: map[string]storage.OpaquePayload{
			posix.Namespace: {Version: []byte{1}, Data: options.Initial.OnCreate.Metadata[posix.Namespace]},
		}}, Outcome: storage.Created}, nil
	}
	var out gofuse.EntryOut
	_, opened, _, errno := root.Create(t.Context(), "file", syscall.O_RDWR|syscall.O_CREAT, 0600, &out)
	if errno != 0 || opened == nil || calls != 2 || session.queries != 1 {
		t.Fatalf("create=%v calls=%d queries=%d", errno, calls, session.queries)
	}
}

func TestFailedReplayOfKnownCompletedActionRemainsUnknown(t *testing.T) {
	for _, operation := range []string{"open", "unlink"} {
		t.Run(operation, func(t *testing.T) {
			base := &namespaceFixture{}
			receiptOperation := storage.OpFileOpenAt
			if operation == "unlink" {
				receiptOperation = storage.OpFileMutateName
			}
			session := &replayingNamespaceFixture{
				namespaceFixture: base,
				receipt:          storage.FileActionReceipt{Operation: receiptOperation, Outcome: storage.FileActionCompleted},
			}
			root := namespaceRoot(session)
			calls := 0
			if operation == "open" {
				base.open = func(storage.ChildName, storage.OpenAtOptions) (storage.OpenResult, error) {
					calls++
					return storage.OpenResult{}, context.Canceled
				}
				_, _, _, errno := root.Create(t.Context(), "file", syscall.O_RDWR|syscall.O_CREAT, 0600, &gofuse.EntryOut{})
				if errno != syscall.EIO {
					t.Fatalf("open replay returned %v", errno)
				}
			} else {
				base.mutate = func(storage.NameCommand) (storage.NameResult, error) {
					calls++
					return storage.NameResult{}, context.Canceled
				}
				if errno := root.Unlink(t.Context(), "file"); errno != syscall.EIO {
					t.Fatalf("unlink replay returned %v", errno)
				}
			}
			if calls != 2 || session.queries != 1 {
				t.Fatalf("calls=%d queries=%d", calls, session.queries)
			}
		})
	}
}

func TestNamespaceMutationsCarryAtomicInitialStateAndActionIdentity(t *testing.T) {
	session := &namespaceFixture{}
	root := namespaceRoot(session)
	var commands []storage.NameCommand
	session.mutate = func(command storage.NameCommand) (storage.NameResult, error) {
		commands = append(commands, command)
		attr := storage.Attr{ID: uint64(20 + len(commands)), Kind: storage.NodeDirectory}
		if command.Kind == storage.NameSymlink {
			attr.Kind = storage.NodeSymlink
			attr.Size = int64(len(command.Initial.LinkTarget))
		}
		return storage.NameResult{Attr: &attr}, nil
	}

	if _, errno := root.Mkdir(t.Context(), "dir", 0710, &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
	if _, errno := root.Symlink(t.Context(), "../target", "link", &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
	if len(commands) != 2 {
		t.Fatalf("commands = %d", len(commands))
	}
	for _, command := range commands {
		if command.Name.Parent.NodeID != 7 || command.Target.State != storage.Absent {
			t.Fatalf("command = %+v", command)
		}
		if epoch, err := command.Action.Epoch(); err != nil || epoch != 7 {
			t.Fatalf("action = %q epoch=%d err=%v", command.Action, epoch, err)
		}
	}
	mode, err := posix.Decode(commands[0].Initial.Metadata[posix.Namespace])
	if err != nil || mode != 0710 || commands[0].Kind != storage.NameMkdir {
		t.Fatalf("mkdir = %+v mode=%v err=%v", commands[0], mode, err)
	}
	if commands[1].Kind != storage.NameSymlink || string(commands[1].Initial.LinkTarget) != "../target" {
		t.Fatalf("symlink = %+v", commands[1])
	}
}

func TestChildLookupKeepsParentIdentityAfterParentRenameAndNameReuse(t *testing.T) {
	session := &namespaceFixture{}
	root := namespaceRoot(session)
	old := root.child(t.Context(), "parent", syscall.S_IFDIR|0755, 8)
	if !root.AddChild("parent", old, false) {
		t.Fatal("attaching parent inode")
	}
	oldNode := old.Operations().(*node)
	if !root.MvChild("parent", &root.Inode, "moved", true) {
		t.Fatal("moving parent inode")
	}
	root.id.move("parent", root.id, "moved")
	replacement := root.child(t.Context(), "parent", syscall.S_IFDIR|0755, 9)
	if !root.AddChild("parent", replacement, false) {
		t.Fatal("attaching replacement parent")
	}

	session.lookup = func(name storage.ChildName) (storage.Attr, error) {
		if name.Parent.NodeID != 8 || string(name.RawLeaf) != "child" {
			t.Fatalf("lookup rebound to replacement parent: %+v", name)
		}
		return storage.Attr{ID: 10, Kind: storage.NodeRegular}, nil
	}
	if _, errno := oldNode.Lookup(t.Context(), "child", &gofuse.EntryOut{}); errno != 0 {
		t.Fatal(errno)
	}
}

type identityOpenSession struct {
	storage.FileSession
	opened  uint64
	options storage.FileOpenOptions
	file    storage.File
}

func (s *identityOpenSession) OpenNode(_ context.Context, id uint64, options storage.FileOpenOptions) (storage.File, error) {
	s.opened = id
	s.options = options
	if options.ExpectedID != id || options.Create || !options.Read && !options.Write || options.Truncate && !options.Write {
		return nil, syscall.EINVAL
	}
	return s.file, nil
}

type identityOpenFile struct {
	storage.File
	attr    storage.Attr
	statErr error
	closed  bool
}

func (f *identityOpenFile) Stat(context.Context) (storage.Attr, error) { return f.attr, f.statErr }
func (f *identityOpenFile) Close(context.Context) error {
	f.closed = true
	return nil
}

func TestTruncatingIdentityOpenDoesNotExposeRetryablePostMutationFailure(t *testing.T) {
	file := &identityOpenFile{statErr: context.Canceled}
	session := &identityOpenSession{file: file}
	root := namespaceRoot(session)
	n := root.child(t.Context(), "file", syscall.S_IFREG|0644, 8).Operations().(*node)

	_, _, errno := n.Open(t.Context(), syscall.O_WRONLY|syscall.O_TRUNC)
	if errno != syscall.EIO || !session.options.Truncate || !file.closed {
		t.Fatalf("open errno=%v truncate=%v closed=%v", errno, session.options.Truncate, file.closed)
	}
}

func TestExistingInodeOpenKeepsIdentityAfterParentRenameAndNameReuse(t *testing.T) {
	file := &identityOpenFile{attr: storage.Attr{ID: 8, Kind: storage.NodeRegular}}
	session := &identityOpenSession{file: file}
	root := namespaceRoot(session)
	old := root.child(t.Context(), "file", syscall.S_IFREG|0644, 8)
	if !root.AddChild("file", old, false) {
		t.Fatal("attaching file inode")
	}
	if !root.MvChild("file", &root.Inode, "moved", true) {
		t.Fatal("moving file inode")
	}
	root.id.move("file", root.id, "moved")
	replacement := root.child(t.Context(), "file", syscall.S_IFREG|0644, 9)
	if !root.AddChild("file", replacement, false) {
		t.Fatal("attaching replacement file")
	}

	opened, _, errno := old.Operations().(*node).Open(t.Context(), syscall.O_RDONLY)
	if errno != 0 || session.opened != 8 {
		t.Fatalf("open errno=%v identity=%d", errno, session.opened)
	}
	if err := opened.(*handle).closeFile(t.Context()); err != nil || !file.closed {
		t.Fatalf("close=%v closed=%v", err, file.closed)
	}
}
