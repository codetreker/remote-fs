package replicated_test

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func retainedSession(t *testing.T, namespace storage.FileStorage) storage.FileSession {
	t.Helper()
	if err := namespace.CheckFileStorage(); err != nil {
		t.Fatal(err)
	}
	session, err := namespace.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return session
}

func retainedOpen(t *testing.T, session storage.FileSession, path string, create bool) storage.File {
	t.Helper()
	file, err := session.OpenFile(t.Context(), path, storage.FileOpenOptions{Read: true, Write: true, Create: create, Mode: 0600})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return file
}

func TestRetainedFileQueriesTheAuthorityAfterRenameAndUnlink(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, local := mount(t, s)
	session := retainedSession(t, mounted)
	file := retainedOpen(t, session, "file", true)
	created, err := mounted.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal("atomic create did not confirm its replica entry:", err)
	}
	if _, err := file.WriteAt(t.Context(), 0, []byte("original")); err != nil {
		t.Fatal(err)
	}
	if attr, err := mounted.Stat(t.Context(), "file"); err != nil || attr.Size != 8 {
		t.Fatalf("linked write returned before its barrier: %+v, %v", attr, err)
	}
	if err := s.elsewhere.Write(t.Context(), "file", []byte("current")); err != nil {
		t.Fatal(err)
	}
	read, err := file.ReadAt(t.Context(), 0, 100)
	if err != nil || string(read.Data) != "current" || read.Attr.Size != 7 || read.Attr.ID != created.ID {
		t.Fatalf("retained read did not capture current authority content: %+v, %v", read, err)
	}
	if err := s.elsewhere.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.elsewhere.Write(t.Context(), "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(t.Context(), 1, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if content, err := s.elsewhere.Read(t.Context(), "moved"); err != nil || string(content) != "c!rrent" {
		t.Fatalf("range write followed the former name: %q, %v", content, err)
	}
	if err := s.elsewhere.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	requireCaughtUp(t, s, local)
	position := local.Position()
	if attr, err := file.Stat(t.Context()); err != nil || attr.ID != created.ID || attr.Size != 7 {
		t.Fatalf("detached file stat: %+v, %v", attr, err)
	}
	if attr, err := session.StatNode(t.Context(), created.ID); err != nil || attr.ID != created.ID {
		t.Fatalf("detached node stat: %+v, %v", attr, err)
	}
	byID, err := session.OpenNode(t.Context(), created.ID, storage.FileOpenOptions{Read: true, ExpectedID: created.ID})
	if err != nil {
		t.Fatal("opening the retained identity required its former name:", err)
	}
	if read, err := byID.ReadAt(t.Context(), 0, 7); err != nil || string(read.Data) != "c!rrent" || read.Attr.ID != created.ID {
		t.Fatalf("identity open selected a replacement: %+v, %v", read, err)
	}
	if err := byID.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if attr, err := file.Truncate(t.Context(), 10); err != nil || attr.Size != 10 {
		t.Fatalf("detached truncate: %+v, %v", attr, err)
	}
	read, err = file.ReadAt(t.Context(), 5, 10)
	if err != nil || !bytes.Equal(read.Data, []byte{'n', 't', 0, 0, 0}) || read.Attr.Size != 10 {
		t.Fatalf("detached read/EOF lost its content revision: %+v, %v", read, err)
	}
	mode := fs.FileMode(0640)
	if attr, err := session.SetNodeAttr(t.Context(), created.ID, storage.AttrChange{Mode: &mode}); err != nil || attr.Mode.Perm() != mode {
		t.Fatalf("detached identity setattr: %+v, %v", attr, err)
	}
	moment := time.Unix(1000, 123)
	if attr, err := file.SetAttr(t.Context(), storage.AttrChange{ModTime: &moment}); err != nil || !attr.ModTime.Equal(moment) {
		t.Fatalf("detached file setattr: %+v, %v", attr, err)
	}
	if err := file.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if local.Position() != position {
		t.Fatal("detached mutations fabricated named replica events")
	}
	if _, err := mounted.Stat(t.Context(), "moved"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("detached mutation recreated its removed name: %v", err)
	}
	if content, err := s.elsewhere.Read(t.Context(), "file"); err != nil || string(content) != "replacement" {
		t.Fatalf("detached mutation changed the replacement: %q, %v", content, err)
	}
}

func TestRetainedSessionPreservesMutationScope(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, _ := mount(t, s)
	if err := mounted.Write(t.Context(), "file", []byte("before")); err != nil {
		t.Fatal(err)
	}
	owner := replicaLockOwner(t, mounted)
	grant := replicaLockGrant(t, mounted, owner)
	proofs := []locking.GrantRef{grant}
	view, err := mounted.Scope(locking.MutationScope{Owner: owner, Grants: proofs})
	if err != nil {
		t.Fatal(err)
	}
	session := retainedSession(t, view.(storage.FileStorage))
	proofs[0].Generation++
	file := retainedOpen(t, session, "file", false)
	if _, err := file.WriteAt(t.Context(), 0, []byte("scoped")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Truncate(locking.WithScope(t.Context(), locking.MutationScope{}), 0); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("explicit anonymous file mutation inherited a grant: %v", err)
	}
	if _, err := mounted.Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	if _, err := file.SetAttr(t.Context(), storage.AttrChange{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("file mutation discarded its expired proof: %v", err)
	}
	if read, err := file.ReadAt(t.Context(), 0, 6); err != nil || string(read.Data) != "scoped" {
		t.Fatalf("ordinary retained read asserted a live strong grant: %+v, %v", read, err)
	}
	if _, err := file.Stat(t.Context()); err != nil {
		t.Fatal("ordinary retained stat asserted a live strong grant:", err)
	}
}

func TestRetainedControlsRemainAvailableWhenTheStreamFails(t *testing.T) {
	s := serve(t, httprest.DefaultLimits())
	mounted, _ := mount(t, s)
	session := retainedSession(t, mounted)
	file := retainedOpen(t, session, "file", true)
	other := retainedOpen(t, session, "file", false)
	s.events.cut()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := mounted.Stat(t.Context(), "file"); errors.Is(err, syscall.EIO) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replica did not observe its disconnected stream")
		}
		time.Sleep(time.Millisecond)
	}
	before := s.calls.total()
	for name, call := range map[string]func() error{
		"stat":     func() error { _, err := file.Stat(t.Context()); return err },
		"read":     func() error { _, err := file.ReadAt(t.Context(), 0, 1); return err },
		"write":    func() error { _, err := file.WriteAt(t.Context(), 0, []byte{'x'}); return err },
		"truncate": func() error { _, err := file.Truncate(t.Context(), 0); return err },
		"setattr":  func() error { _, err := file.SetAttr(t.Context(), storage.AttrChange{}); return err },
		"sync":     func() error { return file.Sync(t.Context()) },
	} {
		if err := call(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("%s fabricated a healthy answer: %v", name, err)
		}
	}
	if s.calls.total() != before {
		t.Fatal("unhealthy retained I/O reached the authority")
	}
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	lock := storage.FileLock{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64}
	if attempt, err := file.SetLock(t.Context(), 1, lock, request); err != nil || attempt.State != storage.LockGranted {
		t.Fatalf("advisory lock was not authoritative: %+v, %v", attempt, err)
	}
	if conflict, err := other.GetLock(t.Context(), 2, lock); err != nil || !conflict.Found {
		t.Fatalf("advisory conflict was not authoritative: %+v, %v", conflict, err)
	}
	if attempt, err := file.QueryLock(t.Context(), 1, request); err != nil || attempt.State != storage.LockGranted {
		t.Fatalf("advisory receipt unavailable: %+v, %v", attempt, err)
	}
	if attempt, err := file.CancelLock(t.Context(), 1, request); err != nil || attempt.State != storage.LockGranted {
		t.Fatalf("advisory cancellation unavailable: %+v, %v", attempt, err)
	}
	if err := file.DropLocks(t.Context(), 1, storage.Flock); err != nil {
		t.Fatal(err)
	}
	if conflict, err := other.GetLock(t.Context(), 2, lock); err != nil || conflict.Found {
		t.Fatalf("advisory cleanup did not release the granted acquisition: %+v, %v", conflict, err)
	}
	if _, err := session.Renew(t.Context()); err != nil {
		t.Fatal(err)
	}
}
