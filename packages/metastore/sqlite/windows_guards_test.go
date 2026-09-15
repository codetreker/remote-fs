package sqlite

import (
	"errors"
	"io/fs"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestWindowsPendingTargetRefusesUnregisteredNamedMutations(t *testing.T) {
	s, ws := windowsAuthority(t)
	target := windowsOpen(t, ws, "target", storage.WindowsAllAccess, storage.WindowsShareAll)
	if err := s.Create(t.Context(), "source"); err != nil {
		t.Fatal(err)
	}
	source, err := s.Stat(t.Context(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.SetDeletePending(t.Context(), true, windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "target"); storage.WindowsFailureOf(err) != storage.WindowsDeletePending {
		t.Fatalf("pendingremove=%v", err)
	}
	if err := s.Rename(t.Context(), "source", "target"); storage.WindowsFailureOf(err) != storage.WindowsDeletePending {
		t.Fatalf("pendingreplace=%v", err)
	}
	if after, err := s.Stat(t.Context(), "source"); err != nil || after.ID != source.ID {
		t.Fatalf("source=%+v %v", after, err)
	}
	if after, err := s.Stat(t.Context(), "target"); err != nil || after.ID != target.id {
		t.Fatalf("target=%+v %v", after, err)
	}
}

func TestWindowsDirectoryHintCannotBypassRegularReadonly(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	if _, err := s.write.Exec(`UPDATE nodes SET windows_attributes=? WHERE id=?`, storage.WindowsDOSDirectory|storage.WindowsDOSReadOnly, f.id); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("contradictoryattr=%v", err)
	}
	if opened, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Write: true}}); !errors.Is(err, syscall.EIO) {
		if opened != nil {
			opened.Close(t.Context())
		}
		t.Fatalf("readonlybypass=%v", err)
	}
	if snapshot, _, err := s.Snapshot(t.Context()); !errors.Is(err, syscall.EIO) {
		if snapshot != nil {
			snapshot.Close()
		}
		t.Fatalf("snapshotcorruption=%v", err)
	}
	if err := ws.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	config := LockingConfig{Database: s.databasePath, Volume: "workspace", SQLite: DefaultOptions(), Locks: locking.DefaultOptions()}
	if reopened, err := OpenLocking(t.Context(), config); !errors.Is(err, syscall.EIO) {
		if reopened != nil {
			reopened.Close()
		}
		t.Fatalf("corruptreopen=%v", err)
	}
}

func TestWindowsReparseConversionPreservesIssuedStrongIdentity(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	p := publicationFixture{store: s.Store, service: s.LockService()}
	owner := p.owner(t)
	resource := p.resource(t, owner, "file")
	grant := p.grant(t, owner, "file", locking.Exclusive)
	if result, err := f.SetLink(publicationScope(t.Context(), owner, grant), "target", windowsActionID(t, ws)); err != nil || result.Attr.ID != uint64(f.id) {
		t.Fatalf("authorizedconversion=%+v %v", result, err)
	}
	mode := fs.FileMode(0o644)
	if err := s.SetAttr(t.Context(), "file", storage.AttrChange{Mode: &mode}); locking.CodeOf(err) != locking.Conflict {
		t.Fatalf("unscopedattrs=%v", err)
	}
	if err := s.Remove(t.Context(), "file"); locking.CodeOf(err) != locking.Conflict {
		t.Fatalf("unscopedremove=%v", err)
	}
	if _, err := f.SetLink(t.Context(), "other", windowsActionID(t, ws)); err == nil {
		t.Fatal("unscoped link change succeeded")
	}
	if _, err := p.service.Resolve(t.Context(), owner, "file"); locking.CodeOf(err) != locking.UnsupportedTarget {
		t.Fatalf("freshsymlinkresolve=%v", err)
	}
	renewed, err := p.service.Renew(t.Context(), locking.RenewRequest{Owner: owner, Request: "renew-reparse", Grant: grant, TTL: 15 * time.Second})
	if err != nil || renewed.Grant == nil || renewed.Grant.State != locking.Active {
		t.Fatalf("renewed=%+v %v", renewed, err)
	}
	grant = renewed.Grant.Ref
	if _, err := p.service.Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	again, err := p.service.Acquire(t.Context(), locking.AcquireRequest{Owner: owner, Request: "same-identity", Resource: resource, Mode: locking.Exclusive, TTL: 10 * time.Second})
	if err != nil || again.Grant == nil || again.Grant.State != locking.Active {
		t.Fatalf("reacquired=%+v %v", again, err)
	}
	if _, err := p.service.Release(t.Context(), owner, again.Grant.Ref); err != nil {
		t.Fatal(err)
	}
	if err := s.SetAttr(t.Context(), "file", storage.AttrChange{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
}
