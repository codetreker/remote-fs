package sqlite

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func windowsAuthority(t *testing.T) (*LockingStore, *windowsSession) {
	t.Helper()
	s, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	state, err := s.WindowsState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if state.Enabled || state.VolumeIdentity == "" || state.VolumeSerial == 0 {
		t.Fatalf("initialstate=%+v", state)
	}
	action, err := storage.NewLockRequestID(state.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	activation, err := s.EnableWindows(t.Context(), action)
	if err != nil || !activation.Enabled {
		t.Fatalf("activation=%+v error=%v", activation, err)
	}
	if replay, err := s.QueryWindowsActivation(t.Context(), action); err != nil || replay.State != storage.WindowsActionCompleted {
		t.Fatalf("activationreceipt=%+v error=%v", replay, err)
	}
	options := storage.DefaultFileSessionOptions()
	options.Lease = time.Minute
	session, err := s.NewWindowsSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	ws := session.(*windowsSession)
	t.Cleanup(func() {
		if err := ws.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return s, ws
}

func windowsActionID(t *testing.T, s *windowsSession) storage.WindowsActionID {
	t.Helper()
	status, err := s.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func windowsOpen(t *testing.T, s *windowsSession, name string, access storage.WindowsAccess, share storage.WindowsShare) *windowsFile {
	t.Helper()
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: access, Share: share, Disposition: storage.WindowsOpenIf}, Lookup: storage.WindowsLookup{ParentID: uint64(s.store.root), Name: name}, Mode: 0o600}
	result, err := s.Open(t.Context(), request, windowsActionID(t, s))
	if err != nil {
		t.Fatal(err)
	}
	return result.Reference.(*windowsFile)
}

func windowsPublish(t *testing.T, f *windowsFile, offset, size int64) storage.WindowsActionID {
	t.Helper()
	id := windowsActionID(t, f.session)
	operation := metastore.WindowsIO{Offset: offset, Length: size - offset, Write: true}
	_, fresh, err := f.BeginContent(t.Context(), id, sha256.Sum256([]byte("write")), operation)
	if err != nil || !fresh {
		t.Fatalf("begin=%t %v", fresh, err)
	}
	before, err := f.Capture(t.Context(), operation)
	if err != nil {
		t.Fatal(err)
	}
	key, err := f.Reserve(t.Context(), size)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.CommitContent(t.Context(), id, before.Revision, metastore.Object{Key: key, Size: size, ModTime: time.Now()})
	if err != nil || result.Attr.Size != size {
		t.Fatalf("commit=%+v error=%v", result, err)
	}
	return id
}

func TestWindowsAuthoritySharingGuardsOrdinaryPathsAndOpens(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsReadData|storage.WindowsReadAttributes, storage.WindowsShareRead)
	_, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Write: true}})
	if storage.WindowsFailureOf(err) != storage.WindowsSharingViolation || !strings.Contains(err.Error(), "Windows sharing violation") {
		t.Fatalf("ordinaryopen=%v", err)
	}
	if err := s.Remove(t.Context(), "file"); storage.WindowsFailureOf(err) != storage.WindowsSharingViolation {
		t.Fatalf("remove=%v", err)
	}
	if err := s.Rename(t.Context(), "file", "moved"); storage.WindowsFailureOf(err) != storage.WindowsSharingViolation {
		t.Fatalf("rename=%v", err)
	}
	if _, err := f.Close(t.Context(), windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	ordinary, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Write: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ordinary.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsReadData, Share: storage.WindowsShareRead, Disposition: storage.WindowsOpen}, Lookup: storage.WindowsLookup{ParentID: uint64(s.root), Name: "file"}}
	if _, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); storage.WindowsFailureOf(err) != storage.WindowsSharingViolation {
		t.Fatalf("reverseconflict=%v", err)
	}
}

func TestWindowsAuthorityContentReceiptsAndRetainedIdentity(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	id := windowsPublish(t, f, 0, 9)
	before, err := f.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := f.CommitContent(t.Context(), id, 1, metastore.Object{}); err != nil || replay.Attr.Size != 9 {
		t.Fatalf("receipt=%+v error=%v", replay, err)
	}
	if err := s.Rename(t.Context(), "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "moved"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	after, err := f.Stat(t.Context())
	if err != nil || after.ID != before.ID || after.NameInfo.State != storage.WindowsNameDetached {
		t.Fatalf("detached=%+v error=%v", after, err)
	}
	windowsPublish(t, f, 9, 12)
	replacement, err := s.Stat(t.Context(), "file")
	if err != nil || uint64(replacement.ID) == before.ID || replacement.Size != 0 {
		t.Fatalf("replacement=%+v error=%v", replacement, err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 12 {
		t.Fatalf("usage=%d %v", used, err)
	}
	if _, err := f.Close(t.Context(), windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("releasedusage=%d %v", used, err)
	}
}

func TestWindowsAuthorityRangeWaitCancellationAndOrdinaryCapture(t *testing.T) {
	s, ws := windowsAuthority(t)
	a := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	windowsPublish(t, a, 0, 20)
	b := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	lock := storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{{Offset: 5, Length: 5, Type: storage.Exclusive, FailImmediately: true}}}
	if _, err := a.LockBatch(t.Context(), lock, windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(metastore.WithFileIO(t.Context(), metastore.WindowsIO{Offset: 0, Length: 20}), "file"); storage.WindowsFailureOf(err) != storage.WindowsLockConflict {
		t.Fatalf("ordinaryread=%v", err)
	}
	if _, err := b.Capture(t.Context(), metastore.WindowsIO{Offset: 0, Length: 5}); err != nil {
		t.Fatal(err)
	}
	lock.Ranges[0].FailImmediately = false
	waiting := windowsActionID(t, ws)
	if result, err := b.LockBatch(t.Context(), lock, waiting); err != nil || result.State != storage.WindowsActionPending {
		t.Fatalf("wait=%+v error=%v", result, err)
	}
	if result, err := ws.CancelAction(t.Context(), waiting); err != nil || result.State != storage.WindowsActionCancelled {
		t.Fatalf("cancel=%+v error=%v", result, err)
	}
	lock.Ranges[0].Type = storage.Unlock
	if _, err := a.LockBatch(t.Context(), lock, windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	if result, err := ws.QueryAction(t.Context(), waiting); err != nil || result.State != storage.WindowsActionCancelled {
		t.Fatalf("cancelreceipt=%+v error=%v", result, err)
	}
	lock.Ranges[0].Type = storage.Exclusive
	lock.Ranges[0].FailImmediately = true
	if _, err := a.LockBatch(t.Context(), lock, windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	lock.Ranges[0].FailImmediately = false
	waiting = windowsActionID(t, ws)
	if _, err := b.LockBatch(t.Context(), lock, waiting); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Close(t.Context(), windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	if result, err := ws.QueryAction(t.Context(), waiting); err != nil || result.State != storage.WindowsActionCompleted {
		t.Fatalf("grantedreceipt=%+v error=%v", result, err)
	}
}

func TestWindowsAuthorityMetadataOnlyAndDirectoryIdentity(t *testing.T) {
	s, ws := windowsAuthority(t)
	if err := s.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	directory := windowsOpen(t, ws, "directory", storage.WindowsAllAccess, storage.WindowsShareAll)
	if err := s.Rename(t.Context(), "directory", "renamed"); err != nil {
		t.Fatal(err)
	}
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsReadAttributes | storage.WindowsWriteAttributes, Share: 0, Disposition: storage.WindowsCreate}, Lookup: storage.WindowsLookup{ParentID: uint64(directory.id), ParentReference: directory.Reference(), Name: "entry"}, Mode: 0o600, DOSAttributes: storage.WindowsDOSHidden}
	result, err := ws.Open(t.Context(), request, windowsActionID(t, ws))
	if err != nil {
		t.Fatal(err)
	}
	f := result.Reference.(*windowsFile)
	if _, err := f.Capture(t.Context(), metastore.WindowsIO{Length: 1}); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("metadataread=%v", err)
	}
	attr, err := f.Stat(t.Context())
	if err != nil || attr.NameInfo.Path != "renamed/entry" || attr.DOSAttributes != storage.WindowsDOSHidden {
		t.Fatalf("attr=%+v error=%v", attr, err)
	}
	listing, err := storage.NewWindowsListResult(4096, 0, func(_ int, n int64, _ storage.WindowsBasicAttr) (int64, error) { return n + 128, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.ListBounded(t.Context(), listing); err != nil {
		t.Fatal(err)
	}
	entries, err := listing.Entries()
	if err != nil || len(entries) != 1 || entries[0].Attr.DOSAttributes != storage.WindowsDOSHidden {
		t.Fatalf("entries=%+v error=%v", entries, err)
	}
	changed := storage.WindowsDOSArchive
	if result, err := f.SetAttr(t.Context(), storage.WindowsAttrChange{DOSAttributes: &changed}, windowsActionID(t, ws)); err != nil || result.Attr.DOSAttributes != changed {
		t.Fatalf("setattr=%+v error=%v", result, err)
	}
}

func TestWindowsAuthorityDeletePendingRetainsOrdinaryOpenAndRejectsNewNames(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	ordinary, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := ordinary.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if _, err := f.SetDeletePending(t.Context(), true, windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Close(t.Context(), windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); storage.WindowsFailureOf(err) != storage.WindowsDeletePending {
		t.Fatalf("newopen=%v", err)
	}
	if err := ordinary.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("deleted=%v", err)
	}
	if err := s.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	d := windowsOpen(t, ws, "directory", storage.WindowsAllAccess, storage.WindowsShareAll)
	if _, err := d.SetDeletePending(t.Context(), true, windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "directory/new"); storage.WindowsFailureOf(err) != storage.WindowsDeletePending {
		t.Fatalf("childcreate=%v", err)
	}
	if _, err := d.Close(t.Context(), windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsAuthorityMutationCancellationAndFingerprintMismatch(t *testing.T) {
	_, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	id := windowsActionID(t, ws)
	digest := sha256.Sum256([]byte("original"))
	operation := metastore.WindowsIO{Write: true, Length: 5}
	if _, fresh, err := f.BeginContent(t.Context(), id, digest, operation); err != nil || !fresh {
		t.Fatalf("begin=%t %v", fresh, err)
	}
	if _, _, err := f.BeginContent(t.Context(), id, sha256.Sum256([]byte("other")), operation); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("mismatch=%v", err)
	}
	if result, err := ws.CancelAction(t.Context(), id); err != nil || result.State != storage.WindowsActionCancelled {
		t.Fatalf("cancel=%+v %v", result, err)
	}
	if result, err := f.CommitContent(t.Context(), id, 1, metastore.Object{Size: 5}); err != nil || result.State != storage.WindowsActionCancelled {
		t.Fatalf("latecommit=%+v %v", result, err)
	}
	attr, err := f.Stat(t.Context())
	if err != nil || attr.Size != 0 {
		t.Fatalf("unchanged=%+v %v", attr, err)
	}
	zero := windowsActionID(t, ws)
	if result, fresh, err := f.BeginContent(t.Context(), zero, digest, metastore.WindowsIO{Write: true}); err != nil || fresh || result.State != storage.WindowsActionCompleted || !result.Attr.ModTime.Equal(attr.ModTime) {
		t.Fatalf("zerowrite=%+v fresh=%t error=%v", result, fresh, err)
	}
}

func TestWindowsAuthoritySymlinkConfinementAndRetainedLookup(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "link", storage.WindowsAllAccess, storage.WindowsShareAll)
	if _, err := f.SetLink(t.Context(), "../escape", windowsActionID(t, ws)); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("escapingtarget=%v", err)
	}
	if result, err := f.SetLink(t.Context(), "target", windowsActionID(t, ws)); err != nil || result.Attr.Size != 6 {
		t.Fatalf("setlink=%+v %v", result, err)
	}
	if target, err := f.ReadLink(t.Context()); err != nil || target.Target != "target" {
		t.Fatalf("readlink=%+v %v", target, err)
	}
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsReadAttributes, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen}, Lookup: storage.WindowsLookup{ParentID: uint64(s.root), Name: "link"}}
	_, err := ws.Open(t.Context(), request, windowsActionID(t, ws))
	var link *storage.WindowsSymlinkError
	if !errors.As(err, &link) || link.Target != "target" || link.Location.Path != "link" {
		t.Fatalf("stopped=%v", err)
	}
	request.OpenReparsePoint = true
	if result, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); err != nil || result.Attr.ID != uint64(f.id) {
		t.Fatalf("reparse=%+v %v", result, err)
	}
	if _, err := s.OpenFile(t.Context(), "link", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, syscall.ELOOP) {
		t.Fatalf("ordinarysymlink=%v", err)
	}
}

func TestWindowsAuthorityRestartFencesPreviousLease(t *testing.T) {
	config := lockingTestConfig(t)
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	state, err := s.WindowsState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewLockRequestID(state.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnableWindows(t.Context(), id); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.Lease = 200 * time.Millisecond
	inner, err := s.NewWindowsSession(t.Context(), options)
	if err != nil {
		t.Fatal(err)
	}
	ws := inner.(*windowsSession)
	windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	if err := ws.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	config.Initialize = false
	reopened, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	if _, err := reopened.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("recoveryopen=%v", err)
	}
	if _, err := reopened.Stat(metastore.WithFileIO(t.Context(), metastore.WindowsIO{Length: 1}), "file"); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("recoveryread=%v", err)
	}
	<-time.After(time.Until(reopened.fileDomain.windows.recoveryUntil) + time.Millisecond)
	file, err := reopened.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsAuthorityDirectorySymlinkAndMetadataNotification(t *testing.T) {
	s, ws := windowsAuthority(t)
	if err := s.Mkdir(t.Context(), "link"); err != nil {
		t.Fatal(err)
	}
	f := windowsOpen(t, ws, "link", storage.WindowsAllAccess, storage.WindowsShareAll)
	result, err := f.SetLink(t.Context(), "target-directory", windowsActionID(t, ws))
	if err != nil || result.Attr.DOSAttributes&storage.WindowsDOSDirectory == 0 {
		t.Fatalf("directorylink=%+v %v", result, err)
	}
	if info, err := f.ReadLink(t.Context()); err != nil || info.Target != "target-directory" || info.Location.Path != "link" {
		t.Fatalf("linkinfo=%+v %v", info, err)
	}
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsReadAttributes, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen, Kind: storage.WindowsDirectory}, Lookup: storage.WindowsLookup{ParentID: uint64(s.root), Name: "link"}}
	if result, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); !errors.Is(err, syscall.ELOOP) || result.Symlink == nil {
		t.Fatalf("directorylinkfollow=%+v %v", result, err)
	}
	request.OpenReparsePoint = true
	if result, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); err != nil || result.Attr.DOSAttributes&storage.WindowsDOSDirectory == 0 {
		t.Fatalf("directoryreparse=%+v %v", result, err)
	}
	attributes := storage.WindowsDOSHidden
	result, err = f.SetAttr(t.Context(), storage.WindowsAttrChange{DOSAttributes: &attributes}, windowsActionID(t, ws))
	if err != nil || result.Attr.DOSAttributes != (storage.WindowsDOSDirectory|storage.WindowsDOSHidden) {
		t.Fatalf("preservedkind=%+v %v", result, err)
	}
}

func TestWindowsAuthorityRetiredEpochCannotReadmitExpiredAction(t *testing.T) {
	_, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	id := windowsActionID(t, ws)
	if _, err := f.SetDeletePending(t.Context(), false, id); err != nil {
		t.Fatal(err)
	}
	if err := ws.store.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	ws.actions[id].expires = time.Now().Add(-time.Second)
	ws.rotates = time.Now().Add(-time.Second)
	ws.store.coordinator.commit.release()
	if _, err := ws.QueryAction(t.Context(), id); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expiredreceipt=%v", err)
	}
	if _, err := f.SetDeletePending(t.Context(), false, id); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("oldreadmission=%v", err)
	}
}

func TestWindowsAuthorityStaleTimerCannotUndoRenewal(t *testing.T) {
	_, ws := windowsAuthority(t)
	before, err := ws.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	after, err := ws.Renew(t.Context())
	if err != nil || after.Revision <= before.Revision {
		t.Fatalf("renewal=%+v %v", after, err)
	}
	ws.expire()
	if _, err := ws.Status(t.Context()); err != nil {
		t.Fatalf("staletimer=%v", err)
	}
	if err := ws.store.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	ws.expires = time.Now().Add(-time.Second)
	ws.store.coordinator.commit.release()
	ws.expire()
	if _, err := ws.Status(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("expiredsession=%v", err)
	}
	if len(ws.store.fileDomain.windows.sessions) != 0 {
		t.Fatal("expired empty session retained its admission")
	}
}

func TestWindowsAuthorityResourceAdmissionHasNoMutationEffects(t *testing.T) {
	s, ws := windowsAuthority(t)
	ws.options.MaxFiles = 1
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate}, Lookup: storage.WindowsLookup{ParentID: uint64(s.root), Name: "rejected"}}
	if _, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("openlimit=%v", err)
	}
	if _, err := s.Stat(t.Context(), "rejected"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("rejectedcreation=%v", err)
	}
	ws.options.MaxLockRanges = 1
	batch := storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{{Offset: 10, Length: 1, Type: storage.Exclusive, FailImmediately: true}}}
	if _, err := f.LockBatch(t.Context(), batch, windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	batch.Ranges[0].Offset = 20
	if _, err := f.LockBatch(t.Context(), batch, windowsActionID(t, ws)); !errors.Is(err, syscall.ENOLCK) {
		t.Fatalf("rangelimit=%v", err)
	}
	ws.options.MaxLockActions = len(ws.actions)
	if _, err := f.SetDeletePending(t.Context(), true, windowsActionID(t, ws)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("actionlimit=%v", err)
	}
	if attr, err := f.Stat(t.Context()); err != nil || attr.DeletePending {
		t.Fatalf("rejecteddelete=%+v %v", attr, err)
	}
}

func TestWindowsAuthorityReadonlyPreservesGrantedAccess(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	readonly := storage.WindowsDOSReadOnly
	if _, err := f.SetAttr(t.Context(), storage.WindowsAttrChange{DOSAttributes: &readonly}, windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsWriteData, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen}, Lookup: storage.WindowsLookup{ParentID: uint64(s.root), Name: "file"}}
	if _, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("readonlyopen=%v", err)
	}
	if _, err := f.SetDeletePending(t.Context(), true, windowsActionID(t, ws)); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("readonlydelete=%v", err)
	}
	windowsPublish(t, f, 0, 5)
	if attr, err := f.Stat(t.Context()); err != nil || attr.Size != 5 {
		t.Fatalf("grantedwrite=%+v %v", attr, err)
	}
}

func TestWindowsAuthorityCreateDispositionsKeepActualResults(t *testing.T) {
	s, ws := windowsAuthority(t)
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate}, Lookup: storage.WindowsLookup{ParentID: uint64(s.root), Name: "file"}, Mode: 0o600}
	id := windowsActionID(t, ws)
	created, err := ws.Open(t.Context(), request, id)
	if err != nil || created.CreateAction != storage.WindowsCreated {
		t.Fatalf("created=%+v %v", created, err)
	}
	if replay, err := ws.Open(t.Context(), request, id); err != nil || replay.Reference != created.Reference {
		t.Fatalf("openreplay=%+v %v", replay, err)
	}
	if _, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("exclusive=%v", err)
	}
	f := created.Reference.(*windowsFile)
	windowsPublish(t, f, 0, 7)
	request.Disposition = storage.WindowsOverwrite
	overwritten, err := ws.Open(t.Context(), request, windowsActionID(t, ws))
	if err != nil || overwritten.CreateAction != storage.WindowsOverwritten || overwritten.Attr.ID != created.Attr.ID || overwritten.Attr.Size != 0 {
		t.Fatalf("overwritten=%+v %v", overwritten, err)
	}
	request.Disposition = storage.WindowsSupersede
	superseded, err := ws.Open(t.Context(), request, windowsActionID(t, ws))
	if err != nil || superseded.CreateAction != storage.WindowsSuperseded || superseded.Attr.ID == created.Attr.ID {
		t.Fatalf("superseded=%+v %v", superseded, err)
	}
	if attr, err := f.Stat(t.Context()); err != nil || attr.NameInfo.State != storage.WindowsNameDetached {
		t.Fatalf("priorobject=%+v %v", attr, err)
	}
	request.Disposition = storage.WindowsOverwriteIf
	request.Lookup.Name = "new"
	if result, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); err != nil || result.CreateAction != storage.WindowsCreated {
		t.Fatalf("overwriteif=%+v %v", result, err)
	}
	request.Disposition = storage.WindowsOpen
	request.Lookup.Name = "missing"
	if _, err := ws.Open(t.Context(), request, windowsActionID(t, ws)); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("missingopen=%v", err)
	}
}

func TestWindowsAuthorityRenameUsesBothExpectedIdentities(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "source", storage.WindowsAllAccess, storage.WindowsShareAll)
	target := windowsOpen(t, ws, "target", storage.WindowsAllAccess, storage.WindowsShareAll)
	request := storage.WindowsRenameRequest{Source: storage.WindowsLookup{ParentID: uint64(s.root), Name: "source", ExpectedID: uint64(f.id)}, Destination: storage.WindowsLookup{ParentID: uint64(s.root), Name: "target"}, Replace: true}
	if _, err := f.Rename(t.Context(), request, windowsActionID(t, ws)); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("unanticipatedtarget=%v", err)
	}
	request.Destination.ExpectedID = uint64(target.id)
	result, err := f.Rename(t.Context(), request, windowsActionID(t, ws))
	if err != nil || result.Attr.ID != uint64(f.id) || result.Attr.NameInfo.Path != "target" {
		t.Fatalf("renamed=%+v %v", result, err)
	}
	if attr, err := target.Stat(t.Context()); err != nil || attr.NameInfo.State != storage.WindowsNameDetached {
		t.Fatalf("displaced=%+v %v", attr, err)
	}
	if _, err := s.Stat(t.Context(), "source"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("oldname=%v", err)
	}
	request.Source.Name = "target"
	request.Source.ExpectedID = uint64(target.id)
	if _, err := f.Rename(t.Context(), request, windowsActionID(t, ws)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("sourceidentity=%v", err)
	}
}

func TestWindowsAuthorityKnownStagingFailureAndClosedReferences(t *testing.T) {
	_, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
	if err := f.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Reserve(t.Context(), -1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative reservation=%v", err)
	}
	if _, err := f.Reserve(t.Context(), ws.options.MaxFileSize+1); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("reservation cap=%v", err)
	}
	if _, err := f.Capture(t.Context(), metastore.WindowsIO{Offset: -1}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative read=%v", err)
	}
	id := windowsActionID(t, ws)
	if _, fresh, err := f.BeginContent(t.Context(), id, sha256.Sum256([]byte("failed")), metastore.WindowsIO{Length: 1, Write: true}); err != nil || !fresh {
		t.Fatalf("begin=%t %v", fresh, err)
	}
	if _, err := f.RejectContent(t.Context(), id, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nilrejection=%v", err)
	}
	cause := errors.New("staging provider rejected bytes")
	result, err := f.RejectContent(t.Context(), id, cause)
	if !errors.Is(err, cause) || result.State != storage.WindowsActionRejected {
		t.Fatalf("rejected=%+v %v", result, err)
	}
	if result, err := ws.QueryAction(t.Context(), id); !errors.Is(err, cause) || result.State != storage.WindowsActionRejected {
		t.Fatalf("receipt=%+v %v", result, err)
	}
	if _, err := f.RejectContent(t.Context(), id, syscall.EINVAL); !errors.Is(err, cause) {
		t.Fatalf("rejectionreplay=%v", err)
	}
	if _, err := f.ReadLink(t.Context()); storage.WindowsFailureOf(err) != storage.WindowsNotReparsePoint {
		t.Fatalf("notlink=%v", err)
	}
	closeID := windowsActionID(t, ws)
	if _, err := f.Close(t.Context(), closeID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Close(t.Context(), closeID); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closedsync=%v", err)
	}
	if _, err := f.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closedstat=%v", err)
	}
	if _, err := f.Reserve(t.Context(), 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("closedreserve=%v", err)
	}
}

func TestWindowsAuthorityEnumerationCannotExposeAnOverBudgetPrefix(t *testing.T) {
	s, ws := windowsAuthority(t)
	if err := s.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "directory/first"); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(t.Context(), "directory/second"); err != nil {
		t.Fatal(err)
	}
	d := windowsOpen(t, ws, "directory", storage.WindowsAllAccess, storage.WindowsShareAll)
	result, err := storage.NewWindowsListResult(150, 0, func(_ int, n int64, _ storage.WindowsBasicAttr) (int64, error) { return n + 100, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := d.ListBounded(t.Context(), result); !errors.Is(err, syscall.EIO) {
		t.Fatalf("overflow=%v", err)
	}
	if entries, err := result.Entries(); err == nil || entries != nil {
		t.Fatalf("prefix=%+v %v", entries, err)
	}
}

func TestWindowsAuthorityDetachedLinkKeepsPublicationAccounting(t *testing.T) {
	s, ws := windowsAuthority(t)
	f := windowsOpen(t, ws, "link", storage.WindowsAllAccess, storage.WindowsShareAll)
	if _, err := f.SetLink(t.Context(), "target", windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	observed := 0
	ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		observed++
		if previous != 6 || next != 6 {
			t.Fatalf("unlinkaccounting=%d ->%d", previous, next)
		}
		return func(result storage.PublicationResult) error {
			if result != storage.PublicationApplied {
				t.Fatalf("unlinkresult=%v", result)
			}
			return nil
		}, nil
	})
	if err := s.Remove(ctx, "link"); err != nil {
		t.Fatal(err)
	}
	if observed != 1 {
		t.Fatalf("unlinksettlements=%d", observed)
	}
	ctx = storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		observed++
		if previous != 6 || next != 0 {
			t.Fatalf("closeaccounting=%d ->%d", previous, next)
		}
		return func(result storage.PublicationResult) error {
			if result != storage.PublicationApplied {
				t.Fatalf("closeresult=%v", result)
			}
			return nil
		}, nil
	})
	if _, err := f.Close(ctx, windowsActionID(t, ws)); err != nil {
		t.Fatal(err)
	}
	if observed != 2 {
		t.Fatalf("totalsettlements=%d", observed)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("released=%d %v", used, err)
	}
}

func TestWindowsAuthorityDirectoryLocksRejectBeforeEffects(t *testing.T) {
	for _, symlink := range []bool{false, true} {
		t.Run(fmt.Sprint(symlink), func(t *testing.T) {
			s, ws := windowsAuthority(t)
			if err := s.Mkdir(t.Context(), "directory"); err != nil {
				t.Fatal(err)
			}
			d := windowsOpen(t, ws, "directory", storage.WindowsAllAccess, storage.WindowsShareAll)
			if symlink {
				if _, err := d.SetLink(t.Context(), "target-directory", windowsActionID(t, ws)); err != nil {
					t.Fatal(err)
				}
			}
			before := ws.remainingRanges()
			id := windowsActionID(t, ws)
			batch := storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{{Offset: 0, Length: 10, Type: storage.Exclusive, FailImmediately: true}}}
			result, err := d.LockBatch(t.Context(), batch, id)
			if !errors.Is(err, syscall.EINVAL) || result.State != storage.WindowsActionRejected || result.Errno != syscall.EINVAL || result.Applied != 0 {
				t.Fatalf("directorylock=%+v %v", result, err)
			}
			if ws.remainingRanges() != before || s.fileDomain.windows.access.RangeCount(d.handle) != 0 {
				t.Fatal("rejected lock consumed range capacity")
			}
			if replay, err := ws.QueryAction(t.Context(), id); !errors.Is(err, syscall.EINVAL) || replay.State != storage.WindowsActionRejected || replay.Applied != 0 {
				t.Fatalf("lockreceipt=%+v %v", replay, err)
			}
			if replay, err := d.LockBatch(t.Context(), batch, id); !errors.Is(err, syscall.EINVAL) || replay.State != storage.WindowsActionRejected {
				t.Fatalf("lockreplay=%+v %v", replay, err)
			}
		})
	}
}
