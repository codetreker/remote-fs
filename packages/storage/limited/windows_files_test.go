package limited_test

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
)

func windowsAction(t *testing.T, epoch uint64) storage.WindowsActionID {
	t.Helper()
	action, err := storage.NewLockRequestID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

func windowsQuotaSession(t *testing.T, s storage.WindowsStorage) (storage.WindowsSession, uint64, uint64) {
	t.Helper()
	state, err := s.WindowsState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	activation := windowsAction(t, state.ActionEpoch)
	enabled, err := s.EnableWindows(t.Context(), activation)
	if err != nil || !enabled.Enabled {
		t.Fatalf("enable Windows = %+v, %v", enabled, err)
	}
	observed, err := s.QueryWindowsActivation(t.Context(), activation)
	if err != nil || observed.Action != activation || !observed.Enabled {
		t.Fatalf("query activation = %+v, %v", observed, err)
	}
	root, err := s.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session, err := s.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("closing Windows session: %v", err)
		}
	})
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return session, root.ID, status.ActionEpoch
}

func windowsQuotaFile(t *testing.T, session storage.WindowsSession, parent, epoch uint64, name string) (storage.WindowsFile, storage.WindowsActionID) {
	t.Helper()
	action := windowsAction(t, epoch)
	opened, err := session.Open(t.Context(), storage.WindowsOpenRequest{
		WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsCreate, Kind: storage.WindowsRegularFile},
		Lookup:            storage.WindowsLookup{ParentID: parent, Name: name}, Mode: 0o600,
	}, action)
	if err != nil || opened.File == nil || opened.CreateAction != storage.WindowsCreated {
		t.Fatalf("create Windows file = %+v, %v", opened, err)
	}
	return opened.File, action
}

func TestWindowsQuotaRetainsReplayFilesAndSettlesFinalCloseOnce(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session, parent, epoch := windowsQuotaSession(t, s)
	file, openAction := windowsQuotaFile(t, session, parent, epoch, "file")
	writeAction := windowsAction(t, epoch)
	written, err := file.WriteAt(t.Context(), 0, content(limited.MinLimit), writeAction)
	if err != nil || written.Attr.Size != limited.MinLimit {
		t.Fatalf("write = %+v, %v", written, err)
	}
	mustUse(t, s, limited.MinLimit)
	if _, err := file.WriteAt(t.Context(), 0, content(limited.MinLimit), writeAction); err != nil {
		t.Fatalf("write replay charged again: %v", err)
	}
	mustUse(t, s, limited.MinLimit)
	for _, query := range []func(context.Context, storage.WindowsActionID) (storage.WindowsActionResult, error){session.QueryAction, session.CancelAction} {
		replay, err := query(t.Context(), openAction)
		if err != nil || replay.File == nil || replay.File.Reference() != file.Reference() {
			t.Fatalf("open receipt lost its reference: %+v, %v", replay, err)
		}
		if _, err := replay.File.WriteAt(t.Context(), limited.MinLimit, []byte{1}, windowsAction(t, epoch)); !errors.Is(err, syscall.EDQUOT) {
			t.Fatalf("receipt file bypassed the allowance: %v", err)
		}
		mustUse(t, s, limited.MinLimit)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, limited.MinLimit)
	closeAction := windowsAction(t, epoch)
	if _, err := file.Close(t.Context(), closeAction); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
	if _, err := file.Close(t.Context(), closeAction); err != nil {
		t.Fatalf("close replay failed: %v", err)
	}
	mustUse(t, s, 0)
	if _, err := s.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("closing a retained reference recreated its name: %v", err)
	}
}

func TestWindowsQuotaSessionCloseRetiresEveryChargedReference(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session, parent, epoch := windowsQuotaSession(t, s)
	file, _ := windowsQuotaFile(t, session, parent, epoch, "file")
	if _, err := file.WriteAt(t.Context(), 0, content(1000), windowsAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Truncate(t.Context(), 2000, windowsAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 2000)
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("closed session left its reference valid: %v", err)
	}
}

func TestWindowsQuotaRejectsMissingCapabilitiesAndUnmeasuredRetention(t *testing.T) {
	backing := newBacking(t)
	s := newStorageOver(t, &faulty{BoundedStorage: backing}, limited.MinLimit)
	for name, operation := range map[string]func() error{
		"capability":       s.CheckWindowsStorage,
		"state":            func() error { _, err := s.WindowsState(t.Context()); return err },
		"activation":       func() error { _, err := s.EnableWindows(t.Context(), ""); return err },
		"activation query": func() error { _, err := s.QueryWindowsActivation(t.Context(), ""); return err },
		"session": func() error {
			_, err := s.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions())
			return err
		},
	} {
		if err := operation(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Errorf("%s without Windows capability = %v", name, err)
		}
	}
	windows := backing.(storage.WindowsStorage)
	missing := &windowsWithoutUsage{WindowsStorage: windows}
	if _, err := limited.New(t.Context(), missing, limited.MinLimit); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("Windows references without authoritative usage = %v", err)
	}
	failure := errors.New("injected Windows authority failure")
	missing.check = failure
	if _, err := limited.New(t.Context(), missing, limited.MinLimit); err != failure {
		t.Fatalf("Windows capability failure = %v, want original cause", err)
	}
}

type windowsWithoutUsage struct {
	storage.WindowsStorage
	check error
}

func (s *windowsWithoutUsage) CheckWindowsStorage() error {
	if s.check != nil {
		return s.check
	}
	return s.WindowsStorage.CheckWindowsStorage()
}

func (s *windowsWithoutUsage) CheckPublicationAccounting() error {
	return checkDelegatedAccounting(s.WindowsStorage)
}

func TestWindowsQuotaMetadataRenameAndDeleteUseRetainedIdentity(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session, parent, epoch := windowsQuotaSession(t, s)
	file, _ := windowsQuotaFile(t, session, parent, epoch, "before")
	written, err := file.WriteAt(t.Context(), 0, content(1000), windowsAction(t, epoch))
	if err != nil {
		t.Fatal(err)
	}
	hidden := uint32(storage.WindowsDOSHidden)
	if _, err := file.SetAttr(t.Context(), storage.WindowsAttrChange{DOSAttributes: &hidden}, windowsAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []storage.LockType{storage.Shared, storage.Unlock} {
		if _, err := file.LockBatch(t.Context(), storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{{Length: 10, Type: kind}}}, windowsAction(t, epoch)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := file.Rename(t.Context(), storage.WindowsRenameRequest{
		Source:      storage.WindowsLookup{ParentID: parent, Name: "before", ExpectedID: written.Attr.ID},
		Destination: storage.WindowsLookup{ParentID: parent, Name: "after"},
	}, windowsAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	attr, err := file.Stat(t.Context())
	if err != nil || attr.ID != written.Attr.ID || attr.NameInfo.Path != "after" || attr.DOSAttributes != hidden {
		t.Fatalf("renamed retained metadata = %+v, %v", attr, err)
	}
	mustUse(t, s, 1000)
	if _, err := file.SetDeletePending(t.Context(), true, windowsAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1000)
	if _, err := file.Close(t.Context(), windowsAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
	if _, err := s.Stat(t.Context(), "after"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("delete-on-close left the renamed node: %v", err)
	}
}

func TestWindowsQuotaLinkTargetRequiresCapacityBeforeConversion(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session, parent, epoch := windowsQuotaSession(t, s)
	mustWrite(t, s, "held", limited.MinLimit-10)
	file, _ := windowsQuotaFile(t, session, parent, epoch, "link")
	before, err := file.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.SetLink(t.Context(), "elevenbytes", windowsAction(t, epoch)); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("link target bypassed the allowance: %v", err)
	}
	unchanged, err := file.Stat(t.Context())
	if err != nil || unchanged.ID != before.ID || !unchanged.Mode.IsRegular() || unchanged.Size != 0 {
		t.Fatalf("rejected link conversion changed the file: %+v, %v", unchanged, err)
	}
	if _, err := file.SetLink(t.Context(), "target", windowsAction(t, epoch)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, limited.MinLimit-10+6)
	want := storage.WindowsSymlinkInfo{Target: "target", Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "link"}}
	if info, err := file.ReadLink(t.Context()); err != nil || info != want {
		t.Fatalf("retained link information = %+v, %v; want %+v", info, err, want)
	}
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, limited.MinLimit-10+6)
}
