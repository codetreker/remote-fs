package locked_test

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func TestWindowsCapabilityPreservesBackendRefusal(t *testing.T) {
	cause := errors.New("Windows capability unavailable")
	for name, backend := range map[string]locked.Backend{
		"missing": &struct{ locked.Backend }{pairedBackend(t)},
		"refused": &windowsScopeProbe{Backend: pairedBackend(t), checkErr: cause},
	} {
		t.Run(name, func(t *testing.T) {
			facade, err := locked.New(backend)
			if err != nil {
				t.Fatal(err)
			}
			want := error(syscall.EOPNOTSUPP)
			if name == "refused" {
				want = cause
			}
			for _, run := range []func() error{
				facade.CheckWindowsStorage,
				func() error { _, err := facade.WindowsState(t.Context()); return err },
				func() error { _, err := facade.EnableWindows(t.Context(), ""); return err },
				func() error {
					_, err := facade.QueryWindowsActivation(t.Context(), "")
					return err
				},
				func() error {
					_, err := facade.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions())
					return err
				},
			} {
				if err := run(); !errors.Is(err, want) {
					t.Fatalf("capability refusal = %v, want %v", err, want)
				}
			}
		})
	}
}

func TestWindowsScopedHandlesRetainProofsOnlyForMutations(t *testing.T) {
	backend := pairedBackend(t)
	facade, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	state, err := facade.WindowsState(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	activation := windowsAction(t, state.ActionEpoch)
	if _, err := facade.EnableWindows(t.Context(), activation); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(t.Context(), "file", []byte("initial")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	owner, grant := acquire(t, facade.LockService(), "file", locking.Exclusive)
	scope := locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}}
	view, err := facade.WithScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	session, err := view.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	status, err := session.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	next := func() storage.WindowsActionID { return windowsAction(t, status.ActionEpoch) }
	request := storage.WindowsOpenRequest{
		WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen},
		Lookup:            storage.WindowsLookup{ParentID: root.ID, Name: "file"},
	}
	openedAction := next()
	opened, err := session.Open(t.Context(), request, openedAction)
	if err != nil {
		t.Fatal(err)
	}
	for name, file := range map[string]storage.WindowsFile{
		"open": opened.File,
		"query": func() storage.WindowsFile {
			result, err := session.QueryAction(t.Context(), openedAction)
			if err != nil {
				t.Fatal(err)
			}
			return result.File
		}(),
		"cancel completed": func() storage.WindowsFile {
			result, err := session.CancelAction(t.Context(), openedAction)
			if err != nil {
				t.Fatal(err)
			}
			return result.File
		}(),
	} {
		if file == nil || file.Reference() != opened.File.Reference() {
			t.Fatalf("%s lost the retained reference", name)
		}
		if _, err := file.WriteAt(t.Context(), 0, []byte("updated"), next()); err != nil {
			t.Fatalf("%s lost mutation scope: %v", name, err)
		}
	}
	if _, err := facade.LockService().Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	inherited := locking.WithScope(t.Context(), scope)
	if got, err := opened.File.ReadAt(inherited, 0, 7); err != nil || string(got.Data) != "updated" {
		t.Fatalf("read through stale scope = %q, %v", got.Data, err)
	}
	if got, err := opened.File.Stat(inherited); err != nil || got.ID != opened.Attr.ID || got.NameInfo.Path != "file" {
		t.Fatalf("stat through stale scope = %+v, %v", got, err)
	}
	if _, err := session.Open(inherited, request, next()); err != nil {
		t.Fatalf("ordinary open inherited stale proofs: %v", err)
	}
	if _, err := opened.File.WriteAt(t.Context(), 0, []byte("bad"), next()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("mutation through stale scope = %v", err)
	}
	batch := storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{{Offset: 0, Length: 1, Type: storage.Exclusive, FailImmediately: true}}}
	if _, err := opened.File.LockBatch(inherited, batch, next()); err != nil {
		t.Fatalf("range lock inherited stale proofs: %v", err)
	}
	batch.Ranges[0].Type, batch.Ranges[0].FailImmediately = storage.Unlock, false
	if _, err := opened.File.LockBatch(inherited, batch, next()); err != nil {
		t.Fatal(err)
	}
	request.Disposition = storage.WindowsOverwrite
	if _, err := session.Open(t.Context(), request, next()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("overwriting open ignored stale proofs: %v", err)
	}
}

func TestWindowsCloseCannotDeleteThroughAReleasedStrongScope(t *testing.T) {
	for _, closeSession := range []bool{false, true} {
		name := "file"
		if closeSession {
			name = "session"
		}
		t.Run(name, func(t *testing.T) {
			backend := pairedBackend(t)
			facade, err := locked.New(backend)
			if err != nil {
				t.Fatal(err)
			}
			state, err := facade.WindowsState(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := facade.EnableWindows(t.Context(), windowsAction(t, state.ActionEpoch)); err != nil {
				t.Fatal(err)
			}
			if err := backend.Write(t.Context(), "file", []byte("protected")); err != nil {
				t.Fatal(err)
			}
			root, err := backend.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			owner, grant := acquire(t, facade.LockService(), "file", locking.Exclusive)
			view, err := facade.WithScope(locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
			if err != nil {
				t.Fatal(err)
			}
			session, err := view.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := session.Close(locking.WithScope(context.Background(), locking.MutationScope{})); err != nil {
					t.Error(err)
				}
			})
			status, err := session.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			next := func() storage.WindowsActionID { return windowsAction(t, status.ActionEpoch) }
			opened, err := session.Open(t.Context(), storage.WindowsOpenRequest{
				WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen},
				Lookup:            storage.WindowsLookup{ParentID: root.ID, Name: "file"},
			}, next())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := opened.File.SetDeletePending(t.Context(), true, next()); err != nil {
				t.Fatal(err)
			}
			if _, err := facade.LockService().Release(t.Context(), owner, grant); err != nil {
				t.Fatal(err)
			}
			reader, protection := acquire(t, facade.LockService(), "file", locking.Shared)
			t.Cleanup(func() {
				if _, err := facade.LockService().Release(context.Background(), reader, protection); err != nil {
					t.Error(err)
				}
			})
			if closeSession {
				err = session.Close(t.Context())
			} else {
				_, err = opened.File.Close(t.Context(), next())
			}
			if !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("closing with a released scope = %v, want ESTALE", err)
			}
			if got, err := backend.Stat(t.Context(), "file"); err != nil || got.ID != opened.Attr.ID {
				t.Fatalf("failed close unlinked or replaced the protected name: %+v, %v", got, err)
			}
		})
	}
}

func windowsAction(t *testing.T, epoch uint64) storage.WindowsActionID {
	t.Helper()
	id, err := storage.NewLockRequestID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestWindowsForwardingPreservesScopesReceiptsAndErrorHandles(t *testing.T) {
	cause := errors.New("injected Windows observation failure")
	probe := &windowsScopeProbe{Backend: pairedBackend(t), err: cause}
	probe.file = &windowsFileProbe{probe: probe}
	probe.result = storage.WindowsActionResult{
		Action: windowsAction(t, 17), State: storage.WindowsActionCompleted,
		Attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{
			Attr: storage.Attr{ID: 72, Size: 3}, CreationTime: time.Unix(100, 2).UTC(),
			ChangeTime: time.Unix(101, 3).UTC(), DOSAttributes: storage.WindowsDOSHidden,
		}, NameInfo: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "dir/file"}},
		File: probe.file, CreateAction: storage.WindowsOpened, Errno: syscall.EIO,
		Failure: storage.WindowsLockConflict, Applied: 2, HistoryRemaining: time.Minute,
	}
	facade, err := locked.New(probe)
	if err != nil {
		t.Fatal(err)
	}
	scope := locking.MutationScope{
		Owner:  locking.OwnerRef{Session: "session", Owner: "owner"},
		Grants: []locking.GrantRef{{ID: "grant", Resource: "resource", Generation: 1}},
	}
	view, err := facade.WithScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	session, err := view.NewWindowsSession(locking.WithScope(t.Context(), scope), storage.DefaultFileSessionOptions())
	if err != nil || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("session creation inherited scope: %+v, %v", probe.observed, err)
	}
	id := probe.result.Action
	opened, err := session.Open(t.Context(), storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Disposition: storage.WindowsOpen}}, id)
	if !errors.Is(err, cause) || opened.File == nil || !reflect.DeepEqual(opened.Attr, probe.result.Attr) || opened.CreateAction != probe.result.CreateAction {
		t.Fatalf("open lost its error or result: %+v, %v", opened, err)
	}
	file := opened.File
	checkResult := func(result storage.WindowsActionResult, err error) (storage.WindowsFile, error) {
		t.Helper()
		got, want := result, probe.result
		got.File, want.File = nil, nil
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("forwarded receipt = %+v, want %+v", got, want)
		}
		return result.File, err
	}
	for _, test := range []struct {
		name     string
		mutation bool
		run      func(context.Context) (storage.WindowsFile, error)
	}{
		{"state", false, func(ctx context.Context) (storage.WindowsFile, error) {
			_, err := view.WindowsState(ctx)
			return nil, err
		}},
		{"enable", true, func(ctx context.Context) (storage.WindowsFile, error) {
			_, err := view.EnableWindows(ctx, id)
			return nil, err
		}},
		{"query activation", false, func(ctx context.Context) (storage.WindowsFile, error) {
			_, err := view.QueryWindowsActivation(ctx, id)
			return nil, err
		}},
		{"query", false, func(ctx context.Context) (storage.WindowsFile, error) {
			return checkResult(session.QueryAction(ctx, id))
		}},
		{"cancel", false, func(ctx context.Context) (storage.WindowsFile, error) {
			return checkResult(session.CancelAction(ctx, id))
		}},
		{"renew", false, func(ctx context.Context) (storage.WindowsFile, error) { _, err := session.Renew(ctx); return nil, err }},
		{"status", false, func(ctx context.Context) (storage.WindowsFile, error) { _, err := session.Status(ctx); return nil, err }},
		{"session close", true, func(ctx context.Context) (storage.WindowsFile, error) { return nil, session.Close(ctx) }},
		{"stat", false, func(ctx context.Context) (storage.WindowsFile, error) { _, err := file.Stat(ctx); return nil, err }},
		{"read", false, func(ctx context.Context) (storage.WindowsFile, error) {
			_, err := file.ReadAt(ctx, 2, 5)
			return nil, err
		}},
		{"write", true, func(ctx context.Context) (storage.WindowsFile, error) {
			return checkResult(file.WriteAt(ctx, 2, []byte("bytes"), id))
		}},
		{"truncate", true, func(ctx context.Context) (storage.WindowsFile, error) { return checkResult(file.Truncate(ctx, 2, id)) }},
		{"attributes", true, func(ctx context.Context) (storage.WindowsFile, error) {
			return checkResult(file.SetAttr(ctx, storage.WindowsAttrChange{}, id))
		}},
		{"list", false, func(ctx context.Context) (storage.WindowsFile, error) { return nil, file.ListBounded(ctx, nil) }},
		{"read link", false, func(ctx context.Context) (storage.WindowsFile, error) {
			info, err := file.ReadLink(ctx)
			want := storage.WindowsSymlinkInfo{Target: "target", Location: probe.result.Attr.NameInfo, Unparsed: "child"}
			if info != want {
				t.Fatalf("retained link information = %+v, want %+v", info, want)
			}
			return nil, err
		}},
		{"set link", true, func(ctx context.Context) (storage.WindowsFile, error) {
			return checkResult(file.SetLink(ctx, "target", id))
		}},
		{"rename", true, func(ctx context.Context) (storage.WindowsFile, error) {
			return checkResult(file.Rename(ctx, storage.WindowsRenameRequest{}, id))
		}},
		{"delete pending", true, func(ctx context.Context) (storage.WindowsFile, error) {
			return checkResult(file.SetDeletePending(ctx, true, id))
		}},
		{"locks", false, func(ctx context.Context) (storage.WindowsFile, error) {
			return checkResult(file.LockBatch(ctx, storage.WindowsLockBatch{}, id))
		}},
		{"sync", false, func(ctx context.Context) (storage.WindowsFile, error) { return nil, file.Sync(ctx) }},
		{"file close", true, func(ctx context.Context) (storage.WindowsFile, error) { return checkResult(file.Close(ctx, id)) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			returned, err := test.run(locking.WithScope(t.Context(), scope))
			if !errors.Is(err, cause) {
				t.Fatalf("operation lost its failure: %v", err)
			}
			want := locking.MutationScope{}
			if test.mutation {
				want = scope
			}
			if !reflect.DeepEqual(probe.observed, want) {
				t.Fatalf("scope = %+v, want %+v", probe.observed, want)
			}
			if returned != nil {
				if returned == probe.file || returned.Reference() != probe.file.Reference() {
					t.Fatal("receipt exposed an unwrapped or changed reference")
				}
				if _, err := returned.WriteAt(t.Context(), 0, nil, id); !errors.Is(err, cause) || !reflect.DeepEqual(probe.observed, scope) {
					t.Fatalf("receipt file lost frozen scope: %+v, %v", probe.observed, err)
				}
			}
		})
	}
	for _, disposition := range []storage.WindowsDisposition{storage.WindowsOpen, storage.WindowsCreate, storage.WindowsOpenIf, storage.WindowsOverwrite, storage.WindowsOverwriteIf, storage.WindowsSupersede} {
		for _, deleteOnClose := range []bool{false, true} {
			_, err := session.Open(t.Context(), storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Disposition: disposition, DeleteOnClose: deleteOnClose}}, id)
			want := locking.MutationScope{}
			if disposition != storage.WindowsOpen || deleteOnClose {
				want = scope
			}
			if !errors.Is(err, cause) || !reflect.DeepEqual(probe.observed, want) {
				t.Fatalf("open %d delete %v scope = %+v, %v", disposition, deleteOnClose, probe.observed, err)
			}
		}
	}
	if _, err := file.WriteAt(locking.WithScope(t.Context(), locking.MutationScope{}), 0, nil, id); !errors.Is(err, cause) || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("explicit anonymous mutation inherited a frozen proof: %+v, %v", probe.observed, err)
	}
	probe.result.File = nil
	if result, err := session.QueryAction(t.Context(), id); !errors.Is(err, cause) || result.File != nil {
		t.Fatalf("receipt without a file = %+v, %v", result, err)
	}
}

type windowsScopeProbe struct {
	locked.Backend
	checkErr error
	err      error
	observed locking.MutationScope
	result   storage.WindowsActionResult
	file     *windowsFileProbe
}

func (p *windowsScopeProbe) record(ctx context.Context) error {
	p.observed = locking.ScopeFromContext(ctx)
	return p.err
}

func (p *windowsScopeProbe) CheckWindowsStorage() error { return p.checkErr }
func (p *windowsScopeProbe) WindowsState(ctx context.Context) (storage.WindowsState, error) {
	return storage.WindowsState{}, p.record(ctx)
}
func (p *windowsScopeProbe) EnableWindows(ctx context.Context, _ storage.WindowsActionID) (storage.WindowsActivation, error) {
	return storage.WindowsActivation{}, p.record(ctx)
}
func (p *windowsScopeProbe) QueryWindowsActivation(ctx context.Context, _ storage.WindowsActionID) (storage.WindowsActivation, error) {
	return storage.WindowsActivation{}, p.record(ctx)
}
func (p *windowsScopeProbe) NewWindowsSession(ctx context.Context, _ storage.FileSessionOptions) (storage.WindowsSession, error) {
	p.record(ctx)
	return &windowsSessionProbe{probe: p}, nil
}

type windowsSessionProbe struct{ probe *windowsScopeProbe }

func (s *windowsSessionProbe) Open(ctx context.Context, _ storage.WindowsOpenRequest, _ storage.WindowsActionID) (storage.WindowsOpenResult, error) {
	r := s.probe.result
	return storage.WindowsOpenResult{File: r.File, Attr: r.Attr, CreateAction: r.CreateAction}, s.probe.record(ctx)
}
func (s *windowsSessionProbe) QueryAction(ctx context.Context, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return s.probe.result, s.probe.record(ctx)
}
func (s *windowsSessionProbe) CancelAction(ctx context.Context, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return s.probe.result, s.probe.record(ctx)
}
func (s *windowsSessionProbe) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{}, s.probe.record(ctx)
}
func (s *windowsSessionProbe) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{}, s.probe.record(ctx)
}
func (s *windowsSessionProbe) Close(ctx context.Context) error { return s.probe.record(ctx) }

type windowsFileProbe struct{ probe *windowsScopeProbe }

func (f *windowsFileProbe) Reference() string { return "retained-reference" }
func (f *windowsFileProbe) Stat(ctx context.Context) (storage.WindowsAttr, error) {
	return f.probe.result.Attr, f.probe.record(ctx)
}
func (f *windowsFileProbe) ReadAt(ctx context.Context, _ int64, _ int) (storage.FileRead, error) {
	return storage.FileRead{}, f.probe.record(ctx)
}
func (f *windowsFileProbe) WriteAt(ctx context.Context, _ int64, _ []byte, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.probe.result, f.probe.record(ctx)
}
func (f *windowsFileProbe) Truncate(ctx context.Context, _ int64, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.probe.result, f.probe.record(ctx)
}
func (f *windowsFileProbe) SetAttr(ctx context.Context, _ storage.WindowsAttrChange, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.probe.result, f.probe.record(ctx)
}
func (f *windowsFileProbe) ListBounded(ctx context.Context, _ *storage.WindowsListResult) error {
	return f.probe.record(ctx)
}
func (f *windowsFileProbe) ReadLink(ctx context.Context) (storage.WindowsSymlinkInfo, error) {
	return storage.WindowsSymlinkInfo{Target: "target", Location: f.probe.result.Attr.NameInfo, Unparsed: "child"}, f.probe.record(ctx)
}
func (f *windowsFileProbe) SetLink(ctx context.Context, _ string, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.probe.result, f.probe.record(ctx)
}
func (f *windowsFileProbe) Rename(ctx context.Context, _ storage.WindowsRenameRequest, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.probe.result, f.probe.record(ctx)
}
func (f *windowsFileProbe) SetDeletePending(ctx context.Context, _ bool, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.probe.result, f.probe.record(ctx)
}
func (f *windowsFileProbe) LockBatch(ctx context.Context, _ storage.WindowsLockBatch, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.probe.result, f.probe.record(ctx)
}
func (f *windowsFileProbe) Sync(ctx context.Context) error { return f.probe.record(ctx) }
func (f *windowsFileProbe) Close(ctx context.Context, _ storage.WindowsActionID) (storage.WindowsActionResult, error) {
	return f.probe.result, f.probe.record(ctx)
}
