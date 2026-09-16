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
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func fileAction(t *testing.T, epoch uint64) storage.FileActionID {
	t.Helper()
	id, err := storage.NewFileActionID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func scopedSession(t *testing.T, backend storage.FileStorage) (storage.FileSession, func() storage.FileActionID) {
	t.Helper()
	session, status, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	next := func() storage.FileActionID { return fileAction(t, status.ActionEpoch) }
	t.Cleanup(func() {
		if _, err := session.Close(locking.WithScope(context.Background(), locking.MutationScope{}), next()); err != nil {
			t.Error(err)
		}
	})
	return session, next
}
func retainedFile(t *testing.T, session storage.FileSession, id uint64, next func() storage.FileActionID) (storage.File, storage.FileActionReceipt) {
	t.Helper()
	receipt, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: id, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, next())
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.Reference(t.Context(), receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	return file, receipt
}

func TestRetainedScopePreservesIdentityReceiptsAndReadIsolation(t *testing.T) {
	backend := pairedBackend(t)
	if err := backend.Write(t.Context(), "file", []byte("initial")); err != nil {
		t.Fatal(err)
	}
	facade, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	attr, err := backend.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	owner, grant := acquire(t, facade.LockService(), "file", locking.Exclusive)
	scope := locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}}
	view, err := facade.WithScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	session, next := scopedSession(t, view)
	file, opened := retainedFile(t, session, attr.ID, next)
	for _, query := range []func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error){session.QueryAction, session.CancelAction} {
		receipt, err := query(t.Context(), opened.Action)
		if err != nil || receipt.Reference != file.Reference() {
			t.Fatalf("retained action = %+v, %v", receipt, err)
		}
		resolved, err := session.Reference(t.Context(), receipt.Reference)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resolved.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("updated")}, next()); err != nil {
			t.Fatalf("resolved reference lost scope: %v", err)
		}
	}
	observed, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil {
		t.Fatal(err)
	}
	metadata := storage.Metadata{{Key: "test", Version: 1, Data: []byte("scoped")}}
	if _, err := file.SetAttr(t.Context(), storage.AttrChange{ExpectedRevision: observed.Attr.MetadataRevision, Metadata: &metadata}, next()); err != nil {
		t.Fatal(err)
	}
	if _, err := facade.LockService().Release(t.Context(), owner, grant); err != nil {
		t.Fatal(err)
	}
	inherited := locking.WithScope(t.Context(), scope)
	if got, err := file.ReadAt(inherited, storage.FileReadRequest{Length: 7}); err != nil || string(got.Data) != "updated" {
		t.Fatalf("read with stale proof = %q, %v", got.Data, err)
	}
	if got, err := file.Stat(inherited, storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); err != nil || got.Attr.ID != attr.ID || !reflect.DeepEqual(got.Attr.Metadata, metadata) {
		t.Fatalf("stat with stale proof = %+v, %v", got, err)
	}
	if _, err := session.Retain(inherited, storage.RetainRequest{NodeID: attr.ID}, next()); err != nil {
		t.Fatalf("identity retain inherited proof: %v", err)
	}
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("bad")}, next()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("stale mutation = %v", err)
	}
	modified := time.Unix(1700000000, 0)
	if _, err := session.SetNodeAttr(t.Context(), attr.ID, storage.AttrChange{ModTime: &modified}, next()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("stale node mutation = %v", err)
	}
	if err := file.Sync(inherited); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(locking.WithScope(t.Context(), locking.MutationScope{}), storage.FileWriteRequest{Data: []byte("good")}, next()); err != nil {
		t.Fatalf("explicit anonymous scope = %v", err)
	}
}

func TestRetainedFacadeForwardsQuotaCapabilityAndDetachedUsage(t *testing.T) {
	facade, err := locked.New(pairedBackend(t))
	if err != nil {
		t.Fatal(err)
	}
	quota, err := limited.New(t.Context(), facade, limited.MinLimit)
	if err != nil {
		t.Fatal(err)
	}
	if err := quota.Write(t.Context(), "file", []byte("held")); err != nil {
		t.Fatal(err)
	}
	attr, err := quota.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	session, next := scopedSession(t, quota)
	file, _ := retainedFile(t, session, attr.ID, next)
	if err := quota.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if err := quota.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	if used, err := facade.Usage(t.Context()); err != nil || used != 4 {
		t.Fatalf("retained usage = %d, %v", used, err)
	}
	if _, err := file.Close(t.Context(), next()); err != nil {
		t.Fatal(err)
	}
	if space, err := quota.Space(t.Context()); err != nil || space.Used != 0 {
		t.Fatalf("closed usage = %+v, %v", space, err)
	}
}

func TestRetainedFacadeRejectsUnsupportedBackend(t *testing.T) {
	backend := &missingAuthority{service: pairedBackend(t).LockService()}
	facade, err := locked.New(backend)
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range []func() error{facade.CheckFileStorage, func() error { _, err := facade.FileState(t.Context()); return err }, func() error {
		_, _, err := facade.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
		return err
	}} {
		if err := run(); err != syscall.EOPNOTSUPP {
			t.Fatalf("unsupported capability = %v", err)
		}
	}
	if _, err := facade.Usage(t.Context()); err != syscall.ENOSYS {
		t.Fatalf("unsupported usage = %v", err)
	}
}

func TestRetirementCannotDeleteThroughReleasedStrongScope(t *testing.T) {
	for _, closeSession := range []bool{false, true} {
		t.Run(map[bool]string{false: "reference", true: "session"}[closeSession], func(t *testing.T) {
			backend := pairedBackend(t)
			if err := backend.Write(t.Context(), "file", []byte("protected")); err != nil {
				t.Fatal(err)
			}
			facade, err := locked.New(backend)
			if err != nil {
				t.Fatal(err)
			}
			attr, err := backend.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			owner, grant := acquire(t, facade.LockService(), "file", locking.Exclusive)
			view, err := facade.WithScope(locking.MutationScope{Owner: owner, Grants: []locking.GrantRef{grant}})
			if err != nil {
				t.Fatal(err)
			}
			session, next := scopedSession(t, view)
			file, _ := retainedFile(t, session, attr.ID, next)
			observed, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
			if err != nil {
				t.Fatal(err)
			}
			leaf := observed.Location.Ancestors[len(observed.Location.Ancestors)-1]
			if _, err := file.DrainEntry(t.Context(), storage.DrainEntryRequest{ExpectedMetadataRevision: observed.Attr.MetadataRevision, Entry: leaf, Witness: *observed.Location, Condition: storage.RemovalFile}, next()); err != nil {
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
				_, err = session.Close(t.Context(), next())
			} else {
				_, err = file.Close(t.Context(), next())
			}
			if !errors.Is(err, syscall.ESTALE) {
				t.Fatalf("retire with stale scope = %v", err)
			}
			if got, err := backend.Stat(t.Context(), "file"); err != nil || got.ID != attr.ID {
				t.Fatalf("failed retirement changed protected identity: %+v, %v", got, err)
			}
		})
	}
}

func TestGenericForwardingPreservesScopesAndErrorReceipts(t *testing.T) {
	cause := errors.New("injected observation failure")
	probe := &fileScopeProbe{Backend: pairedBackend(t), err: cause}
	probe.receipt = storage.FileActionReceipt{Action: fileAction(t, 17), State: storage.FileActionCompleted, Reference: 72, Effects: storage.EffectContentChanged, Errno: syscall.EIO, HistoryRemaining: time.Minute, Observation: storage.FileObservation{Attr: storage.Attr{ID: 90, Kind: storage.NodeRegular, Size: 3, MetadataRevision: 5, Metadata: storage.Metadata{{Key: "test", Version: 1, Data: []byte("owned")}}}}}
	facade, err := locked.New(probe)
	if err != nil {
		t.Fatal(err)
	}
	scope := locking.MutationScope{Owner: locking.OwnerRef{Session: "session", Owner: "owner"}, Grants: []locking.GrantRef{{ID: "grant", Resource: "resource", Generation: 1}}}
	view, err := facade.WithScope(scope)
	if err != nil {
		t.Fatal(err)
	}
	session, initial, err := view.NewFileSession(locking.WithScope(t.Context(), scope), storage.DefaultFileSessionOptions())
	if err != nil || initial.Epoch != "initial" || initial.ActionEpoch != 17 || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("session scope = %+v, %v", probe.observed, err)
	}
	file, err := session.Reference(t.Context(), probe.receipt.Reference)
	if !errors.Is(err, cause) || file == nil || file.Reference() != probe.receipt.Reference {
		t.Fatalf("reference plus error = %v, %v", file, err)
	}
	check := func(receipt storage.FileActionReceipt, err error) error {
		t.Helper()
		if !reflect.DeepEqual(receipt, probe.receipt) {
			t.Fatalf("receipt = %+v, want %+v", receipt, probe.receipt)
		}
		return err
	}
	id := probe.receipt.Action
	prepared := storage.RemovalFile
	for _, test := range []struct {
		name     string
		mutation bool
		run      func(context.Context) error
	}{
		{"state", false, func(ctx context.Context) error { _, err := view.FileState(ctx); return err }},
		{"reference", false, func(ctx context.Context) error { _, err := session.Reference(ctx, 72); return err }},
		{"stat node", false, func(ctx context.Context) error {
			_, err := session.StatNode(ctx, 90, storage.ObservationOptions{})
			return err
		}},
		{"set node", true, func(ctx context.Context) error { return check(session.SetNodeAttr(ctx, 90, storage.AttrChange{}, id)) }},
		{"query", false, func(ctx context.Context) error { return check(session.QueryAction(ctx, id)) }},
		{"cancel", false, func(ctx context.Context) error { return check(session.CancelAction(ctx, id)) }},
		{"retire owner", false, func(ctx context.Context) error { return check(session.RetireRangeOwner(ctx, 1, id)) }},
		{"renew", false, func(ctx context.Context) error { _, err := session.Renew(ctx); return err }},
		{"status", false, func(ctx context.Context) error { _, err := session.Status(ctx); return err }},
		{"session close", true, func(ctx context.Context) error { return check(session.Close(ctx, id)) }},
		{"stat", false, func(ctx context.Context) error {
			_, err := file.Stat(ctx, storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
			return err
		}},
		{"read", false, func(ctx context.Context) error {
			_, err := file.ReadAt(ctx, storage.FileReadRequest{Length: 1})
			return err
		}},
		{"lookup", false, func(ctx context.Context) error { _, err := file.LookupAt(ctx, []byte("name")); return err }},
		{"list", false, func(ctx context.Context) error {
			_, err := file.ListAt(ctx, storage.DirectoryPageRequest{})
			return err
		}},
		{"check observation", false, func(ctx context.Context) error {
			_, err := file.CheckObservation(ctx, storage.ObservationCondition{})
			return err
		}},
		{"file retire owner", false, func(ctx context.Context) error { return check(file.RetireRangeOwner(ctx, 1, storage.RangeScope{}, id)) }},
		{"range snapshot", false, func(ctx context.Context) error {
			_, err := file.RangeSnapshot(ctx, 1, storage.RangeScope{})
			return err
		}},
		{"sync", false, func(ctx context.Context) error { return file.Sync(ctx) }},
		{"cancel prepared", true, func(ctx context.Context) error { return check(file.CancelPrepared(ctx, 1, id)) }},
		{"file close", true, func(ctx context.Context) error { return check(file.Close(ctx, id)) }},
		{"Retain", false, func(ctx context.Context) error { return check(session.Retain(ctx, storage.RetainRequest{}, id)) }},
		{"RetainAt", false, func(ctx context.Context) error { return check(session.RetainAt(ctx, storage.RetainAtRequest{}, id)) }},
		{"CreateAndRetainAt", true, func(ctx context.Context) error {
			return check(session.CreateAndRetainAt(ctx, storage.CreateAndRetainRequest{}, id))
		}},
		{"ResetAndRetainAt", true, func(ctx context.Context) error {
			return check(session.ResetAndRetainAt(ctx, storage.ResetAndRetainRequest{}, id))
		}},
		{"ReplaceAndRetainAt", true, func(ctx context.Context) error {
			return check(session.ReplaceAndRetainAt(ctx, storage.CreateAndRetainRequest{}, id))
		}},
		{"WriteAt", true, func(ctx context.Context) error { return check(file.WriteAt(ctx, storage.FileWriteRequest{}, id)) }},
		{"Truncate", true, func(ctx context.Context) error { return check(file.Truncate(ctx, storage.FileTruncateRequest{}, id)) }},
		{"SetAttr", true, func(ctx context.Context) error { return check(file.SetAttr(ctx, storage.AttrChange{}, id)) }},
		{"SetKind", true, func(ctx context.Context) error { return check(file.SetKind(ctx, storage.SetKindRequest{}, id)) }},
		{"Rename", true, func(ctx context.Context) error { return check(file.Rename(ctx, storage.RenameRequest{}, id)) }},
		{"ReplaceClaim", false, func(ctx context.Context) error { return check(file.ReplaceClaim(ctx, storage.AccessClaim{}, id)) }},
		{"PrepareRemoval", true, func(ctx context.Context) error {
			return check(file.PrepareRemoval(ctx, storage.PrepareRemovalRequest{}, id))
		}},
		{"DrainEntry", true, func(ctx context.Context) error { return check(file.DrainEntry(ctx, storage.DrainEntryRequest{}, id)) }},
		{"CancelDrain", true, func(ctx context.Context) error { return check(file.CancelDrain(ctx, storage.CancelDrainRequest{}, id)) }},
		{"ReplaceRanges", false, func(ctx context.Context) error {
			return check(file.ReplaceRanges(ctx, storage.RangeReplaceRequest{}, id))
		}},
		{"WaitRanges", false, func(ctx context.Context) error { return check(file.WaitRanges(ctx, storage.RangeWaitRequest{}, id)) }},

		{"retain prepared", true, func(ctx context.Context) error {
			return check(session.Retain(ctx, storage.RetainRequest{Prepared: &prepared}, id))
		}},
		{"retain at prepared", true, func(ctx context.Context) error {
			return check(session.RetainAt(ctx, storage.RetainAtRequest{Prepared: &prepared}, id))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(locking.WithScope(t.Context(), scope)); !errors.Is(err, cause) {
				t.Fatalf("lost error: %v", err)
			}
			want := locking.MutationScope{}
			if test.mutation {
				want = scope
			}
			if !reflect.DeepEqual(probe.observed, want) {
				t.Fatalf("scope = %+v, want %+v", probe.observed, want)
			}
		})
	}
	if _, err := file.WriteAt(locking.WithScope(t.Context(), locking.MutationScope{}), storage.FileWriteRequest{}, id); !errors.Is(err, cause) || !reflect.DeepEqual(probe.observed, locking.MutationScope{}) {
		t.Fatalf("anonymous write inherited frozen proof: %+v, %v", probe.observed, err)
	}
	probe.noReference = true
	if got, err := session.Reference(t.Context(), 72); got != nil || !errors.Is(err, cause) {
		t.Fatalf("missing reference = %v, %v", got, err)
	}
	probe.checkErr = cause
	if err := facade.CheckFileStorage(); err != cause {
		t.Fatalf("lost backend refusal: %v", err)
	}
}

type fileScopeProbe struct {
	locked.Backend
	err, checkErr error
	observed      locking.MutationScope
	receipt       storage.FileActionReceipt
	noReference   bool
}

func (p *fileScopeProbe) record(ctx context.Context) error {
	p.observed = locking.ScopeFromContext(ctx)
	return p.err
}
func (p *fileScopeProbe) CheckFileStorage() error { return p.checkErr }
func (p *fileScopeProbe) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	return storage.FileVolumeState{}, p.record(ctx)
}
func (p *fileScopeProbe) NewFileSession(ctx context.Context, _ storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	p.record(ctx)
	return &fileSessionProbe{probe: p}, storage.FileSessionStatus{Epoch: "initial", ActionEpoch: 17}, nil
}

type fileSessionProbe struct{ probe *fileScopeProbe }

func (s *fileSessionProbe) Reference(ctx context.Context, _ storage.FileReferenceID) (storage.File, error) {
	if s.probe.noReference {
		return nil, s.probe.record(ctx)
	}
	return &retainedProbe{probe: s.probe}, s.probe.record(ctx)
}
func (s *fileSessionProbe) StatNode(ctx context.Context, _ uint64, _ storage.ObservationOptions) (storage.FileObservation, error) {
	return s.probe.receipt.Observation, s.probe.record(ctx)
}
func (s *fileSessionProbe) SetNodeAttr(ctx context.Context, _ uint64, _ storage.AttrChange, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) RetireRangeOwner(ctx context.Context, _ storage.RangeOwnerID, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) Retain(ctx context.Context, _ storage.RetainRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) RetainAt(ctx context.Context, _ storage.RetainAtRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) CreateAndRetainAt(ctx context.Context, _ storage.CreateAndRetainRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) ResetAndRetainAt(ctx context.Context, _ storage.ResetAndRetainRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) ReplaceAndRetainAt(ctx context.Context, _ storage.CreateAndRetainRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) QueryAction(ctx context.Context, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) CancelAction(ctx context.Context, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) Close(ctx context.Context, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.probe.receipt, s.probe.record(ctx)
}
func (s *fileSessionProbe) Renew(ctx context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{}, s.probe.record(ctx)
}
func (s *fileSessionProbe) Status(ctx context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{}, s.probe.record(ctx)
}

type retainedProbe struct{ probe *fileScopeProbe }

func (f *retainedProbe) NodeID() uint64                     { return f.probe.receipt.Observation.Attr.ID }
func (f *retainedProbe) Reference() storage.FileReferenceID { return f.probe.receipt.Reference }
func (f *retainedProbe) Stat(ctx context.Context, _ storage.ObservationOptions) (storage.FileObservation, error) {
	return f.probe.receipt.Observation, f.probe.record(ctx)
}
func (f *retainedProbe) ReadAt(ctx context.Context, _ storage.FileReadRequest) (storage.FileRead, error) {
	return storage.FileRead{}, f.probe.record(ctx)
}
func (f *retainedProbe) ListAt(ctx context.Context, _ storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	return storage.DirectoryPage{}, f.probe.record(ctx)
}
func (f *retainedProbe) RangeSnapshot(ctx context.Context, _ storage.RangeOwnerID, _ storage.RangeScope) (storage.RangeSnapshot, error) {
	return storage.RangeSnapshot{}, f.probe.record(ctx)
}
func (f *retainedProbe) Sync(ctx context.Context) error { return f.probe.record(ctx) }
func (f *retainedProbe) Close(ctx context.Context, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) WriteAt(ctx context.Context, _ storage.FileWriteRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) Truncate(ctx context.Context, _ storage.FileTruncateRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) SetAttr(ctx context.Context, _ storage.AttrChange, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) SetKind(ctx context.Context, _ storage.SetKindRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) Rename(ctx context.Context, _ storage.RenameRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) ReplaceClaim(ctx context.Context, _ storage.AccessClaim, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) PrepareRemoval(ctx context.Context, _ storage.PrepareRemovalRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) DrainEntry(ctx context.Context, _ storage.DrainEntryRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) CancelDrain(ctx context.Context, _ storage.CancelDrainRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) ReplaceRanges(ctx context.Context, _ storage.RangeReplaceRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}
func (f *retainedProbe) WaitRanges(ctx context.Context, _ storage.RangeWaitRequest, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}

func (f *retainedProbe) CancelPrepared(ctx context.Context, _ storage.RemovalIntentID, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}

func (f *retainedProbe) CheckObservation(ctx context.Context, _ storage.ObservationCondition) (storage.FileObservation, error) {
	return f.probe.receipt.Observation, f.probe.record(ctx)
}
func (f *retainedProbe) RetireRangeOwner(ctx context.Context, _ storage.RangeOwnerID, _ storage.RangeScope, _ storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.probe.receipt, f.probe.record(ctx)
}

func (f *retainedProbe) LookupAt(ctx context.Context, _ []byte) (storage.EntryLookup, error) {
	return storage.EntryLookup{}, f.probe.record(ctx)
}
