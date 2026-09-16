package limited_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

type quotaSession struct {
	storage.FileSession
	backing storage.FileStorage
	epoch   uint64
}

func nextAction(t *testing.T, session *quotaSession) storage.FileActionID {
	t.Helper()
	id, err := storage.NewFileActionID(session.epoch)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func retainedSession(t *testing.T, backing storage.FileStorage, ctx context.Context, options storage.FileSessionOptions) *quotaSession {
	t.Helper()
	inner, status, err := backing.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	session := &quotaSession{FileSession: inner, backing: backing, epoch: status.ActionEpoch}
	t.Cleanup(func() {
		if _, err := session.Close(context.Background(), nextAction(t, session)); err != nil {
			t.Errorf("closing retained session: %v", err)
		}
	})
	return session
}
func retainedFile(t *testing.T, session *quotaSession, name string, create bool) storage.File {
	t.Helper()
	var receipt storage.FileActionReceipt
	var err error
	if create {
		root, statErr := session.backing.Stat(t.Context(), "")
		if statErr != nil {
			t.Fatal(statErr)
		}
		held, retainErr := session.Retain(t.Context(), storage.RetainRequest{NodeID: root.ID}, nextAction(t, session))
		if retainErr != nil {
			t.Fatal(retainErr)
		}
		receipt, err = session.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: storage.EntryTarget{Parent: held.Reference, ParentID: root.ID, Name: []byte(name), DirectoryRevision: root.DirectoryRevision, Witness: &storage.EntryLocation{State: storage.LocationRoot, RootNodeID: root.ID, NodeID: root.ID}}, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nextAction(t, session))
	} else {
		attr, statErr := session.backing.Stat(t.Context(), name)
		if statErr != nil {
			t.Fatal(statErr)
		}
		target := quotaTarget(t, session, name)
		target.ExpectedNodeID = attr.ID
		receipt, err = session.RetainAt(t.Context(), storage.RetainAtRequest{Target: target, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nextAction(t, session))
	}
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.Reference(t.Context(), receipt.Reference)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func TestRetainedQuotaSurvivesUnlinkAndSettlesFinalCloseOnce(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	state, err := s.FileState(t.Context())
	if err != nil || state.VolumeIdentity == "" {
		t.Fatalf("volume state = %+v, %v", state, err)
	}
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	first := retainedFile(t, session, "file", true)
	second := retainedFile(t, session, "file", false)
	if _, err := first.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: content(1024)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1024)
	if _, err := first.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 1024, Data: content(1024)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 2048)
	before, err := first.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil {
		t.Fatal(err)
	}
	metadata := storage.Metadata{{Key: "test", Version: 1, Data: []byte("detached")}}
	modified := time.Unix(1_700_000_000, 123)
	changed, err := first.SetAttr(t.Context(), storage.AttrChange{ExpectedRevision: before.Attr.MetadataRevision, Metadata: &metadata, ModTime: &modified}, nextAction(t, session))
	if err != nil || changed.Observation.Attr.Size != 2048 || !reflect.DeepEqual(changed.Observation.Attr.Metadata, metadata) || !changed.Observation.Attr.ModTime.Equal(modified) {
		t.Fatalf("detached descriptor attributes = %+v, %v", changed, err)
	}
	accessed := modified.Add(-time.Hour)
	byID, err := session.SetNodeAttr(t.Context(), changed.Observation.Attr.ID, storage.AttrChange{AccessTime: &accessed}, nextAction(t, session))
	if err != nil || byID.Observation.Attr.ID != changed.Observation.Attr.ID || byID.Observation.Attr.Size != 2048 || !reflect.DeepEqual(byID.Observation.Attr.Metadata, metadata) ||
		!byID.Observation.Attr.ModTime.Equal(modified) || !byID.Observation.Attr.AccessTime.Equal(accessed) {
		t.Fatalf("detached identity attributes = %+v, %v", byID, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	unapplied := storage.Metadata{{Key: "test", Version: 1, Data: []byte("unapplied")}}
	if _, err := first.SetAttr(cancelled, storage.AttrChange{ExpectedRevision: byID.Observation.Attr.MetadataRevision, Metadata: &unapplied}, nextAction(t, session)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled descriptor attributes lost their cause: %v", err)
	}
	if _, err := session.SetNodeAttr(cancelled, changed.Observation.Attr.ID, storage.AttrChange{ExpectedRevision: byID.Observation.Attr.MetadataRevision, Metadata: &unapplied}, nextAction(t, session)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled identity attributes lost their cause: %v", err)
	}
	observed, err := second.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || !reflect.DeepEqual(observed.Attr.Clone(), byID.Observation.Attr.Clone()) || observed.Location == nil || observed.Location.State != storage.LocationDetached {
		t.Fatalf("second retained reference observed attributes %+v, %v; want %+v", observed, err, byID)
	}
	mustUse(t, s, 2048)
	if err := s.Write(t.Context(), "new", content(3072)); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("detached file did not retain its charge: %v", err)
	}
	if _, err := first.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 2048)
	if _, err := second.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
	mustWrite(t, s, "new", 512)
	if _, err := second.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 512)
	if _, err := s.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("retained writes recreated the unlinked name: %v", err)
	}
}

func TestRetainedQuotaExpiryUsesUncancelledLifetimeAccounting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := newStorage(t, limited.MinLimit)
		ctx, cancel := context.WithCancel(t.Context())
		options := storage.DefaultFileSessionOptions()
		options.Lease = 80 * time.Millisecond
		session := retainedSession(t, s, ctx, options)
		file := retainedFile(t, session, "file", true)
		if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: content(512)}, nextAction(t, session)); err != nil {
			t.Fatal(err)
		}
		if err := s.Remove(t.Context(), "file"); err != nil {
			t.Fatal(err)
		}
		mustUse(t, s, 512)
		cancel()
		synctest.Wait()
		mustUse(t, s, 512)
		deadline := time.After(3 * time.Second)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			space, err := s.Space(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if space.Used == 0 {
				break
			}
			select {
			case <-deadline:
				t.Fatalf("expired detached file retained %d charged bytes", space.Used)
			case <-ticker.C:
			}
		}
		if _, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}); !errors.Is(err, syscall.ESTALE) {
			t.Fatalf("expired file returned %v, want ESTALE", err)
		}
	})
}

func TestRetainedQuotaFailedCleanupKeepsCharge(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	failure := errors.New("injected detached cleanup refusal")
	var reject atomic.Bool
	reject.Store(true)
	ctx := storage.WithPublicationAccounting(t.Context(), func(previous, next int64) (storage.PublicationSettlement, error) {
		if previous > 0 && next == 0 && reject.Load() {
			return nil, failure
		}
		return func(storage.PublicationResult) error { return nil }, nil
	})
	session := retainedSession(t, s, ctx, storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: content(512)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Close(t.Context(), nextAction(t, session)); !errors.Is(err, failure) {
		t.Fatalf("cleanup failure returned %v", err)
	}
	mustUse(t, s, 512)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 512)
	reject.Store(false)
	if _, err := file.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
}

func TestRetainedQuotaStartupAndRecountIncludeExistingDetachedFiles(t *testing.T) {
	_, backing := memoryfixture.New(t, "retained-usage", 0, locking.DefaultOptions())
	session := retainedSession(t, backing, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: content(1024)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := backing.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	s := newStorageOver(t, backing, limited.MinLimit)
	mustUse(t, s, 1024)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1024)
	if _, err := file.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
}

func TestRetainedQuotaRecountRetriesAcrossCleanupSettlement(t *testing.T) {
	_, backing := memoryfixture.New(t, "retained-recount", 0, locking.DefaultOptions())
	p := &pausedUsage{Storage: backing}
	s := newStorageOver(t, p, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: content(512)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	p.sampled, p.resume = make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(p.resume) }) }
	t.Cleanup(resume)
	done := make(chan error, 1)
	go func() { done <- s.Recount(t.Context()) }()
	select {
	case <-p.sampled:
	case <-time.After(3 * time.Second):
		t.Fatal("recount did not sample authoritative usage")
	}
	if _, err := file.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	resume()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("recount deadlocked with final cleanup")
	}
	mustUse(t, s, 0)
}

type pausedUsage struct {
	*objectstore.Storage
	once    sync.Once
	sampled chan struct{}
	resume  chan struct{}
}

func (p *pausedUsage) Usage(ctx context.Context) (int64, error) {
	count, err := p.Storage.Usage(ctx)
	if p.sampled != nil {
		p.once.Do(func() {
			close(p.sampled)
			select {
			case <-p.resume:
			case <-ctx.Done():
				err = ctx.Err()
			}
		})
	}
	return count, err
}

func TestRetainedQuotaRequiresNativeAccountingAndAuthoritativeUsage(t *testing.T) {
	backing := newBacking(t)
	missingAccounting := &missingAccounting{BoundedStorage: backing}
	if value, err := limited.New(t.Context(), missingAccounting, limited.MinLimit); value != nil || !errors.Is(err, syscall.ENOSYS) || missingAccounting.calls != 0 {
		t.Fatalf("missing accounting capability returned storage=%v calls=%d err=%v", value != nil, missingAccounting.calls, err)
	}
	withoutFiles := newStorageOver(t, &faulty{BoundedStorage: backing}, limited.MinLimit)
	for _, run := range []func() error{withoutFiles.CheckFileStorage, func() error { _, err := withoutFiles.FileState(t.Context()); return err }, func() error {
		_, _, err := withoutFiles.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
		return err
	}} {
		if err := run(); err != syscall.EOPNOTSUPP {
			t.Fatalf("unsupported retained capability = %v", err)
		}
	}
	missing := &missingRetainedUsage{BoundedStorage: backing}
	if _, err := limited.New(t.Context(), missing, limited.MinLimit); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("retained volume without authoritative usage returned %v", err)
	}
	failure := errors.New("injected retained capability failure")
	missing.checkErr = failure
	if _, err := limited.New(t.Context(), missing, limited.MinLimit); err != failure {
		t.Fatalf("capability failure lost its cause: %v", err)
	}
	missing.checkErr = nil
	for _, result := range []struct {
		usage int64
		err   error
		want  error
	}{{-1, nil, syscall.EIO}, {0, errors.Join(syscall.ENOSYS, syscall.EIO), syscall.EIO}} {
		p := &invalidRetainedUsage{missingRetainedUsage: missing, usage: result.usage, err: result.err}
		if _, err := limited.New(t.Context(), p, limited.MinLimit); !errors.Is(err, result.want) {
			t.Fatalf("invalid authoritative usage returned %v, want %v", err, result.want)
		}
	}
}

type missingRetainedUsage struct {
	storage.BoundedStorage
	checkErr error
}

func (s *missingRetainedUsage) CheckFileStorage() error { return s.checkErr }
func (*missingRetainedUsage) FileState(context.Context) (storage.FileVolumeState, error) {
	panic("measurement precedes state observation")
}
func (s *missingRetainedUsage) CheckPublicationAccounting() error {
	return checkDelegatedAccounting(s.BoundedStorage)
}
func (*missingRetainedUsage) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	panic("measurement must precede file session creation")
}

type invalidRetainedUsage struct {
	*missingRetainedUsage
	usage int64
	err   error
}

func (p *invalidRetainedUsage) Usage(context.Context) (int64, error) { return p.usage, p.err }

func TestRetainedQuotaIdentityAndTruncatePreserveCharge(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	original := retainedFile(t, session, "file", true)
	attr, err := original.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 0, Data: []byte("preserve")}, nextAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	retained, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: attr.Observation.Attr.ID, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}, nextAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.Reference(t.Context(), retained.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Rename(t.Context(), "file", "renamed"); err != nil {
		t.Fatal(err)
	}
	shrunk, err := opened.Truncate(t.Context(), storage.FileTruncateRequest{Size: 3}, nextAction(t, session))
	if err != nil || shrunk.Observation.Attr.ID != attr.Observation.Attr.ID || shrunk.Observation.Attr.Size != 3 {
		t.Fatalf("truncate = %+v, %v", shrunk, err)
	}
	mustUse(t, s, 3)
	read, err := original.ReadAt(t.Context(), storage.FileReadRequest{Offset: 0, Length: 10})
	if err != nil || string(read.Data) != "pre" {
		t.Fatalf("other reference = %+v, %v", read, err)
	}
	if _, err := opened.Truncate(t.Context(), storage.FileTruncateRequest{Size: limited.MinLimit + 1}, nextAction(t, session)); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("over-quota truncate = %v", err)
	}
	mustUse(t, s, 3)
	after, err := opened.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || after.Attr.Size != 3 {
		t.Fatalf("rejected truncate changed size: %+v, %v", after, err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := session.Retain(cancelled, storage.RetainRequest{NodeID: attr.Observation.Attr.ID, Claim: storage.AccessClaim{Uses: storage.ReadContent}}, nextAction(t, session)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled open = %v", err)
	}
	if _, err := opened.Truncate(cancelled, storage.FileTruncateRequest{Size: 0}, nextAction(t, session)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled truncate = %v", err)
	}
	if _, err := opened.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := original.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 3)
	if _, err := s.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("old name = %v", err)
	}
}

func TestRetainedQuotaReplayResolvesOnlyScopedReferences(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	observed, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil {
		t.Fatal(err)
	}
	action := nextAction(t, session)
	retained, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: observed.Attr.ID, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, action)
	if err != nil {
		t.Fatal(err)
	}
	writeAction := nextAction(t, session)
	written, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: content(limited.MinLimit)}, writeAction)
	if err != nil || written.Observation.Attr.Size != limited.MinLimit {
		t.Fatalf("write = %+v, %v", written, err)
	}
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: content(limited.MinLimit)}, writeAction); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, limited.MinLimit)
	for _, query := range []func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error){session.QueryAction, session.CancelAction} {
		replay, err := query(t.Context(), action)
		if err != nil || replay.Reference != retained.Reference {
			t.Fatalf("receipt identity = %+v, %v", replay, err)
		}
		resolved, err := session.Reference(t.Context(), replay.Reference)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resolved.WriteAt(t.Context(), storage.FileWriteRequest{Offset: limited.MinLimit, Data: []byte{1}}, nextAction(t, session)); !errors.Is(err, syscall.EDQUOT) {
			t.Fatalf("resolved reference bypassed quota: %v", err)
		}
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	resolved, err := session.Reference(t.Context(), retained.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolved.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, limited.MinLimit)
	action = nextAction(t, session)
	if _, err := file.Close(t.Context(), action); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
	if _, err := file.Close(t.Context(), action); err != nil {
		t.Fatalf("close replay: %v", err)
	}
	mustUse(t, s, 0)
}

func quotaTarget(t *testing.T, session *quotaSession, name string) storage.EntryTarget {
	t.Helper()
	root, err := session.backing.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	retained, err := session.Retain(t.Context(), storage.RetainRequest{NodeID: root.ID}, nextAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	target := storage.EntryTarget{Parent: retained.Reference, ParentID: root.ID, Name: []byte(name), DirectoryRevision: root.DirectoryRevision, Witness: &storage.EntryLocation{State: storage.LocationRoot, RootNodeID: root.ID, NodeID: root.ID}}
	dir, err := session.Reference(t.Context(), retained.Reference)
	if err != nil {
		t.Fatal(err)
	}
	page, err := dir.ListAt(t.Context(), storage.DirectoryPageRequest{Revision: root.DirectoryRevision, MaxEntries: 1024, MaxBytes: 1 << 20})
	if err != nil || !page.Done {
		t.Fatalf("bounded root page = %+v, %v", page, err)
	}
	for _, entry := range page.Entries {
		if string(entry.Name) == name {
			target.ExpectedEntryID = entry.EntryID
			target.ExpectedNodeID = entry.Attr.ID
			target.ExpectedMetadataRevision = entry.Attr.MetadataRevision
		}
	}
	return target
}

func TestRetainedQuotaMetadataRenameRangesAndDrainKeepIdentity(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "before", true)
	written, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: content(1000)}, nextAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	metadata := storage.Metadata{{Key: "application", Version: 2, Data: []byte("opaque")}}
	if _, err := file.SetAttr(t.Context(), storage.AttrChange{ExpectedRevision: written.Observation.Attr.MetadataRevision, Metadata: &metadata}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	scope := storage.RangeScope{Enforced: true}
	snapshot, err := file.RangeSnapshot(t.Context(), 1, scope)
	if err != nil {
		t.Fatal(err)
	}
	locked, err := file.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: 1, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: []storage.RangeAcquisition{{ID: 1, End: 9}}}, nextAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: 1, Scope: scope, ExpectedRevision: locked.RangeRevision}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Rename(t.Context(), storage.RenameRequest{Source: quotaTarget(t, session, "before"), Destination: quotaTarget(t, session, "after")}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	observed, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || observed.Attr.ID != written.Observation.Attr.ID || !reflect.DeepEqual(observed.Attr.Metadata, metadata) {
		t.Fatalf("retained metadata = %+v, %v", observed, err)
	}
	leaf := observed.Location.Ancestors[len(observed.Location.Ancestors)-1]
	if string(leaf.Name) != "after" {
		t.Fatalf("renamed location = %+v", observed.Location)
	}
	mustUse(t, s, 1000)
	if _, err := file.DrainEntry(t.Context(), storage.DrainEntryRequest{ExpectedMetadataRevision: observed.Attr.MetadataRevision, Entry: leaf, Witness: *observed.Location, Condition: storage.RemovalFile}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1000)
	if _, err := file.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
	if _, err := s.Stat(t.Context(), "after"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("drained entry survived: %v", err)
	}
}

func TestRetainedQuotaLinkConversionRequiresCapacity(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	mustWrite(t, s, "held", limited.MinLimit-10)
	file := retainedFile(t, session, "link", true)
	before, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil {
		t.Fatal(err)
	}
	change := storage.SetKindRequest{ExpectedRevision: before.Attr.MetadataRevision, Kind: storage.NodeSymlink, LinkTarget: []byte("elevenbytes"), Metadata: before.Attr.Metadata}
	if _, err := file.SetKind(t.Context(), change, nextAction(t, session)); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("link conversion bypassed quota: %v", err)
	}
	unchanged, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || unchanged.Attr.ID != before.Attr.ID || unchanged.Attr.Kind != storage.NodeRegular || unchanged.Attr.Size != 0 {
		t.Fatalf("rejected conversion changed node: %+v, %v", unchanged, err)
	}
	change.LinkTarget = []byte("target")
	if _, err := file.SetKind(t.Context(), change, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	observed, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true})
	if err != nil || string(observed.LinkTarget) != "target" || observed.Attr.Kind != storage.NodeSymlink {
		t.Fatalf("retained link = %+v, %v", observed, err)
	}
	mustUse(t, s, limited.MinLimit-4)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, limited.MinLimit-4)
}

func TestRetainedQuotaAtomicResetAndReplacePreserveOldCharges(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	original := retainedFile(t, session, "file", true)
	if _, err := original.WriteAt(t.Context(), storage.FileWriteRequest{Data: content(1000)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	target := quotaTarget(t, session, "file")
	reset, err := session.ResetAndRetainAt(t.Context(), storage.ResetAndRetainRequest{Target: target, ExpectedRevision: target.ExpectedMetadataRevision, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nextAction(t, session))
	if err != nil || reset.Observation.Attr.ID != target.ExpectedNodeID || reset.Observation.Attr.Size != 0 {
		t.Fatalf("atomic reset = %+v, %v", reset, err)
	}
	mustUse(t, s, 0)
	resetFile, err := session.Reference(t.Context(), reset.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resetFile.WriteAt(t.Context(), storage.FileWriteRequest{Data: content(1000)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	target = quotaTarget(t, session, "file")
	replaced, err := session.ReplaceAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: storage.NodeInitial{Kind: storage.NodeRegular}, Claim: storage.AccessClaim{Uses: storage.AllAccessUses}}, nextAction(t, session))
	if err != nil || replaced.Observation.Attr.ID == target.ExpectedNodeID {
		t.Fatalf("atomic replace = %+v, %v", replaced, err)
	}
	mustUse(t, s, 1000)
	replacement, err := session.Reference(t.Context(), replaced.Reference)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replacement.WriteAt(t.Context(), storage.FileWriteRequest{Data: content(500)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1500)
	if _, err := original.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1500)
	if _, err := resetFile.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 500)
}

func TestRetainedQuotaCancellingRemovalKeepsLiveContentCharged(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: content(1000)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	observed, err := file.Stat(t.Context(), storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		t.Fatal(err)
	}
	leaf := observed.Location.Ancestors[len(observed.Location.Ancestors)-1]
	prepared, err := file.PrepareRemoval(t.Context(), storage.PrepareRemovalRequest{ExpectedMetadataRevision: observed.Attr.MetadataRevision, Entry: leaf, Witness: *observed.Location, Condition: storage.RemovalFile}, nextAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.CancelPrepared(t.Context(), prepared.Removal.IntentID, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	drained, err := file.DrainEntry(t.Context(), storage.DrainEntryRequest{ExpectedMetadataRevision: observed.Attr.MetadataRevision, Entry: leaf, Witness: *observed.Location, Condition: storage.RemovalFile}, nextAction(t, session))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.CancelDrain(t.Context(), storage.CancelDrainRequest{EntryID: leaf.EntryID, Generation: drained.Removal.Generation}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.ReplaceClaim(t.Context(), storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1000)
	if got, err := s.Read(t.Context(), "file"); err != nil || len(got) != 1000 {
		t.Fatalf("cancelled removal lost content: %d, %v", len(got), err)
	}
}

func TestRetainedQuotaPreservesConfirmedReceiptWhenAccountingBecomesUnknown(t *testing.T) {
	_, backend := memoryfixture.New(t, "uncertain-retained-receipt", 0, locking.DefaultOptions())
	settle, err := storage.PreparePublication(t.Context(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	uncertain := settle(storage.PublicationUnknown)
	if !storage.IsPublicationAccountingUncertain(uncertain) {
		t.Fatalf("missing accounting classification: %v", uncertain)
	}
	facade := &uncertainRetainedBackend{Storage: backend, failure: uncertain}
	s := newStorageOver(t, facade, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	action := nextAction(t, session)
	result, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: content(1000)}, action)
	if !errors.Is(err, uncertain) || result.State != storage.FileActionCompleted || result.Observation.Attr.Size != 1000 || result.Effects&storage.EffectContentChanged == 0 {
		t.Fatalf("publication lost confirmed effect: %+v, %v", result, err)
	}
	if _, err := s.Space(t.Context()); !errors.Is(err, uncertain) {
		t.Fatalf("uncertain accounting did not fence allowance: %v", err)
	}
	if used, err := backend.Usage(t.Context()); err != nil || used != 1000 {
		t.Fatalf("confirmed backend usage = %d, %v", used, err)
	}
	replay, err := session.QueryAction(t.Context(), action)
	if err != nil || replay.Reference != result.Reference || replay.Observation.Attr.ID != result.Observation.Attr.ID || replay.Observation.Attr.Size != 1000 {
		t.Fatalf("fenced allowance lost receipt: %+v, %v", replay, err)
	}
}

type uncertainRetainedBackend struct {
	*objectstore.Storage
	failure error
}

func (s *uncertainRetainedBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	session, status, err := s.Storage.NewFileSession(ctx, options)
	if session == nil {
		return nil, status, err
	}
	return &uncertainRetainedSession{FileSession: session, failure: s.failure}, status, err
}

type uncertainRetainedSession struct {
	storage.FileSession
	failure error
}

func (s *uncertainRetainedSession) Reference(ctx context.Context, id storage.FileReferenceID) (storage.File, error) {
	file, err := s.FileSession.Reference(ctx, id)
	if file == nil {
		return nil, err
	}
	return &uncertainRetainedFile{File: file, failure: s.failure}, err
}

type uncertainRetainedFile struct {
	storage.File
	failure error
}

func (f *uncertainRetainedFile) WriteAt(ctx context.Context, request storage.FileWriteRequest, action storage.FileActionID) (storage.FileActionReceipt, error) {
	receipt, err := f.File.WriteAt(ctx, request, action)
	if err != nil {
		return receipt, err
	}
	return receipt, f.failure
}

func TestRetainedQuotaRangeWaitAndOwnerRetirementDoNotInventChargesOrGrants(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	scope := storage.RangeScope{Domain: 17}
	snapshot, err := file.RangeSnapshot(t.Context(), 1, scope)
	if err != nil {
		t.Fatal(err)
	}
	ranges := []storage.RangeAcquisition{{ID: 1, Start: 0, End: 99, Exclusive: true}}
	if _, err := file.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: 1, Scope: scope, ExpectedRevision: snapshot.Revision, Ranges: ranges}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	blocked, err := file.RangeSnapshot(t.Context(), 2, scope)
	if err != nil {
		t.Fatal(err)
	}
	waitID := nextAction(t, session)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	type outcome struct {
		receipt storage.FileActionReceipt
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		receipt, err := file.WaitRanges(ctx, storage.RangeWaitRequest{Owner: 2, Scope: scope, ExpectedRevision: blocked.Revision, Ranges: ranges}, waitID)
		done <- outcome{receipt, err}
	}()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		receipt, err := session.QueryAction(ctx, waitID)
		if err == nil && receipt.State == storage.FileActionPending {
			break
		}
		select {
		case result := <-done:
			t.Fatalf("wait ended before holder retirement: %+v, %v", result.receipt, result.err)
		case <-ctx.Done():
			t.Fatalf("wait was not admitted: %+v, %v", receipt, err)
		case <-ticker.C:
		}
	}
	if _, err := file.RetireRangeOwner(t.Context(), 1, scope, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil || result.receipt.State != storage.FileActionCompleted || result.receipt.Effects&storage.EffectRangesChanged != 0 {
			t.Fatalf("revision wait returned a grant: %+v, %v", result.receipt, result.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	ready, err := file.RangeSnapshot(t.Context(), 2, scope)
	if err != nil || len(ready.Own) != 0 {
		t.Fatalf("wait invented owned ranges: %+v, %v", ready, err)
	}
	if _, err := file.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{Owner: 2, Scope: scope, ExpectedRevision: ready.Revision, Ranges: ranges}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.RetireRangeOwner(t.Context(), 2, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	retired, err := file.RangeSnapshot(t.Context(), 2, scope)
	if err != nil || len(retired.Own) != 0 {
		t.Fatalf("session retirement kept owned ranges: %+v, %v", retired, err)
	}
	mustUse(t, s, 0)
}

func TestRetainedQuotaSessionCloseRetiresEveryChargedReference(t *testing.T) {
	s := newStorage(t, limited.MinLimit)
	session := retainedSession(t, s, t.Context(), storage.DefaultFileSessionOptions())
	file := retainedFile(t, session, "file", true)
	if _, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: content(1000)}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Truncate(t.Context(), storage.FileTruncateRequest{Size: 2000}, nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "file"); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 2000)
	if _, err := session.Close(t.Context(), nextAction(t, session)); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
	if _, err := file.Stat(t.Context(), storage.ObservationOptions{}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("closed session kept reference active: %v", err)
	}
}
