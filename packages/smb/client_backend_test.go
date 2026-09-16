package smb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type clientTestSource struct {
	storage.FileStorage
	state                          storage.FileVolumeState
	stateErr, checkErr, sessionErr error
	checks, sessions               int
	owner                          storage.FileSession
	initial                        storage.FileSessionStatus
}

func (s *clientTestSource) CheckFileStorage() error { s.checks++; return s.checkErr }
func (s *clientTestSource) FileState(context.Context) (storage.FileVolumeState, error) {
	return s.state, s.stateErr
}
func (s *clientTestSource) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, storage.FileSessionStatus, error) {
	s.sessions++
	return s.owner, s.initial, s.sessionErr
}

type clientTestSession struct {
	storage.FileSession
	files       map[storage.FileReferenceID]*clientTestFile
	status      storage.FileSessionStatus
	retains     []storage.RetainRequest
	onRetain    func(storage.RetainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onReference func(storage.FileReferenceID) (storage.File, error)
	onRetainAt  func(storage.RetainAtRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onCreate    func(storage.CreateAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onReset     func(storage.ResetAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onReplace   func(storage.CreateAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onQuery     func(storage.FileActionID) (storage.FileActionReceipt, error)
	onClose     func(storage.FileActionID) (storage.FileActionReceipt, error)
}

func (s *clientTestSession) Reference(_ context.Context, id storage.FileReferenceID) (storage.File, error) {
	if s.onReference != nil {
		return s.onReference(id)
	}
	if f := s.files[id]; f != nil {
		return f, nil
	}
	return nil, syscall.ESTALE
}
func (s *clientTestSession) StatNode(_ context.Context, id uint64, options storage.ObservationOptions) (storage.FileObservation, error) {
	for _, f := range s.files {
		if f.NodeID() == id {
			return f.Stat(context.Background(), options)
		}
	}
	return storage.FileObservation{}, syscall.ENOENT
}
func (s *clientTestSession) Status(context.Context) (storage.FileSessionStatus, error) {
	return s.status, nil
}
func (s *clientTestSession) Renew(context.Context) (storage.FileSessionStatus, error) {
	return s.status, nil
}
func (s *clientTestSession) Retain(_ context.Context, r storage.RetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.retains = append(s.retains, r)
	if s.onRetain != nil {
		return s.onRetain(r, id)
	}
	for _, f := range s.files {
		if f.NodeID() == r.NodeID {
			return storage.FileActionReceipt{Action: id, Operation: storage.OpFileRetain, State: storage.FileActionCompleted, Effects: storage.EffectRetained | storage.EffectClaimChanged, Reference: f.ref, Observation: f.observation.Clone()}, nil
		}
	}
	return storage.FileActionReceipt{}, syscall.ENOENT
}
func (s *clientTestSession) RetainAt(_ context.Context, r storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.onRetainAt(r, id)
}
func (s *clientTestSession) CreateAndRetainAt(_ context.Context, r storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.onCreate(r, id)
}
func (s *clientTestSession) ResetAndRetainAt(_ context.Context, r storage.ResetAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.onReset(r, id)
}
func (s *clientTestSession) ReplaceAndRetainAt(_ context.Context, r storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.onReplace(r, id)
}
func (s *clientTestSession) QueryAction(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return s.onQuery(id)
}
func (s *clientTestSession) Close(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if s.onClose != nil {
		return s.onClose(id)
	}
	return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted}, nil
}

type clientTestFile struct {
	storage.File
	ref         storage.FileReferenceID
	observation storage.FileObservation
	entries     []storage.DirectoryEntry
	stats       []storage.ObservationOptions
	lists       []storage.DirectoryPageRequest
	checks      []storage.ObservationCondition
	checkErr    error
	onList      func(storage.DirectoryPageRequest) (storage.DirectoryPage, error)
	onStat      func(storage.ObservationOptions) (storage.FileObservation, error)
	onClose     func(storage.FileActionID) (storage.FileActionReceipt, error)
}

func (f *clientTestFile) Reference() storage.FileReferenceID { return f.ref }
func (f *clientTestFile) NodeID() uint64                     { return f.observation.Attr.ID }
func (f *clientTestFile) Stat(_ context.Context, options storage.ObservationOptions) (storage.FileObservation, error) {
	f.stats = append(f.stats, options)
	if f.onStat != nil {
		return f.onStat(options)
	}
	r := f.observation.Clone()
	if !options.IncludeLocation {
		r.Location = nil
	}
	if !options.IncludeLinkTarget {
		r.LinkTarget = nil
	}
	return r, nil
}
func (f *clientTestFile) ListAt(_ context.Context, r storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	f.lists = append(f.lists, r)
	if f.onList != nil {
		return f.onList(r)
	}
	return storage.DirectoryPage{ParentID: f.NodeID(), Revision: f.observation.Attr.DirectoryRevision, Entries: f.entries, Done: true}, nil
}
func (f *clientTestFile) CheckObservation(_ context.Context, c storage.ObservationCondition) (storage.FileObservation, error) {
	f.checks = append(f.checks, c)
	return f.observation.Clone(), f.checkErr
}
func (f *clientTestFile) Close(_ context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if f.onClose != nil {
		return f.onClose(id)
	}
	return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted}, nil
}

func newClientTestSession(t *testing.T) (*clientSession, *clientTestSession, *clientTestFile, *clientTestFile, *clientTestFile) {
	t.Helper()
	rootLocation := storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 1, NodeID: 1}
	parentEdge := storage.EntryCondition{ParentID: 1, DirectoryRevision: 31, EntryID: 12, NodeID: 2, Name: []byte("Parent")}
	parentLocation := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 2, Ancestors: []storage.EntryCondition{parentEdge}}
	childEdge := storage.EntryCondition{ParentID: 2, DirectoryRevision: 41, EntryID: 23, NodeID: 3, Name: []byte("Report.TXT")}
	childLocation := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 3, Ancestors: []storage.EntryCondition{parentEdge, childEdge}}
	root := &clientTestFile{ref: 11, observation: storage.FileObservation{Attr: storage.Attr{ID: 1, Kind: storage.NodeDirectory, MetadataRevision: 5, DirectoryRevision: 31}, Location: &rootLocation}}
	parent := &clientTestFile{ref: 22, observation: storage.FileObservation{Attr: storage.Attr{ID: 2, Kind: storage.NodeDirectory, MetadataRevision: 6, DirectoryRevision: 41}, Location: &parentLocation}}
	child := &clientTestFile{ref: 33, observation: storage.FileObservation{Attr: storage.Attr{ID: 3, Kind: storage.NodeRegular, MetadataRevision: 7, Size: 19}, Location: &childLocation}}
	root.entries = []storage.DirectoryEntry{{EntryID: 12, Name: []byte("Parent"), Attr: parent.observation.Attr.Clone()}}
	parent.entries = []storage.DirectoryEntry{{EntryID: 23, Name: []byte("Report.TXT"), Attr: child.observation.Attr.Clone()}}
	status := storage.FileSessionStatus{ActionEpoch: 17, Remaining: time.Minute}
	raw := &clientTestSession{files: map[storage.FileReferenceID]*clientTestFile{11: root, 22: parent, 33: child}, status: status}
	source := &clientTestSource{state: storage.FileVolumeState{VolumeIdentity: "authority:volume", RootID: 1, MaxEventBytes: 1024}, owner: raw, initial: status}
	backend := &clientBackend{source: source, limits: DefaultLimits(), volume: "trusted", authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })}
	session, err := backend.NewSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	return session.(*clientSession), raw, root, parent, child
}

func TestClientPublicPublishUsesGenericStorage(t *testing.T) {
	session, _, _, _, _ := newClientTestSession(t)
	source := session.backend.source.(*clientTestSource)
	server, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	export, err := server.Publish(Share{Name: "work", Volume: "trusted", Backend: source, Changes: testNotifySource(testNotifyStream())})
	if err != nil {
		t.Fatal(err)
	}
	if source.checks != 1 || source.sessions != 1 || export.volumeIdentity != "authority:volume" {
		t.Fatalf("generic publication checks=%d sessions=%d identity=%q", source.checks, source.sessions, export.volumeIdentity)
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestClientBackendRejectsUnknownState(t *testing.T) {
	for _, state := range []storage.FileVolumeState{{RootID: 1, MaxEventBytes: 1}, {VolumeIdentity: "volume", MaxEventBytes: 1}, {VolumeIdentity: "volume", RootID: 1}} {
		b := &clientBackend{source: &clientTestSource{state: state}}
		if _, err := b.State(t.Context()); !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid state %+v: %v", state, err)
		}
	}
	cause := errors.New("state unavailable")
	b := &clientBackend{source: &clientTestSource{stateErr: cause}}
	if _, err := b.State(t.Context()); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	b.source = &clientTestSource{checkErr: cause}
	if err := b.Check(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
}

func TestClientNewSessionErrorRetainsCleanupOwnership(t *testing.T) {
	session, raw, _, _, _ := newClientTestSession(t)
	source := session.backend.source.(*clientTestSource)
	cause := errors.New("admission response incomplete")
	source.sessionErr = cause
	owner, err := session.backend.NewSession(t.Context(), storage.DefaultFileSessionOptions())
	if owner == nil || !errors.Is(err, cause) {
		t.Fatalf("owner=%v error=%v", owner, err)
	}
	var closeIDs []storage.FileActionID
	raw.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		closeIDs = append(closeIDs, id)
		if len(closeIDs) == 1 {
			return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EIO
		}
		return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted}, nil
	}
	if err := owner.Close(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if err := owner.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(closeIDs) != 2 || closeIDs[0] != closeIDs[1] {
		t.Fatalf("cleanup identities %v", closeIDs)
	}
	epoch, err := closeIDs[0].Epoch()
	if err != nil || epoch != 17 {
		t.Fatalf("initial cleanup epoch %d %v", epoch, err)
	}
}

func TestClientIdentityMetadataDefaultsAndCorruptPayload(t *testing.T) {
	session, _, _, _, child := newClientTestSession(t)
	session.backend.defaults = windowsMetadata{Attributes: dosArchive}
	foreign := storage.Metadata{{Key: "business", Version: 9, Data: []byte("opaque")}}
	child.observation.Attr.Metadata = foreign.Clone()
	child.observation.Location.Ancestors[0].Name = []byte("bad:name")
	file := &clientFile{session: session, raw: child, access: windowsReadAttributes}
	attr, err := file.Stat(t.Context())
	if err != nil || attr.DOSAttributes != dosArchive || attr.NameInfo.State != 0 {
		t.Fatalf("absent metadata attr=%+v err=%v", attr, err)
	}
	if len(child.stats) != 1 || child.stats[0].IncludeLocation || !reflect.DeepEqual(child.observation.Attr.Metadata, foreign) {
		t.Fatal("identity stat requested names or modified foreign metadata")
	}
	for _, metadata := range []storage.Metadata{
		{{Key: windowsMetadataKey, Version: 1, Data: nil}},
		{{Key: windowsMetadataKey, Version: 2, Data: make([]byte, 8)}},
	} {
		child.observation.Attr.Metadata = metadata
		if _, err := file.Stat(t.Context()); err == nil {
			t.Fatalf("corrupt present metadata accepted: %+v", metadata)
		}
	}
}

func TestClientHistoryRetainsEveryUnsettledOrCurrentAction(t *testing.T) {
	session, _, _, _, _ := newClientTestSession(t)
	now := time.Now()
	old, err := storage.NewFileActionID(16)
	if err != nil {
		t.Fatal(err)
	}
	current := clientTestAction(t)
	for _, tc := range []struct {
		name     string
		actual   storage.FileActionID
		terminal bool
		expiry   time.Time
		retained bool
	}{
		{"expired retired", old, true, now.Add(-time.Second), false},
		{"current epoch", current, true, now.Add(-time.Second), true},
		{"within history", old, true, now.Add(time.Second), true},
		{"pending", old, false, now.Add(-time.Second), true},
		{"unknown", old, false, time.Time{}, true},
		{"unknown deadline", old, true, time.Time{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := clientTestAction(t)
			session.actions = map[windowsActionID]*clientAction{original: {actual: tc.actual, terminal: tc.terminal, expires: tc.expiry}}
			session.options.MaxActions = 1
			session.mu.Lock()
			session.pruneActionsLocked(now)
			session.mu.Unlock()
			_, present := session.actions[original]
			if present != tc.retained {
				t.Fatalf("history retained=%t want=%t", present, tc.retained)
			}
			err := session.rememberOpen(clientTestAction(t), clientTestAction(t), clientAction{})
			if tc.retained && !errors.Is(err, syscall.ENOMEM) || !tc.retained && err != nil {
				t.Fatalf("history capacity err=%v", err)
			}
		})
	}
}

func TestClientFreshTerminalQueryReleasesOnlyRetiredExpiredHistory(t *testing.T) {
	session, raw, _, _, _ := newClientTestSession(t)
	original := clientTestAction(t)
	actual, err := storage.NewFileActionID(16)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.rememberOpen(original, actual, clientAction{}); err != nil {
		t.Fatal(err)
	}
	session.recordOpen(original, storage.FileActionReceipt{Action: actual, State: storage.FileActionUnknown})
	raw.onQuery = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		if id != actual {
			t.Fatalf("query=%q want=%q", id, actual)
		}
		return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted, HistoryRemaining: time.Minute}, nil
	}
	result, err := session.QueryAction(t.Context(), original)
	if err != nil || result.Action != original {
		t.Fatalf("query result=%+v err=%v", result, err)
	}
	record := session.action(original)
	if record == nil || !record.terminal || record.expires.IsZero() {
		t.Fatalf("terminal query not recorded %+v", record)
	}
	session.mu.Lock()
	session.pruneActionsLocked(record.expires.Add(-time.Nanosecond))
	session.mu.Unlock()
	if session.action(original) == nil {
		t.Fatal("history released before deadline")
	}
	session.mu.Lock()
	session.pruneActionsLocked(record.expires)
	session.mu.Unlock()
	if session.action(original) != nil {
		t.Fatal("retired terminal history remained charged")
	}
}

func TestClientCloseRetriesKnownFailureWithNewAction(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   storage.FileActionState
		effects storage.FileEffects
	}{
		{"not applied", storage.FileActionNotApplied, 0},
		{"partial reference retirement", storage.FileActionCompleted, storage.EffectReferenceRetired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session, raw, _, _, _ := newClientTestSession(t)
			if err := session.remember(clientTestAction(t), clientAction{}); err != nil {
				t.Fatal(err)
			}
			var ids []storage.FileActionID
			raw.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				ids = append(ids, id)
				if len(ids) == 1 {
					return storage.FileActionReceipt{Action: id, State: tc.state, Effects: tc.effects, Errno: syscall.ENOSPC}, nil
				}
				return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted, Effects: storage.EffectReferenceRetired}, nil
			}
			if err := session.Close(t.Context()); !errors.Is(err, syscall.ENOSPC) {
				t.Fatalf("known close failure: %v", err)
			}
			if session.closed || len(session.actions) != 1 {
				t.Fatal("failed close discarded live session ownership")
			}
			if err := session.Close(t.Context()); err != nil {
				t.Fatalf("capacity recovery: %v", err)
			}
			if len(ids) != 2 || ids[0] == ids[1] || !session.closed || len(session.actions) != 0 {
				t.Fatalf("cleanup actions=%v closed=%t retained=%d", ids, session.closed, len(session.actions))
			}
			if err := session.Close(t.Context()); err != nil || len(ids) != 2 {
				t.Fatalf("confirmed close replayed err=%v actions=%v", err, ids)
			}
		})
	}
}

func TestClientCloseRetriesUnsettledActionWithoutNewAdmission(t *testing.T) {
	for _, state := range []storage.FileActionState{storage.FileActionUnknown, storage.FileActionPending} {
		t.Run(fmt.Sprintf("state_%d", state), func(t *testing.T) {
			session, raw, _, _, _ := newClientTestSession(t)
			var ids []storage.FileActionID
			raw.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				ids = append(ids, id)
				if len(ids) == 1 {
					return storage.FileActionReceipt{Action: id, State: state}, syscall.EIO
				}
				return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted}, nil
			}
			if err := session.Close(t.Context()); !errors.Is(err, syscall.EIO) || session.closed {
				t.Fatalf("unsettled close err=%v closed=%t", err, session.closed)
			}
			if err := session.Close(t.Context()); err != nil || len(ids) != 2 || ids[0] != ids[1] {
				t.Fatalf("uncertain close changed admission err=%v actions=%v", err, ids)
			}
		})
	}
}

func TestClientTemporaryCleanupUsesBoundAuthorityInstallationGate(t *testing.T) {
	for _, failFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("first_close_failure_%t", failFirst), func(t *testing.T) {
			connection, authenticated, tr, _, _, _ := testConnection(t)
			if _, status := cleanupDispatch(t, t.Context(), connection, authenticated, wire.TreeDisconnect, tr.id, wire.EmptyResponseBody()); status != statusOK {
				t.Fatalf("initial disconnect=%x", status)
			}
			fixture, raw, _, _, temporary := newClientTestSession(t)
			tr.export.backend = fixture.backend
			if _, status := cleanupDispatch(t, t.Context(), connection, authenticated, wire.TreeConnect, 0, cleanupTreeConnectBody()); status != statusOK {
				t.Fatalf("generic tree connect=%x", status)
			}
			authenticated.mu.Lock()
			authority := authenticated.authorities[tr.export]
			authenticated.mu.Unlock()
			if authority == nil {
				t.Fatal("generic factory did not register authority")
			}
			client, ok := authority.session.(*clientSession)
			if !ok {
				t.Fatalf("factory session=%T", authority.session)
			}
			client.mu.Lock()
			retire := client.retirement
			client.mu.Unlock()
			if retire == nil {
				t.Fatal("factory did not bind authority retirement")
			}
			if err := client.remember(clientTestAction(t), clientAction{}); err != nil {
				t.Fatal(err)
			}
			client.planBytes = 64
			temporary.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted, Errno: syscall.ENOSPC}, syscall.ENOSPC
			}
			entered := make(chan struct{})
			client.bindRetirement(func(ctx context.Context) error { close(entered); return retire(ctx) })
			started := make(chan storage.FileActionID, 2)
			var ids []storage.FileActionID
			raw.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				ids = append(ids, id)
				started <- id
				if failFirst && len(ids) == 1 {
					return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted, Effects: storage.EffectReferenceRetired, Errno: syscall.ENOSPC}, syscall.ENOSPC
				}
				return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted}, nil
			}
			authority.installMu.RLock()
			locked := true
			t.Cleanup(func() {
				if locked {
					authority.installMu.RUnlock()
				}
			})
			done := make(chan error, 1)
			go func() { done <- client.closeTemporary(t.Context(), temporary) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("temporary cleanup did not request bound retirement")
			}
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			for {
				select {
				case id := <-started:
					t.Fatalf("raw close %q crossed active installation", id)
				case <-deadline.C:
					t.Fatal("bound retirement did not wait for installation")
				default:
				}
				if !authority.installMu.TryRLock() {
					break
				}
				authority.installMu.RUnlock()
				runtime.Gosched()
			}
			select {
			case id := <-started:
				t.Fatalf("raw close %q began before installation ended", id)
			default:
			}
			authority.installMu.RUnlock()
			locked = false
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("failed temporary close was hidden")
				}
			case <-time.After(time.Second):
				t.Fatal("temporary cleanup did not settle")
			}
			if !authority.isStopping() || authority.isClosed() == failFirst {
				t.Fatalf("authority stopping=%t closed=%t", authority.isStopping(), authority.isClosed())
			}
			if failFirst {
				if client.closed || len(client.actions) != 1 || client.planBytes != 64 {
					t.Fatal("failed retirement discarded retained action or byte charge")
				}
				if err := authority.close(t.Context()); err != nil {
					t.Fatalf("retirement retry: %v", err)
				}
				if len(ids) != 2 || ids[0] == ids[1] {
					t.Fatalf("known failed retirement reused action %v", ids)
				}
			}
			if !authority.isClosed() || !client.closed || len(client.actions) != 0 || client.planBytes != 0 {
				t.Fatal("confirmed retirement retained ownership")
			}
		})
	}
}
