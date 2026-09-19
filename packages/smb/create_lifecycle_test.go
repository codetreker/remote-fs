package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type createLifecycleResult struct {
	reference storage.NodeReference
	attr      storage.Attr
	outcome   storage.OpenOutcome
	err       error
}

type createLifecycleSession struct {
	storage.FileSession
	open                        func(context.Context, int32) createLifecycleResult
	opens, files, children, ids atomic.Int32
}

func (*createLifecycleSession) CheckAtomicFileOpen() error { return nil }
func (*createLifecycleSession) CheckNodeReferences() error { return nil }

func (s *createLifecycleSession) OpenAt(ctx context.Context, _ storage.ChildName, _ storage.OpenAtOptions) (storage.OpenResult, error) {
	s.files.Add(1)
	result := s.open(ctx, s.opens.Add(1))
	var file storage.File
	if result.reference != nil {
		file = result.reference.(storage.File)
	}
	return storage.OpenResult{File: file, Attr: result.attr, Outcome: result.outcome}, result.err
}

func (s *createLifecycleSession) OpenChildRef(ctx context.Context, _ storage.ChildName, _ storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.children.Add(1)
	result := s.open(ctx, s.opens.Add(1))
	return storage.NodeOpenResult{Reference: result.reference, Attr: result.attr, Outcome: result.outcome}, result.err
}

func (s *createLifecycleSession) OpenNodeRef(ctx context.Context, _ uint64, _ storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	s.ids.Add(1)
	result := s.open(ctx, s.opens.Add(1))
	return storage.NodeOpenResult{Reference: result.reference, Attr: result.attr, Outcome: result.outcome}, result.err
}

type createLifecycleIdentity uint64

func (i createLifecycleIdentity) ReferenceNodeID() (uint64, error) { return uint64(i), nil }

type createLifecycleNode struct {
	*handleTestReference
	createLifecycleIdentity
}

type createLifecycleFile struct {
	*handleTestFile
	createLifecycleIdentity
}

func createLifecycleReference(ordinary bool, id uint64, closeFn func(context.Context, int32) error) (storage.NodeReference, *handleTestReference) {
	reference := &handleTestReference{closeFn: closeFn}
	if ordinary {
		return &createLifecycleFile{handleTestFile: &handleTestFile{handleTestReference: reference}, createLifecycleIdentity: createLifecycleIdentity(id)}, reference
	}
	return &createLifecycleNode{handleTestReference: reference, createLifecycleIdentity: createLifecycleIdentity(id)}, reference
}

type createLifecycleFixture struct {
	connection *connection
	tree       *tree
	raw        *createLifecycleSession
	resolves   atomic.Int32
}

func newCreateLifecycleFixture(t *testing.T) *createLifecycleFixture {
	t.Helper()
	registry := handleTestRegistry()
	raw := &createLifecycleSession{}
	registry.tree.authority.raw = raw
	registry.tree.export.share = Share{Name: "share", Volume: "trusted-volume", Backend: struct{ storage.FileStorage }{}}
	connection := newConnection(registry.tree.export.server, &capturedConnection{})
	t.Cleanup(connection.cancel)
	return &createLifecycleFixture{connection: connection, tree: registry.tree, raw: raw}
}

func (f *createLifecycleFixture) resolve(context.Context, storage.FileStorage, storage.FileSession, string, Limits, func(context.Context, storage.Operation) error) (resolvedName, error) {
	f.resolves.Add(1)
	return resolvedName{
		RootID: 1, Target: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("leaf")},
		Condition: storage.ChildCondition{State: storage.Absent},
		Guards:    storage.NamespaceGuards{RootID: 1, Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: []byte{1}}}},
	}, nil
}

func createLifecycleRequest(ordinary bool) wire.CreateRequest {
	access := fileReadAttributes
	if ordinary {
		access = fileReadData
	}
	return wire.CreateRequest{Name: "leaf", Impersonation: 2, DesiredAccess: access, ShareAccess: 7, Disposition: 2, Attributes: dosNormal}
}

func createLifecycleAttr(t *testing.T) storage.Attr {
	t.Helper()
	birth, changed := time.Unix(1, 0).UTC(), time.Unix(4, 0).UTC()
	payload, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosArchive})
	if err != nil {
		t.Fatal(err)
	}
	return storage.Attr{ID: 47, Kind: storage.NodeRegular, Size: 513, BirthTime: &birth, ChangeTime: &changed,
		AccessTime: time.Unix(2, 0).UTC(), ModTime: time.Unix(3, 0).UTC(),
		Metadata: map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte{1}, Data: payload}}}
}

func createLifecycleReservation(t *testing.T, registry *handleRegistry) *openReservation {
	t.Helper()
	registry.mu.Lock()
	count := len(registry.reservations)
	var result *openReservation
	for reservation := range registry.reservations {
		result = reservation
	}
	registry.mu.Unlock()
	if count != 1 {
		t.Fatalf("retained CREATE reservations = %d, want one", count)
	}
	return result
}

func createLifecycleCalls(t *testing.T, fixture *createLifecycleFixture, ordinary bool, count int32) {
	t.Helper()
	files, children := int32(0), count
	if ordinary {
		files, children = count, 0
	}
	if fixture.resolves.Load() != count || fixture.raw.opens.Load() != count || fixture.raw.files.Load() != files || fixture.raw.children.Load() != children || fixture.raw.ids.Load() != 0 {
		t.Fatalf("CREATE calls = resolves %d, total %d, files %d, children %d, identities %d; want %d, %d, %d, %d, 0", fixture.resolves.Load(), fixture.raw.opens.Load(), fixture.raw.files.Load(), fixture.raw.children.Load(), fixture.raw.ids.Load(), count, count, files, children)
	}
}

func TestCreateLifecycleRetainsPartialOpenResultsUntilCleanupSucceeds(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		for _, zeroAttr := range []bool{false, true} {
			name := "metadata reference"
			if ordinary {
				name = "ordinary file"
			}
			if zeroAttr {
				name += "/zero attributes"
			} else {
				name += "/captured attributes"
			}
			t.Run(name, func(t *testing.T) {
				fixture := newCreateLifecycleFixture(t)
				attr, outcome := createLifecycleAttr(t), storage.Created
				if zeroAttr {
					attr, outcome = storage.Attr{}, 0
				}
				openFailure, cleanupFailure := errors.New("native open outcome unknown"), errors.New("native reference cleanup refused")
				reference, native := createLifecycleReference(ordinary, 47, func(_ context.Context, attempt int32) error {
					if attempt == 1 {
						return cleanupFailure
					}
					return nil
				})
				fixture.raw.open = func(context.Context, int32) createLifecycleResult {
					return createLifecycleResult{reference: reference, attr: attr, outcome: outcome, err: openFailure}
				}
				body, err := fixture.connection.createOpen(t.Context(), fixture.tree, createLifecycleRequest(ordinary), fixture.resolve)
				if body != nil || !errors.Is(err, openFailure) || !errors.Is(err, cleanupFailure) {
					t.Fatalf("partial CREATE result = %x, %v", body, err)
				}
				createLifecycleCalls(t, fixture, ordinary, 1)
				reservation := createLifecycleReservation(t, fixture.tree.files)
				reservation.mu.Lock()
				charge := reservation.charge
				owned := reservation.reference == reference && !reservation.finished && !reservation.installed && reflect.DeepEqual(reservation.attr, attr) && reservation.outcome == outcome
				reservation.mu.Unlock()
				if !owned || charge <= 0 || native.closeCalls.Load() != 1 || native.attributeCalls.Load() != 0 {
					t.Fatal("failed CREATE lost its exact cleanup reference, captured result, or charge")
				}
				handleTestAccounting(t, fixture.tree.files, 1, 1, 0, charge)
				if err := fixture.tree.files.close(t.Context()); err != nil {
					t.Fatal(err)
				}
				handleTestAccounting(t, fixture.tree.files, 0, 0, 0, 0)
				if native.closeCalls.Load() != 2 || native.attributeCalls.Load() != 0 {
					t.Fatalf("CREATE cleanup retries = %d, attribute probes = %d", native.closeCalls.Load(), native.attributeCalls.Load())
				}
			})
		}
	}
}

func TestCreateLifecycleEncodingFailureClosesTheCapturedReference(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		name := "metadata reference"
		if ordinary {
			name = "ordinary file"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newCreateLifecycleFixture(t)
			attr := createLifecycleAttr(t)
			attr.BirthTime = nil
			reference, native := createLifecycleReference(ordinary, attr.ID, func(context.Context, int32) error { return nil })
			fixture.raw.open = func(context.Context, int32) createLifecycleResult {
				return createLifecycleResult{reference: reference, attr: attr, outcome: storage.Created}
			}
			body, err := fixture.connection.createOpen(t.Context(), fixture.tree, createLifecycleRequest(ordinary), fixture.resolve)
			if body != nil || !errors.Is(err, syscall.EOPNOTSUPP) {
				t.Fatalf("unknown captured creation time = %x, %v", body, err)
			}
			createLifecycleCalls(t, fixture, ordinary, 1)
			handleTestAccounting(t, fixture.tree.files, 0, 0, 0, 0)
			if native.closeCalls.Load() != 1 || native.attributeCalls.Load() != 0 {
				t.Fatalf("encoding failure cleanup = %d close calls, %d attribute probes", native.closeCalls.Load(), native.attributeCalls.Load())
			}
		})
	}
}

func TestCreateLifecycleRetirementRejectsALateSuccessfulOpen(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		name := "metadata reference"
		if ordinary {
			name = "ordinary file"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fixture := newCreateLifecycleFixture(t)
				attr := createLifecycleAttr(t)
				reference, native := createLifecycleReference(ordinary, attr.ID, func(context.Context, int32) error { return nil })
				entered, resume := make(chan struct{}), make(chan struct{})
				release := sync.OnceFunc(func() { close(resume) })
				var workers sync.WaitGroup
				defer func() { release(); workers.Wait() }()
				fixture.raw.open = func(context.Context, int32) createLifecycleResult {
					close(entered)
					<-resume
					return createLifecycleResult{reference: reference, attr: attr, outcome: storage.Created}
				}
				type response struct {
					body []byte
					err  error
				}
				completed := make(chan response, 1)
				workers.Add(1)
				go func() {
					defer workers.Done()
					body, err := fixture.connection.createOpen(t.Context(), fixture.tree, createLifecycleRequest(ordinary), fixture.resolve)
					completed <- response{body: body, err: err}
				}()
				select {
				case <-entered:
				case result := <-completed:
					t.Fatalf("CREATE finished before native admission: %x, %v", result.body, result.err)
				}
				reservation := createLifecycleReservation(t, fixture.tree.files)
				handleTestAccounting(t, fixture.tree.files, 1, 1, 0, reservation.charge)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if err := fixture.tree.files.close(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("retirement while native open is pending = %v", err)
				}
				release()
				result := <-completed
				if result.body != nil || !errors.Is(result.err, syscall.EIO) {
					t.Fatalf("late CREATE installed after retirement = %x, %v", result.body, result.err)
				}
				createLifecycleCalls(t, fixture, ordinary, 1)
				handleTestAccounting(t, fixture.tree.files, 0, 0, 0, 0)
				if native.closeCalls.Load() != 1 || native.attributeCalls.Load() != 0 {
					t.Fatalf("late open cleanup = %d close calls, %d attribute probes", native.closeCalls.Load(), native.attributeCalls.Load())
				}
			})
		})
	}
}

func TestCreateLifecycleDoesNotRetryUnknownOrNonzeroFailedResults(t *testing.T) {
	for _, test := range []struct {
		name      string
		err       error
		attr      storage.Attr
		outcome   storage.OpenOutcome
		reference bool
	}{
		{name: "unknown I/O outcome", err: syscall.EIO},
		{name: "unclassified EAGAIN", err: syscall.EAGAIN},
		{name: "condition with size", err: storage.ErrConditionConflict, attr: storage.Attr{Size: 1}},
		{name: "condition with nonnil empty metadata", err: storage.ErrConditionConflict, attr: storage.Attr{Metadata: map[string]storage.OpaquePayload{}}},
		{name: "condition with outcome", err: storage.ErrConditionConflict, outcome: storage.Opened},
		{name: "condition with cleanup reference", err: storage.ErrConditionConflict, reference: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCreateLifecycleFixture(t)
			var reference storage.NodeReference
			var native *handleTestReference
			if test.reference {
				reference, native = createLifecycleReference(true, 47, func(context.Context, int32) error { return nil })
			}
			fixture.raw.open = func(context.Context, int32) createLifecycleResult {
				return createLifecycleResult{reference: reference, attr: test.attr, outcome: test.outcome, err: test.err}
			}
			body, err := fixture.connection.createOpen(t.Context(), fixture.tree, createLifecycleRequest(true), fixture.resolve)
			if body != nil || !errors.Is(err, test.err) {
				t.Fatalf("failed CREATE result = %x, %v; want %v", body, err, test.err)
			}
			createLifecycleCalls(t, fixture, true, 1)
			handleTestAccounting(t, fixture.tree.files, 0, 0, 0, 0)
			if native != nil && (native.closeCalls.Load() != 1 || native.attributeCalls.Load() != 0) {
				t.Fatalf("nonzero error-result cleanup = %d close calls, %d attribute probes", native.closeCalls.Load(), native.attributeCalls.Load())
			}
		})
	}
}

func TestCreateLifecycleRetriesOnlySettledZeroResultConflicts(t *testing.T) {
	for _, succeeds := range []bool{false, true} {
		name := "conflict exhaustion"
		if succeeds {
			name = "fourth attempt succeeds"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newCreateLifecycleFixture(t)
			attr := createLifecycleAttr(t)
			reference, native := createLifecycleReference(true, attr.ID, func(context.Context, int32) error { return nil })
			fixture.raw.open = func(_ context.Context, attempt int32) createLifecycleResult {
				if succeeds && attempt == 4 {
					return createLifecycleResult{reference: reference, attr: attr, outcome: storage.Created}
				}
				return createLifecycleResult{err: storage.ErrConditionConflict}
			}
			body, err := fixture.connection.createOpen(t.Context(), fixture.tree, createLifecycleRequest(true), fixture.resolve)
			createLifecycleCalls(t, fixture, true, 4)
			if !succeeds {
				if body != nil || !errors.Is(err, storage.ErrConditionConflict) {
					t.Fatalf("exhausted zero-effect conflicts = %x, %v", body, err)
				}
				handleTestAccounting(t, fixture.tree.files, 0, 0, 0, 0)
				if native.closeCalls.Load() != 0 {
					t.Fatal("zero-result failure closed an unreturned reference")
				}
				return
			}
			if err != nil || len(body) != 88 || binary.LittleEndian.Uint16(body) != 89 || binary.LittleEndian.Uint32(body[4:]) != 2 || binary.LittleEndian.Uint64(body[40:]) != 1024 || binary.LittleEndian.Uint64(body[48:]) != 513 {
				t.Fatalf("CREATE after settled conflicts = %x, %v", body, err)
			}
			var id wire.FileID
			copy(id[:], body[64:80])
			handle := fixture.tree.files.get(id)
			if handle == nil || handle.reference != reference || handle.nodeID != attr.ID || handle.grantedAccess != fileReadData || handle.shareAccess != 7 {
				t.Fatalf("successful retry did not install its captured reference: %+v", handle)
			}
			handleTestAccounting(t, fixture.tree.files, 1, 0, 1, 0)
			if native.closeCalls.Load() != 0 || native.attributeCalls.Load() != 0 {
				t.Fatal("successful CREATE closed or re-observed its captured reference")
			}
			if err := fixture.tree.files.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			handleTestAccounting(t, fixture.tree.files, 0, 0, 0, 0)
			if native.closeCalls.Load() != 1 || native.attributeCalls.Load() != 0 {
				t.Fatalf("successful retry cleanup = %d close calls, %d attribute probes", native.closeCalls.Load(), native.attributeCalls.Load())
			}
		})
	}
}

type createLifecycleHeldIdentity struct {
	*handleTestReference
	id      uint64
	entered chan struct{}
	resume  <-chan struct{}
	once    sync.Once
}

func (r *createLifecycleHeldIdentity) ReferenceNodeID() (uint64, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.resume
	return r.id, nil
}

func TestCreateLifecycleRetirementWaitsForCapturedResponseConsumption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fixture := newCreateLifecycleFixture(t)
		attr := createLifecycleAttr(t)
		entered, resume := make(chan struct{}), make(chan struct{})
		release := sync.OnceFunc(func() { close(resume) })
		var workers sync.WaitGroup
		defer func() { release(); workers.Wait() }()
		native := &handleTestReference{closeFn: func(context.Context, int32) error { return nil }}
		reference := &createLifecycleHeldIdentity{handleTestReference: native, id: attr.ID, entered: entered, resume: resume}
		fixture.raw.open = func(context.Context, int32) createLifecycleResult {
			return createLifecycleResult{reference: reference, attr: attr, outcome: storage.Created}
		}
		type response struct {
			body []byte
			err  error
		}
		created := make(chan response, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			body, err := fixture.connection.createOpen(t.Context(), fixture.tree, createLifecycleRequest(false), fixture.resolve)
			created <- response{body: body, err: err}
		}()
		select {
		case <-entered:
		case result := <-created:
			t.Fatalf("CREATE completed before consuming its captured reference: %x, %v", result.body, result.err)
		}
		registry := fixture.tree.files
		reservation := createLifecycleReservation(t, registry)
		reservation.mu.Lock()
		charge := reservation.charge
		reservation.mu.Unlock()
		select {
		case <-reservation.ready:
		default:
			t.Fatal("identity getter ran before the native result was attached")
		}
		assertRetained := func() {
			t.Helper()
			handleTestAccounting(t, registry, 1, 1, 0, charge)
			reservation.mu.Lock()
			owned := reservation.reference == reference && !reservation.finished && !reservation.installed && reservation.charge == charge && reflect.DeepEqual(reservation.attr, attr) && reservation.outcome == storage.Created
			reservation.mu.Unlock()
			if !owned || native.closeCalls.Load() != 0 || native.attributeCalls.Load() != 0 {
				t.Fatal("retirement reclaimed a reference or response metadata still being consumed")
			}
		}
		cleanupCtx, cancelCleanup := context.WithTimeout(t.Context(), fixture.connection.server.config.Limits.CleanupTimeout)
		defer cancelCleanup()
		cleanupDone := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			cleanupDone <- registry.close(cleanupCtx)
		}()
		synctest.Wait()
		registry.mu.Lock()
		retired := registry.retired
		registry.mu.Unlock()
		if !retired {
			t.Fatal("registry cleanup did not fence installation")
		}
		select {
		case err := <-cleanupDone:
			t.Fatalf("retirement finished while response consumption was held: %v", err)
		default:
		}
		assertRetained()
		cancelCleanup()
		if err := <-cleanupDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled response cleanup = %v", err)
		}
		assertRetained()
		retryCtx, cancelRetry := context.WithTimeout(t.Context(), fixture.connection.server.config.Limits.CleanupTimeout)
		defer cancelRetry()
		retried := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			retried <- registry.close(retryCtx)
		}()
		synctest.Wait()
		select {
		case err := <-retried:
			t.Fatalf("cleanup retry passed the response hold: %v", err)
		default:
		}
		assertRetained()
		release()
		result := <-created
		if result.body != nil || !errors.Is(result.err, syscall.EIO) {
			t.Fatalf("CREATE installed after response-consumer retirement: %x, %v", result.body, result.err)
		}
		if err := <-retried; err != nil {
			t.Fatal(err)
		}
		createLifecycleCalls(t, fixture, false, 1)
		handleTestAccounting(t, registry, 0, 0, 0, 0)
		if native.closeCalls.Load() != 1 || native.attributeCalls.Load() != 0 {
			t.Fatalf("response consumption cleanup = %d close calls, %d attribute probes", native.closeCalls.Load(), native.attributeCalls.Load())
		}
	})
}
