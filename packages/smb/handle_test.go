package smb

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

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type handleTestReference struct {
	closeFn                    func(context.Context, int32) error
	closeCalls, attributeCalls atomic.Int32
}

func (r *handleTestReference) Stat(context.Context) (storage.Attr, error) {
	r.attributeCalls.Add(1)
	return storage.Attr{}, syscall.EBADF
}

func (r *handleTestReference) SetAttr(context.Context, storage.AttrChange) (storage.Attr, error) {
	r.attributeCalls.Add(1)
	return storage.Attr{}, syscall.EBADF
}

func (r *handleTestReference) Close(ctx context.Context) error {
	return r.closeFn(ctx, r.closeCalls.Add(1))
}

type handleTestFile struct{ *handleTestReference }

type handleTestFileSession struct {
	storage.FileSession
	closeErr   error
	closeCalls atomic.Int32
}

func (s *handleTestFileSession) Close(context.Context) error {
	s.closeCalls.Add(1)
	return s.closeErr
}

type handleTestOwnerSession struct {
	*handleTestFileSession
	storage.UseOwners
	explicitRetires atomic.Int32
}

func (s *handleTestOwnerSession) RetireUseOwner(context.Context, storage.UseOwner) error {
	s.explicitRetires.Add(1)
	return nil
}

func (*handleTestFile) ReadAt(context.Context, int64, int) (storage.FileRead, error) {
	return storage.FileRead{}, syscall.EBADF
}
func (*handleTestFile) WriteAt(context.Context, int64, []byte) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (*handleTestFile) Truncate(context.Context, int64) (storage.Attr, error) {
	return storage.Attr{}, syscall.EBADF
}
func (*handleTestFile) Sync(context.Context) error { return syscall.EBADF }

func handleTestRegistry() *handleRegistry {
	config := testConfig()
	config.Limits.MaxOpens = 1
	server := &Server{config: config}
	export := &Export{server: server, published: true}
	authority := &authoritySession{export: export, deadline: time.Now().Add(time.Hour)}
	tree := &tree{kind: volumeTree, export: export, authority: authority}
	tree.files = newHandleRegistry(tree, config.Limits)
	return tree.files
}

func handleTestReserve(t *testing.T, registry *handleRegistry) *openReservation {
	t.Helper()
	reservation, err := registry.reserve()
	if err != nil || reservation == nil || reservation.charge <= 0 {
		t.Fatalf("reserve handle = %v, %v", reservation, err)
	}
	return reservation
}

func handleTestAttach(p *openReservation, reference *handleTestReference, ordinary bool, attr storage.Attr, outcome storage.OpenOutcome) (storage.NodeReference, storage.File) {
	if ordinary {
		file := &handleTestFile{handleTestReference: reference}
		p.attachFile(storage.OpenResult{File: file, Attr: attr, Outcome: outcome})
		return file, file
	}
	p.attachNode(storage.NodeOpenResult{Reference: reference, Attr: attr, Outcome: outcome})
	return reference, nil
}

func handleTestAccounting(t *testing.T, r *handleRegistry, slots, reservations, handles int, resultBytes int64) {
	t.Helper()
	r.mu.Lock()
	gotSlots, gotReservations, gotHandles, gotBytes := r.slots, len(r.reservations), len(r.handles), r.resultBytes
	r.mu.Unlock()
	r.tree.export.server.mu.Lock()
	exportOpens := r.tree.export.opens
	r.tree.export.server.mu.Unlock()
	if gotSlots != slots || exportOpens != slots || gotReservations != reservations || gotHandles != handles || gotBytes != resultBytes {
		t.Fatalf("handle accounting = slots %d, export opens %d, reservations %d, handles %d, result bytes %d; want %d, %d, %d, %d, %d", gotSlots, exportOpens, gotReservations, gotHandles, gotBytes, slots, slots, reservations, handles, resultBytes)
	}
}

func TestHandleReservationRetainsCleanupOnlyReferences(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		name := "metadata reference"
		if ordinary {
			name = "ordinary file"
		}
		t.Run(name, func(t *testing.T) {
			registry := handleTestRegistry()
			reservation := handleTestReserve(t, registry)
			charge := reservation.charge
			failure := errors.New("reference cleanup refused")
			reference := &handleTestReference{closeFn: func(_ context.Context, attempt int32) error {
				if attempt == 1 {
					return failure
				}
				return nil
			}}
			retained, file := handleTestAttach(reservation, reference, ordinary, storage.Attr{}, 0)
			if id, err := reservation.install(0, 0); !errors.Is(err, syscall.EINVAL) || id != (wire.FileID{}) {
				t.Fatalf("cleanup-only installation = %v, %v", id, err)
			}
			if err := reservation.finish(t.Context()); !errors.Is(err, failure) {
				t.Fatalf("failed open cleanup = %v", err)
			}
			handleTestAccounting(t, registry, 1, 1, 0, charge)
			reservation.mu.Lock()
			owned := reservation.reference == retained && reservation.file == file && !reservation.finished && reservation.charge == charge && reflect.DeepEqual(reservation.attr, storage.Attr{})
			reservation.mu.Unlock()
			if !owned {
				t.Fatal("failed cleanup lost its original reference or charge")
			}
			if _, err := registry.reserve(); !errors.Is(err, syscall.ENOMEM) {
				t.Fatalf("failed cleanup released admission: %v", err)
			}
			if err := reservation.finish(t.Context()); err != nil {
				t.Fatal(err)
			}
			handleTestAccounting(t, registry, 0, 0, 0, 0)
			if err := reservation.finish(t.Context()); err != nil || reference.closeCalls.Load() != 2 || reference.attributeCalls.Load() != 0 {
				t.Fatalf("repeated cleanup = %v, close calls %d, attribute calls %d", err, reference.closeCalls.Load(), reference.attributeCalls.Load())
			}
			next := handleTestReserve(t, registry)
			next.attachNode(storage.NodeOpenResult{})
			if err := next.finish(t.Context()); err != nil {
				t.Fatal(err)
			}
			handleTestAccounting(t, registry, 0, 0, 0, 0)
		})
	}
}

func TestHandleInstallationTransfersOneReferenceAndKeepsCapturedResults(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		name := "metadata reference"
		if ordinary {
			name = "ordinary file"
		}
		t.Run(name, func(t *testing.T) {
			registry := handleTestRegistry()
			reservation := handleTestReserve(t, registry)
			charge := reservation.charge
			attr := storage.Attr{ID: 41, Kind: storage.NodeRegular, Size: 17, Metadata: map[string]storage.OpaquePayload{"test.result": {Version: []byte{1}, Data: []byte{0xff, 7}}}}
			reference := &handleTestReference{closeFn: func(context.Context, int32) error { return nil }}
			retained, file := handleTestAttach(reservation, reference, ordinary, attr, storage.Created)
			id, err := reservation.install(0x1234, 0x7)
			if err != nil {
				t.Fatal(err)
			}
			handle := registry.get(id)
			if handle == nil || handle.reference != retained || handle.file != file || handle.nodeID != attr.ID || handle.grantedAccess != 0x1234 || handle.shareAccess != 0x7 {
				t.Fatalf("installed handle = %+v", handle)
			}
			if reservation.reference != nil || reservation.file != nil || reservation.outcome != storage.Created || !reflect.DeepEqual(reservation.attr, attr) {
				t.Fatal("installation lost captured attributes or retained a second reference owner")
			}
			handleTestAccounting(t, registry, 1, 1, 1, charge)
			if _, err := reservation.install(0, 0); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("repeated installation = %v", err)
			}
			if err := reservation.finish(t.Context()); err != nil || reference.closeCalls.Load() != 0 {
				t.Fatalf("response completion closed installed reference: %v", err)
			}
			handleTestAccounting(t, registry, 1, 0, 1, 0)
			if err := registry.closeID(t.Context(), id, handle); err != nil {
				t.Fatal(err)
			}
			if err := registry.closeID(t.Context(), id, handle); err != nil || reference.closeCalls.Load() != 1 || reference.attributeCalls.Load() != 0 {
				t.Fatalf("repeated handle close = %v, close calls %d, attribute calls %d", err, reference.closeCalls.Load(), reference.attributeCalls.Load())
			}
			handleTestAccounting(t, registry, 0, 0, 0, 0)
		})
	}
}

func TestHandleCloseFailureRetainsReferenceAndResponseCharge(t *testing.T) {
	registry := handleTestRegistry()
	reservation := handleTestReserve(t, registry)
	charge := reservation.charge
	attr := storage.Attr{ID: 42, Kind: storage.NodeRegular, Size: 9, ModTime: time.Unix(100, 0).UTC(), Metadata: map[string]storage.OpaquePayload{"test.result": {Version: []byte{1}, Data: []byte("captured")}}}
	failure := errors.New("reference still owns native resources")
	reference := &handleTestReference{closeFn: func(_ context.Context, attempt int32) error {
		if attempt == 1 {
			return failure
		}
		return nil
	}}
	retained, file := handleTestAttach(reservation, reference, true, attr, storage.Opened)
	id, err := reservation.install(3, 7)
	if err != nil {
		t.Fatal(err)
	}
	handle := registry.get(id)
	if err := registry.closeID(t.Context(), id, handle); !errors.Is(err, failure) {
		t.Fatalf("native close refusal = %v", err)
	}
	handleTestAccounting(t, registry, 1, 1, 1, charge)
	if registry.get(id) != handle || handle.reference != retained || handle.file != file || !handle.retiring || handle.closed {
		t.Fatal("failed close dropped or replaced the installed reference")
	}
	if !reflect.DeepEqual(reservation.attr, attr) || reservation.outcome != storage.Opened || reservation.charge != charge || reservation.finished {
		t.Fatal("failed close lost the original open response or its charge")
	}
	if _, err := handle.borrow(); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("retiring reference allowed new borrow: %v", err)
	}
	if _, err := registry.reserve(); !errors.Is(err, syscall.ENOMEM) {
		t.Fatalf("failed close released its slot: %v", err)
	}
	if err := registry.closeID(t.Context(), id, handle); err != nil {
		t.Fatal(err)
	}
	handleTestAccounting(t, registry, 0, 1, 0, charge)
	if handle.reference != nil || handle.file != nil || !handle.closed || !reflect.DeepEqual(reservation.attr, attr) {
		t.Fatal("successful reference cleanup changed pending response ownership")
	}
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	handleTestAccounting(t, registry, 0, 0, 0, 0)
	if err := registry.closeID(t.Context(), id, handle); err != nil || reference.closeCalls.Load() != 2 || reference.attributeCalls.Load() != 0 {
		t.Fatalf("cleanup retried after success: %v, close calls %d, attribute calls %d", err, reference.closeCalls.Load(), reference.attributeCalls.Load())
	}
}

func TestHandleCloseLeavesReferenceOwnerRetirementToTheReference(t *testing.T) {
	registry := handleTestRegistry()
	raw := &handleTestOwnerSession{handleTestFileSession: &handleTestFileSession{}}
	registry.tree.authority.raw = raw
	reservation := handleTestReserve(t, registry)
	charge := reservation.charge
	failure := errors.New("reference retirement has not completed")
	var referenceRetires atomic.Int32
	reference := &handleTestReference{closeFn: func(_ context.Context, attempt int32) error {
		if attempt == 1 {
			return failure
		}
		referenceRetires.Add(1)
		return nil
	}}
	retained, file := handleTestAttach(reservation, reference, true, storage.Attr{ID: 46, Kind: storage.NodeRegular, Size: 5}, storage.Opened)
	id, err := reservation.install(3, 7)
	if err != nil {
		t.Fatal(err)
	}
	handle := registry.get(id)
	owner := storage.UseOwner(73)
	handle.rangeOwner = &owner
	if err := registry.closeID(t.Context(), id, handle); !errors.Is(err, failure) {
		t.Fatalf("reference retirement refusal = %v", err)
	}
	if raw.explicitRetires.Load() != 0 || referenceRetires.Load() != 0 || handle.rangeOwner != &owner || handle.reference != retained || handle.file != file || handle.closed {
		t.Fatalf("failed reference close released owner protection: explicit retire calls %d, reference retire calls %d", raw.explicitRetires.Load(), referenceRetires.Load())
	}
	handleTestAccounting(t, registry, 1, 1, 1, charge)
	if err := registry.closeID(t.Context(), id, handle); err != nil {
		t.Fatal(err)
	}
	if raw.explicitRetires.Load() != 0 || referenceRetires.Load() != 1 || reference.closeCalls.Load() != 2 || handle.rangeOwner != nil || handle.reference != nil || handle.file != nil || !handle.closed {
		t.Fatalf("successful reference close ownership = explicit retire calls %d, reference retire calls %d, close calls %d", raw.explicitRetires.Load(), referenceRetires.Load(), reference.closeCalls.Load())
	}
	handleTestAccounting(t, registry, 0, 1, 0, charge)
	if err := reservation.finish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := registry.closeID(t.Context(), id, handle); err != nil || raw.explicitRetires.Load() != 0 || referenceRetires.Load() != 1 || reference.attributeCalls.Load() != 0 {
		t.Fatalf("completed owner cleanup repeated work: %v", err)
	}
	handleTestAccounting(t, registry, 0, 0, 0, 0)
}

func TestHandleLateOpenCannotInstallAfterRetirement(t *testing.T) {
	for _, stop := range []string{"registry", "authority"} {
		t.Run(stop, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				registry := handleTestRegistry()
				reservation := handleTestReserve(t, registry)
				charge := reservation.charge
				reference := &handleTestReference{closeFn: func(context.Context, int32) error { return nil }}
				resume := make(chan struct{})
				release := sync.OnceFunc(func() { close(resume) })
				var workers sync.WaitGroup
				defer func() { release(); workers.Wait() }()
				type installResult struct {
					id  wire.FileID
					err error
				}
				installed := make(chan installResult, 1)
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-resume
					reservation.attachNode(storage.NodeOpenResult{Reference: reference, Attr: storage.Attr{ID: 43, Kind: storage.NodeDirectory}, Outcome: storage.Opened})
					id, err := reservation.install(0, 0)
					installed <- installResult{id: id, err: err}
				}()
				synctest.Wait()
				if stop == "registry" {
					ctx, cancel := context.WithCancel(t.Context())
					cancel()
					if err := registry.close(ctx); !errors.Is(err, context.Canceled) {
						t.Fatalf("retirement awaiting late result = %v", err)
					}
				} else {
					authority := registry.tree.authority
					failure := errors.New("authority still owns native references")
					raw := &handleTestFileSession{closeErr: failure}
					authority.raw = raw
					if err := authority.close(t.Context()); !errors.Is(err, failure) {
						t.Fatalf("authority retirement failure = %v", err)
					}
					if !authority.isStopping() || authority.isClosed() || raw.closeCalls.Load() != 1 {
						t.Fatal("failed authority close did not preserve stopping ownership")
					}
				}
				handleTestAccounting(t, registry, 1, 1, 0, charge)
				release()
				result := <-installed
				if !errors.Is(result.err, syscall.EIO) || result.id != (wire.FileID{}) {
					t.Fatalf("late install after %s retirement = %v, %v", stop, result.id, result.err)
				}
				if reservation.reference != reference || reservation.finished || reference.closeCalls.Load() != 0 {
					t.Fatal("refused installation discarded its late cleanup reference")
				}
				if err := registry.close(t.Context()); err != nil {
					t.Fatal(err)
				}
				handleTestAccounting(t, registry, 0, 0, 0, 0)
				if reference.closeCalls.Load() != 1 || reference.attributeCalls.Load() != 0 {
					t.Fatalf("late reference cleanup = %d close calls, %d attribute calls", reference.closeCalls.Load(), reference.attributeCalls.Load())
				}
			})
		})
	}
}

func TestHandleCloseDrainsBorrowedWorkAndRejectsNewBorrows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := handleTestRegistry()
		reservation := handleTestReserve(t, registry)
		nativeClosed := make(chan struct{})
		reference := &handleTestReference{closeFn: func(context.Context, int32) error {
			close(nativeClosed)
			return nil
		}}
		handleTestAttach(reservation, reference, true, storage.Attr{ID: 44, Kind: storage.NodeRegular}, storage.Opened)
		id, err := reservation.install(3, 7)
		if err != nil {
			t.Fatal(err)
		}
		handle := registry.get(id)
		if err := reservation.finish(t.Context()); err != nil {
			t.Fatal(err)
		}
		done, err := handle.borrow()
		if err != nil {
			t.Fatal(err)
		}
		releaseBorrow := sync.OnceFunc(done)
		var workers sync.WaitGroup
		defer func() { releaseBorrow(); workers.Wait() }()
		closed := make(chan error, 1)
		workers.Add(1)
		go func() {
			defer workers.Done()
			closed <- registry.closeID(t.Context(), id, handle)
		}()
		<-nativeClosed
		synctest.Wait()
		select {
		case err := <-closed:
			t.Fatalf("close finished before borrowed work drained: %v", err)
		default:
		}
		if _, err := handle.borrow(); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("retiring handle admitted another borrow: %v", err)
		}
		handleTestAccounting(t, registry, 1, 0, 1, 0)
		handle.mu.Lock()
		active, retiring, finished := handle.active, handle.retiring, handle.closed
		handle.mu.Unlock()
		if active != 1 || !retiring || finished {
			t.Fatalf("draining handle = active %d, retiring %v, closed %v", active, retiring, finished)
		}
		releaseBorrow()
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		handleTestAccounting(t, registry, 0, 0, 0, 0)
		if registry.get(id) != nil || reference.closeCalls.Load() != 1 || reference.attributeCalls.Load() != 0 {
			t.Fatalf("drained close retained lookup or repeated native work: close calls %d, attribute calls %d", reference.closeCalls.Load(), reference.attributeCalls.Load())
		}
	})
}

func TestHandleCleanupWaitersShareOneImmutableAttempt(t *testing.T) {
	for _, kind := range []string{"reservation", "installed handle"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				registry := handleTestRegistry()
				reservation := handleTestReserve(t, registry)
				charge := reservation.charge
				entered, resume := make(chan struct{}), make(chan struct{})
				release := sync.OnceFunc(func() { close(resume) })
				var workers sync.WaitGroup
				defer func() { release(); workers.Wait() }()
				failure := errors.New("first cleanup attempt failed")
				reference := &handleTestReference{closeFn: func(ctx context.Context, attempt int32) error {
					if attempt != 1 {
						return nil
					}
					close(entered)
					select {
					case <-resume:
						return failure
					case <-ctx.Done():
						return ctx.Err()
					}
				}}
				attr := storage.Attr{ID: 45, Kind: storage.NodeDirectory, Metadata: map[string]storage.OpaquePayload{"test.result": {Version: []byte{1}, Data: []byte("original")}}}
				handleTestAttach(reservation, reference, false, attr, storage.Created)
				cleanup := reservation.finish
				reservations, handles := 1, 0
				if kind == "installed handle" {
					id, err := reservation.install(0, 0)
					if err != nil {
						t.Fatal(err)
					}
					handle := registry.get(id)
					if err := reservation.finish(t.Context()); err != nil {
						t.Fatal(err)
					}
					cleanup = func(ctx context.Context) error { return registry.closeID(ctx, id, handle) }
					reservations, handles, charge = 0, 1, 0
				}
				first, second := make(chan error, 1), make(chan error, 1)
				workers.Add(1)
				go func() {
					defer workers.Done()
					first <- cleanup(t.Context())
				}()
				<-entered
				workers.Add(1)
				go func() {
					defer workers.Done()
					second <- cleanup(t.Context())
				}()
				synctest.Wait()
				handleTestAccounting(t, registry, 1, reservations, handles, charge)
				release()
				firstErr, secondErr := <-first, <-second
				if !errors.Is(firstErr, failure) || secondErr != firstErr || reference.closeCalls.Load() != 1 {
					t.Fatalf("concurrent cleanup outcomes = %v, %v; native attempts %d", firstErr, secondErr, reference.closeCalls.Load())
				}
				handleTestAccounting(t, registry, 1, reservations, handles, charge)
				if kind == "reservation" && (!reflect.DeepEqual(reservation.attr, attr) || reservation.outcome != storage.Created || reservation.reference != reference) {
					t.Fatal("failed shared attempt changed its captured result or reference")
				}
				if err := cleanup(t.Context()); err != nil {
					t.Fatal(err)
				}
				handleTestAccounting(t, registry, 0, 0, 0, 0)
				if err := cleanup(t.Context()); err != nil || reference.closeCalls.Load() != 2 || reference.attributeCalls.Load() != 0 {
					t.Fatalf("explicit retry cleanup = %v, close calls %d, attribute calls %d", err, reference.closeCalls.Load(), reference.attributeCalls.Load())
				}
				if !errors.Is(firstErr, failure) || secondErr != firstErr {
					t.Fatal("successful retry changed a prior cleanup attempt's result")
				}
			})
		})
	}
}
