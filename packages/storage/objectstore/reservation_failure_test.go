package objectstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type putFailureObjects struct {
	objectstore.Objects
	failure error
}

type cancelingPutObjects struct {
	objectstore.Objects
	cancel context.CancelFunc
}

type landedPutFailureObjects struct {
	objectstore.Objects
	failure error
	key     string
}

func (o *landedPutFailureObjects) Put(ctx context.Context, key string, content []byte) ([]byte, error) {
	digest, err := o.Objects.Put(ctx, key, content)
	if err != nil {
		return nil, err
	}
	o.key = key
	return digest, o.failure
}

func (o *cancelingPutObjects) Put(ctx context.Context, _ string, _ []byte) ([]byte, error) {
	o.cancel()
	return nil, ctx.Err()
}

func (o *putFailureObjects) Put(context.Context, string, []byte) ([]byte, error) {
	return nil, o.failure
}

type writeFailureStore struct {
	metastore.Store
	commitErr     error
	abandonErr    error
	quarantineErr error
}

type landedCommitFailureStore struct {
	metastore.Store
	failure error
}

func (s *landedCommitFailureStore) Commit(ctx context.Context, path string, object metastore.Object) error {
	if err := s.Store.Commit(ctx, path, object); err != nil {
		return err
	}
	return s.failure
}

func (s *writeFailureStore) Commit(ctx context.Context, path string, object metastore.Object) error {
	if s.commitErr != nil {
		return s.commitErr
	}
	return s.Store.Commit(ctx, path, object)
}

func (s *writeFailureStore) Abandon(ctx context.Context, key metastore.Key) error {
	if s.abandonErr != nil {
		return s.abandonErr
	}
	return s.Store.Abandon(ctx, key)
}

func (s *writeFailureStore) Quarantine(ctx context.Context, key metastore.Key) error {
	if s.quarantineErr != nil {
		return s.quarantineErr
	}
	return s.Store.Quarantine(ctx, key)
}

type blockingAbandonStore struct {
	metastore.Store
	commitErr error
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func (s *blockingAbandonStore) Commit(context.Context, string, metastore.Object) error {
	return s.commitErr
}

func (s *blockingAbandonStore) Abandon(ctx context.Context, _ metastore.Key) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.release:
		return errors.New("the test released Abandon")
	}
}

type gatedReserveStore struct {
	metastore.Store
	reserved chan metastore.Key
	release  chan struct{}
}

func (s *gatedReserveStore) Reserve(ctx context.Context, path string, size int64) (metastore.Key, error) {
	key, err := s.Store.Reserve(ctx, path, size)
	if err != nil {
		return "", err
	}
	s.reserved <- key
	select {
	case <-s.release:
		return key, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func reservationMeta(t *testing.T) *sqlite.Store {
	t.Helper()
	meta, err := sqlite.Open(t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	return meta
}

func requireNoReservations(t *testing.T, meta *sqlite.Store) sqlite.ObjectStatus {
	t.Helper()
	status, err := meta.ObjectStatus(t.Context())
	if err != nil {
		t.Fatalf("reading object status: %v", err)
	}
	if status.ReservedCount != 0 || status.ReservedBytes != 0 {
		t.Fatalf("failed writes retained %d reservations holding %d bytes", status.ReservedCount, status.ReservedBytes)
	}
	return status
}

func TestRepeatedPutFailuresFillTheBoundedUnresolvedBacklog(t *testing.T) {
	meta, err := sqlite.OpenWithObjectLimits(
		t.Context(), filepath.Join(t.TempDir(), "meta.db"), "workspace", limited.MinLimit,
		sqlite.DefaultWindow(), sqlite.ObjectLimits{MaxPendingObjects: 3, MaxPendingBytes: 1024},
	)
	if err != nil {
		t.Fatalf("opening the metastore: %v", err)
	}
	failure := errors.New("the object store refused the upload")
	objects := &putFailureObjects{Objects: memory.New(), failure: failure}
	namespace := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Errorf("closing the namespace: %v", err)
		}
	})

	const attempts = 3
	for range attempts {
		if err := namespace.Write(t.Context(), "f", []byte("content that never landed")); !errors.Is(err, failure) {
			t.Fatalf("failed Put returned %v, want the object-store failure", err)
		}
	}
	status, err := meta.ObjectStatus(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.ReservedCount != 0 || status.UnresolvedCount != attempts ||
		status.UnresolvedBytes != attempts*int64(len("content that never landed")) || status.GarbageCount != 0 {
		t.Fatalf("failed Puts left status %+v, want three bounded unresolved reservations", status)
	}
	if err := namespace.Write(t.Context(), "blocked", []byte("another")); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("writing beyond the unresolved backlog limit returned %v, want EAGAIN", err)
	}
	if removed, err := namespace.Sweep(t.Context(), 100); err != nil || removed != 0 {
		t.Fatalf("sweeping unresolved reservations removed %d objects (%v), want none", removed, err)
	}
}

func TestRepeatedCommitFailuresDoNotAccumulateReservations(t *testing.T) {
	meta := reservationMeta(t)
	failure := errors.New("the metadata commit failed")
	store := &writeFailureStore{Store: meta, commitErr: failure}
	namespace := objectstore.New(memory.New(), store)
	t.Cleanup(func() {
		if err := namespace.Close(); err != nil {
			t.Errorf("closing the namespace: %v", err)
		}
	})

	const attempts = 32
	for range attempts {
		if err := namespace.Write(t.Context(), "f", []byte("content whose commit failed")); !errors.Is(err, failure) {
			t.Fatalf("failed Commit returned %v, want the metastore failure", err)
		}
		requireNoReservations(t, meta)
	}
	await(t, "objects from failed commits to be swept", func() bool {
		return requireNoReservations(t, meta).GarbageCount == 0
	})
}

func TestWriteFailurePreservesReservationCleanupFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		objects    func(error) objectstore.Objects
		commitFail bool
	}{
		{
			name: "put",
			objects: func(failure error) objectstore.Objects {
				return &putFailureObjects{Objects: memory.New(), failure: failure}
			},
		},
		{
			name:       "commit",
			objects:    func(error) objectstore.Objects { return memory.New() },
			commitFail: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			meta := reservationMeta(t)
			operationFailure := errors.New(test.name + " failed")
			cleanupFailure := errors.New("the reservation cleanup failed")
			store := &writeFailureStore{Store: meta}
			if test.commitFail {
				store.commitErr = operationFailure
				store.abandonErr = cleanupFailure
			} else {
				store.quarantineErr = cleanupFailure
			}
			namespace := objectstore.New(test.objects(operationFailure), store)
			t.Cleanup(func() { _ = namespace.Close() })

			err := namespace.Write(t.Context(), "f", []byte("reserved content"))
			if !errors.Is(err, operationFailure) || !errors.Is(err, cleanupFailure) {
				t.Fatalf("Write returned %v, want both the %s and cleanup failures", err, test.name)
			}
			status, statusErr := meta.ObjectStatus(t.Context())
			if statusErr != nil {
				t.Fatalf("reading object status: %v", statusErr)
			}
			if status.ReservedCount != 1 || status.ReservedBytes != int64(len("reserved content")) {
				t.Fatalf("failed abandon left status %+v, want the reservation intact", status)
			}
		})
	}
}

func TestCanceledPutQuarantinesItsReservation(t *testing.T) {
	meta := reservationMeta(t)
	ctx, cancel := context.WithCancel(t.Context())
	objects := &cancelingPutObjects{Objects: memory.New(), cancel: cancel}
	namespace := objectstore.New(objects, meta)
	t.Cleanup(func() { _ = namespace.Close() })

	if err := namespace.Write(ctx, "f", []byte("content")); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Put returned %v, want context cancellation", err)
	}
	status := requireNoReservations(t, meta)
	if status.UnresolvedCount != 1 || status.UnresolvedBytes != int64(len("content")) {
		t.Fatalf("canceled Put left status %+v, want one unresolved reservation", status)
	}
}

func TestPutThatLandedBeforeFailingRemainsUnresolved(t *testing.T) {
	meta := reservationMeta(t)
	failure := errors.New("the upload response was lost")
	objects := &landedPutFailureObjects{Objects: memory.New(), failure: failure}
	namespace := objectstore.New(objects, meta)
	t.Cleanup(func() { _ = namespace.Close() })

	if err := namespace.Write(t.Context(), "f", []byte("ambiguously stored content")); !errors.Is(err, failure) {
		t.Fatalf("ambiguously landed Put returned %v, want its response failure", err)
	}
	status := requireNoReservations(t, meta)
	if status.UnresolvedCount != 1 || status.GarbageCount != 0 {
		t.Fatalf("ambiguously landed Put left status %+v, want one unresolved object", status)
	}
	if removed, err := namespace.Sweep(t.Context(), 100); err != nil || removed != 0 {
		t.Fatalf("sweeping an unresolved Put removed %d objects (%v), want none", removed, err)
	}
	content, err := objects.Objects.Get(t.Context(), objects.key)
	if err != nil || string(content) != "ambiguously stored content" {
		t.Fatalf("the ambiguously landed object reads as %q (%v), want it preserved", content, err)
	}
}

func TestCommitThatLandedBeforeFailingKeepsItsReferencedObject(t *testing.T) {
	responseFailure := errors.New("the commit response was lost")
	independentFailure := errors.New("an independent response failure")
	for _, test := range []struct {
		name     string
		failure  error
		retained error
	}{
		{name: "unclassified response", failure: responseFailure, retained: responseFailure},
		{name: "EEXIST", failure: syscall.EEXIST},
		{name: "ENOENT", failure: syscall.ENOENT},
		{
			name:     "joined namespace fact and independent failure",
			failure:  errors.Join(syscall.ENOENT, independentFailure),
			retained: independentFailure,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			meta := reservationMeta(t)
			store := &landedCommitFailureStore{Store: meta, failure: test.failure}
			namespace := objectstore.New(memory.New(), store)
			t.Cleanup(func() { _ = namespace.Close() })

			err := namespace.Write(t.Context(), "f", []byte("committed content"))
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("ambiguously landed Commit returned %v, want EIO", err)
			}
			if test.retained != nil && !errors.Is(err, test.retained) {
				t.Fatalf("ambiguously landed Commit returned %v, want retained failure %v", err, test.retained)
			}
			for _, errno := range factsAboutNames {
				if errors.Is(err, errno) {
					t.Fatalf("ambiguously landed Commit exposed namespace fact %v: %v", errno, err)
				}
			}
			if got := storage.ErrnoNameOf(err); got != "EIO" {
				t.Fatalf("ambiguously landed Commit travels over the transport as %s, want EIO", got)
			}
			content, readErr := namespace.Read(t.Context(), "f")
			if readErr != nil || string(content) != "committed content" {
				t.Fatalf("ambiguously committed file reads as %q (%v), want its referenced content", content, readErr)
			}
			status := requireNoReservations(t, meta)
			if status.GarbageCount != 0 || status.GarbageBytes != 0 {
				t.Fatalf("ambiguously committed object was made garbage: %+v", status)
			}
		})
	}
}

func TestExistingObjectAtReservedKeyIsNeverDeleted(t *testing.T) {
	database := filepath.Join(t.TempDir(), "meta.db")
	meta, err := sqlite.Open(t.Context(), database, "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	objects := memory.New()
	gated := &gatedReserveStore{
		Store: meta, reserved: make(chan metastore.Key, 1), release: make(chan struct{}),
	}
	namespace := objectstore.New(objects, gated)

	written := make(chan error, 1)
	go func() { written <- namespace.Write(t.Context(), "f", []byte("replacement")) }()
	key := <-gated.reserved
	if _, err := objects.Put(t.Context(), string(key), []byte("pre-existing")); err != nil {
		t.Fatalf("pre-seeding the reserved key: %v", err)
	}
	close(gated.release)
	err = <-written
	if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EEXIST) {
		t.Fatalf("writing through a colliding object key returned %v, want EIO without EEXIST", err)
	}
	for _, errno := range factsAboutNames {
		if errors.Is(err, errno) {
			t.Fatalf("the object-key collision arrived as namespace fact %v: %v", errno, err)
		}
	}
	if got := storage.ErrnoNameOf(err); got != "EIO" {
		t.Fatalf("the object-key collision travels over the transport as %s, want EIO", got)
	}
	status, statusErr := meta.ObjectStatus(t.Context())
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	if status.UnresolvedCount != 1 || status.GarbageCount != 0 {
		t.Fatalf("the collision left status %+v, want one unresolved reservation", status)
	}
	if removed, err := namespace.Sweep(t.Context(), 100); err != nil || removed != 0 {
		t.Fatalf("sweeping the collision removed %d objects (%v), want none", removed, err)
	}
	if err := namespace.Close(); err != nil {
		t.Fatalf("closing the first namespace: %v", err)
	}

	reopenedMeta, err := sqlite.Open(t.Context(), database, "workspace", limited.MinLimit, sqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	reopened := objectstore.New(objects, reopenedMeta)
	t.Cleanup(func() { _ = reopened.Close() })
	if removed, err := reopened.Sweep(t.Context(), 100); err != nil || removed != 0 {
		t.Fatalf("startup-era sweep removed %d objects (%v), want none", removed, err)
	}
	content, err := objects.Get(t.Context(), string(key))
	if err != nil || string(content) != "pre-existing" {
		t.Fatalf("the pre-existing object reads as %q (%v), want it preserved", content, err)
	}
}

func TestAzurePutCollisionDoesNotExposeRawTransportDiagnostics(t *testing.T) {
	p := newParts(t, 0)
	gated := &gatedReserveStore{
		Store: borrowedStore{Store: p.meta}, reserved: make(chan metastore.Key, 1), release: make(chan struct{}),
	}
	namespace := objectstore.New(&failingObjects{Objects: p.objects}, gated)
	t.Cleanup(func() { _ = namespace.Close() })

	written := make(chan error, 1)
	go func() { written <- namespace.Write(t.Context(), "f", []byte("replacement")) }()
	key := <-gated.reserved
	if _, err := p.objects.Put(t.Context(), string(key), []byte("pre-existing")); err != nil {
		t.Fatalf("pre-seeding the Azure object: %v", err)
	}
	close(gated.release)
	err := <-written
	if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EEXIST) {
		t.Fatalf("Azure Put collision returned %v, want EIO without EEXIST", err)
	}
	if got := storage.ErrnoNameOf(err); got != "EIO" {
		t.Fatalf("Azure Put collision travels over the transport as %s, want EIO", got)
	}
	for _, raw := range []string{serviceURL(), "RESPONSE ", "<?xml", "<Error>"} {
		if strings.Contains(err.Error(), raw) {
			t.Fatalf("Azure Put collision exposed raw transport diagnostic %q: %v", raw, err)
		}
	}
	if !strings.Contains(err.Error(), "BlobAlreadyExists") && !strings.Contains(err.Error(), "ConditionNotMet") {
		t.Fatalf("Azure Put collision lost its safe service result: %v", err)
	}
	var response *azcore.ResponseError
	if !errors.As(err, &response) {
		t.Fatalf("Azure Put collision did not retain the typed service response: %v", err)
	}
	content, readErr := p.objects.Get(t.Context(), string(key))
	if readErr != nil || string(content) != "pre-existing" {
		t.Fatalf("the collided Azure object reads as %q (%v), want it preserved", content, readErr)
	}
}

func TestCloseCancelsAbandonBeforeWaitingForAdmittedWrite(t *testing.T) {
	meta := reservationMeta(t)
	commitFailure := errors.New("the metadata commit failed")
	store := &blockingAbandonStore{
		Store: meta, commitErr: commitFailure, started: make(chan struct{}), release: make(chan struct{}),
	}
	namespace := objectstore.New(memory.New(), store)
	written := make(chan error, 1)
	go func() { written <- namespace.Write(context.Background(), "f", []byte("content")) }()
	select {
	case <-store.started:
	case <-time.After(2 * time.Second):
		t.Fatal("Write never entered Abandon")
	}

	closed := make(chan error, 1)
	go func() { closed <- namespace.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("closing the namespace: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(store.release)
		<-written
		<-closed
		t.Fatal("Close did not cancel the blocked Abandon")
	}
	err := <-written
	if !errors.Is(err, commitFailure) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Write returned %v, want the commit failure and shutdown cancellation", err)
	}
}
