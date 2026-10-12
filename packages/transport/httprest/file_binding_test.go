package httprest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

func fileBindingContext(t *testing.T, subject, access string) context.Context {
	t.Helper()
	ctx, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: subject, AccessID: access})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestHTTPFileAccessBindingProtectsCapabilitiesAndTerminalHistory(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-file-binding", 1<<20, locking.DefaultOptions())
	owner := fileBindingContext(t, "owner", "first-access")
	if err := backend.Create(owner, "file"); err != nil {
		t.Fatal(err)
	}
	handler, err := NewHandler(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := handler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	enrollment, err := handler.files.enroll(owner, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	openID, err := storage.NewLockRequestID(enrollment.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := handler.fileCall(owner, fileRequest{
		Op: storage.OpFileOpen, Session: enrollment.Session, Action: openID,
		Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}},
	}, sha256.Sum256([]byte(openID)))
	if err != nil || opened.File == "" {
		t.Fatalf("open=%+v err=%v", opened, err)
	}
	if _, err := handler.fileCall(owner, fileRequest{Op: storage.OpFileAck, Session: enrollment.Session, File: opened.File}, [32]byte{}); err != nil {
		t.Fatal(err)
	}
	closeID, err := storage.NewLockRequestID(enrollment.Epoch)
	if err != nil {
		t.Fatal(err)
	}
	closeRequest := fileRequest{Op: storage.OpFileSessionClose, Session: enrollment.Session, Action: closeID, CloseGeneration: 1}
	requests := []fileRequest{
		{Op: storage.OpFileOpen, Session: enrollment.Session, Action: openID, Path: []byte("file"), Open: storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Write: true}}},
		{Op: storage.OpFileStatus, Session: enrollment.Session},
		{Op: storage.OpFileRenew, Session: enrollment.Session},
		{Op: storage.OpFileStatNode, Session: enrollment.Session, Node: 1},
		{Op: storage.OpFileStat, Session: enrollment.Session, File: opened.File},
		{Op: storage.OpFileWrite, Session: enrollment.Session, File: opened.File, Data: []byte("stolen")},
		{Op: storage.OpFileAck, Session: enrollment.Session, File: opened.File},
		{Op: storage.OpFileQueryAction, Session: enrollment.Session, FileAction: storage.FileActionID(openID)},
		closeRequest,
	}
	attackers := []context.Context{
		fileBindingContext(t, "owner", "second-access"),
		fileBindingContext(t, "other-owner", "first-access"),
		t.Context(),
	}
	for _, attacker := range attackers {
		for _, request := range requests {
			if _, err := handler.fileCall(attacker, request, [32]byte{}); !errors.Is(err, syscall.EACCES) {
				t.Fatalf("stolen %s returned %v", request.Op, err)
			}
		}
		if _, err := handler.fileContentEffects(attacker, fileRequest{Op: storage.OpFileMutate, Session: enrollment.Session, File: opened.File, Mutation: &fileMutationOptions{ContentEffects: []uint16{0}}}); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("stolen content-effect enrollment returned %v", err)
		}
		mutation := storage.FileMutation{Action: storage.FileActionID(openID), Kind: storage.MutateWriteAt, Data: []byte("stolen")}
		body, err := json.Marshal(fileRequest{Op: storage.OpFileMutate, Session: enrollment.Session, File: opened.File, Action: openID, ResultBytes: MaxFileRecoveryBytes, Path: []byte{}, Data: []byte{}, Mutation: fileMutationOf(mutation)})
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, Prefix+string(OpFileRecovery), bytes.NewReader(body)).WithContext(attacker)
		request.Header.Set("Content-Type", contentJSON)
		response := httptest.NewRecorder()
		handler.serveFile(response, request)
		var failure ErrorResponse
		if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil || response.Code != StatusStorageError || failure.Errno != "EACCES" {
			t.Fatalf("stolen recovery status=%d response=%s decode=%v", response.Code, response.Body.String(), err)
		}
		handler.files.mu.Lock()
		served := handler.files.sessions[enrollment.Session]
		handler.files.mu.Unlock()
		served.mu.Lock()
		lane := served.recoveryAdmission
		served.mu.Unlock()
		if lane != nil {
			t.Fatal("stolen recovery request created a session recovery lane")
		}
	}
	if _, err := handler.fileCall(owner, fileRequest{Op: storage.OpFileStat, Session: enrollment.Session, File: opened.File}, [32]byte{}); err != nil {
		t.Fatalf("rejected theft damaged original reference: %v", err)
	}
	digest := sha256.Sum256([]byte(closeID))
	closed, err := handler.fileCall(owner, closeRequest, digest)
	if err != nil || closed.CloseResult == nil || !closed.CloseResult.Released || !closed.CloseResult.Determined {
		t.Fatalf("owner close=%+v err=%v", closed.CloseResult, err)
	}
	query := fileRequest{Op: storage.OpFileQueryAction, Session: enrollment.Session, FileAction: storage.FileActionID(closeID)}
	for _, attacker := range attackers {
		for _, request := range []fileRequest{query, closeRequest, {Op: opFileSessionReleaseResult, Session: enrollment.Session}} {
			if _, err := handler.fileCall(attacker, request, digest); !errors.Is(err, syscall.EACCES) {
				t.Fatalf("stolen terminal %s returned %v", request.Op, err)
			}
		}
	}
	receipt, err := handler.fileCall(owner, query, [32]byte{})
	if err != nil || receipt.ActionReceipt == nil || receipt.ActionReceipt.Outcome != storage.FileActionCompleted {
		t.Fatalf("original terminal receipt=%+v err=%v", receipt.ActionReceipt, err)
	}
}

type blockedFileEnrollment struct {
	storage.FileStorage
	entered chan struct{}
	resume  chan struct{}
}

func (s blockedFileEnrollment) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	close(s.entered)
	select {
	case <-s.resume:
		return s.FileStorage.NewFileSession(ctx, options)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestHTTPFileRetirementRejectsEnrollmentInFlight(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-file-enrollment-retirement", 1<<20, locking.DefaultOptions())
	registry := newFileRegistry(backend, DefaultFileLimits())
	blocked := blockedFileEnrollment{FileStorage: backend, entered: make(chan struct{}), resume: make(chan struct{})}
	registry.backend = blocked
	var resumeOnce sync.Once
	resume := func() { resumeOnce.Do(func() { close(blocked.resume) }) }
	t.Cleanup(func() {
		resume()
		attempt := registry.stop()
		<-attempt.done
		if attempt.err != nil {
			t.Error(attempt.err)
		}
	})
	result := make(chan error, 1)
	go func() {
		_, err := registry.enroll(t.Context(), storage.DefaultFileSessionOptions())
		result <- err
	}()
	select {
	case <-blocked.entered:
	case err := <-result:
		t.Fatalf("enrollment ended before native admission: %v", err)
	}
	if _, err := registry.reserveRetirement(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("in-flight enrollment retirement=%v", err)
	}
	resume()
	if err := <-result; err != nil {
		t.Fatalf("busy reservation disrupted admitted enrollment: %v", err)
	}
}

func TestHTTPFileRetirementReservationPreservesLiveState(t *testing.T) {
	_, backend := memoryfixture.New(t, "http-file-retirement", 1<<20, locking.DefaultOptions())
	registry := newFileRegistry(backend, DefaultFileLimits())
	rollback, err := registry.reserveRetirement()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.enroll(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("enrollment crossed retirement reservation: %v", err)
	}
	rollback()
	rollback()
	enrolled, err := registry.enroll(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		attempt := registry.stop()
		<-attempt.done
		if attempt.err != nil {
			t.Error(attempt.err)
		}
	})
	if _, err := registry.reserveRetirement(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("live session retirement=%v", err)
	}
	registry.mu.Lock()
	closed, session := registry.closed, registry.sessions[enrolled.Session]
	registry.mu.Unlock()
	if closed {
		t.Fatal("busy reservation stopped registry")
	}
	if _, err := session.native.Status(t.Context()); err != nil {
		t.Fatalf("busy reservation disrupted live session: %v", err)
	}
	registry.closeRetiringSession(enrolled.Session, session, false)
	if _, err := registry.reserveRetirement(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("live volatile history retirement=%v", err)
	}
	registry.mu.Lock()
	terminal := registry.terminalCloses[enrolled.Session]
	terminal.mu.Lock()
	terminal.expires = time.Now().Add(-time.Second)
	terminal.mu.Unlock()
	registry.mu.Unlock()
	rollback, err = registry.reserveRetirement()
	if err != nil {
		t.Fatalf("expired volatile history blocked retirement: %v", err)
	}
	rollback()
}
