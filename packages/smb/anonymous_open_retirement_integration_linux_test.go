//go:build linux

package smb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type anonymousNativeSession interface {
	createSessionContract
	storage.InlineCloseSettlement
	storage.DirectoryReader
}

type anonymousNativeProbe struct {
	anonymousNativeSession
	loseOpen    bool
	unavailable atomic.Bool
	opens       atomic.Int32
	queries     atomic.Int32
	closes      atomic.Int32
	action      storage.FileActionID
}

func (p *anonymousNativeProbe) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	if p.unavailable.Load() {
		return storage.OpenResult{}, syscall.EIO
	}
	p.opens.Add(1)
	p.action = options.Action
	opened, err := p.anonymousNativeSession.OpenAt(ctx, selection, options)
	if err == nil && p.loseOpen {
		p.unavailable.Store(true)
		return storage.OpenResult{}, syscall.EIO
	}
	return opened, err
}

func (p *anonymousNativeProbe) QueryFileAction(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	p.queries.Add(1)
	if p.unavailable.Load() {
		return storage.FileActionReceipt{}, syscall.EIO
	}
	return p.anonymousNativeSession.QueryFileAction(ctx, action)
}

func (p *anonymousNativeProbe) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	p.closes.Add(1)
	return p.anonymousNativeSession.CloseWithResult(ctx)
}

type anonymousNativeBackend struct {
	*objectstore.Storage
	mu       sync.Mutex
	enrolled []*anonymousNativeProbe
}

func (b *anonymousNativeBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	raw, err := b.Storage.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	probe := &anonymousNativeProbe{anonymousNativeSession: raw.(anonymousNativeSession)}
	b.mu.Lock()
	b.enrolled = append(b.enrolled, probe)
	b.mu.Unlock()
	return probe, nil
}

type anonymousHTTPOutage struct {
	base          http.RoundTripper
	outage        atomic.Bool
	delivered     atomic.Int32
	queries       atomic.Int32
	sessionCloses atomic.Int32
}

func (r *anonymousHTTPOutage) RoundTrip(request *http.Request) (*http.Response, error) {
	var command struct {
		Op storage.Operation `json:"op"`
	}
	if request.GetBody != nil {
		body, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		err = json.NewDecoder(body).Decode(&command)
		_ = body.Close()
		if err != nil {
			return nil, err
		}
	}
	if command.Op == storage.OpFileQueryAction {
		r.queries.Add(1)
	}
	if command.Op == storage.OpFileSessionClose {
		r.sessionCloses.Add(1)
	}
	if r.outage.Load() && command.Op != "" {
		return nil, syscall.EIO
	}
	response, err := r.base.RoundTrip(request)
	if err == nil && command.Op == storage.OpFileOpenAt && r.delivered.Add(1) == 1 {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		r.outage.Store(true)
		return nil, errors.New("atomic open response lost")
	}
	return response, err
}

func anonymousFixture(t *testing.T, transport string, lease time.Duration) (realSMBFixture, *anonymousNativeProbe, *anonymousHTTPOutage) {
	t.Helper()
	fixture := newRealSMBFixture(t)
	options := storage.DefaultFileSessionOptions()
	options.Lease = lease
	var raw storage.FileSession
	var native *anonymousNativeProbe
	var outage *anonymousHTTPOutage
	var err error
	if transport == "direct" {
		enrolled, openErr := fixture.volume.NewFileSession(t.Context(), options)
		if openErr != nil {
			t.Fatal(openErr)
		}
		native = &anonymousNativeProbe{anonymousNativeSession: enrolled.(anonymousNativeSession), loseOpen: true}
		raw = native
	} else {
		backend := &anonymousNativeBackend{Storage: fixture.volume}
		handlerOptions := httprest.DefaultHandlerOptions()
		handlerOptions.Files = httprest.DefaultFileLimits()
		handlerOptions.Files.Session = options
		handlerOptions.Files.PendingAck = min(handlerOptions.Files.PendingAck, lease)
		handler, handlerErr := httprest.NewHandlerWithOptions(backend, nil, handlerOptions)
		if handlerErr != nil {
			t.Fatal(handlerErr)
		}
		server := httptest.NewServer(handler)
		t.Cleanup(func() {
			server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := handler.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		clientTransport := server.Client().Transport
		outage = &anonymousHTTPOutage{base: clientTransport}
		client, clientErr := httprest.Dial(server.URL, &http.Client{Transport: outage})
		if clientErr != nil {
			t.Fatal(clientErr)
		}
		raw, err = client.NewFileSession(t.Context(), options)
		if err != nil {
			t.Fatal(err)
		}
		backend.mu.Lock()
		native = backend.enrolled[0]
		backend.mu.Unlock()
		fixture.tree.export.share.Backend = client
	}
	t.Cleanup(func() {
		native.unavailable.Store(false)
		if outage != nil {
			outage.outage.Store(false)
		}
		if err := raw.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	state, err := raw.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := raw.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fixture.raw, fixture.state = raw, state
	fixture.server.config.Limits.MaxHandles = 1
	fixture.server.config.Limits.CleanupTimeout = 100 * time.Millisecond
	fixture.tree.id, fixture.tree.sessionID, fixture.tree.done = 1, fixture.session.id, make(chan struct{})
	ready, done := make(chan struct{}), make(chan struct{})
	close(ready)
	close(done)
	fixture.tree.authority = &authoritySession{raw: raw, identity: identity, export: fixture.tree.export, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining), refs: 1, ready: ready, done: done, connection: fixture.connection, smbSession: fixture.session}
	fixture.tree.export.refs, fixture.tree.export.trees = 1, 1
	fixture.session.trees = map[uint32]*tree{1: fixture.tree}
	fixture.session.authorities = map[*Export]*authoritySession{fixture.tree.export: fixture.tree.authority}
	fixture.connection.sessions = map[uint64]*session{fixture.session.id: fixture.session}
	fixture.connection.pending = make(map[uint64]*pendingRequest)
	return fixture, native, outage
}

func TestAnonymousOpenRetirementAfterExpiredLostResultDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			fixture, native, outage := anonymousFixture(t, transport, 2*time.Second)
			request := createRequestForTest("anonymous", 2, accessReadData)
			binary.LittleEndian.PutUint32(request.Body[32:], 0)
			_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, request)
			if status != statusIO || id != (wire.FileID{}) || native.opens.Load() != 1 {
				t.Fatalf("lost original result status=%#x id=%x nativeopens=%d", status, id, native.opens.Load())
			}
			owners := fixture.server.HandleOwners()
			if len(owners) != 1 || owners[0].NodeID != 0 || owners[0].OpenAction != native.action {
				t.Fatalf("unknown open not retained: %+v", owners)
			}
			if _, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("capacity", 2, accessReadData)); status != statusResources {
				t.Fatalf("anonymous owner released capacity early: %#x", status)
			}
			deadline := fixture.tree.authority.deadline
			if wait := time.Until(deadline); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-t.Context().Done():
					timer.Stop()
					t.Fatal(t.Context().Err())
				}
			}
			native.unavailable.Store(false)
			if outage != nil {
				outage.outage.Store(false)
			}
			queryDeadline := time.NewTimer(2 * time.Second)
			defer queryDeadline.Stop()
			queryTick := time.NewTicker(time.Millisecond)
			defer queryTick.Stop()
			var queryErr error
			for {
				_, queryErr = native.anonymousNativeSession.QueryFileAction(t.Context(), native.action)
				if errors.Is(queryErr, syscall.ESTALE) {
					break
				}
				if queryErr != nil {
					t.Fatalf("expired query returned unrelated error: %v", queryErr)
				}
				select {
				case <-queryTick.C:
				case <-queryDeadline.C:
					t.Fatal("authority did not retire after its confirmed lease")
				case <-t.Context().Done():
					t.Fatal(t.Context().Err())
				}
			}
			owners = fixture.server.HandleOwners()
			handle := fixture.tree.findFileHandle(owners[0].FileID)
			noReference, recoveryErr := handle.pendingOpen(t.Context())
			if noReference || recoveryErr == nil {
				t.Fatalf("expired original recovery must remain unknown: noReference=%v err=%v", noReference, recoveryErr)
			}
			closeErr := fixture.connection.closeTreeContext(t.Context(), fixture.tree)
			for !fixture.tree.closed {
				select {
				case <-queryTick.C:
				case <-queryDeadline.C:
					t.Fatalf("parent retirement did not finish: %v", closeErr)
				case <-t.Context().Done():
					t.Fatal(t.Context().Err())
				}
				closeErr = fixture.connection.closeTreeContext(t.Context(), fixture.tree)
			}
			if closeErr == nil {
				t.Fatal("parent release changed original unknown open into success")
			}
			if !fixture.tree.closed || !fixture.tree.authority.isClosed() || len(fixture.server.HandleOwners()) != 0 || fixture.tree.export.refs != 0 || fixture.tree.export.trees != 0 || native.closes.Load() < 1 {
				t.Fatalf("parent release retained ownership: treeclosed=%v authorityclosed=%v owners=%+v refs=%d trees=%d parentcalls=%d err=%v", fixture.tree.closed, fixture.tree.authority.isClosed(), fixture.server.HandleOwners(), fixture.tree.export.refs, fixture.tree.export.trees, native.closes.Load(), closeErr)
			}
			if native.opens.Load() != 1 {
				t.Fatalf("retirement replayed new open: %d", native.opens.Load())
			}
			if outage != nil && (outage.delivered.Load() < 1 || outage.sessionCloses.Load() < 1) {
				t.Fatalf("HTTP retirement did not preserve original operation: delivered=%d querycalls=%d parentcalls=%d", outage.delivered.Load(), outage.queries.Load(), outage.sessionCloses.Load())
			}
			if err := fixture.volume.Write(t.Context(), "anonymous", []byte("released")); err != nil {
				t.Fatalf("parent certificate left claim: %v", err)
			}
			if _, err := fixture.volume.Stat(t.Context(), "capacity"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("capacity refused CREATE changed namespace: %v", err)
			}
		})
	}
}

func TestAnonymousOpenRetirementPreservesLiveSiblingDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			fixture, native, outage := anonymousFixture(t, transport, 5*time.Minute)
			fixture.server.config.Limits.MaxHandles = 2
			first := fixture.tree
			second := &tree{kind: volumeTree, id: 2, sessionID: fixture.session.id, export: first.export, authority: first.authority, done: make(chan struct{})}
			fixture.session.trees[2] = second
			first.export.refs, first.export.trees = 2, 2
			first.authority.refs = 2
			native.loseOpen = false
			// The HTTP outage is armed only for the anonymous result, after the sibling
			// has a reference whose bytes and sharing protection remain observable.
			if outage != nil {
				outage.delivered.Store(1)
			}
			request := createRequestForTest("sibling", 2, accessReadData)
			binary.LittleEndian.PutUint32(request.Body[32:], 5)
			_, status, siblingID := fixture.connection.createFile(t.Context(), fixture.session, second, request)
			if status != statusOK {
				t.Fatalf("sibling CREATE status=%#x", status)
			}
			native.loseOpen = true
			if outage != nil {
				outage.delivered.Store(0)
			}
			if _, status, _ := fixture.connection.createFile(t.Context(), fixture.session, first, createRequestForTest("anonymous", 2, accessReadData)); status != statusIO {
				t.Fatalf("anonymous CREATE status=%#x", status)
			}
			closeErr := fixture.connection.closeTreeContext(t.Context(), first)
			if closeErr == nil || first.closed || first.authority.isClosed() || native.closes.Load() != 0 || len(fixture.server.HandleOwners()) != 2 {
				t.Fatalf("live sibling parent prematurely released: first=%v authority=%v parentcloses=%d owners=%+v err=%v", first.closed, first.authority.isClosed(), native.closes.Load(), fixture.server.HandleOwners(), closeErr)
			}
			if outage != nil {
				outage.outage.Store(false)
			}
			sibling := second.findFileHandle(siblingID)
			if _, err := sibling.file.ReadAt(t.Context(), 0, 1); err != nil {
				t.Fatalf("first tree retirement invalidated sibling: %v", err)
			}
			if err := fixture.volume.Write(t.Context(), "sibling", []byte("blocked")); !errors.Is(err, storage.ErrUseConflict) {
				t.Fatalf("first tree retirement lost sibling claim: %v", err)
			}
			// Restore transport data access while preserving the unresolved original
			// action. This forces final retirement to use the parent certificate.
			native.unavailable.Store(true)
			finalErr := fixture.connection.closeOwnedExport(t.Context(), first.export)
			if finalErr == nil {
				t.Fatal("final retirement concealed unknown original result")
			}
			if !first.closed || !second.closed || !first.authority.isClosed() || len(fixture.server.HandleOwners()) != 0 || first.export.refs != 0 || first.export.trees != 0 || native.closes.Load() != 1 {
				t.Fatalf("all-tree parent retirement retained responsibility: first=%v second=%v authority=%v owners=%+v refs=%d trees=%d closes=%d err=%v", first.closed, second.closed, first.authority.isClosed(), fixture.server.HandleOwners(), first.export.refs, first.export.trees, native.closes.Load(), finalErr)
			}
			if err := fixture.volume.Write(t.Context(), "sibling", []byte("released")); err != nil {
				t.Fatalf("final parent retained sibling claim: %v", err)
			}
		})
	}
}
