//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/storage/replicated"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type ioReplicaEventGate struct {
	handler     http.Handler
	armed       atomic.Bool
	entered     chan struct{}
	released    chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func (g *ioReplicaEventGate) release() { g.releaseOnce.Do(func() { close(g.released) }) }

func (g *ioReplicaEventGate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	op := httprest.Op(strings.TrimPrefix(r.URL.Path, httprest.Prefix))
	if op == httprest.OpSubscribe || op == httprest.OpResubscribe {
		g.handler.ServeHTTP(&ioReplicaEventWriter{ResponseWriter: w, ctx: r.Context(), gate: g}, r)
		return
	}
	g.handler.ServeHTTP(w, r)
}

type ioReplicaEventWriter struct {
	http.ResponseWriter
	ctx  context.Context
	gate *ioReplicaEventGate
}

func (w *ioReplicaEventWriter) Write(data []byte) (int, error) {
	if w.gate.armed.Load() {
		w.gate.enterOnce.Do(func() { close(w.gate.entered) })
		select {
		case <-w.gate.released:
		case <-w.ctx.Done():
			return 0, context.Cause(w.ctx)
		}
	}
	return w.ResponseWriter.Write(data)
}

func (w *ioReplicaEventWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type ioNativeCloseReply struct {
	CloseResult *struct {
		Released       bool `json:"released"`
		Determined     bool `json:"determined"`
		BarrierPending bool `json:"barrierPending"`
	} `json:"closeResult"`
	Barrier *httprest.MutationBarrier `json:"barrier"`
}

type ioCaptureNativeClose struct {
	next    http.RoundTripper
	mu      sync.Mutex
	replies []ioNativeCloseReply
}

func (c *ioCaptureNativeClose) RoundTrip(request *http.Request) (*http.Response, error) {
	var command struct {
		Op storage.Operation `json:"op"`
	}
	if request.Body != nil {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		if err := json.Unmarshal(body, &command); err != nil {
			return nil, err
		}
	}
	response, err := c.next.RoundTrip(request)
	if err != nil || command.Op != storage.OpFileClose {
		return response, err
	}
	body, err := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil {
		return nil, errors.Join(err, closeErr)
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	var reply ioNativeCloseReply
	if err := json.Unmarshal(body, &reply); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.replies = append(c.replies, reply)
	c.mu.Unlock()
	return response, nil
}

func newReplicaCloseProofSMBFixture(t *testing.T) (realSMBFixture, *sqlite.Replica, *ioReplicaEventGate, *ioCaptureNativeClose) {
	t.Helper()
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "authority.db"), Volume: "files", Allowance: 1 << 20,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Error(err)
		}
	})
	for name, data := range map[string]string{"unknown": "before", "sibling": "sibling body"} {
		if err := volume.Write(t.Context(), name, []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	options := httprest.DefaultHandlerOptions()
	options.Files = httprest.DefaultFileLimits()
	options.Files.Session = fixtureFileSessionOptions()
	handler, err := httprest.NewHandlerWithOptions(volume, meta, options)
	if err != nil {
		t.Fatal(err)
	}
	gate := &ioReplicaEventGate{handler: handler, entered: make(chan struct{}), released: make(chan struct{})}
	endpoint := httptest.NewServer(gate)
	t.Cleanup(func() {
		gate.release()
		endpoint.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	capture := &ioCaptureNativeClose{next: endpoint.Client().Transport}
	drop := &ioDropMutations{next: capture, drop: true}
	remote, err := httprest.Dial(endpoint.URL, &http.Client{Transport: drop})
	if err != nil {
		t.Fatal(err)
	}
	local, err := sqlite.OpenReplica(t.Context(), filepath.Join(t.TempDir(), "replica.db"))
	if err != nil {
		t.Fatal(err)
	}
	replicaOptions := replicated.DefaultOptions()
	replicaOptions.ConfirmationGrace = 250 * time.Millisecond
	backing, err := replicated.NewWithOptions(t.Context(), local, remote, replicaOptions)
	if err != nil {
		local.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		gate.release()
		if err := backing.Close(); err != nil {
			t.Error(err)
		}
	})
	raw, err := backing.NewFileSession(t.Context(), fixtureFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		gate.release()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := raw.Close(ctx); err != nil {
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
	server := &Server{config: Config{Limits: DefaultLimits(), Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })}, nameComparer: testNameCompare}
	export := &Export{server: server, share: Share{Volume: "files", Backend: backing, BackendVolume: identity.Backend.Volume, RootNodeID: identity.Backend.RootNodeID}}
	authority := &authoritySession{raw: raw, identity: identity, export: export, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining)}
	session := &session{id: 21}
	tree := &tree{kind: volumeTree, export: export, authority: authority, sessionID: session.id}
	return realSMBFixture{volume: volume, raw: raw, state: state, server: server, tree: tree, session: session, connection: &connection{server: server}}, local, gate, capture
}

func TestFileIOReplicaUnknownWriteRetainsPayloadUntilCloseBarrierSettles(t *testing.T) {
	fixture, local, gate, capture := newReplicaCloseProofSMBFixture(t)
	_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("unknown", 1, accessWriteData))
	if status != statusOK {
		t.Fatalf("unknown CREATE=%#x", status)
	}
	_, status, siblingID := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("sibling", 1, accessReadData))
	if status != statusOK {
		t.Fatalf("sibling CREATE=%#x", status)
	}
	handle := fixture.tree.findFileHandle(id)
	gate.armed.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	writeCtx, finalize := ioResponseContextForTest(ctx)
	_, status = fixture.connection.writeFile(writeCtx, fixture.session, fixture.tree, writeRequestForTest(id, 0, []byte("after!")))
	finalize()
	if status != statusIO {
		t.Fatalf("unknown WRITE=%#x", status)
	}
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("WRITE event did not reach the replica delivery gate")
	}
	if data, err := fixture.volume.Read(ctx, "unknown"); err != nil || string(data) != "after!" {
		t.Fatalf("native mutation=%q, %v", data, err)
	}
	before := fixture.server.Status()
	if before.UnknownWrites != 1 || before.RetainedWriteBytes < 6 || len(before.WriteFailures) != 1 || before.WriteFailures[0].PayloadReleased {
		t.Fatalf("unknown WRITE owner=%+v", before)
	}
	if handle.pendingWrite == nil {
		t.Fatal("unknown WRITE lost its handle owner")
	}
	handle.pendingWrite.mu.Lock()
	payload := bytes.Clone(handle.pendingWrite.command.Data)
	handle.pendingWrite.mu.Unlock()
	if string(payload) != "after!" {
		t.Fatalf("retained original payload=%q", payload)
	}
	closeErr := fixture.tree.closeFileHandle(ctx, handle)
	var settlement *storage.CloseSettlementError
	if !errors.As(closeErr, &settlement) || settlement.State != storage.CloseSettlementPending {
		t.Fatalf("withheld replica CLOSE=%v, settlement=%+v", closeErr, settlement)
	}
	capture.mu.Lock()
	replies := append([]ioNativeCloseReply(nil), capture.replies...)
	capture.mu.Unlock()
	if len(replies) != 1 || replies[0].CloseResult == nil || !replies[0].CloseResult.Released || replies[0].CloseResult.BarrierPending || replies[0].Barrier == nil {
		t.Fatalf("native close did not positively release with its barrier: %+v", replies)
	}
	barrier := replies[0].Barrier
	if int64(local.Position()) >= barrier.Position {
		t.Fatalf("replica position=%d already reached withheld close barrier=%d", local.Position(), barrier.Position)
	}
	if fixture.tree.findFileHandle(id) != handle || handleState(handle.state.Load()) != handleBarrierOnly || !handle.released || handle.pendingWrite == nil || fixture.tree.authority.isClosed() {
		t.Fatalf("local release retired outer owner before settlement: handle=%p state=%d released=%t pending=%p authorityClosed=%t", fixture.tree.findFileHandle(id), handle.state.Load(), handle.released, handle.pendingWrite, fixture.tree.authority.isClosed())
	}
	handle.pendingWrite.mu.Lock()
	payloadAfterNativeRelease := bytes.Clone(handle.pendingWrite.command.Data)
	retiredBeforeBarrier := handle.pendingWrite.retired
	handle.pendingWrite.mu.Unlock()
	if string(payloadAfterNativeRelease) != "after!" || retiredBeforeBarrier {
		t.Fatalf("native release dropped original WRITE payload: data=%q retired=%t", payloadAfterNativeRelease, retiredBeforeBarrier)
	}
	after := fixture.server.Status()
	if after.RetainedWriteBytes != before.RetainedWriteBytes || after.UnknownWrites != 1 || len(after.WriteFailures) != 1 || after.WriteFailures[0].PayloadReleased || after.WriteFailures[0].ChainSettled || after.WriteFailures[0].Terminal {
		t.Fatalf("native release prematurely retired WRITE charge/fact: before=%+v after=%+v", before, after)
	}
	if err := fixture.server.AcknowledgeWriteFailures([]WriteOwnerID{after.WriteFailures[0].Owner}); err == nil {
		t.Fatal("unsettled WRITE failure could be acknowledged")
	}
	attempt := *handle.attempt
	gate.release()
	if err := fixture.tree.closeFileHandle(ctx, handle); err == nil {
		t.Fatal("settled CLOSE hid original unknown WRITE")
	}
	if int64(local.Position()) < barrier.Position || fixture.tree.findFileHandle(id) != nil {
		t.Fatalf("confirmed close did not retire FileId: position=%d barrier=%d handle=%p", local.Position(), barrier.Position, fixture.tree.findFileHandle(id))
	}
	handle.pendingWrite.mu.Lock()
	retired, remainingPayload := handle.pendingWrite.retired, len(handle.pendingWrite.command.Data)
	handle.pendingWrite.mu.Unlock()
	if !retired || remainingPayload != 0 {
		t.Fatalf("confirmed close did not retire original payload owner: retired=%t bytes=%d", retired, remainingPayload)
	}
	capture.mu.Lock()
	closeCalls := len(capture.replies)
	capture.mu.Unlock()
	if closeCalls != 1 || handle.attempt == nil || *handle.attempt != attempt {
		t.Fatalf("barrier confirmation replaced/reissued native close: calls=%d before=%+v after=%+v", closeCalls, attempt, handle.attempt)
	}
	terminal := fixture.server.Status()
	if terminal.RetainedWriteBytes != 0 || terminal.UnknownWrites != 1 || len(terminal.WriteFailures) != 1 {
		t.Fatalf("settled owner charges=%+v", terminal)
	}
	fact := terminal.WriteFailures[0]
	if fact.Execution != WriteExecutionUnknown || !fact.PayloadReleased || !fact.ReferenceReleased || !fact.ChainSettled || !fact.Terminal || fact.Action != before.WriteFailures[0].Action || fact.Digest != before.WriteFailures[0].Digest {
		t.Fatalf("terminal fact lost original unknown result or full proof: %+v", fact)
	}
	if err := fixture.server.AcknowledgeWriteFailures([]WriteOwnerID{fact.Owner}); err != nil {
		t.Fatalf("settled fact acknowledgement=%v", err)
	}
	closeFixtureHandle(t, fixture, siblingID)
}
