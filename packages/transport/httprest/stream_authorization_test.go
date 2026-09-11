package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

type streamAuthorizationIdentityKey struct{}

const streamAuthorizationVolume = "configured-volume"

func TestStreamAuthorizationInitialCallbacksUseResponseAdmission(t *testing.T) {
	for _, op := range []Op{OpSubscribe, OpResubscribe, OpSnapshot} {
		for _, trigger := range []string{"policy-denied", "request-cancel", "handler-stop"} {
			t.Run(string(op)+"/"+trigger, func(t *testing.T) {
				meta, backing := memoryfixture.New(t, "stored-volume", 1<<20, locking.DefaultOptions())
				log := &streamAuthorizationLog{Log: meta}
				entered := make(chan struct{})
				denied := make(chan struct{})
				var calls atomic.Int32
				options := DefaultHandlerOptions()
				options.Volume = streamAuthorizationVolume
				options.MaxConcurrentResponses = 1
				options.MaxWaitingResponses = 1
				options.Authorizer = authz.AuthorizerFunc(func(ctx context.Context, access authz.AccessRequest) error {
					streamAuthorizationCheckAccess(t, ctx, access, op)
					if calls.Add(1) == 1 {
						close(entered)
					}
					select {
					case <-denied:
						return authz.ErrDenied
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				h, err := NewHandlerWithOptions(backing, log, options)
				if err != nil {
					t.Fatal(err)
				}
				runningContext, cancelRunning := context.WithCancel(t.Context())
				defer cancelRunning()
				running := newStreamAuthorizationWriter()
				running.blockJSON = true
				runningDone := streamAuthorizationServe(t, h, running, streamAuthorizationRequest(t, runningContext, op))
				streamAuthorizationWait(t, entered, "initial authorization")
				if got := streamAuthorizationAdmission(h.responses); got != [3]int64{1, h.maxBodyBytes, 0} {
					t.Fatalf("running authorization response admission=%v", got)
				}

				waitingContext, cancelWaiting := context.WithCancel(t.Context())
				defer cancelWaiting()
				waitingDone := streamAuthorizationServe(t, h, newStreamAuthorizationWriter(), streamAuthorizationRequest(t, waitingContext, op))
				waitForAdmissionWaiters(t, h.responses, 1)
				overflow := newStreamAuthorizationWriter()
				overflowDone := streamAuthorizationServe(t, h, overflow, streamAuthorizationRequest(t, t.Context(), op))
				streamAuthorizationWait(t, overflowDone, "overflow refusal")
				var response ErrorResponse
				if err := json.Unmarshal([]byte(overflow.String()), &response); err != nil {
					t.Fatal(err)
				}
				if response.Errno != "EAGAIN" || calls.Load() != 1 || log.calls.Load() != 0 {
					t.Fatalf("overflow errno=%s policy calls=%d log calls=%d", response.Errno, calls.Load(), log.calls.Load())
				}
				if got := streamAuthorizationAdmission(h.responses); got != [3]int64{1, h.maxBodyBytes, 1} {
					t.Fatalf("overflow changed occupied response admission: %v", got)
				}

				if trigger != "handler-stop" {
					cancelWaiting()
					streamAuthorizationWait(t, waitingDone, "canceled admission waiter")
					if trigger == "policy-denied" {
						close(denied)
					} else {
						cancelRunning()
					}
				} else {
					stopped := make(chan struct{})
					go func() { h.Stop(); close(stopped) }()
					streamAuthorizationWait(t, stopped, "nonblocking handler Stop")
					streamAuthorizationWait(t, waitingDone, "stopped admission waiter")
				}
				streamAuthorizationWait(t, running.blocked, "authorization error response")
				if got := streamAuthorizationAdmission(h.responses); got != [3]int64{1, h.maxBodyBytes, 0} {
					t.Errorf("authorization error response lost its reservation: %v", got)
				}
				running.abort()
				streamAuthorizationWait(t, runningDone, "running authorization cleanup")
				if calls.Load() != 1 || log.calls.Load() != 0 {
					t.Errorf("finished requests reached policy/backend: policy=%d log=%d", calls.Load(), log.calls.Load())
				}
				streamAuthorizationAssertReleased(t, h)
			})
		}
	}
}

func TestStreamAuthorizationAdmissionEndsBeforeStream(t *testing.T) {
	for _, op := range []Op{OpSubscribe, OpSnapshot} {
		for _, enabled := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/authorizer=%v", op, enabled), func(t *testing.T) {
				meta, backing := memoryfixture.New(t, "stored-volume", 1<<20, locking.DefaultOptions())
				var h *Handler
				checkControlledAdmission := func() {
					want := [3]int64{}
					if !enabled {
						want = [3]int64{1, h.maxBodyBytes, 0}
					}
					if got := streamAuthorizationAdmission(h.responses); got != want {
						t.Errorf("response admission during controlled log access=%v, want %v", got, want)
					}
				}
				var snap *streamAuthorizationSnap
				log := &streamAuthorizationLog{Log: meta, incarnation: checkControlledAdmission, snapshot: func(original metastore.Snap) metastore.Snap {
					checkControlledAdmission()
					snap = &streamAuthorizationSnap{Snap: original, block: true, entered: make(chan struct{}), exited: make(chan struct{})}
					return snap
				}}
				options := DefaultHandlerOptions()
				options.MaxConcurrentResponses = 1
				options.Replication.Keepalive = time.Hour
				var calls atomic.Int32
				if enabled {
					options.Volume = streamAuthorizationVolume
					options.Authorizer = authz.AuthorizerFunc(func(ctx context.Context, access authz.AccessRequest) error {
						streamAuthorizationCheckAccess(t, ctx, access, op)
						want := [3]int64{}
						if calls.Add(1) == 1 {
							want = [3]int64{1, h.maxBodyBytes, 0}
						}
						if got := streamAuthorizationAdmission(h.responses); got != want {
							t.Errorf("response admission during authorization=%v, want %v", got, want)
						}
						return nil
					})
				}
				var err error
				h, err = NewHandlerWithOptions(backing, log, options)
				if err != nil {
					t.Fatal(err)
				}
				if !enabled {
					release, err := h.responses.acquire(t.Context(), h.maxBodyBytes)
					if err != nil {
						t.Fatal(err)
					}
					defer release()
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				w := newStreamAuthorizationWriter()
				done := streamAuthorizationServe(t, h, w, streamAuthorizationRequest(t, ctx, op))
				streamAuthorizationWait(t, w.flushed, "established stream")
				select {
				case <-done:
					t.Fatalf("stream ended during initial admission: %s", w.String())
				default:
				}
				want := [3]int64{}
				if !enabled {
					want = [3]int64{1, h.maxBodyBytes, 0}
				}
				if got := streamAuthorizationAdmission(h.responses); got != want {
					t.Errorf("established stream retained response admission: got=%v want=%v", got, want)
				}
				if enabled && calls.Load() != 2 {
					t.Errorf("establishing stream made %d authorization calls, want entry plus first frame", calls.Load())
				}
				cancel()
				streamAuthorizationWait(t, done, "established stream cancellation")
				if snap != nil && (!snap.closed.Load() || snap.closedDuringNext.Load()) {
					t.Error("established snapshot did not close after its producer returned")
				}
				if enabled {
					streamAuthorizationAssertReleased(t, h)
				}
			})
		}
	}
}

func TestStreamAuthorizationPrecedesControlledAdmission(t *testing.T) {
	for _, op := range []Op{OpSubscribe, OpResubscribe, OpSnapshot} {
		for _, resource := range []string{"unavailable-log", "available-log", "full-admission"} {
			t.Run(string(op)+"/"+resource, func(t *testing.T) {
				meta, backing := memoryfixture.New(t, "stored-volume", 1<<20, locking.DefaultOptions())
				observed := &streamAuthorizationLog{Log: meta}
				var log metastore.Log = observed
				if resource == "unavailable-log" {
					log = nil
				}
				options := DefaultHandlerOptions()
				options.Volume = streamAuthorizationVolume
				options.Replication.MaxSubscriptions = 1
				options.Replication.Snapshots = 1
				var calls atomic.Int32
				options.Authorizer = authz.AuthorizerFunc(func(ctx context.Context, access authz.AccessRequest) error {
					streamAuthorizationCheckAccess(t, ctx, access, op)
					calls.Add(1)
					return fmt.Errorf("private policy service: %w", authz.ErrDenied)
				})
				h, err := NewHandlerWithOptions(backing, log, options)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(h.Stop)
				if resource == "full-admission" {
					if op == OpSnapshot {
						release, err := h.snapshots.acquireOperation(t.Context())
						if err != nil {
							t.Fatal(err)
						}
						defer release()
					} else {
						_, release, err := h.publisher.attach()
						if err != nil {
							t.Fatal(err)
						}
						defer release()
					}
				}
				beforeSnapshots, beforeFrames := streamAuthorizationAdmission(h.snapshots), streamAuthorizationAdmission(h.snapshotFrames)
				beforeSubscribers := streamAuthorizationSubscribers(h)
				w := httptest.NewRecorder()
				h.ServeHTTP(w, streamAuthorizationRequest(t, t.Context(), op))
				if w.Code != http.StatusUnprocessableEntity || w.Header().Get(HeaderProtocol) != Version {
					t.Fatalf("denial response: status=%d protocol=%q body=%s", w.Code, w.Header().Get(HeaderProtocol), w.Body.String())
				}
				var response map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(response, map[string]any{"errno": "EACCES", "message": "access denied"}) {
					t.Fatalf("denial response = %v", response)
				}
				if calls.Load() != 1 || observed.calls.Load() != 0 {
					t.Fatalf("authorization calls=%d log calls=%d", calls.Load(), observed.calls.Load())
				}
				if got := streamAuthorizationAdmission(h.snapshots); got != beforeSnapshots {
					t.Fatalf("denied request changed snapshot admission: before=%v after=%v", beforeSnapshots, got)
				}
				if got := streamAuthorizationAdmission(h.snapshotFrames); got != beforeFrames {
					t.Fatalf("denied request changed frame admission: before=%v after=%v", beforeFrames, got)
				}
				if got := streamAuthorizationSubscribers(h); got != beforeSubscribers {
					t.Fatalf("denied request changed subscribers: before=%d after=%d", beforeSubscribers, got)
				}
			})
		}
	}
}

func streamAuthorizationCheckAccess(t *testing.T, ctx context.Context, access authz.AccessRequest, op Op) {
	t.Helper()
	want := authz.AccessRequest{Volume: streamAuthorizationVolume, Operation: streamAuthorizationOperation(op)}
	if access != want || ctx.Value(streamAuthorizationIdentityKey{}) != "original-identity-and-trace" {
		t.Errorf("authorization lost operation or context: access=%+v identity=%v", access, ctx.Value(streamAuthorizationIdentityKey{}))
	}
}

func streamAuthorizationOperation(op Op) authz.Operation {
	switch op {
	case OpSubscribe:
		return authz.ReplicationSubscribe
	case OpResubscribe:
		return authz.ReplicationResubscribe
	case OpSnapshot:
		return authz.ReplicationSnapshot
	default:
		panic("unexpected stream operation")
	}
}

func streamAuthorizationRequest(t *testing.T, ctx context.Context, op Op) *http.Request {
	t.Helper()
	base, err := url.Parse("http://server.invalid")
	if err != nil {
		t.Fatal(err)
	}
	req := Request{Op: op}
	if op == OpResubscribe {
		req.Incarnation = "previous-incarnation"
	}
	u, err := req.URL(base)
	if err != nil {
		t.Fatal(err)
	}
	ctx = context.WithValue(ctx, streamAuthorizationIdentityKey{}, "original-identity-and-trace")
	return httptest.NewRequestWithContext(ctx, req.Method(), u.String(), nil)
}

func streamAuthorizationAdmission(a *bodyAdmission) [3]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return [3]int64{int64(a.operations), a.bytes, int64(a.waiters)}
}

func streamAuthorizationSubscribers(h *Handler) int {
	if h.publisher == nil {
		return 0
	}
	h.publisher.mu.Lock()
	defer h.publisher.mu.Unlock()
	return len(h.publisher.wakes)
}

type streamAuthorizationLog struct {
	metastore.Log
	calls       atomic.Int32
	incarnation func()
	since       func(int)
	snapshot    func(metastore.Snap) metastore.Snap
}

func (l *streamAuthorizationLog) Incarnation(ctx context.Context, maxBytes int64) (metastore.Incarnation, error) {
	l.calls.Add(1)
	value, err := l.Log.Incarnation(ctx, maxBytes)
	if l.incarnation != nil {
		l.incarnation()
	}
	return value, err
}

func (l *streamAuthorizationLog) Since(ctx context.Context, after metastore.Position, limit int, result *metastore.ChangeResult) (metastore.Retention, error) {
	l.calls.Add(1)
	if l.since != nil {
		l.since(limit)
	}
	return l.Log.Since(ctx, after, limit, result)
}

func (l *streamAuthorizationLog) Snapshot(ctx context.Context) (metastore.Snap, metastore.Position, error) {
	l.calls.Add(1)
	snap, at, err := l.Log.Snapshot(ctx)
	if err == nil && l.snapshot != nil {
		snap = l.snapshot(snap)
	}
	return snap, at, err
}

func (l *streamAuthorizationLog) Barrier(ctx context.Context, maxBytes int64) (metastore.LogBarrier, error) {
	l.calls.Add(1)
	return l.Log.Barrier(ctx, maxBytes)
}

func (l *streamAuthorizationLog) CommittedPosition(ctx context.Context) (metastore.Position, error) {
	l.calls.Add(1)
	return l.Log.CommittedPosition(ctx)
}

func TestStreamAuthorizationRechecksEveryOutputBoundary(t *testing.T) {
	for _, boundary := range []string{"start", "rebuild", "change", "resubscribe-change", "open", "rows", "done", "subscription-keepalive", "snapshot-keepalive"} {
		for _, failure := range []string{"denied", "policy-failure"} {
			t.Run(boundary+"/"+failure, func(t *testing.T) {
				meta, backing := memoryfixture.New(t, "stored-volume", 1<<20, locking.DefaultOptions())
				log := &streamAuthorizationLog{Log: meta}
				var revoked atomic.Bool
				var calls atomic.Int32
				op := OpSubscribe
				if boundary == "rebuild" || boundary == "resubscribe-change" {
					op = OpResubscribe
				}
				if boundary == "open" || boundary == "rows" || boundary == "done" || boundary == "snapshot-keepalive" {
					op = OpSnapshot
				}
				options := DefaultHandlerOptions()
				options.Volume = streamAuthorizationVolume
				options.Replication.Keepalive = 10 * time.Millisecond
				if !strings.HasSuffix(boundary, "keepalive") {
					options.Replication.Keepalive = time.Hour
				}
				var snap *streamAuthorizationSnap
				options.Authorizer = authz.AuthorizerFunc(func(ctx context.Context, access authz.AccessRequest) error {
					streamAuthorizationCheckAccess(t, ctx, access, op)
					calls.Add(1)
					if !revoked.Load() {
						return nil
					}
					if boundary == "snapshot-keepalive" {
						streamAuthorizationWait(t, snap.entered, "snapshot producer entry")
					}
					if failure == "denied" {
						return errors.Join(authz.ErrDenied, errors.New("private policy detail"), syscall.EIO)
					}
					return fmt.Errorf("private policy timeout: %w", context.DeadlineExceeded)
				})
				if boundary == "start" || boundary == "rebuild" {
					log.incarnation = func() { revoked.Store(true) }
				}
				if boundary == "change" || boundary == "resubscribe-change" {
					log.since = func(limit int) {
						if limit > 0 && !revoked.Swap(true) {
							if err := backing.Mkdir(t.Context(), "undelivered-change"); err != nil {
								t.Error(err)
							}
						}
					}
				}
				log.snapshot = func(original metastore.Snap) metastore.Snap {
					snap = &streamAuthorizationSnap{Snap: original, entered: make(chan struct{}), exited: make(chan struct{})}
					if boundary == "open" {
						revoked.Store(true)
					}
					if boundary == "done" {
						snap.afterClose = func() { revoked.Store(true) }
					}
					snap.block = boundary == "snapshot-keepalive"
					return snap
				}
				w := newStreamAuthorizationWriter()
				w.afterFlush = func(frame string) {
					if (boundary == "rows" || boundary == "snapshot-keepalive") && strings.HasPrefix(frame, "event: open\n") ||
						boundary == "subscription-keepalive" && strings.HasPrefix(frame, "event: start\n") {
						revoked.Store(true)
					}
				}
				h, err := NewHandlerWithOptions(backing, log, options)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(h.Stop)
				r := streamAuthorizationRequest(t, t.Context(), op)
				if boundary == "resubscribe-change" {
					incarnation, err := meta.Incarnation(t.Context(), h.maxIncarnationBytes)
					if err != nil {
						t.Fatal(err)
					}
					base, _ := url.Parse("http://server.invalid")
					r.URL, err = (Request{Op: op, Incarnation: incarnation}).URL(base)
					if err != nil {
						t.Fatal(err)
					}
				}
				done := streamAuthorizationServe(t, h, w, r)
				streamAuthorizationWait(t, done, "authorization fault")
				wantEvents := []string{eventFault}
				switch boundary {
				case "change", "resubscribe-change", "subscription-keepalive":
					wantEvents = []string{eventStart, eventFault}
				case "rows", "snapshot-keepalive":
					wantEvents = []string{eventOpen, eventFault}
				case "done":
					wantEvents = []string{eventOpen, eventRows, eventFault}
				}
				streamAuthorizationAssertFrames(t, w.String(), wantEvents, failure)
				if calls.Load() != int32(len(wantEvents)+1) {
					t.Errorf("authorization calls=%d, want entry plus each attempted output (%d)", calls.Load(), len(wantEvents)+1)
				}
				if snap != nil {
					if !snap.closed.Load() || snap.closedDuringNext.Load() {
						t.Errorf("snapshot closure: closed=%v closed-during-next=%v", snap.closed.Load(), snap.closedDuringNext.Load())
					}
					if boundary == "rows" && snap.nextCalls.Load() == 0 {
						t.Error("no queued page was produced before revocation")
					}
				}
				streamAuthorizationAssertReleased(t, h)
			})
		}
	}
}

func streamAuthorizationAssertFrames(t *testing.T, body string, wantEvents []string, failure string) {
	t.Helper()
	var events []string
	var fault map[string]any
	for _, frame := range strings.Split(strings.TrimSuffix(body, "\n\n"), "\n\n") {
		lines := strings.Split(frame, "\n")
		if len(lines) != 2 || !strings.HasPrefix(lines[0], fieldEvent) || !strings.HasPrefix(lines[1], fieldData) {
			t.Fatalf("invalid frame or keepalive after revocation: %q", frame)
		}
		event := strings.TrimPrefix(lines[0], fieldEvent)
		events = append(events, event)
		if event == eventFault {
			if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], fieldData)), &fault); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events=%v, want %v; body=%q", events, wantEvents, body)
	}
	wantFault := map[string]any{"errno": "EACCES", "message": "access denied"}
	if failure == "policy-failure" {
		wantFault = map[string]any{"errno": "EIO", "message": "authorization failed"}
	}
	if !reflect.DeepEqual(fault, wantFault) {
		t.Errorf("fault=%v, want %v", fault, wantFault)
	}
}

type streamAuthorizationSnap struct {
	metastore.Snap
	block            bool
	entered          chan struct{}
	exited           chan struct{}
	afterClose       func()
	nextCalls        atomic.Int32
	inNext           atomic.Bool
	closed           atomic.Bool
	closedDuringNext atomic.Bool
}

func (s *streamAuthorizationSnap) Next(ctx context.Context, limit int, result *metastore.RowResult) (bool, error) {
	s.nextCalls.Add(1)
	s.inNext.Store(true)
	defer s.inNext.Store(false)
	if s.block {
		close(s.entered)
		defer close(s.exited)
		<-ctx.Done()
		return false, result.Fail(ctx.Err())
	}
	return s.Snap.Next(ctx, limit, result)
}

func (s *streamAuthorizationSnap) Close() error {
	s.closedDuringNext.Store(s.inNext.Load())
	s.closed.Store(true)
	err := s.Snap.Close()
	if s.afterClose != nil {
		s.afterClose()
	}
	return err
}

func TestStreamAuthorizationCancellationInterruptsBlockedWrites(t *testing.T) {
	for _, op := range []Op{OpSubscribe, OpSnapshot} {
		for _, phase := range []string{"initial-flush", "active-frame"} {
			for _, trigger := range []string{"request-cancel", "handler-stop"} {
				t.Run(string(op)+"/"+phase+"/"+trigger, func(t *testing.T) {
					meta, backing := memoryfixture.New(t, "stored-volume", 1<<20, locking.DefaultOptions())
					var snapshot *streamAuthorizationSnap
					log := &streamAuthorizationLog{Log: meta, snapshot: func(snap metastore.Snap) metastore.Snap {
						snapshot = &streamAuthorizationSnap{Snap: snap}
						return snapshot
					}}
					options := DefaultHandlerOptions()
					options.Volume = streamAuthorizationVolume
					options.Authorizer = authz.AuthorizerFunc(func(ctx context.Context, access authz.AccessRequest) error {
						streamAuthorizationCheckAccess(t, ctx, access, op)
						return nil
					})
					h, err := NewHandlerWithOptions(backing, log, options)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(h.Stop)
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					w := newStreamAuthorizationWriter()
					w.blockInitialFlush = phase == "initial-flush"
					if phase == "active-frame" {
						w.blockEvent = eventStart
						if op == OpSnapshot {
							w.blockEvent = eventOpen
						}
					}
					done := streamAuthorizationServe(t, h, w, streamAuthorizationRequest(t, ctx, op))
					streamAuthorizationWait(t, w.blocked, "blocked "+phase)
					if op == OpSubscribe {
						w.mu.Lock()
						for _, deadline := range w.deadlines {
							if !deadline.IsZero() {
								t.Errorf("healthy subscription received write deadline %v", deadline)
							}
						}
						w.mu.Unlock()
					}
					started := time.Now()
					if trigger == "request-cancel" {
						cancel()
					} else {
						stopped := make(chan struct{})
						go func() { h.Stop(); close(stopped) }()
						streamAuthorizationWait(t, stopped, "nonblocking handler Stop")
					}
					streamAuthorizationWait(t, done, "interrupted "+phase)
					if elapsed := time.Since(started); elapsed > 10*departureGrace {
						t.Errorf("blocked write ended after %v; departure grace is %v", elapsed, departureGrace)
					}
					if snapshot != nil && (!snapshot.closed.Load() || snapshot.closedDuringNext.Load()) {
						t.Error("snapshot was not closed after its blocked response ended")
					}
					streamAuthorizationAssertReleased(t, h)
				})
			}
		}
	}
}

func TestStreamAuthorizationFaultWriteIsBoundedAndJoinsSnapshotProducer(t *testing.T) {
	meta, backing := memoryfixture.New(t, "stored-volume", 1<<20, locking.DefaultOptions())
	if err := backing.Mkdir(t.Context(), "second-page"); err != nil {
		t.Fatal(err)
	}
	var snap *streamAuthorizationJoiningSnap
	log := &streamAuthorizationLog{Log: meta, snapshot: func(original metastore.Snap) metastore.Snap {
		snap = &streamAuthorizationJoiningSnap{
			Snap: original, entered: make(chan struct{}), canceled: make(chan struct{}), join: make(chan struct{}),
		}
		return snap
	}}
	var revoked atomic.Bool
	options := DefaultHandlerOptions()
	options.Volume = streamAuthorizationVolume
	options.Replication.SnapshotPage = 1
	options.Replication.Keepalive = time.Hour
	options.Authorizer = authz.AuthorizerFunc(func(ctx context.Context, access authz.AccessRequest) error {
		streamAuthorizationCheckAccess(t, ctx, access, OpSnapshot)
		if revoked.Load() {
			streamAuthorizationWait(t, snap.entered, "prefetched second page")
			return authz.ErrDenied
		}
		return nil
	})
	h, err := NewHandlerWithOptions(backing, log, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	w := newStreamAuthorizationWriter()
	w.blockEvent = eventFault
	w.afterFlush = func(frame string) {
		if strings.HasPrefix(frame, "event: open\n") {
			revoked.Store(true)
		}
	}
	done := streamAuthorizationServe(t, h, w, streamAuthorizationRequest(t, t.Context(), OpSnapshot))
	// Next's cancellation observation precedes its return, so closure cannot be credited
	// merely because a cancellation signal was sent.
	streamAuthorizationWait(t, w.flushed, "snapshot open")
	t.Cleanup(func() { snap.release() })
	streamAuthorizationWait(t, snap.canceled, "producer cancellation")
	if snap.closed.Load() {
		t.Error("snapshot closed while its producer had not returned")
	}
	select {
	case <-done:
		t.Error("request returned before the snapshot producer joined")
	default:
	}
	snap.release()
	streamAuthorizationWait(t, w.blocked, "terminal fault write")
	started := time.Now()
	streamAuthorizationWait(t, done, "bounded terminal fault")
	if elapsed := time.Since(started); elapsed > 10*departureGrace {
		t.Errorf("terminal fault write took %v; departure grace is %v", elapsed, departureGrace)
	}
	if !snap.closed.Load() || snap.closedDuringNext.Load() {
		t.Errorf("snapshot closure: closed=%v closed-during-next=%v", snap.closed.Load(), snap.closedDuringNext.Load())
	}
	if body := w.String(); strings.Contains(body, "event: rows") || strings.Contains(body, "event: done") || strings.Contains(body, ": alive") {
		t.Errorf("data escaped after revocation: %q", body)
	}
	streamAuthorizationAssertReleased(t, h)
}

type streamAuthorizationJoiningSnap struct {
	metastore.Snap
	entered, canceled, join chan struct{}
	joinOnce                sync.Once
	nextCalls               int
	inNext                  atomic.Bool
	closed                  atomic.Bool
	closedDuringNext        atomic.Bool
}

func (s *streamAuthorizationJoiningSnap) Next(ctx context.Context, limit int, result *metastore.RowResult) (bool, error) {
	s.inNext.Store(true)
	defer s.inNext.Store(false)
	s.nextCalls++
	if s.nextCalls == 1 {
		return s.Snap.Next(ctx, limit, result)
	}
	close(s.entered)
	<-ctx.Done()
	close(s.canceled)
	<-s.join
	return false, result.Fail(ctx.Err())
}

func (s *streamAuthorizationJoiningSnap) release() { s.joinOnce.Do(func() { close(s.join) }) }

func (s *streamAuthorizationJoiningSnap) Close() error {
	s.closedDuringNext.Store(s.inNext.Load())
	s.closed.Store(true)
	return s.Snap.Close()
}

func TestStreamAuthorizationRevocationReachesHTTPSubscription(t *testing.T) {
	meta, backing := memoryfixture.New(t, "stored-volume", 1<<20, locking.DefaultOptions())
	var revoked atomic.Bool
	options := DefaultHandlerOptions()
	options.Volume = streamAuthorizationVolume
	options.Replication.Keepalive = 10 * time.Millisecond
	options.Authorizer = authz.AuthorizerFunc(func(ctx context.Context, access authz.AccessRequest) error {
		streamAuthorizationCheckAccess(t, ctx, access, OpSubscribe)
		if revoked.Load() {
			return fmt.Errorf("private host policy: %w", authz.ErrDenied)
		}
		return nil
	})
	h, err := NewHandlerWithOptions(backing, meta, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Stop)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), streamAuthorizationIdentityKey{}, "original-identity-and-trace")
		h.ServeHTTP(w, r.WithContext(ctx))
	}))
	t.Cleanup(server.Close)
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	sub, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	revoked.Store(true)
	if _, err := sub.Next(); !errors.Is(err, syscall.EACCES) || strings.Contains(err.Error(), "private host policy") {
		t.Fatalf("revoked HTTP subscription = %v, want sanitized EACCES", err)
	}
	waitForSubscriptions(t, h.publisher, 0)
}

func streamAuthorizationAssertReleased(t *testing.T, h *Handler) {
	t.Helper()
	for name, admission := range map[string]*bodyAdmission{
		"snapshots": h.snapshots, "frames": h.snapshotFrames, "bodies": h.bodies, "responses": h.responses,
	} {
		if got := streamAuthorizationAdmission(admission); got != [3]int64{} {
			t.Errorf("%s admission retained operations/bytes/waiters: %v", name, got)
		}
	}
	if got := streamAuthorizationSubscribers(h); got != 0 {
		t.Errorf("%d subscriptions remain attached", got)
	}
}

func streamAuthorizationWait(t *testing.T, done <-chan struct{}, phase string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", phase)
	}
}

func streamAuthorizationServe(t *testing.T, h *Handler, w *streamAuthorizationWriter, r *http.Request) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	t.Cleanup(func() {
		h.Stop()
		w.abort()
		streamAuthorizationWait(t, done, "request cleanup")
	})
	go func() {
		defer close(done)
		h.ServeHTTP(w, r)
	}()
	return done
}

// Expired deadlines release the selected write without relying on socket buffer size.
// Future deadlines remain observable so a healthy subscription cannot hide a timeout.
type streamAuthorizationWriter struct {
	header            http.Header
	mu                sync.Mutex
	body              bytes.Buffer
	pending           strings.Builder
	deadlines         []time.Time
	blockInitialFlush bool
	blockEvent        string
	blockJSON         bool
	afterFlush        func(string)
	flushes           int
	blocked           chan struct{}
	flushed           chan struct{}
	expired           chan struct{}
	blockedOnce       sync.Once
	flushedOnce       sync.Once
	expiredOnce       sync.Once
}

func newStreamAuthorizationWriter() *streamAuthorizationWriter {
	return &streamAuthorizationWriter{
		header: make(http.Header), blocked: make(chan struct{}), flushed: make(chan struct{}), expired: make(chan struct{}),
	}
}

func (w *streamAuthorizationWriter) Header() http.Header { return w.header }
func (w *streamAuthorizationWriter) WriteHeader(int)     {}

func (w *streamAuthorizationWriter) Write(p []byte) (int, error) {
	select {
	case <-w.expired:
		return 0, os.ErrDeadlineExceeded
	default:
	}
	w.mu.Lock()
	block := w.blockEvent != "" && strings.HasPrefix(w.pending.String()+string(p), fieldEvent+w.blockEvent) ||
		w.blockJSON && len(p) > 0 && p[0] == '{'
	w.mu.Unlock()
	if block {
		w.blockedOnce.Do(func() { close(w.blocked) })
		<-w.expired
		return 0, os.ErrDeadlineExceeded
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending.Write(p)
	return w.body.Write(p)
}

func (w *streamAuthorizationWriter) FlushError() error {
	w.mu.Lock()
	w.flushes++
	block := w.blockInitialFlush && w.flushes == 1
	frame := w.pending.String()
	w.pending.Reset()
	w.mu.Unlock()
	if block {
		w.blockedOnce.Do(func() { close(w.blocked) })
		<-w.expired
		return os.ErrDeadlineExceeded
	}
	if w.afterFlush != nil {
		w.afterFlush(frame)
	}
	if frame != "" {
		w.flushedOnce.Do(func() { close(w.flushed) })
	}
	return nil
}

func (w *streamAuthorizationWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadlines = append(w.deadlines, deadline)
	w.mu.Unlock()
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		w.abort()
	}
	return nil
}

func (w *streamAuthorizationWriter) abort() { w.expiredOnce.Do(func() { close(w.expired) }) }

func (w *streamAuthorizationWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.String()
}
