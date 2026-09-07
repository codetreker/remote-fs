package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	metasqlite "github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

func TestListResponseFitCalculationMatchesTheWire(t *testing.T) {
	entries := []storage.Entry{
		{
			Name: "plain",
			Attr: storage.Attr{
				ID:         1,
				Mode:       0o640,
				Size:       7,
				AccessTime: time.Unix(-1, 999_999_999),
				ModTime:    time.Unix(1<<40, 1),
			},
		},
		{
			Name: "\x00\xff not utf-8",
			Attr: storage.Attr{
				ID:         ^uint64(0),
				Mode:       fs.ModeDir | 0o755,
				Size:       -1 << 63,
				AccessTime: time.Unix(1<<62, 0),
				ModTime:    time.Unix(-1<<62, 123_456_789),
			},
		},
	}
	encoded, err := json.Marshal(ListResponse{Entries: EntriesOf(entries)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := newListResult(int64(len(encoded)))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := result.Add(entry); err != nil {
			t.Fatalf("a %d-byte listing did not fit its exact size: %v", len(encoded), err)
		}
	}
	result, err = newListResult(int64(len(encoded) - 1))
	if err != nil {
		t.Fatal(err)
	}
	for i, entry := range entries {
		err = result.Add(entry)
		if err != nil {
			break
		}
		if i == len(entries)-1 {
			t.Fatalf("a %d-byte listing fit inside %d bytes", len(encoded), len(encoded)-1)
		}
	}
}

func TestAdmissionBoundsWaitersAndCancellationReleasesThem(t *testing.T) {
	admission := newBodyAdmission(1, 4, 1)
	release, err := admission.acquire(t.Context(), 4)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	waitingContext, cancel := context.WithCancel(t.Context())
	waiting := make(chan error, 1)
	go func() {
		_, err := admission.acquire(waitingContext, 4)
		waiting <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		admission.mu.Lock()
		waiters := admission.waiters
		admission.mu.Unlock()
		if waiters == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the blocked acquisition never entered the bounded waiter set")
		}
		runtime.Gosched()
	}

	if _, err := admission.acquire(t.Context(), 4); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("an acquisition above the waiter bound returned %v, want EAGAIN", err)
	}
	cancel()
	if err := <-waiting; !errors.Is(err, context.Canceled) {
		t.Fatalf("the cancelled waiter returned %v", err)
	}
	admission.mu.Lock()
	waiters := admission.waiters
	admission.mu.Unlock()
	if waiters != 0 {
		t.Fatalf("cancellation left %d admission waiters", waiters)
	}
}

func TestClientAdmissionCoversFixedResponsesAndLastsThroughDecoding(t *testing.T) {
	var calls atomic.Int64
	client := &http.Client{Transport: internalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		header := http.Header{}
		header.Set(HeaderProtocol, Version)
		return &http.Response{
			Status:        "200 OK",
			StatusCode:    http.StatusOK,
			Header:        header,
			Body:          io.NopCloser(bytes.NewReader(nil)),
			ContentLength: 0,
		}, nil
	})}
	options := DefaultDialOptions()
	options.MaxBodyBytes = 1024
	options.MaxWriteBytes = 1024
	options.MaxConcurrentResponses = 1
	options.MaxInFlightResponseBytes = 4 * options.MaxBodyBytes
	options.MaxWaitingResponses = 1
	s, err := DialWithOptions("http://server.invalid", client, options)
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.call(t.Context(), Request{Op: OpStat, Path: "f"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.release()

	secondDone := make(chan error, 1)
	go func() {
		answer, err := s.call(t.Context(), Request{Op: OpSpace}, nil)
		if answer != nil {
			answer.release()
		}
		secondDone <- err
	}()
	waitForAdmissionWaiters(t, s.responses, 1)
	if got := calls.Load(); got != 1 {
		t.Fatalf("a fixed response waiting for decode admission reached HTTP; calls=%d", got)
	}
	if _, err := s.call(t.Context(), Request{Op: OpCreate, Path: "g"}, nil); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("a mutation above the client waiter bound returned %v, want EAGAIN", err)
	}
	first.release()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("HTTP received %d calls, want the two admitted operations", got)
	}
}

func TestStreamSetupErrorsUseClientResponseAdmission(t *testing.T) {
	blocked := &internalBlockingReader{entered: make(chan struct{}, 1), release: make(chan struct{})}
	var calls atomic.Int64
	client := &http.Client{Transport: internalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		header := http.Header{}
		header.Set(HeaderProtocol, Version)
		return &http.Response{
			Status: "422 storage error", StatusCode: StatusStorageError, Header: header,
			Body: io.NopCloser(blocked),
		}, nil
	})}
	options := DefaultDialOptions()
	options.MaxBodyBytes = 1024
	options.MaxWriteBytes = 1024
	options.MaxConcurrentResponses = 1
	options.MaxInFlightResponseBytes = retainedResponseMultiplier * options.MaxBodyBytes
	options.MaxWaitingResponses = 1
	s, err := DialWithOptions("http://server.invalid", client, options)
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() {
		_, err := s.Subscribe(t.Context())
		first <- err
	}()
	<-blocked.entered

	waiting, cancel := context.WithCancel(t.Context())
	second := make(chan error, 1)
	go func() {
		_, err := s.Snapshot(waiting)
		second <- err
	}()
	waitForAdmissionWaiters(t, s.responses, 1)
	if got := calls.Load(); got != 1 {
		t.Fatalf("a stream setup waiting for response admission reached HTTP; calls=%d", got)
	}
	if _, err := s.Subscribe(t.Context()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("stream setup above the response waiter bound returned %v, want EAGAIN", err)
	}
	cancel()
	if err := <-second; err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("cancelled stream setup waiter returned %v", err)
	}
	close(blocked.release)
	if err := <-first; err == nil {
		t.Fatal("empty storage-error response was accepted")
	}
}

func TestHandlerAdmissionBoundsStatWaitersAndAQueuedWrite(t *testing.T) {
	backing, err := pairedDirectory(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	blocked := &blockedStatStorage{
		BoundedStorage: backing,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	options := DefaultHandlerOptions()
	options.MaxBodyBytes = 1024
	options.MaxWriteBytes = 1024
	options.MaxInFlightBodyBytes = 1024
	options.MaxConcurrentResponses = 1
	options.MaxInFlightResponseBytes = 4 * options.MaxBodyBytes
	options.MaxWaitingResponses = 1
	handler, err := NewHandlerWithOptions(blocked, nil, options)
	if err != nil {
		t.Fatal(err)
	}

	firstDone := serveInternalAsync(t, handler, Request{Op: OpStat, Path: "missing"}, nil)
	<-blocked.entered
	secondDone := serveInternalAsync(t, handler, Request{Op: OpCreate, Path: "created"}, nil)
	waitForAdmissionWaiters(t, handler.responses, 1)

	writeBody := &countingReader{from: bytes.NewReader([]byte("x"))}
	third := serveInternal(t, handler, Request{Op: OpWrite, Path: "not-written"}, writeBody)
	if third.Code != StatusStorageError {
		t.Fatalf("a write above the response waiter bound answered %d: %s", third.Code, third.Body)
	}
	var response ErrorResponse
	if err := json.Unmarshal(third.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Errno != "EAGAIN" || writeBody.read != 0 {
		t.Fatalf("queued write answered %+v after reading %d body bytes", response, writeBody.read)
	}

	close(blocked.release)
	if first := <-firstDone; first.Code != StatusStorageError {
		t.Fatalf("blocked stat answered %d: %s", first.Code, first.Body)
	}
	if second := <-secondDone; second.Code != http.StatusOK {
		t.Fatalf("queued create answered %d: %s", second.Code, second.Body)
	}
}

func TestHandlerChargesFixedErrorResponsesAgainstAggregateBytes(t *testing.T) {
	backing, err := pairedDirectory(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	blocked := &multiBlockedStatStorage{
		BoundedStorage: backing,
		entered:        make(chan struct{}, 5),
		release:        make(chan struct{}),
	}
	options := DefaultHandlerOptions()
	options.MaxBodyBytes = 1024
	options.MaxWriteBytes = 1024
	options.MaxInFlightBodyBytes = 1024
	options.MaxConcurrentResponses = 8
	options.MaxInFlightResponseBytes = 4 * options.MaxBodyBytes
	options.MaxWaitingResponses = 1
	handler, err := NewHandlerWithOptions(blocked, nil, options)
	if err != nil {
		t.Fatal(err)
	}

	done := make([]<-chan *httptest.ResponseRecorder, 0, 5)
	for i := 0; i < 4; i++ {
		done = append(done, serveInternalAsync(t, handler, Request{Op: OpStat, Path: "missing"}, nil))
	}
	for i := 0; i < 4; i++ {
		<-blocked.entered
	}
	done = append(done, serveInternalAsync(t, handler, Request{Op: OpStat, Path: "waiting"}, nil))
	waitForAdmissionWaiters(t, handler.responses, 1)
	refused := serveInternal(t, handler, Request{Op: OpStat, Path: "refused"}, nil)
	var response ErrorResponse
	if err := json.Unmarshal(refused.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Errno != "EAGAIN" {
		t.Fatalf("a fixed response above the aggregate byte bound answered %+v", response)
	}

	close(blocked.release)
	for _, completed := range done {
		if response := <-completed; response.Code != StatusStorageError {
			t.Fatalf("admitted stat answered %d: %s", response.Code, response.Body)
		}
	}
}

func TestSnapshotFrameAdmissionBoundsAggregateWaitersAndCancellation(t *testing.T) {
	backing, err := pairedDirectory(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultHandlerOptions()
	options.Replication.Keepalive = time.Hour
	options.MaxFrameBytes = 1024
	options.MaxConcurrentSnapshotFrames = 2
	options.MaxInFlightSnapshotFrameBytes = retainedFrameMultiplier * options.MaxFrameBytes
	options.MaxWaitingSnapshotFrames = 1
	handler, err := NewHandlerWithOptions(backing, nil, options)
	if err != nil {
		t.Fatal(err)
	}

	release, err := handler.acquireSnapshotFrame(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	waiting, cancel := context.WithCancel(t.Context())
	second := make(chan error, 1)
	go func() {
		acquired, err := handler.acquireSnapshotFrame(waiting)
		if acquired != nil {
			acquired()
		}
		second <- err
	}()
	waitForAdmissionWaiters(t, handler.snapshotFrames, 1)
	if _, err := handler.acquireSnapshotFrame(t.Context()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("a frame above the waiter bound returned %v, want EAGAIN", err)
	}
	cancel()
	if err := <-second; !errors.Is(err, context.Canceled) {
		t.Fatalf("a cancelled frame waiter returned %v", err)
	}
	waitForAdmissionWaiters(t, handler.snapshotFrames, 0)
	release()
	replacement, err := handler.acquireSnapshotFrame(t.Context())
	if err != nil {
		t.Fatalf("frame admission did not recover after release: %v", err)
	}
	replacement()
}

func TestDerivedStreamFrameBoundsUseCheckedProducts(t *testing.T) {
	events, err := retainedEventFrameBytes(DefaultLimits().MaxSubscriptions, DefaultMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	wantEvents := int64(DefaultLimits().MaxSubscriptions) * retainedFrameMultiplier * DefaultMaxFrameBytes
	if events != wantEvents {
		t.Fatalf("event frame bound = %d, want %d", events, wantEvents)
	}
	cursors, err := retainedSnapshotCursorBytes(DefaultLimits().Snapshots, DefaultMaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(DefaultLimits().Snapshots) * DefaultMaxFrameBytes; cursors != want {
		t.Fatalf("snapshot cursor bound = %d, want %d", cursors, want)
	}
	if _, err := retainedEventFrameBytes(math.MaxInt, math.MaxInt64); err == nil {
		t.Fatal("overflowing event frame product was accepted")
	}
	if _, err := retainedSnapshotCursorBytes(math.MaxInt, math.MaxInt64); err == nil {
		t.Fatal("overflowing snapshot cursor product was accepted")
	}
}

func TestChangeDeliveryIsIndependentOfSnapshotAdmissionAndASlowSubscriber(t *testing.T) {
	log, err := metasqlite.Open(t.Context(), filepath.Join(t.TempDir(), "log.db"), "workspace", 0, metasqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	if err := log.Create(t.Context(), "changed"); err != nil {
		t.Fatal(err)
	}
	backing, err := pairedDirectory(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultHandlerOptions()
	options.MaxFrameBytes = 1024
	options.MaxConcurrentSnapshotFrames = 1
	options.MaxInFlightSnapshotFrameBytes = retainedFrameMultiplier * options.MaxFrameBytes
	handler, err := NewHandlerWithOptions(backing, log, options)
	if err != nil {
		t.Fatal(err)
	}
	releaseSnapshot, err := handler.acquireSnapshotFrame(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer releaseSnapshot()

	blocked := newFrameTestWriter(true)
	blockedOut, err := openStream(blocked, options.MaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	blockedContext, cancelBlocked := context.WithCancel(t.Context())
	blockedDone := make(chan error, 1)
	go func() { blockedDone <- handler.publish(blockedContext, blockedOut, 0, make(chan struct{})) }()
	<-blocked.entered

	healthy := newFrameTestWriter(false)
	healthyOut, err := openStream(healthy, options.MaxFrameBytes)
	if err != nil {
		t.Fatal(err)
	}
	healthyContext, cancelHealthy := context.WithCancel(t.Context())
	healthyDone := make(chan error, 1)
	go func() { healthyDone <- handler.publish(healthyContext, healthyOut, 0, make(chan struct{})) }()
	select {
	case <-healthy.change:
	case <-time.After(time.Second):
		t.Fatal("a saturated snapshot gate and slow subscriber blocked healthy change delivery")
	}
	cancelHealthy()
	if err := <-healthyDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("healthy publisher ended with %v", err)
	}
	cancelBlocked()
	close(blocked.release)
	if err := <-blockedDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked publisher ended with %v", err)
	}
}

func TestSnapshotKeepalivesContinueWhileFrameAdmissionWaits(t *testing.T) {
	log, err := metasqlite.Open(t.Context(), filepath.Join(t.TempDir(), "log.db"), "workspace", 0, metasqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	if err := log.Create(t.Context(), "entry"); err != nil {
		t.Fatal(err)
	}
	backing, err := pairedDirectory(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	options := DefaultHandlerOptions()
	options.MaxFrameBytes = 1024
	options.MaxConcurrentSnapshotFrames = 1
	options.MaxInFlightSnapshotFrameBytes = retainedFrameMultiplier * options.MaxFrameBytes
	options.Replication.Keepalive = 20 * time.Millisecond
	options.Replication.SnapshotDeadline = time.Second
	handler, err := NewHandlerWithOptions(backing, log, options)
	if err != nil {
		t.Fatal(err)
	}
	release, err := handler.acquireSnapshotFrame(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		handler.Stop()
		server.Close()
	})
	dial := DefaultDialOptions()
	dial.Silence = 60 * time.Millisecond
	dial.MaxFrameBytes = options.MaxFrameBytes
	client, err := DialWithOptions(server.URL, server.Client(), dial)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := client.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer snap.Close()
	next := make(chan error, 1)
	go func() {
		_, err := snap.Next()
		next <- err
	}()
	select {
	case err := <-next:
		t.Fatalf("snapshot read ended while admission was held: %v", err)
	case <-time.After(120 * time.Millisecond):
	}
	release()
	select {
	case err := <-next:
		if err != nil {
			t.Fatalf("snapshot did not resume after admission release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("snapshot did not resume after admission release")
	}
}

func TestBoundedFrameSizersMatchTheEncodedChangeAndSnapshotPage(t *testing.T) {
	change := metastore.Change{
		Position: 7, Kind: metastore.Renamed, Parent: 1, Name: []byte{0xff, 0x00, 'n'},
		From: &metastore.Location{Parent: 2, Name: []byte("from")},
		Node: &metastore.Node{ID: 3, Mode: 0o644, Size: 5, Content: "object"},
	}
	wireChange, err := ChangeOf(change)
	if err != nil {
		t.Fatal(err)
	}
	encodedChange, err := json.Marshal(wireChange)
	if err != nil {
		t.Fatal(err)
	}
	changeBytes, err := encodedFrameBytes(eventChange, int64(len(encodedChange)))
	if err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int64{0, -1} {
		result, err := newChangeFrameResult(changeBytes + delta)
		if err != nil {
			t.Fatal(err)
		}
		meta := change
		name, fromName, content := meta.Name, meta.From.Name, meta.Node.Content
		meta.Name = []byte{}
		from, node := *meta.From, *meta.Node
		from.Name, node.Content = []byte{}, ""
		meta.From, meta.Node = &from, &node
		reservation, fits, reserveErr := result.Reserve(meta, metastore.ChangePayloadLengths{
			Name: int64(len(name)), FromName: int64(len(fromName)), Content: int64(len(content)),
		})
		if delta == -1 {
			if !errors.Is(reserveErr, syscall.EFBIG) || fits || reservation != nil {
				t.Fatalf("change under exact-1 bound: reservation=%v fits=%v err=%v", reservation, fits, reserveErr)
			}
			continue
		}
		if reserveErr != nil || !fits {
			t.Fatalf("change under exact bound: fits=%v err=%v", fits, reserveErr)
		}
		if err := reservation.Commit(name, fromName, content); err != nil {
			t.Fatal(err)
		}
	}

	rows := []metastore.Row{
		{Node: metastore.Node{ID: 1, Mode: fs.ModeDir | 0o755}},
		{Parent: 1, Name: []byte{0xff, 'x'}, Node: metastore.Node{ID: 2, Mode: 0o644, Content: "key"}},
	}
	wireRows := SnapshotPage{Rows: []Row{RowOf(rows[0]), RowOf(rows[1])}}
	encodedRows, err := json.Marshal(wireRows)
	if err != nil {
		t.Fatal(err)
	}
	rowBytes, err := encodedFrameBytes(eventRows, int64(len(encodedRows)))
	if err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int64{0, -1} {
		result, err := newSnapshotFrameResult(rowBytes + delta)
		if err != nil {
			t.Fatal(err)
		}
		var reserveErr error
		pageFull := false
		for _, row := range rows {
			meta, name, content := row, row.Name, row.Node.Content
			if name == nil {
				meta.Name = nil
			} else {
				meta.Name = []byte{}
			}
			meta.Node.Content = ""
			reservation, fits, err := result.Reserve(meta, metastore.RowPayloadLengths{
				Name: int64(len(name)), Content: int64(len(content)),
			})
			if err != nil || !fits {
				reserveErr = err
				pageFull = !fits && err == nil
				break
			}
			reserveErr = reservation.Commit(name, content)
			if reserveErr != nil {
				break
			}
		}
		if delta == -1 {
			if reserveErr != nil || !pageFull {
				empty, _ := json.Marshal(SnapshotPage{Rows: []Row{}})
				one0, _ := json.Marshal(SnapshotPage{Rows: []Row{RowOf(rows[0])}})
				one1, _ := json.Marshal(SnapshotPage{Rows: []Row{RowOf(rows[1])}})
				t.Fatalf("snapshot page under exact-1 bound returned full=%v err=%v; actual=%d empty=%d one=(%d,%d)",
					pageFull, reserveErr, rowBytes, len(empty), len(one0), len(one1))
			}
			continue
		}
		if reserveErr != nil {
			t.Fatalf("snapshot page under exact bound returned %v", reserveErr)
		}
	}
}

func TestSubscriptionAdmissionReleasesOnCancelAndStop(t *testing.T) {
	backing, err := pairedDirectory(t, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	log, err := metasqlite.Open(t.Context(), filepath.Join(t.TempDir(), "log.db"), "workspace", 0, metasqlite.DefaultWindow())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	limits := DefaultLimits()
	limits.MaxSubscriptions = 1
	handler, err := NewHandlerWithLimits(backing, log, limits)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}

	first, err := client.Subscribe(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	waitForSubscriptions(t, handler.publisher, 1)
	if second, err := client.Subscribe(t.Context()); !errors.Is(err, syscall.EAGAIN) || second != nil {
		t.Fatalf("a subscription above the limit returned %v, %v; want EAGAIN", second, err)
	}
	waitForSubscriptions(t, handler.publisher, 1)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSubscriptions(t, handler.publisher, 0)

	replacement, err := client.Subscribe(t.Context())
	if err != nil {
		t.Fatalf("subscription capacity was not released after cancellation: %v", err)
	}
	waitForSubscriptions(t, handler.publisher, 1)
	handler.Stop()
	if _, err := replacement.Next(); !errors.Is(err, ErrServerStopping) {
		t.Fatalf("stopped subscription returned %v, want ErrServerStopping", err)
	}
	waitForSubscriptions(t, handler.publisher, 0)
}

func waitForAdmissionWaiters(t *testing.T, admission *bodyAdmission, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		admission.mu.Lock()
		waiters := admission.waiters
		admission.mu.Unlock()
		if waiters == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("admission has %d waiters, want %d", waiters, want)
		}
		runtime.Gosched()
	}
}

func waitForSubscriptions(t *testing.T, publisher *publisher, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		publisher.mu.Lock()
		attached := len(publisher.wakes)
		publisher.mu.Unlock()
		if attached == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("publisher has %d subscriptions, want %d", attached, want)
		}
		runtime.Gosched()
	}
}

type internalRoundTripFunc func(*http.Request) (*http.Response, error)

func (f internalRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type internalBlockingReader struct {
	entered chan struct{}
	release chan struct{}
}

func (r *internalBlockingReader) Read([]byte) (int, error) {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-r.release
	return 0, io.EOF
}

type frameTestWriter struct {
	header  http.Header
	block   bool
	entered chan struct{}
	release chan struct{}
	change  chan struct{}
	once    sync.Once
	bytes.Buffer
}

func newFrameTestWriter(block bool) *frameTestWriter {
	return &frameTestWriter{
		header: make(http.Header), block: block, entered: make(chan struct{}),
		release: make(chan struct{}), change: make(chan struct{}),
	}
}

func (w *frameTestWriter) Header() http.Header { return w.header }
func (w *frameTestWriter) WriteHeader(int)     {}
func (w *frameTestWriter) Flush()              {}

func (w *frameTestWriter) Write(p []byte) (int, error) {
	if w.block {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	n, err := w.Buffer.Write(p)
	if bytes.Contains(w.Buffer.Bytes(), []byte("event: change\n")) {
		select {
		case <-w.change:
		default:
			close(w.change)
		}
	}
	return n, err
}

type blockedStatStorage struct {
	storage.BoundedStorage
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

type multiBlockedStatStorage struct {
	storage.BoundedStorage
	entered chan struct{}
	release chan struct{}
}

func (s *multiBlockedStatStorage) LockService() locking.Service {
	return s.BoundedStorage.(locked.Backend).LockService()
}

func (s *multiBlockedStatStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	s.entered <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return storage.Attr{}, ctx.Err()
	}
	return s.BoundedStorage.Stat(ctx, path)
}

func (s *blockedStatStorage) LockService() locking.Service {
	return s.BoundedStorage.(locked.Backend).LockService()
}

func (s *blockedStatStorage) Stat(ctx context.Context, path string) (storage.Attr, error) {
	s.once.Do(func() { close(s.entered) })
	select {
	case <-s.release:
	case <-ctx.Done():
		return storage.Attr{}, ctx.Err()
	}
	return s.BoundedStorage.Stat(ctx, path)
}

type countingReader struct {
	from io.Reader
	read int
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.from.Read(p)
	r.read += n
	return n, err
}

func serveInternalAsync(t *testing.T, handler http.Handler, request Request, body io.Reader) <-chan *httptest.ResponseRecorder {
	t.Helper()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- serveInternal(t, handler, request, body) }()
	return done
}

func serveInternal(t *testing.T, handler http.Handler, request Request, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	base, _ := url.Parse("http://server.invalid")
	u, err := request.URL(base)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := httptest.NewRequest(request.Method(), u.String(), body)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httpRequest)
	return response
}

func pairedDirectory(t *testing.T, root string) (*localdir.Storage, error) {
	t.Helper()
	config := localdir.Config{
		Root:      root,
		StateRoot: t.TempDir(),
		Locks:     locking.DefaultOptions(),
		Limits:    localdir.DefaultLimits(),
	}
	if err := os.Chmod(config.StateRoot, 0o700); err != nil {
		return nil, err
	}
	if err := localdir.Init(t.Context(), config); err != nil {
		return nil, err
	}
	backend, err := localdir.Open(t.Context(), config)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close directory: %v", err)
		}
	})
	return backend, nil
}
