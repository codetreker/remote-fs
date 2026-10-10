package replicated

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type settlementSessionProbe struct {
	*fileSessionStub
	response func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error)
	calls    int
}

func (s *settlementSessionProbe) CloseWithBarrier(context.Context) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
	s.calls++
	return s.response(s.calls)
}

type settlementFixture struct {
	base     *Storage
	close    func(context.Context) (storage.ReferenceCloseResult, error)
	closed   func() bool
	calls    func() int
	attempts func() []storage.CloseAttempt
}

func newSettlementFixture(t *testing.T, kind string, response func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error)) settlementFixture {
	t.Helper()
	if kind == "session" {
		remote := &settlementSessionProbe{fileSessionStub: &fileSessionStub{}, response: response}
		session := retainedTestSession(t, remote)
		return settlementFixture{base: session.base, close: session.CloseWithResult, closed: func() bool { return session.closed }, calls: func() int { return remote.calls }}
	}
	session := retainedTestSession(t, nil)
	attempt := closeAttemptForTest(t)
	if kind == "file" {
		remote := &actionFileProbe{fileAuthorityStub: &fileAuthorityStub{}, result: response}
		file := &retainedFile{session: session, remote: remote}
		return settlementFixture{
			base: session.base, close: func(ctx context.Context) (storage.ReferenceCloseResult, error) {
				return file.CloseWithAction(ctx, attempt)
			},
			closed: func() bool { return file.closed }, calls: func() int { return len(remote.attempts) }, attempts: func() []storage.CloseAttempt { return remote.attempts },
		}
	}
	remote := &actionReferenceProbe{barrierReferenceStub: &barrierReferenceStub{}, result: response}
	reference := &nodeReference{session: session, remote: remote}
	return settlementFixture{
		base: session.base, close: func(ctx context.Context) (storage.ReferenceCloseResult, error) {
			return reference.CloseWithAction(ctx, attempt)
		},
		closed: func() bool { return reference.closed }, calls: func() int { return len(remote.attempts) }, attempts: func() []storage.CloseAttempt { return remote.attempts },
	}
}

func assertCloseSettlement(t *testing.T, result storage.ReferenceCloseResult, err error, state storage.CloseSettlementState) {
	t.Helper()
	var unsettled *storage.CloseSettlementError
	if !result.Released || !result.Determined || !errors.As(err, &unsettled) || unsettled.State != state || unsettled.Check() != nil || !errors.Is(err, syscall.EIO) {
		t.Fatalf("close settlement = %+v, %v; want state %v", result, err, state)
	}
}

func TestReleasedCloseProjectsNeutralSettlementEvidence(t *testing.T) {
	for _, kind := range []string{"file", "node", "session"} {
		for _, test := range []struct {
			name    string
			barrier *httprest.MutationBarrier
			err     error
			state   storage.CloseSettlementState
		}{
			{name: "server pending", err: &httprest.CloseBarrierPendingError{State: storage.CloseSettlementPending, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EIO}, state: storage.CloseSettlementPending},
			{name: "server unknown", err: &httprest.CloseBarrierPendingError{State: storage.CloseSettlementUnknown, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EIO}, state: storage.CloseSettlementUnknown},
			{name: "missing barrier", err: syscall.ENOTEMPTY, state: storage.CloseSettlementUnknown},
			{name: "negative position", barrier: &httprest.MutationBarrier{Incarnation: "log", Position: -1}, err: syscall.ENOTEMPTY, state: storage.CloseSettlementUnknown},
			{name: "wrong log", barrier: &httprest.MutationBarrier{Incarnation: "other"}, err: syscall.ENOTEMPTY, state: storage.CloseSettlementUnknown},
		} {
			t.Run(kind+"/"+test.name, func(t *testing.T) {
				fixture := newSettlementFixture(t, kind, func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
					return storage.ReferenceCloseResult{Released: true, Determined: true}, test.barrier, test.err
				})
				result, err := fixture.close(t.Context())
				assertCloseSettlement(t, result, err, test.state)
				var unsettled *storage.CloseSettlementError
				if !errors.As(err, &unsettled) || !errors.Is(unsettled.SemanticErr, syscall.ENOTEMPTY) || fixture.closed() {
					t.Fatalf("semantic error or owner lost: %v, closed=%t", err, fixture.closed())
				}
			})
		}
	}
}

func TestReleasedCloseSettlesLocalBarrierWithoutRepeatingAuthorityClose(t *testing.T) {
	for _, kind := range []string{"file", "node", "session"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newSettlementFixture(t, kind, func(int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
				return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log", Position: 1}, syscall.ENOTEMPTY
			})
			cut, cancel := context.WithCancel(t.Context())
			cancel()
			result, err := fixture.close(cut)
			assertCloseSettlement(t, result, err, storage.CloseSettlementPending)
			fixture.base.mu.Lock()
			fixture.base.at = 1
			fixture.base.wake()
			fixture.base.mu.Unlock()
			for range 2 {
				result, err = fixture.close(t.Context())
				var unsettled *storage.CloseSettlementError
				if !result.Released || !result.Determined || !errors.Is(err, syscall.ENOTEMPTY) || errors.As(err, &unsettled) || errors.Is(err, syscall.EIO) || !fixture.closed() || fixture.calls() != 1 {
					t.Fatalf("settled close = %+v, %v, closed=%t, calls=%d", result, err, fixture.closed(), fixture.calls())
				}
			}
		})
	}
}

func TestReleasedCloseReplaysOriginalAttemptAndPreservesJoinedSemanticErrors(t *testing.T) {
	semantic := errors.New("cleanup semantic failure")
	for _, kind := range []string{"file", "node", "session"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newSettlementFixture(t, kind, func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
				if call == 1 {
					return storage.ReferenceCloseResult{Released: true, Determined: true}, nil,
						fmt.Errorf("authority close: %w", errors.Join(semantic, &httprest.CloseBarrierPendingError{State: storage.CloseSettlementPending, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EIO}))
				}
				if call == 2 {
					return storage.ReferenceCloseResult{}, nil, syscall.ECONNRESET
				}
				return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
			})
			result, err := fixture.close(t.Context())
			assertCloseSettlement(t, result, err, storage.CloseSettlementPending)
			result, err = fixture.close(t.Context())
			assertCloseSettlement(t, result, err, storage.CloseSettlementUnknown)
			if !errors.Is(err, syscall.ECONNRESET) || !errors.Is(err, semantic) || !errors.Is(err, syscall.ENOTEMPTY) {
				t.Fatalf("lost replay discarded original semantic errors: %v", err)
			}
			for range 2 {
				result, err = fixture.close(t.Context())
				var unsettled *storage.CloseSettlementError
				if !result.Released || !result.Determined || !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, semantic) || errors.As(err, &unsettled) || errors.Is(err, syscall.EIO) || !fixture.closed() || fixture.calls() != 3 {
					t.Fatalf("settled close = %+v, %v, closed=%t, calls=%d", result, err, fixture.closed(), fixture.calls())
				}
			}
			if fixture.attempts != nil {
				attempts := fixture.attempts()
				for _, attempt := range attempts[1:] {
					if attempt != attempts[0] {
						t.Fatalf("settlement replay changed action or generation: %+v", attempts)
					}
				}
			}
		})
	}
}

func TestFailedOpenSessionCloseRetainsReleasedSemanticErrorUntilSettlement(t *testing.T) {
	base := confirmationTestStorage(DefaultOptions())
	t.Cleanup(base.stop)
	remote := &settlementSessionProbe{fileSessionStub: &fileSessionStub{}}
	remote.response = func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
		if call == 1 {
			return storage.ReferenceCloseResult{Released: true, Determined: true}, nil,
				&httprest.CloseBarrierPendingError{State: storage.CloseSettlementPending, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EIO}
		}
		if call == 2 {
			return storage.ReferenceCloseResult{}, nil, syscall.ECONNRESET
		}
		return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
	}
	openingErr := errors.New("session open was not confirmed")
	session, err := base.cleanupFailedOpenSession(remote, openingErr)
	if session == nil || !errors.Is(err, openingErr) || !errors.Is(err, syscall.ENOTEMPTY) || len(base.fileCleanupSessions) != 1 {
		t.Fatalf("failed open lost cleanup owner: %T, %v, owners=%d", session, err, len(base.fileCleanupSessions))
	}
	result, err := session.CloseWithResult(t.Context())
	assertCloseSettlement(t, result, err, storage.CloseSettlementUnknown)
	if !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("failed-open replay lost semantic outcome: %v", err)
	}
	for range 2 {
		result, err = session.CloseWithResult(t.Context())
		var unsettled *storage.CloseSettlementError
		if !result.Released || !result.Determined || !errors.Is(err, syscall.ENOTEMPTY) || errors.As(err, &unsettled) || errors.Is(err, syscall.EIO) || len(base.fileCleanupSessions) != 0 || remote.calls != 3 {
			t.Fatalf("failed-open settlement = %+v, %v, owners=%d, calls=%d", result, err, len(base.fileCleanupSessions), remote.calls)
		}
	}
}

func TestReleasedCloseWaitsForLowerNeutralSettlementBeforeReplicaBarrier(t *testing.T) {
	for _, kind := range []string{"file", "node", "session"} {
		for _, state := range []storage.CloseSettlementState{storage.CloseSettlementPending, storage.CloseSettlementUnknown} {
			t.Run(fmt.Sprintf("%s/%d", kind, state), func(t *testing.T) {
				semantic := errors.New("lower cleanup semantic error")
				effects := 0
				fixture := newSettlementFixture(t, kind, func(call int) (storage.ReferenceCloseResult, *httprest.MutationBarrier, error) {
					if call == 1 {
						effects++
						return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"},
							fmt.Errorf("lower adapter: %w", errors.Join(semantic, &storage.CloseSettlementError{State: state, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EIO}))
					}
					return storage.ReferenceCloseResult{Released: true, Determined: true}, &httprest.MutationBarrier{Incarnation: "log"}, nil
				})
				result, err := fixture.close(t.Context())
				assertCloseSettlement(t, result, err, state)
				if fixture.closed() || effects != 1 || fixture.calls() != 1 {
					t.Fatalf("lower settlement was dropped: closed=%t, effects=%d, calls=%d", fixture.closed(), effects, fixture.calls())
				}
				for range 2 {
					result, err = fixture.close(t.Context())
					var unsettled *storage.CloseSettlementError
					if !result.Released || !result.Determined || !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, semantic) || errors.As(err, &unsettled) || errors.Is(err, syscall.EIO) || !fixture.closed() || fixture.calls() != 2 || effects != 1 {
						t.Fatalf("lower settlement = %+v, %v, closed=%t, calls=%d, effects=%d", result, err, fixture.closed(), fixture.calls(), effects)
					}
				}
				if fixture.attempts != nil {
					attempts := fixture.attempts()
					if len(attempts) != 2 || attempts[0] != attempts[1] {
						t.Fatalf("lower settlement changed action or generation: %+v", attempts)
					}
				}
			})
		}
	}
}

type neutralCleanupSessionProbe struct {
	storage.FileSession
	calls int
}

func (s *neutralCleanupSessionProbe) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	s.calls++
	if s.calls == 1 {
		return storage.ReferenceCloseResult{Released: true, Determined: true},
			&storage.CloseSettlementError{State: storage.CloseSettlementPending, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EIO}
	}
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}

func TestFailedOpenSessionRetainsLowerNeutralCleanupSettlement(t *testing.T) {
	base := confirmationTestStorage(DefaultOptions())
	t.Cleanup(base.stop)
	remote := &neutralCleanupSessionProbe{}
	session, err := base.cleanupFailedOpenSession(remote, errors.New("open failed"))
	var unsettled *storage.CloseSettlementError
	if session == nil || !errors.As(err, &unsettled) || unsettled.Check() != nil || unsettled.State != storage.CloseSettlementPending || len(base.fileCleanupSessions) != 1 || remote.calls != 1 {
		t.Fatalf("lower neutral cleanup owner = %T, %v, owners=%d, calls=%d", session, err, len(base.fileCleanupSessions), remote.calls)
	}
	for range 2 {
		result, err := session.CloseWithResult(t.Context())
		if !result.Released || !result.Determined || !errors.Is(err, syscall.ENOTEMPTY) || errors.As(err, &unsettled) || errors.Is(err, syscall.EIO) || len(base.fileCleanupSessions) != 0 || remote.calls != 2 {
			t.Fatalf("lower cleanup settlement = %+v, %v, owners=%d, calls=%d", result, err, len(base.fileCleanupSessions), remote.calls)
		}
	}
}
