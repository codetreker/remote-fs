package replicated

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type partialCleanupIdentity struct {
	id        uint64
	err       error
	statCalls int
}

func (p *partialCleanupIdentity) ReferenceNodeID() (uint64, error) { return p.id, p.err }

type partialNeutralFileProbe struct {
	*cleanupFileStub
	*partialCleanupIdentity
	response func(int) (storage.ReferenceCloseResult, error)
}

func (p *partialNeutralFileProbe) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	p.closes++
	return p.response(p.closes)
}

func (p *partialNeutralFileProbe) Stat(context.Context) (storage.Attr, error) {
	p.statCalls++
	return storage.Attr{ID: 999}, nil
}

type partialNeutralReferenceProbe struct {
	*cleanupReferenceStub
	*partialCleanupIdentity
	response func(int) (storage.ReferenceCloseResult, error)
}

func (p *partialNeutralReferenceProbe) CloseWithResult(context.Context) (storage.ReferenceCloseResult, error) {
	p.closes++
	return p.response(p.closes)
}

func (p *partialNeutralReferenceProbe) Stat(context.Context) (storage.Attr, error) {
	p.statCalls++
	return storage.Attr{ID: 999}, nil
}

type partialCleanupHandle interface {
	Stat(context.Context) (storage.Attr, error)
	CloseWithResult(context.Context) (storage.ReferenceCloseResult, error)
}

func TestPartialOpenRetainsNeutralCloseSettlementAndImmutableIdentity(t *testing.T) {
	for _, kind := range []string{"file", "node"} {
		t.Run(kind, func(t *testing.T) {
			session := retainedTestSession(t, nil)
			identity := &partialCleanupIdentity{id: 23}
			openingErr := errors.New("open response was not confirmed")
			response := func(call int) (storage.ReferenceCloseResult, error) {
				if call == 1 {
					return storage.ReferenceCloseResult{Released: true, Determined: true},
						&storage.CloseSettlementError{State: storage.CloseSettlementPending, SemanticErr: syscall.ENOTEMPTY, Cause: syscall.EIO}
				}
				if call == 2 {
					return storage.ReferenceCloseResult{}, syscall.ECONNRESET
				}
				return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
			}
			var handle partialCleanupHandle
			var err error
			var calls func() int
			if kind == "file" {
				remote := &partialNeutralFileProbe{cleanupFileStub: &cleanupFileStub{}, partialCleanupIdentity: identity, response: response}
				opened, failure := session.wrapOpenResult(storage.OpenResult{File: remote, Attr: storage.Attr{ID: 23}}, openingErr)
				handle, err, calls = opened.File, failure, func() int { return remote.closes }
			} else {
				remote := &partialNeutralReferenceProbe{cleanupReferenceStub: &cleanupReferenceStub{}, partialCleanupIdentity: identity, response: response}
				opened, failure := session.openReference(t.Context(), func(context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
					return storage.NodeOpenResult{Reference: remote, Attr: storage.Attr{ID: 23}}, nil, openingErr
				})
				handle, err, calls = opened.Reference, failure, func() int { return remote.closes }
			}
			var pending *storage.CloseSettlementError
			if handle == nil || !errors.Is(err, openingErr) || !errors.As(err, &pending) || pending.Check() != nil || pending.State != storage.CloseSettlementPending || calls() != 1 {
				t.Fatalf("partial cleanup owner = %T, %v, calls=%d", handle, err, calls())
			}
			if id, err := storage.ReferenceNodeID(handle); id != 23 || err != nil || identity.statCalls != 0 {
				t.Fatalf("partial identity = %d, %v, stat calls=%d", id, err, identity.statCalls)
			}
			if _, err := handle.Stat(t.Context()); !errors.Is(err, openingErr) || identity.statCalls != 0 {
				t.Fatalf("partial metadata access = %v, stat calls=%d", err, identity.statCalls)
			}
			result, err := handle.CloseWithResult(t.Context())
			assertCloseSettlement(t, result, err, storage.CloseSettlementUnknown)
			if !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ECONNRESET) || calls() != 2 {
				t.Fatalf("partial lost replay = %+v, %v, calls=%d", result, err, calls())
			}
			for range 2 {
				result, err = handle.CloseWithResult(t.Context())
				if !result.Released || !result.Determined || !errors.Is(err, syscall.ENOTEMPTY) || errors.As(err, &pending) || errors.Is(err, syscall.EIO) || calls() != 3 {
					t.Fatalf("partial settled close = %+v, %v, calls=%d", result, err, calls())
				}
			}
			if id, err := storage.ReferenceNodeID(handle); id != 23 || err != nil || identity.statCalls != 0 {
				t.Fatalf("retired partial identity = %d, %v, stat calls=%d", id, err, identity.statCalls)
			}
			identity.err = errors.New("native immutable identity unavailable")
			if _, err := storage.ReferenceNodeID(handle); !errors.Is(err, identity.err) || identity.statCalls != 0 {
				t.Fatalf("native identity error was changed: %v, stat calls=%d", err, identity.statCalls)
			}
		})
	}
}

type partialBarrierFileProbe struct {
	*semanticReplayFileStub
	*partialCleanupIdentity
}

type partialBarrierReferenceProbe struct {
	*semanticReplayReferenceStub
	*partialCleanupIdentity
}

func TestPartialOpenBarrierReplayKeepsOriginalCloseSemanticError(t *testing.T) {
	for _, kind := range []string{"file", "node"} {
		t.Run(kind, func(t *testing.T) {
			session := retainedTestSession(t, nil)
			identity := &partialCleanupIdentity{id: 23}
			openingErr := errors.New("open response was not confirmed")
			var handle partialCleanupHandle
			var err error
			var calls func() int
			if kind == "file" {
				remote := &partialBarrierFileProbe{semanticReplayFileStub: &semanticReplayFileStub{fileAuthorityStub: &fileAuthorityStub{}}, partialCleanupIdentity: identity}
				opened, failure := session.wrapOpenResult(storage.OpenResult{File: remote, Attr: storage.Attr{ID: 23}}, openingErr)
				handle, err, calls = opened.File, failure, func() int { return remote.closes }
			} else {
				remote := &partialBarrierReferenceProbe{semanticReplayReferenceStub: &semanticReplayReferenceStub{barrierReferenceStub: &barrierReferenceStub{}}, partialCleanupIdentity: identity}
				opened, failure := session.openReference(t.Context(), func(context.Context) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
					return storage.NodeOpenResult{Reference: remote, Attr: storage.Attr{ID: 23}}, nil, openingErr
				})
				handle, err, calls = opened.Reference, failure, func() int { return remote.closes }
			}
			var unsettled *storage.CloseSettlementError
			if handle == nil || !errors.Is(err, openingErr) || !errors.Is(err, syscall.ENOTEMPTY) || !errors.As(err, &unsettled) || unsettled.Check() != nil || unsettled.State != storage.CloseSettlementUnknown || calls() != 1 {
				t.Fatalf("partial unknown barrier = %T, %v, calls=%d", handle, err, calls())
			}
			result, err := handle.CloseWithResult(t.Context())
			assertCloseSettlement(t, result, err, storage.CloseSettlementUnknown)
			if !errors.Is(err, syscall.ENOTEMPTY) || !errors.Is(err, syscall.ECONNRESET) || calls() != 2 {
				t.Fatalf("partial barrier lost replay = %+v, %v, calls=%d", result, err, calls())
			}
			for range 2 {
				result, err = handle.CloseWithResult(t.Context())
				if !result.Released || !result.Determined || !errors.Is(err, syscall.ENOTEMPTY) || errors.As(err, &unsettled) || errors.Is(err, syscall.EIO) || calls() != 3 {
					t.Fatalf("partial barrier settled close = %+v, %v, calls=%d", result, err, calls())
				}
			}
			if id, err := storage.ReferenceNodeID(handle); id != 23 || err != nil {
				t.Fatalf("partial barrier retired identity = %d, %v", id, err)
			}
		})
	}
}
