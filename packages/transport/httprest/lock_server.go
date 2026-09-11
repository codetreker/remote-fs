package httprest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	DefaultMaxLockControlBytes       int64 = 16 << 10
	DefaultMaxConcurrentLockControls       = 16
	DefaultMaxWaitingLockControls          = 16
)

func configuredLockControlAdmission(active, waiting int) *bodyAdmission {
	return newBodyAdmission(active, retainedResponseMultiplier*int64(active)*DefaultMaxLockControlBytes, waiting)
}

func checkLockControlLimits(active, waiting int) error {
	if active <= 0 || active > maximumStreams || waiting < 0 || waiting > maximumStreams {
		return fmt.Errorf("httprest: lock controls require 1..%d active calls and 0..%d waiters", maximumStreams, maximumStreams)
	}
	return nil
}

func isLockControl(op Op) bool {
	switch op {
	case OpSessionEnrollment, OpSessionOpen, OpSessionClose, OpOwnerCreate, OpOwnerRetire,
		OpLockResolve, OpLockAcquire, OpLockRenew, OpLockRelease, OpLockCancel,
		OpLockQueryAction, OpLockQueryGrant, OpLockStatus:
		return true
	default:
		return false
	}
}

func init() {
	for _, op := range []Op{OpSessionEnrollment, OpSessionOpen, OpSessionClose, OpOwnerCreate,
		OpOwnerRetire, OpLockResolve, OpLockAcquire, OpLockRenew, OpLockRelease, OpLockCancel,
		OpLockQueryAction, OpLockQueryGrant, OpLockStatus} {
		ops[op] = opSpec{method: http.MethodPost, body: contentJSON}
	}
}

func (h *Handler) serveLockControl(w http.ResponseWriter, r *http.Request, req Request) {
	if len(r.Header.Values(HeaderMutationScope)) != 0 {
		h.writeLockFault(w, http.StatusBadRequest, "mutation scope is invalid on lock controls")
		return
	}
	if r.Header.Get("Content-Type") != contentJSON {
		h.writeLockFault(w, http.StatusUnsupportedMediaType, "lock controls require application/json")
		return
	}
	if r.ContentLength > DefaultMaxLockControlBytes {
		h.writeLockFault(w, http.StatusRequestEntityTooLarge, "lock control body exceeds its bound")
		return
	}
	release, err := h.lockControls.acquire(r.Context(), retainedResponseMultiplier*DefaultMaxLockControlBytes)
	if err != nil {
		code := locking.Unavailable
		if errors.Is(err, syscall.EAGAIN) {
			code = locking.Capacity
		}
		h.writeLockError(w, locking.Wrap(code, "lock control admission failed", err))
		return
	}
	defer release()
	body, err := readAtMost(r.Body, DefaultMaxLockControlBytes)
	if err != nil || r.ContentLength >= 0 && int64(len(body)) != r.ContentLength {
		status := http.StatusBadRequest
		if errors.Is(err, errBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		h.writeLockFault(w, status, "lock control body did not arrive whole")
		return
	}
	response, err := h.dispatchLockControl(r.Context(), req.Op, body)
	if err != nil {
		if response, ok := authorizationResponse(err); ok {
			h.writeLockJSON(w, StatusStorageError, response)
			return
		}
		if errors.Is(err, errInvalidLockRequest) {
			h.writeLockFault(w, http.StatusBadRequest, "invalid lock control request")
			return
		}
		failure := lockErrorOf(err)
		if action, ok := response.(lockActionResponse); ok && failure.Recorded && action.Action.Recorded && action.Action.Receipt.Outcome == locking.Rejected && action.Action.Receipt.Code == failure.Code {
			failure.Action = &action.Action
		}
		h.writeLockJSON(w, StatusStorageError, failure)
		return
	}
	h.writeLockJSON(w, http.StatusOK, response)
}

func (h *Handler) dispatchLockControl(ctx context.Context, op Op, body []byte) (any, error) {
	decode := func(value any) error {
		if err := decodeLockJSON(body, value); err != nil {
			return fmt.Errorf("%w: %w", errInvalidLockRequest, err)
		}
		return nil
	}
	switch op {
	case OpSessionEnrollment:
		if err := decode(&struct{}{}); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockSessionEnrollment}); err != nil {
			return nil, err
		}
		value, err := h.locks.BeginEnrollment(ctx)
		return lockTicketMessage{Ticket: value}, err
	case OpSessionOpen:
		var request lockTicketMessage
		if err := decode(&request); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockSessionOpen}); err != nil {
			return nil, err
		}
		value, err := h.locks.OpenSession(ctx, request.Ticket)
		return lockSessionResponse{Session: value}, err
	case OpSessionClose:
		var request lockSessionRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockSessionClose}); err != nil {
			return nil, err
		}
		return struct{}{}, h.locks.CloseSession(ctx, request.Session)
	case OpOwnerCreate:
		var request lockCreateOwnerRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockOwnerCreate}); err != nil {
			return nil, err
		}
		value, err := h.locks.CreateOwner(ctx, request.Session, request.Request)
		return lockOwnerResponse{Owner: value}, err
	case OpOwnerRetire:
		var request lockOwnerRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockOwnerRetire}); err != nil {
			return nil, err
		}
		return struct{}{}, h.locks.RetireOwner(ctx, request.Owner)
	case OpLockResolve:
		var request lockResolveRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		path, err := storage.CleanPath(string(request.Path))
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidLockRequest, err)
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockResolve}); err != nil {
			return nil, err
		}
		value, err := h.locks.Resolve(ctx, request.Owner, path)
		return lockResourceResponse{Resource: value}, err
	case OpLockAcquire:
		var request lockAcquireRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		intent, err := request.locking()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidLockRequest, err)
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockAcquire}); err != nil {
			return nil, err
		}
		value, actionErr := h.locks.Acquire(ctx, intent)
		if actionErr != nil && !value.Recorded {
			return nil, actionErr
		}
		wire, encodeErr := lockActionOf(value)
		if encodeErr != nil {
			return nil, encodeErr
		}
		return lockActionResponse{Action: wire}, actionErr
	case OpLockRenew:
		var request lockRenewRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		intent, err := request.locking()
		if err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidLockRequest, err)
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockRenew}); err != nil {
			return nil, err
		}
		value, actionErr := h.locks.Renew(ctx, intent)
		if actionErr != nil && !value.Recorded {
			return nil, actionErr
		}
		wire, encodeErr := lockActionOf(value)
		if encodeErr != nil {
			return nil, encodeErr
		}
		return lockActionResponse{Action: wire}, actionErr
	case OpLockRelease:
		var request lockGrantRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockRelease}); err != nil {
			return nil, err
		}
		value, err := h.locks.Release(ctx, request.Owner, request.Grant)
		return lockReleaseResponse{Release: value}, err
	case OpLockCancel:
		var request lockActionRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockCancel}); err != nil {
			return nil, err
		}
		value, err := h.locks.Cancel(ctx, request.Owner, request.Request)
		return lockCancelResponse{Cancel: value}, err
	case OpLockQueryAction:
		var request lockActionRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockQueryAction}); err != nil {
			return nil, err
		}
		value, actionErr := h.locks.QueryAction(ctx, request.Owner, request.Request)
		if actionErr != nil && !value.Recorded {
			return nil, actionErr
		}
		wire, encodeErr := lockActionOf(value)
		if encodeErr != nil {
			return nil, encodeErr
		}
		return lockActionResponse{Action: wire}, actionErr
	case OpLockQueryGrant:
		var request lockGrantRequest
		if err := decode(&request); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockQueryGrant}); err != nil {
			return nil, err
		}
		value, err := h.locks.QueryGrant(ctx, request.Owner, request.Grant)
		return lockGrantResponse{Grant: value}, err
	case OpLockStatus:
		if err := decode(&struct{}{}); err != nil {
			return nil, err
		}
		if err := h.authorize(ctx, authz.AccessRequest{Operation: authz.LockStatus}); err != nil {
			return nil, err
		}
		service, ok := h.locks.(interface {
			Status(context.Context) (locking.Status, error)
		})
		if !ok {
			return nil, locking.Wrap(locking.Unavailable, "authority status is unavailable", nil)
		}
		value, err := service.Status(ctx)
		return lockStatusResponse{Status: value}, err
	default:
		return nil, locking.Wrap(locking.Invalid, "unknown lock control", nil)
	}
}

var errInvalidLockRequest = errors.New("invalid lock control request")

func (h *Handler) writeLockFault(w http.ResponseWriter, status int, message string) {
	h.writeLockJSON(w, status, ErrorResponse{Message: message})
}

func (h *Handler) writeLockError(w http.ResponseWriter, err error) {
	h.writeLockJSON(w, StatusStorageError, lockErrorOf(err))
}

func (h *Handler) writeLockJSON(w http.ResponseWriter, status int, value any) {
	body, err := marshalLockJSON(value)
	if err != nil || int64(len(body)) > DefaultMaxLockControlBytes {
		status = http.StatusInternalServerError
		body = []byte(`{"errno":"EIO","lockCode":"unavailable","recorded":false,"message":"lock response violates the wire contract"}`)
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Content-Type", contentJSON)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
