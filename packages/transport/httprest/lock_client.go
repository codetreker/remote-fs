package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"syscall"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

var _ locking.Service = (*Storage)(nil)

func (s *Storage) BeginEnrollment(ctx context.Context) (locking.EnrollmentTicket, error) {
	var response lockTicketMessage
	err := s.lockControl(ctx, OpSessionEnrollment, struct{}{}, &response)
	if err != nil {
		return "", err
	}
	return response.Ticket, nil
}

func (s *Storage) OpenSession(ctx context.Context, ticket locking.EnrollmentTicket) (locking.Session, error) {
	var response lockSessionResponse
	err := s.lockControl(ctx, OpSessionOpen, lockTicketMessage{Ticket: ticket}, &response)
	if err != nil {
		return locking.Session{}, err
	}
	return response.Session, nil
}

func (s *Storage) CreateOwner(ctx context.Context, session locking.SessionID, request locking.RequestID) (locking.Owner, error) {
	var response lockOwnerResponse
	err := s.lockControl(ctx, OpOwnerCreate, lockCreateOwnerRequest{Session: session, Request: request}, &response)
	if err != nil {
		return locking.Owner{}, err
	}
	if response.Owner.Ref.Session != session {
		return locking.Owner{}, lockControlFailure(OpOwnerCreate, errors.New("owner response has a different session"), false)
	}
	return response.Owner, nil
}

func (s *Storage) RetireOwner(ctx context.Context, owner locking.OwnerRef) error {
	return s.lockControl(ctx, OpOwnerRetire, lockOwnerRequest{Owner: owner}, &struct{}{})
}

func (s *Storage) CloseSession(ctx context.Context, session locking.SessionID) error {
	return s.lockControl(ctx, OpSessionClose, lockSessionRequest{Session: session}, &struct{}{})
}

func (s *Storage) Resolve(ctx context.Context, owner locking.OwnerRef, path string) (locking.ResourceRef, error) {
	if int64(len(path)) > DefaultMaxLockControlBytes {
		return locking.ResourceRef{}, locking.Wrap(locking.Invalid, "lock path exceeds its wire byte limit", nil)
	}
	var response lockResourceResponse
	err := s.lockControl(ctx, OpLockResolve, lockResolveRequest{Owner: owner, Path: []byte(path)}, &response)
	if err != nil {
		return locking.ResourceRef{}, err
	}
	return response.Resource, nil
}

// Acquire returns the retained action result, which may still be Pending. QueryAction
// and Cancel reconcile that request without creating another acquisition.
func (s *Storage) Acquire(ctx context.Context, request locking.AcquireRequest) (locking.ActionResult, error) {
	wire, err := lockAcquireOf(request)
	if err != nil {
		return locking.ActionResult{}, locking.Wrap(locking.Invalid, "invalid lock acquisition", err)
	}
	return s.lockAction(ctx, OpLockAcquire, wire)
}

func (s *Storage) Renew(ctx context.Context, request locking.RenewRequest) (locking.ActionResult, error) {
	wire, err := lockRenewOf(request)
	if err != nil {
		return locking.ActionResult{}, locking.Wrap(locking.Invalid, "invalid lock renewal", err)
	}
	return s.lockAction(ctx, OpLockRenew, wire)
}

func (s *Storage) Release(ctx context.Context, owner locking.OwnerRef, grant locking.GrantRef) (locking.ReleaseResult, error) {
	var response lockReleaseResponse
	err := s.lockControl(ctx, OpLockRelease, lockGrantRequest{Owner: owner, Grant: grant}, &response)
	if err != nil {
		return locking.ReleaseResult{}, err
	}
	if response.Release.Grant.Ref != grant {
		return locking.ReleaseResult{}, lockControlFailure(OpLockRelease, errors.New("release response has a different grant"), false)
	}
	return response.Release, nil
}

func (s *Storage) Cancel(ctx context.Context, owner locking.OwnerRef, request locking.RequestID) (locking.CancelResult, error) {
	var response lockCancelResponse
	err := s.lockControl(ctx, OpLockCancel, lockActionRequest{Owner: owner, Request: request}, &response)
	if err != nil {
		return locking.CancelResult{}, err
	}
	return response.Cancel, nil
}

func (s *Storage) QueryAction(ctx context.Context, owner locking.OwnerRef, request locking.RequestID) (locking.ActionResult, error) {
	return s.lockAction(ctx, OpLockQueryAction, lockActionRequest{Owner: owner, Request: request})
}

func (s *Storage) QueryGrant(ctx context.Context, owner locking.OwnerRef, grant locking.GrantRef) (locking.GrantStatus, error) {
	var response lockGrantResponse
	err := s.lockControl(ctx, OpLockQueryGrant, lockGrantRequest{Owner: owner, Grant: grant}, &response)
	if err != nil {
		return locking.GrantStatus{}, err
	}
	if response.Grant.Ref != grant {
		return locking.GrantStatus{}, lockControlFailure(OpLockQueryGrant, errors.New("query response has a different grant"), false)
	}
	return response.Grant, nil
}

func (s *Storage) Status(ctx context.Context) (locking.Status, error) {
	var response lockStatusResponse
	err := s.lockControl(ctx, OpLockStatus, struct{}{}, &response)
	if err != nil {
		return locking.Status{}, err
	}
	return response.Status, nil
}

func (s *Storage) lockAction(ctx context.Context, op Op, request any) (locking.ActionResult, error) {
	var response lockActionResponse
	controlErr := s.lockControl(ctx, op, request, &response)
	if controlErr != nil {
		var typed *locking.Error
		if !errors.As(controlErr, &typed) || !response.Action.Recorded {
			return locking.ActionResult{}, controlErr
		}
	}
	value, err := response.Action.locking()
	if err != nil {
		return locking.ActionResult{}, lockControlFailure(op, err, false)
	}
	if err := matchLockReceipt(request, value.Receipt); err != nil {
		return locking.ActionResult{}, lockControlFailure(op, err, false)
	}
	return value, controlErr
}

func isReadOnlyLockControl(op Op) bool {
	return op == OpLockResolve || op == OpLockQueryAction || op == OpLockQueryGrant || op == OpLockStatus
}

func (s *Storage) lockControl(ctx context.Context, op Op, request, result any) error {
	// Admission precedes encoding, and covers request, response, and decoder retention.
	release, err := s.lockControls.acquire(ctx, retainedResponseMultiplier*DefaultMaxLockControlBytes)
	if err != nil {
		if errors.Is(err, syscall.EAGAIN) {
			return locking.Wrap(locking.Capacity, "lock control admission is full", err)
		}
		return lockControlFailure(op, err, true)
	}
	defer release()
	body, err := marshalLockJSON(request)
	if err != nil {
		return locking.Wrap(locking.Invalid, "invalid lock control request", err)
	}
	if int64(len(body)) > DefaultMaxLockControlBytes {
		return locking.Wrap(locking.Invalid, "lock control request exceeds its byte limit", nil)
	}
	u, err := (Request{Op: op}).URL(s.base)
	if err != nil {
		return lockControlFailure(op, err, true)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return lockControlFailure(op, err, true)
	}
	httpRequest.Header.Set("Content-Type", contentJSON)
	httpRequest.Header.Set("Accept", contentJSON)
	if err := ctx.Err(); err != nil {
		return lockControlFailure(op, err, true)
	}
	response, err := s.http.Do(httpRequest)
	if err != nil {
		return lockControlFailure(op, err, isReadOnlyLockControl(op))
	}
	defer response.Body.Close()
	if response.Header.Get(HeaderProtocol) != Version {
		return lockControlFailure(op, errors.New("lock response protocol version is invalid"), false)
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != StatusStorageError {
		return lockControlFailure(op, errors.New("lock response status is invalid"), false)
	}
	content, err := readWhole(response, DefaultMaxLockControlBytes)
	if err != nil {
		return lockControlFailure(op, err, isReadOnlyLockControl(op))
	}
	if response.StatusCode == StatusStorageError {
		var members map[string]json.RawMessage
		if err := json.Unmarshal(content, &members); err != nil {
			return lockControlFailure(op, err, false)
		}
		_, hasCode := members["lockCode"]
		_, hasRecorded := members["recorded"]
		if !hasCode && !hasRecorded {
			var failure struct {
				Errno   string `json:"errno"`
				Message string `json:"message"`
			}
			if err := decodeLockJSON(content, &failure); err != nil {
				return lockControlFailure(op, err, false)
			}
			errno, known := storage.ErrnoByName(failure.Errno)
			if !known || errno != syscall.EACCES && errno != syscall.EIO {
				return lockControlFailure(op, errors.New("lock authorization response errno is invalid"), false)
			}
			return &operationError{req: Request{Op: op}, errno: errno, detail: failure.Message}
		}
		var failure lockErrorResponse
		if err := decodeLockJSON(content, &failure); err != nil {
			return lockControlFailure(op, err, false)
		}
		if _, actionEndpoint := result.(*lockActionResponse); actionEndpoint && failure.Recorded && failure.Action == nil {
			return lockControlFailure(op, errors.New("recorded lock failure has no action receipt"), false)
		}
		if failure.Action != nil {
			action, ok := result.(*lockActionResponse)
			if !ok || !failure.Recorded || !failure.Action.Recorded {
				return lockControlFailure(op, errors.New("lock error receipt is inconsistent"), false)
			}
			action.Action = *failure.Action
		}
		return failure.locking()
	}
	if err := decodeLockJSON(content, result); err != nil {
		return lockControlFailure(op, err, false)
	}
	if action, ok := result.(*lockActionResponse); ok && action.Action.Receipt.Outcome == locking.Rejected {
		return lockControlFailure(op, errors.New("rejected lock action has a successful response status"), false)
	}
	return nil
}

func lockControlFailure(op Op, cause error, interruptible bool) error {
	failure := operationFailure(Request{Op: op}, cause, interruptible).(*operationError)
	// Network and JSON errors may contain peer-controlled strings or URL credentials.
	failure.detail = "lock control exchange failed"
	return failure
}

func matchLockReceipt(request any, receipt locking.ActionReceipt) error {
	matches := false
	switch intent := request.(type) {
	case lockAcquireRequest:
		decoded, err := intent.locking()
		matches = err == nil && receipt.Kind == locking.AcquireAction && receipt.Acquire != nil && *receipt.Acquire == decoded
		if receipt.Kind == locking.AcquireAction && receipt.Request == intent.Request && receipt.Outcome == locking.Cancelled && receipt.Acquire == nil && receipt.Renew == nil {
			matches = true
		}
	case lockRenewRequest:
		decoded, err := intent.locking()
		matches = err == nil && receipt.Kind == locking.RenewAction && receipt.Renew != nil && *receipt.Renew == decoded
	case lockActionRequest:
		matches = receipt.Request == intent.Request
		if receipt.Acquire != nil {
			matches = matches && receipt.Acquire.Owner == intent.Owner
		}
		if receipt.Renew != nil {
			matches = matches && receipt.Renew.Owner == intent.Owner
		}
	}
	if !matches {
		return errors.New("lock receipt does not match the requested intent")
	}
	return nil
}
