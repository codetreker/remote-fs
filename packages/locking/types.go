// Package locking provides bounded explicit file leases and final publication authorization.
package locking

import (
	"context"
	"time"
)

type SessionID string
type OwnerID string
type RequestID string
type ResourceID string
type GrantID string
type EnrollmentTicket string
type BackendKey string
type Mode string

const (
	Shared    Mode = "S"
	Exclusive Mode = "X"
)

type OwnerRef struct {
	Session SessionID `json:"session"`
	Owner   OwnerID   `json:"owner"`
}

type ResourceRef struct {
	ID                   ResourceID `json:"id"`
	ExpiresMillis        int64      `json:"expiresMillis"`
	NowMillis            int64      `json:"nowMillis"`
	HistoryExpiresMillis int64      `json:"historyExpiresMillis"`
}

type GrantRef struct {
	ID         GrantID    `json:"id"`
	Resource   ResourceID `json:"resource"`
	Generation uint64     `json:"generation"`
}

type MutationScope struct {
	Owner  OwnerRef   `json:"owner"`
	Grants []GrantRef `json:"grants"`
}

type Session struct {
	ID                   SessionID `json:"id"`
	Authority            string    `json:"authority"`
	HistoryExpiresMillis int64     `json:"historyExpiresMillis"`
	NowMillis            int64     `json:"nowMillis"`
	Retired              bool      `json:"retired"`
}

type Owner struct {
	Ref                  OwnerRef `json:"ref"`
	ActionCapacity       int      `json:"actionCapacity"`
	HistoryExpiresMillis int64    `json:"historyExpiresMillis"`
	NowMillis            int64    `json:"nowMillis"`
	Retired              bool     `json:"retired"`
}

type AcquireRequest struct {
	Owner    OwnerRef      `json:"owner"`
	Request  RequestID     `json:"request"`
	Resource ResourceRef   `json:"resource"`
	Mode     Mode          `json:"mode"`
	TTL      time.Duration `json:"ttl"`
	Wait     time.Duration `json:"wait"`
}

type RenewRequest struct {
	Owner   OwnerRef      `json:"owner"`
	Request RequestID     `json:"request"`
	Grant   GrantRef      `json:"grant"`
	TTL     time.Duration `json:"ttl"`
}

type ActionKind string
type ActionOutcome string
type GrantState string

const (
	AcquireAction ActionKind    = "acquire"
	RenewAction   ActionKind    = "renew"
	Pending       ActionOutcome = "pending"
	Granted       ActionOutcome = "granted"
	Renewed       ActionOutcome = "renewed"
	Cancelled     ActionOutcome = "cancelled"
	TimedOut      ActionOutcome = "timedOut"
	Rejected      ActionOutcome = "rejected"
	Active        GrantState    = "active"
	Released      GrantState    = "released"
	Expired       GrantState    = "expired"
	TargetGone    GrantState    = "targetGone"
	OwnerRetired  GrantState    = "ownerRetired"
)

// ActionReceipt is immutable once Outcome becomes terminal. ActionResult.Grant
// reports the current grant state. An unseen cancellation has no Acquire intent.
type ActionReceipt struct {
	Kind           ActionKind      `json:"kind"`
	Request        RequestID       `json:"request"`
	Acquire        *AcquireRequest `json:"acquire,omitempty"`
	Renew          *RenewRequest   `json:"renew,omitempty"`
	Outcome        ActionOutcome   `json:"outcome"`
	Code           Code            `json:"code,omitempty"`
	Grant          *GrantRef       `json:"grant,omitempty"`
	DeadlineMillis int64           `json:"deadlineMillis,omitempty"`
	Revision       uint64          `json:"revision,omitempty"`
}

type GrantStatus struct {
	Ref            GrantRef   `json:"ref"`
	Mode           Mode       `json:"mode"`
	State          GrantState `json:"state"`
	Revision       uint64     `json:"revision"`
	DeadlineMillis int64      `json:"deadlineMillis"`
	// RemainingMillis floors the unrounded remaining interval. A client anchors
	// this interval to request-send time; response delivery cannot extend it.
	RemainingMillis      int64 `json:"remainingMillis"`
	NowMillis            int64 `json:"nowMillis"`
	HistoryExpiresMillis int64 `json:"historyExpiresMillis"`
}

type ActionResult struct {
	Recorded             bool          `json:"recorded"`
	Receipt              ActionReceipt `json:"receipt"`
	Grant                *GrantStatus  `json:"grant,omitempty"`
	NowMillis            int64         `json:"nowMillis"`
	HistoryExpiresMillis int64         `json:"historyExpiresMillis"`
}

type ReleaseResult struct {
	// State is the first release result; Grant reports current lifecycle state.
	State                GrantState  `json:"state"`
	Grant                GrantStatus `json:"grant"`
	NowMillis            int64       `json:"nowMillis"`
	HistoryExpiresMillis int64       `json:"historyExpiresMillis"`
}

type CancelResult struct {
	// Outcome is the acquisition outcome at cancellation. Cancelling a granted
	// acquisition releases its grant without changing the original receipt.
	Outcome              ActionOutcome `json:"outcome"`
	Released             bool          `json:"released"`
	NowMillis            int64         `json:"nowMillis"`
	HistoryExpiresMillis int64         `json:"historyExpiresMillis"`
}

type Service interface {
	BeginEnrollment(context.Context) (EnrollmentTicket, error)
	OpenSession(context.Context, EnrollmentTicket) (Session, error)
	CreateOwner(context.Context, SessionID, RequestID) (Owner, error)
	RetireOwner(context.Context, OwnerRef) error
	CloseSession(context.Context, SessionID) error
	Resolve(context.Context, OwnerRef, string) (ResourceRef, error)
	// Acquire returns Pending after enrollment when a conflicting holder exists.
	// Wait limits the retained intent's lifetime, not the duration of this call.
	// A recorded rejection returns both its receipt and a recorded Error.
	Acquire(context.Context, AcquireRequest) (ActionResult, error)
	Renew(context.Context, RenewRequest) (ActionResult, error)
	Release(context.Context, OwnerRef, GrantRef) (ReleaseResult, error)
	Cancel(context.Context, OwnerRef, RequestID) (CancelResult, error)
	QueryAction(context.Context, OwnerRef, RequestID) (ActionResult, error)
	QueryGrant(context.Context, OwnerRef, GrantRef) (GrantStatus, error)
}

type MutationKind string

const (
	WriteMutation   MutationKind = "write"
	SetAttrMutation MutationKind = "setAttr"
	RemoveMutation  MutationKind = "remove"
	RenameMutation  MutationKind = "rename"
	CreateMutation  MutationKind = "create"
)

type Publication struct {
	Kind    MutationKind
	Targets []BackendKey
	Scope   MutationScope
}

type PublicationOutcome struct {
	Known   bool
	Retired []BackendKey
	Changed bool
	Err     error
}

// Publish is entered under the backend's final target and observation ordering.
// The callback performs only the final transition; staging precedes this call.
type PublicationAuthority interface {
	Publish(context.Context, Publication, func() PublicationOutcome) error
}

type scopeKey struct{}

func HasScope(ctx context.Context) bool {
	_, ok := ctx.Value(scopeKey{}).(MutationScope)
	return ok
}

// WithScope takes an immutable copy. It does not assert that the proofs are live.
func WithScope(ctx context.Context, scope MutationScope) context.Context {
	return context.WithValue(ctx, scopeKey{}, CloneScope(scope))
}

func ScopeFromContext(ctx context.Context) MutationScope {
	scope, _ := ctx.Value(scopeKey{}).(MutationScope)
	return CloneScope(scope)
}
