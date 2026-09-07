package locking

import (
	"context"
	"time"
)

// Native callbacks run under the backend's final target and observation ordering.
// Discover transfers a retained native pin only when its callback returns true.
// Guard must revalidate that the same logical file is live and supported.
// Forget releases one adopted pin after every reference and live action drains.
// No callback or native method is invoked while holding the authority state mutex.
type Native interface {
	Discover(context.Context, string, func(BackendKey) (bool, error)) error
	Guard(context.Context, BackendKey, func() error) error
	Forget(context.Context, BackendKey) error
}

// Persistence is opened only after exclusive workspace ownership is acquired.
// RaiseMaxLease durably advances validated paired evidence and never decreases it.
// RecoveryStart is that ownership acquisition's monotonic timestamp.
type Persistence interface {
	MaxLease(context.Context) (time.Duration, error)
	RaiseMaxLease(context.Context, time.Duration) error
	RecoveryStart() time.Time
}

// Clock must preserve monotonic elapsed time across Now and After.
type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

type Options struct {
	MaxSessions            int
	MaxTickets             int
	MaxOwners              int
	MaxResources           int
	MaxActions             int
	MaxGrants              int
	MaxQueued              int
	OwnersPerSession       int
	OwnerActionsPerSession int
	ActionsPerOwner        int
	GrantsPerOwner         int
	QueuedPerOwner         int
	QueuedPerResource      int
	MaxProofs              int
	MaxRequestBytes        int
	MaxLease               time.Duration
	MaxWait                time.Duration
	TicketTTL              time.Duration
	SessionIdle            time.Duration
	ResourceTTL            time.Duration
	Clock                  Clock
}

func DefaultOptions() Options {
	return Options{
		MaxSessions: 1024, MaxTickets: 1024, MaxOwners: 4096,
		MaxResources: 4096, MaxActions: 262144, MaxGrants: 8192, MaxQueued: 4096,
		OwnersPerSession: 64, OwnerActionsPerSession: 256, ActionsPerOwner: 1024,
		GrantsPerOwner: 64, QueuedPerOwner: 64, QueuedPerResource: 128,
		MaxProofs: 16, MaxRequestBytes: 128,
		MaxLease: time.Minute, MaxWait: time.Minute, TicketTTL: time.Minute,
		SessionIdle: 10 * time.Minute, ResourceTTL: time.Minute,
	}
}

func (o Options) Validate() error {
	for _, n := range []int{o.MaxSessions, o.MaxTickets, o.MaxOwners, o.MaxResources,
		o.MaxActions, o.MaxGrants, o.MaxQueued, o.OwnersPerSession, o.OwnerActionsPerSession,
		o.ActionsPerOwner, o.GrantsPerOwner, o.QueuedPerOwner, o.QueuedPerResource,
		o.MaxProofs, o.MaxRequestBytes} {
		if n <= 0 {
			return fail(Invalid, "all capacity limits must be positive")
		}
	}
	for _, d := range []time.Duration{o.MaxLease, o.MaxWait, o.TicketTTL, o.SessionIdle, o.ResourceTTL} {
		if d < time.Millisecond {
			return fail(Invalid, "all durations must be at least one millisecond")
		}
	}
	if o.SessionIdle < o.MaxLease || o.SessionIdle < o.MaxWait {
		return fail(Invalid, "session idle lifetime must cover maximum lease and wait")
	}
	return nil
}

type Status struct {
	Authority               string `json:"authority"`
	NowMillis               int64  `json:"nowMillis"`
	Recovering              bool   `json:"recovering"`
	RecoveryRemainingMillis int64  `json:"recoveryRemainingMillis"`
	Sessions                int    `json:"sessions"`
	Owners                  int    `json:"owners"`
	Resources               int    `json:"resources"`
	Actions                 int    `json:"actions"`
	Grants                  int    `json:"grants"`
	Queued                  int    `json:"queued"`
	Unavailable             bool   `json:"unavailable"`
}

type StatusService interface {
	Status(context.Context) (Status, error)
}

func CloneScope(scope MutationScope) MutationScope {
	scope.Grants = append([]GrantRef(nil), scope.Grants...)
	return scope
}

func ValidateScope(scope MutationScope, maxProofs int) error {
	if maxProofs <= 0 || len(scope.Grants) > maxProofs {
		return fail(Invalid, "mutation proof limit exceeded")
	}
	if (scope.Owner.Session == "") != (scope.Owner.Owner == "") {
		return fail(Invalid, "incomplete mutation owner")
	}
	if len(scope.Grants) > 0 && scope.Owner.Owner == "" {
		return fail(Invalid, "mutation proofs require an owner")
	}
	if len(scope.Owner.Session) > 512 || len(scope.Owner.Owner) > 512 {
		return fail(Invalid, "mutation owner capability exceeds the byte limit")
	}
	seen := make(map[GrantID]bool, len(scope.Grants))
	for _, g := range scope.Grants {
		if g.ID == "" || g.Resource == "" || len(g.ID) > 512 || len(g.Resource) > 512 || g.Generation == 0 || seen[g.ID] {
			return fail(Invalid, "invalid or duplicate mutation proof")
		}
		seen[g.ID] = true
	}
	return nil
}
