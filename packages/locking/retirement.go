package locking

import "context"

// Quiescence reports retained authority obligations without renewing history.
// Resources include live references, native pins and pins being released.
type Quiescence struct {
	Sessions                int
	Tickets                 int
	Owners                  int
	Resources               int
	Actions                 int
	Grants                  int
	Queued                  int
	Publications            int
	NativeCalls             int
	RecoveryRemainingMillis int64
	Reserved                bool
}

func (q Quiescence) Busy() bool {
	return q.Sessions != 0 || q.Tickets != 0 || q.Owners != 0 || q.Resources != 0 ||
		q.Actions != 0 || q.Grants != 0 || q.Queued != 0 || q.Publications != 0 ||
		q.NativeCalls != 0 || q.RecoveryRemainingMillis != 0
}

func (a *Authority) quiescenceLocked() Quiescence {
	now := a.clock.Now()
	a.expireSessionsLocked(now)
	remaining := a.recoveryUntil.Sub(now)
	if remaining < 0 {
		remaining = 0
	}
	// A sub-millisecond promise still prevents retirement.
	millis := remaining.Milliseconds()
	if remaining > 0 && millis == 0 {
		millis = 1
	}
	return Quiescence{Sessions: len(a.sessions), Tickets: len(a.tickets), Owners: len(a.owners),
		Resources: len(a.resources), Actions: a.actions, Grants: a.grants, Queued: a.queued,
		Publications: a.publications, NativeCalls: a.nativeCalls,
		RecoveryRemainingMillis: millis, Reserved: a.retirementReserved}
}

// Quiescence reports the retained obligations checked by ReserveRetirement.
func (a *Authority) Quiescence(ctx context.Context) (Quiescence, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Quiescence{}, err
	}
	return a.quiescenceLocked(), nil
}

// CheckRetirement checks authority health for host retirement reads after
// ReserveRetirement. The reservation stays held and ordinary capability use,
// enrollment and publication remain rejected.
func (a *Authority) CheckRetirement(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.closed {
		return fail(Retired, "authority is closed")
	}
	if a.fenced != nil {
		return Wrap(Unavailable, "authority requires recovery", a.fenced)
	}
	return nil
}

// ReserveRetirement atomically proves quiescence and prevents new admission.
// Busy leaves the authority and all existing capabilities available. The host
// must independently drain its own references and request admission gate.
func (a *Authority) ReserveRetirement(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.healthyLocked(ctx, false); err != nil {
		return err
	}
	if a.quiescenceLocked().Busy() {
		return fail(Conflict, "authority retains live capabilities or work")
	}
	a.retirementReserved = true
	return nil
}

// CancelRetirement reopens admission after a host abandons its reservation.
// A closed or fenced authority keeps its existing failure behavior.
func (a *Authority) CancelRetirement() {
	a.mu.Lock()
	a.retirementReserved = false
	a.mu.Unlock()
}
