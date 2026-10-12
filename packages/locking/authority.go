package locking

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Authority struct {
	mu                      sync.Mutex
	raiseMu                 sync.Mutex
	options                 Options
	native                  Native
	persistence             Persistence
	clock                   Clock
	start                   time.Time
	recoveryUntil           time.Time
	watermark               time.Duration
	incarnation             string
	secret                  [32]byte
	sessions                map[SessionID]*sessionRecord
	owners                  map[OwnerID]*ownerRecord
	tickets                 map[EnrollmentTicket]ticketRecord
	resources               map[ResourceID]*resourceRecord
	keys                    map[BackendKey]*resourceRecord
	lastGeneration          uint64
	actions, grants, queued int
	publications            int
	publicationDone         chan struct{}
	nativeCalls             int
	nativeDone              chan struct{}
	closeDone               chan struct{}
	closeErr                error
	fenced                  error
	closed                  bool
	stop                    chan struct{}
	workerContext           context.Context
	cancelWorkers           context.CancelFunc
	wake                    chan struct{}
	wg                      sync.WaitGroup
}

type ticketRecord struct {
	session SessionID
	expires time.Time
}
type sessionRecord struct {
	id      SessionID
	expires time.Time
	owners  map[OwnerID]*ownerRecord
	creates map[RequestID]OwnerID
}
type ownerRecord struct {
	ref                OwnerRef
	session            *sessionRecord
	actions            map[RequestID]*actionRecord
	grants             map[GrantID]*grantRecord
	liveGrants, queued int
}
type actionRecord struct {
	owner      *ownerRecord
	receipt    ActionReceipt
	resource   *resourceRecord
	waitUntil  time.Time
	grant      *grantRecord
	cancel     *CancelResult
	tombstone  bool
	processing bool
	failure    error
}
type grantRecord struct {
	owner    *ownerRecord
	resource *resourceRecord
	ref      GrantRef
	mode     Mode
	state    GrantState
	deadline time.Time
	revision uint64
	release  *ReleaseResult
}
type resourceRecord struct {
	id             ResourceID
	key            BackendKey
	referenceUntil time.Time
	grants         map[GrantID]*grantRecord
	queue          []*actionRecord
	busy           bool
	busyDone       chan struct{}
	retired        bool
	worker         bool
	holds          int
	forgetting     bool
	wake           chan struct{}
}

func New(ctx context.Context, options Options, native Native, persistence Persistence) (*Authority, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	if native == nil || persistence == nil {
		return nil, fail(Invalid, "native backend and persistence are required")
	}
	clock := options.Clock
	if clock == nil {
		clock = systemClock{}
	}
	watermark, err := persistence.MaxLease(ctx)
	if err != nil {
		return nil, Wrap(Unavailable, "read lease recovery evidence", err)
	}
	if watermark < 0 || persistence.RecoveryStart().IsZero() {
		return nil, fail(Invalid, "invalid lease recovery interval")
	}
	a := &Authority{
		options: options, native: native, persistence: persistence, clock: clock,
		start: clock.Now(), recoveryUntil: persistence.RecoveryStart().Add(watermark), watermark: watermark,
		sessions: make(map[SessionID]*sessionRecord), owners: make(map[OwnerID]*ownerRecord),
		tickets: make(map[EnrollmentTicket]ticketRecord), resources: make(map[ResourceID]*resourceRecord),
		keys: make(map[BackendKey]*resourceRecord), stop: make(chan struct{}), wake: make(chan struct{}, 1),
	}
	if _, err := rand.Read(a.secret[:]); err != nil {
		return nil, Wrap(Unavailable, "create authority secret", err)
	}
	var incarnation [16]byte
	if _, err := rand.Read(incarnation[:]); err != nil {
		return nil, Wrap(Unavailable, "create authority incarnation", err)
	}
	a.incarnation = base64.RawURLEncoding.EncodeToString(incarnation[:])
	a.workerContext, a.cancelWorkers = context.WithCancel(context.Background())
	a.wg.Add(1)
	go a.maintain()
	return a, nil
}

func (a *Authority) token(kind, parent, extra string) string {
	var nonce [16]byte
	// crypto/rand.Read always fills its input and terminates the process if the system RNG fails.
	rand.Read(nonce[:])
	payload := kind + "." + a.incarnation + "." + base64.RawURLEncoding.EncodeToString(nonce[:]) + "." + extra
	mac := hmac.New(sha256.New, a.secret[:])
	mac.Write([]byte(parent))
	mac.Write([]byte{0})
	mac.Write([]byte(payload))
	return payload + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func (a *Authority) verify(token, kind, parent string) (string, error) {
	if len(token) > 512 {
		return "", fail(Invalid, "invalid capability")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 5 || parts[0] != kind {
		return "", fail(Invalid, "invalid capability")
	}
	if parts[1] != a.incarnation {
		return "", fail(Retired, "authority incarnation is retired")
	}
	mac := hmac.New(sha256.New, a.secret[:])
	mac.Write([]byte(parent))
	mac.Write([]byte{0})
	mac.Write([]byte(strings.Join(parts[:4], ".")))
	signature, err := base64.RawURLEncoding.DecodeString(parts[4])
	if err != nil || !hmac.Equal(signature, mac.Sum(nil)) {
		return "", fail(Invalid, "invalid capability")
	}
	return parts[3], nil
}

func (a *Authority) tick(t time.Time) int64 { return t.Sub(a.start).Milliseconds() }
func (a *Authority) healthyLocked(ctx context.Context, admission bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.closed {
		return fail(Retired, "authority is closed")
	}
	if a.fenced != nil {
		return Wrap(Unavailable, "authority requires recovery", a.fenced)
	}
	if admission && a.clock.Now().Before(a.recoveryUntil) {
		return fail(Recovering, "lease recovery interval has not elapsed")
	}
	return nil
}

func (a *Authority) ownerLocked(ref OwnerRef) (*ownerRecord, error) {
	if _, err := a.verify(string(ref.Session), "s", ""); err != nil {
		return nil, err
	}
	if _, err := a.verify(string(ref.Owner), "o", string(ref.Session)); err != nil {
		return nil, err
	}
	o := a.owners[ref.Owner]
	if o == nil || o.ref != ref {
		return nil, fail(Retired, "owner history is retired")
	}
	return o, nil
}

func (a *Authority) sessionLocked(id SessionID) (*sessionRecord, error) {
	if _, err := a.verify(string(id), "s", ""); err != nil {
		return nil, err
	}
	s := a.sessions[id]
	if s == nil {
		return nil, fail(Retired, "session history is retired")
	}
	return s, nil
}

func (a *Authority) touchLocked(o *ownerRecord) { a.touchSessionLocked(o.session) }
func (a *Authority) touchSessionLocked(s *sessionRecord) {
	until := a.clock.Now().Add(a.options.SessionIdle)
	if until.After(s.expires) {
		s.expires = until
	}
}

func (a *Authority) signalLocked(r *resourceRecord) {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}
func (a *Authority) wakeMaintenance() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

func (a *Authority) Status(ctx context.Context) (Status, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Status{}, err
	}
	now := a.clock.Now()
	remaining := a.recoveryUntil.Sub(now)
	if remaining < 0 {
		remaining = 0
	}
	return Status{Authority: a.incarnation, NowMillis: a.tick(now), Recovering: remaining > 0,
		RecoveryRemainingMillis: remaining.Milliseconds(), Sessions: len(a.sessions), Owners: len(a.owners),
		Resources: len(a.resources), Actions: a.actions, Grants: a.grants, Queued: a.queued, Unavailable: a.closed || a.fenced != nil}, nil
}

func (a *Authority) raise(ctx context.Context, ttl time.Duration) error {
	a.raiseMu.Lock()
	defer a.raiseMu.Unlock()
	a.mu.Lock()
	err := a.healthyLocked(ctx, false)
	current := a.watermark
	a.mu.Unlock()
	if err != nil {
		return err
	}
	if ttl <= current {
		return nil
	}
	if err := a.persistence.RaiseMaxLease(ctx, ttl); err != nil {
		return Wrap(Unavailable, "persist lease duration", err)
	}
	a.mu.Lock()
	a.watermark = ttl
	a.mu.Unlock()
	return nil
}

func requestValid(id RequestID, max int) bool { return len(id) > 0 && len(id) <= max }

func (a *Authority) beginNative(ctx context.Context) (context.Context, func(), error) {
	a.mu.Lock()
	if err := a.healthyLocked(ctx, false); err != nil {
		a.mu.Unlock()
		return nil, nil, err
	}
	if a.nativeCalls == 0 {
		a.nativeDone = make(chan struct{})
	}
	a.nativeCalls++
	a.mu.Unlock()
	derived, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(a.workerContext, cancel)
	return derived, func() {
		stop()
		cancel()
		a.mu.Lock()
		a.nativeCalls--
		if a.nativeCalls == 0 {
			close(a.nativeDone)
		}
		a.mu.Unlock()
	}, nil
}

// Close stops admission, cancels waiting native work, and drains admitted I/O
// before releasing native pins. Concurrent calls share the same cleanup result.
// An uncertain publication or pin release remains an error after closure.
func (a *Authority) Close() error {
	a.mu.Lock()
	if a.closed {
		done := a.closeDone
		a.mu.Unlock()
		<-done
		a.mu.Lock()
		result := a.closeErr
		a.mu.Unlock()
		return result
	}
	a.closed = true
	a.closeDone = make(chan struct{})
	a.cancelWorkers()
	close(a.stop)
	for _, r := range a.resources {
		a.signalLocked(r)
	}
	var nativeDone, publicationDone <-chan struct{}
	if a.nativeCalls != 0 {
		nativeDone = a.nativeDone
	}
	if a.publications != 0 {
		publicationDone = a.publicationDone
	}
	a.mu.Unlock()
	if nativeDone != nil {
		<-nativeDone
	}
	if publicationDone != nil {
		<-publicationDone
	}
	a.wg.Wait()
	a.mu.Lock()
	resources := a.resources
	a.resources = make(map[ResourceID]*resourceRecord)
	a.keys = make(map[BackendKey]*resourceRecord)
	a.mu.Unlock()
	var err error
	for _, r := range resources {
		if r.forgetting {
			continue
		}
		err = errors.Join(err, a.native.Forget(context.Background(), r.key))
	}
	a.mu.Lock()
	a.closeErr = errors.Join(a.fenced, err)
	result := a.closeErr
	close(a.closeDone)
	a.mu.Unlock()
	return result
}

func (a *Authority) ticketExpiry(extra string) (time.Time, error) {
	n, err := strconv.ParseInt(extra, 10, 64)
	if err != nil || n < 0 || n > int64((1<<63-1)/time.Millisecond) {
		return time.Time{}, fail(Invalid, "invalid enrollment expiry")
	}
	return a.start.Add(time.Duration(n) * time.Millisecond), nil
}
