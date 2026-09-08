package httprest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

const (
	OpSessionEnrollment Op = "session-enrollment"
	OpSessionOpen       Op = "session-open"
	OpSessionClose      Op = "session-close"
	OpOwnerCreate       Op = "owner-create"
	OpOwnerRetire       Op = "owner-retire"
	OpLockResolve       Op = "lock-resolve"
	OpLockAcquire       Op = "lock-acquire"
	OpLockRenew         Op = "lock-renew"
	OpLockRelease       Op = "lock-release"
	OpLockCancel        Op = "lock-cancel"
	OpLockQueryAction   Op = "lock-query-action"
	OpLockQueryGrant    Op = "lock-query-grant"
	OpLockStatus        Op = "lock-status"
)

type lockStatusResponse struct {
	Status locking.Status `json:"status"`
}

type lockTicketMessage struct {
	Ticket locking.EnrollmentTicket `json:"ticket"`
}
type lockSessionRequest struct {
	Session locking.SessionID `json:"session"`
}
type lockSessionResponse struct {
	Session locking.Session `json:"session"`
}
type lockCreateOwnerRequest struct {
	Session locking.SessionID `json:"session"`
	Request locking.RequestID `json:"request"`
}
type lockOwnerResponse struct {
	Owner locking.Owner `json:"owner"`
}
type lockOwnerRequest struct {
	Owner locking.OwnerRef `json:"owner"`
}
type lockResolveRequest struct {
	Owner locking.OwnerRef `json:"owner"`
	Path  []byte           `json:"path"`
}
type lockResourceResponse struct {
	Resource locking.ResourceRef `json:"resource"`
}
type lockGrantRequest struct {
	Owner locking.OwnerRef `json:"owner"`
	Grant locking.GrantRef `json:"grant"`
}
type lockActionRequest struct {
	Owner   locking.OwnerRef  `json:"owner"`
	Request locking.RequestID `json:"request"`
}
type lockGrantResponse struct {
	Grant locking.GrantStatus `json:"grant"`
}
type lockReleaseResponse struct {
	Release locking.ReleaseResult `json:"release"`
}
type lockCancelResponse struct {
	Cancel locking.CancelResult `json:"cancel"`
}
type lockActionResponse struct {
	Action lockActionResult `json:"action"`
}

type lockAcquireRequest struct {
	Owner      locking.OwnerRef    `json:"owner"`
	Request    locking.RequestID   `json:"request"`
	Resource   locking.ResourceRef `json:"resource"`
	Mode       locking.Mode        `json:"mode"`
	TTLMillis  int64               `json:"ttlMillis"`
	WaitMillis int64               `json:"waitMillis"`
}
type lockRenewRequest struct {
	Owner     locking.OwnerRef  `json:"owner"`
	Request   locking.RequestID `json:"request"`
	Grant     locking.GrantRef  `json:"grant"`
	TTLMillis int64             `json:"ttlMillis"`
}
type lockActionReceipt struct {
	Kind           locking.ActionKind    `json:"kind"`
	Request        locking.RequestID     `json:"request"`
	Acquire        *lockAcquireRequest   `json:"acquire,omitempty"`
	Renew          *lockRenewRequest     `json:"renew,omitempty"`
	Outcome        locking.ActionOutcome `json:"outcome"`
	Code           locking.Code          `json:"code,omitempty"`
	Grant          *locking.GrantRef     `json:"grant,omitempty"`
	DeadlineMillis int64                 `json:"deadlineMillis,omitempty"`
	Revision       uint64                `json:"revision,omitempty"`
}
type lockActionResult struct {
	Recorded             bool                 `json:"recorded"`
	Receipt              lockActionReceipt    `json:"receipt"`
	Grant                *locking.GrantStatus `json:"grant,omitempty"`
	NowMillis            int64                `json:"nowMillis"`
	HistoryExpiresMillis int64                `json:"historyExpiresMillis"`
}

type lockErrorResponse struct {
	Errno    string            `json:"errno"`
	Action   *lockActionResult `json:"action,omitempty"`
	Code     locking.Code      `json:"lockCode"`
	Recorded bool              `json:"recorded"`
	Message  string            `json:"message"`
}

func lockErrorOf(err error) lockErrorResponse {
	message := err.Error()
	if len(message) > 1024 || !utf8.ValidString(message) {
		message = "lock control diagnostic cannot be represented within the wire text bound"
	}
	if typed := namespaceLockFailure(err); typed != nil {
		return lockErrorResponse{Errno: storage.ErrnoNameOf(locking.Errno(typed.Code)), Code: typed.Code, Recorded: typed.Recorded, Message: message}
	}
	return lockErrorResponse{Errno: "EIO", Code: locking.Unavailable, Message: message}
}

func (e lockErrorResponse) locking() error {
	return &locking.Error{Code: e.Code, Recorded: e.Recorded, Message: e.Message}
}

func lockDurationMillis(value time.Duration, positive bool) (int64, error) {
	if value < 0 || (positive && value == 0) || value%time.Millisecond != 0 {
		return 0, errors.New("lock durations must be whole milliseconds within the allowed range")
	}
	return value.Milliseconds(), nil
}

func lockMillisDuration(value int64, positive bool) (time.Duration, error) {
	if value < 0 || (positive && value == 0) || value > math.MaxInt64/int64(time.Millisecond) {
		return 0, errors.New("lock duration is outside the allowed millisecond range")
	}
	return time.Duration(value) * time.Millisecond, nil
}

func lockAcquireOf(r locking.AcquireRequest) (lockAcquireRequest, error) {
	ttl, err := lockDurationMillis(r.TTL, true)
	if err != nil {
		return lockAcquireRequest{}, err
	}
	wait, err := lockDurationMillis(r.Wait, false)
	if err != nil {
		return lockAcquireRequest{}, err
	}
	return lockAcquireRequest{r.Owner, r.Request, r.Resource, r.Mode, ttl, wait}, nil
}

func (r lockAcquireRequest) locking() (locking.AcquireRequest, error) {
	ttl, err := lockMillisDuration(r.TTLMillis, true)
	if err != nil {
		return locking.AcquireRequest{}, err
	}
	wait, err := lockMillisDuration(r.WaitMillis, false)
	if err != nil {
		return locking.AcquireRequest{}, err
	}
	return locking.AcquireRequest{Owner: r.Owner, Request: r.Request, Resource: r.Resource, Mode: r.Mode, TTL: ttl, Wait: wait}, nil
}

func lockRenewOf(r locking.RenewRequest) (lockRenewRequest, error) {
	ttl, err := lockDurationMillis(r.TTL, true)
	if err != nil {
		return lockRenewRequest{}, err
	}
	return lockRenewRequest{r.Owner, r.Request, r.Grant, ttl}, nil
}

func (r lockRenewRequest) locking() (locking.RenewRequest, error) {
	ttl, err := lockMillisDuration(r.TTLMillis, true)
	if err != nil {
		return locking.RenewRequest{}, err
	}
	return locking.RenewRequest{Owner: r.Owner, Request: r.Request, Grant: r.Grant, TTL: ttl}, nil
}

func lockActionOf(r locking.ActionResult) (lockActionResult, error) {
	c := r.Receipt
	wire := lockActionResult{Recorded: r.Recorded, Grant: r.Grant, NowMillis: r.NowMillis, HistoryExpiresMillis: r.HistoryExpiresMillis,
		Receipt: lockActionReceipt{Kind: c.Kind, Request: c.Request, Outcome: c.Outcome, Code: c.Code, Grant: c.Grant, DeadlineMillis: c.DeadlineMillis, Revision: c.Revision}}
	if c.Acquire != nil {
		value, err := lockAcquireOf(*c.Acquire)
		if err != nil {
			return lockActionResult{}, err
		}
		wire.Receipt.Acquire = &value
	}
	if c.Renew != nil {
		value, err := lockRenewOf(*c.Renew)
		if err != nil {
			return lockActionResult{}, err
		}
		wire.Receipt.Renew = &value
	}
	return wire, nil
}

func (r lockActionResult) locking() (locking.ActionResult, error) {
	c := r.Receipt
	value := locking.ActionResult{Recorded: r.Recorded, Grant: r.Grant, NowMillis: r.NowMillis, HistoryExpiresMillis: r.HistoryExpiresMillis,
		Receipt: locking.ActionReceipt{Kind: c.Kind, Request: c.Request, Outcome: c.Outcome, Code: c.Code, Grant: c.Grant, DeadlineMillis: c.DeadlineMillis, Revision: c.Revision}}
	if c.Acquire != nil {
		request, err := c.Acquire.locking()
		if err != nil {
			return locking.ActionResult{}, err
		}
		value.Receipt.Acquire = &request
	}
	if c.Renew != nil {
		request, err := c.Renew.locking()
		if err != nil {
			return locking.ActionResult{}, err
		}
		value.Receipt.Renew = &request
	}
	return value, nil
}

// The reflected field set requires every non-optional member, including false and zero.
// Decoder.DisallowUnknownFields alone accepts duplicate, absent, and null members.
func decodeLockJSON(data []byte, target any) error {
	if !utf8.Valid(data) {
		return errors.New("lock JSON must be UTF-8")
	}
	t := reflect.TypeOf(target)
	if t == nil || t.Kind() != reflect.Pointer || reflect.ValueOf(target).IsNil() {
		return errors.New("lock JSON destination must be a non-nil pointer")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := checkLockJSON(decoder, t.Elem()); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("lock JSON contains trailing content")
	}
	if err := json.Unmarshal(data, target); err != nil {
		return errors.New("lock JSON contains an invalid field value")
	}
	return validateLockValue(reflect.ValueOf(target).Elem())
}

func marshalLockJSON(value any) ([]byte, error) {
	if err := validateLockValue(reflect.ValueOf(value)); err != nil {
		return nil, err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, errors.New("lock control cannot be encoded")
	}
	return data, nil
}

func checkLockJSON(decoder *json.Decoder, typ reflect.Type) error {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	token, err := decoder.Token()
	if err != nil {
		return errors.New("lock JSON is incomplete or invalid")
	}
	if token == nil {
		return errors.New("lock JSON members cannot be null")
	}
	if typ == reflect.TypeOf(locking.Code("")) {
		code, ok := token.(string)
		if !ok || !validLockCode(locking.Code(code)) {
			return errors.New("lock error code is unknown")
		}
	}
	switch typ.Kind() {
	case reflect.Struct:
		if token != json.Delim('{') {
			return errors.New("lock JSON requires an object")
		}
		fields := make(map[string]reflect.StructField, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name != "-" && field.IsExported() {
				fields[name] = field
			}
		}
		seen := make(map[string]bool, len(fields))
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return errors.New("lock JSON member name is invalid")
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("lock JSON member name is invalid")
			}
			field, ok := fields[name]
			if !ok || seen[name] {
				return errors.New("lock JSON contains an unknown or duplicate member")
			}
			seen[name] = true
			if err := checkLockJSON(decoder, field.Type); err != nil {
				return err
			}
		}
		for name, field := range fields {
			if !seen[name] && !strings.Contains(field.Tag.Get("json"), ",omitempty") {
				return errors.New("lock JSON is missing a required member")
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return errors.New("lock JSON object is incomplete")
		}
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			if _, ok := token.(string); !ok {
				return errors.New("lock JSON byte strings require base64 text")
			}
			return nil
		}
		if token != json.Delim('[') {
			return errors.New("lock JSON requires an array")
		}
		count := 0
		for decoder.More() {
			count++
			if count > 16 {
				return errors.New("lock JSON array exceeds its element limit")
			}
			if err := checkLockJSON(decoder, typ.Elem()); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return errors.New("lock JSON array is incomplete")
		}
	default:
		if _, nested := token.(json.Delim); nested {
			return errors.New("lock JSON scalar has an invalid type")
		}
	}
	return nil
}

func validLockCode(code locking.Code) bool {
	switch code {
	case locking.Invalid, locking.UnsupportedTarget, locking.Conflict, locking.AlreadyHeld,
		locking.RequestMismatch, locking.Capacity, locking.Retired, locking.OutcomeUnknown,
		locking.StaleResource, locking.StaleGrant, locking.UnrelatedProof, locking.Recovering, locking.Unavailable:
		return true
	default:
		return false
	}
}

func validateLockValue(value reflect.Value) error {
	if !value.IsValid() {
		return errors.New("lock value cannot be absent")
	}
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return errors.New("lock value cannot be null")
		}
		value = value.Elem()
	}
	if value.CanInterface() {
		switch typed := value.Interface().(type) {
		case locking.SessionID, locking.OwnerID, locking.RequestID, locking.ResourceID, locking.GrantID, locking.EnrollmentTicket:
			if value.Len() == 0 || value.Len() > 512 {
				return errors.New("lock identifier exceeds its allowed length")
			}
		case locking.Code:
			if !validLockCode(typed) {
				return errors.New("lock error code is unknown")
			}
		case locking.Mode:
			if typed != locking.Shared && typed != locking.Exclusive {
				return errors.New("lock mode is unknown")
			}
		case locking.ActionKind:
			if typed != locking.AcquireAction && typed != locking.RenewAction {
				return errors.New("lock action kind is unknown")
			}
		case locking.ActionOutcome:
			switch typed {
			case locking.Pending, locking.Granted, locking.Renewed, locking.Cancelled, locking.TimedOut, locking.Rejected:
			default:
				return errors.New("lock action outcome is unknown")
			}
		case locking.GrantState:
			switch typed {
			case locking.Active, locking.Released, locking.Expired, locking.TargetGone, locking.OwnerRetired:
			default:
				return errors.New("lock grant state is unknown")
			}
		case locking.ResourceRef:
			if typed.NowMillis < 0 || typed.ExpiresMillis < typed.NowMillis || typed.HistoryExpiresMillis < typed.NowMillis {
				return errors.New("lock resource reference has inconsistent capture or history timing")
			}
		case locking.GrantRef:
			if typed.Generation == 0 {
				return errors.New("lock grant generation must be positive")
			}
		case locking.GrantStatus:
			if typed.RemainingMillis < 0 || typed.RemainingMillis > math.MaxInt64/int64(time.Millisecond) ||
				typed.DeadlineMillis <= 0 || typed.NowMillis < 0 || typed.HistoryExpiresMillis < 0 || typed.Revision == 0 {
				return errors.New("lock grant timing or revision is invalid")
			}
			if typed.State == locking.Active && (typed.DeadlineMillis < typed.NowMillis || typed.RemainingMillis > typed.DeadlineMillis-typed.NowMillis || typed.DeadlineMillis-typed.NowMillis-typed.RemainingMillis > 1) {
				return errors.New("active lock grant has inconsistent timing")
			}
			if typed.State == locking.Expired && typed.NowMillis < typed.DeadlineMillis {
				return errors.New("expired lock grant has a future deadline")
			}
			if typed.State != locking.Active && typed.RemainingMillis != 0 {
				return errors.New("inactive lock grant has remaining time")
			}
		case lockAcquireRequest:
			if _, err := typed.locking(); err != nil {
				return err
			}
		case lockRenewRequest:
			if _, err := typed.locking(); err != nil {
				return err
			}
		case lockActionReceipt:
			if err := validateLockReceipt(typed); err != nil {
				return err
			}
		case lockActionResult:
			if err := validateLockActionResult(typed); err != nil {
				return err
			}
		case locking.ReleaseResult:
			if typed.State == locking.Active || typed.State != typed.Grant.State {
				return errors.New("lock release has no matching terminal grant state")
			}
		case locking.CancelResult:
			switch typed.Outcome {
			case locking.Cancelled, locking.Granted, locking.TimedOut, locking.Rejected:
			default:
				return errors.New("lock cancellation has no terminal acquisition outcome")
			}
			if typed.Released && typed.Outcome != locking.Granted {
				return errors.New("lock cancellation released a grant without a granted acquisition")
			}
		case lockErrorResponse:
			if typed.Action != nil && (!typed.Recorded || !typed.Action.Recorded || typed.Action.Receipt.Outcome != locking.Rejected || typed.Action.Receipt.Code != typed.Code) {
				return errors.New("lock error receipt is inconsistent")
			}
			errno, ok := storage.ErrnoByName(typed.Errno)
			if !ok || errno != locking.Errno(typed.Code) {
				return errors.New("lock error classification is inconsistent")
			}
			if len(typed.Message) > 1024 {
				return errors.New("lock error message exceeds its limit")
			}
		}
	}
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if value.Int() < 0 {
			return errors.New("lock numeric field cannot be negative")
		}
	case reflect.String:
		if !utf8.ValidString(value.String()) || value.Len() > 1024 {
			return errors.New("lock text exceeds its allowed encoding or length")
		}
	case reflect.Struct:
		typ := value.Type()
		for i := 0; i < value.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() || field.Tag.Get("json") == "-" {
				continue
			}
			member := value.Field(i)
			if strings.Contains(field.Tag.Get("json"), ",omitempty") && member.IsZero() {
				continue
			}
			if err := validateLockValue(member); err != nil {
				return err
			}
		}
	case reflect.Slice:
		if value.IsNil() {
			return errors.New("lock array cannot be null")
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			if int64(value.Len()) > DefaultMaxLockControlBytes {
				return errors.New("lock byte string exceeds its byte limit")
			}
			return nil
		}
		if value.Len() > 16 {
			return errors.New("lock proof array exceeds its limit")
		}
		for i := 0; i < value.Len(); i++ {
			if err := validateLockValue(value.Index(i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateLockReceipt(receipt lockActionReceipt) error {
	if receipt.Kind == locking.AcquireAction {
		if receipt.Renew != nil || receipt.Acquire == nil && receipt.Outcome != locking.Cancelled {
			return errors.New("acquisition receipt is missing or has a different intent")
		}
		if receipt.Outcome == locking.Renewed {
			return errors.New("acquisition receipt has a renewal outcome")
		}
	} else if receipt.Kind == locking.RenewAction {
		if receipt.Acquire != nil || receipt.Renew == nil {
			return errors.New("renewal receipt is missing or has a different intent")
		}
		if receipt.Outcome == locking.Granted || receipt.Outcome == locking.TimedOut {
			return errors.New("renewal receipt has an acquisition outcome")
		}
	}
	if receipt.Acquire != nil && receipt.Acquire.Request != receipt.Request || receipt.Renew != nil && receipt.Renew.Request != receipt.Request {
		return errors.New("lock receipt request does not match its intent")
	}
	if receipt.Outcome == locking.Rejected && !validLockCode(receipt.Code) || receipt.Outcome != locking.Rejected && receipt.Code != "" {
		return errors.New("lock receipt rejection code disagrees with its outcome")
	}
	if receipt.Outcome != locking.Granted && receipt.Outcome != locking.Renewed {
		if receipt.Grant != nil || receipt.DeadlineMillis != 0 || receipt.Revision != 0 {
			return errors.New("unsuccessful lock receipt carries a granted interval")
		}
		return nil
	}
	if receipt.Grant == nil || receipt.DeadlineMillis <= 0 || receipt.Revision == 0 {
		return errors.New("successful lock receipt is missing its grant interval")
	}
	if receipt.Acquire != nil && receipt.Grant.Resource != receipt.Acquire.Resource.ID {
		return errors.New("acquisition grant belongs to a different resource")
	}
	if receipt.Renew != nil && *receipt.Grant != receipt.Renew.Grant {
		return errors.New("renewal receipt identifies a different grant")
	}
	return nil
}

func validateLockActionResult(result lockActionResult) error {
	if !result.Recorded {
		return errors.New("lock action result has no retained receipt")
	}
	receipt := result.Receipt
	success := receipt.Outcome == locking.Granted || receipt.Outcome == locking.Renewed
	if success && result.Grant == nil {
		return errors.New("successful lock action has no current grant status")
	}
	if result.Grant == nil {
		return nil
	}
	grant := result.Grant
	if receipt.Acquire != nil {
		if !success {
			return errors.New("unsuccessful acquisition has a current grant")
		}
		if grant.Ref.Resource != receipt.Acquire.Resource.ID || grant.Mode != receipt.Acquire.Mode {
			return errors.New("current acquisition grant has a different resource or mode")
		}
	}
	if receipt.Renew != nil && grant.Ref != receipt.Renew.Grant {
		return errors.New("current renewal status identifies a different grant")
	}
	if receipt.Acquire == nil && receipt.Renew == nil {
		return errors.New("lock status has no matching action intent")
	}
	if receipt.Grant != nil && grant.Ref != *receipt.Grant {
		return errors.New("current grant does not match its action receipt")
	}
	if success {
		if grant.Revision < receipt.Revision || grant.DeadlineMillis < receipt.DeadlineMillis {
			return errors.New("current lock grant precedes its acknowledged interval")
		}
		if grant.Revision == receipt.Revision && grant.DeadlineMillis != receipt.DeadlineMillis {
			return errors.New("unchanged lock revision has a different deadline")
		}
	}
	return nil
}

// ConservativeLockDeadline estimates a grant's usable local deadline from the instant
// captured before the control call. Transport delay can only shorten the usable interval.
// The estimate cannot prove current ownership; publication validates the grant again.
func ConservativeLockDeadline(requestStarted time.Time, grant locking.GrantStatus) (time.Time, error) {
	if requestStarted.IsZero() {
		return time.Time{}, errors.New("lock deadline requires a request start time")
	}
	if err := validateLockValue(reflect.ValueOf(grant)); err != nil {
		return time.Time{}, err
	}
	if grant.State != locking.Active {
		return time.Time{}, errors.New("lock grant is inactive")
	}
	return requestStarted.Add(time.Duration(grant.RemainingMillis) * time.Millisecond), nil
}
