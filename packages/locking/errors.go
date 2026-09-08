package locking

import "syscall"

type Code string

const (
	Invalid           Code = "invalid"
	UnsupportedTarget Code = "unsupportedTarget"
	Conflict          Code = "conflict"
	AlreadyHeld       Code = "alreadyHeld"
	RequestMismatch   Code = "requestMismatch"
	Capacity          Code = "capacity"
	Retired           Code = "retired"
	OutcomeUnknown    Code = "outcomeUnknown"
	StaleResource     Code = "staleResource"
	StaleGrant        Code = "staleGrant"
	UnrelatedProof    Code = "unrelatedProof"
	Recovering        Code = "recovering"
	Unavailable       Code = "unavailable"
)

type Error struct {
	Code     Code   `json:"code"`
	Recorded bool   `json:"recorded"`
	Message  string `json:"message"`
	Cause    error  `json:"-"`
}

func (e *Error) Error() string         { return "file lock: " + string(e.Code) + ": " + e.Message }
func (e *Error) Unwrap() error         { return e.Cause }
func (e *Error) Classification() error { return Errno(e.Code) }
func (e *Error) Is(target error) bool {
	if other, ok := target.(*Error); ok {
		return e.Code == other.Code
	}
	return target == Errno(e.Code)
}

func Errno(code Code) syscall.Errno {
	switch code {
	case Invalid, RequestMismatch:
		return syscall.EINVAL
	case UnsupportedTarget:
		return syscall.EOPNOTSUPP
	case Conflict, AlreadyHeld:
		return syscall.EBUSY
	case Capacity, Recovering:
		return syscall.EAGAIN
	case Retired, StaleResource, StaleGrant, UnrelatedProof:
		return syscall.ESTALE
	default:
		return syscall.EIO
	}
}

// CodeOf preserves a lock outcome only through transparent wrappers around one
// error. An enclosing classification or independent failures cannot inherit a
// nested lock refusal: their combined outcome may require recovery.
func CodeOf(err error) Code {
	for err != nil {
		if typed, ok := err.(*Error); ok {
			return typed.Code
		}
		if _, classified := err.(interface{ Classification() error }); classified {
			return Unavailable
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			if len(children) != 1 {
				return Unavailable
			}
			err = children[0]
			continue
		}
		wrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return Unavailable
		}
		err = wrapped.Unwrap()
	}
	return Unavailable
}

func fail(code Code, message string) error { return &Error{Code: code, Message: message} }

func Wrap(code Code, message string, cause error) error {
	return &Error{Code: code, Message: message, Cause: cause}
}
