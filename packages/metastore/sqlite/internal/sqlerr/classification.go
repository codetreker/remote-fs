// Package sqlerr preserves SQLite fault causes and their filesystem classification.
package sqlerr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
	"modernc.org/sqlite"
)

// Failure preserves cancellation and known operation errors. An unclassified database
// failure or an expired deadline cannot establish a filesystem result and is EIO.
func Failure(err error) error {
	if err == nil || storage.ErrnoOf(err) != syscall.EIO || errors.Is(err, syscall.EIO) {
		return err
	}
	return fmt.Errorf("%w: %w", syscall.EIO, err)
}

// SQLite can return SQLITE_INTERRUPT without the driver's ctx.Err substitution. A read
// transaction can also return ErrTxDone when automatic rollback wins after its context
// check. ctx must own the transaction when classifying ErrTxDone. Independent joined
// failures and writes with an uncertain outcome retain their fault classification.
// https://gitlab.com/cznic/sqlite/-/blob/6e86ac4a89e3f36359d1947e36355c469b18430c/rows.go#L107-120
// https://github.com/golang/go/blob/e3336a22ad3f0a90bd252c95d8b5544e02674205/src/database/sql/sql.go#L2245-L2258
func ReadFailure(ctx context.Context, err error) error {
	if ctx.Err() != nil && (err == sql.ErrTxDone || interruptedRead(err)) {
		return Failure(&readCancellationError{cause: err, canceled: ctx.Err()})
	}
	return Failure(err)
}

func interruptedRead(err error) bool {
	if err == nil {
		return false
	}
	if _, classified := err.(interface{ Classification() error }); classified {
		return false
	}
	if coded, ok := err.(interface{ Code() int }); ok {
		const sqliteInterrupt = 9
		return coded.Code() == sqliteInterrupt
	}
	if err == context.Canceled || err == syscall.EINTR {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !interruptedRead(cause) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return interruptedRead(wrapped.Unwrap())
	}
	return false
}

type readCancellationError struct {
	cause    error
	canceled error
}

func (e *readCancellationError) Error() string         { return e.cause.Error() }
func (e *readCancellationError) Unwrap() []error       { return []error{e.cause, e.canceled} }
func (e *readCancellationError) Classification() error { return e.canceled }

// IsUniqueViolation reports whether err is the database refusing a duplicate name.
//
// Two codes, because the schema raises it through a primary key while a uniqueness
// constraint declared any other way raises the other. Measured against modernc.org/sqlite
// v1.57.0: a duplicate (parent, name) surfaces as *sqlite.Error with code 1555,
// SQLITE_CONSTRAINT_PRIMARYKEY.
func IsUniqueViolation(err error) bool {
	var e *sqlite.Error
	if !errors.As(err, &e) {
		return false
	}
	const (
		constraintPrimaryKey = 1555
		constraintUnique     = 2067
	)
	return e.Code() == constraintPrimaryKey || e.Code() == constraintUnique
}
