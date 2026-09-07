package azblob

import (
	"context"
	"errors"
	"fmt"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// errnoByCode maps the service's error codes onto the contract's errno vocabulary.
//
// One entry yields ENOENT and there will only ever be one: BlobNotFound is the single code
// that means the object is not there. Every other code, and every failure that carries no
// code at all, is something else — and R-ERR-2 makes the difference load-bearing rather
// than cosmetic, because a sweeper told "not there" deletes the record that the object
// exists, and a writer told "not there" writes it again. ContainerNotFound is the trap the
// rule exists for: it arrives as the same 404 as BlobNotFound and says nothing whatever
// about the object, so it lands in the default below along with AuthenticationFailed,
// ServerBusy, OperationTimedOut and InternalError.
//
// A code that is absent here is EIO, which is the vocabulary's word for a failure the
// sender cannot name. That default is the safe direction: mistaking a determinate refusal
// for an indeterminate one costs a retry, and mistaking an indeterminate one for "not
// there" costs the object.
//
// Where the service is precise, so is this. AuthenticationFailed is not "I could not find
// out" — the request was refused, nothing was written, and the operator's problem is a
// credential rather than a network — so it reports EACCES. What every one of these has in
// common, and what the rule actually demands, is that none of them is ENOENT.
var errnoByCode = map[bloberror.Code]syscall.Errno{
	bloberror.BlobNotFound: syscall.ENOENT,

	// The key was written before. Put asks for If-None-Match: *, and the service names the
	// refusal BlobAlreadyExists on Put Blob; ConditionNotMet is the generic answer to an
	// unmet conditional header and is mapped alongside it so that neither reads as success.
	bloberror.BlobAlreadyExists: syscall.EEXIST,
	bloberror.ConditionNotMet:   syscall.EEXIST,

	// The request was refused. Nothing was stored, read, or removed.
	bloberror.AccountIsDisabled:                 syscall.EACCES,
	bloberror.AuthenticationFailed:              syscall.EACCES,
	bloberror.AuthorizationFailure:              syscall.EACCES,
	bloberror.AuthorizationPermissionMismatch:   syscall.EACCES,
	bloberror.AuthorizationProtocolMismatch:     syscall.EACCES,
	bloberror.AuthorizationResourceTypeMismatch: syscall.EACCES,
	bloberror.AuthorizationServiceMismatch:      syscall.EACCES,
	bloberror.AuthorizationSourceIPMismatch:     syscall.EACCES,
	bloberror.InsufficientAccountPermissions:    syscall.EACCES,
	bloberror.InvalidAuthenticationInfo:         syscall.EACCES,
	bloberror.NoAuthenticationInformation:       syscall.EACCES,
	bloberror.UnauthorizedBlobOverwrite:         syscall.EACCES,

	// A policy on the object forbids the operation, rather than the caller's identity.
	bloberror.BlobImmutableDueToPolicy: syscall.EPERM,

	// Somebody else holds a lease on the blob. The object is there and will accept the
	// operation once the lease is gone, which is what EBUSY says and ENOENT would not.
	bloberror.LeaseIDMismatchWithBlobOperation: syscall.EBUSY,
	bloberror.LeaseIDMissing:                   syscall.EBUSY,
	bloberror.LeaseLost:                        syscall.EBUSY,

	// The service declined to start. Nothing was done, and the same request may work later.
	// OperationTimedOut and InternalError are deliberately not here: those are the service
	// stopping partway, where what was done is unknown, and EIO is the honest answer.
	bloberror.AccountBeingCreated: syscall.EAGAIN,
	bloberror.ServerBusy:          syscall.EAGAIN,

	// The request was malformed before it reached the object.
	bloberror.InvalidHeaderValue:            syscall.EINVAL,
	bloberror.InvalidInput:                  syscall.EINVAL,
	bloberror.InvalidQueryParameterValue:    syscall.EINVAL,
	bloberror.InvalidResourceName:           syscall.EINVAL,
	bloberror.InvalidURI:                    syscall.EINVAL,
	bloberror.OutOfRangeInput:               syscall.EINVAL,
	bloberror.OutOfRangeQueryParameterValue: syscall.EINVAL,

	// More bytes than the service will take in one write. MaxObjectBytes refuses these
	// before they are sent; this covers an account whose limit is lower than the general one.
	bloberror.RequestBodyTooLarge: syscall.EFBIG,
}

// errnoOf names the failure err reports, in the vocabulary the contract allows.
func errnoOf(err error) syscall.Errno {
	// A withdrawn request is the one failure whose cause is the caller rather than the
	// service, and EINTR says exactly that. It is checked first because a cancellation that
	// races a response arrives wrapped around one.
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}
	var response *azcore.ResponseError
	if errors.As(err, &response) {
		if errno, ok := errnoByCode[bloberror.Code(response.ErrorCode)]; ok {
			return errno
		}
	}
	return syscall.EIO
}

// failed is an operation that did not do what was asked.
//
// It names the key rather than the blob's URL, so the account, the endpoint, and the
// container stay out of a message that a caller may log or pass on — the key is what the
// caller gave us and the only part of the address it can act on.
type failed struct {
	op string
	// key is the caller's key, not the prefixed blob name.
	key string
	// answer is the error code the service gave, or its status where it named none. It is
	// empty only when nothing came back at all.
	answer string
	errno  syscall.Errno
	cause  error
}

// Error renders the cause's own text only where nothing came back from the service, which
// is where the message would otherwise say nothing an operator could act on. A response
// that arrived is described by its code instead: azcore renders a *ResponseError as a
// multi-line dump of the whole exchange, which is not what belongs inside a one-line error.
func (f *failed) Error() string {
	if f.answer != "" {
		return fmt.Sprintf("%s %q: the service answered %s: %s", f.op, f.key, f.answer, f.errno)
	}
	return fmt.Sprintf("%s %q: no answer from the service: %s: %s", f.op, f.key, f.cause, f.errno)
}

// Unwrap exposes both the errno the contract travels under and the SDK's own error, so that
// errors.Is reaches the first and errors.As reaches the second — a *ResponseError carries
// the request id, which is what an Azure support case is opened with.
func (f *failed) Unwrap() []error {
	return []error{f.cause, f.errno}
}

// Classification identifies the contract result independently of the diagnostic service
// response retained by Unwrap. A caller deciding whether every component means absence can
// use this without mistaking the response detail for a second, independent failure.
func (f *failed) Classification() error { return f.errno }

// failure describes err as a failure of op on key.
func failure(op, key string, err error) error {
	return &failed{op: op, key: key, answer: answerOf(err), errno: errnoOf(err), cause: err}
}

// answerOf is what the service said, if it said anything. A response with no error code
// still answered, so it is described by its status rather than treated as silence.
func answerOf(err error) string {
	var response *azcore.ResponseError
	if !errors.As(err, &response) {
		return ""
	}
	if response.ErrorCode != "" {
		return response.ErrorCode
	}
	return fmt.Sprintf("HTTP %d with no error code", response.StatusCode)
}
