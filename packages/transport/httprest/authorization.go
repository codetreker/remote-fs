package httprest

import (
	"context"
	"errors"
	"reflect"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
)

var errHandlerStopped = errors.New("handler is stopping")

func checkAuthorizationOptions(authorizer authz.Authorizer, volume string) error {
	if authorizer != nil {
		value := reflect.ValueOf(authorizer)
		switch value.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if value.IsNil() {
				return errors.New("httprest: HandlerOptions.Authorizer cannot contain a nil value")
			}
		}
	}
	if (authorizer == nil) != (volume == "") {
		return errors.New("httprest: HandlerOptions.Authorizer and Volume must be configured together")
	}
	return nil
}

func (h *Handler) authorizationContext(parent context.Context) (context.Context, func()) {
	if h.authorizer == nil {
		return parent, func() {}
	}
	ctx, cancel := context.WithCancelCause(parent)
	stop := context.AfterFunc(h.lifetime, func() { cancel(errHandlerStopped) })
	return ctx, func() { stop(); cancel(context.Canceled) }
}

type authzFailure struct {
	errno syscall.Errno
	cause error
}

func (e *authzFailure) Error() string {
	if e.errno == syscall.EACCES {
		return "access denied"
	}
	return "authorization failed"
}
func (e *authzFailure) Classification() error { return e.errno }
func (e *authzFailure) Unwrap() error         { return e.cause }

type authorizationLifecycleFailure struct {
	classification error
	cause          error
	message        string
}

func (e *authorizationLifecycleFailure) Error() string         { return e.message }
func (e *authorizationLifecycleFailure) Classification() error { return e.classification }
func (e *authorizationLifecycleFailure) Unwrap() error         { return e.cause }

func (h *Handler) authorizationLifecycleError(ctx context.Context) error {
	if h.stopped() {
		return &authorizationLifecycleFailure{classification: syscall.EIO, cause: errors.Join(errHandlerStopped, ctx.Err(), context.Cause(ctx)), message: errHandlerStopped.Error()}
	}
	if err := ctx.Err(); err != nil {
		return &authorizationLifecycleFailure{classification: err, cause: errors.Join(err, context.Cause(ctx)), message: err.Error()}
	}
	return nil
}

func (h *Handler) authorize(ctx context.Context, request authz.AccessRequest) error {
	if h.authorizer == nil {
		return nil
	}
	if err := h.authorizationLifecycleError(ctx); err != nil {
		return err
	}
	request.Volume = h.volume
	err := h.authorizer.Authorize(ctx, request)
	if lifecycle := h.authorizationLifecycleError(ctx); lifecycle != nil {
		return lifecycle
	}
	if err == nil {
		return nil
	}
	errno := syscall.EIO
	if errors.Is(err, authz.ErrDenied) {
		errno = syscall.EACCES
	}
	return &authzFailure{errno: errno, cause: err}
}

// Only library-owned authorization failures supply policy fault fields. A callback's
// error chain is diagnostic data and cannot choose errno, lock metadata, or wire text.
func authorizationResponse(err error) (ErrorResponse, bool) {
	for err != nil {
		if failure, ok := err.(*authzFailure); ok {
			errno := "EIO"
			if failure.errno == syscall.EACCES {
				errno = "EACCES"
			}
			return ErrorResponse{Errno: errno, Message: failure.Error()}, true
		}
		if _, classified := err.(interface{ Classification() error }); classified {
			break
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			if len(children) != 1 {
				break
			}
			err = children[0]
			continue
		}
		err = errors.Unwrap(err)
	}
	return ErrorResponse{}, false
}

func volumeAuthorizationOperation(op Op) authz.Operation {
	switch op {
	case OpStat:
		return authz.VolumeStat
	case OpList:
		return authz.VolumeList
	case OpRead:
		return authz.VolumeRead
	case OpSpace:
		return authz.VolumeSpace
	case OpSetAttr:
		return authz.VolumeSetAttr
	case OpWrite:
		return authz.VolumeWrite
	case OpCreate:
		return authz.VolumeCreate
	case OpMkdir:
		return authz.VolumeMkdir
	case OpRemove:
		return authz.VolumeRemove
	case OpRemoveDir:
		return authz.VolumeRemoveDir
	case OpRename:
		return authz.VolumeRename
	default:
		return ""
	}
}
