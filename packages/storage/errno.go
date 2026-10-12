package storage

import (
	"context"
	"maps"
	"slices"
	"syscall"
)

// errnoNames is the contract's error vocabulary: the errnos a Storage's operations can
// report, each under a symbolic name.
//
// It belongs to the contract rather than to any one transport, because it is the set of
// answers an implementation is allowed to give. A transport carries the name rather than
// the number, so that the two ends agree on a vocabulary rather than on one platform's
// numbering.
//
// It is deliberately a closed set, and it does double duty in both directions. An
// implementation reporting an errno that is not here can only be carried as EIO, because
// nothing can name what happened. A name that is not here likewise reads as EIO, because
// the receiver does not know what it was told — and an errno it cannot interpret is
// exactly the "outcome unknown" case, not a number to hand onwards to a kernel.
//
// Aliases are absent on purpose: on Linux EWOULDBLOCK is EAGAIN and ENOTSUP is
// EOPNOTSUPP, and listing both would make one errno answer to two names. Duplicate keys
// here fail to compile, which is the guard that keeps that true.
var errnoNames = map[syscall.Errno]string{
	syscall.EACCES:       "EACCES",
	syscall.EAGAIN:       "EAGAIN",
	syscall.EBADF:        "EBADF",
	syscall.EBUSY:        "EBUSY",
	syscall.EDEADLK:      "EDEADLK",
	syscall.EDQUOT:       "EDQUOT",
	syscall.EEXIST:       "EEXIST",
	syscall.EFBIG:        "EFBIG",
	syscall.EINTR:        "EINTR",
	syscall.EINVAL:       "EINVAL",
	syscall.EIO:          "EIO",
	syscall.EISDIR:       "EISDIR",
	syscall.ELOOP:        "ELOOP",
	syscall.EMFILE:       "EMFILE",
	syscall.EMLINK:       "EMLINK",
	syscall.ENAMETOOLONG: "ENAMETOOLONG",
	syscall.ENFILE:       "ENFILE",
	syscall.ENODEV:       "ENODEV",
	syscall.ENOENT:       "ENOENT",
	syscall.ENOLCK:       "ENOLCK",
	syscall.ENOMEM:       "ENOMEM",
	syscall.ENOSPC:       "ENOSPC",
	syscall.ENOSYS:       "ENOSYS",
	syscall.ENOTDIR:      "ENOTDIR",
	syscall.ENOTEMPTY:    "ENOTEMPTY",
	syscall.ENXIO:        "ENXIO",
	syscall.EOPNOTSUPP:   "EOPNOTSUPP",
	syscall.EOVERFLOW:    "EOVERFLOW",
	syscall.EPERM:        "EPERM",
	syscall.EROFS:        "EROFS",
	syscall.ESTALE:       "ESTALE",
	syscall.ETXTBSY:      "ETXTBSY",
	syscall.EXDEV:        "EXDEV",
}

var errnosByName = func() map[string]syscall.Errno {
	byName := make(map[string]syscall.Errno, len(errnoNames))
	for errno, name := range errnoNames {
		byName[name] = errno
	}
	return byName
}()

// ErrnoName returns the name e travels under, and whether e is in the vocabulary at all.
func ErrnoName(e syscall.Errno) (string, bool) {
	name, ok := errnoNames[e]
	return name, ok
}

// ErrnoNameOf names the failure err reports, so that it can be carried under a name.
//
// Anything it cannot name is EIO. That covers an error carrying no errno at all and an
// errno outside the vocabulary: in both cases the sender knows the operation failed but
// not how to say why, and choosing the nearest available name would put a specific claim
// — "no such file" above all — behind an unspecific failure.
func ErrnoNameOf(err error) string {
	if name, ok := errnoNames[ErrnoOf(err)]; ok {
		return name
	}
	return errnoNames[syscall.EIO]
}

// ErrnoOf classifies an operation's error; nil is success. A canceled request is EINTR,
// while a deadline or an unrecognized failure is EIO. An error may implement
// Classification() error to identify its result while retaining diagnostic causes.
// That classification owns only its subtree: independent joined failures still count.
// Joined failures must agree, except that a failure takes precedence over cancellation.
func ErrnoOf(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	if classified, ok := err.(interface{ Classification() error }); ok {
		if result := classified.Classification(); result != nil {
			return ErrnoOf(result)
		}
		return syscall.EIO
	}
	if errno, ok := err.(syscall.Errno); ok {
		if _, known := errnoNames[errno]; known {
			return errno
		}
		return syscall.EIO
	}
	if err == context.Canceled {
		return syscall.EINTR
	}
	if err == context.DeadlineExceeded {
		return syscall.EIO
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var result syscall.Errno
		for _, child := range joined.Unwrap() {
			next := ErrnoOf(child)
			switch {
			case next == 0:
			case result == 0 || result == syscall.EINTR:
				result = next
			case next == syscall.EINTR || next == result:
			default:
				return syscall.EIO
			}
		}
		if result != 0 {
			return result
		}
		return syscall.EIO
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		if cause := wrapped.Unwrap(); cause != nil {
			return ErrnoOf(cause)
		}
	}
	return syscall.EIO
}

// ErrnoByName returns the errno that name stands for, and whether the name is one this
// contract knows.
func ErrnoByName(name string) (syscall.Errno, bool) {
	errno, ok := errnosByName[name]
	return errno, ok
}

// Errnos returns the whole vocabulary, sorted, so that it can be enumerated and checked.
func Errnos() []syscall.Errno {
	return slices.Sorted(maps.Keys(errnoNames))
}
