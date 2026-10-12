package storage

import (
	"context"
	"syscall"
)

const MaxAccessBindingBytes = 128

// AccessBinding identifies the subject and access context that own a stateful
// capability. Credential rotation may preserve AccessID; a new access context
// cannot inherit existing references, grants or volatile receipts. Durable
// actions use ActionKey and survive the retirement of this access context.
// Both identity strings are bounded UTF-8 and are compared without normalization.
type AccessBinding struct {
	SubjectID string
	AccessID  string
}

func (b AccessBinding) Check() error {
	if !boundedOpaque(b.SubjectID, MaxAccessBindingBytes) || !boundedOpaque(b.AccessID, MaxAccessBindingBytes) {
		return syscall.EINVAL
	}
	return nil
}

// AccessBindingHook derives binding from trusted application context. A failure
// rejects admission or capability use; the absence of an application identity
// cannot be represented by a successful empty binding.
type AccessBindingHook func(context.Context) (AccessBinding, error)

type accessBindingContextKey struct{}

// WithAccessBinding copies a validated binding into a derived context. No
// account, role or authentication mechanism is introduced into storage.
func WithAccessBinding(ctx context.Context, binding AccessBinding) (context.Context, error) {
	if err := binding.Check(); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, accessBindingContextKey{}, binding), nil
}

// AccessBindingFrom distinguishes unbound embedded operation from a bound
// operation. Only application-created contexts establish this binding.
func AccessBindingFrom(ctx context.Context) (AccessBinding, bool) {
	binding, ok := ctx.Value(accessBindingContextKey{}).(AccessBinding)
	return binding, ok
}

// RequireAccessBinding rejects use of a bound capability from an absent or
// different context, including another AccessID belonging to the same subject.
// An unbound capability is usable only by an unbound context.
func RequireAccessBinding(ctx context.Context, expected AccessBinding) error {
	actual, present := AccessBindingFrom(ctx)
	if expected == (AccessBinding{}) {
		if present {
			return syscall.EACCES
		}
		return nil
	}
	if err := expected.Check(); err != nil {
		return err
	}
	if !present || actual != expected {
		return syscall.EACCES
	}
	return nil
}
