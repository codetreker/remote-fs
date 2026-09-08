package fuse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type controlledMutationFile struct {
	storage.File
	before func(context.Context) error
	after  func()
}

func (f controlledMutationFile) WriteAt(ctx context.Context, offset int64, data []byte) (storage.Attr, error) {
	if f.before != nil {
		if err := f.before(ctx); err != nil {
			return storage.Attr{}, err
		}
	}
	attr, err := f.File.WriteAt(ctx, offset, data)
	if err == nil && f.after != nil {
		f.after()
	}
	return attr, err
}

func (f controlledMutationFile) Truncate(ctx context.Context, size int64) (storage.Attr, error) {
	if f.before != nil {
		if err := f.before(ctx); err != nil {
			return storage.Attr{}, err
		}
	}
	attr, err := f.File.Truncate(ctx, size)
	if err == nil && f.after != nil {
		f.after()
	}
	return attr, err
}

func TestRetainedMutationCancellationPreservesFailureClassificationAndFile(t *testing.T) {
	fault := errors.New("quota authority unavailable")
	for _, cause := range []error{
		context.Canceled, fmt.Errorf("mutation: %w", context.Canceled),
		syscall.EINTR, fmt.Errorf("mutation: %w", syscall.EINTR),
		context.DeadlineExceeded, fault, errors.Join(context.Canceled, fault), errors.Join(fault, context.Canceled),
	} {
		for _, truncate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/truncate=%v", cause, truncate), func(t *testing.T) {
				h := aHandle(t, []byte("unchanged"), 1<<20)
				h.node.ns.storage = forbiddenCapacityProbe{}
				calls := 0
				h.file = controlledMutationFile{File: h.file, before: func(context.Context) error {
					calls++
					if calls == 1 {
						return cause
					}
					return syscall.EDQUOT
				}}
				apply := func() syscall.Errno {
					if truncate {
						return errnoOf(h.resize(t.Context(), 100))
					}
					n, errno := h.Write(t.Context(), []byte("growth"), 9)
					if n != 0 {
						t.Fatalf("refused write accepted %d bytes", n)
					}
					return errno
				}
				if errno := apply(); errno != storage.ErrnoOf(cause) || calls != 1 {
					t.Fatalf("mutation returned %v after %d calls; want %v", errno, calls, storage.ErrnoOf(cause))
				}
				if got := retainedContents(t, h); string(got) != "unchanged" {
					t.Fatalf("refusal changed content: %q", got)
				}
				if errno := apply(); errno != syscall.EDQUOT || calls != 2 {
					t.Fatalf("second authoritative refusal returned %v after %d calls", errno, calls)
				}
				if got := retainedContents(t, h); string(got) != "unchanged" {
					t.Fatalf("quota refusal changed content: %q", got)
				}
			})
		}
	}
}

func TestSuccessfulRetainedMutationIgnoresLateCancellation(t *testing.T) {
	for _, truncate := range []bool{false, true} {
		t.Run(fmt.Sprintf("truncate=%v", truncate), func(t *testing.T) {
			h := aHandle(t, []byte("contents"), 1<<20)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			h.file = controlledMutationFile{File: h.file, after: cancel}
			if truncate {
				if err := h.resize(ctx, 3); err != nil {
					t.Fatal(err)
				}
			} else if n, errno := h.Write(ctx, []byte("new"), 0); errno != 0 || n != 3 {
				t.Fatalf("acknowledged write returned %d, %v", n, errno)
			}
			if ctx.Err() != context.Canceled {
				t.Fatal("mutation did not trigger late cancellation")
			}
			want := "newtents"
			if truncate {
				want = "con"
			}
			if got := retainedContents(t, h); string(got) != want {
				t.Fatalf("acknowledged mutation left %q, want %q", got, want)
			}
		})
	}
}

func TestAuthoritativeQuotaRejectsTheMutationAndPermitsShrinking(t *testing.T) {
	const allowance = 64 << 10
	for _, truncate := range []bool{false, true} {
		t.Run(fmt.Sprintf("truncate=%v", truncate), func(t *testing.T) {
			original := bytes.Repeat([]byte("x"), allowance)
			h := aHandleWithAllowance(t, original, 1<<20, allowance)
			h.node.ns.storage = forbiddenCapacityProbe{}
			if truncate {
				if err := h.resize(t.Context(), allowance+1); !errors.Is(err, syscall.EDQUOT) {
					t.Fatalf("over-quota truncate: %v", err)
				}
			} else if n, errno := h.Write(t.Context(), []byte("x"), allowance); n != 0 || errno != syscall.EDQUOT {
				t.Fatalf("over-quota write returned %d, %v", n, errno)
			}
			if got := retainedContents(t, h); !bytes.Equal(got, original) {
				t.Fatal("quota refusal changed file contents")
			}
			if err := h.resize(t.Context(), allowance/2); err != nil {
				t.Fatalf("shrink at full quota: %v", err)
			}
			if n, errno := h.Write(t.Context(), []byte("released space"), allowance/2); errno != 0 || n != 14 {
				t.Fatalf("growth after shrink returned %d, %v", n, errno)
			}
			if got := retainedContents(t, h); len(got) != allowance/2+14 {
				t.Fatalf("growth after shrink has %d bytes", len(got))
			}
		})
	}
}
