package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type classifiedFailure struct {
	classification error
	causes         []error
}

func (e classifiedFailure) Error() string         { return "classified failure" }
func (e classifiedFailure) Unwrap() []error       { return e.causes }
func (e classifiedFailure) Classification() error { return e.classification }

func TestErrorClassificationPreservesFailureProvenance(t *testing.T) {
	fault := errors.New("namespace unavailable")
	unknown := classifiedFailure{classification: syscall.EIO, causes: []error{context.Canceled}}
	mapped := classifiedFailure{classification: syscall.ENOENT, causes: []error{fault, syscall.ENOENT}}
	interrupted := classifiedFailure{classification: syscall.EINTR, causes: []error{context.Canceled, fault}}
	for _, test := range []struct {
		name string
		err  error
		want syscall.Errno
	}{
		{"success", nil, 0},
		{"cancel", context.Canceled, syscall.EINTR},
		{"wrapped cancel", fmt.Errorf("stat: %w", context.Canceled), syscall.EINTR},
		{"deadline", context.DeadlineExceeded, syscall.EIO},
		{"wrapped deadline", fmt.Errorf("stat: %w", context.DeadlineExceeded), syscall.EIO},
		{"unknown", fault, syscall.EIO},
		{"unrecognized errno", syscall.ECONNREFUSED, syscall.EIO},
		{"native path error", &os.PathError{Op: "stat", Err: syscall.ENOENT}, syscall.ENOENT},
		{"classified unknown", unknown, syscall.EIO},
		{"wrapped classified unknown", fmt.Errorf("write: %w", unknown), syscall.EIO},
		{"mapped diagnostic cause", mapped, syscall.ENOENT},
		{"classified interruption", interrupted, syscall.EINTR},
		{"missing classification", classifiedFailure{causes: []error{syscall.ENOENT}}, syscall.EIO},
		{"same joined errno", errors.Join(syscall.ENOENT, syscall.ENOENT), syscall.ENOENT},
		{"conflicting namespace facts", errors.Join(syscall.ENOENT, syscall.EEXIST), syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := storage.ErrnoOf(test.err); got != test.want {
				t.Fatalf("ErrnoOf(%v) = %v, want %v", test.err, got, test.want)
			}
			wantName := "EIO"
			if test.want != 0 {
				wantName, _ = storage.ErrnoName(test.want)
			}
			if got := storage.ErrnoNameOf(test.err); got != wantName {
				t.Fatalf("ErrnoNameOf(%v) = %s, want %s", test.err, got, wantName)
			}
		})
	}
	for _, other := range []error{fault, syscall.EIO, syscall.ENOENT, unknown, mapped} {
		for _, cancellation := range []error{context.Canceled, interrupted} {
			for _, joined := range []error{errors.Join(other, cancellation), errors.Join(cancellation, other)} {
				if got, want := storage.ErrnoOf(joined), storage.ErrnoOf(other); got != want {
					t.Errorf("ErrnoOf(%v) = %v, want independent result %v", joined, got, want)
				}
			}
		}
	}
}

func TestEveryKnownErrnoSurvivesWrappingAndCancellation(t *testing.T) {
	for _, errno := range storage.Errnos() {
		for _, err := range []error{
			errno,
			fmt.Errorf("operation: %w", errno),
			errors.Join(errno, context.Canceled),
			errors.Join(context.Canceled, errno),
		} {
			if got := storage.ErrnoOf(err); got != errno {
				t.Errorf("ErrnoOf(%v) = %v, want %v", err, got, errno)
			}
		}
	}
}
