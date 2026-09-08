package limited_test

import (
	"bytes"
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
)

func TestFailedAccountingUnwindFencesAllowanceBeforePublication(t *testing.T) {
	p := newPublicationProbe(t)
	s := newStorageOver(t, p, limited.MinLimit)
	prepareFailure := errors.New("injected preparation failure")
	settlementFailure := errors.New("injected unwind failure")
	ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			if result != storage.PublicationNotApplied {
				t.Errorf("unwind result = %v, want NotApplied", result)
			}
			return settlementFailure
		}, nil
	})
	ctx = storage.WithPublicationAccounting(ctx, func(int64, int64) (storage.PublicationSettlement, error) {
		return nil, prepareFailure
	})
	err := s.Write(ctx, "a", content(1024))
	for _, cause := range []error{syscall.EIO, prepareFailure, settlementFailure} {
		if !errors.Is(err, cause) {
			t.Errorf("failed unwind error %v lost cause %v", err, cause)
		}
	}
	if _, err := p.Stat(t.Context(), "a"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed accounting preparation published a file: %v", err)
	}
	if _, err := s.Space(t.Context()); !errors.Is(err, settlementFailure) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("allowance after failed accounting unwind returned %v, want the retained uncertainty", err)
	}
	if err := s.Recount(t.Context()); !errors.Is(err, settlementFailure) {
		t.Fatalf("recount cleared accounting uncertainty: %v", err)
	}
}

func TestCleanAccountingPreparationFailureDoesNotFenceAllowance(t *testing.T) {
	p := newPublicationProbe(t)
	s := newStorageOver(t, p, limited.MinLimit)
	failure := errors.Join(syscall.EIO, errors.New("injected preparation failure"))
	ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		return func(storage.PublicationResult) error { return nil }, failure
	})
	if err := s.Write(ctx, "a", content(1024)); !errors.Is(err, failure) {
		t.Fatalf("preparation failure returned %v, want the original cause", err)
	}
	mustUse(t, s, 0)
	mustWrite(t, s, "a", 1024)
	mustUse(t, s, 1024)
}

func TestFailedSettlementPreservesTheActualNamespaceEffect(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		outcome storage.PublicationResult
	}{{"applied", storage.PublicationApplied}, {"not applied", storage.PublicationNotApplied}} {
		t.Run(scenario.name, func(t *testing.T) {
			p := newPublicationProbe(t)
			p.outcome = scenario.outcome
			s := newStorageOver(t, p, limited.MinLimit)
			failure := errors.New("injected settlement failure")
			ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
				return func(storage.PublicationResult) error { return failure }, nil
			})
			if err := s.Write(ctx, "a", content(1024)); !errors.Is(err, syscall.EIO) || !errors.Is(err, failure) {
				t.Fatalf("failed settlement returned %v, want EIO and original cause", err)
			}
			got, err := p.Read(t.Context(), "a")
			if scenario.outcome == storage.PublicationApplied {
				if err != nil || !bytes.Equal(got, content(1024)) {
					t.Fatalf("applied effect lost its bytes: length %d, error %v", len(got), err)
				}
			} else if !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("unapplied effect left a file: %v", err)
			}
			if _, err := s.Space(t.Context()); !errors.Is(err, syscall.EIO) || !errors.Is(err, failure) {
				t.Fatalf("failed settlement left quota usable: %v", err)
			}
		})
	}
}

func TestSettlementFailureFencesEveryNativeMutation(t *testing.T) {
	failure := errors.New("injected settlement failure")
	ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		return func(storage.PublicationResult) error { return failure }, nil
	})
	settle, err := storage.PreparePublication(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	uncertain := settle(storage.PublicationNotApplied)
	for name, operation := range map[string]func(*limited.Storage) error{
		"write":            func(s *limited.Storage) error { return s.Write(t.Context(), "a", nil) },
		"remove":           func(s *limited.Storage) error { return s.Remove(t.Context(), "a") },
		"rename":           func(s *limited.Storage) error { return s.Rename(t.Context(), "a", "b") },
		"setattr":          func(s *limited.Storage) error { return s.SetAttr(t.Context(), "a", storage.AttrChange{}) },
		"create":           func(s *limited.Storage) error { return s.Create(t.Context(), "a") },
		"mkdir":            func(s *limited.Storage) error { return s.Mkdir(t.Context(), "a") },
		"remove directory": func(s *limited.Storage) error { return s.RemoveDir(t.Context(), "a") },
	} {
		t.Run(name, func(t *testing.T) {
			p := &uncertainMutation{publicationProbe: newPublicationProbe(t), err: uncertain}
			s := newStorageOver(t, p, limited.MinLimit)
			if err := operation(s); err != uncertain {
				t.Fatalf("mutation changed the returned error: %v", err)
			}
			if _, err := s.Space(t.Context()); !errors.Is(err, failure) || !errors.Is(err, syscall.EIO) {
				t.Fatalf("allowance remained usable after settlement failure: %v", err)
			}
		})
	}
}

func TestUnmanagedWrapperRetainsUncertainBackingFailure(t *testing.T) {
	failure := errors.New("injected hidden backing settlement failure")
	ctx := storage.WithPublicationAccounting(t.Context(), func(int64, int64) (storage.PublicationSettlement, error) {
		return func(storage.PublicationResult) error { return failure }, nil
	})
	settle, err := storage.PreparePublication(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	uncertain := settle(storage.PublicationNotApplied)
	p := &faulty{BoundedStorage: newBacking(t), write: uncertain}
	s := newStorageOver(t, p, limited.MinLimit)
	if err := s.Write(t.Context(), "a", content(1024)); err != uncertain {
		t.Fatalf("unmanaged wrapper changed the uncertain backing error: %v", err)
	}
	if _, err := s.Space(t.Context()); !errors.Is(err, failure) || !errors.Is(err, syscall.EIO) {
		t.Fatalf("unmanaged wrapper left quota usable after uncertainty: %v", err)
	}
	if _, err := p.Stat(t.Context(), "a"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("failed backing write left a file: %v", err)
	}
}

type uncertainMutation struct {
	*publicationProbe
	err error
}

func (p *uncertainMutation) Write(context.Context, string, []byte) error  { return p.err }
func (p *uncertainMutation) Remove(context.Context, string) error         { return p.err }
func (p *uncertainMutation) Rename(context.Context, string, string) error { return p.err }
func (p *uncertainMutation) SetAttr(context.Context, string, storage.AttrChange) error {
	return p.err
}
func (p *uncertainMutation) Create(context.Context, string) error    { return p.err }
func (p *uncertainMutation) Mkdir(context.Context, string) error     { return p.err }
func (p *uncertainMutation) RemoveDir(context.Context, string) error { return p.err }
