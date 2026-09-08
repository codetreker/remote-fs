package storage_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestPublicationComposition(t *testing.T) {
	for _, result := range []storage.PublicationResult{
		storage.PublicationNotApplied, storage.PublicationApplied, storage.PublicationUnknown,
	} {
		t.Run(fmt.Sprint(result), func(t *testing.T) {
			var calls []string
			firstErr, secondErr := errors.New("first settlement"), errors.New("second settlement")
			ctx := context.Background()
			for i, cause := range []error{firstErr, secondErr} {
				ctx = storage.WithPublicationAccounting(ctx, func(previous, next int64) (storage.PublicationSettlement, error) {
					if previous != math.MaxInt64 || next != 0 {
						t.Fatalf("byte counts = (%d, %d)", previous, next)
					}
					calls = append(calls, fmt.Sprintf("prepare %d", i))
					return func(got storage.PublicationResult) error {
						if got != result {
							t.Fatalf("settlement result = %v, want %v", got, result)
						}
						calls = append(calls, fmt.Sprintf("settle %d", i))
						return fmt.Errorf("accounting: %w", cause)
					}, nil
				})
			}
			settle, err := storage.PreparePublication(ctx, math.MaxInt64, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := []string{"prepare 0", "prepare 1"}; !reflect.DeepEqual(calls, got) {
				t.Fatalf("calls before settlement = %v, want %v", calls, got)
			}
			err = settle(result)
			if !errors.Is(err, firstErr) || !errors.Is(err, secondErr) {
				t.Fatalf("settlement lost causes: %v", err)
			}
			if !errors.Is(err, syscall.EIO) || !storage.IsPublicationAccountingUncertain(err) {
				t.Fatalf("failed settlement lacks accounting uncertainty: %v", err)
			}
			want := []string{"prepare 0", "prepare 1", "settle 1", "settle 0"}
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("calls = %v, want %v", calls, want)
			}
			if err := settle(result); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("duplicate failed settlement = %v", err)
			}
		})
	}
}

func TestPublicationPreparationFailureUnwindsAllCharges(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint(partial), func(t *testing.T) {
			primary := errors.New("quota rejected")
			firstCleanup, partialCleanup := errors.New("first cleanup"), errors.New("partial cleanup")
			var calls []string
			ctx := storage.WithPublicationAccounting(context.Background(), func(_, _ int64) (storage.PublicationSettlement, error) {
				calls = append(calls, "prepare first")
				return func(result storage.PublicationResult) error {
					if result != storage.PublicationNotApplied {
						t.Fatalf("rollback result = %v", result)
					}
					calls = append(calls, "rollback first")
					return firstCleanup
				}, nil
			})
			ctx = storage.WithPublicationAccounting(ctx, func(_, _ int64) (storage.PublicationSettlement, error) {
				calls = append(calls, "prepare failing")
				if !partial {
					return nil, primary
				}
				return func(result storage.PublicationResult) error {
					if result != storage.PublicationNotApplied {
						t.Fatalf("partial rollback result = %v", result)
					}
					calls = append(calls, "rollback partial")
					return partialCleanup
				}, primary
			})
			ctx = storage.WithPublicationAccounting(ctx, func(_, _ int64) (storage.PublicationSettlement, error) {
				t.Fatal("preparation continued after a failure")
				return nil, nil
			})
			settle, err := storage.PreparePublication(ctx, 4, 8)
			if settle != nil || !errors.Is(err, primary) || !errors.Is(err, firstCleanup) || errors.Is(err, partialCleanup) != partial {
				t.Fatalf("preparation lost failure or cleanup causes: %v (settlement nil: %v)", err, settle == nil)
			}
			want := []string{"prepare first", "prepare failing"}
			if partial {
				want = append(want, "rollback partial")
			}
			want = append(want, "rollback first")
			if !reflect.DeepEqual(calls, want) {
				t.Fatalf("calls = %v, want %v", calls, want)
			}
		})
	}
}

func TestPublicationRejectsMissingSettlementAndRetainsCleanupFailure(t *testing.T) {
	cleanup := errors.New("cleanup failed")
	ctx := storage.WithPublicationAccounting(context.Background(), func(_, _ int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			if result != storage.PublicationNotApplied {
				t.Fatalf("rollback result = %v", result)
			}
			return cleanup
		}, nil
	})
	ctx = storage.WithPublicationAccounting(ctx, func(_, _ int64) (storage.PublicationSettlement, error) {
		return nil, nil
	})
	settle, err := storage.PreparePublication(ctx, 0, 0)
	if settle != nil || !errors.Is(err, syscall.EIO) || !errors.Is(err, cleanup) {
		t.Fatalf("missing settlement result = %v (settlement nil: %v)", err, settle == nil)
	}
}

func TestPublicationCancellationUnwindsPreparedHooks(t *testing.T) {
	for _, cancelAt := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(cancelAt), func(t *testing.T) {
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := context.Context(base)
			prepared, settled := 0, 0
			cleanup := errors.New("rollback failed")
			for i := range 2 {
				ctx = storage.WithPublicationAccounting(ctx, func(_, _ int64) (storage.PublicationSettlement, error) {
					prepared++
					if i == cancelAt {
						cancel()
					}
					return func(result storage.PublicationResult) error {
						if result != storage.PublicationNotApplied {
							t.Fatalf("cancellation settlement = %v", result)
						}
						settled++
						return cleanup
					}, nil
				})
			}
			if cancelAt == -1 {
				cancel()
			}
			settle, err := storage.PreparePublication(ctx, 1, 2)
			if settle != nil || !errors.Is(err, context.Canceled) || errors.Is(err, cleanup) != (cancelAt >= 0) {
				t.Fatalf("canceled preparation = %v (settlement nil: %v)", err, settle == nil)
			}
			if prepared != cancelAt+1 || settled != prepared {
				t.Fatalf("prepared %d and settled %d after cancel at %d", prepared, settled, cancelAt)
			}
		})
	}
}

func TestPublicationSettlementSurvivesCancellation(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	called := false
	ctx := storage.WithPublicationAccounting(base, func(_, _ int64) (storage.PublicationSettlement, error) {
		return func(result storage.PublicationResult) error {
			called = true
			if result != storage.PublicationApplied {
				t.Fatalf("published result = %v", result)
			}
			return nil
		}, nil
	})
	settle, err := storage.PreparePublication(ctx, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := settle(storage.PublicationApplied); err != nil || !called {
		t.Fatalf("settlement after cancellation = %v, called = %v", err, called)
	}
}

func TestPublicationInvalidInputCannotRunAccounting(t *testing.T) {
	called := false
	ctx := storage.WithPublicationAccounting(context.Background(), func(_, _ int64) (storage.PublicationSettlement, error) {
		called = true
		return func(storage.PublicationResult) error { return nil }, nil
	})
	for _, sizes := range [][2]int64{{-1, 0}, {0, -1}, {-1, -1}} {
		settle, err := storage.PreparePublication(ctx, sizes[0], sizes[1])
		if settle != nil || !errors.Is(err, syscall.EINVAL) || called {
			t.Fatalf("invalid sizes %v: %v, accounting called: %v", sizes, err, called)
		}
	}
}

func TestPublicationRejectsNilHook(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("nil accounting hook did not panic")
		}
	}()
	storage.WithPublicationAccounting(context.Background(), nil)
}

func TestPublicationWithoutAccountingValidatesOutcomes(t *testing.T) {
	for _, result := range []storage.PublicationResult{
		storage.PublicationNotApplied, storage.PublicationApplied, storage.PublicationUnknown,
	} {
		t.Run(fmt.Sprint(result), func(t *testing.T) {
			settle, err := storage.PreparePublication(context.Background(), 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, invalid := range []storage.PublicationResult{0, 255} {
				if err := settle(invalid); !errors.Is(err, syscall.EINVAL) {
					t.Fatalf("invalid result %d = %v", invalid, err)
				}
			}
			err = settle(result)
			if result == storage.PublicationUnknown {
				if !errors.Is(err, syscall.EIO) {
					t.Fatalf("unknown publication = %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if err := settle(result); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("duplicate settlement = %v", err)
			}
		})
	}
}

func TestPublicationUnknownKeepsConservativeAccounting(t *testing.T) {
	for _, next := range []int64{6, 14} {
		t.Run(fmt.Sprint(next), func(t *testing.T) {
			count := int64(10)
			ctx := storage.WithPublicationAccounting(context.Background(), func(previous, next int64) (storage.PublicationSettlement, error) {
				growth := max(next-previous, 0)
				count += growth
				return func(result storage.PublicationResult) error {
					switch result {
					case storage.PublicationNotApplied:
						count -= growth
					case storage.PublicationApplied:
						count -= max(previous-next, 0)
					case storage.PublicationUnknown:
						return syscall.EIO
					}
					return nil
				}, nil
			})
			settle, err := storage.PreparePublication(ctx, 10, next)
			if err != nil {
				t.Fatal(err)
			}
			if count != max(10, next) {
				t.Fatalf("prepared count = %d, next = %d", count, next)
			}
			if err := settle(storage.PublicationUnknown); !errors.Is(err, syscall.EIO) {
				t.Fatalf("unknown settlement = %v", err)
			}
			if count != max(10, next) {
				t.Fatalf("unknown publication released bytes: count = %d, next = %d", count, next)
			}
		})
	}
}

func TestPublicationConcurrentSettlementConsumesOnce(t *testing.T) {
	var calls atomic.Int64
	cause := errors.New("settlement failed")
	ctx := storage.WithPublicationAccounting(context.Background(), func(_, _ int64) (storage.PublicationSettlement, error) {
		return func(storage.PublicationResult) error {
			calls.Add(1)
			return cause
		}, nil
	})
	settle, err := storage.PreparePublication(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	const attempts = 32
	var wg sync.WaitGroup
	results := make(chan error, attempts)
	for range attempts {
		wg.Go(func() { results <- settle(storage.PublicationApplied) })
	}
	wg.Wait()
	close(results)
	consumed := 0
	for err := range results {
		switch {
		case errors.Is(err, cause):
			consumed++
		case errors.Is(err, syscall.EINVAL):
		default:
			t.Fatalf("concurrent settlement = %v", err)
		}
	}
	if consumed != 1 || calls.Load() != 1 {
		t.Fatalf("consumed %d times with %d callback calls", consumed, calls.Load())
	}
}

func TestPublicationContextCompositionIsImmutable(t *testing.T) {
	calls := 0
	base := storage.WithPublicationAccounting(context.Background(), func(_, _ int64) (storage.PublicationSettlement, error) {
		calls++
		return func(storage.PublicationResult) error { return nil }, nil
	})
	const depth = 4096
	derived := base
	for i := range depth {
		derived = storage.WithPublicationAccounting(derived, func(_, _ int64) (storage.PublicationSettlement, error) {
			if calls != i+1 {
				t.Fatalf("preparation order: index %d at call %d", i, calls)
			}
			calls++
			return func(storage.PublicationResult) error { return nil }, nil
		})
	}
	settle, err := storage.PreparePublication(base, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("derived context mutated base: %d hooks called", calls)
	}
	if err := settle(storage.PublicationNotApplied); err != nil {
		t.Fatal(err)
	}
	calls = 0
	settle, err = storage.PreparePublication(derived, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if calls != depth+1 {
		t.Fatalf("prepared %d hooks, want %d", calls, depth+1)
	}
	if err := settle(storage.PublicationNotApplied); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationPreparationDistinguishesRefusalFromFailedUnwind(t *testing.T) {
	for _, refusal := range []error{syscall.EDQUOT, syscall.EIO, context.Canceled} {
		for _, cleanupFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/cleanup-fails=%v", refusal, cleanupFails), func(t *testing.T) {
				cleanup := errors.New("reservation release failed")
				unwound := false
				ctx := storage.WithPublicationAccounting(context.Background(), func(_, _ int64) (storage.PublicationSettlement, error) {
					return func(result storage.PublicationResult) error {
						if result != storage.PublicationNotApplied {
							t.Fatalf("unwind result = %v", result)
						}
						unwound = true
						if cleanupFails {
							return cleanup
						}
						return nil
					}, nil
				})
				ctx = storage.WithPublicationAccounting(ctx, func(_, _ int64) (storage.PublicationSettlement, error) {
					return nil, fmt.Errorf("preparing charge: %w", refusal)
				})
				settle, err := storage.PreparePublication(ctx, 4, 8)
				if settle != nil || !unwound || !errors.Is(err, refusal) || errors.Is(err, cleanup) != cleanupFails {
					t.Fatalf("prepare result = %v, unwound = %v, settlement nil = %v", err, unwound, settle == nil)
				}
				if storage.IsPublicationAccountingUncertain(err) != cleanupFails {
					t.Fatalf("accounting uncertainty = %v, cleanup failed = %v", err, cleanupFails)
				}
				want := storage.ErrnoOf(refusal)
				if cleanupFails {
					want = syscall.EIO
				}
				if got := storage.ErrnoOf(err); got != want {
					t.Fatalf("classification = %v, want %v for %v", got, want, err)
				}
			})
		}
	}
}

func TestPublicationSettlementUncertaintySurvivesWrapping(t *testing.T) {
	for _, result := range []storage.PublicationResult{
		storage.PublicationNotApplied, storage.PublicationApplied, storage.PublicationUnknown,
	} {
		for _, cause := range []error{nil, context.Canceled, syscall.EDQUOT} {
			t.Run(fmt.Sprintf("%v/cause=%v", result, cause), func(t *testing.T) {
				ctx := storage.WithPublicationAccounting(context.Background(), func(_, _ int64) (storage.PublicationSettlement, error) {
					return func(storage.PublicationResult) error { return cause }, nil
				})
				settle, err := storage.PreparePublication(ctx, 4, 8)
				if err != nil {
					t.Fatal(err)
				}
				err = settle(result)
				uncertain := cause != nil || result == storage.PublicationUnknown
				if storage.IsPublicationAccountingUncertain(err) != uncertain {
					t.Fatalf("accounting uncertainty = %v, want %v", err, uncertain)
				}
				if !uncertain {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				if cause != nil && !errors.Is(err, cause) {
					t.Fatalf("settlement lost cause %v: %v", cause, err)
				}
				nested := fmt.Errorf("native publication: %w", errors.Join(syscall.EINTR, fmt.Errorf("accounting: %w", err)))
				if !storage.IsPublicationAccountingUncertain(nested) || !errors.Is(nested, syscall.EIO) || storage.ErrnoOf(nested) != syscall.EIO {
					t.Fatalf("nested uncertainty lost EIO classification: %v", nested)
				}
			})
		}
	}
	for _, err := range []error{nil, syscall.EIO, errors.Join(syscall.EIO, syscall.EDQUOT)} {
		if storage.IsPublicationAccountingUncertain(err) {
			t.Fatalf("ordinary error became accounting uncertainty: %v", err)
		}
	}
}
