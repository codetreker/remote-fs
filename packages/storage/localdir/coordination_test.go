package localdir

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/codetreker/remote-fs/packages/locking"
)

func coordinationRuntime(t *testing.T, limits Limits) *nativeRuntime {
	t.Helper()
	limits, err := limits.Effective()
	if err != nil {
		t.Fatal(err)
	}
	return newNativeRuntime(limits)
}

func claimPaths(t *testing.T, r *nativeRuntime, paths ...pathIntent) func() {
	t.Helper()
	claims, err := r.expandPaths(paths)
	if err != nil {
		t.Fatal(err)
	}
	release, err := r.enterPaths(t.Context(), claims)
	if err != nil {
		t.Fatal(err)
	}
	return release
}

func TestNativePathClaimsOrderAncestorsAndAllowSiblings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := coordinationRuntime(t, Limits{})
		child := claimPaths(t, r, pathIntent{path: "dir/file"})
		directory := make(chan func(), 1)
		go func() { directory <- claimPaths(t, r, pathIntent{path: "dir", write: true}) }()
		synctest.Wait()
		select {
		case <-directory:
			t.Fatal("directory mutation crossed a child observation")
		default:
		}
		unrelated := claimPaths(t, r, pathIntent{path: "other/file", write: true})
		unrelated()
		laterChild := make(chan func(), 1)
		go func() { laterChild <- claimPaths(t, r, pathIntent{path: "dir/second"}) }()
		synctest.Wait()
		select {
		case <-laterChild:
			t.Fatal("new descendant overtook its waiting ancestor mutation")
		default:
		}
		child()
		releaseDirectory := <-directory
		synctest.Wait()
		select {
		case <-laterChild:
			t.Fatal("descendant crossed an active ancestor mutation")
		default:
		}
		releaseDirectory()
		(<-laterChild)()
		if len(r.gates) != 0 || len(r.pending) != 0 {
			t.Fatalf("claims retained after release: %+v, %+v", r.gates, r.pending)
		}
	})
}

func TestNativePathClaimsCancelAtomicTwoPathWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := coordinationRuntime(t, Limits{})
		a := claimPaths(t, r, pathIntent{path: "a", write: true})
		ctx, cancel := context.WithCancel(t.Context())
		paths, err := r.expandPaths([]pathIntent{{path: "b", write: true}, {path: "a", write: true}})
		if err != nil {
			t.Fatal(err)
		}
		waiting := make(chan error, 1)
		go func() {
			release, err := r.enterPaths(ctx, paths)
			if release != nil {
				release()
			}
			waiting <- err
		}()
		synctest.Wait()
		b := make(chan func(), 1)
		go func() { b <- claimPaths(t, r, pathIntent{path: "b", write: true}) }()
		synctest.Wait()
		cancel()
		if err := <-waiting; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled path wait: %v", err)
		}
		(<-b)()
		a()
	})
}

func TestNativeAdmissionBoundsWaitersAndCloseDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := coordinationRuntime(t, Limits{MaxOperations: 1, MaxWaiters: 1})
		ctx, finish, err := r.begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		waiting := make(chan error, 1)
		go func() {
			_, done, err := r.begin(t.Context())
			if done != nil {
				done()
			}
			waiting <- err
		}()
		synctest.Wait()
		if _, _, err := r.begin(t.Context()); locking.CodeOf(err) != locking.Capacity {
			t.Fatalf("unbounded waiter admission: %v", err)
		}
		r.stop()
		if err := <-waiting; locking.CodeOf(err) != locking.Unavailable {
			t.Fatalf("stopped admission: %v", err)
		}
		synctest.Wait()
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("active operation not cancelled: %v", ctx.Err())
		}
		drained := make(chan struct{})
		go func() { r.wait(); close(drained) }()
		synctest.Wait()
		select {
		case <-drained:
			t.Fatal("draining returned while an operation was active")
		default:
		}
		finish()
		finish()
		<-drained
		if _, _, err := r.begin(t.Context()); locking.CodeOf(err) != locking.Unavailable {
			t.Fatalf("reopened stopped admission: %v", err)
		}
	})
}
