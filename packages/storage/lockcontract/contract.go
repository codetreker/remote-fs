// Package lockcontract exercises explicit lease behavior against complete namespaces.
// Builders own backend initialization and register all lifecycle cleanup with testing.T.
package lockcontract

import (
	"context"
	"errors"
	"io/fs"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type Fixture struct {
	Storage storage.BoundedStorage
	Locks   locking.Service
	Scope   func(locking.MutationScope) (storage.BoundedStorage, error)
}

type Builder func(*testing.T, locking.Options) Fixture

// Run applies the same protection, identity, replay and cleanup obligations to
// native, wrapped and remote namespaces. Each subtest receives a fresh authority.
func Run(t *testing.T, build Builder) {
	t.Helper()
	for _, test := range []struct {
		name string
		run  func(*testing.T, Fixture)
	}{
		{"shared_protection", sharedProtection},
		{"exclusive_mutations", exclusiveMutations},
		{"immutable_scopes", immutableScopes},
		{"resource_identity", resourceIdentity},
		{"discovery_and_queued_removal", removedResources},
		{"stale_and_unrelated_proofs", staleProofs},
		{"expiry_without_successor", expiry},
		{"fair_queue_and_cancel", fairQueue},
		{"cancel_and_replay", cancellation},
		{"unsupported_targets", unsupportedTargets},
	} {
		t.Run(test.name, func(t *testing.T) { test.run(t, build(t, locking.DefaultOptions())) })
	}
	t.Run("bounded_history_cleanup", func(t *testing.T) {
		options := locking.DefaultOptions()
		options.ActionsPerOwner = 2
		history(t, build(t, options))
	})
}

func owner(t *testing.T, f Fixture) locking.OwnerRef {
	t.Helper()
	ticket, err := f.Locks.BeginEnrollment(t.Context())
	must(t, err)
	session, err := f.Locks.OpenSession(t.Context(), ticket)
	must(t, err)
	replayed, err := f.Locks.OpenSession(t.Context(), ticket)
	must(t, err)
	if replayed.ID != session.ID {
		t.Fatal("replaying enrollment created another session")
	}
	o, err := f.Locks.CreateOwner(t.Context(), session.ID, "owner")
	must(t, err)
	t.Cleanup(func() {
		if err := f.Locks.CloseSession(context.Background(), session.ID); err != nil {
			t.Errorf("closing lock session: %v", err)
		}
	})
	return o.Ref
}

func request(t *testing.T, f Fixture, o locking.OwnerRef, name string, mode locking.Mode, id locking.RequestID) locking.AcquireRequest {
	t.Helper()
	ref, err := f.Locks.Resolve(t.Context(), o, name)
	must(t, err)
	return locking.AcquireRequest{Owner: o, Request: id, Resource: ref, Mode: mode, TTL: 10 * time.Second}
}

func acquire(t *testing.T, f Fixture, req locking.AcquireRequest) locking.GrantRef {
	t.Helper()
	result, err := f.Locks.Acquire(t.Context(), req)
	must(t, err)
	if !result.Recorded || result.Receipt.Outcome != locking.Granted || result.Grant == nil || result.Grant.State != locking.Active {
		t.Fatalf("acquisition did not produce an active recorded grant: outcome=%s", result.Receipt.Outcome)
	}
	return result.Grant.Ref
}

func scoped(t *testing.T, f Fixture, o locking.OwnerRef, grants ...locking.GrantRef) storage.BoundedStorage {
	t.Helper()
	s, err := f.Scope(locking.MutationScope{Owner: o, Grants: grants})
	must(t, err)
	return s
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func code(t *testing.T, err error, want locking.Code) {
	t.Helper()
	if !errors.Is(err, &locking.Error{Code: want}) {
		t.Fatalf("error = %v, want lock code %s", err, want)
	}
}

func contents(t *testing.T, s storage.Storage, name, want string) {
	t.Helper()
	got, err := s.Read(t.Context(), name)
	must(t, err)
	if string(got) != want {
		t.Fatalf("contents at %q = %q, want %q", name, got, want)
	}
}

func sharedProtection(t *testing.T, f Fixture) {
	must(t, f.Storage.Write(t.Context(), "file", []byte("original")))
	a, b, c := owner(t, f), owner(t, f), owner(t, f)
	ga := acquire(t, f, request(t, f, a, "file", locking.Shared, "shared-a"))
	gb := acquire(t, f, request(t, f, b, "file", locking.Shared, "shared-b"))
	if ga.Resource != gb.Resource {
		t.Fatal("owners resolved different identities for the same file")
	}
	_, err := f.Locks.Acquire(t.Context(), request(t, f, a, "file", locking.Exclusive, "implicit-upgrade"))
	code(t, err, locking.AlreadyHeld)
	code(t, f.Storage.Write(t.Context(), "file", []byte("anonymous")), locking.Conflict)
	code(t, scoped(t, f, a, ga).Write(t.Context(), "file", []byte("shared-owner")), locking.Conflict)
	contents(t, f.Storage, "file", "original")
	wantX := request(t, f, c, "file", locking.Exclusive, "exclusive-conflict")
	rejected, err := f.Locks.Acquire(t.Context(), wantX)
	code(t, err, locking.Conflict)
	if !rejected.Recorded || rejected.Receipt.Outcome != locking.Rejected || rejected.Receipt.Code != locking.Conflict {
		t.Fatal("conflicting acquisition did not retain its rejection")
	}
	_, err = f.Locks.Release(t.Context(), a, ga)
	must(t, err)
	_, err = f.Locks.Release(t.Context(), b, gb)
	must(t, err)
	replayed, err := f.Locks.Acquire(t.Context(), wantX)
	code(t, err, locking.Conflict)
	if replayed.Receipt.Outcome != locking.Rejected {
		t.Fatal("replaying a rejected acquisition granted after contention ended")
	}
	must(t, f.Storage.Write(t.Context(), "file", []byte("unprotected")))
}

func exclusiveMutations(t *testing.T, f Fixture) {
	a, b := owner(t, f), owner(t, f)
	mode := fs.FileMode(0o600)
	stamp := time.Unix(1700000000, 0)
	for _, test := range []struct {
		name string
		run  func(storage.Storage, string) error
	}{
		{"write", func(s storage.Storage, p string) error { return s.Write(t.Context(), p, []byte("updated")) }},
		{"mode", func(s storage.Storage, p string) error {
			return s.SetAttr(t.Context(), p, storage.AttrChange{Mode: &mode})
		}},
		{"mtime", func(s storage.Storage, p string) error {
			return s.SetAttr(t.Context(), p, storage.AttrChange{ModTime: &stamp})
		}},
		{"atime", func(s storage.Storage, p string) error {
			return s.SetAttr(t.Context(), p, storage.AttrChange{AccessTime: &stamp})
		}},
		{"remove", func(s storage.Storage, p string) error { return s.Remove(t.Context(), p) }},
		{"rename", func(s storage.Storage, p string) error { return s.Rename(t.Context(), p, p+"-moved") }},
	} {
		name := test.name
		must(t, f.Storage.Write(t.Context(), name, []byte("original")))
		grant := acquire(t, f, request(t, f, a, name, locking.Exclusive, locking.RequestID(name)))
		_, err := f.Locks.Acquire(t.Context(), request(t, f, b, name, locking.Exclusive, locking.RequestID(name)))
		code(t, err, locking.Conflict)
		code(t, test.run(f.Storage, name), locking.Conflict)
		contents(t, f.Storage, name, "original")
		must(t, test.run(scoped(t, f, a, grant), name))
	}
}

func immutableScopes(t *testing.T, f Fixture) {
	must(t, f.Storage.Write(t.Context(), "file", []byte("original")))
	o := owner(t, f)
	g := acquire(t, f, request(t, f, o, "file", locking.Exclusive, "grant"))
	proofs := []locking.GrantRef{g}
	s, err := f.Scope(locking.MutationScope{Owner: o, Grants: proofs})
	must(t, err)
	proofs[0].Generation++
	must(t, s.Write(t.Context(), "file", []byte("scoped")))
	contents(t, f.Storage, "file", "scoped")
	code(t, f.Storage.Write(t.Context(), "file", []byte("unscoped")), locking.Conflict)
	code(t, s.Write(locking.WithScope(t.Context(), locking.MutationScope{}), "file", []byte("anonymous request")), locking.Conflict)
	anonymous, err := f.Scope(locking.MutationScope{})
	must(t, err)
	code(t, anonymous.Write(t.Context(), "file", []byte("anonymous scope")), locking.Conflict)
	_, err = f.Locks.Release(t.Context(), o, g)
	must(t, err)
	contents(t, s, "file", "scoped")
	code(t, s.SetAttr(t.Context(), "file", storage.AttrChange{}), locking.StaleGrant)
	code(t, s.Rename(t.Context(), "file", "file"), locking.StaleGrant)
}

func resourceIdentity(t *testing.T, f Fixture) {
	must(t, f.Storage.Mkdir(t.Context(), "parent"))
	must(t, f.Storage.Write(t.Context(), "parent/source", []byte("source")))
	must(t, f.Storage.Write(t.Context(), "target", []byte("target")))
	o := owner(t, f)
	gs := acquire(t, f, request(t, f, o, "parent/source", locking.Exclusive, "source"))
	gt := acquire(t, f, request(t, f, o, "target", locking.Exclusive, "target"))
	must(t, f.Storage.Rename(t.Context(), "parent", "moved"))
	ref, err := f.Locks.Resolve(t.Context(), o, "moved/source")
	must(t, err)
	if ref.ID != gs.Resource {
		t.Fatal("ancestor rename changed a file's lock identity")
	}
	code(t, scoped(t, f, o, gs).Rename(t.Context(), "moved/source", "target"), locking.Conflict)
	must(t, scoped(t, f, o, gs, gt).Rename(t.Context(), "moved/source", "target"))
	contents(t, f.Storage, "target", "source")
	ref, err = f.Locks.Resolve(t.Context(), o, "target")
	must(t, err)
	if ref.ID != gs.Resource || ref.ID == gt.Resource {
		t.Fatal("replacement retained the displaced target's lock identity")
	}
	code(t, scoped(t, f, o, gt).Write(t.Context(), "target", []byte("stale destination")), locking.StaleGrant)
	must(t, scoped(t, f, o, gs).Write(t.Context(), "target", []byte("retained source")))
	must(t, f.Storage.Write(t.Context(), "moved/source", []byte("recreated")))
	code(t, scoped(t, f, o, gs).Write(t.Context(), "moved/source", []byte("wrong identity")), locking.UnrelatedProof)
	must(t, scoped(t, f, o, gs).Remove(t.Context(), "target"))
	must(t, f.Storage.Write(t.Context(), "target", []byte("new target")))
	code(t, scoped(t, f, o, gs).Write(t.Context(), "target", []byte("deleted identity")), locking.StaleGrant)
	contents(t, f.Storage, "target", "new target")
}

func removedResources(t *testing.T, f Fixture) {
	must(t, f.Storage.Write(t.Context(), "file", []byte("first")))
	a, b := owner(t, f), owner(t, f)
	old := request(t, f, a, "file", locking.Exclusive, "discovered")
	must(t, f.Storage.Remove(t.Context(), "file"))
	must(t, f.Storage.Write(t.Context(), "file", []byte("second")))
	rejected, err := f.Locks.Acquire(t.Context(), old)
	code(t, err, locking.StaleResource)
	if !rejected.Recorded || rejected.Receipt.Outcome != locking.Rejected {
		t.Fatal("a removed discovered resource did not retain its rejection")
	}
	current := request(t, f, a, "file", locking.Exclusive, "current")
	if current.Resource.ID == old.Resource.ID {
		t.Fatal("deletion and recreation rebound a live discovered resource")
	}
	g := acquire(t, f, current)
	queued := request(t, f, b, "file", locking.Exclusive, "queued")
	queued.Wait = time.Second
	pending, err := f.Locks.Acquire(t.Context(), queued)
	must(t, err)
	if pending.Receipt.Outcome != locking.Pending {
		t.Fatal("replacement removal fixture did not queue its conflicting acquisition")
	}
	must(t, scoped(t, f, a, g).Remove(t.Context(), "file"))
	result, err := f.Locks.QueryAction(t.Context(), b, queued.Request)
	code(t, err, locking.StaleResource)
	if !result.Recorded || result.Receipt.Outcome != locking.Rejected || result.Receipt.Code != locking.StaleResource {
		t.Fatal("removing a queued acquisition target did not retain its terminal rejection")
	}
	must(t, f.Storage.Write(t.Context(), "file", []byte("third")))
	replay, err := f.Locks.Acquire(t.Context(), queued)
	code(t, err, locking.StaleResource)
	if replay.Receipt.Outcome != locking.Rejected {
		t.Fatal("a queued acquisition migrated to a recreated path")
	}
	contents(t, f.Storage, "file", "third")
}

func staleProofs(t *testing.T, f Fixture) {
	must(t, f.Storage.Write(t.Context(), "file", []byte("original")))
	must(t, f.Storage.Write(t.Context(), "other", []byte("other")))
	must(t, f.Storage.Mkdir(t.Context(), "directory"))
	o := owner(t, f)
	req := request(t, f, o, "file", locking.Exclusive, "grant")
	g := acquire(t, f, req)
	code(t, scoped(t, f, o, g).Write(t.Context(), "other", []byte("unrelated")), locking.UnrelatedProof)
	bad := g
	bad.Generation++
	code(t, scoped(t, f, o, bad).Write(t.Context(), "file", []byte("bad generation")), locking.StaleGrant)
	_, err := f.Locks.Release(t.Context(), o, g)
	must(t, err)
	code(t, scoped(t, f, o, g).Write(t.Context(), "file", []byte("expired proof")), locking.StaleGrant)
	code(t, scoped(t, f, o, g).Create(t.Context(), "new-file"), locking.StaleGrant)
	code(t, scoped(t, f, o, g).Mkdir(t.Context(), "new-directory"), locking.StaleGrant)
	code(t, scoped(t, f, o, g).RemoveDir(t.Context(), "directory"), locking.StaleGrant)
	must(t, f.Storage.RemoveDir(t.Context(), "directory"))
	replay, err := f.Locks.Acquire(t.Context(), req)
	must(t, err)
	if replay.Receipt.Outcome != locking.Granted || replay.Grant == nil || replay.Grant.State != locking.Released {
		t.Fatal("release changed the immutable acquisition receipt or hid current state")
	}
	contents(t, f.Storage, "file", "original")
}

func expiry(t *testing.T, f Fixture) {
	must(t, f.Storage.Write(t.Context(), "file", []byte("original")))
	o := owner(t, f)
	req := request(t, f, o, "file", locking.Exclusive, "short")
	req.TTL = 40 * time.Millisecond
	result, err := f.Locks.Acquire(t.Context(), req)
	must(t, err)
	if !result.Recorded || result.Receipt.Outcome != locking.Granted || result.Receipt.Grant == nil {
		t.Fatal("the short acquisition did not retain its grant receipt")
	}
	g := *result.Receipt.Grant
	wait(t, func() bool {
		status, err := f.Locks.QueryGrant(t.Context(), o, g)
		must(t, err)
		return status.State == locking.Expired
	})
	code(t, scoped(t, f, o, g).Write(t.Context(), "file", []byte("too late")), locking.StaleGrant)
	contents(t, f.Storage, "file", "original")
	must(t, f.Storage.Write(t.Context(), "file", []byte("anonymous after expiry")))
}

func fairQueue(t *testing.T, f Fixture) {
	must(t, f.Storage.Write(t.Context(), "file", []byte("original")))
	a, b, c := owner(t, f), owner(t, f), owner(t, f)
	ga := acquire(t, f, request(t, f, a, "file", locking.Shared, "holding"))
	x := request(t, f, b, "file", locking.Exclusive, "waiting-writer")
	x.Wait = 5 * time.Second
	queued, err := f.Locks.Acquire(t.Context(), x)
	must(t, err)
	if queued.Receipt.Outcome != locking.Pending {
		t.Fatal("conflicting bounded acquisition was not queued")
	}
	s := request(t, f, c, "file", locking.Shared, "later-reader")
	s.Wait = 5 * time.Second
	later, err := f.Locks.Acquire(t.Context(), s)
	must(t, err)
	if later.Receipt.Outcome != locking.Pending {
		t.Fatal("a later shared acquisition bypassed the queued writer")
	}
	_, err = f.Locks.Release(t.Context(), a, ga)
	must(t, err)
	var writer locking.ActionResult
	wait(t, func() bool {
		writer, err = f.Locks.QueryAction(t.Context(), b, x.Request)
		must(t, err)
		return writer.Receipt.Outcome == locking.Granted
	})
	later, err = f.Locks.QueryAction(t.Context(), c, s.Request)
	must(t, err)
	if later.Receipt.Outcome != locking.Pending {
		t.Fatal("shared acquisition overlapped the queued writer's grant")
	}
	cancelled, err := f.Locks.Cancel(t.Context(), b, x.Request)
	must(t, err)
	if !cancelled.Released {
		t.Fatal("cancelling an acquisition already won did not report release")
	}
	wait(t, func() bool {
		later, err = f.Locks.QueryAction(t.Context(), c, s.Request)
		must(t, err)
		return later.Receipt.Outcome == locking.Granted
	})
	status, err := f.Locks.QueryGrant(t.Context(), b, writer.Grant.Ref)
	must(t, err)
	if status.State != locking.Released {
		t.Fatal("cancelled writer retained an active grant")
	}
	d := owner(t, f)
	queuedCancel := request(t, f, d, "file", locking.Exclusive, "cancel-pending")
	queuedCancel.Wait = time.Second
	pending, err := f.Locks.Acquire(t.Context(), queuedCancel)
	must(t, err)
	if pending.Receipt.Outcome != locking.Pending {
		t.Fatal("the cancellation target was not pending")
	}
	_, err = f.Locks.Cancel(t.Context(), d, queuedCancel.Request)
	must(t, err)
	replayed, err := f.Locks.Acquire(t.Context(), queuedCancel)
	must(t, err)
	if replayed.Receipt.Outcome != locking.Cancelled {
		t.Fatal("replaying a cancelled pending acquisition recreated its intent")
	}
	queuedTimeout := request(t, f, d, "file", locking.Exclusive, "timed-out")
	queuedTimeout.Wait = 40 * time.Millisecond
	_, err = f.Locks.Acquire(t.Context(), queuedTimeout)
	must(t, err)
	wait(t, func() bool {
		timed, err := f.Locks.QueryAction(t.Context(), d, queuedTimeout.Request)
		must(t, err)
		return timed.Receipt.Outcome == locking.TimedOut
	})
}

func cancellation(t *testing.T, f Fixture) {
	must(t, f.Storage.Write(t.Context(), "file", []byte("original")))
	o := owner(t, f)
	req := request(t, f, o, "file", locking.Exclusive, "cancel-before-delivery")
	_, err := f.Locks.QueryAction(t.Context(), o, req.Request)
	code(t, err, locking.OutcomeUnknown)
	cancelled, err := f.Locks.Cancel(t.Context(), o, req.Request)
	must(t, err)
	if cancelled.Outcome != locking.Cancelled {
		t.Fatal("cancellation did not retain a tombstone for delayed acquisition")
	}
	result, err := f.Locks.Acquire(t.Context(), req)
	must(t, err)
	if result.Receipt.Outcome != locking.Cancelled || result.Grant != nil {
		t.Fatal("a delayed acquisition escaped its cancellation tombstone")
	}
	actual := request(t, f, o, "file", locking.Exclusive, "delivered")
	g := acquire(t, f, actual)
	actual.TTL += time.Millisecond
	_, err = f.Locks.Acquire(t.Context(), actual)
	code(t, err, locking.RequestMismatch)
	_, err = f.Locks.Cancel(t.Context(), o, "delivered")
	must(t, err)
	_, err = f.Locks.Cancel(t.Context(), o, "delivered")
	must(t, err)
	code(t, scoped(t, f, o, g).Write(t.Context(), "file", []byte("cancelled")), locking.StaleGrant)
	must(t, f.Storage.Write(t.Context(), "file", []byte("released")))
}

func unsupportedTargets(t *testing.T, f Fixture) {
	must(t, f.Storage.Mkdir(t.Context(), "directory"))
	o := owner(t, f)
	_, err := f.Locks.Resolve(t.Context(), o, "directory")
	code(t, err, locking.UnsupportedTarget)
	_, err = f.Locks.Resolve(t.Context(), o, "")
	code(t, err, locking.UnsupportedTarget)
	_, err = f.Locks.Resolve(t.Context(), o, "absent")
	code(t, err, locking.UnsupportedTarget)
}

func history(t *testing.T, f Fixture) {
	must(t, f.Storage.Write(t.Context(), "file", []byte("original")))
	o := owner(t, f)
	req := request(t, f, o, "file", locking.Exclusive, "acquire")
	g := acquire(t, f, req)
	renew := locking.RenewRequest{Owner: o, Request: "renew", Grant: g, TTL: 10 * time.Second}
	renewed, err := f.Locks.Renew(t.Context(), renew)
	must(t, err)
	if renewed.Receipt.Outcome != locking.Renewed {
		t.Fatal("renewal did not retain a successful action")
	}
	renew.Request = "overflow"
	notAdmitted, err := f.Locks.Renew(t.Context(), renew)
	code(t, err, locking.Capacity)
	if notAdmitted.Recorded {
		t.Fatal("history overflow claimed to record a new action")
	}
	_, err = f.Locks.Cancel(t.Context(), o, "unseen-at-capacity")
	code(t, err, locking.OutcomeUnknown)
	status, err := f.Locks.QueryGrant(t.Context(), o, g)
	must(t, err)
	if status.State != locking.Active || status.DeadlineMillis != renewed.Grant.DeadlineMillis {
		t.Fatal("failed renewal changed the acknowledged grant interval")
	}
	_, err = f.Locks.Release(t.Context(), o, g)
	must(t, err)
	must(t, f.Locks.RetireOwner(t.Context(), o))
	must(t, f.Locks.RetireOwner(t.Context(), o))
	_, err = f.Locks.Acquire(t.Context(), req)
	code(t, err, locking.Retired)
	must(t, f.Storage.Write(t.Context(), "file", []byte("after retirement")))
}

func wait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("lock state did not reach the expected transition")
		}
		time.Sleep(time.Millisecond)
	}
}
