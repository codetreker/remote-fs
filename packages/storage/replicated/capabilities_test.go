package replicated

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

type capabilityAuthorityStub struct {
	*fileSessionStub
	storage.AtomicFileOpener
	storage.NamespaceAccess
	storage.NodeReferences
	storage.MetadataAccess
	storage.UseOwners
	storage.RangeControl
	barrier   *httprest.MutationBarrier
	file      storage.File
	reference storage.NodeReference
	captured  storage.Attr
	seen      any
	checkErr  error
}

func (s *capabilityAuthorityStub) CheckAtomicFileOpen() error  { return s.checkErr }
func (s *capabilityAuthorityStub) CheckNamespaceAccess() error { return s.checkErr }
func (s *capabilityAuthorityStub) CheckNodeReferences() error  { return s.checkErr }
func (s *capabilityAuthorityStub) CheckMetadataAccess() error  { return s.checkErr }
func (s *capabilityAuthorityStub) CheckUseOwners() error       { return s.checkErr }
func (s *capabilityAuthorityStub) CheckRangeControl() error    { return s.checkErr }
func (s *capabilityAuthorityStub) OpenAtWithBarrier(_ context.Context, n storage.ChildName, o storage.OpenAtOptions) (storage.OpenResult, *httprest.MutationBarrier, error) {
	s.seen = struct {
		Name    storage.ChildName
		Options storage.OpenAtOptions
	}{n, o}
	return storage.OpenResult{File: s.file, Attr: s.captured, Outcome: storage.Replaced}, s.barrier, nil
}
func (s *capabilityAuthorityStub) LookupAt(_ context.Context, name storage.ChildName) (storage.Attr, error) {
	s.seen = name
	return s.captured, nil
}
func (s *capabilityAuthorityStub) ReadDirNode(_ context.Context, d storage.DirectoryTarget) (storage.ObservedDirectory, error) {
	s.seen = d
	return storage.ObservedDirectory{Observation: storage.DirectoryObservation{ParentID: d.NodeID, Revision: []byte{7}}, Entries: []storage.ObservedEntry{{RawLeaf: []byte{0xff}, Attr: s.captured}}}, nil
}
func (s *capabilityAuthorityStub) MutateNameWithBarrier(_ context.Context, c storage.NameCommand) (storage.NameResult, *httprest.MutationBarrier, error) {
	s.seen = c
	return storage.NameResult{Attr: &s.captured}, s.barrier, nil
}
func (s *capabilityAuthorityStub) SetMetadataWithBarrier(_ context.Context, id uint64, ns string, v, p []byte) (storage.OpaquePayload, *httprest.MutationBarrier, error) {
	s.seen = struct {
		ID               uint64
		Namespace        string
		Version, Payload []byte
	}{id, ns, v, p}
	return storage.OpaquePayload{Version: []byte{9}, Data: p}, s.barrier, nil
}
func (s *capabilityAuthorityStub) OpenNodeRefWithBarrier(_ context.Context, id uint64, o storage.NodeRefOptions) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
	s.seen = struct {
		ID      uint64
		Options storage.NodeRefOptions
	}{id, o}
	return storage.NodeOpenResult{Reference: s.reference, Attr: s.captured, Outcome: storage.Opened}, s.barrier, nil
}
func (s *capabilityAuthorityStub) OpenChildRefWithBarrier(_ context.Context, n storage.ChildName, o storage.NodeRefOptions) (storage.NodeOpenResult, *httprest.MutationBarrier, error) {
	s.seen = struct {
		Name    storage.ChildName
		Options storage.NodeRefOptions
	}{n, o}
	return storage.NodeOpenResult{Reference: s.reference, Attr: s.captured, Outcome: storage.Created}, s.barrier, nil
}
func (s *capabilityAuthorityStub) NewUseOwner(_ context.Context, id uint64, scope storage.UseScope, o storage.OwnerOptions) (storage.UseOwner, error) {
	s.seen = struct {
		ID      uint64
		Scope   storage.UseScope
		Options storage.OwnerOptions
	}{id, scope, o}
	return 73, nil
}
func (s *capabilityAuthorityStub) RetireUseOwner(_ context.Context, o storage.UseOwner) error {
	s.seen = o
	return nil
}
func (s *capabilityAuthorityStub) GetConflict(_ context.Context, o storage.UseOwner, c storage.RangeCommand) (storage.RangeConflict, error) {
	s.seen = struct {
		Owner   storage.UseOwner
		Command storage.RangeCommand
	}{o, c}
	return storage.RangeConflict{Found: true, Owner: 9, Range: c.Range, Mode: c.Mode}, nil
}
func (s *capabilityAuthorityStub) Apply(_ context.Context, o storage.UseOwner, c []storage.RangeCommand, id storage.LockRequestID) (storage.RangeAttempt, error) {
	s.seen = struct {
		Owner    storage.UseOwner
		Commands []storage.RangeCommand
		Request  storage.LockRequestID
	}{o, c, id}
	return storage.RangeAttempt{Request: id, State: storage.Granted, Commands: c, EverGranted: true}, nil
}
func (s *capabilityAuthorityStub) Query(_ context.Context, o storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	s.seen = o
	return storage.RangeAttempt{Request: id, State: storage.Granted}, nil
}
func (s *capabilityAuthorityStub) Cancel(_ context.Context, o storage.UseOwner, id storage.LockRequestID) (storage.RangeAttempt, error) {
	s.seen = o
	return storage.RangeAttempt{Request: id, State: storage.Cancelled}, nil
}
func (s *capabilityAuthorityStub) Drop(_ context.Context, o storage.UseOwner, d storage.ConflictDomain) error {
	s.seen = struct {
		Owner  storage.UseOwner
		Domain storage.ConflictDomain
	}{o, d}
	return nil
}

func TestCapabilitySessionPreservesCapturedAuthorityResults(t *testing.T) {
	remote := &capabilityAuthorityStub{fileSessionStub: &fileSessionStub{}, barrier: &httprest.MutationBarrier{Incarnation: "log"}, captured: storage.Attr{ID: 41, Kind: storage.NodeRegular, Size: 17}}
	closes := 0
	ref := &fileAuthorityStub{close: func(context.Context) error { closes++; return nil }}
	remote.file, remote.reference = ref, ref
	session := retainedTestSession(t, remote)
	name := storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 9}, RawLeaf: []byte{0xfe}}
	options := storage.OpenAtOptions{Read: true, Existing: storage.ReplaceNode, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 8}}
	opened, err := session.OpenAt(t.Context(), name, options)
	if err != nil || opened.Outcome != storage.Replaced || !reflect.DeepEqual(opened.Attr, remote.captured) {
		t.Fatalf("captured open: %+v, %v", opened, err)
	}
	if _, ok := opened.File.(*retainedFile); !ok {
		t.Fatal("atomic file bypassed replica wrapper")
	}
	if !reflect.DeepEqual(remote.seen, struct {
		Name    storage.ChildName
		Options storage.OpenAtOptions
	}{name, options}) {
		t.Fatalf("open conditions changed: %+v", remote.seen)
	}
	lookedUp, err := session.LookupAt(t.Context(), name)
	if err != nil || !reflect.DeepEqual(lookedUp, remote.captured) || !reflect.DeepEqual(remote.seen, name) {
		t.Fatalf("identity-relative lookup changed its name or result: %+v, %v", lookedUp, err)
	}
	observed, err := session.ReadDirNode(t.Context(), name.Parent)
	if err != nil || !reflect.DeepEqual(observed.Entries[0].RawLeaf, []byte{0xff}) || observed.Observation.ParentID != 9 {
		t.Fatalf("directory observation: %+v, %v", observed, err)
	}
	command := storage.NameCommand{Kind: storage.NameRemove, Name: name, Target: options.Target}
	result, err := session.MutateName(t.Context(), command)
	if err != nil || result.Attr.ID != 41 || !reflect.DeepEqual(remote.seen, command) {
		t.Fatalf("name result: %+v, %v", result, err)
	}
	symlink := storage.NameCommand{Kind: storage.NameSymlink, Name: name, Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{LinkTarget: []byte{0xfe, 0xff}}}
	if _, err := session.MutateName(t.Context(), symlink); err != nil || !reflect.DeepEqual(remote.seen, symlink) {
		t.Fatalf("symlink mutation changed raw target bytes: %+v, %v", remote.seen, err)
	}
	payload, err := session.SetMetadata(t.Context(), 41, "test.data", []byte{1}, []byte{0, 0xff})
	if err != nil || !reflect.DeepEqual(payload, storage.OpaquePayload{Version: []byte{9}, Data: []byte{0, 0xff}}) {
		t.Fatalf("metadata result: %+v, %v", payload, err)
	}
	if !reflect.DeepEqual(remote.seen, struct {
		ID               uint64
		Namespace        string
		Version, Payload []byte
	}{41, "test.data", []byte{1}, []byte{0, 0xff}}) {
		t.Fatalf("metadata conditions changed: %+v", remote.seen)
	}
	refOptions := storage.NodeRefOptions{Kind: storage.NodeDirectory, MetadataAccess: storage.ReadMetadata}
	node, err := session.OpenNodeRef(t.Context(), 41, refOptions)
	if err != nil || node.Outcome != storage.Opened || node.Attr.ID != 41 {
		t.Fatalf("identity ref: %+v, %v", node, err)
	}
	if _, ok := node.Reference.(*nodeReference); !ok {
		t.Fatal("identity reference bypassed wrapper")
	}
	child, err := session.OpenChildRef(t.Context(), name, refOptions)
	if err != nil || child.Outcome != storage.Created || child.Attr.ID != 41 {
		t.Fatalf("child ref: %+v, %v", child, err)
	}
	if !reflect.DeepEqual(remote.seen, struct {
		Name    storage.ChildName
		Options storage.NodeRefOptions
	}{name, refOptions}) {
		t.Fatalf("reference options changed: %+v", remote.seen)
	}
	if closes != 0 {
		t.Fatal("successful opens cleaned returned references")
	}
}

func TestCapabilitySessionControlsRemainAuthoritativeWithoutStream(t *testing.T) {
	remote := &capabilityAuthorityStub{fileSessionStub: &fileSessionStub{}}
	session := retainedTestSession(t, remote)
	session.base.failure = errors.New("stream lost")
	scope := storage.UseScope{Token: "retained"}
	owner, err := session.NewUseOwner(t.Context(), 8, scope, storage.OwnerOptions{Lifetime: storage.OwnerReference})
	if err != nil || owner != 73 {
		t.Fatalf("owner admission: %v, %v", owner, err)
	}
	if !reflect.DeepEqual(remote.seen, struct {
		ID      uint64
		Scope   storage.UseScope
		Options storage.OwnerOptions
	}{8, scope, storage.OwnerOptions{Lifetime: storage.OwnerReference}}) {
		t.Fatal("owner binding changed")
	}
	command := storage.RangeCommand{Domain: storage.DomainRecord, Edit: storage.Replace, Mode: storage.RangeExclusive, Range: storage.Range{Kind: storage.Bytes, Start: 3, Length: 8}}
	conflict, err := session.GetConflict(t.Context(), owner, command)
	if err != nil || !conflict.Found || conflict.Owner != 9 || conflict.Range != command.Range {
		t.Fatalf("conflict: %+v, %v", conflict, err)
	}
	request := storage.LockRequestID("1:0123456789abcdef0123456789abcdef")
	attempt, err := session.Apply(t.Context(), owner, []storage.RangeCommand{command}, request)
	if err != nil || attempt.State != storage.Granted || !attempt.EverGranted || attempt.Request != request {
		t.Fatalf("apply: %+v, %v", attempt, err)
	}
	if !reflect.DeepEqual(remote.seen, struct {
		Owner    storage.UseOwner
		Commands []storage.RangeCommand
		Request  storage.LockRequestID
	}{owner, []storage.RangeCommand{command}, request}) {
		t.Fatal("range action changed")
	}
	if got, err := session.Query(t.Context(), owner, request); err != nil || got.State != storage.Granted || got.Request != request {
		t.Fatalf("query: %+v, %v", got, err)
	}
	if got, err := session.Cancel(t.Context(), owner, request); err != nil || got.State != storage.Cancelled || got.Request != request {
		t.Fatalf("cancel: %+v, %v", got, err)
	}
	if err := session.Drop(t.Context(), owner, storage.DomainRecord); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(remote.seen, struct {
		Owner  storage.UseOwner
		Domain storage.ConflictDomain
	}{owner, storage.DomainRecord}) {
		t.Fatal("range drop changed")
	}
	if err := session.RetireUseOwner(t.Context(), owner); err != nil || remote.seen != owner {
		t.Fatalf("owner retirement: %v", err)
	}
	remote.seen = nil
	if _, err := session.LookupAt(t.Context(), storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 8}, RawLeaf: []byte{0xff}}); !errors.Is(err, syscall.EIO) || remote.seen != nil {
		t.Fatalf("unhealthy lookup was dispatched: %v", err)
	}
	if _, err := session.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: 8}); !errors.Is(err, syscall.EIO) || remote.seen != nil {
		t.Fatalf("unhealthy ordinary read was dispatched: %v", err)
	}
}

func TestCapabilityOpensCleanReferencesAfterFailedConfirmation(t *testing.T) {
	for _, kind := range []string{"file", "node", "child"} {
		t.Run(kind, func(t *testing.T) {
			closes := 0
			cause := errors.New("cleanup uncertain")
			ref := &fileAuthorityStub{close: func(context.Context) error {
				closes++
				if closes == 1 {
					return cause
				}
				return nil
			}}
			remote := &capabilityAuthorityStub{fileSessionStub: &fileSessionStub{}, file: ref, reference: ref}
			session := retainedTestSession(t, remote)
			var err error
			var pending interface{ Close(context.Context) error }
			switch kind {
			case "file":
				var r storage.OpenResult
				r, err = session.OpenAt(t.Context(), storage.ChildName{}, storage.OpenAtOptions{})
				if r.File == nil {
					t.Fatal("failed cleanup lost file ownership")
				}
				pending = r.File
			case "node":
				var r storage.NodeOpenResult
				r, err = session.OpenNodeRef(t.Context(), 1, storage.NodeRefOptions{})
				if r.Reference == nil {
					t.Fatal("failed cleanup lost node ownership")
				}
				pending = r.Reference
			case "child":
				var r storage.NodeOpenResult
				r, err = session.OpenChildRef(t.Context(), storage.ChildName{}, storage.NodeRefOptions{})
				if r.Reference == nil {
					t.Fatal("failed cleanup lost child ownership")
				}
				pending = r.Reference
			}
			if !errors.Is(err, syscall.EIO) || !errors.Is(err, cause) || closes != 1 {
				t.Fatalf("cleanup ownership lost: %v; closes %d", err, closes)
			}
			if err := pending.Close(t.Context()); err != nil || closes != 2 {
				t.Fatalf("retry cleanup: %v;closes %d", err, closes)
			}
		})
	}
}

func TestCapabilityChecksRequireUnderlyingSupport(t *testing.T) {
	remote := &capabilityAuthorityStub{fileSessionStub: &fileSessionStub{}, checkErr: errors.New("native unsupported")}
	session := retainedTestSession(t, remote)
	checks := []func() error{session.CheckAtomicFileOpen, session.CheckNamespaceAccess, session.CheckNodeReferences, session.CheckMetadataAccess, session.CheckUseOwners, session.CheckRangeControl}
	for _, check := range checks {
		if !errors.Is(check(), remote.checkErr) {
			t.Fatal("capability check discarded backing-chain error")
		}
	}
	session.remote = &fileSessionStub{}
	for _, check := range checks {
		if !errors.Is(check(), syscall.EOPNOTSUPP) {
			t.Fatal("capability check invented backing support")
		}
	}
	if _, err := session.LookupAt(t.Context(), storage.ChildName{}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported lookup: %v", err)
	}
	if _, err := session.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: 1}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("unsupported call: %v", err)
	}
}

func (s *capabilityAuthorityStub) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	observed, err := s.ReadDirNode(ctx, target)
	if err != nil {
		return storage.DirectoryObservation{}, result.Fail(err)
	}
	for _, entry := range observed.Entries {
		if err := result.Add(storage.Entry{Name: string(entry.RawLeaf), Attr: entry.Attr}); err != nil {
			return storage.DirectoryObservation{}, err
		}
	}
	return observed.Observation, nil
}

type bareCapabilityFile struct {
	storage.File
	closes int
}

func (f *bareCapabilityFile) Close(context.Context) error {
	f.closes++
	if f.closes == 1 {
		return syscall.EIO
	}
	return nil
}
func TestFailedCapabilityNegotiationPreservesCleanupOnlyResults(t *testing.T) {
	for _, kind := range []string{"file", "node", "child"} {
		t.Run(kind, func(t *testing.T) {
			native := &bareCapabilityFile{}
			remote := &capabilityAuthorityStub{fileSessionStub: &fileSessionStub{}, file: native, reference: native, barrier: &httprest.MutationBarrier{Incarnation: "log"}}
			session := retainedTestSession(t, remote)
			var cleanup storage.NodeReference
			var err error
			switch kind {
			case "file":
				var result storage.OpenResult
				result, err = session.OpenAt(t.Context(), storage.ChildName{}, storage.OpenAtOptions{})
				cleanup = result.File
				if result.File != nil {
					if _, e := result.File.ReadAt(t.Context(), 0, 1); !errors.Is(e, syscall.EIO) {
						t.Fatalf("failed file allowed read: %v", e)
					}
					if _, e := result.File.WriteAt(t.Context(), 0, nil); !errors.Is(e, syscall.EIO) {
						t.Fatalf("failed file allowed write: %v", e)
					}
					if _, e := result.File.Truncate(t.Context(), 0); !errors.Is(e, syscall.EIO) {
						t.Fatalf("failed file allowed truncate: %v", e)
					}
					if e := result.File.Sync(t.Context()); !errors.Is(e, syscall.EIO) {
						t.Fatalf("failed file allowed sync: %v", e)
					}
				}
			case "node":
				var result storage.NodeOpenResult
				result, err = session.OpenNodeRef(t.Context(), 1, storage.NodeRefOptions{})
				cleanup = result.Reference
			case "child":
				var result storage.NodeOpenResult
				result, err = session.OpenChildRef(t.Context(), storage.ChildName{}, storage.NodeRefOptions{})
				cleanup = result.Reference
			}
			if !errors.Is(err, syscall.EIO) || cleanup == nil || native.closes != 1 {
				t.Fatalf("negotiation lost cleanup: %v;ref=%v,closes=%d", err, cleanup, native.closes)
			}
			if _, err := cleanup.Stat(t.Context()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("cleanup-only stat: %v", err)
			}
			if _, err := cleanup.SetAttr(t.Context(), storage.AttrChange{}); !errors.Is(err, syscall.EIO) {
				t.Fatalf("cleanup-only setattr: %v", err)
			}
			if err := cleanup.Close(t.Context()); err != nil || native.closes != 2 {
				t.Fatalf("retry cleanup: %v;closes=%d", err, native.closes)
			}
		})
	}
}

func TestBoundedDirectoryCapabilityPreservesObservationAndRawNames(t *testing.T) {
	remote := &capabilityAuthorityStub{fileSessionStub: &fileSessionStub{}, captured: storage.Attr{ID: 8, Kind: storage.NodeRegular}}
	session := retainedTestSession(t, remote)
	result, err := storage.NewListResult(4096, 0, func(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
		return 256 + nameBytes + metadataBytes, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	target := storage.DirectoryTarget{NodeID: 1}
	observation, err := session.ReadDirNodeBounded(t.Context(), target, result)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil || len(entries) != 1 || entries[0].Name != string([]byte{0xff}) || observation.ParentID != 1 || !reflect.DeepEqual(remote.seen, target) {
		t.Fatalf("bounded directory changed: %+v,%+v,%v", observation, entries, err)
	}
}
