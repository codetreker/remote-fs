package objectstore

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type referenceObservationNativeProbe struct {
	id               uint64
	checkErr, getErr error
	state            metastore.ReferenceState
	observation      storage.MetadataObservation
	hook             func(context.Context)
	request          storage.MetadataObservationRequest
}

type referenceObservationFileNative struct {
	metastore.File
	*referenceObservationNativeProbe
}

type referenceObservationNodeNative struct {
	metastore.NodeReference
	*referenceObservationNativeProbe
}

func (p *referenceObservationFileNative) CheckMetadataAccess() error {
	return p.referenceObservationNativeProbe.CheckMetadataAccess()
}
func (p *referenceObservationFileNative) ObserveMetadata(ctx context.Context, request storage.MetadataObservationRequest) (storage.MetadataObservation, error) {
	return p.referenceObservationNativeProbe.ObserveMetadata(ctx, request)
}
func (p *referenceObservationNodeNative) CheckReferenceState() error {
	return p.referenceObservationNativeProbe.CheckReferenceState()
}
func (p *referenceObservationNodeNative) State(ctx context.Context, projection storage.StateProjection) (metastore.ReferenceState, error) {
	return p.referenceObservationNativeProbe.State(ctx, projection)
}
func (p *referenceObservationNodeNative) CheckMetadataAccess() error {
	return p.referenceObservationNativeProbe.CheckMetadataAccess()
}
func (p *referenceObservationNodeNative) ObserveMetadata(ctx context.Context, request storage.MetadataObservationRequest) (storage.MetadataObservation, error) {
	return p.referenceObservationNativeProbe.ObserveMetadata(ctx, request)
}

func (p *referenceObservationNativeProbe) ReferenceNodeID() (uint64, error) { return p.id, nil }
func (p *referenceObservationNativeProbe) CheckReferenceState() error       { return p.checkErr }
func (p *referenceObservationNativeProbe) CheckMetadataAccess() error       { return p.checkErr }
func (p *referenceObservationNativeProbe) State(ctx context.Context, projection storage.StateProjection) (metastore.ReferenceState, error) {
	if p.hook != nil {
		p.hook(ctx)
	}
	return p.state, p.getErr
}
func (p *referenceObservationNativeProbe) ObserveMetadata(ctx context.Context, request storage.MetadataObservationRequest) (storage.MetadataObservation, error) {
	p.request = request
	if p.hook != nil {
		p.hook(ctx)
	}
	return p.observation, p.getErr
}

type allocationUnitNativeProbe struct {
	*identityCapabilityProbe
	checkErr error
	unit     uint64
	unitErr  error
	hook     func()
}

func (p *allocationUnitNativeProbe) CheckAllocationReporting() error { return p.checkErr }
func (p *allocationUnitNativeProbe) AllocationUnitBytes(context.Context) (uint64, error) {
	if p.hook != nil {
		p.hook()
	}
	return p.unit, p.unitErr
}

func TestObjectAllocationReportingFollowsMetadataAuthority(t *testing.T) {
	missing := &identityAuthorityProbe{}
	session := &fileSession{native: missing, storage: &Storage{meta: missing, objects: struct{ BoundedObjects }{}}}
	for _, check := range []func() error{session.CheckAllocationReporting} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatal(err)
		}
	}
	cause := errors.New("native enrollment refused")
	native := &allocationUnitNativeProbe{identityCapabilityProbe: &identityCapabilityProbe{identityAuthorityProbe: missing}, checkErr: cause}
	session.native, session.storage.meta = native, native
	session.hasBackendIdentity = true
	session.backendIdentity = storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}
	for _, check := range []func() error{session.CheckAllocationReporting} {
		if err := check(); !errors.Is(err, cause) {
			t.Fatal(err)
		}
	}
	native.checkErr = nil
	missing.fileErr = cause
	for _, check := range []func() error{session.CheckAllocationReporting} {
		if err := check(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestObjectReferenceStateAndMetadataCapabilityChecks(t *testing.T) {
	for _, cause := range []error{nil, errors.New("native check failed")} {
		probe := &referenceObservationNativeProbe{id: 19, checkErr: cause}
		file := &openFile{native: &referenceObservationFileNative{referenceObservationNativeProbe: probe}}
		ref := &nodeReference{native: &referenceObservationNodeNative{referenceObservationNativeProbe: probe}}
		for _, check := range []func() error{file.CheckReferenceState, ref.CheckReferenceState, file.CheckMetadataAccess, ref.CheckMetadataAccess} {
			if err := check(); err != cause {
				t.Fatalf("capability check=%v;want %v", err, cause)
			}
		}
	}
}

func TestObjectReferenceScalarStateRejectsFailuresAndRetirement(t *testing.T) {
	for _, node := range []bool{false, true} {
		for _, scenario := range []string{"valid", "identity", "changedidentity", "size", "allocation", "check", "getter", "cancel", "retired", "session"} {
			t.Run(scenario+map[bool]string{false: "/file", true: "/node"}[node], func(t *testing.T) {
				session := actionTestSession(t, 10)
				probe := &referenceObservationNativeProbe{id: 19, state: metastore.ReferenceState{State: metastore.FileState{Node: metastore.Node{ID: 19, Kind: storage.NodeRegular, Size: 4, AllocationSize: 4096, AllocationKnown: true}}}}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				file := &openFile{session: session, native: &referenceObservationFileNative{referenceObservationNativeProbe: probe}, active: true}
				ref := &nodeReference{session: session, native: &referenceObservationNodeNative{referenceObservationNativeProbe: probe}, active: true}
				cause := errors.New("native failed")
				var want error
				switch scenario {
				case "identity":
					probe.state.State.ID++
					want = syscall.EIO
				case "changedidentity":
					probe.hook = func(context.Context) { probe.id++ }
					want = syscall.EIO
				case "size":
					probe.state.State.Size = -1
					want = syscall.EIO
				case "allocation":
					probe.state.State.AllocationKnown = false
					want = syscall.EIO
				case "check":
					probe.checkErr = cause
					want = cause
				case "getter":
					probe.getErr = cause
					want = cause
				case "cancel":
					probe.hook = func(context.Context) { cancel() }
					want = context.Canceled
				case "retired":
					probe.hook = func(context.Context) { session.mu.Lock(); file.active, ref.active = false, false; session.mu.Unlock() }
					want = syscall.EBADF
				case "session":
					probe.hook = func(context.Context) { session.mu.Lock(); session.active = false; session.mu.Unlock() }
					want = syscall.ESTALE
				}
				var got storage.ReferenceState
				var err error
				if node {
					got, err = ref.State(ctx, storage.StateScalars)
				} else {
					got, err = file.State(ctx, storage.StateScalars)
				}
				if want != nil {
					if !errors.Is(err, want) || !reflect.DeepEqual(got, storage.ReferenceState{}) {
						t.Fatalf("got=%+v,%v; want %v", got, err, want)
					}
				} else if err != nil || !reflect.DeepEqual(got, publicReferenceState(probe.state)) {
					t.Fatalf("got=%+v,%v", got, err)
				}
			})
		}
	}
}

func TestObjectScalarStateRejectsNativeProtectedFacts(t *testing.T) {
	timestamp := time.Unix(1, 0)
	cases := []struct {
		name   string
		expose func(*metastore.ReferenceState)
	}{
		{"metadata", func(s *metastore.ReferenceState) { s.State.Metadata = map[string]storage.OpaquePayload{} }},
		{"birthtime", func(s *metastore.ReferenceState) { s.State.BirthTime = &timestamp }},
		{"changetime", func(s *metastore.ReferenceState) { s.State.ChangeTime = &timestamp }},
		{"accesstime", func(s *metastore.ReferenceState) { s.State.AccessTime = timestamp }},
		{"modtime", func(s *metastore.ReferenceState) { s.State.ModTime = timestamp }},
		{"node_link", func(s *metastore.ReferenceState) { s.State.LinkTarget = []byte{} }},
		{"directory_revision", func(s *metastore.ReferenceState) { s.State.DirectoryRevision = []byte{} }},
		{"content", func(s *metastore.ReferenceState) { s.State.Content = metastore.Key("private-object") }},
		{"revision", func(s *metastore.ReferenceState) { s.State.Revision = 1 }},
		{"link", func(s *metastore.ReferenceState) { s.LinkTarget = []byte{} }},
		{"pending_generation", func(s *metastore.ReferenceState) { s.PendingGeneration = []byte{} }},
	}
	for _, node := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(tc.name+map[bool]string{false: "/file", true: "/node"}[node], func(t *testing.T) {
				session := actionTestSession(t, 10)
				probe := &referenceObservationNativeProbe{id: 19, state: metastore.ReferenceState{State: metastore.FileState{Node: metastore.Node{ID: 19, Kind: storage.NodeRegular, AllocationKnown: true}}}}
				tc.expose(&probe.state)
				var access storage.ReferenceStateAccess
				if node {
					access = &nodeReference{session: session, native: &referenceObservationNodeNative{referenceObservationNativeProbe: probe}, active: true}
				} else {
					access = &openFile{session: session, native: &referenceObservationFileNative{referenceObservationNativeProbe: probe}, active: true}
				}
				if got, err := access.State(t.Context(), storage.StateScalars); !errors.Is(err, syscall.EIO) || !reflect.DeepEqual(got, storage.ReferenceState{}) {
					t.Fatalf("protected state=%+v,%v", got, err)
				}
			})
		}
	}
}

func TestObjectReferenceStateRejectsInvalidProjectionBeforeNativeCall(t *testing.T) {
	for _, projection := range []storage.StateProjection{0, 99} {
		for _, node := range []bool{false, true} {
			session := actionTestSession(t, 10)
			probe := &referenceObservationNativeProbe{id: 19, hook: func(context.Context) { t.Fatal("invalid projection reached native state") }}
			var access storage.ReferenceStateAccess
			if node {
				access = &nodeReference{session: session, native: &referenceObservationNodeNative{referenceObservationNativeProbe: probe}, active: true}
			} else {
				access = &openFile{session: session, native: &referenceObservationFileNative{referenceObservationNativeProbe: probe}, active: true}
			}
			if got, err := access.State(t.Context(), projection); !errors.Is(err, syscall.EINVAL) || !reflect.DeepEqual(got, storage.ReferenceState{}) {
				t.Fatalf("projection=%d,node=%t,state=%+v,%v", projection, node, got, err)
			}
		}
	}
}

func TestObjectMetadataObservationEnforcesBoundsAndCopiesNativeBytes(t *testing.T) {
	for _, node := range []bool{false, true} {
		for _, scenario := range []string{"valid", "absent", "empty", "identity", "changedidentity", "version", "oversize", "permission", "index", "check", "getter", "cancel", "retired", "session"} {
			t.Run(scenario+map[bool]string{false: "/file", true: "/node"}[node], func(t *testing.T) {
				session := actionTestSession(t, 10)
				probe := &referenceObservationNativeProbe{id: 19, observation: storage.MetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2, 3}}}}
				request := storage.MetadataObservationRequest{Purpose: storage.MetadataObservationNamespace, Namespace: "test.flags", MaxPayloadBytes: 2}
				file := &openFile{session: session, native: &referenceObservationFileNative{referenceObservationNativeProbe: probe}, metadata: storage.WriteMetadata, active: true}
				ref := &nodeReference{session: session, native: &referenceObservationNodeNative{referenceObservationNativeProbe: probe}, metadata: storage.WriteMetadata, active: true}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				cause := errors.New("native failed")
				var want error
				switch scenario {
				case "absent":
					probe.observation.Value = nil
				case "empty":
					probe.observation.Value.Data = nil
				case "identity":
					probe.observation.NodeID++
					want = syscall.EIO
				case "changedidentity":
					probe.hook = func(context.Context) { probe.id++ }
					want = syscall.EIO
				case "version":
					probe.observation.Value.Version = nil
					want = syscall.EIO
				case "oversize":
					probe.observation.Value.Data = []byte{1, 2, 3}
					want = syscall.EFBIG
				case "permission":
					file.metadata, ref.metadata = storage.ReadMetadata, storage.ReadMetadata
					want = syscall.EBADF
				case "index":
					request.EffectIndex = 1
					want = syscall.EINVAL
				case "check":
					probe.checkErr = cause
					want = cause
				case "getter":
					probe.getErr = cause
					want = cause
				case "cancel":
					probe.hook = func(context.Context) { cancel() }
					want = context.Canceled
				case "retired":
					probe.hook = func(context.Context) { session.mu.Lock(); file.active, ref.active = false, false; session.mu.Unlock() }
					want = syscall.EBADF
				case "session":
					probe.hook = func(context.Context) { session.mu.Lock(); session.active = false; session.mu.Unlock() }
					want = syscall.ESTALE
				}
				var got storage.MetadataObservation
				var err error
				if node {
					got, err = ref.ObserveMetadata(ctx, request)
				} else {
					got, err = file.ObserveMetadata(ctx, request)
				}
				if want != nil {
					if !errors.Is(err, want) || !reflect.DeepEqual(got, storage.MetadataObservation{}) {
						t.Fatalf("got=%+v,%v;want=%v", got, err, want)
					}
				} else {
					if err != nil || !reflect.DeepEqual(got, probe.observation) {
						t.Fatalf("got=%+v,%v", got, err)
					}
					if got.Value != nil {
						got.Value.Version[0] = 9
						if probe.observation.Value.Version[0] != 1 {
							t.Fatal("version alias")
						}
						if len(got.Value.Data) != 0 {
							got.Value.Data[0] = 9
							if probe.observation.Value.Data[0] != 2 {
								t.Fatal("data alias")
							}
						}
					}
				}
			})
		}
	}
}

func TestObjectAllocationUnitBindsBackendBeforeAndAfterNativeCall(t *testing.T) {
	for _, scenario := range []string{"valid", "zero", "getter", "before", "after", "session", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			session := actionTestSession(t, 10)
			session.cleanup = context.Background()
			session.files = make(map[retainedReference]struct{})
			session.options.MaxOperations = 1
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			backend := storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}
			probe := &allocationUnitNativeProbe{identityCapabilityProbe: &identityCapabilityProbe{identityAuthorityProbe: &identityAuthorityProbe{}, result: backend}, unit: 4096}
			session.native, session.storage.meta = probe, probe
			session.storage.objects = struct{ BoundedObjects }{}
			session.backendIdentity = backend
			session.hasBackendIdentity = true
			session.epoch = "session"
			var want error
			switch scenario {
			case "zero":
				probe.unit = 0
				want = syscall.EIO
			case "getter":
				probe.unitErr = syscall.EIO
				want = syscall.EIO
			case "before":
				probe.result.Authority = "different"
				want = syscall.ESTALE
			case "after":
				probe.hook = func() { probe.result.Authority = "different" }
				want = syscall.ESTALE
			case "session":
				probe.hook = func() { session.mu.Lock(); session.active = false; session.mu.Unlock() }
				want = syscall.ESTALE
			case "cancel":
				probe.hook = func() { cancel() }
				want = context.Canceled
			}
			got, err := session.AllocationUnitBytes(ctx)
			if want != nil {
				if !errors.Is(err, want) || got != 0 {
					t.Fatalf("got=%+v,%v;want=%v", got, err, want)
				}
			} else if err != nil || got != probe.unit {
				t.Fatalf("got=%+v,%v", got, err)
			}
		})
	}
}
