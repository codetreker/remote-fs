package objectstore

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type informationNativeProbe struct {
	id               uint64
	checkErr, getErr error
	information      storage.ReferenceInformation
	observation      storage.ContentMetadataObservation
	hook             func(context.Context)
	index            uint16
}

type informationFileNative struct {
	metastore.File
	*informationNativeProbe
}

type informationNodeNative struct {
	metastore.NodeReference
	*informationNativeProbe
}

func (p *informationNativeProbe) ReferenceNodeID() (uint64, error) { return p.id, nil }
func (p *informationNativeProbe) CheckReferenceInformation() error { return p.checkErr }
func (p *informationNativeProbe) CheckMetadataObservation() error  { return p.checkErr }
func (p *informationNativeProbe) Information(ctx context.Context) (storage.ReferenceInformation, error) {
	if p.hook != nil {
		p.hook(ctx)
	}
	return p.information, p.getErr
}
func (p *informationNativeProbe) ObserveMetadata(ctx context.Context, index uint16) (storage.ContentMetadataObservation, error) {
	p.index = index
	if p.hook != nil {
		p.hook(ctx)
	}
	return p.observation, p.getErr
}

type informationEnrollmentProbe struct {
	*identityCapabilityProbe
	checkErr    error
	geometry    storage.VolumeGeometry
	geometryErr error
	hook        func()
}

func (p *informationEnrollmentProbe) CheckOpenMetadataObservation() error { return p.checkErr }
func (p *informationEnrollmentProbe) CheckVolumeGeometry() error          { return p.checkErr }
func (p *informationEnrollmentProbe) VolumeGeometry(context.Context) (storage.VolumeGeometry, error) {
	if p.hook != nil {
		p.hook()
	}
	return p.geometry, p.geometryErr
}

func TestObjectInformationEnrollmentRequiresCompleteNativeChain(t *testing.T) {
	missing := &identityAuthorityProbe{}
	session := &fileSession{native: missing, storage: &Storage{meta: missing, objects: struct{ BoundedObjects }{}}}
	for _, check := range []func() error{session.CheckOpenMetadataObservation, session.CheckVolumeGeometry} {
		if err := check(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatal(err)
		}
	}
	for _, native := range []any{nil, &struct{}{}} {
		if _, _, err := informationCapability(native); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatal(err)
		}
		if _, _, err := metadataObservationCapability(native); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatal(err)
		}
	}
	cause := errors.New("native enrollment refused")
	native := &informationEnrollmentProbe{identityCapabilityProbe: &identityCapabilityProbe{identityAuthorityProbe: missing}, checkErr: cause}
	session.native, session.storage.meta = native, native
	session.hasBackendIdentity = true
	session.backendIdentity = storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}
	for _, check := range []func() error{session.CheckOpenMetadataObservation, session.CheckVolumeGeometry} {
		if err := check(); !errors.Is(err, cause) {
			t.Fatal(err)
		}
	}
	native.checkErr = nil
	missing.fileErr = cause
	for _, check := range []func() error{session.CheckOpenMetadataObservation, session.CheckVolumeGeometry} {
		if err := check(); !errors.Is(err, cause) {
			t.Fatal(err)
		}
	}
}

func TestObjectMetadataEnrollmentRefusesBeforeNativeOpen(t *testing.T) {
	for _, refusal := range []error{syscall.EOPNOTSUPP, errors.New("native enrollment refused")} {
		session := actionTestSession(t, 10)
		missing := &identityAuthorityProbe{}
		session.native = missing
		if !errors.Is(refusal, syscall.EOPNOTSUPP) {
			session.native = &informationEnrollmentProbe{identityCapabilityProbe: &identityCapabilityProbe{identityAuthorityProbe: missing}, checkErr: refusal}
		}
		selection := storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("file")}}
		scopes := []storage.MetadataObservationScope{{Namespace: "test.flags", MaxPayloadBytes: 8}}
		fileOptions := storage.OpenAtOptions{Write: true, MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes, Target: storage.ChildCondition{State: storage.Any}, Existing: storage.Keep, Action: actionID(t, session), Use: storage.UseClaim{Uses: storage.WriteData}}
		if got, err := session.OpenAt(t.Context(), selection, fileOptions); !errors.Is(err, refusal) || got.File != nil || got.Attr.ID != 0 {
			t.Fatalf("OpenAt=%+v,%v", got, err)
		}
		nodeOptions := storage.NodeRefOptions{Kind: storage.NodeRegular, MetadataAccess: storage.WriteMetadata, MetadataObservations: scopes, Target: storage.ChildCondition{State: storage.SameNode, NodeID: 19}, Action: actionID(t, session)}
		if got, err := session.OpenNodeRef(t.Context(), 19, nodeOptions); !errors.Is(err, refusal) || got.Reference != nil || got.Attr.ID != 0 {
			t.Fatalf("OpenNodeRef=%+v,%v", got, err)
		}
		nodeOptions.Action = actionID(t, session)
		if got, err := session.OpenChildRef(t.Context(), selection, nodeOptions); !errors.Is(err, refusal) || got.Reference != nil || got.Attr.ID != 0 {
			t.Fatalf("OpenChildRef=%+v,%v", got, err)
		}
		if len(session.actions) != 0 {
			t.Fatal("failed enrollment consumed recovery history")
		}
	}
}

func TestObjectReferenceInformationCapabilityChecks(t *testing.T) {
	for _, cause := range []error{nil, errors.New("native check failed")} {
		probe := &informationNativeProbe{id: 19, checkErr: cause}
		file := &openFile{native: &informationFileNative{informationNativeProbe: probe}}
		ref := &nodeReference{native: &informationNodeNative{informationNativeProbe: probe}}
		for _, check := range []func() error{file.CheckReferenceInformation, ref.CheckReferenceInformation, file.CheckMetadataObservation, ref.CheckMetadataObservation} {
			if err := check(); err != cause {
				t.Fatalf("capability check=%v;want %v", err, cause)
			}
		}
	}
}

func TestObjectReferenceInformationRejectsFailuresAndRetirement(t *testing.T) {
	for _, node := range []bool{false, true} {
		for _, scenario := range []string{"valid", "identity", "changedidentity", "size", "allocation", "check", "getter", "cancel", "retired", "session"} {
			t.Run(scenario+map[bool]string{false: "/file", true: "/node"}[node], func(t *testing.T) {
				session := actionTestSession(t, 10)
				probe := &informationNativeProbe{id: 19, information: storage.ReferenceInformation{NodeID: 19, Kind: storage.NodeRegular, Size: 4, AllocationSize: 4096, AllocationKnown: true}}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				file := &openFile{session: session, native: &informationFileNative{informationNativeProbe: probe}, active: true}
				ref := &nodeReference{session: session, native: &informationNodeNative{informationNativeProbe: probe}, active: true}
				cause := errors.New("native failed")
				var want error
				switch scenario {
				case "identity":
					probe.information.NodeID++
					want = syscall.EIO
				case "changedidentity":
					probe.hook = func(context.Context) { probe.id++ }
					want = syscall.EIO
				case "size":
					probe.information.Size = -1
					want = syscall.EIO
				case "allocation":
					probe.information.AllocationKnown = false
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
				var got storage.ReferenceInformation
				var err error
				if node {
					got, err = ref.Information(ctx)
				} else {
					got, err = file.Information(ctx)
				}
				if want != nil {
					if !errors.Is(err, want) || got != (storage.ReferenceInformation{}) {
						t.Fatalf("got=%+v,%v; want %v", got, err, want)
					}
				} else if err != nil || got != probe.information {
					t.Fatalf("got=%+v,%v", got, err)
				}
			})
		}
	}
}

func TestObjectMetadataObservationEnforcesScopesAndCopiesNativeBytes(t *testing.T) {
	for _, node := range []bool{false, true} {
		for _, scenario := range []string{"valid", "absent", "empty", "identity", "changedidentity", "version", "oversize", "permission", "index", "check", "getter", "cancel", "retired", "session"} {
			t.Run(scenario+map[bool]string{false: "/file", true: "/node"}[node], func(t *testing.T) {
				session := actionTestSession(t, 10)
				probe := &informationNativeProbe{id: 19, observation: storage.ContentMetadataObservation{NodeID: 19, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{2, 3}}}}
				scopes := []storage.MetadataObservationScope{{Namespace: "test.flags", MaxPayloadBytes: 2}}
				file := &openFile{session: session, native: &informationFileNative{informationNativeProbe: probe}, metadata: storage.WriteMetadata, metadataObservations: scopes, active: true}
				ref := &nodeReference{session: session, native: &informationNodeNative{informationNativeProbe: probe}, metadata: storage.WriteMetadata, metadataObservations: scopes, active: true}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				index := uint16(0)
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
					index = 1
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
				var got storage.ContentMetadataObservation
				var err error
				if node {
					got, err = ref.ObserveMetadata(ctx, index)
				} else {
					got, err = file.ObserveMetadata(ctx, index)
				}
				if want != nil {
					if !errors.Is(err, want) || !reflect.DeepEqual(got, storage.ContentMetadataObservation{}) {
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

func TestObjectVolumeGeometryBindsBackendBeforeAndAfterNativeCall(t *testing.T) {
	for _, scenario := range []string{"valid", "volume", "zero", "getter", "before", "after", "session", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			session := actionTestSession(t, 10)
			session.cleanup = context.Background()
			session.files = make(map[retainedReference]struct{})
			session.options.MaxOperations = 1
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			backend := storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}
			probe := &informationEnrollmentProbe{identityCapabilityProbe: &identityCapabilityProbe{identityAuthorityProbe: &identityAuthorityProbe{}, result: backend}, geometry: storage.VolumeGeometry{Volume: backend.Volume, BytesPerSector: 4096, SectorsPerAllocationUnit: 1}}
			session.native, session.storage.meta = probe, probe
			session.storage.objects = struct{ BoundedObjects }{}
			session.backendIdentity = backend
			session.hasBackendIdentity = true
			session.epoch = "session"
			var want error
			switch scenario {
			case "volume":
				probe.geometry.Volume = "different"
				want = syscall.EIO
			case "zero":
				probe.geometry.BytesPerSector = 0
				want = syscall.EINVAL
			case "getter":
				probe.geometryErr = syscall.EIO
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
			got, err := session.VolumeGeometry(ctx)
			if want != nil {
				if !errors.Is(err, want) || got != (storage.VolumeGeometry{}) {
					t.Fatalf("got=%+v,%v;want=%v", got, err, want)
				}
			} else if err != nil || got != probe.geometry {
				t.Fatalf("got=%+v,%v", got, err)
			}
		})
	}
}

func TestObjectObservationEnrollmentDigestOwnsEveryOpenPath(t *testing.T) {
	scopes := []storage.MetadataObservationScope{{Namespace: "test.flags", MaxPayloadBytes: 8}}
	for _, input := range []any{openAtActionInput{Options: storage.OpenAtOptions{MetadataObservations: scopes}}, nodeRefActionInput{Node: 19, Options: storage.NodeRefOptions{MetadataObservations: scopes}}, nodeRefActionInput{Selection: &storage.ChildSelection{}, Options: storage.NodeRefOptions{MetadataObservations: scopes}}} {
		canonical := canonicalFileActionInput(input)
		before, err := fileActionDigest(storage.OpFileOpenAt, canonical)
		if err != nil {
			t.Fatal(err)
		}
		scopes[0].MaxPayloadBytes++
		after, err := fileActionDigest(storage.OpFileOpenAt, canonical)
		if err != nil {
			t.Fatal(err)
		}
		changed, err := fileActionDigest(storage.OpFileOpenAt, input)
		if err != nil {
			t.Fatal(err)
		}
		if before != after || before == changed {
			t.Fatal("scope digest lost enrollment or retained alias")
		}
	}
}
