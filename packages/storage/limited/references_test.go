package limited

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type referenceProbe struct {
	storage.File
	probe  *capabilityProbe
	closed int
}

func (r *referenceProbe) Stat(context.Context) (storage.Attr, error) {
	return r.probe.state.Attr, r.probe.callError
}
func (r *referenceProbe) SetAttr(ctx context.Context, change storage.AttrChange) (storage.Attr, error) {
	r.probe.record("attributes", change)
	return r.probe.state.Attr, r.probe.publish(ctx)
}
func (r *referenceProbe) Close(context.Context) error         { r.closed++; return r.probe.callError }
func (r *referenceProbe) CheckReferenceState() error          { return r.probe.checkError }
func (r *referenceProbe) CheckScopedReference() error         { return r.probe.checkError }
func (r *referenceProbe) CheckMetadataAccess() error          { return r.probe.checkError }
func (r *referenceProbe) CheckDeleteIntent() error            { return r.probe.checkError }
func (r *referenceProbe) CheckConditionalFileMutation() error { return r.probe.checkError }
func (r *referenceProbe) State(context.Context) (storage.ReferenceState, error) {
	return r.probe.state, r.probe.callError
}
func (r *referenceProbe) Scope(context.Context) (storage.UseScope, error) {
	return storage.UseScope{Token: "bound-reference"}, r.probe.callError
}
func (r *referenceProbe) SetMetadata(ctx context.Context, namespace string, version, data []byte) (storage.OpaquePayload, error) {
	r.probe.record("metadata", struct {
		Namespace     string
		Version, Data []byte
	}{namespace, version, data})
	return r.probe.payload, r.probe.publish(ctx)
}
func (r *referenceProbe) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	r.probe.record("pending", command)
	return r.probe.state, r.probe.publish(ctx)
}
func (r *referenceProbe) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	r.probe.record("clear", command)
	return r.probe.state, r.probe.publish(ctx)
}
func (r *referenceProbe) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	r.probe.record("conditional", command)
	return r.probe.state.Attr, r.probe.publish(ctx)
}

func TestOptionalReferencesPreserveCheckedCapabilitiesAndPartialOwnership(t *testing.T) {
	failure := errors.New("known applied open failed confirmation")
	for _, kind := range []string{"file", "node", "child"} {
		t.Run(kind, func(t *testing.T) {
			p := &capabilityProbe{callError: failure, before: 1024, after: 512, state: storage.ReferenceState{Attr: storage.Attr{ID: 31, Kind: storage.NodeRegular, Size: 512}, Detached: true, PendingUnlink: true, PendingGeneration: []byte{7}}}
			native := &referenceProbe{probe: p}
			p.open = storage.OpenResult{File: native, Attr: p.state.Attr, Outcome: storage.Reset}
			p.node = storage.NodeOpenResult{Reference: native, Attr: p.state.Attr, Outcome: storage.Opened}
			s := &fileSession{FileSession: p, storage: &Storage{limit: MinLimit, count: 1024}}
			var reference storage.NodeReference
			switch kind {
			case "file":
				result, err := s.OpenAt(t.Context(), storage.ChildName{}, storage.OpenAtOptions{})
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
				reference = result.File
			case "node":
				result, err := s.OpenNodeRef(t.Context(), 31, storage.NodeRefOptions{})
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
				reference = result.Reference
			case "child":
				result, err := s.OpenChildRef(t.Context(), storage.ChildName{}, storage.NodeRefOptions{})
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
				reference = result.Reference
			}
			if reference == nil || reference == native {
				t.Fatal("partial ownership was lost or bypassed wrapper")
			}
			requireCount(t, s.storage, 512)
			state := reference.(storage.ReferenceStateAccess)
			if err := state.CheckReferenceState(); err != nil {
				t.Fatal(err)
			}
			got, err := state.State(t.Context())
			if !errors.Is(err, failure) || !reflect.DeepEqual(got, p.state) {
				t.Fatalf("state=%+v,%v", got, err)
			}
			scoped := reference.(storage.ScopedReference)
			if err := scoped.CheckScopedReference(); err != nil {
				t.Fatal(err)
			}
			scope, err := scoped.Scope(t.Context())
			if !errors.Is(err, failure) || scope.Token != "bound-reference" {
				t.Fatalf("scope=%+v,%v", scope, err)
			}
			if err := reference.Close(t.Context()); !errors.Is(err, failure) || native.closed != 1 {
				t.Fatalf("partial close=%v,calls=%d", err, native.closed)
			}
		})
	}
}

func TestOptionalReferenceMutationsCannotBypassNativeQuotaAccounting(t *testing.T) {
	for _, kind := range []string{"file", "node"} {
		for _, method := range []string{"attributes", "metadata", "pending", "clear", "conditional"} {
			t.Run(kind+"/"+method, func(t *testing.T) {
				p := &capabilityProbe{before: 512, after: MinLimit + 1, state: storage.ReferenceState{Attr: storage.Attr{ID: 31, Kind: storage.NodeRegular, Size: 512}}, payload: storage.OpaquePayload{Version: []byte{2}, Data: []byte{9}}}
				s := &Storage{limit: MinLimit, count: 512}
				native := &referenceProbe{probe: p}
				var reference storage.NodeReference = wrapNodeReference(s, native)
				if kind == "file" {
					reference = wrapFile(s, native)
				}
				var call func() error
				switch method {
				case "attributes":
					call = func() error { _, err := reference.SetAttr(t.Context(), storage.AttrChange{}); return err }
				case "metadata":
					access := reference.(storage.ReferenceMetadataAccess)
					if err := access.CheckMetadataAccess(); err != nil {
						t.Fatal(err)
					}
					call = func() error { _, err := access.SetMetadata(t.Context(), "app.tag", []byte{1}, []byte{9}); return err }
				case "pending":
					access := reference.(storage.DeleteIntent)
					if err := access.CheckDeleteIntent(); err != nil {
						t.Fatal(err)
					}
					call = func() error {
						_, err := access.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Condition: storage.UnlinkFile})
						return err
					}
				case "clear":
					access := reference.(storage.DeleteIntent)
					call = func() error {
						_, err := access.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Generation: []byte{4}})
						return err
					}
				case "conditional":
					access := reference.(storage.ConditionalFileMutation)
					if err := access.CheckConditionalFileMutation(); err != nil {
						t.Fatal(err)
					}
					call = func() error {
						_, err := access.MutateFile(t.Context(), storage.FileMutation{Kind: storage.MutateTruncate, Size: MinLimit + 1})
						return err
					}
				}
				if err := call(); !errors.Is(err, syscall.EDQUOT) {
					t.Fatalf("overquota mutation=%v", err)
				}
				requireCount(t, s, 512)
				p.after = 1024
				if err := call(); err != nil {
					t.Fatal(err)
				}
				requireCount(t, s, 1024)
				if len(p.calls) != 2 || p.calls[0].name != method || !reflect.DeepEqual(p.calls[0], p.calls[1]) {
					t.Fatalf("mutation inputs changed: %+v", p.calls)
				}
			})
		}
	}
}

func TestOptionalReferenceChecksRefuseMissingAndBrokenBacking(t *testing.T) {
	failure := errors.New("reference capability check failed")
	for _, test := range []struct {
		name    string
		backing storage.NodeReference
		want    error
	}{
		{"missing", &struct{ storage.NodeReference }{}, syscall.EOPNOTSUPP},
		{"failed", &referenceProbe{probe: &capabilityProbe{checkError: failure}}, failure},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := wrapNodeReference(&Storage{limit: MinLimit}, test.backing).(*nodeReference)
			checks := []func() error{r.CheckReferenceState, r.CheckScopedReference, r.CheckMetadataAccess, r.CheckDeleteIntent, r.CheckConditionalFileMutation}
			for _, check := range checks {
				if err := check(); !errors.Is(err, test.want) {
					t.Fatalf("check=%v", err)
				}
			}
			calls := []func() error{
				func() error { _, e := r.State(t.Context()); return e },
				func() error { _, e := r.Scope(t.Context()); return e },
				func() error { _, e := r.SetMetadata(t.Context(), "app.tag", nil, nil); return e },
				func() error { _, e := r.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{}); return e },
				func() error { _, e := r.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{}); return e },
				func() error { _, e := r.MutateFile(t.Context(), storage.FileMutation{}); return e },
			}
			for _, call := range calls {
				if err := call(); !errors.Is(err, test.want) {
					t.Fatalf("call=%v", err)
				}
			}
		})
	}
	if wrapNodeReference(&Storage{}, nil) != nil || wrapFile(&Storage{}, nil) != nil {
		t.Fatal("nil ownership was fabricated")
	}
}

func TestConditionalWritesKeepPredicatesAndNativeGrowthAccounting(t *testing.T) {
	for _, kind := range []storage.FileMutationKind{storage.MutateWriteAt, storage.MutateAppend} {
		p := &capabilityProbe{before: 512, after: 1024, state: storage.ReferenceState{Attr: storage.Attr{ID: 31, Kind: storage.NodeRegular, Size: 1024}}}
		allowance := &Storage{limit: MinLimit, count: 512}
		file := wrapFile(allowance, &referenceProbe{probe: p}).(storage.ConditionalFileMutation)
		expected := int64(512)
		command := storage.FileMutation{Kind: kind, ExpectedSize: &expected, ExpectedMetadata: map[string][]byte{"app.tag": {3}}, Uses: []storage.TargetUse{{NodeID: 31, Scope: storage.UseScope{Token: "bound-reference"}}}, Guards: &storage.NamespaceGuards{RootID: 11, Directories: []storage.DirectoryObservation{{ParentID: 11, Revision: []byte{4}}}, Edges: []storage.ObservedEdge{{ParentID: 11, RawLeaf: []byte("f"), ChildID: 31}}}}
		if kind == storage.MutateWriteAt {
			command.Offset = 1023
			command.Data = []byte{0xff}
		} else {
			command.Data = make([]byte, 512)
		}
		attr, err := file.MutateFile(t.Context(), command)
		if err != nil || attr.ID != 31 || attr.Size != 1024 {
			t.Fatalf("conditional content=%+v,%v", attr, err)
		}
		if len(p.calls) != 1 || !reflect.DeepEqual(p.calls[0].args, command) {
			t.Fatalf("conditional predicates changed: %+v", p.calls)
		}
		requireCount(t, allowance, 1024)
	}
}
