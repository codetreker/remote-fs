//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestCreateSupersedeDirectAndHTTP(t *testing.T) {
	seed := newRealSMBFixture(t)
	for _, transport := range []struct {
		name    string
		backend storage.FileStorage
	}{
		{"direct", seed.volume},
		{"http", httpSMBBackendForTest(t, seed.volume)},
	} {
		t.Run(transport.name, func(t *testing.T) {
			base := realSMBFixtureForSession(t, seed.volume, transport.backend, seed.raw)
			for _, family := range []struct {
				name string
				run  func(*testing.T, realSMBFixture, string)
			}{
				{"reset-and-lost-result", testCreateSupersedeResetAndLostResult},
				{"missing-node-ref", testCreateSupersedeMissingUsesOnlyIncomingRightsDirectAndHTTP},
				{"mixed-data-grant", testCreateSupersedeMixedDataGrant},
			} {
				t.Run(family.name, func(t *testing.T) {
					family.run(t, newFileSessionFixture(t, base), transport.name+"-")
				})
			}
		})
	}
}

func newSupersedeMappingFixture(t *testing.T) (realSMBFixture, *nativeDeleteBackend, *createResultProbe) {
	t.Helper()
	backend := NewNativeNameProbeBackend()
	raw, err := backend.NewFileSession(t.Context(), fixtureFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := raw.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	wrapped := &supersedeSelectionRaceProbe{createSessionContract: raw.(createSessionContract)}
	wrapped.after = func(action storage.FileActionID, operation storage.Operation, empty bool, err error) {
		if empty && (errors.Is(err, storage.ErrUseConflict) || errors.Is(err, storage.ErrConditionConflict)) {
			backend.mu.Lock()
			backend.recordNameRefusal(action, operation, err)
			backend.mu.Unlock()
		}
	}
	probe := &createResultProbe{createSessionContract: wrapped}
	f := realSMBFixtureForSession(t, nil, backend, raw)
	f.tree.authority.raw = probe
	return f, backend, probe
}

func TestCreateSupersedeAdapterMapping(t *testing.T) {
	t.Run("existing-rights", testCreateSupersedeExistingRightsMapping)
	t.Run("missing-rights", testCreateSupersedeMissingRightsMapping)
	t.Run("sharing-and-attributes", testCreateSupersedeSharingAndAttributesMapping)
	t.Run("existing-selection-race", testCreateSupersedeExistingSelectionRaceMapping)
	t.Run("missing-selection-races", testCreateSupersedeMissingSelectionRaceMapping)
	t.Run("allocation-and-authorization", testCreateSupersedeAllocationAndRemoveAuthorizationBeforeEffect)
}

func testCreateSupersedeExistingRightsMapping(t *testing.T) {
	for _, incoming := range []uint32{0, accessDelete, accessReadData, accessWriteData, accessAppend, accessReadData | accessWriteData | accessDelete} {
		t.Run(fmt.Sprintf("access-%x", incoming), func(t *testing.T) {
			intent, err := classifyCreate(wire.CreateRequest{Disposition: 0, DesiredAccess: incoming, ShareAccess: 7})
			if err != nil {
				t.Fatal(err)
			}
			intent.setAccess(intent.access|accessDelete|accessWriteAttr|accessWriteEA, 7)
			read, write := incoming&accessReadData != 0, incoming&(accessWriteData|accessAppend) != 0
			uses := storage.DeleteName
			if read {
				uses |= storage.ReadData
			}
			if write {
				uses |= storage.WriteData
			}
			if intent.read != read || intent.write != write || intent.access != incoming|accessDelete|accessWriteEA|accessWriteAttr || intent.use.Uses != uses || intent.metadata != storage.WriteMetadata {
				t.Fatalf("existing access classification = %+v", intent)
			}
			if write {
				return
			}
			f, backend, probe := newSupersedeMappingFixture(t)
			before := backend.attr(8).Clone()
			body, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("supersede.txt", 0, incoming))
			if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != 0 || len(probe.calls) != 1 {
				t.Fatalf("existing mapping incoming=%x status=%#x body=%x calls=%+v", incoming, status, body, probe.calls)
			}
			h := f.tree.findFileHandle(id)
			options := probe.calls[0]
			if options.Read != read || options.Write != write || options.Create || options.Existing != storage.ResetContent || options.ResetUse != storage.DeleteName || options.Target.State != storage.SameNode || options.Target.NodeID != before.ID || options.Use.Uses != uses || options.MetadataAccess != storage.WriteMetadata || h.nodeID != before.ID || h.access != incoming|accessDelete|accessWriteEA|accessWriteAttr {
				t.Fatalf("existing grants incoming=%x options=%+v handle=%+v", incoming, options, h)
			}
			file := h.file.(*nativeDeleteFile)
			if file.readable != read || file.writable != write {
				t.Fatalf("returned grant incoming=%x read=%v write=%v", incoming, file.readable, file.writable)
			}
			metadata, err := decodeWindowsMetadataPayload(options.Initial.OnReset.Metadata[windowsMetadataKey])
			if err != nil || metadata.Attributes != dosArchive || !options.Initial.OnReset.Attr.Empty() {
				t.Fatalf("reset field mapping = %+v, %v", options.Initial.OnReset, err)
			}
			closeFixtureHandle(t, f, id)
		})
	}
}

func testCreateSupersedeMissingRightsMapping(t *testing.T) {
	for _, incoming := range []uint32{0, accessReadAttr, accessReadData, accessWriteData, accessAppend} {
		t.Run(fmt.Sprintf("access-%x", incoming), func(t *testing.T) {
			intent, err := classifyCreate(wire.CreateRequest{Disposition: 0, DesiredAccess: incoming, ShareAccess: 7})
			if err != nil || !intent.create || intent.reset || intent.resetUse != 0 || intent.access != incoming || intent.read != (incoming&accessReadData != 0) || intent.write != (incoming&(accessWriteData|accessAppend) != 0) || intent.use.Uses&storage.DeleteName != 0 {
				t.Fatalf("missing access classification = %+v, %v", intent, err)
			}
			if intent.write {
				return
			}
			f, _, probe := newSupersedeMappingFixture(t)
			f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
				if request.Operation == storage.OpVolumeRemove {
					return authz.ErrDenied
				}
				return nil
			})
			body, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("missing", 0, incoming))
			if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != uint32(storage.Created) {
				t.Fatalf("missing mapping incoming=%x status=%#x body=%x", incoming, status, body)
			}
			h := f.tree.findFileHandle(id)
			if h.access != incoming || (h.file != nil) != (incoming&(accessReadData|accessWriteData|accessAppend) != 0) {
				t.Fatalf("missing grants incoming=%x handle=%+v", incoming, h)
			}
			if h.file != nil {
				if len(probe.calls) != 1 {
					t.Fatalf("missing atomic calls = %d", len(probe.calls))
				}
				options := probe.calls[0]
				if options.ResetUse != 0 || options.Existing != storage.Keep || !options.Create || options.Target.State != storage.Absent || options.Use.Uses&storage.DeleteName != 0 || options.Read != (incoming&accessReadData != 0) || options.Write != (incoming&(accessWriteData|accessAppend) != 0) {
					t.Fatalf("missing atomic mapping = %+v", options)
				}
			} else {
				ref := h.node.(*nativeDeleteReference)
				metadata := storage.MetadataPermissions(0)
				if incoming == accessReadAttr {
					metadata = storage.ReadMetadata
				}
				if len(probe.calls) != 0 || ref.use.Uses != 0 || ref.metadata != metadata {
					t.Fatalf("missing regular NodeRef grants calls=%d use=%+v metadata=%d", len(probe.calls), ref.use, ref.metadata)
				}
			}
			closeFixtureHandle(t, f, id)
		})
	}
}

func testCreateSupersedeMissingSelectionRaceMapping(t *testing.T) {
	for _, incoming := range []uint32{accessReadData} {
		t.Run(fmt.Sprintf("access-%x", incoming), func(t *testing.T) {
			f, backend, probe := newSupersedeMappingFixture(t)
			wrapped := probe.createSessionContract.(*supersedeSelectionRaceProbe)
			calls := 0
			winner := backend.attr(8).Clone()
			winner.ID, winner.Size = 999, 11
			wrapped.before = func() {
				calls++
				backend.mu.Lock()
				backend.views[1] = append(backend.views[1], storage.Entry{Name: "winner", Attr: winner})
				backend.revision++
				backend.mu.Unlock()
			}
			_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("winner", 0, incoming))
			if status != statusRetry || id != (wire.FileID{}) || calls != 1 {
				t.Fatalf("missing selection mapping incoming=%x status=%#x id=%x calls=%d", incoming, status, id, calls)
			}
			if current := backend.attr(winner.ID); !reflect.DeepEqual(current, winner) {
				t.Fatalf("missing selection changed observed winner = %+v", current)
			}
			if owners := f.server.HandleOwners(); len(owners) != 0 {
				t.Fatalf("known refusal retained cleanup = %+v", owners)
			}
		})
	}
}

func testCreateSupersedeExistingSelectionRaceMapping(t *testing.T) {
	f, backend, probe := newSupersedeMappingFixture(t)
	before := backend.attr(8).Clone()
	wrapped := probe.createSessionContract.(*supersedeSelectionRaceProbe)
	calls := 0
	winner := before.Clone()
	winner.ID, winner.Size = 999, 11
	wrapped.before = func() {
		calls++
		backend.mu.Lock()
		for index := range backend.views[1] {
			if backend.views[1][index].Attr.ID == before.ID {
				backend.views[1][index].Name = "original"
			}
		}
		backend.views[1] = append(backend.views[1], storage.Entry{Name: "supersede.txt", Attr: winner})
		backend.revision++
		backend.mu.Unlock()
	}
	_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("supersede.txt", 0, 0))
	if status != statusRetry || id != (wire.FileID{}) || calls != 1 || len(probe.calls) != 1 || probe.calls[0].Target.State != storage.SameNode || probe.calls[0].Target.NodeID != before.ID || probe.calls[0].Create || probe.calls[0].ResetUse != storage.DeleteName {
		t.Fatalf("existing frozen selection = %#x, %x, calls=%+v", status, id, probe.calls)
	}
	if !reflect.DeepEqual(backend.attr(before.ID), before) || !reflect.DeepEqual(backend.attr(winner.ID), winner) {
		t.Fatal("stale existing selection changed a fixture object")
	}
	if owners := f.server.HandleOwners(); len(owners) != 0 {
		t.Fatalf("known existing refusal retained cleanup = %+v", owners)
	}
}

func testCreateSupersedeResetAndLostResult(t *testing.T, f realSMBFixture, prefix string) {
	incoming := uint32(0)
	name := prefix + "supersede"
	if err := f.volume.Write(t.Context(), name, []byte("body")); err != nil {
		t.Fatal(err)
	}
	before, err := f.volume.Stat(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	birth, access, modified := time.Unix(10, 0), time.Unix(20, 0), time.Unix(30, 0)
	if _, err := f.raw.SetNodeAttr(t.Context(), before.ID, storage.AttrChange{BirthTime: &birth, AccessTime: &access, ModTime: &modified}); err != nil {
		t.Fatal(err)
	}
	metadata, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosHidden | dosSystem | dosReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string][]byte{windowsMetadataKey: metadata, "other": []byte("preserve")} {
		if _, err := f.raw.(storage.MetadataAccess).SetMetadata(t.Context(), before.ID, key, nil, value); err != nil {
			t.Fatal(err)
		}
	}
	oldRequest := createRequestForTest(name, 1, accessReadData|accessReadAttr)
	binary.LittleEndian.PutUint32(oldRequest.Body[32:], 5)
	_, status, oldID := f.connection.createFile(t.Context(), f.session, f.tree, oldRequest)
	if status != statusOK {
		t.Fatalf("old deny-write allow-delete handle = %#x", status)
	}
	before, err = f.volume.Stat(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract), loseFirst: true}
	authorize := f.server.config.Authorize
	var captured storage.Attr
	var outcome storage.OpenOutcome
	probe.change = func(result *storage.OpenResult) {
		captured, outcome = result.Attr.Clone(), result.Outcome
	}
	probe.afterOpen = func() {
		probe.afterOpen = nil
		options := probe.calls[0]
		if outcome != storage.Reset || captured.ID != before.ID || options.Read || options.Write || options.Create || options.Existing != storage.ResetContent || options.ResetUse != storage.DeleteName || options.Target.State != storage.SameNode || options.Target.NodeID != before.ID || options.Use.Uses != storage.DeleteName || options.MetadataAccess != storage.WriteMetadata {
			t.Fatalf("live zero-data reset options=%+v attr=%+v outcome=%d", options, captured, outcome)
		}
		attributes, projectErr := projectWindowsAttributes(captured)
		if projectErr != nil || captured.Size != 0 || attributes != dosHidden|dosSystem|dosArchive || !reflect.DeepEqual(captured.BirthTime, before.BirthTime) || !captured.AccessTime.Equal(before.AccessTime) || !captured.ModTime.After(before.ModTime) || captured.ChangeTime == nil || !captured.ChangeTime.After(*before.ChangeTime) || !reflect.DeepEqual(captured.Metadata["other"], before.Metadata["other"]) {
			t.Fatalf("live reset metadata before=%+v after=%+v attrs=%x err=%v", before, captured, attributes, projectErr)
		}
		state, err := probe.last.(storage.ReferenceStateAccess).State(t.Context(), storage.StateScalars)
		if err != nil || state.Attr.ID != before.ID || state.Attr.Size != 0 {
			t.Fatalf("reset reference not live before response loss: %+v, %v", state, err)
		}
		oldRead, err := f.tree.findFileHandle(oldID).file.ReadAt(t.Context(), 0, 4)
		if err != nil || oldRead.Attr.ID != before.ID || oldRead.Attr.Size != 0 || len(oldRead.Data) != 0 {
			t.Fatalf("old handle changed object or missed live reset = %+v, %v", oldRead, err)
		}
		if _, err := probe.last.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("live supersede granted data read: %v", err)
		}
		if _, err := probe.last.WriteAt(t.Context(), 0, []byte("x")); !errors.Is(err, syscall.EBADF) {
			t.Fatalf("live supersede granted data write: %v", err)
		}
		newID := wire.FileID{}
		for _, owner := range f.server.HandleOwners() {
			if owner.OpenAction == options.Action {
				newID = owner.FileID
			}
		}
		if newID == (wire.FileID{}) || newID == oldID {
			t.Fatalf("supersede did not reserve a separate FileId: %x/%x", oldID, newID)
		}
		if err := f.volume.Rename(t.Context(), name, prefix+"reset-original"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), name, []byte("replacement")); err != nil {
			t.Fatal(err)
		}
		f.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
	}
	f.tree.authority.raw = probe
	request := createRequestForTest(name, 0, incoming)
	binary.LittleEndian.PutUint32(request.Body[28:], dosHidden|dosSystem)
	_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, request)
	if status != statusIO || id != (wire.FileID{}) || len(probe.calls) != 2 || !reflect.DeepEqual(probe.calls[0], probe.calls[1]) || !reflect.DeepEqual(probe.selections[0], probe.selections[1]) {
		t.Fatalf("lost reset result = %#x, %x, calls=%+v", status, id, probe.calls)
	}
	after, err := f.volume.Stat(t.Context(), prefix+"reset-original")
	if err != nil || after.ID != before.ID || after.Size != 0 {
		t.Fatalf("original reset effect = %+v, %v", after, err)
	}
	replacement, err := f.volume.Read(t.Context(), name)
	if err != nil || !bytes.Equal(replacement, []byte("replacement")) {
		t.Fatalf("reset replay touched replacement = %q, %v", replacement, err)
	}
	f.server.config.Authorize = authorize
	closeFixtureHandle(t, f, oldID)
	if owners := f.server.HandleOwners(); len(owners) != 0 {
		t.Fatalf("lost reset retained cleanup responsibility = %+v", owners)
	}
}

func testCreateSupersedeMissingUsesOnlyIncomingRightsDirectAndHTTP(t *testing.T, f realSMBFixture, prefix string) {
	f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if request.Operation == storage.OpVolumeRemove {
			return authz.ErrDenied
		}
		return nil
	})
	probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract)}
	f.tree.authority.raw = probe
	incoming := uint32(0)
	name := prefix + "missing"
	body, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest(name, 0, incoming))
	if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != uint32(storage.Created) {
		t.Fatalf("incoming %x missing supersede = %#x, %x", incoming, status, body)
	}
	h := f.tree.findFileHandle(id)
	if h.access != incoming || (h.file != nil) != (incoming&(accessReadData|accessWriteData|accessAppend) != 0) {
		t.Fatalf("missing supersede extra grant or wrong path: incoming=%x handle=%+v", incoming, h)
	}
	if h.file != nil {
		options := probe.calls[len(probe.calls)-1]
		if options.ResetUse != 0 || options.Existing != storage.Keep || !options.Create || options.Target.State != storage.Absent || options.Use.Uses&storage.DeleteName != 0 {
			t.Fatalf("missing supersede reset/remove effect = %+v", options)
		}
	}
	closeFixtureHandle(t, f, id)
	if len(probe.calls) != 0 {
		t.Fatalf("metadata-only creation entered AtomicFileOpener: %d calls", len(probe.calls))
	}
}

func testCreateSupersedeSharingAndAttributesMapping(t *testing.T) {
	for _, test := range []struct {
		name       string
		share      uint32
		attributes uint32
		incoming   uint32
		want       uint32
	}{
		{"deny-delete", 3, 0, 0, statusSharingViolation},
		{"deny-write", 5, 0, accessWriteData, statusSharingViolation},
		{"readonly-writer", 7, dosReadOnly, accessWriteData, statusDenied},
		{"hidden-missing", 7, dosHidden, 0, statusDenied},
		{"system-missing", 7, dosSystem, 0, statusDenied},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, backend, probe := newSupersedeMappingFixture(t)
			before := backend.attr(8).Clone()
			if test.attributes != 0 {
				metadata, err := encodeWindowsMetadata(windowsMetadata{Attributes: test.attributes})
				if err != nil {
					t.Fatal(err)
				}
				before.Metadata = map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte("observed"), Data: metadata}}
				backend.updateAttr(before)
			}
			oldRequest := createRequestForTest("supersede.txt", 1, accessReadData)
			binary.LittleEndian.PutUint32(oldRequest.Body[32:], test.share)
			_, status, oldID := f.connection.createFile(t.Context(), f.session, f.tree, oldRequest)
			if status != statusOK {
				t.Fatalf("old open %s = %#x", test.name, status)
			}
			probe.calls = nil
			_, status, newID := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("supersede.txt", 0, test.incoming))
			if status != test.want || newID != (wire.FileID{}) {
				t.Fatalf("refusal %s = %#x, %x", test.name, status, newID)
			}
			if after := backend.attr(8); !reflect.DeepEqual(before, after) {
				t.Fatalf("refusal %s changed fixture before=%+v after=%+v", test.name, before, after)
			}
			if test.want == statusDenied && len(probe.calls) != 0 || test.want == statusSharingViolation && len(probe.calls) != 1 {
				t.Fatalf("refusal %s dispatch count = %d", test.name, len(probe.calls))
			}
			closeFixtureHandle(t, f, oldID)
			if owners := f.server.HandleOwners(); len(owners) != 0 {
				t.Fatalf("mapping refusal retained cleanup = %+v", owners)
			}
		})
	}
}

type supersedeSelectionRaceProbe struct {
	createSessionContract
	before func()
	after  func(storage.FileActionID, storage.Operation, bool, error)
}

func (p *supersedeSelectionRaceProbe) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	if p.before != nil {
		p.before()
	}
	result, err := p.createSessionContract.OpenAt(ctx, selection, options)
	if p.after != nil {
		p.after(options.Action, storage.OpFileOpenAt, result.File == nil && result.Attr.ID == 0 && result.Outcome == 0, err)
	}
	return result, err
}

func (p *supersedeSelectionRaceProbe) OpenChildRef(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if p.before != nil {
		p.before()
	}
	result, err := p.createSessionContract.OpenChildRef(ctx, selection, options)
	if p.after != nil {
		p.after(options.Action, storage.OpFileOpenChildRef, result.Reference == nil && result.Attr.ID == 0 && result.Outcome == 0, err)
	}
	return result, err
}

func testCreateSupersedeMixedDataGrant(t *testing.T, f realSMBFixture, prefix string) {
	if err := f.volume.Write(t.Context(), prefix+"mixed-data", []byte("body")); err != nil {
		t.Fatal(err)
	}
	before, err := f.volume.Stat(t.Context(), prefix+"mixed-data")
	if err != nil {
		t.Fatal(err)
	}
	probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract)}
	f.tree.authority.raw = probe
	incoming := accessReadData | accessWriteData | accessDelete
	body, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest(prefix+"mixed-data", 0, incoming))
	if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != 0 || len(probe.calls) != 1 {
		t.Fatalf("mixed supersede = %#x, %x, calls=%+v", status, body, probe.calls)
	}
	options := probe.calls[0]
	if !options.Read || !options.Write || options.ResetUse != storage.DeleteName || options.Existing != storage.ResetContent || options.Create || options.Target.State != storage.SameNode || options.Target.NodeID != before.ID || options.Use.Uses != storage.ReadData|storage.WriteData|storage.DeleteName || options.MetadataAccess != storage.WriteMetadata || !reflect.DeepEqual(options.ContentMetadataEffects, windowsContentEffects()) {
		t.Fatalf("mixed real grant = %+v", options)
	}
	h := f.tree.findFileHandle(id)
	read, err := h.file.ReadAt(t.Context(), 0, 1)
	if err != nil || read.Attr.ID != before.ID || read.Attr.Size != 0 || len(read.Data) != 0 {
		t.Fatalf("mixed real read = %+v, %v", read, err)
	}
	written, err := h.file.WriteAt(t.Context(), 0, []byte("x"))
	if err != nil || written.ID != before.ID || written.Size != 1 {
		t.Fatalf("mixed real write = %+v, %v", written, err)
	}
	closeFixtureHandle(t, f, id)
}

func testCreateSupersedeAllocationAndRemoveAuthorizationBeforeEffect(t *testing.T) {
	f, backend, probe := newSupersedeMappingFixture(t)
	before := backend.attr(8).Clone()
	allocation := testCreateContext("AlSi", 8)
	binary.LittleEndian.PutUint64(allocation.Data, 1)
	for _, name := range []string{"supersede.txt", "allocation-missing"} {
		request := createContextsRequest(t, createRequestForTest(name, 0, 0), allocation)
		if _, status, _ := f.connection.createFile(t.Context(), f.session, f.tree, request); status != statusUnsupported || len(probe.calls) != 0 || probe.observations != 0 {
			t.Fatalf("nonzero allocation %s = %#x calls=%d observations=%d", name, status, len(probe.calls), probe.observations)
		}
	}
	f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if request.Operation == storage.OpVolumeRemove {
			return authz.ErrDenied
		}
		return nil
	})
	if _, status, _ := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("supersede.txt", 0, 0)); status != statusDenied || len(probe.calls) != 0 {
		t.Fatalf("denied remove authorization = %#x calls=%d", status, len(probe.calls))
	}
	if after := backend.attr(8); !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected admission changed fixture before=%+v after=%+v", before, after)
	}
	for _, entry := range backend.views[1] {
		if entry.Name == "allocation-missing" {
			t.Fatal("rejected allocation created missing target")
		}
	}
	f.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
	binary.LittleEndian.PutUint64(allocation.Data, 0)
	request := createContextsRequest(t, createRequestForTest("supersede.txt", 0, 0), allocation)
	body, status, id := f.connection.createFile(t.Context(), f.session, f.tree, request)
	if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != 0 {
		t.Fatalf("zero allocation supersede = %#x, %x", status, body)
	}
	closeFixtureHandle(t, f, id)
}
