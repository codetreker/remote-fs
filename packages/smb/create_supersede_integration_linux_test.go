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
	fixtures := []struct {
		name string
		seed realSMBFixture
	}{
		{"direct", newRealSMBFixture(t)},
		{"http", newHTTPSMBFixture(t)},
	}
	for _, family := range []struct {
		name string
		run  func(*testing.T, realSMBFixture)
	}{
		{"TestCreateSupersedeImplicitRightsPreserveObjectAndMetadataDirectAndHTTP", testCreateSupersedeImplicitRightsPreserveObjectAndMetadataDirectAndHTTP},
		{"TestCreateSupersedeMissingUsesOnlyIncomingRightsDirectAndHTTP", testCreateSupersedeMissingUsesOnlyIncomingRightsDirectAndHTTP},
		{"TestCreateSupersedeSelectionRaceKeepsFrozenBranchDirectAndHTTP", testCreateSupersedeSelectionRaceKeepsFrozenBranchDirectAndHTTP},
		{"TestCreateSupersedeLostResultReplaysOriginalEffectDirectAndHTTP", testCreateSupersedeLostResultReplaysOriginalEffectDirectAndHTTP},
		{"mixed-data-grant", testCreateSupersedeMixedDataGrant},
	} {
		t.Run(family.name, func(t *testing.T) {
			for _, fixture := range fixtures {
				t.Run(fixture.name, func(t *testing.T) {
					family.run(t, newFileSessionFixture(t, fixture.seed))
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

func testCreateSupersedeImplicitRightsPreserveObjectAndMetadataDirectAndHTTP(t *testing.T, f realSMBFixture) {
	incoming := uint32(0)
	name := fmt.Sprintf("supersede-%x", incoming)
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
	probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract)}
	f.tree.authority.raw = probe
	request := createRequestForTest(name, 0, incoming)
	binary.LittleEndian.PutUint32(request.Body[28:], dosHidden|dosSystem)
	body, status, newID := f.connection.createFile(t.Context(), f.session, f.tree, request)
	if status != statusOK || newID == oldID || newID == (wire.FileID{}) || binary.LittleEndian.Uint32(body[4:]) != 0 {
		t.Fatalf("supersede = status %#x action/body %x ids %x/%x", status, body, oldID, newID)
	}
	h := f.tree.findFileHandle(newID)
	if h.file == nil || h.node != nil || h.nodeID != before.ID || h.access != incoming|accessDelete|accessWriteEA|accessWriteAttr || len(probe.calls) != 1 {
		t.Fatalf("supersede reference/access = %+v calls=%+v", h, probe.calls)
	}
	options := probe.calls[0]
	if options.Read || options.Write || options.Create || options.Existing != storage.ResetContent || options.ResetUse != storage.DeleteName || options.Target.State != storage.SameNode || options.Target.NodeID != before.ID || options.Use.Uses != storage.DeleteName || options.MetadataAccess != storage.WriteMetadata {
		t.Fatalf("actual atomic grant = %+v", options)
	}
	after, err := f.volume.Stat(t.Context(), name)
	attributes, projectErr := projectWindowsAttributes(after)
	if err != nil || projectErr != nil || after.ID != before.ID || after.Size != 0 || attributes != dosHidden|dosSystem|dosArchive || !reflect.DeepEqual(after.BirthTime, before.BirthTime) || !after.AccessTime.Equal(before.AccessTime) || !after.ModTime.After(before.ModTime) || after.ChangeTime == nil || !after.ChangeTime.After(*before.ChangeTime) || !reflect.DeepEqual(after.Metadata["other"], before.Metadata["other"]) {
		t.Fatalf("atomic reset metadata before=%+v after=%+v attrs=%x err=%v/%v", before, after, attributes, err, projectErr)
	}
	oldRead, err := f.tree.findFileHandle(oldID).file.ReadAt(t.Context(), 0, 4)
	if err != nil || oldRead.Attr.ID != before.ID || oldRead.Attr.Size != 0 || len(oldRead.Data) != 0 {
		t.Fatalf("old handle changed object or missed reset = %+v, %v", oldRead, err)
	}
	if _, err := h.file.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("implicit rights granted data read: %v", err)
	}
	if _, err := h.file.WriteAt(t.Context(), 0, []byte("x")); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("implicit rights granted data write: %v", err)
	}
	closeFixtureHandle(t, f, newID)
	closeFixtureHandle(t, f, oldID)
	f.tree.authority.raw = f.raw
}

func testCreateSupersedeMissingUsesOnlyIncomingRightsDirectAndHTTP(t *testing.T, f realSMBFixture) {
	f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
		if request.Operation == storage.OpVolumeRemove {
			return authz.ErrDenied
		}
		return nil
	})
	probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract)}
	f.tree.authority.raw = probe
	incoming := uint32(0)
	name := fmt.Sprintf("missing-%x", incoming)
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

func testCreateSupersedeSelectionRaceKeepsFrozenBranchDirectAndHTTP(t *testing.T, f realSMBFixture) {
	const name = "existing"
	if err := f.volume.Write(t.Context(), name, []byte("original")); err != nil {
		t.Fatal(err)
	}
	original, err := f.volume.Stat(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	probe := &supersedeSelectionRaceProbe{createSessionContract: f.raw.(createSessionContract)}
	probe.before = func() {
		calls++
		if err := f.volume.Rename(t.Context(), name, name+"-original"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), name, []byte("replacement")); err != nil {
			t.Fatal(err)
		}
	}
	f.tree.authority.raw = probe
	_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest(name, 0, 0))
	if status != statusRetry || id != (wire.FileID{}) || calls != 1 {
		t.Fatalf("existing selection race = %#x, %x, calls=%d", status, id, calls)
	}
	data, err := f.volume.Read(t.Context(), name)
	if err != nil || !bytes.Equal(data, []byte("replacement")) {
		t.Fatalf("selection race changed replacement = %q, %v", data, err)
	}
	retained, err := f.volume.Stat(t.Context(), name+"-original")
	if err != nil || retained.ID != original.ID || retained.Size != original.Size {
		t.Fatalf("selection race reset original = %+v, %v", retained, err)
	}
}

func testCreateSupersedeLostResultReplaysOriginalEffectDirectAndHTTP(t *testing.T, f realSMBFixture) {
	if err := f.volume.Write(t.Context(), "lost-file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	original, err := f.volume.Stat(t.Context(), "lost-file")
	if err != nil {
		t.Fatal(err)
	}
	probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract), loseFirst: true}
	probe.afterOpen = func() {
		probe.afterOpen = nil
		if err := f.volume.Rename(t.Context(), "lost-file", "lost-original"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), "lost-file", []byte("replacement")); err != nil {
			t.Fatal(err)
		}
		f.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
	}
	f.tree.authority.raw = probe
	_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("lost-file", 0, 0))
	if status != statusIO || id != (wire.FileID{}) || len(probe.calls) != 2 || !reflect.DeepEqual(probe.calls[0], probe.calls[1]) || !reflect.DeepEqual(probe.selections[0], probe.selections[1]) {
		t.Fatalf("lost supersede result = %#x, %x, calls=%+v", status, id, probe.calls)
	}
	after, err := f.volume.Stat(t.Context(), "lost-original")
	if err != nil || after.ID != original.ID || after.Size != 0 {
		t.Fatalf("original supersede effect = %+v, %v", after, err)
	}
	replacement, err := f.volume.Read(t.Context(), "lost-file")
	if err != nil || !bytes.Equal(replacement, []byte("replacement")) {
		t.Fatalf("replay touched replacement = %q, %v", replacement, err)
	}
	if owners := f.server.HandleOwners(); len(owners) != 0 {
		t.Fatalf("recovered supersede retained cleanup responsibility = %+v", owners)
	}
}

func testCreateSupersedeMixedDataGrant(t *testing.T, f realSMBFixture) {
	if err := f.volume.Write(t.Context(), "mixed-data", []byte("body")); err != nil {
		t.Fatal(err)
	}
	before, err := f.volume.Stat(t.Context(), "mixed-data")
	if err != nil {
		t.Fatal(err)
	}
	probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract)}
	f.tree.authority.raw = probe
	incoming := accessReadData | accessWriteData | accessDelete
	body, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("mixed-data", 0, incoming))
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
