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

func TestCreateSupersedeImplicitRightsPreserveObjectAndMetadataDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			f := factory(t)
			for _, incoming := range []uint32{0, accessDelete} {
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
		})
	}
}

func TestCreateSupersedeMissingUsesOnlyIncomingRightsDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			f := factory(t)
			f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
				if request.Operation == storage.OpVolumeRemove {
					return authz.ErrDenied
				}
				return nil
			})
			probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract)}
			f.tree.authority.raw = probe
			for _, incoming := range []uint32{0, accessReadAttr, accessReadData, accessWriteData, accessAppend} {
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
			}
			if len(probe.calls) != 3 {
				t.Fatalf("metadata-only creation entered AtomicFileOpener: %d calls", len(probe.calls))
			}
		})
	}
}

func TestCreateSupersedeSharingAndReadonlyRefusalsHaveNoEffectDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			f := factory(t)
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
				if err := f.volume.Write(t.Context(), test.name, []byte("body")); err != nil {
					t.Fatal(err)
				}
				before, err := f.volume.Stat(t.Context(), test.name)
				if err != nil {
					t.Fatal(err)
				}
				if test.attributes != 0 {
					metadata, err := encodeWindowsMetadata(windowsMetadata{Attributes: test.attributes})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := f.raw.(storage.MetadataAccess).SetMetadata(t.Context(), before.ID, windowsMetadataKey, nil, metadata); err != nil {
						t.Fatal(err)
					}
				}
				oldRequest := createRequestForTest(test.name, 1, accessReadData)
				binary.LittleEndian.PutUint32(oldRequest.Body[32:], test.share)
				_, status, oldID := f.connection.createFile(t.Context(), f.session, f.tree, oldRequest)
				if status != statusOK {
					t.Fatalf("old open %s = %#x", test.name, status)
				}
				before, err = f.volume.Stat(t.Context(), test.name)
				if err != nil {
					t.Fatal(err)
				}
				_, status, newID := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest(test.name, 0, test.incoming))
				if status != test.want || newID != (wire.FileID{}) {
					t.Fatalf("refusal %s = %#x, %x", test.name, status, newID)
				}
				after, err := f.volume.Stat(t.Context(), test.name)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("refusal %s changed object: before=%+v after=%+v err=%v", test.name, before, after, err)
				}
				closeFixtureHandle(t, f, oldID)
			}
		})
	}
}

type supersedeSelectionRaceProbe struct {
	createSessionContract
	before func()
}

func (p *supersedeSelectionRaceProbe) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	p.before()
	return p.createSessionContract.OpenAt(ctx, selection, options)
}

func (p *supersedeSelectionRaceProbe) OpenChildRef(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	p.before()
	return p.createSessionContract.OpenChildRef(ctx, selection, options)
}

func TestCreateSupersedeSelectionRaceKeepsFrozenBranchDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			f := factory(t)
			for _, test := range []struct {
				name     string
				existing bool
				access   uint32
			}{
				{"existing", true, 0}, {"missing-node", false, 0}, {"missing-file", false, accessReadData},
			} {
				var original storage.Attr
				if test.existing {
					if err := f.volume.Write(t.Context(), test.name, []byte("original")); err != nil {
						t.Fatal(err)
					}
					var err error
					original, err = f.volume.Stat(t.Context(), test.name)
					if err != nil {
						t.Fatal(err)
					}
				}
				calls := 0
				probe := &supersedeSelectionRaceProbe{createSessionContract: f.raw.(createSessionContract)}
				probe.before = func() {
					calls++
					if test.existing {
						if err := f.volume.Rename(t.Context(), test.name, test.name+"-original"); err != nil {
							t.Fatal(err)
						}
					}
					if err := f.volume.Write(t.Context(), test.name, []byte("replacement")); err != nil {
						t.Fatal(err)
					}
				}
				f.tree.authority.raw = probe
				_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest(test.name, 0, test.access))
				if status != statusRetry || id != (wire.FileID{}) || calls != 1 {
					t.Fatalf("selection race %s = %#x, %x, calls=%d", test.name, status, id, calls)
				}
				data, err := f.volume.Read(t.Context(), test.name)
				if err != nil || !bytes.Equal(data, []byte("replacement")) {
					t.Fatalf("selection race changed replacement %s = %q, %v", test.name, data, err)
				}
				if test.existing {
					retained, err := f.volume.Stat(t.Context(), test.name+"-original")
					if err != nil || retained.ID != original.ID || retained.Size != original.Size {
						t.Fatalf("selection race reset original = %+v, %v", retained, err)
					}
				}
				f.tree.authority.raw = f.raw
			}
		})
	}
}

func TestCreateSupersedeLostResultReplaysOriginalEffectDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			f := factory(t)
			if err := f.volume.Write(t.Context(), "file", []byte("original")); err != nil {
				t.Fatal(err)
			}
			original, err := f.volume.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract), loseFirst: true}
			probe.afterOpen = func() {
				probe.afterOpen = nil
				if err := f.volume.Rename(t.Context(), "file", "original"); err != nil {
					t.Fatal(err)
				}
				if err := f.volume.Write(t.Context(), "file", []byte("replacement")); err != nil {
					t.Fatal(err)
				}
				f.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
			}
			f.tree.authority.raw = probe
			_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("file", 0, 0))
			if status != statusIO || id != (wire.FileID{}) || len(probe.calls) != 2 || !reflect.DeepEqual(probe.calls[0], probe.calls[1]) || !reflect.DeepEqual(probe.selections[0], probe.selections[1]) {
				t.Fatalf("lost supersede result = %#x, %x, calls=%+v", status, id, probe.calls)
			}
			after, err := f.volume.Stat(t.Context(), "original")
			if err != nil || after.ID != original.ID || after.Size != 0 {
				t.Fatalf("original supersede effect = %+v, %v", after, err)
			}
			replacement, err := f.volume.Read(t.Context(), "file")
			if err != nil || !bytes.Equal(replacement, []byte("replacement")) {
				t.Fatalf("replay touched replacement = %q, %v", replacement, err)
			}
			if owners := f.server.HandleOwners(); len(owners) != 0 {
				t.Fatalf("recovered supersede retained cleanup responsibility = %+v", owners)
			}
		})
	}
}

func TestCreateSupersedeActualDataRightsDirectAndHTTP(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			f := factory(t)
			for _, incoming := range []uint32{accessReadData, accessWriteData, accessAppend, accessReadData | accessWriteData | accessDelete} {
				name := fmt.Sprintf("actual-%x", incoming)
				if err := f.volume.Write(t.Context(), name, []byte("body")); err != nil {
					t.Fatal(err)
				}
				_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest(name, 0, incoming))
				if status != statusOK {
					t.Fatalf("actual access %x supersede = %#x", incoming, status)
				}
				h := f.tree.findFileHandle(id)
				_, readErr := h.file.ReadAt(t.Context(), 0, 1)
				_, writeErr := h.file.WriteAt(t.Context(), 0, []byte("x"))
				if incoming&accessReadData != 0 && readErr != nil || incoming&accessReadData == 0 && !errors.Is(readErr, syscall.EBADF) || incoming&(accessWriteData|accessAppend) != 0 && writeErr != nil || incoming&(accessWriteData|accessAppend) == 0 && !errors.Is(writeErr, syscall.EBADF) {
					t.Fatalf("actual access %x data grants read=%v write=%v", incoming, readErr, writeErr)
				}
				closeFixtureHandle(t, f, id)
			}
		})
	}
}

func TestCreateSupersedeAllocationAndRemoveAuthorizationBeforeEffect(t *testing.T) {
	f := newRealSMBFixture(t)
	if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	before, err := f.volume.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	probe := &createResultProbe{createSessionContract: f.raw.(createSessionContract)}
	f.tree.authority.raw = probe
	allocation := testCreateContext("AlSi", 8)
	binary.LittleEndian.PutUint64(allocation.Data, 1)
	for _, name := range []string{"file", "missing"} {
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
	if _, status, _ := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("file", 0, 0)); status != statusDenied || len(probe.calls) != 0 {
		t.Fatalf("denied remove authorization = %#x calls=%d", status, len(probe.calls))
	}
	after, err := f.volume.Stat(t.Context(), "file")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected effect changed original before=%+v after=%+v err=%v", before, after, err)
	}
	if _, err := f.volume.Stat(t.Context(), "missing"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("rejected allocation created missing target: %v", err)
	}
	f.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
	binary.LittleEndian.PutUint64(allocation.Data, 0)
	request := createContextsRequest(t, createRequestForTest("file", 0, 0), allocation)
	body, status, id := f.connection.createFile(t.Context(), f.session, f.tree, request)
	if status != statusOK || binary.LittleEndian.Uint32(body[4:]) != 0 {
		t.Fatalf("zero allocation supersede = %#x, %x", status, body)
	}
	closeFixtureHandle(t, f, id)
}
