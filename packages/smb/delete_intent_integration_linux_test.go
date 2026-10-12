//go:build linux

package smb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http/httptest"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func enableDeleteIntentsForTest(t *testing.T, f realSMBFixture) realSMBFixture {
	t.Helper()
	config := endpointConfig()
	config.Limits = f.server.config.Limits
	server, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	server.nameComparer = testNameCompare
	owner, err := storage.NewDeleteIntentOwner()
	if err != nil {
		t.Fatal(err)
	}
	share := f.tree.export.share
	share.Name, share.DeleteIntentOwner = "data", owner
	export, err := server.Publish(share)
	if err != nil {
		t.Fatal(err)
	}
	f.server, f.connection.server, f.tree.export = server, server, export
	f.tree.authority.export = export
	f.session.principal = testPrincipal("0123456789abcdef")
	f.tree.authority.principal = f.session.principal
	f.tree.sessionID = f.session.id
	if err := export.RecoverDeleteIntents(WithPrincipal(t.Context(), f.session.principal)); err != nil {
		t.Fatalf("initial delete discovery: %v", err)
	}
	return f
}

func deleteCreateRequestForTest(name string, disposition, access, options uint32) wire.Request {
	request := createRequestForTest(name, disposition, access)
	binary.LittleEndian.PutUint32(request.Body[40:], options|createDeleteOnClose)
	return request
}

func acceptedDeleteIntentForTest(t *testing.T, f realSMBFixture, node uint64) storage.DeleteIntentStatus {
	t.Helper()
	page, err := f.raw.(storage.FileActions).ListDeleteIntents(t.Context(), f.tree.export.share.DeleteIntentOwner, 0, storage.MaxDeleteIntentPageEntries)
	if err != nil {
		t.Fatal(err)
	}
	for _, intent := range page.Intents {
		if intent.NodeID == node {
			return intent
		}
	}
	t.Fatalf("accepted node %d has no durable intent: %+v", node, page)
	return storage.DeleteIntentStatus{}
}

func requireDeleteOutcomeForTest(t *testing.T, f realSMBFixture, intent storage.DeleteIntentID, want storage.DeleteIntentOutcome) {
	t.Helper()
	status, err := f.raw.(storage.FileActions).QueryDeleteIntent(t.Context(), f.tree.export.share.DeleteIntentOwner, intent)
	if err != nil || status.Outcome != want {
		t.Fatalf("intent %s outcome=%+v error=%v want=%d", intent, status, err, want)
	}
}

func TestDeleteOnCloseAtomicDispositions(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		for _, test := range []struct {
			name                         string
			disposition, access, options uint32
			exists                       bool
			outcome                      storage.OpenOutcome
		}{
			{"open", 1, accessReadData | accessReadAttr | accessDelete, 0, true, storage.Opened},
			{"create", 2, accessWriteData | accessReadAttr | accessDelete, 0, false, storage.Created},
			{"open-if-present", 3, accessReadData | accessReadAttr | accessDelete, 0, true, storage.Opened},
			{"open-if-absent", 3, accessReadData | accessReadAttr | accessDelete, 0, false, storage.Created},
			{"overwrite", 4, accessWriteData | accessReadAttr | accessDelete, 0, true, storage.Reset},
			{"overwrite-if-present", 5, accessWriteData | accessReadAttr | accessDelete, 0, true, storage.Reset},
			{"overwrite-if-absent", 5, accessWriteData | accessReadAttr | accessDelete, 0, false, storage.Created},
			{"metadata-only", 1, accessReadAttr | accessDelete, 0, true, storage.Opened},
			{"directory-create", 2, accessReadAttr | accessDelete, createDirectory, false, storage.Created},
		} {
			t.Run(transport+"/"+test.name, func(t *testing.T) {
				factory := newRealSMBFixture
				if transport == "http" {
					factory = newHTTPSMBFixture
				}
				f := enableDeleteIntentsForTest(t, factory(t))
				var previous storage.Attr
				if test.exists {
					if err := f.volume.Write(t.Context(), "victim", []byte("original")); err != nil {
						t.Fatal(err)
					}
					previous = seedIOWindowsMetadata(t, f, "victim", dosHidden|dosSystem)
				}
				request := deleteCreateRequestForTest("victim", test.disposition, test.access, test.options)
				if test.exists && test.disposition >= 4 {
					binary.LittleEndian.PutUint32(request.Body[28:], dosHidden|dosSystem)
				}
				body, status, id := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, request)
				if status != statusOK || id == (wire.FileID{}) || len(body) != 88 || binary.LittleEndian.Uint32(body[4:]) != uint32(test.outcome) {
					t.Fatalf("atomic create status=%#x id=%x body=%x", status, id, body)
				}
				attr, err := f.volume.Stat(t.Context(), "victim")
				if err != nil || test.exists && attr.ID != previous.ID {
					t.Fatalf("atomic identity=%+v previous=%+v err=%v", attr, previous, err)
				}
				if test.disposition >= 4 && attr.Size != 0 {
					t.Fatalf("reset retained length: %+v", attr)
				}
				intent := acceptedDeleteIntentForTest(t, f, attr.ID)
				if intent.Outcome != storage.DeleteIntentArmed {
					t.Fatalf("accepted intent=%+v", intent)
				}
				standard := queryInfoForTest(t, f, id, wire.FileStandardInformation)
				if standard[20] != 0 || binary.LittleEndian.Uint32(standard[16:]) != 1 {
					t.Fatalf("armed standard=%x", standard)
				}
				closeDeleteFixtureHandle(t, f, id)
				if _, err := f.volume.Stat(t.Context(), "victim"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("close left name: %v", err)
				}
			})
		}
	}
}

func TestDeleteOnCloseWaitsForCompatibleSMBAndSDKReferences(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, fixture realSMBFixture) {
		f := enableDeleteIntentsForTest(t, fixture)
		if err := f.volume.Write(t.Context(), "victim", []byte("original")); err != nil {
			t.Fatal(err)
		}
		original, err := f.volume.Stat(t.Context(), "victim")
		if err != nil {
			t.Fatal(err)
		}
		_, status, deleting := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, deleteCreateRequestForTest("victim", 1, accessReadData|accessDelete, 0))
		if status != statusOK {
			t.Fatalf("arm status=%#x", status)
		}
		old := openInfoFileForTest(t, f, "victim", accessReadData|accessWriteData)
		foreign, err := f.volume.NewFileSession(t.Context(), fixtureFileSessionOptions())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := foreign.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		pin, err := foreign.OpenFile(t.Context(), "victim", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, Use: storage.UseClaim{Uses: storage.ReadData}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := pin.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		intent := acceptedDeleteIntentForTest(t, f, original.ID)
		closeDeleteFixtureHandle(t, f, deleting)
		requireDeleteOutcomeForTest(t, f, intent.ID, storage.DeleteIntentPending)
		standard := queryInfoForTest(t, f, old, wire.FileStandardInformation)
		if standard[20] != 1 || binary.LittleEndian.Uint32(standard[16:]) != 0 {
			t.Fatalf("pending Standard=%x", standard)
		}
		if _, status, _ := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, createRequestForTest("victim", 1, accessReadData)); status == statusOK {
			t.Fatal("pending allowed new SMB open")
		}
		if _, err := foreign.OpenFile(t.Context(), "victim", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}); err == nil {
			t.Fatal("pending allowed new SDK open")
		}
		if err := f.volume.Rename(t.Context(), "victim", "escaped"); err == nil {
			t.Fatal("pending allowed SDK rename")
		}
		if err := f.volume.Remove(t.Context(), "victim"); err == nil {
			t.Fatal("pending allowed SDK unlink")
		}
		requireIOWrite(t, f, old, 0, "changed!")
		body, err := pin.ReadAt(t.Context(), 0, 8)
		if err != nil || string(body.Data) != "changed!" {
			t.Fatalf("old SDK reference=%q %v", body.Data, err)
		}
		closeDeleteFixtureHandle(t, f, old)
		requireDeleteOutcomeForTest(t, f, intent.ID, storage.DeleteIntentPending)
		if attr, err := f.volume.Stat(t.Context(), "victim"); err != nil || attr.ID != original.ID {
			t.Fatalf("last SDK pin lost name=%+v %v", attr, err)
		}
		if err := pin.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		if _, err := f.volume.Stat(t.Context(), "victim"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("last reference left name: %v", err)
		}
		if err := f.tree.export.RecoverDeleteIntents(WithPrincipal(t.Context(), f.session.principal)); err != nil {
			t.Fatal(err)
		}
		requireDeleteOutcomeForTest(t, f, intent.ID, storage.DeleteIntentUnknown)
	})
}

func TestDeleteOnCloseTracksOriginalAssociationThroughSDKMutations(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		for _, mutation := range []string{"rename", "unlink", "replacement"} {
			t.Run(transport+"/"+mutation, func(t *testing.T) {
				factory := newRealSMBFixture
				if transport == "http" {
					factory = newHTTPSMBFixture
				}
				f := enableDeleteIntentsForTest(t, factory(t))
				if err := f.volume.Write(t.Context(), "victim", []byte("original")); err != nil {
					t.Fatal(err)
				}
				original, err := f.volume.Stat(t.Context(), "victim")
				if err != nil {
					t.Fatal(err)
				}
				_, status, id := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, deleteCreateRequestForTest("victim", 1, accessReadData|accessDelete, 0))
				if status != statusOK {
					t.Fatalf("arm status=%#x", status)
				}
				intent := acceptedDeleteIntentForTest(t, f, original.ID)
				if mutation == "rename" {
					if err := f.volume.Rename(t.Context(), "victim", "moved"); err != nil {
						t.Fatal(err)
					}
				} else if mutation == "unlink" {
					if err := f.volume.Remove(t.Context(), "victim"); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := f.volume.Write(t.Context(), "source", []byte("replacement")); err != nil {
						t.Fatal(err)
					}
					if err := f.volume.Rename(t.Context(), "source", "victim"); err != nil {
						t.Fatal(err)
					}
				}
				if mutation != "replacement" {
					if err := f.volume.Write(t.Context(), "victim", []byte("replacement")); err != nil {
						t.Fatal(err)
					}
				}
				replacement := requireIOContents(t, f, "victim", "replacement")
				if replacement.ID == original.ID {
					t.Fatal("SDK replacement reused identity")
				}
				old := f.tree.findFileHandle(id).file
				body, err := old.ReadAt(t.Context(), 0, 8)
				if err != nil || string(body.Data) != "original" {
					t.Fatalf("old reference rebound=%q %v", body.Data, err)
				}
				closeDeleteFixtureHandle(t, f, id)
				requireIOContents(t, f, "victim", "replacement")
				if mutation == "rename" {
					if _, err := f.volume.Stat(t.Context(), "moved"); !errors.Is(err, syscall.ENOENT) {
						t.Fatalf("renamed association left: %v", err)
					}
				} else {
					outcome, err := f.raw.(storage.FileActions).QueryDeleteIntent(t.Context(), f.tree.export.share.DeleteIntentOwner, intent.ID)
					if err != nil || outcome.Outcome != storage.DeleteIntentNotExecuted && outcome.Outcome != storage.DeleteIntentUnknown {
						t.Fatalf("detached intent=%+v %v", outcome, err)
					}
				}
			})
		}
	}
}

func closeDeleteFixtureHandle(t *testing.T, f realSMBFixture, id wire.FileID) {
	t.Helper()
	body := make([]byte, 24)
	binary.LittleEndian.PutUint16(body, 24)
	copy(body[8:], id[:])
	if _, status := f.connection.closeFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}, wire.FileID{}); status != statusOK {
		t.Fatalf("CLOSE %x status=%#x", id, status)
	}
}

func TestDeleteOnCloseRejectsBeforeAtomicEffects(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		for _, test := range []struct {
			name                                              string
			disposition, access, options, attributes, sharing uint32
			exists, deny                                      bool
		}{
			{"missing-delete", 3, accessReadData, 0, 0, 7, false, false},
			{"readonly-existing", 4, accessReadData | accessWriteData | accessDelete, 0, 0, 7, true, false},
			{"readonly-created", 3, accessReadData | accessDelete, 0, dosReadOnly, 7, false, false},
			{"readonly-reset", 5, accessWriteData | accessDelete, 0, dosReadOnly, 7, true, false},
			{"root", 1, accessReadAttr | accessDelete, createDirectory, 0, 7, false, false},
			{"delete-denied", 5, accessWriteData | accessDelete, 0, 0, 7, true, true},
		} {
			t.Run(transport+"/"+test.name, func(t *testing.T) {
				factory := newRealSMBFixture
				if transport == "http" {
					factory = newHTTPSMBFixture
				}
				f := enableDeleteIntentsForTest(t, factory(t))
				name := "victim"
				if test.name == "root" {
					name = ""
				}
				var original storage.Attr
				if test.exists {
					if err := f.volume.Write(t.Context(), name, []byte("preserved")); err != nil {
						t.Fatal(err)
					}
					flags := uint32(dosHidden)
					if test.name == "readonly-existing" {
						flags |= dosReadOnly
					}
					original = seedIOWindowsMetadata(t, f, name, flags)
				}
				deleteAuthorization := false
				if test.deny {
					f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, r authz.AccessRequest) error {
						if r.Operation == storage.OpVolumeRemove || r.Operation == storage.OpFileSetPendingUnlink {
							deleteAuthorization = true
							return authz.ErrDenied
						}
						return nil
					})
				}
				request := deleteCreateRequestForTest(name, test.disposition, test.access, test.options)
				attributes := test.attributes
				if test.exists && test.disposition >= 4 {
					attributes |= dosHidden
				}
				binary.LittleEndian.PutUint32(request.Body[28:], attributes)
				binary.LittleEndian.PutUint32(request.Body[32:], test.sharing)
				if _, status, id := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, request); status == statusOK || id != (wire.FileID{}) {
					t.Fatalf("refused request status=%#x id=%x", status, id)
				}
				if test.deny && !deleteAuthorization {
					t.Fatal("delete policy was never checked")
				}
				page, err := f.raw.(storage.FileActions).ListDeleteIntents(t.Context(), f.tree.export.share.DeleteIntentOwner, 0, 1)
				if err != nil || len(page.Intents) != 0 {
					t.Fatalf("refusal left intent=%+v %v", page, err)
				}
				if test.exists {
					current := requireIOContents(t, f, name, "preserved")
					if current.ID != original.ID || !reflect.DeepEqual(current.Metadata, original.Metadata) {
						t.Fatalf("refusal changed existing metadata=%+v", current)
					}
				} else if name != "" {
					if _, err := f.volume.Stat(t.Context(), name); !errors.Is(err, syscall.ENOENT) {
						t.Fatalf("refusal created name: %v", err)
					}
				}
			})
		}
	}
}

func TestDeleteOnCloseDirectoryTriggerAndPendingAdmission(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, fixture realSMBFixture) {
		f := enableDeleteIntentsForTest(t, fixture)
		if err := f.volume.Mkdir(t.Context(), "nonempty"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), "nonempty/child", []byte("keep")); err != nil {
			t.Fatal(err)
		}
		_, status, nonempty := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, deleteCreateRequestForTest("nonempty", 1, accessReadAttr|accessDelete, createDirectory))
		if status != statusOK {
			t.Fatalf("nonempty acceptance=%#x", status)
		}
		closeBody := make([]byte, 24)
		binary.LittleEndian.PutUint16(closeBody, 24)
		copy(closeBody[8:], nonempty[:])
		if _, status := f.connection.closeFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: closeBody}, wire.FileID{}); status != closeStatusError(syscall.ENOTEMPTY) {
			t.Fatalf("nonempty trigger status=%#x", status)
		}
		requireIOContents(t, f, "nonempty/child", "keep")
		if f.tree.findFileHandle(nonempty) != nil {
			t.Fatal("released directory FileId still live")
		}
		if err := f.volume.Mkdir(t.Context(), "empty"); err != nil {
			t.Fatal(err)
		}
		_, status, first := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, deleteCreateRequestForTest("empty", 1, accessReadAttr|accessDelete, createDirectory))
		if status != statusOK {
			t.Fatalf("empty acceptance=%#x", status)
		}
		request := createRequestForTest("empty", 1, accessReadAttr)
		binary.LittleEndian.PutUint32(request.Body[40:], createDirectory)
		_, status, old := f.connection.createFile(t.Context(), f.session, f.tree, request)
		if status != statusOK {
			t.Fatalf("compatible old directory=%#x", status)
		}
		closeDeleteFixtureHandle(t, f, first)
		standard := queryInfoForTest(t, f, old, wire.FileStandardInformation)
		if standard[20] != 1 || standard[21] != 1 || binary.LittleEndian.Uint32(standard[16:]) != 0 {
			t.Fatalf("pending directory Standard=%x", standard)
		}
		if err := f.volume.Write(t.Context(), "empty/child", []byte("escape")); err == nil {
			t.Fatal("pending directory accepted SDK child")
		}
		if _, status, _ := f.connection.createFile(t.Context(), f.session, f.tree, createRequestForTest("empty/child", 3, accessWriteData)); status == statusOK {
			t.Fatal("pending directory accepted SMB child")
		}
		closeDeleteFixtureHandle(t, f, old)
		if _, err := f.volume.Stat(t.Context(), "empty"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("last directory close left name=%v", err)
		}
	})
}

func TestDeleteOnCloseAcceptedAuthoritySurvivesCurrentPermissionRevocation(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, fixture realSMBFixture) {
		f := enableDeleteIntentsForTest(t, fixture)
		_, status, id := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, deleteCreateRequestForTest("victim", 3, accessReadData|accessDelete, 0))
		if status != statusOK {
			t.Fatalf("accepted=%#x", status)
		}
		attr, err := f.volume.Stat(t.Context(), "victim")
		if err != nil {
			t.Fatal(err)
		}
		intent := acceptedDeleteIntentForTest(t, f, attr.ID)
		f.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, r authz.AccessRequest) error {
			if r.Operation == storage.OpFileSetPendingUnlink || r.Operation == storage.OpFileQueryDeleteIntent || r.Operation == storage.OpFileAcknowledgeDeleteIntent {
				return authz.ErrDenied
			}
			return nil
		})
		body := make([]byte, 24)
		binary.LittleEndian.PutUint16(body, 24)
		copy(body[8:], id[:])
		if _, status := f.connection.closeFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}, wire.FileID{}); status != statusDenied {
			t.Fatalf("revoked supplementary query status=%#x", status)
		}
		if f.tree.findFileHandle(id) != nil {
			t.Fatal("settled reference still has FileId")
		}
		if _, err := f.volume.Stat(t.Context(), "victim"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("revocation canceled accepted delete: %v", err)
		}
		requireDeleteOutcomeForTest(t, f, intent.ID, storage.DeleteIntentCompleted)
		if got := f.server.Status(); got.LocalDeleteOwners != 1 {
			t.Fatalf("revoked query lost responsibility=%+v", got)
		}
		if err := f.tree.export.RecoverDeleteIntents(WithPrincipal(t.Context(), f.session.principal)); !errors.Is(err, authz.ErrDenied) {
			t.Fatalf("recovery reused old authorization=%v", err)
		}
		if err := f.tree.export.RecoverDeleteIntents(t.Context()); !errors.Is(err, ErrIdentityDenied) {
			t.Fatalf("recovery accepted missing current principal=%v", err)
		}
		f.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
		if err := f.tree.export.RecoverDeleteIntents(WithPrincipal(t.Context(), f.session.principal)); err != nil {
			t.Fatal(err)
		}
		requireDeleteOutcomeForTest(t, f, intent.ID, storage.DeleteIntentUnknown)
		if got := f.server.Status(); got.LocalDeleteOwners != 0 || got.DeleteRecoveryCleanupOwners != 0 {
			t.Fatalf("recovery retained settled responsibility=%+v", got)
		}
	})
}

func TestDeleteOnCloseFiniteRecoveryKeepsProducerGateAndRevisitsEarlierPending(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, fixture realSMBFixture) {
		fixture.server.config.Limits.MaxUnresolvedOwners = 2
		f := enableDeleteIntentsForTest(t, fixture)
		var intents []storage.DeleteIntentID
		var retained storage.File
		for index := range 5 {
			action, err := storage.NewFileActionID(f.state.ActionEpoch)
			if err != nil {
				t.Fatal(err)
			}
			intent, err := storage.NewDeleteIntentID()
			if err != nil {
				t.Fatal(err)
			}
			leaf := fmt.Sprintf("recovery-%d", index)
			opened, err := f.raw.(storage.AtomicFileOpener).OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: f.tree.export.share.RootNodeID}, RawLeaf: []byte(leaf)}}, storage.OpenAtOptions{
				Read: true, Create: true, Existing: storage.Keep, Target: storage.ChildCondition{State: storage.Absent}, Action: action,
				Use:         storage.UseClaim{Uses: storage.ReadData | storage.DeleteName},
				CloseIntent: &storage.CloseIntent{ID: intent, Owner: f.tree.export.share.DeleteIntentOwner, Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile},
			})
			if err != nil || opened.File == nil {
				t.Fatalf("seed accepted SDK obligation=%+v %v", opened, err)
			}
			if index == 0 {
				retained, err = f.raw.OpenFile(t.Context(), leaf, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, Use: storage.UseClaim{Uses: storage.ReadData}})
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := opened.File.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
			intents = append(intents, intent)
		}
		ctx := WithPrincipal(t.Context(), f.session.principal)
		if err := f.tree.export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("bounded first pass=%v", err)
		}
		requireDeleteOutcomeForTest(t, f, intents[0], storage.DeleteIntentPending)
		requireDeleteOutcomeForTest(t, f, intents[1], storage.DeleteIntentUnknown)
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := f.tree.export.RecoverDeleteIntents(canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled sweep=%v", err)
		}
		for range 2 {
			if _, status, _ := f.connection.createFile(ctx, f.session, f.tree, deleteCreateRequestForTest("producer", 3, accessReadData|accessDelete, 0)); status == statusOK {
				t.Fatal("partial discovery admitted new producer")
			}
			if _, err := f.volume.Stat(t.Context(), "producer"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("closed producer gate had effects=%v", err)
			}
		}
		_, status, ordinary := f.connection.createFile(ctx, f.session, f.tree, createRequestForTest("ordinary", 3, accessReadData))
		if status != statusOK {
			t.Fatalf("producer fencing blocked ordinary I/O=%#x", status)
		}
		closeDeleteFixtureHandle(t, f, ordinary)
		for attempts := 0; ; attempts++ {
			err := f.tree.export.RecoverDeleteIntents(ctx)
			if err == nil {
				break
			}
			if !errors.Is(err, syscall.EAGAIN) || attempts >= 4 {
				t.Fatalf("finite tail did not finish=%v", err)
			}
		}
		requireDeleteOutcomeForTest(t, f, intents[0], storage.DeleteIntentPending)
		for _, intent := range intents[1:] {
			requireDeleteOutcomeForTest(t, f, intent, storage.DeleteIntentUnknown)
		}
		if err := retained.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		for attempts := 0; ; attempts++ {
			err := f.tree.export.RecoverDeleteIntents(ctx)
			if err == nil {
				break
			}
			if !errors.Is(err, syscall.EAGAIN) || attempts >= 2 {
				t.Fatalf("new pass did not revisit early pending=%v", err)
			}
		}
		requireDeleteOutcomeForTest(t, f, intents[0], storage.DeleteIntentUnknown)
		if status := f.server.Status(); status.DeleteRecoveryReadyExports != 1 || status.DeleteRecoveryCleanupOwners != 0 {
			t.Fatalf("completed pass not settled=%+v", status)
		}
	})
}

func TestDeleteOnCloseRecoveryReportsRealHTTPEnrollmentSaturation(t *testing.T) {
	f := newRealSMBFixture(t)
	options := httprest.DefaultHandlerOptions()
	options.Files = httprest.DefaultFileLimits()
	options.Files.MaxSessions = 1
	options.Files.Session = fixtureFileSessionOptions()
	handler, err := httprest.NewHandlerWithOptions(f.volume, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	service := httptest.NewServer(handler)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
		service.Close()
	})
	client, err := httprest.Dial(service.URL, service.Client())
	if err != nil {
		t.Fatal(err)
	}
	occupant, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := occupant.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	owner, err := storage.NewDeleteIntentOwner()
	if err != nil {
		t.Fatal(err)
	}
	server, export := newSMBDeleteCrashEndpoint(t, client, owner)
	ctx := WithPrincipal(t.Context(), testPrincipal("0123456789abcdef"))
	if err := export.RecoverDeleteIntents(ctx); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("real remote session saturation=%v", err)
	}
	if status := server.Status(); status.DeleteRecoveryPendingExports != 1 || status.DeleteRecoveryReadyExports != 0 || status.DeleteRecoveryCleanupOwners != 0 {
		t.Fatalf("failed enrollment lost configured owner=%+v", status)
	}
	if err := f.volume.Write(t.Context(), "ordinary", []byte("available")); err != nil {
		t.Fatal(err)
	}
	requireIOContents(t, f, "ordinary", "available")
	if err := occupant.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatalf("released remote headroom did not recover=%v", err)
	}
	if status := server.Status(); status.DeleteRecoveryReadyExports != 1 || status.DeleteRecoveryCleanupOwners != 0 {
		t.Fatalf("enrollment retry did not settle=%+v", status)
	}
	if err := export.Unpublish(ctx); err != nil {
		t.Fatal(err)
	}
}
