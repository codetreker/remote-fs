//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func renameInformationForTest(name string, replace bool) []byte {
	encoded := wire.EncodeUTF16(name)
	data := make([]byte, 20+len(encoded))
	if replace {
		data[0] = 255
	}
	binary.LittleEndian.PutUint32(data[16:], uint32(len(encoded)))
	copy(data[20:], encoded)
	return data
}

type nameMutationInterceptSession struct {
	storage.FileSession
	storage.FileSessionIdentity
	storage.DirectoryMetadataObserver
	storage.NamespaceAccess
	storage.FileActions
	mutate func(context.Context, storage.NameCommand) (storage.NameResult, error)
	query  func(context.Context, storage.FileActionID) (storage.FileActionReceipt, error)
}

func (s *nameMutationInterceptSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	return s.mutate(ctx, command)
}
func (s *nameMutationInterceptSession) QueryFileAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	if s.query != nil {
		return s.query(ctx, id)
	}
	return s.FileActions.QueryFileAction(ctx, id)
}

func nameMutationInterceptForTest(raw storage.FileSession) *nameMutationInterceptSession {
	return &nameMutationInterceptSession{FileSession: raw, FileSessionIdentity: raw.(storage.FileSessionIdentity), DirectoryMetadataObserver: raw.(storage.DirectoryMetadataObserver), NamespaceAccess: raw.(storage.NamespaceAccess), FileActions: raw.(storage.FileActions)}
}

func TestSMBNameMutationBoundConflictsReobserveOriginalReference(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "original", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "original", accessDelete)
		h := f.tree.findFileHandle(id)
		raw := f.tree.authority.raw
		intercept := nameMutationInterceptForTest(raw)
		calls := 0
		actions := make(map[storage.FileActionID]bool)
		intercept.mutate = func(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
			calls++
			if command.Target.NodeID != h.nodeID || actions[command.Action] {
				t.Fatalf("retry lost original identity/fresh action: %+v", command)
			}
			actions[command.Action] = true
			if calls <= 2 {
				if err := f.volume.Write(t.Context(), "interference", []byte{byte(calls)}); err != nil {
					t.Fatal(err)
				}
				if err := f.volume.Rename(t.Context(), "interference", "interference-next"); err != nil {
					t.Fatal(err)
				}
				if err := f.volume.Remove(t.Context(), "interference-next"); err != nil {
					t.Fatal(err)
				}
			}
			return intercept.NamespaceAccess.MutateName(ctx, command)
		}
		f.tree.authority.raw = intercept
		if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest("renamed", false)); status != statusOK || calls != 3 {
			t.Fatalf("two guarded retries status=%#x calls=%d", status, calls)
		}
		f.tree.authority.raw = raw
		if got := normalizedNameForTest(t, f, id); got != `\renamed` {
			t.Fatalf("result name=%q", got)
		}
		closeFixtureHandle(t, f, id)
		if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
			t.Fatalf("retry captured=%d", got)
		}
	})
}

func TestSMBNameMutationUnknownRetainsFrozenEffect(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "original", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "original", accessDelete)
		raw := f.tree.authority.raw
		intercept := nameMutationInterceptForTest(raw)
		calls, queries := 0, 0
		var frozen *storage.NameCommand
		intercept.mutate = func(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
			calls++
			if calls == 1 {
				frozen = mutationCommand{name: &command}.clone().name
			} else if !reflect.DeepEqual(*frozen, command) {
				t.Fatal("unknown action changed frozen payload")
			}
			result, err := intercept.NamespaceAccess.MutateName(ctx, command)
			if calls == 1 && err == nil {
				return storage.NameResult{}, syscall.EIO
			}
			return result, err
		}
		intercept.query = func(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
			queries++
			if queries == 1 {
				return storage.FileActionReceipt{}, syscall.EIO
			}
			return intercept.FileActions.QueryFileAction(ctx, id)
		}
		f.tree.authority.raw = intercept
		if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest("renamed", false)); status != statusIO {
			t.Fatalf("lost result=%#x", status)
		}
		if f.tree.findFileHandle(id).pendingWrite == nil || calls != 1 {
			t.Fatalf("unknown discarded calls=%d", calls)
		}
		if err := f.volume.Rename(t.Context(), "renamed", "external"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), "original", []byte("replacement")); err != nil {
			t.Fatal(err)
		}
		if got := normalizedNameForTest(t, f, id); got != `\external` || calls != 2 {
			t.Fatalf("recovered name=%q calls=%d", got, calls)
		}
		f.tree.authority.raw = raw
		if data, err := f.volume.Read(t.Context(), "original"); err != nil || string(data) != "replacement" {
			t.Fatalf("recovery rebound original: %q %v", data, err)
		}
		body := make([]byte, 24)
		binary.LittleEndian.PutUint16(body, 24)
		copy(body[8:], id[:])
		if _, status := f.connection.closeFile(t.Context(), f.session, f.tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}, wire.FileID{}); status != statusIO {
			t.Fatalf("recovered error CLOSE=%#x", status)
		}
		if f.server.Status().Handles != 0 {
			t.Fatal("CLOSE did not release recovered reference")
		}
		if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
			t.Fatalf("unknown captured=%d", got)
		}
	})
}

func nameSetStatusForTest(t *testing.T, f realSMBFixture, id wire.FileID, class byte, data []byte) uint32 {
	t.Helper()
	ctx, finish := ioResponseContextForTest(t.Context())
	defer finish()
	_, status := f.connection.setFileInfo(ctx, f.session, f.tree, setInfoRequestForTest(id, class, data))
	return status
}

func normalizedNameForTest(t *testing.T, f realSMBFixture, id wire.FileID) string {
	t.Helper()
	body, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileNormalizedNameInformation, 4096))
	if status != statusOK || len(body) < 12 {
		t.Fatalf("normalized name status=%#x body=%x", status, body)
	}
	length := int(binary.LittleEndian.Uint32(body[8:]))
	if length != len(body)-12 {
		t.Fatalf("normalized counted length=%d body=%x", length, body)
	}
	name, err := wire.DecodeUTF16(body[12:])
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func TestSMBNameMutationCurrentIdentityAndReplacement(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		for _, directory := range []string{"old", "new"} {
			if err := f.volume.Mkdir(t.Context(), directory); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.volume.Write(t.Context(), "old/source", []byte("original")); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), "new/target", []byte("replaced")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, `old\source`, accessReadData|accessDelete)
		h := f.tree.findFileHandle(id)
		target := openInfoFileForTest(t, f, `new\target`, accessReadData)
		oldTargetID := f.tree.findFileHandle(target).nodeID
		if err := f.volume.Rename(t.Context(), "old/source", "old/current"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), "old/source", []byte("new occupant")); err != nil {
			t.Fatal(err)
		}
		if got := normalizedNameForTest(t, f, id); got != `\old\current` {
			t.Fatalf("current name=%q", got)
		}
		if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest(`new\target`, false)); status != statusNameCollision {
			t.Fatalf("collision=%#x", status)
		}
		if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest(`new\TARGET`, true)); status != statusOK {
			t.Fatalf("rename=%#x", status)
		}
		if got := normalizedNameForTest(t, f, id); got != `\new\TARGET` {
			t.Fatalf("renamed name=%q", got)
		}
		attr, err := f.volume.Stat(t.Context(), "new/TARGET")
		if err != nil || attr.ID != h.nodeID || attr.ID == oldTargetID {
			t.Fatalf("source identity changed attr=%+v err=%v", attr, err)
		}
		if data, err := f.volume.Read(t.Context(), "old/source"); err != nil || string(data) != "new occupant" {
			t.Fatalf("replacement source=%q %v", data, err)
		}
		body, status := f.connection.readFile(t.Context(), f.session, f.tree, readRequestForTest(target, 0, 20, 0))
		if status != statusOK || !bytes.Equal(body[16:], []byte("replaced")) {
			t.Fatalf("old target reference status=%#x body=%x", status, body)
		}
		if _, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(target, wire.FileNormalizedNameInformation, 4096)); status != statusInvalid {
			t.Fatalf("detached normalized name=%#x", status)
		}
		closeFixtureHandle(t, f, target)
		closeFixtureHandle(t, f, id)
		if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
			t.Fatalf("retained name bytes=%d", got)
		}
	})
}

func TestSMBOrdinaryDispositionAuthorizationAndMetadata(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "file", accessDelete)
		if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, nil); status != statusInfoLengthMismatch {
			t.Fatalf("empty disposition=%#x", status)
		}
		if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{255, 255}); status != statusOK {
			t.Fatalf("ordinary TRUE=%#x", status)
		}
		standard := queryInfoForTest(t, f, id, wire.FileStandardInformation)
		if standard[20] != 1 {
			t.Fatalf("not delete pending: %x", standard)
		}
		if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{0, 255}); status != statusOK {
			t.Fatalf("ordinary FALSE=%#x", status)
		}
		if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{0}); status != statusOK {
			t.Fatalf("actual empty-CAS CLEAR=%#x", status)
		}
		prior := f.server.config.Authorize
		f.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
			if request.Operation == storage.OpFileClearPendingUnlink {
				return authz.ErrDenied
			}
			return prior.Authorize(ctx, request)
		})
		if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{0}); status != statusDenied {
			t.Fatalf("CLEAR bypassed authorization=%#x", status)
		}
		f.server.config.Authorize = prior
		closeFixtureHandle(t, f, id)
		if _, err := f.volume.Stat(t.Context(), "file"); err != nil {
			t.Fatal(err)
		}
		seedIOWindowsMetadata(t, f, "file", dosReadOnly)
		id = openInfoFileForTest(t, f, "file", accessDelete)
		if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{1}); status != 0xc0000121 {
			t.Fatalf("readonly TRUE=%#x", status)
		}
		if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest("renamed", false)); status != statusOK {
			t.Fatalf("readonly rename=%#x", status)
		}
		closeFixtureHandle(t, f, id)
		if _, err := f.volume.Stat(t.Context(), "renamed"); err != nil {
			t.Fatal(err)
		}
		if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
			t.Fatalf("retained=%d", got)
		}
	})
}

func TestSMBOrdinaryDispositionCloseAndDirectoryRestrictions(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Mkdir(t.Context(), "dir"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), "dir/child", []byte("body")); err != nil {
			t.Fatal(err)
		}
		request := createRequestForTest("dir", 1, accessDelete)
		binary.LittleEndian.PutUint32(request.Body[40:], createDirectory)
		_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, request)
		if status != statusOK {
			t.Fatalf("directory open=%#x", status)
		}
		child := openInfoFileForTest(t, f, `dir\child`, accessReadData)
		if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest("moved", false)); status != statusDenied {
			t.Fatalf("open descendant rename=%#x", status)
		}
		if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{1}); status != statusDirectoryNotEmpty {
			t.Fatalf("nonempty disposition=%#x", status)
		}
		closeFixtureHandle(t, f, child)
		if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest("moved", false)); status != statusOK {
			t.Fatalf("directory rename=%#x", status)
		}
		if err := f.volume.Remove(t.Context(), "moved/child"); err != nil {
			t.Fatal(err)
		}
		if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{1}); status != statusOK {
			t.Fatalf("empty disposition=%#x", status)
		}
		closeFixtureHandle(t, f, id)
		if _, err := f.volume.Stat(t.Context(), "moved"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("directory close removal=%v", err)
		}
	})
}

func TestSMBNormalizedNameBuffersRootAndResourceRefund(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Mkdir(t.Context(), "dir"); err != nil {
			t.Fatal(err)
		}
		if err := f.volume.Write(t.Context(), "dir/long-name", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, `dir\long-name`, accessDelete)
		if _, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileNameInformation, 4096)); status != statusUnsupported {
			t.Fatalf("class9=%#x", status)
		}
		for _, length := range []uint32{0, 7} {
			body, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileNormalizedNameInformation, length))
			if status != statusInfoLengthMismatch || !bytes.Equal(body, wire.InfoLengthMismatchErrorBody()) {
				t.Fatalf("short header %d status=%#x body=%x", length, status, body)
			}
		}
		request := queryInfoRequestForTest(id, wire.FileNormalizedNameInformation, 9)
		body, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, request)
		if status != statusBufferOverflow || len(body) != 16 || binary.LittleEndian.Uint32(body[8:]) != 28 || !bytes.Equal(body[12:], wire.EncodeUTF16(`\d`)) {
			t.Fatalf("overflow status=%#x body=%x", status, body)
		}
		if responseBudget(request) != 88 || relatedOperationFailed(request, status, body) {
			t.Fatalf("valid name warning failed compound budget=%d", responseBudget(request))
		}
		if !relatedOperationFailed(queryInfoRequestForTest(id, wire.FileNameInformation, 9), status, body) {
			t.Fatal("unsupported class warning accepted")
		}
		before := f.server.Status().RetainedDirectoryBytes
		f.server.config.Limits.MaxRetainedDirectoryBytes = before + 100
		if _, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileNormalizedNameInformation, 4096)); status != statusResources {
			t.Fatalf("name resource status=%#x", status)
		}
		if got := f.server.Status().RetainedDirectoryBytes; got != before {
			t.Fatalf("error retained=%d before=%d", got, before)
		}
		f.server.config.Limits.MaxRetainedDirectoryBytes = DefaultLimits().MaxRetainedDirectoryBytes
		closeFixtureHandle(t, f, id)
		requestRoot := createRequestForTest("", 1, 0)
		binary.LittleEndian.PutUint32(requestRoot.Body[40:], createDirectory)
		_, status, root := f.connection.createFile(t.Context(), f.session, f.tree, requestRoot)
		if status != statusOK {
			t.Fatalf("root open=%#x", status)
		}
		if got := normalizedNameForTest(t, f, root); got != `\` {
			t.Fatalf("root name=%q", got)
		}
		closeFixtureHandle(t, f, root)
		if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
			t.Fatalf("root retained=%d", got)
		}
	})
}

type nameDirectoryInterceptSession struct {
	storage.FileSession
	storage.FileSessionIdentity
	storage.DirectoryMetadataObserver
	after func(context.Context, storage.DirectoryTarget)
}

func (s *nameDirectoryInterceptSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	observed, err := s.DirectoryMetadataObserver.ObserveDirectoryMetadata(ctx, target, options, result)
	if err == nil && s.after != nil {
		s.after(ctx, target)
	}
	return observed, err
}

func TestSMBNameCaptureRejectsMixedHistoryAndFinalStaleness(t *testing.T) {
	for _, phase := range []string{"parent-binding", "final-validation"} {
		t.Run(phase, func(t *testing.T) {
			fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
				if err := f.volume.Mkdir(t.Context(), "old"); err != nil {
					t.Fatal(err)
				}
				if err := f.volume.Write(t.Context(), "old/file", []byte("body")); err != nil {
					t.Fatal(err)
				}
				id := openInfoFileForTest(t, f, `old\file`, accessDelete)
				parent, err := f.volume.Stat(t.Context(), "old")
				if err != nil {
					t.Fatal(err)
				}
				original := f.tree.authority.raw
				intercept := &nameDirectoryInterceptSession{FileSession: original, FileSessionIdentity: original.(storage.FileSessionIdentity), DirectoryMetadataObserver: original.(storage.DirectoryMetadataObserver)}
				changed := false
				intercept.after = func(ctx context.Context, target storage.DirectoryTarget) {
					if changed {
						return
					}
					if phase == "parent-binding" && target.NodeID == parent.ID {
						changed = true
						if err := f.volume.Rename(ctx, "old", "new"); err != nil {
							t.Fatal(err)
						}
					}
					if phase == "final-validation" && target.NodeID == f.tree.authority.identity.Backend.RootNodeID {
						changed = true
						if err := f.volume.Rename(ctx, "old/file", "old/changed"); err != nil {
							t.Fatal(err)
						}
					}
				}
				f.tree.authority.raw = intercept
				_, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileNormalizedNameInformation, 4096))
				if !changed || status != statusRetry {
					t.Fatalf("mixed observation changed=%v status=%#x", changed, status)
				}
				f.tree.authority.raw = original
				want := `\new\file`
				if phase == "final-validation" {
					want = `\old\changed`
				}
				if got := normalizedNameForTest(t, f, id); got != want {
					t.Fatalf("fresh name=%q want=%q", got, want)
				}
				closeFixtureHandle(t, f, id)
				if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
					t.Fatalf("capture leak=%d", got)
				}
			})
		})
	}
}

func TestSMBOrdinaryDispositionClearPreservesCloseIntent(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		f = enableDeleteIntentsForTest(t, f)
		if err := f.volume.Write(t.Context(), "file", []byte("body")); err != nil {
			t.Fatal(err)
		}
		other := openInfoFileForTest(t, f, "file", accessDelete)
		_, status, id := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, deleteCreateRequestForTest("file", 1, accessDelete, 0))
		if status != statusOK {
			t.Fatalf("DOC open=%#x", status)
		}
		intent := acceptedDeleteIntentForTest(t, f, f.tree.findFileHandle(id).nodeID)
		for _, value := range []byte{0, 1, 0} {
			if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{value}); status != statusOK {
				t.Fatalf("DOC ordinary %d=%#x", value, status)
			}
			requireDeleteOutcomeForTest(t, f, intent.ID, storage.DeleteIntentArmed)
		}
		closeDeleteFixtureHandle(t, f, id)
		requireDeleteOutcomeForTest(t, f, intent.ID, storage.DeleteIntentPending)
		for _, value := range []byte{0, 1, 0} {
			if status := nameSetStatusForTest(t, f, other, wire.FileDispositionInformation, []byte{value}); status != statusOK {
				t.Fatalf("pending DOC ordinary %d=%#x", value, status)
			}
			if standard := queryInfoForTest(t, f, other, wire.FileStandardInformation); standard[20] != 1 {
				t.Fatalf("ordinary CLEAR canceled pending DOC: %x", standard)
			}
			requireDeleteOutcomeForTest(t, f, intent.ID, storage.DeleteIntentPending)
		}
		closeDeleteFixtureHandle(t, f, other)
		requireDeleteOutcomeForTest(t, f, intent.ID, storage.DeleteIntentCompleted)
		if _, err := f.volume.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("DOC canceled by ordinary CLEAR: %v", err)
		}
		if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
			t.Fatalf("DOC name retained=%d", got)
		}
	})
}

func TestSMBNameMutationRequiresRenamePolicyBeforeAndAfterFIFO(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "admission", true: "after-FIFO"}[queued], func(t *testing.T) {
			fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
				if err := f.volume.Write(t.Context(), "original", []byte("body")); err != nil {
					t.Fatal(err)
				}
				id := openInfoFileForTest(t, f, "original", accessDelete)
				h := f.tree.findFileHandle(id)
				raw := f.tree.authority.raw
				intercept := nameMutationInterceptForTest(raw)
				var calls atomic.Int32
				intercept.mutate = func(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
					calls.Add(1)
					return intercept.NamespaceAccess.MutateName(ctx, command)
				}
				f.tree.authority.raw = intercept
				prior := f.server.config.Authorize
				var denied atomic.Bool
				denied.Store(!queued)
				f.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
					if request.Operation == storage.OpVolumeRename && denied.Load() {
						return authz.ErrDenied
					}
					return prior.Authorize(ctx, request)
				})
				status := uint32(0)
				if !queued {
					status = nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest("renamed", false))
				} else {
					first, err := f.tree.admitFileIO(t.Context(), f.session, id, f.server.config.Limits.MaxHandleIORequests)
					if err != nil {
						t.Fatal(err)
					}
					defer first.finish()
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					result := make(chan uint32, 1)
					go func() {
						_, status := f.connection.setFileInfo(ctx, f.session, f.tree, setInfoRequestForTest(id, wire.FileRenameInformation, renameInformationForTest("renamed", false)))
						result <- status
					}()
					for {
						h.ioMu.Lock()
						waiting := len(h.ioQueue) != 0
						h.ioMu.Unlock()
						if waiting {
							break
						}
						select {
						case status := <-result:
							t.Fatalf("rename ended before queue: %#x", status)
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						default:
							runtime.Gosched()
						}
					}
					denied.Store(true)
					first.finish()
					select {
					case status = <-result:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if status != statusDenied || calls.Load() != 0 || h.pendingWrite != nil {
					t.Fatalf("rename policy status=%#x calls=%d owner=%v", status, calls.Load(), h.pendingWrite != nil)
				}
				f.server.config.Authorize = prior
				f.tree.authority.raw = raw
				if got := normalizedNameForTest(t, f, id); got != `\original` {
					t.Fatalf("denied rename changed name=%q", got)
				}
				if _, err := f.volume.Stat(t.Context(), "renamed"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("denied rename target=%v", err)
				}
				closeFixtureHandle(t, f, id)
				if f.server.Status().RetainedDirectoryBytes != 0 {
					t.Fatal("denied rename retained capture")
				}
			})
		})
	}
}

func TestSMBNameMutationRetryRechecksRenamePolicy(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "original", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "original", accessDelete)
		h := f.tree.findFileHandle(id)
		raw := f.tree.authority.raw
		intercept := nameMutationInterceptForTest(raw)
		prior := f.server.config.Authorize
		var denied atomic.Bool
		f.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
			if request.Operation == storage.OpVolumeRename && denied.Load() {
				return authz.ErrDenied
			}
			return prior.Authorize(ctx, request)
		})
		calls, queries := 0, 0
		var action storage.FileActionID
		intercept.mutate = func(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
			calls++
			action = command.Action
			if err := f.volume.Mkdir(t.Context(), "interference"); err != nil {
				t.Fatal(err)
			}
			return intercept.NamespaceAccess.MutateName(ctx, command)
		}
		intercept.query = func(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
			queries++
			receipt, err := intercept.FileActions.QueryFileAction(ctx, id)
			if err == nil && receipt.Outcome == storage.FileActionNotExecuted {
				denied.Store(true)
			}
			return receipt, err
		}
		f.tree.authority.raw = intercept
		if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest("renamed", false)); status != statusDenied || calls != 1 || queries != 1 {
			t.Fatalf("retry policy status=%#x calls=%d queries=%d", status, calls, queries)
		}
		owner := h.pendingWrite
		if owner == nil || owner.pending() || !errors.Is(owner.lastErr, authz.ErrDenied) {
			t.Fatalf("known refusal lost authorization error owner=%v", owner)
		}
		facts := f.server.Status().WriteFailures
		if len(facts) != 1 || facts[0].Execution != WriteExecutionNotExecuted || !facts[0].ChainSettled || !facts[0].PayloadReleased {
			t.Fatalf("known refusal facts=%+v", facts)
		}
		receipt, err := intercept.FileActions.QueryFileAction(t.Context(), action)
		if err != nil || receipt.Outcome != storage.FileActionNotExecuted {
			t.Fatalf("original action lost nonexecution receipt=%+v err=%v", receipt, err)
		}
		f.server.config.Authorize = prior
		f.tree.authority.raw = raw
		if attr, err := f.volume.Stat(t.Context(), "original"); err != nil || attr.ID != h.nodeID {
			t.Fatalf("retry denied source=%+v err=%v", attr, err)
		}
		if _, err := f.volume.Stat(t.Context(), "renamed"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("revoked retry target=%v", err)
		}
		if err := f.tree.closeFileHandle(t.Context(), h); !errors.Is(err, authz.ErrDenied) {
			t.Fatalf("CLOSE hid authorization error: %v", err)
		}
		if f.tree.findFileHandle(id) != nil || f.server.Status().RetainedDirectoryBytes != 0 {
			t.Fatal("known refusal retained reference/capture")
		}
	})
}

func TestSMBNameMutationUnknownRecoveryRechecksRenamePolicy(t *testing.T) {
	fileInfoFixturesForTest(t, func(t *testing.T, f realSMBFixture) {
		if err := f.volume.Write(t.Context(), "original", []byte("body")); err != nil {
			t.Fatal(err)
		}
		id := openInfoFileForTest(t, f, "original", accessDelete)
		h := f.tree.findFileHandle(id)
		raw := f.tree.authority.raw
		intercept := nameMutationInterceptForTest(raw)
		prior := f.server.config.Authorize
		var denied atomic.Bool
		f.server.config.Authorize = authz.AuthorizerFunc(func(ctx context.Context, request authz.AccessRequest) error {
			if request.Operation == storage.OpVolumeRename && denied.Load() {
				return authz.ErrDenied
			}
			return prior.Authorize(ctx, request)
		})
		calls, queries := 0, 0
		var frozen *storage.NameCommand
		intercept.mutate = func(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
			calls++
			if calls == 1 {
				frozen = mutationCommand{name: &command}.clone().name
			} else if !reflect.DeepEqual(*frozen, command) {
				t.Fatal("recovery changed original action/payload")
			}
			result, err := intercept.NamespaceAccess.MutateName(ctx, command)
			if calls == 1 && err == nil {
				denied.Store(true)
				return storage.NameResult{}, syscall.EIO
			}
			return result, err
		}
		intercept.query = func(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
			queries++
			return intercept.FileActions.QueryFileAction(ctx, id)
		}
		f.tree.authority.raw = intercept
		if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest("renamed", false)); status != statusIO {
			t.Fatalf("revoked query converted unknown to %#x", status)
		}
		owner := h.pendingWrite
		if owner == nil || !owner.pending() || !errors.Is(owner.lastErr, authz.ErrDenied) {
			t.Fatalf("unknown lost authorization error owner=%v", owner)
		}
		if _, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileNormalizedNameInformation, 4096)); status != statusIO || calls != 1 || queries != 0 {
			t.Fatalf("revoked recovery status=%#x calls=%d queries=%d", status, calls, queries)
		}
		facts := f.server.Status().WriteFailures
		if len(facts) != 1 || facts[0].Execution != WriteExecutionUnknown || facts[0].PayloadReleased || facts[0].ChainSettled || owner.command.action() != frozen.Action {
			t.Fatalf("unknown lost frozen responsibility=%+v", facts)
		}
		denied.Store(false)
		if got := normalizedNameForTest(t, f, id); got != `\renamed` || calls != 2 || queries != 1 {
			t.Fatalf("authorized recovery name=%q calls=%d queries=%d", got, calls, queries)
		}
		f.server.config.Authorize = prior
		f.tree.authority.raw = raw
		if err := f.tree.closeFileHandle(t.Context(), h); !errors.Is(err, authz.ErrDenied) {
			t.Fatalf("CLOSE hid original authorization error: %v", err)
		}
		if f.tree.findFileHandle(id) != nil || f.server.Status().RetainedDirectoryBytes != 0 {
			t.Fatal("recovered refusal retained reference/capture")
		}
	})
}
