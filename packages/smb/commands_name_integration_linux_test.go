//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestSMBNameOperationsDirectAndHTTP(t *testing.T) {
	cases := []struct {
		name string
		run  func(*testing.T, realSMBFixture, string)
	}{
		{"TestSMBNameMutationBoundConflictsReobserveOriginalReference", testSMBNameMutationBoundConflictsReobserveOriginalReference},
		{"TestSMBNameMutationUnknownRetainsFrozenEffect", testSMBNameMutationUnknownRetainsFrozenEffect},
		{"TestSMBNameMutationCurrentIdentityAndReplacement", testSMBNameMutationCurrentIdentityAndReplacement},
		{"TestSMBOrdinaryDispositionClearPreservesCloseIntent", testSMBOrdinaryDispositionClearPreservesCloseIntent},
	}
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			seed := factory(t)
			for index, entry := range cases {
				t.Run(entry.name, func(t *testing.T) {
					f := newFileSessionFixture(t, seed)
					f.tree.sessionID = f.session.id
					entry.run(t, f, fmt.Sprintf("case%d-", index))
					status := f.server.Status()
					if status.Handles != 0 || status.OpeningHandles != 0 || status.RetainedDirectoryBytes != 0 || status.UnknownWrites != 0 || status.RetainedWriteBytes != 0 || status.PendingFlushes != 0 {
						t.Fatalf("name case retained live responsibility: %+v", status)
					}
				})
			}
			if transport == "direct" {
				t.Run("TestSMBNameMutationUnknownRecoveryRechecksRenamePolicy", func(t *testing.T) {
					f := newFileSessionFixture(t, seed)
					f.tree.sessionID = f.session.id
					testSMBNameMutationUnknownRecoveryRechecksRenamePolicy(t, f, "unknown-policy-")
				})
			}
		})
	}
}

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

func testSMBNameMutationBoundConflictsReobserveOriginalReference(t *testing.T, f realSMBFixture, prefix string) {
	if err := f.volume.Write(t.Context(), prefix+"original", []byte("body")); err != nil {
		t.Fatal(err)
	}
	id := openInfoFileForTest(t, f, prefix+"original", accessDelete)
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
			if err := f.volume.Mkdir(t.Context(), fmt.Sprintf("%sinterference-%d", prefix, calls)); err != nil {
				t.Fatal(err)
			}
		}
		return intercept.NamespaceAccess.MutateName(ctx, command)
	}
	f.tree.authority.raw = intercept
	if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest(prefix+"renamed", false)); status != statusOK || calls != 3 {
		t.Fatalf("two guarded retries status=%#x calls=%d", status, calls)
	}
	f.tree.authority.raw = raw
	if got := normalizedNameForTest(t, f, id); got != `\`+prefix+`renamed` {
		t.Fatalf("result name=%q", got)
	}
	closeFixtureHandle(t, f, id)
	if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
		t.Fatalf("retry captured=%d", got)
	}
}

func testSMBNameMutationUnknownRetainsFrozenEffect(t *testing.T, f realSMBFixture, prefix string) {
	if err := f.volume.Write(t.Context(), prefix+"original", []byte("body")); err != nil {
		t.Fatal(err)
	}
	id := openInfoFileForTest(t, f, prefix+"original", accessDelete)
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
	if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest(prefix+"renamed", false)); status != statusIO {
		t.Fatalf("lost result=%#x", status)
	}
	if f.tree.findFileHandle(id).pendingWrite == nil || calls != 1 {
		t.Fatalf("unknown discarded calls=%d", calls)
	}
	if err := f.volume.Rename(t.Context(), prefix+"renamed", prefix+"external"); err != nil {
		t.Fatal(err)
	}
	if err := f.volume.Write(t.Context(), prefix+"original", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	if got := normalizedNameForTest(t, f, id); got != `\`+prefix+`external` || calls != 2 {
		t.Fatalf("recovered name=%q calls=%d", got, calls)
	}
	f.tree.authority.raw = raw
	if data, err := f.volume.Read(t.Context(), prefix+"original"); err != nil || string(data) != "replacement" {
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

func testSMBNameMutationCurrentIdentityAndReplacement(t *testing.T, f realSMBFixture, prefix string) {
	for _, directory := range []string{"old", "new"} {
		if err := f.volume.Mkdir(t.Context(), prefix+directory); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.volume.Write(t.Context(), prefix+"old/source", []byte("original")); err != nil {
		t.Fatal(err)
	}
	if err := f.volume.Write(t.Context(), prefix+"new/target", []byte("replaced")); err != nil {
		t.Fatal(err)
	}
	id := openInfoFileForTest(t, f, prefix+`old\source`, accessReadData|accessDelete)
	h := f.tree.findFileHandle(id)
	target := openInfoFileForTest(t, f, prefix+`new\target`, accessReadData)
	oldTargetID := f.tree.findFileHandle(target).nodeID
	if err := f.volume.Rename(t.Context(), prefix+"old/source", prefix+"old/current"); err != nil {
		t.Fatal(err)
	}
	if err := f.volume.Write(t.Context(), prefix+"old/source", []byte("new occupant")); err != nil {
		t.Fatal(err)
	}
	if got := normalizedNameForTest(t, f, id); got != `\`+prefix+`old\current` {
		t.Fatalf("current name=%q", got)
	}
	if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest(prefix+`new\target`, false)); status != statusNameCollision {
		t.Fatalf("collision=%#x", status)
	}
	if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest(prefix+`new\TARGET`, true)); status != statusOK {
		t.Fatalf("rename=%#x", status)
	}
	if got := normalizedNameForTest(t, f, id); got != `\`+prefix+`new\TARGET` {
		t.Fatalf("renamed name=%q", got)
	}
	attr, err := f.volume.Stat(t.Context(), prefix+"new/TARGET")
	if err != nil || attr.ID != h.nodeID || attr.ID == oldTargetID {
		t.Fatalf("source identity changed attr=%+v err=%v", attr, err)
	}
	if data, err := f.volume.Read(t.Context(), prefix+"old/source"); err != nil || string(data) != "new occupant" {
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
}

func testSMBOrdinaryDispositionClearPreservesCloseIntent(t *testing.T, f realSMBFixture, prefix string) {
	f = enableDeleteIntentsForTest(t, f)
	if err := f.volume.Write(t.Context(), prefix+"file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	other := openInfoFileForTest(t, f, prefix+"file", accessDelete)
	_, status, id := f.connection.createFile(WithPrincipal(t.Context(), f.session.principal), f.session, f.tree, deleteCreateRequestForTest(prefix+"file", 1, accessDelete, 0))
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
	if _, err := f.volume.Stat(t.Context(), prefix+"file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("DOC canceled by ordinary CLEAR: %v", err)
	}
	if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
		t.Fatalf("DOC name retained=%d", got)
	}
	ctx := WithPrincipal(t.Context(), f.session.principal)
	if err := f.tree.export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatalf("settle completed DOC owner: %v", err)
	}
	if err := f.server.Shutdown(ctx); err != nil {
		t.Fatalf("DOC endpoint cleanup: %v", err)
	}
}

func testSMBNameMutationUnknownRecoveryRechecksRenamePolicy(t *testing.T, f realSMBFixture, prefix string) {
	if err := f.volume.Write(t.Context(), prefix+"original", []byte("body")); err != nil {
		t.Fatal(err)
	}
	id := openInfoFileForTest(t, f, prefix+"original", accessDelete)
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
	if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest(prefix+"renamed", false)); status != statusIO {
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
	if got := normalizedNameForTest(t, f, id); got != `\`+prefix+`renamed` || calls != 2 || queries != 1 {
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
}
