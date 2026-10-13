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
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func TestSMBNameHostContracts(t *testing.T) {
	cases := []struct {
		name string
		run  func(*testing.T, realSMBFixture, *nativeDeleteBackend)
	}{
		{"TestSMBOrdinaryDispositionAuthorizationAndMetadata", nameHostDispositionPolicy},
		{"TestSMBOrdinaryDispositionCloseAndDirectoryRestrictions", nameHostDirectoryRestrictions},
		{"TestSMBNormalizedNameBuffersRootAndResourceRefund", nameHostNormalizedBuffers},
		{"TestSMBNameCaptureRejectsMixedHistoryAndFinalStaleness", nameHostCaptureHistory},
		{"TestSMBNameMutationRequiresRenamePolicyBeforeAndAfterFIFO", nameHostQueuedPolicy},
		{"TestSMBNameMutationRetryRechecksRenamePolicy", nameHostRetryPolicy},
	}
	for _, entry := range cases {
		t.Run(entry.name, func(t *testing.T) {
			model := NewNativeNameProbeBackend()
			raw, err := model.NewFileSession(t.Context(), fixtureFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := raw.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			f := realSMBFixtureForSession(t, nil, model, raw)
			f.tree.sessionID = f.session.id
			entry.run(t, f, model)
			status := f.server.Status()
			if status.Handles != 0 || status.OpeningHandles != 0 || status.RetainedDirectoryBytes != 0 || status.UnknownWrites != 0 || status.RetainedWriteBytes != 0 || status.PendingFlushes != 0 {
				t.Fatalf("host case retained live responsibility: %+v", status)
			}
			model.mu.Lock()
			references := len(model.refs)
			model.mu.Unlock()
			if references != 0 {
				t.Fatalf("host case retained %d references", references)
			}
		})
	}
}

func nameHostDispositionPolicy(t *testing.T, f realSMBFixture, model *nativeDeleteBackend) {
	id := openInfoFileForTest(t, f, "alpha.txt", accessDelete)
	if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, nil); status != statusInfoLengthMismatch {
		t.Fatalf("empty disposition=%#x", status)
	}
	if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{255, 255}); status != statusOK {
		t.Fatalf("ordinary TRUE=%#x", status)
	}
	if standard := queryInfoForTest(t, f, id, wire.FileStandardInformation); standard[20] != 1 {
		t.Fatalf("not pending=%x", standard)
	}
	for _, data := range [][]byte{{0, 255}, {0}} {
		if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, data); status != statusOK {
			t.Fatalf("ordinary/empty-CAS CLEAR=%#x", status)
		}
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
	if state := model.ProbeNodeState(3); state.Detached || state.Attr.ID != 3 {
		t.Fatalf("CLEAR lost source=%+v", state)
	}
	data, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	model.mu.Lock()
	attr := model.attr(3)
	attr.Metadata = map[string]storage.OpaquePayload{windowsMetadataKey: {Version: []byte("readonly"), Data: data}}
	model.updateAttr(attr)
	model.mu.Unlock()
	id = openInfoFileForTest(t, f, "alpha.txt", accessDelete)
	if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{1}); status != 0xc0000121 {
		t.Fatalf("readonly TRUE=%#x", status)
	}
	if status := nameSetStatusForTest(t, f, id, wire.FileRenameInformation, renameInformationForTest("renamed", false)); status != statusOK {
		t.Fatalf("readonly source rename=%#x", status)
	}
	closeFixtureHandle(t, f, id)
	model.mu.Lock()
	binding := model.binding(3)
	model.mu.Unlock()
	if binding.State != storage.NameLinked || string(binding.RawLeaf) != "renamed" {
		t.Fatalf("readonly source binding=%+v", binding)
	}
}

func nameHostDirectoryRestrictions(t *testing.T, f realSMBFixture, model *nativeDeleteBackend) {
	request := createRequestForTest("folder", 1, accessDelete)
	binary.LittleEndian.PutUint32(request.Body[40:], createDirectory)
	_, status, id := f.connection.createFile(t.Context(), f.session, f.tree, request)
	if status != statusOK {
		t.Fatalf("directory open=%#x", status)
	}
	child := openInfoFileForTest(t, f, `folder\nested.txt`, accessReadData)
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
	model.mu.Lock()
	model.unlink(6)
	model.mu.Unlock()
	if status := nameSetStatusForTest(t, f, id, wire.FileDispositionInformation, []byte{1}); status != statusOK {
		t.Fatalf("empty disposition=%#x", status)
	}
	closeFixtureHandle(t, f, id)
	model.mu.Lock()
	binding := model.binding(2)
	model.mu.Unlock()
	if binding.State != storage.NameDetached {
		t.Fatalf("directory close binding=%+v", binding)
	}
}

func nameHostNormalizedBuffers(t *testing.T, f realSMBFixture, _ *nativeDeleteBackend) {
	id := openInfoFileForTest(t, f, `folder\nested.txt`, accessDelete)
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
	if status != statusBufferOverflow || len(body) != 16 || binary.LittleEndian.Uint32(body[8:]) != uint32(len(wire.EncodeUTF16(`\folder\nested.txt`))) || !bytes.Equal(body[12:], wire.EncodeUTF16(`\f`)) {
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
		t.Fatalf("name resources=%#x", status)
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
}

func nameHostCaptureHistory(t *testing.T, f realSMBFixture, model *nativeDeleteBackend) {
	for _, phase := range []string{"parent-binding", "final-validation"} {
		t.Run(phase, func(t *testing.T) {
			model.mu.Lock()
			model.views[1][2].Name = "folder"
			model.views[2][0].Name = "nested.txt"
			model.revision++
			model.mu.Unlock()
			id := openInfoFileForTest(t, f, `folder\nested.txt`, accessDelete)
			original := f.tree.authority.raw
			intercept := &nameDirectoryInterceptSession{FileSession: original, FileSessionIdentity: original.(storage.FileSessionIdentity), DirectoryMetadataObserver: original.(storage.DirectoryMetadataObserver)}
			changed := false
			intercept.after = func(_ context.Context, target storage.DirectoryTarget) {
				if changed {
					return
				}
				if phase == "parent-binding" && target.NodeID == 2 {
					changed = true
					model.mu.Lock()
					model.views[1][2].Name = "moved"
					model.revision++
					model.mu.Unlock()
				}
				if phase == "final-validation" && target.NodeID == 1 {
					changed = true
					model.mu.Lock()
					model.views[2][0].Name = "changed.txt"
					model.revision++
					model.mu.Unlock()
				}
			}
			f.tree.authority.raw = intercept
			_, status := f.connection.queryFileInfo(t.Context(), f.session, f.tree, queryInfoRequestForTest(id, wire.FileNormalizedNameInformation, 4096))
			if !changed || status != statusRetry {
				t.Fatalf("mixed observation changed=%v status=%#x", changed, status)
			}
			f.tree.authority.raw = original
			want := `\moved\nested.txt`
			if phase == "final-validation" {
				want = `\folder\changed.txt`
			}
			if got := normalizedNameForTest(t, f, id); got != want {
				t.Fatalf("fresh name=%q want=%q", got, want)
			}
			closeFixtureHandle(t, f, id)
			if got := f.server.Status().RetainedDirectoryBytes; got != 0 {
				t.Fatalf("capture leak=%d", got)
			}
		})
	}
}
func nameHostQueuedPolicy(t *testing.T, f realSMBFixture, model *nativeDeleteBackend) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "admission", true: "after-FIFO"}[queued], func(t *testing.T) {
			before := model.ProbeNodeState(3)
			id := openInfoFileForTest(t, f, "alpha.txt", accessDelete)
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
			if got := normalizedNameForTest(t, f, id); got != `\alpha.txt` {
				t.Fatalf("denied rename changed name=%q", got)
			}
			if after := model.ProbeNodeState(3); !reflect.DeepEqual(before, after) {
				t.Fatalf("denied rename changed source facts: before=%+v after=%+v", before, after)
			}
			model.mu.Lock()
			binding := model.binding(3)
			model.mu.Unlock()
			if binding.State != storage.NameLinked || string(binding.RawLeaf) != "alpha.txt" {
				t.Fatalf("denied rename binding=%+v", binding)
			}
			closeFixtureHandle(t, f, id)
			if f.server.Status().RetainedDirectoryBytes != 0 {
				t.Fatal("denied rename retained capture")
			}
		})
	}
}

func nameHostRetryPolicy(t *testing.T, f realSMBFixture, model *nativeDeleteBackend) {
	before := model.ProbeNodeState(3)
	id := openInfoFileForTest(t, f, "alpha.txt", accessDelete)
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
		model.mu.Lock()
		model.revision++
		model.mu.Unlock()
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
	if after := model.ProbeNodeState(3); !reflect.DeepEqual(before, after) {
		t.Fatalf("retry changed source: before=%+v after=%+v", before, after)
	}
	model.mu.Lock()
	binding := model.binding(3)
	model.mu.Unlock()
	if binding.State != storage.NameLinked || string(binding.RawLeaf) != "alpha.txt" {
		t.Fatalf("revoked retry binding=%+v", binding)
	}
	if err := f.tree.closeFileHandle(t.Context(), h); !errors.Is(err, authz.ErrDenied) {
		t.Fatalf("CLOSE hid authorization error: %v", err)
	}
	if f.tree.findFileHandle(id) != nil || f.server.Status().RetainedDirectoryBytes != 0 {
		t.Fatal("known refusal retained reference/capture")
	}
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
