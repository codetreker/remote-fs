package httprest

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func windowsEscapedPath(n int) string {
	var path strings.Builder
	for n > 256 {
		path.WriteString(strings.Repeat("&", 255))
		path.WriteByte('/')
		n -= 256
	}
	if n == 256 {
		path.WriteString(strings.Repeat("&", 254))
		path.WriteByte('/')
		path.WriteByte('&')
	} else {
		path.WriteString(strings.Repeat("&", n))
	}
	return path.String()
}

func TestWindowsControlBoundIncludesCompleteWorstCaseReceipt(t *testing.T) {
	// Ampersands expand to six JSON bytes per UTF-16 unit.
	path := windowsEscapedPath(min(storage.WindowsMaxNameInfoBytes, storage.WindowsMaxPathUTF16Units))
	attr := windowsTestAttr()
	attr.NameInfo.Path = path
	attr.ID = math.MaxUint64
	attr.Size = math.MaxInt64
	link := &windowsSymlink{Target: windowsEscapedPath(storage.WindowsMaxLinkTargetBytes), Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: path}, Unparsed: "/" + windowsEscapedPath(storage.WindowsMaxLinkTargetBytes-1)}
	if err := validateWindowsSymlink(link); err != nil {
		t.Fatal(err)
	}
	if err := validateWindowsAttr(windowsAttrOf(attr)); err != nil {
		t.Fatal(err)
	}
	action := &windowsAction{Action: storage.WindowsActionID("18446744073709551615:" + strings.Repeat("f", 32)), State: storage.WindowsActionRejected, Attr: windowsAttrOf(attr), Errno: "ELOOP", HistoryRemaining: time.Duration(math.MaxInt64), Symlink: link}
	if err := validateWindowsAction(*action); err != nil {
		t.Fatal(err)
	}
	complete := windowsErrorResponse{Errno: "ELOOP", Message: strings.Repeat("&", windowsDiagnosticBytes), Action: action, Symlink: link}
	encoded, err := json.Marshal(complete)
	if err != nil {
		t.Fatal(err)
	}
	limit := windowsControlResponseLimit()
	if int64(len(encoded)) > limit {
		t.Fatalf("complete %d-byte receipt exceeds %d-byte proven bound", len(encoded), limit)
	}
	if len(encoded) <= int(DefaultMaxLockControlBytes) {
		t.Fatal("receipt did not exercise the prior control bound")
	}
	var decoded windowsErrorResponse
	if err := decodeWindowsJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Action.Attr.NameInfo.Path != path || decoded.Action.Symlink.Target != link.Target || decoded.Symlink.Unparsed != link.Unparsed || decoded.Message != complete.Message {
		t.Fatal("complete receipt lost authoritative fields")
	}
}

func TestWindowsClientBodyCapabilityRefusesBeforeAdmissionOrEffect(t *testing.T) {
	calls := 0
	client := windowsTestClient(t, func(windowsRequest) (windowsResponse, error) {
		calls++
		return windowsResponse{State: &storage.WindowsState{ActionEpoch: 1, MaxEventBytes: 512 << 10, VolumeIdentity: "volume"}}, nil
	})
	client.maxBodyBytes = windowsControlResponseLimit() - 1
	session := &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}
	if _, err := client.WindowsState(t.Context()); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("small body capability = %v", err)
	}
	if _, err := client.EnableWindows(t.Context(), windowsTestID(t)); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("small body activation = %v", err)
	}
	if _, err := client.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("small body session = %v", err)
	}
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Disposition: storage.WindowsOpen, Kind: storage.WindowsDirectory}}
	if _, err := session.Open(t.Context(), request, windowsTestID(t)); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("small body open = %v", err)
	}
	if calls != 0 {
		t.Fatalf("insufficient complete-receipt capacity dispatched %d calls", calls)
	}
	client.maxBodyBytes = windowsControlResponseLimit()
	state, err := client.WindowsState(t.Context())
	if err != nil || state.MaxEventBytes != 512<<10 || calls != 1 {
		t.Fatalf("exact body capability = %+v, %v; calls=%d", state, err, calls)
	}
}

func TestWindowsControlIdentitiesAndDiagnosticsRemainBounded(t *testing.T) {
	for name, state := range map[string]storage.WindowsState{
		"missing":      {ActionEpoch: 1, MaxEventBytes: 4096},
		"long":         {ActionEpoch: 1, MaxEventBytes: 4096, VolumeIdentity: strings.Repeat("i", MaxLockCapabilityBytes+1)},
		"invalid utf8": {ActionEpoch: 1, MaxEventBytes: 4096, VolumeIdentity: "\xff"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateWindowsState(&state); err == nil {
				t.Fatal("invalid identity accepted")
			}
		})
	}
	status := windowsTestStatus()
	status.Epoch = strings.Repeat("e", MaxLockCapabilityBytes+1)
	response := emptyWindowsResponse()
	response.Status = &status
	if err := validateWindowsResponse(windowsRequest{Op: storage.OpWindowsStatus}, response); err == nil {
		t.Fatal("unbounded epoch accepted")
	}
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		return windowsTestHTTPResponse(t, StatusStorageError, windowsErrorResponse{Errno: "EACCES", Message: strings.Repeat("&", windowsDiagnosticBytes+1)}), nil
	})
	if _, err := client.WindowsState(t.Context()); !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EACCES) {
		t.Fatalf("unbounded diagnostic accepted: %v", err)
	}
}

func TestWindowsControlAdmissionIsIndependentOfBulkAndNativeLockControls(t *testing.T) {
	client := windowsTestClient(t, func(windowsRequest) (windowsResponse, error) {
		return windowsResponse{State: &storage.WindowsState{ActionEpoch: 1, MaxEventBytes: 4096, VolumeIdentity: "volume"}}, nil
	})
	bulkBytes := retainedResponseMultiplier * client.maxBodyBytes
	client.fileRequests = newBodyAdmission(1, bulkBytes, 0)
	bulk, err := client.fileRequests.acquire(t.Context(), bulkBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer bulk()
	lockBytes := retainedResponseMultiplier * DefaultMaxLockControlBytes
	client.lockControls = newBodyAdmission(1, lockBytes, 0)
	locks, err := client.lockControls.acquire(t.Context(), lockBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer locks()
	if _, err := client.WindowsState(t.Context()); err != nil {
		t.Fatalf("independent Windows controls were blocked: %v", err)
	}
}
