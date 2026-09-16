package httprest

import (
	"context"
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

func TestFileControlBoundIncludesCompleteWorstCaseReceipt(t *testing.T) {
	metadata := storage.Metadata{{Key: strings.Repeat("k", storage.MaxMetadataKeyBytes), Version: math.MaxUint32, Data: make([]byte, storage.MaxMetadataBytes-6-10-storage.MaxMetadataKeyBytes)}}
	attr := protocolAttr()
	attr.ID = math.MaxUint64
	attr.Kind = storage.NodeSymlink
	attr.Size = storage.MaxLinkTargetBytes
	attr.Metadata = metadata
	attr.MetadataRevision = math.MaxUint64
	location := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: math.MaxUint64 - storage.MaxLocationDepth, NodeID: attr.ID}
	for i := range storage.MaxLocationDepth {
		location.Ancestors = append(location.Ancestors, storage.EntryCondition{ParentID: math.MaxUint64 - storage.MaxLocationDepth + uint64(i), NodeID: math.MaxUint64 - storage.MaxLocationDepth + uint64(i) + 1, EntryID: storage.EntryID(math.MaxUint64 - uint64(i)), DirectoryRevision: math.MaxUint64, Name: []byte(strings.Repeat("&", storage.MaxLocationNameBytes/storage.MaxLocationDepth))})
	}
	observation, err := fileObservationOf(storage.FileObservation{Attr: attr, Location: &location, LinkTarget: []byte(strings.Repeat("&", storage.MaxLinkTargetBytes))})
	if err != nil {
		t.Fatal(err)
	}
	receipt := fileReceipt{Action: storage.FileActionID("18446744073709551615:" + strings.Repeat("f", 32)), Operation: storage.OpFileRetain, State: storage.FileActionCompleted, Effects: storage.EffectRetained, Reference: math.MaxUint64, Observation: observation, RangeRevision: math.MaxUint64, HistoryRemaining: math.MaxInt64}
	req := fileRequest{Op: storage.OpFileQueryAction, Action: receipt.Action}
	if err := validateFileReceipt(req, receipt); err != nil {
		t.Fatal(err)
	}
	complete := fileErrorResponse{Errno: "EIO", Message: strings.Repeat("&", fileDiagnosticBytes), Receipt: &receipt}
	data, err := json.Marshal(complete)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(data)) > fileControlResponseLimit() || len(data) <= int(DefaultMaxLockControlBytes) {
		t.Fatalf("receipt bytes=%d bound=%d", len(data), fileControlResponseLimit())
	}
	var decoded fileErrorResponse
	if err := decodeFileJSON(data, &decoded); err != nil {
		t.Fatal(err)
	}
	got, err := decoded.Receipt.storage()
	if err != nil || len(got.Observation.Location.Ancestors) != storage.MaxLocationDepth || len(got.Observation.LinkTarget) != storage.MaxLinkTargetBytes || decoded.Message != complete.Message {
		t.Fatalf("bounded facts lost: %+v %v", got, err)
	}
}

func TestFileClientBodyCapabilityRefusesBeforeAdmissionOrEffect(t *testing.T) {
	calls := 0
	client := protocolClient(t, func(fileRequest) (*http.Response, error) {
		calls++
		return protocolResponse(t, 200, fileResponse{State: &storage.FileVolumeState{RootID: 1, VolumeIdentity: "volume", MaxEventBytes: 4096}}), nil
	})
	client.maxBodyBytes = fileControlResponseLimit() - 1
	if _, err := client.FileState(t.Context()); !errors.Is(err, syscall.EFBIG) || !storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("state capacity=%v", err)
	}
	if _, _, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EFBIG) || !storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("enrollment capacity=%v", err)
	}
	if _, err := protocolSession(client).Retain(t.Context(), storage.RetainRequest{NodeID: 7}, protocolID(t)); !errors.Is(err, syscall.EFBIG) || !storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("retain capacity=%v", err)
	}
	if calls != 0 {
		t.Fatalf("insufficient receipt capacity dispatched %d calls", calls)
	}
	client.maxBodyBytes = fileControlResponseLimit()
	if _, err := client.FileState(t.Context()); err != nil || calls != 1 {
		t.Fatalf("exact capacity=%v calls=%d", err, calls)
	}
}

func TestFileControlIdentitiesDiagnosticsAndEventCapabilityRemainBounded(t *testing.T) {
	for name, identity := range map[string]string{"missing": "", "long": strings.Repeat("i", MaxLockCapabilityBytes+1), "invalid utf8": "\xff"} {
		t.Run(name, func(t *testing.T) {
			if err := validateFileResponse(fileRequest{Op: storage.OpFileState}, fileResponse{State: &storage.FileVolumeState{VolumeIdentity: identity, RootID: 1, MaxEventBytes: 4096}}); err == nil {
				t.Fatal("invalid volume identity accepted")
			}
		})
	}
	status := protocolStatus()
	status.Epoch = strings.Repeat("e", MaxLockCapabilityBytes+1)
	if err := validateFileResponse(fileRequest{Op: storage.OpFileStatus}, fileResponse{Status: &status}); err == nil {
		t.Fatal("unbounded epoch accepted")
	}
	client := protocolClient(t, func(fileRequest) (*http.Response, error) {
		return protocolResponse(t, StatusStorageError, fileErrorResponse{Errno: "EACCES", Message: strings.Repeat("x", fileDiagnosticBytes+1)}), nil
	})
	if _, err := protocolSession(client).Status(t.Context()); !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EACCES) {
		t.Fatalf("unbounded diagnostic=%v", err)
	}
	client = protocolClient(t, func(fileRequest) (*http.Response, error) {
		return protocolResponse(t, 200, fileResponse{State: &storage.FileVolumeState{VolumeIdentity: "volume", RootID: 1, MaxEventBytes: 4096}}), nil
	})
	for _, frameLimit := range []int64{1, 1024, DefaultMaxFrameBytes} {
		client.maxFrameBytes = frameLimit
		state, err := client.FileState(t.Context())
		if err != nil || state.MaxEventBytes != 4096 || state.VolumeIdentity != "volume" || state.RootID != 1 {
			t.Fatalf("generic state changed under frame limit %d: %+v %v", frameLimit, state, err)
		}
	}
}

func TestFileControlAdmissionIndependentOfBulkAndRangeWaits(t *testing.T) {
	client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
		status := protocolStatus()
		return protocolResponse(t, 200, fileResponse{Status: &status}), nil
	})
	bulk, err := client.fileRequests.acquire(t.Context(), client.fileRequests.maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer bulk()
	wait, err := client.fileWaits.acquire(t.Context(), client.fileWaits.maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer wait()
	locks, err := client.lockControls.acquire(t.Context(), client.lockControls.maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer locks()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := protocolSession(client).Status(ctx); err != nil {
		t.Fatalf("bulk prevented control: %v", err)
	}
}
