package httprest

import (
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

const windowsDiagnosticBytes = 1024

// The failure envelope can contain the action's attributes, its recorded symlink,
// and the returned symlink error together. All three paths and both target/suffix
// pairs must remain reconcilable after an open has already succeeded.
func windowsControlResponseLimit() int64 {
	stamp := Time{UnixSec: math.MinInt64, Nanos: 999999999}
	attr := &windowsAttr{Basic: &windowsBasicAttr{Attr: &Attr{ID: math.MaxUint64, Mode: math.MaxUint32, Size: math.MaxInt64, AccessTime: stamp, ModTime: stamp}, CreationTime: stamp, ChangeTime: stamp, DOSAttributes: math.MaxUint32}, NameInfo: storage.WindowsNameInfo{State: storage.WindowsNameLinked}}
	link := &windowsSymlink{Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked}}
	actionID := storage.WindowsActionID("18446744073709551615:" + strings.Repeat("f", 32))
	capability := strings.Repeat("f", 64)
	errnoName := ""
	for _, errno := range storage.Errnos() {
		name, _ := storage.ErrnoName(errno)
		if len(name) > len(errnoName) {
			errnoName = name
		}
	}
	failure := storage.WindowsSharingViolation
	for _, code := range []storage.WindowsFailure{storage.WindowsLockConflict, storage.WindowsDeletePending, storage.WindowsRangeNotLocked, storage.WindowsNotReparsePoint} {
		if len(code) > len(failure) {
			failure = code
		}
	}
	action := &windowsAction{Action: actionID, State: storage.WindowsActionRejected, Attr: attr, File: capability, CreateAction: storage.WindowsSuperseded, Errno: errnoName, Failure: failure, Applied: math.MaxInt, HistoryRemaining: time.Duration(math.MaxInt64), Symlink: link}
	activation := &windowsActivation{Action: actionID, State: storage.WindowsActionRejected, Errno: errnoName, HistoryRemaining: time.Duration(math.MaxInt64)}
	failureEnvelope := windowsErrorResponse{Errno: errnoName, Failure: failure, Action: action, Activation: activation, Symlink: link}
	fixed, err := json.Marshal(failureEnvelope)
	if err != nil {
		panic(err)
	}
	stringsBound := int64(3*storage.WindowsMaxNameInfoBytes + 4*storage.WindowsMaxLinkTargetBytes + windowsDiagnosticBytes)
	receiptBound := int64(len(fixed)) + 6*stringsBound
	// A capability or session status uses bounded identity text in place of paths.
	// Including both status shapes is conservative even though the wire separates them.
	statuses := windowsResponse{State: &storage.WindowsState{ActionEpoch: math.MaxUint64, MaxEventBytes: math.MaxInt64, VolumeIdentity: strings.Repeat("&", MaxLockCapabilityBytes), VolumeSerial: math.MaxUint64}, Status: &storage.FileSessionStatus{Epoch: strings.Repeat("&", MaxLockCapabilityBytes), Remaining: time.Duration(math.MaxInt64), Revision: math.MaxUint64, ActionEpoch: math.MaxUint64, HistoryRemaining: time.Duration(math.MaxInt64)}, Session: capability, Data: []byte{}, Entries: []windowsEntry{}}
	fixed, err = json.Marshal(statuses)
	if err != nil {
		panic(err)
	}
	return max(receiptBound, int64(len(fixed)))
}
