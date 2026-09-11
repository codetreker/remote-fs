package httprest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/codetreker/remote-fs/packages/storage"
	"io"
	"math"
	"reflect"
	"unicode/utf8"
)

func decodeFileJSON(data []byte, target any) error {
	if !utf8.Valid(data) {
		return errors.New("file JSON must be UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := checkLockJSON(decoder, reflect.TypeOf(target).Elem()); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("file JSON contains trailing content")
	}
	return json.Unmarshal(data, target)
}

func validFileCapability(cap string) bool {
	if len(cap) != 64 {
		return false
	}
	for _, c := range cap {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validateFileRequest(r fileRequest) error {
	expected := fileRequest{Op: r.Op, Session: r.Session, Action: r.Action, Path: []byte{}, Data: []byte{}}
	if r.Op == storage.OpFileSessionOpen {
		if r.Session != "" {
			return errors.New("new file session cannot name a previous session")
		}
	} else if !validFileCapability(r.Session) {
		return errors.New("invalid file session capability")
	}
	if fileActionRequired(r.Op) || r.Action != "" && (r.Op == storage.OpFileClose || r.Op == storage.OpFileSessionClose) {
		if _, err := r.Action.Epoch(); err != nil {
			return err
		}
	} else if r.Action != "" {
		return errors.New("file operation does not accept an action identity")
	}
	switch r.Op {
	case storage.OpFileSessionOpen:
		expected.Options = r.Options
	case storage.OpFileStatus, storage.OpFileRenew, storage.OpFileSessionClose:
	case storage.OpFileOpen:
		expected.Path = r.Path
		expected.Open = r.Open
	case storage.OpFileOpenNode:
		expected.Node = r.Node
		expected.Open = r.Open
	case storage.OpFileStatNode:
		expected.Node = r.Node
	case storage.OpFileSetNodeAttr:
		expected.Node = r.Node
		expected.Change = r.Change
	case storage.OpFileStat, storage.OpFileSync, storage.OpFileClose, storage.OpFileAck:
		expected.File = r.File
	case storage.OpFileRead:
		expected.File = r.File
		expected.Offset = r.Offset
		expected.Length = r.Length
	case storage.OpFileWrite:
		expected.File = r.File
		expected.Offset = r.Offset
		expected.Data = r.Data
	case storage.OpFileTruncate:
		expected.File = r.File
		expected.Offset = r.Offset
	case storage.OpFileSetAttr:
		expected.File = r.File
		expected.Change = r.Change
	case storage.OpFileGetLock:
		expected.File = r.File
		expected.Owner = r.Owner
		expected.Lock = r.Lock
	case storage.OpFileSetLock, storage.OpFileUnlock:
		expected.File = r.File
		expected.Owner = r.Owner
		expected.Lock = r.Lock
		expected.LockID = r.LockID
	case storage.OpFileQueryLock, storage.OpFileCancelLock:
		expected.File = r.File
		expected.Owner = r.Owner
		expected.LockID = r.LockID
	case storage.OpFileDropLocks:
		expected.File = r.File
		expected.Owner = r.Owner
		expected.Family = r.Family
	default:
		return errors.New("unknown retained file operation")
	}
	if !reflect.DeepEqual(r, expected) {
		return errors.New("file operation carries unrelated operands")
	}
	if expected.File != "" && !validFileCapability(expected.File) {
		return errors.New("invalid file reference capability")
	}
	switch r.Op {
	case storage.OpFileStat, storage.OpFileSync, storage.OpFileClose, storage.OpFileAck, storage.OpFileRead, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileGetLock, storage.OpFileSetLock, storage.OpFileUnlock, storage.OpFileQueryLock, storage.OpFileCancelLock, storage.OpFileDropLocks:
		if !validFileCapability(r.File) {
			return errors.New("file operation has no reference capability")
		}
	}
	if (r.Op == storage.OpFileSetAttr || r.Op == storage.OpFileSetNodeAttr) && r.Change == nil {
		return errors.New("file attribute operation carries no change")
	}
	return nil
}

func validateFileResponse(req fileRequest, r fileResponse) error {
	if r.Epoch == 0 {
		return errors.New("file response carries no action epoch")
	}
	expected := fileResponse{Epoch: r.Epoch, Data: []byte{}}
	if r.Retry {
		if !fileActionRequired(req.Op) {
			return errors.New("unexpected file action retry receipt")
		}
		expected.Retry = true
	} else {
		switch req.Op {
		case storage.OpFileSessionOpen:
			expected.Session = r.Session
			expected.Status = r.Status
		case storage.OpFileStatus, storage.OpFileRenew:
			expected.Status = r.Status
		case storage.OpFileOpen, storage.OpFileOpenNode:
			expected.File = r.File
			expected.Barrier = r.Barrier
		case storage.OpFileStat, storage.OpFileStatNode:
			expected.Attr = r.Attr
		case storage.OpFileRead:
			expected.Attr = r.Attr
			expected.Data = r.Data
		case storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileSetNodeAttr:
			expected.Attr = r.Attr
			expected.Barrier = r.Barrier
		case storage.OpFileSync:
			expected.Barrier = r.Barrier
		case storage.OpFileGetLock:
			expected.Conflict = r.Conflict
		case storage.OpFileSetLock, storage.OpFileUnlock, storage.OpFileQueryLock, storage.OpFileCancelLock:
			expected.Attempt = r.Attempt
		case storage.OpFileAck, storage.OpFileClose, storage.OpFileSessionClose, storage.OpFileDropLocks:
		default:
			return errors.New("unknown file response variant")
		}
	}
	if !reflect.DeepEqual(r, expected) {
		return errors.New("file response carries unrelated result fields")
	}
	if r.Retry {
		return nil
	}
	switch req.Op {
	case storage.OpFileSessionOpen:
		if !validFileCapability(r.Session) || r.Status == nil {
			return errors.New("file session response has no valid capability or status")
		}
	case storage.OpFileStatus, storage.OpFileRenew:
		if r.Status == nil {
			return errors.New("file response carries no status")
		}
	case storage.OpFileOpen, storage.OpFileOpenNode:
		if !validFileCapability(r.File) {
			return errors.New("file open response carries no reference capability")
		}
	case storage.OpFileRead, storage.OpFileStat, storage.OpFileStatNode, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileSetNodeAttr:
		if r.Attr == nil {
			return errors.New("file response carries no attributes")
		}
	case storage.OpFileGetLock:
		if r.Conflict == nil {
			return errors.New("file response carries no conflict result")
		}
	case storage.OpFileSetLock, storage.OpFileUnlock, storage.OpFileQueryLock, storage.OpFileCancelLock:
		if r.Attempt == nil {
			return errors.New("file response carries no lock action result")
		}
	}
	if r.Status != nil {
		s := r.Status
		if s.Epoch == "" || len(s.Epoch) > MaxLockCapabilityBytes || s.Revision == 0 || s.ActionEpoch == 0 || s.Remaining < 0 || s.HistoryRemaining < 0 {
			return errors.New("invalid file session status")
		}
	}
	if r.Attr != nil {
		if r.Attr.ID == 0 || r.Attr.AccessTime.Nanos < 0 || r.Attr.AccessTime.Nanos >= 1e9 || r.Attr.ModTime.Nanos < 0 || r.Attr.ModTime.Nanos >= 1e9 {
			return errors.New("invalid captured file attributes")
		}
		if req.Op != storage.OpFileStatNode && req.Op != storage.OpFileSetNodeAttr && (r.Attr.Size < 0 || !r.Attr.Storage().Mode.IsRegular()) {
			return errors.New("file reference returned nonregular attributes")
		}
	}
	if r.Conflict != nil {
		if err := validateFileConflict(*r.Conflict); err != nil {
			return err
		}
	}
	if r.Attempt != nil {
		attempt, err := r.Attempt.storage()
		if err != nil {
			return err
		}
		if err := validateFileAttempt(req, attempt); err != nil {
			return err
		}
	}
	return nil
}

func validateFileConflict(c storage.LockConflict) error {
	if !c.Found {
		if c != (storage.LockConflict{}) {
			return errors.New("absent lock conflict carries owner or range state")
		}
		return nil
	}
	if err := c.Lock.Check(); err != nil {
		return err
	}
	if c.Lock.Type == storage.Unlock {
		return errors.New("lock conflict identifies an unlocked range")
	}
	return nil
}

func validateFileAttempt(req fileRequest, a storage.LockAttempt) error {
	if a.Request != req.LockID {
		return errors.New("lock result identifies a different action")
	}
	if err := a.Lock.Check(); err != nil {
		return err
	}
	if err := validateFileConflict(a.Conflict); err != nil {
		return err
	}
	if a.HistoryRemaining < 0 {
		return errors.New("lock history lifetime is negative")
	}
	if (req.Op == storage.OpFileSetLock || req.Op == storage.OpFileUnlock) && a.Lock != req.Lock {
		return errors.New("lock result identifies a different intent")
	}
	switch a.State {
	case storage.LockPending:
		if !a.Lock.Wait || a.EverGranted || a.Errno != 0 {
			return errors.New("inconsistent pending lock result")
		}
	case storage.LockGranted:
		if !a.EverGranted || a.Errno != 0 || a.Lock.Type == storage.Unlock {
			return errors.New("inconsistent granted lock result")
		}
	case storage.LockReleased:
		if a.Errno != 0 || a.EverGranted != (a.Lock.Type != storage.Unlock) {
			return errors.New("inconsistent released lock result")
		}
	case storage.LockCancelled:
		if a.EverGranted || a.Errno != 0 {
			return errors.New("inconsistent cancelled lock result")
		}
	case storage.LockRejected:
		if _, ok := storage.ErrnoName(a.Errno); !ok || a.EverGranted {
			return errors.New("inconsistent rejected lock result")
		}
	default:
		return fmt.Errorf("unknown advisory lock action state %d", a.State)
	}
	return nil
}

var fileReadEnvelopeBytes = func() int64 {
	worstTime := Time{UnixSec: math.MinInt64, Nanos: 999999999}
	response := fileResponse{Epoch: math.MaxUint64, Data: []byte{}, Attr: &Attr{ID: math.MaxUint64, Mode: math.MaxUint32, Size: math.MaxInt64, AccessTime: worstTime, ModTime: worstTime}}
	encoded, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}
	return int64(len(encoded))
}()

func fileReadLimit(limit int64) int64 {
	if limit < fileReadEnvelopeBytes {
		return -1
	}
	return (limit - fileReadEnvelopeBytes) / 4 * 3
}
