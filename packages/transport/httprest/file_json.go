package httprest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
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
	if r.Op == authz.FileSessionOpen {
		if r.Session != "" {
			return errors.New("new file session cannot name a previous session")
		}
	} else if !validFileCapability(r.Session) {
		return errors.New("invalid file session capability")
	}
	if fileActionRequired(r.Op) || r.Action != "" && (r.Op == authz.FileClose || r.Op == authz.FileSessionClose) {
		if _, err := r.Action.Epoch(); err != nil {
			return err
		}
	} else if r.Action != "" {
		return errors.New("file operation does not accept an action identity")
	}
	switch r.Op {
	case authz.FileSessionOpen:
		expected.Options = r.Options
	case authz.FileStatus, authz.FileRenew, authz.FileSessionClose:
	case authz.FileOpen:
		expected.Path = r.Path
		expected.Open = r.Open
	case authz.FileOpenNode:
		expected.Node = r.Node
		expected.Open = r.Open
	case authz.FileStatNode:
		expected.Node = r.Node
	case authz.FileSetNodeAttr:
		expected.Node = r.Node
		expected.Change = r.Change
	case authz.FileStat, authz.FileSync, authz.FileClose, authz.FileAck:
		expected.File = r.File
	case authz.FileRead:
		expected.File = r.File
		expected.Offset = r.Offset
		expected.Length = r.Length
	case authz.FileWrite:
		expected.File = r.File
		expected.Offset = r.Offset
		expected.Data = r.Data
	case authz.FileTruncate:
		expected.File = r.File
		expected.Offset = r.Offset
	case authz.FileSetAttr:
		expected.File = r.File
		expected.Change = r.Change
	case authz.FileGetLock:
		expected.File = r.File
		expected.Owner = r.Owner
		expected.Lock = r.Lock
	case authz.FileSetLock, authz.FileUnlock:
		expected.File = r.File
		expected.Owner = r.Owner
		expected.Lock = r.Lock
		expected.LockID = r.LockID
	case authz.FileQueryLock, authz.FileCancelLock:
		expected.File = r.File
		expected.Owner = r.Owner
		expected.LockID = r.LockID
	case authz.FileDropLocks:
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
	case authz.FileStat, authz.FileSync, authz.FileClose, authz.FileAck, authz.FileRead, authz.FileWrite, authz.FileTruncate, authz.FileSetAttr, authz.FileGetLock, authz.FileSetLock, authz.FileUnlock, authz.FileQueryLock, authz.FileCancelLock, authz.FileDropLocks:
		if !validFileCapability(r.File) {
			return errors.New("file operation has no reference capability")
		}
	}
	if (r.Op == authz.FileSetAttr || r.Op == authz.FileSetNodeAttr) && r.Change == nil {
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
		case authz.FileSessionOpen:
			expected.Session = r.Session
			expected.Status = r.Status
		case authz.FileStatus, authz.FileRenew:
			expected.Status = r.Status
		case authz.FileOpen, authz.FileOpenNode:
			expected.File = r.File
			expected.Barrier = r.Barrier
		case authz.FileStat, authz.FileStatNode:
			expected.Attr = r.Attr
		case authz.FileRead:
			expected.Attr = r.Attr
			expected.Data = r.Data
		case authz.FileWrite, authz.FileTruncate, authz.FileSetAttr, authz.FileSetNodeAttr:
			expected.Attr = r.Attr
			expected.Barrier = r.Barrier
		case authz.FileSync:
			expected.Barrier = r.Barrier
		case authz.FileGetLock:
			expected.Conflict = r.Conflict
		case authz.FileSetLock, authz.FileUnlock, authz.FileQueryLock, authz.FileCancelLock:
			expected.Attempt = r.Attempt
		case authz.FileAck, authz.FileClose, authz.FileSessionClose, authz.FileDropLocks:
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
	case authz.FileSessionOpen:
		if !validFileCapability(r.Session) || r.Status == nil {
			return errors.New("file session response has no valid capability or status")
		}
	case authz.FileStatus, authz.FileRenew:
		if r.Status == nil {
			return errors.New("file response carries no status")
		}
	case authz.FileOpen, authz.FileOpenNode:
		if !validFileCapability(r.File) {
			return errors.New("file open response carries no reference capability")
		}
	case authz.FileRead, authz.FileStat, authz.FileStatNode, authz.FileWrite, authz.FileTruncate, authz.FileSetAttr, authz.FileSetNodeAttr:
		if r.Attr == nil {
			return errors.New("file response carries no attributes")
		}
	case authz.FileGetLock:
		if r.Conflict == nil {
			return errors.New("file response carries no conflict result")
		}
	case authz.FileSetLock, authz.FileUnlock, authz.FileQueryLock, authz.FileCancelLock:
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
		if req.Op != authz.FileStatNode && req.Op != authz.FileSetNodeAttr && (r.Attr.Size < 0 || !r.Attr.Storage().Mode.IsRegular()) {
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
	if (req.Op == authz.FileSetLock || req.Op == authz.FileUnlock) && a.Lock != req.Lock {
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
