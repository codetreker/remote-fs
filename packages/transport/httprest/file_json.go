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
	if r.Op == "new" {
		if r.Session != "" {
			return errors.New("new file session cannot name a previous session")
		}
	} else if !validFileCapability(r.Session) {
		return errors.New("invalid file session capability")
	}
	if fileActionRequired(r.Op) || r.Action != "" && (r.Op == "close" || r.Op == "session-close") {
		if _, err := r.Action.Epoch(); err != nil {
			return err
		}
	} else if r.Action != "" {
		return errors.New("file operation does not accept an action identity")
	}
	switch r.Op {
	case "new":
		expected.Options = r.Options
	case "status", "renew", "session-close":
	case "open":
		expected.Path = r.Path
		expected.Open = r.Open
	case "open-node":
		expected.Node = r.Node
		expected.Open = r.Open
	case "stat-node":
		expected.Node = r.Node
	case "set-node-attr":
		expected.Node = r.Node
		expected.Change = r.Change
	case "stat", "sync", "close", "ack":
		expected.File = r.File
	case "read":
		expected.File = r.File
		expected.Offset = r.Offset
		expected.Length = r.Length
	case "write":
		expected.File = r.File
		expected.Offset = r.Offset
		expected.Data = r.Data
	case "truncate":
		expected.File = r.File
		expected.Offset = r.Offset
	case "set-attr":
		expected.File = r.File
		expected.Change = r.Change
	case "get-lock":
		expected.File = r.File
		expected.Owner = r.Owner
		expected.Lock = r.Lock
	case "set-lock":
		expected.File = r.File
		expected.Owner = r.Owner
		expected.Lock = r.Lock
		expected.LockID = r.LockID
	case "query-lock", "cancel-lock":
		expected.File = r.File
		expected.Owner = r.Owner
		expected.LockID = r.LockID
	case "drop-locks":
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
	case "stat", "sync", "close", "ack", "read", "write", "truncate", "set-attr", "get-lock", "set-lock", "query-lock", "cancel-lock", "drop-locks":
		if !validFileCapability(r.File) {
			return errors.New("file operation has no reference capability")
		}
	}
	if (r.Op == "set-attr" || r.Op == "set-node-attr") && r.Change == nil {
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
		case "new":
			expected.Session = r.Session
			expected.Status = r.Status
		case "status", "renew":
			expected.Status = r.Status
		case "open", "open-node":
			expected.File = r.File
			expected.Barrier = r.Barrier
		case "stat", "stat-node":
			expected.Attr = r.Attr
		case "read":
			expected.Attr = r.Attr
			expected.Data = r.Data
		case "write", "truncate", "set-attr", "set-node-attr":
			expected.Attr = r.Attr
			expected.Barrier = r.Barrier
		case "sync":
			expected.Barrier = r.Barrier
		case "get-lock":
			expected.Conflict = r.Conflict
		case "set-lock", "query-lock", "cancel-lock":
			expected.Attempt = r.Attempt
		case "ack", "close", "session-close", "drop-locks":
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
	case "new":
		if !validFileCapability(r.Session) || r.Status == nil {
			return errors.New("file session response has no valid capability or status")
		}
	case "status", "renew":
		if r.Status == nil {
			return errors.New("file response carries no status")
		}
	case "open", "open-node":
		if !validFileCapability(r.File) {
			return errors.New("file open response carries no reference capability")
		}
	case "read", "stat", "stat-node", "write", "truncate", "set-attr", "set-node-attr":
		if r.Attr == nil {
			return errors.New("file response carries no attributes")
		}
	case "get-lock":
		if r.Conflict == nil {
			return errors.New("file response carries no conflict result")
		}
	case "set-lock", "query-lock", "cancel-lock":
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
		if req.Op != "stat-node" && req.Op != "set-node-attr" && (r.Attr.Size < 0 || !r.Attr.Storage().Mode.IsRegular()) {
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
	if req.Op == "set-lock" && a.Lock != req.Lock {
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
