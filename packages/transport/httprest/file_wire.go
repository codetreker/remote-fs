package httprest

import (
	"context"
	"fmt"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
	"time"
)

const OpFile Op = "file"
const OpFileControl Op = "file-control"

func fileControl(op string) bool {
	switch op {
	case "status", "renew", "session-close", "close", "ack", "get-lock", "set-lock", "query-lock", "cancel-lock", "drop-locks":
		return true
	}
	return false
}

type fileRequest struct {
	Op      string                     `json:"op"`
	Session string                     `json:"session"`
	File    string                     `json:"file"`
	Action  storage.LockRequestID      `json:"action"`
	Path    []byte                     `json:"path"`
	Node    uint64                     `json:"node"`
	Options storage.FileSessionOptions `json:"options"`
	Open    storage.FileOpenOptions    `json:"open"`
	Offset  int64                      `json:"offset"`
	Length  int                        `json:"length"`
	Data    []byte                     `json:"data"`
	Change  *AttrChange                `json:"change,omitempty"`
	Owner   storage.LockOwner          `json:"owner"`
	Lock    storage.FileLock           `json:"lock"`
	LockID  storage.LockRequestID      `json:"lockId"`
	Family  storage.LockFamily         `json:"family"`
}

type fileResponse struct {
	Session  string                     `json:"session,omitempty"`
	File     string                     `json:"file,omitempty"`
	Retry    bool                       `json:"retry,omitempty"`
	Epoch    uint64                     `json:"epoch"`
	Status   *storage.FileSessionStatus `json:"status,omitempty"`
	Attr     *Attr                      `json:"attr,omitempty"`
	Data     []byte                     `json:"data"`
	Conflict *storage.LockConflict      `json:"conflict,omitempty"`
	Attempt  *fileLockAttempt           `json:"attempt,omitempty"`
	Barrier  *MutationBarrier           `json:"barrier,omitempty"`
}

func fileMutation(op string) bool {
	switch op {
	case "open", "open-node", "set-node-attr", "write", "truncate", "set-attr", "sync":
		return true
	}
	return false
}

func fileActionRequired(op string) bool {
	switch op {
	case "open", "open-node", "set-node-attr", "write", "truncate", "set-attr", "sync", "drop-locks":
		return true
	}
	return false
}

// FileWithBarrier exposes authority progress without requiring a directory entry
// for detached files. A nil barrier means the namespace has no change log.
type FileWithBarrier interface {
	storage.File
	WriteAtWithBarrier(context.Context, int64, []byte) (storage.Attr, *MutationBarrier, error)
	TruncateWithBarrier(context.Context, int64) (storage.Attr, *MutationBarrier, error)
	SetAttrWithBarrier(context.Context, storage.AttrChange) (storage.Attr, *MutationBarrier, error)
}

type FileSessionWithBarrier interface {
	storage.FileSession
	OpenFileWithBarrier(context.Context, string, storage.FileOpenOptions) (storage.File, *MutationBarrier, error)
	OpenNodeWithBarrier(context.Context, uint64, storage.FileOpenOptions) (storage.File, *MutationBarrier, error)
	SetNodeAttrWithBarrier(context.Context, uint64, storage.AttrChange) (storage.Attr, *MutationBarrier, error)
}

type fileLockAttempt struct {
	Request          storage.LockRequestID
	State            storage.LockAttemptState
	Lock             storage.FileLock
	Conflict         storage.LockConflict
	Errno            string
	EverGranted      bool
	HistoryRemaining time.Duration
}

func fileAttemptOf(a storage.LockAttempt) (*fileLockAttempt, error) {
	name := ""
	if a.Errno != 0 {
		var ok bool
		name, ok = storage.ErrnoName(a.Errno)
		if !ok {
			return nil, fmt.Errorf("advisory action returned an unnameable error: %w", syscall.EIO)
		}
	}
	return &fileLockAttempt{Request: a.Request, State: a.State, Lock: a.Lock, Conflict: a.Conflict, Errno: name, EverGranted: a.EverGranted, HistoryRemaining: a.HistoryRemaining}, nil
}

func (a fileLockAttempt) storage() (storage.LockAttempt, error) {
	var errno syscall.Errno
	if a.Errno != "" {
		var ok bool
		errno, ok = storage.ErrnoByName(a.Errno)
		if !ok {
			return storage.LockAttempt{}, fmt.Errorf("advisory action carries unknown errno %q", a.Errno)
		}
	}
	return storage.LockAttempt{Request: a.Request, State: a.State, Lock: a.Lock, Conflict: a.Conflict, Errno: errno, EverGranted: a.EverGranted, HistoryRemaining: a.HistoryRemaining}, nil
}
