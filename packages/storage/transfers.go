package storage

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"syscall"
)

// FileTransfer requires native unpublished imports and immutable export pins
// throughout the backing chain. Repeated range reads and path writes cannot
// emulate this capability's single-version and atomic-publication guarantees.
type FileTransfer interface {
	CheckFileTransfer() error
	BeginImport(context.Context, ImportCommand) (FileImport, error)
	// ResumeImport returns a handle only for sealed or terminal durable work.
	// Unsealed nonterminal work returns EBUSY and retains its original stream
	// ownership; it cannot be resumed into a second receive handle.
	ResumeImport(context.Context, ActionKey) (FileImport, error)
	QueryImport(context.Context, ActionKey) (ImportReceipt, error)
	// CancelImport abandons an accepted action using its stable SubjectID and
	// current authorization, without inheriting a receive handle or AccessID.
	// A live receive owner prevents cancellation with EBUSY. Once that ownership
	// has settled, cancellation uses the native action and returns its exact
	// receipt; completed publication is preserved. Unknown ownership or cleanup
	// remains an error with retained responsibility, never proof of absence.
	CancelImport(context.Context, ActionKey) (ImportReceipt, error)
	ExportFile(context.Context, ExportCommand) (FileExport, error)
}

type ImportMode uint8

const (
	ImportCreate ImportMode = iota + 1
	ImportReplaceName
)

// ImportCommand fixes the selected parent, raw leaf, observed child and exact
// content before receiving bytes. ReplaceName publishes a new node; it never
// changes a previously opened object's identity or follows a symbolic link.
type ImportCommand struct {
	Action        ActionKey
	Fingerprint   ActionFingerprint
	Target        ChildSelection
	Expected      ChildCondition
	Mode          ImportMode
	DeclaredBytes int64
	Digest        [sha256.Size]byte
}

func (c ImportCommand) Check() error {
	if err := c.Action.Check(); err != nil {
		return err
	}
	if err := c.Target.Check(); err != nil {
		return err
	}
	if err := c.Expected.Check(); err != nil {
		return err
	}
	if c.DeclaredBytes < 0 ||
		(c.Mode != ImportCreate && c.Mode != ImportReplaceName) ||
		(c.Mode == ImportCreate && c.Expected.State != Absent) ||
		(c.Mode == ImportReplaceName && c.Expected.State != SameNode) ||
		c.Fingerprint != c.EffectFingerprint() {
		return syscall.EINVAL
	}
	return nil
}

// EffectFingerprint uses the fixed command encoding, including native scope,
// namespace guards and content seal. Durable actor identity is carried by Action
// and credential identity does not participate in this fingerprint.
func (c ImportCommand) EffectFingerprint() ActionFingerprint {
	c.Action = ActionKey{}
	c.Fingerprint = ActionFingerprint{}
	encoded, err := json.Marshal(c)
	if err != nil {
		panic("storage: import command contains an unsupported field")
	}
	h := sha256.New()
	_, _ = h.Write([]byte("remote-fs/import/v1\x00"))
	_, _ = h.Write(encoded)
	var fingerprint ActionFingerprint
	copy(fingerprint[:], h.Sum(nil))
	return fingerprint
}

// FileImport owns private staging and native reservation until publication or
// confirmed cancellation. Receive succeeds only for exactly DeclaredBytes whose
// digest matches the command; received bytes are never named or counted as files.
// Receive failure cannot publish. Sealed work may resume under the same stable
// SubjectID after restart; unsealed streams remain bound to their original access.
//
// Publish performs final native scope, child, sharing, lock and quota checks and
// commits binding, counters, log and action receipt together. Admission fixes the
// action; cancellation thereafter only ends waiting and may return Unknown.
// Cancel never rolls back a completed publication. Unknown cleanup keeps every
// reservation and pin charged until exact settlement is confirmed.
//
// Detach retires this volatile handle without abandoning sealed durable work.
// The same SubjectID may resume it after credential renewal or restart. Unsealed
// nonterminal work returns EBUSY and stays owned; Cancel explicitly abandons it.
// Released from Detach confirms only the volatile handle's settlement, leaving
// sealed staging and native reservations charged to their durable action owner.
// A detached handle cannot Receive or Publish; uncertain settlement remains owned
// and must be reconciled before another handle is admitted for that action.
type FileImport interface {
	Receive(context.Context, io.Reader) (ImportSeal, error)
	Publish(context.Context) (ImportReceipt, error)
	Cancel(context.Context) (ReferenceCloseResult, error)
	Detach(context.Context) (ReferenceCloseResult, error)
}

type ImportSeal struct {
	Bytes  int64
	Digest [sha256.Size]byte
}

// ImportReceipt is the native publication result, independently of HTTP stream
// completion. Attr is the newly published node's captured state only when the
// outcome is Completed; later namespace changes do not alter this receipt.
type ImportReceipt struct {
	Action      ActionKey
	Fingerprint ActionFingerprint
	Outcome     DurableActionOutcome
	Attr        Attr
	Failure     syscall.Errno
}

func (r ImportReceipt) Check() error {
	if err := r.Action.Check(); err != nil {
		return err
	}
	if r.Outcome < DurableActionPending || r.Outcome > DurableActionHistoryExpired {
		return syscall.EINVAL
	}
	if r.Outcome == DurableActionCompleted {
		if r.Attr.ID == 0 || r.Attr.Kind != NodeRegular || r.Attr.Size < 0 || r.Failure != 0 {
			return syscall.EINVAL
		}
	} else if r.Attr.ID != 0 || (r.Failure != 0 && r.Outcome != DurableActionNotExecuted) {
		return syscall.EINVAL
	}
	return nil
}

// ExportCommand selects one existing regular node without resolving a path. A
// supplied Scope must remain valid, grant ReadData and ReadMetadata, and cannot
// be ignored in favor of NodeID. The authority checks these grants because the
// export exposes both content and full captured attributes, including metadata;
// Check validates only the command's structure, not its native scope grants.
type ExportCommand struct {
	NodeID uint64
	Scope  *UseScope `json:",omitempty"`
}

func (c ExportCommand) Check() error {
	if c.NodeID == 0 {
		return syscall.EINVAL
	}
	if c.Scope != nil {
		return c.Scope.Check()
	}
	return nil
}

// ExportInfo describes the captured node. DigestAvailable distinguishes a known
// SHA-256 from an unavailable digest; zero bytes do not represent unknown data.
// A backend may establish the digest during WriteTo. After successful verified
// copying, Info must expose that complete content's digest with the flag set.
type ExportInfo struct {
	Attr            Attr
	Digest          [sha256.Size]byte
	DigestAvailable bool
}

// FileExport retains both the exact node and captured immutable object key.
// WriteTo verifies that complete captured content while copying with bounded
// memory; consumers write into private staging before publishing success or
// exposing bytes. Concurrent writes, rename, unlink and GC cannot change it.
// A failed copy invalidates all produced output. Callers arrange interruption of
// blocking I/O when the context ends. CloseWithResult releases pins only after
// confirmed native settlement; failed/unknown cleanup remains owned and charged.
type FileExport interface {
	Info() ExportInfo
	WriteTo(context.Context, io.Writer) error
	CloseWithResult(context.Context) (ReferenceCloseResult, error)
}

// CopyTransferContent copies an exact-length SHA-256-sealed stream into private
// staging with a 64-KiB memory bound. Any error invalidates the complete output;
// staging ownership and cleanup remain with the caller. Context cancellation is
// checked between I/O calls, so the caller must arrange cancellation of blocking
// reads and writes. A size mismatch is EINVAL/EFBIG and digest mismatch is EIO.
func CopyTransferContent(ctx context.Context, dst io.Writer, src io.Reader, declaredBytes int64, digest [sha256.Size]byte) (ImportSeal, error) {
	if declaredBytes < 0 || dst == nil || src == nil {
		return ImportSeal{}, syscall.EINVAL
	}
	if err := ctx.Err(); err != nil {
		return ImportSeal{}, err
	}
	hash := sha256.New()
	buffer := make([]byte, 64<<10)
	remaining := declaredBytes
	emptyReads := 0
	eof := false
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return ImportSeal{}, err
		}
		count := int64(len(buffer))
		if remaining < count {
			count = remaining
		}
		n, readErr := src.Read(buffer[:count])
		if n < 0 || int64(n) > count {
			return ImportSeal{}, fmt.Errorf("transfer reader returned an invalid byte count: %w", syscall.EIO)
		}
		if n > 0 {
			if err := ctx.Err(); err != nil {
				return ImportSeal{}, err
			}
			written, writeErr := dst.Write(buffer[:n])
			if writeErr != nil {
				return ImportSeal{}, writeErr
			}
			if written != n {
				return ImportSeal{}, io.ErrShortWrite
			}
			_, _ = hash.Write(buffer[:n])
			remaining -= int64(n)
			emptyReads = 0
		} else if readErr == nil {
			emptyReads++
			if emptyReads == 100 {
				return ImportSeal{}, io.ErrNoProgress
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) {
				return ImportSeal{}, readErr
			}
			if remaining != 0 {
				return ImportSeal{}, fmt.Errorf("transfer ended before its declared length: %w", syscall.EINVAL)
			}
			eof = true
		}
	}
	for !eof {
		if err := ctx.Err(); err != nil {
			return ImportSeal{}, err
		}
		n, err := src.Read(buffer[:1])
		if n < 0 || n > 1 {
			return ImportSeal{}, fmt.Errorf("transfer reader returned an invalid byte count: %w", syscall.EIO)
		}
		if n != 0 {
			return ImportSeal{}, fmt.Errorf("transfer exceeds its declared length: %w", syscall.EFBIG)
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return ImportSeal{}, err
			}
			eof = true
		} else {
			emptyReads++
			if emptyReads == 100 {
				return ImportSeal{}, io.ErrNoProgress
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return ImportSeal{}, err
	}
	var actual [sha256.Size]byte
	copy(actual[:], hash.Sum(nil))
	if actual != digest {
		return ImportSeal{}, fmt.Errorf("transfer digest does not match its content seal: %w", syscall.EIO)
	}
	return ImportSeal{Bytes: declaredBytes, Digest: actual}, nil
}
