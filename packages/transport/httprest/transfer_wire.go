package httprest

import (
	"context"
	"errors"
	"path/filepath"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// TransferOptions configures native imports and verified exports. Native
// import staging belongs to the backing chain; verified exports use SpoolDir.
type TransferOptions struct {
	Enabled       bool
	SpoolDir      string
	MaxBytes      int64
	MaxConcurrent int
	Lifetime      time.Duration
	// TerminalResults and TerminalLifetime bound exact cleanup replay. Zero
	// selects eight results per concurrent transfer and at most two minutes.
	TerminalResults  int
	TerminalLifetime time.Duration
	ActionNamespace  func(context.Context, storage.ActionKey) error
}

func (o TransferOptions) Check() error {
	if !o.Enabled {
		if o.SpoolDir != "" || o.MaxBytes != 0 || o.MaxConcurrent != 0 || o.Lifetime != 0 || o.TerminalResults != 0 || o.TerminalLifetime != 0 || o.ActionNamespace != nil {
			return errors.New("httprest: disabled transfers cannot contain transfer settings")
		}
		return nil
	}
	if !filepath.IsAbs(o.SpoolDir) || o.MaxBytes <= 0 || o.MaxConcurrent <= 0 || o.MaxConcurrent > 65536 || o.Lifetime <= 0 || o.ActionNamespace == nil {
		return errors.New("httprest: transfers require an absolute private spool directory, finite limits and a namespace admission hook")
	}
	if o.TerminalResults < 0 || o.TerminalResults > 1<<20 || o.TerminalLifetime < 0 || o.TerminalLifetime > 24*time.Hour {
		return errors.New("httprest: transfer terminal history exceeds its count or lifetime bound")
	}
	return nil
}

type transferRequest struct {
	Op      string                 `json:"op"`
	Command *storage.ImportCommand `json:"command,omitempty"`
	Action  *storage.ActionKey     `json:"action,omitempty"`
	Export  *storage.ExportCommand `json:"export,omitempty"`
	Token   string                 `json:"token,omitempty"`
}

type transferCapabilities struct {
	MaxBytes    int64 `json:"max_bytes"`
	Replication bool  `json:"replication"`
}

type transferReceipt struct {
	Action      storage.ActionKey            `json:"action"`
	Fingerprint storage.ActionFingerprint    `json:"fingerprint"`
	Outcome     storage.DurableActionOutcome `json:"outcome"`
	Attr        *Attr                        `json:"attr,omitempty"`
	Failure     string                       `json:"failure,omitempty"`
}

type transferExportInfo struct {
	Attr   *Attr  `json:"attr"`
	Digest []byte `json:"digest"`
}

type transferCloseSettlement struct {
	State    storage.CloseSettlementState `json:"state"`
	Semantic *ErrorResponse               `json:"semantic,omitempty"`
	Cause    *ErrorResponse               `json:"cause"`
}

type transferResponse struct {
	Capabilities  *transferCapabilities    `json:"capabilities,omitempty"`
	Token         string                   `json:"token,omitempty"`
	Seal          *storage.ImportSeal      `json:"seal,omitempty"`
	Receipt       *transferReceipt         `json:"receipt,omitempty"`
	Info          *transferExportInfo      `json:"info,omitempty"`
	DeclaredBytes *int64                   `json:"declared_bytes,omitempty"`
	Close         *referenceCloseResult    `json:"close,omitempty"`
	Barrier       *MutationBarrier         `json:"barrier,omitempty"`
	Error         *ErrorResponse           `json:"error,omitempty"`
	Settlement    *transferCloseSettlement `json:"settlement,omitempty"`
}

func transferReceiptOf(receipt storage.ImportReceipt) *transferReceipt {
	result := &transferReceipt{Action: receipt.Action, Fingerprint: receipt.Fingerprint, Outcome: receipt.Outcome}
	if receipt.Outcome == storage.DurableActionCompleted {
		result.Attr = AttrOf(receipt.Attr)
	}
	if receipt.Failure != 0 {
		result.Failure = storage.ErrnoNameOf(receipt.Failure)
	}
	return result
}
