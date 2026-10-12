package metastore

import (
	"context"
	"github.com/codetreker/remote-fs/packages/storage"
)

// NativeFileTransfer publishes prepared immutable objects under the same
// authority transaction as their namespace binding and action receipt.
type NativeFileTransfer interface {
	CheckNativeFileTransfer() error
	ResumeImportReservation(context.Context, storage.ActionKey) (ImportReservation, error)
	PrepareImport(context.Context, storage.ImportCommand) (ImportReservation, error)
	SealImport(context.Context, storage.ActionKey, Object) error
	PublishImport(context.Context, storage.ActionKey) (storage.ImportReceipt, error)
	QueryImportAction(context.Context, storage.ActionKey) (storage.ImportReceipt, error)
	AbandonImport(context.Context, storage.ActionKey) error
	CaptureExport(context.Context, storage.ExportCommand) (ExportCapture, error)
}

type ImportReservation struct {
	Binding storage.AccessBinding
	Key     Key
	Command storage.ImportCommand
	Sealed  bool
	Receipt storage.ImportReceipt
}

type ExportCapture struct {
	State           FileState
	Key             Key
	Digest          [32]byte
	DigestAvailable bool
	Reference       ExportReference
}

// ExportReference retains both the captured node and its exact immutable key.
// An unsettled close keeps both responsibilities owned for retry.
type ExportReference interface {
	CloseWithResult(context.Context) (storage.ReferenceCloseResult, error)
}
