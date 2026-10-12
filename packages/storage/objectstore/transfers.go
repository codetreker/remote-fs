package objectstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

const maxRetainedTransfers = 1024

var _ storage.FileTransfer = (*Storage)(nil)

func (s *Storage) CheckFileTransfer() error {
	objects, ok := s.objects.(StreamingObjects)
	if !ok {
		return fmt.Errorf("object backend has no bounded stream support: %w", syscall.EOPNOTSUPP)
	}
	if err := objects.CheckStreamingObjects(); err != nil {
		return err
	}
	native, ok := s.meta.(metastore.NativeFileTransfer)
	if !ok {
		return fmt.Errorf("metastore has no native file transfer: %w", syscall.EOPNOTSUPP)
	}
	return native.CheckNativeFileTransfer()
}

func (s *Storage) retainTransfer(transfer any) error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closeDone != nil || s.transfersClosing {
		return syscall.EIO
	}
	s.transferMu.Lock()
	defer s.transferMu.Unlock()
	if file, ok := transfer.(*fileImport); ok {
		if _, busy := s.importCancels[file.action]; busy {
			return syscall.EBUSY
		}
		if s.importActionRetiringLocked(file.action) {
			return syscall.EBUSY
		}
	}
	if len(s.transfers)+len(s.importCancels) >= maxRetainedTransfers {
		return syscall.EAGAIN
	}
	if s.transfers == nil {
		s.transfers = make(map[any]struct{})
	}
	s.transfers[transfer] = struct{}{}
	return nil
}

func (s *Storage) releaseTransfer(transfer any) {
	s.transferMu.Lock()
	delete(s.transfers, transfer)
	s.transferMu.Unlock()
}

func (s *Storage) checkTransferSettlement() error {
	s.transferMu.Lock()
	defer s.transferMu.Unlock()
	if len(s.transfers)+len(s.importCancels) != 0 {
		return fmt.Errorf("%d file transfers retain volume ownership: %w", len(s.transfers)+len(s.importCancels), syscall.EBUSY)
	}
	return nil
}

// LiveTransferCount includes retained import and export handles, uncertain
// cancellation owners, and action retirement calls that exclude new imports.
func (s *Storage) LiveTransferCount() int {
	s.transferMu.Lock()
	defer s.transferMu.Unlock()
	return len(s.transfers) + len(s.importCancels)
}

func transferSubject(ctx context.Context, key storage.ActionKey) error {
	if binding, present := storage.AccessBindingFrom(ctx); present && binding.SubjectID != key.SubjectID {
		return syscall.EACCES
	}
	return nil
}

func (s *Storage) BeginImport(ctx context.Context, command storage.ImportCommand) (storage.FileImport, error) {
	if err := s.CheckFileTransfer(); err != nil {
		return nil, err
	}
	if err := command.Check(); err != nil {
		return nil, err
	}
	if err := transferSubject(ctx, command.Action); err != nil {
		return nil, err
	}
	if err := s.beginOperation(); err != nil {
		return nil, err
	}
	defer s.endOperation()
	file := &fileImport{storage: s, action: command.Action}
	if err := s.retainTransfer(file); err != nil {
		return nil, err
	}
	reservation, err := s.meta.(metastore.NativeFileTransfer).PrepareImport(ctx, command)
	if err != nil {
		s.releaseTransfer(file)
		return nil, err
	}
	if !reservation.Sealed && !reservation.Receipt.Outcome.Terminal() {
		if err := storage.RequireAccessBinding(ctx, reservation.Binding); err != nil {
			s.releaseTransfer(file)
			return nil, err
		}
	}
	file.reservation = reservation
	file.binding, _ = storage.AccessBindingFrom(ctx)
	if reservation.Receipt.Outcome.Terminal() {
		file.closed = true
		s.releaseTransfer(file)
	}
	return file, nil
}

func (s *Storage) ResumeImport(ctx context.Context, key storage.ActionKey) (storage.FileImport, error) {
	if err := s.CheckFileTransfer(); err != nil {
		return nil, err
	}
	if err := key.Check(); err != nil {
		return nil, err
	}
	if err := transferSubject(ctx, key); err != nil {
		return nil, err
	}
	if err := s.beginOperation(); err != nil {
		return nil, err
	}
	defer s.endOperation()
	file := &fileImport{storage: s, action: key}
	if err := s.retainTransfer(file); err != nil {
		return nil, err
	}
	reservation, err := s.meta.(metastore.NativeFileTransfer).ResumeImportReservation(ctx, key)
	if err != nil {
		s.releaseTransfer(file)
		return nil, err
	}
	if !reservation.Sealed && !reservation.Receipt.Outcome.Terminal() {
		if err := storage.RequireAccessBinding(ctx, reservation.Binding); err != nil {
			s.releaseTransfer(file)
			return nil, err
		}
	}
	if !reservation.Sealed && !reservation.Receipt.Outcome.Terminal() {
		s.releaseTransfer(file)
		return nil, syscall.EBUSY
	}
	file.reservation = reservation
	file.binding, _ = storage.AccessBindingFrom(ctx)
	if reservation.Receipt.Outcome.Terminal() {
		file.closed = true
		s.releaseTransfer(file)
	}
	return file, nil
}

func (s *Storage) QueryImport(ctx context.Context, key storage.ActionKey) (storage.ImportReceipt, error) {
	if err := s.CheckFileTransfer(); err != nil {
		return storage.ImportReceipt{}, err
	}
	if err := key.Check(); err != nil {
		return storage.ImportReceipt{}, err
	}
	if err := transferSubject(ctx, key); err != nil {
		return storage.ImportReceipt{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.ImportReceipt{}, err
	}
	defer s.endOperation()
	return s.meta.(metastore.NativeFileTransfer).QueryImportAction(ctx, key)
}

type fileImport struct {
	mu          sync.Mutex
	storage     *Storage
	reservation metastore.ImportReservation
	binding     storage.AccessBinding
	closed      bool
	detached    bool
	action      storage.ActionKey
}

func (f *fileImport) seal() storage.ImportSeal {
	return storage.ImportSeal{Bytes: f.reservation.Command.DeclaredBytes, Digest: f.reservation.Command.Digest}
}

func (f *fileImport) Receive(ctx context.Context, source io.Reader) (storage.ImportSeal, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := storage.RequireAccessBinding(ctx, f.binding); err != nil {
		return storage.ImportSeal{}, err
	}
	if f.detached {
		return storage.ImportSeal{}, syscall.ESTALE
	}
	if f.reservation.Receipt.Outcome.Terminal() {
		return storage.ImportSeal{}, syscall.EALREADY
	}
	if f.closed {
		return storage.ImportSeal{}, syscall.ESTALE
	}
	if err := f.storage.beginOperation(); err != nil {
		return storage.ImportSeal{}, err
	}
	defer f.storage.endOperation()
	if f.reservation.Sealed {
		return f.seal(), nil
	}

	command := f.reservation.Command
	objects := f.storage.objects.(StreamingObjects)
	// A crash after durable Put but before Seal can leave the reserved key already
	// present. Reverification proves the persisted command before recording its seal.
	seal, err := objects.GetStream(ctx, string(f.reservation.Key), io.Discard)
	if err != nil && !errors.Is(err, syscall.ENOENT) {
		return storage.ImportSeal{}, err
	}
	if err == nil {
		if seal.Size != command.DeclaredBytes || seal.Digest != command.Digest {
			return storage.ImportSeal{}, syscall.EIO
		}
	} else if err := objects.PutStream(ctx, string(f.reservation.Key), command.DeclaredBytes, command.Digest, source); err != nil {
		return storage.ImportSeal{}, err
	}
	object := metastore.Object{Key: f.reservation.Key, Size: command.DeclaredBytes, Digest: append([]byte(nil), command.Digest[:]...), ModTime: f.storage.now()}
	if err := f.storage.meta.(metastore.NativeFileTransfer).SealImport(ctx, command.Action, object); err != nil {
		return storage.ImportSeal{}, err
	}
	f.reservation.Sealed = true
	return f.seal(), nil
}

func (f *fileImport) Publish(ctx context.Context) (storage.ImportReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := storage.RequireAccessBinding(ctx, f.binding); err != nil {
		return storage.ImportReceipt{}, err
	}
	if f.detached {
		return storage.ImportReceipt{}, syscall.ESTALE
	}
	if f.reservation.Receipt.Outcome.Terminal() {
		if f.reservation.Receipt.Failure != 0 {
			return f.reservation.Receipt, f.reservation.Receipt.Failure
		}
		return f.reservation.Receipt, nil
	}
	if f.closed {
		return storage.ImportReceipt{}, syscall.ESTALE
	}
	if !f.reservation.Sealed {
		return storage.ImportReceipt{}, syscall.EINVAL
	}
	if err := f.storage.beginOperation(); err != nil {
		return storage.ImportReceipt{}, err
	}
	defer f.storage.endOperation()
	receipt, err := f.storage.meta.(metastore.NativeFileTransfer).PublishImport(ctx, f.action)
	if receipt.Outcome.Terminal() {
		f.reservation.Receipt = receipt
		f.closed = true
		f.storage.releaseTransfer(f)
		f.storage.sweepAfterMutation()
	}
	return receipt, err
}

func (f *fileImport) Detach(ctx context.Context) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := storage.RequireAccessBinding(ctx, f.binding); err != nil {
		return storage.ReferenceCloseResult{Determined: true}, err
	}
	if err := ctx.Err(); err != nil {
		return storage.ReferenceCloseResult{Determined: true}, err
	}
	if f.closed {
		f.detached = true
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	if err := f.storage.beginOperation(); err != nil {
		return storage.ReferenceCloseResult{Determined: true}, err
	}
	defer f.storage.endOperation()
	// Another handle may have sealed the same durable action, or the seal
	// transaction may have committed while its response failed.
	current, err := f.storage.meta.(metastore.NativeFileTransfer).ResumeImportReservation(ctx, f.action)
	if err != nil {
		return storage.ReferenceCloseResult{Determined: true}, err
	}
	if !current.Sealed && !current.Receipt.Outcome.Terminal() {
		return storage.ReferenceCloseResult{Determined: true}, syscall.EBUSY
	}
	f.reservation = current
	f.closed = true
	f.detached = true
	f.storage.releaseTransfer(f)
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}

func (f *fileImport) Cancel(ctx context.Context) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := storage.RequireAccessBinding(ctx, f.binding); err != nil {
		return storage.ReferenceCloseResult{Determined: true}, err
	}
	if f.detached {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, syscall.ESTALE
	}
	if f.closed {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
	}
	if err := f.storage.beginOperation(); err != nil {
		return storage.ReferenceCloseResult{Determined: true}, err
	}
	defer f.storage.endOperation()
	if err := f.storage.meta.(metastore.NativeFileTransfer).AbandonImport(ctx, f.action); err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	f.closed = true
	f.storage.releaseTransfer(f)
	f.storage.sweepAfterMutation()
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}

func (s *Storage) ExportFile(ctx context.Context, command storage.ExportCommand) (storage.FileExport, error) {
	if err := s.CheckFileTransfer(); err != nil {
		return nil, err
	}
	if err := command.Check(); err != nil {
		return nil, err
	}
	if err := s.beginOperation(); err != nil {
		return nil, err
	}
	defer s.endOperation()
	file := &fileExport{storage: s}
	if err := s.retainTransfer(file); err != nil {
		return nil, err
	}
	capture, err := s.meta.(metastore.NativeFileTransfer).CaptureExport(ctx, command)
	if err != nil {
		s.releaseTransfer(file)
		return nil, err
	}
	if capture.Reference == nil {
		s.releaseTransfer(file)
		return nil, fmt.Errorf("native export did not retain a key pin: %w", syscall.EIO)
	}
	file.capture = capture
	file.binding, _ = storage.AccessBindingFrom(ctx)
	return file, nil
}

type fileExport struct {
	mu       sync.Mutex
	storage  *Storage
	capture  metastore.ExportCapture
	binding  storage.AccessBinding
	closed   bool
	closeErr error
}

func (f *fileExport) Info() storage.ExportInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return storage.ExportInfo{Attr: f.capture.State.Attr(), Digest: f.capture.Digest, DigestAvailable: f.capture.DigestAvailable}
}

func (f *fileExport) WriteTo(ctx context.Context, destination io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := storage.RequireAccessBinding(ctx, f.binding); err != nil {
		return err
	}
	if f.closed {
		return syscall.ESTALE
	}
	if destination == nil {
		return syscall.EINVAL
	}
	if err := f.storage.beginOperation(); err != nil {
		return err
	}
	defer f.storage.endOperation()
	if f.capture.Key == "" {
		if f.capture.State.Size != 0 {
			return syscall.EIO
		}
		f.capture.Digest = sha256.Sum256(nil)
		f.capture.DigestAvailable = true
		return nil
	}
	seal, err := f.storage.objects.(StreamingObjects).GetStream(ctx, string(f.capture.Key), destination)
	if err != nil {
		return err
	}
	if seal.Size != f.capture.State.Size {
		return fmt.Errorf("captured object length changed: %w", syscall.EIO)
	}
	// Existing []byte backends may have persisted no independent object digest;
	// the local envelope still verifies its own SHA-256 before success.
	if f.capture.DigestAvailable && seal.Digest != f.capture.Digest {
		return fmt.Errorf("captured object checksum changed: %w", syscall.EIO)
	}
	f.capture.Digest = seal.Digest
	f.capture.DigestAvailable = true
	return nil
}

func (f *fileExport) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := storage.RequireAccessBinding(ctx, f.binding); err != nil {
		return storage.ReferenceCloseResult{Determined: true}, err
	}
	if f.closed {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, f.closeErr
	}
	if err := f.storage.beginOperation(); err != nil {
		return storage.ReferenceCloseResult{Determined: true}, err
	}
	defer f.storage.endOperation()
	result, err := f.capture.Reference.CloseWithResult(ctx)
	var settlement *storage.CloseSettlementError
	if result.Released && result.Determined && !errors.As(err, &settlement) {
		f.closeErr = err
		f.closed = true
		f.storage.releaseTransfer(f)
		f.storage.sweepAfterMutation()
	}
	return result, err
}

// FenceTransferAdmission refuses new retained handles only after all existing
// transfer responsibilities have settled. A busy refusal keeps admission open.
func (s *Storage) FenceTransferAdmission() error {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if err := s.checkTransferSettlement(); err != nil {
		return err
	}
	s.transfersClosing = true
	return nil
}

type importCancelGate struct{ running bool }

func (s *Storage) beginImportCancel(key storage.ActionKey) (*importCancelGate, bool, error) {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closeDone != nil || s.transfersClosing {
		return nil, false, syscall.EIO
	}
	s.transferMu.Lock()
	defer s.transferMu.Unlock()
	if s.importActionRetiringLocked(key) {
		return nil, false, syscall.EBUSY
	}
	for transfer := range s.transfers {
		if file, ok := transfer.(*fileImport); ok && file.action == key {
			return nil, false, syscall.EBUSY
		}
	}
	if gate := s.importCancels[key]; gate != nil {
		if gate.running {
			return nil, false, syscall.EBUSY
		}
		gate.running = true
		return gate, true, nil
	}
	if len(s.transfers)+len(s.importCancels) >= maxRetainedTransfers {
		return nil, false, syscall.EAGAIN
	}
	if s.importCancels == nil {
		s.importCancels = make(map[storage.ActionKey]*importCancelGate)
	}
	gate := &importCancelGate{running: true}
	s.importCancels[key] = gate
	return gate, false, nil
}

func (s *Storage) finishImportCancel(key storage.ActionKey, gate *importCancelGate, settled bool) {
	s.transferMu.Lock()
	defer s.transferMu.Unlock()
	if s.importCancels[key] != gate {
		panic("objectstore: import cancellation ownership changed")
	}
	if settled {
		delete(s.importCancels, key)
	} else {
		gate.running = false
	}
}

func importReceiptFailureOnly(receipt storage.ImportReceipt, err error) bool {
	// An error classification can conceal additional I/O causes and does not
	// establish cleanup settlement.
	return receipt.Outcome.Terminal() && receipt.Failure != 0 && onlyUnwrappedLeaves(err, receipt.Failure)
}

// CancelImport settles accepted work without inheriting its former access context.
// Live handles retain their receive ownership; uncertain cancellation keeps the
// action gate until an exact native receipt establishes settlement.
func (s *Storage) CancelImport(ctx context.Context, key storage.ActionKey) (storage.ImportReceipt, error) {
	if err := s.CheckFileTransfer(); err != nil {
		return storage.ImportReceipt{}, err
	}
	if err := key.Check(); err != nil {
		return storage.ImportReceipt{}, err
	}
	if err := transferSubject(ctx, key); err != nil {
		return storage.ImportReceipt{}, err
	}
	if err := s.beginOperation(); err != nil {
		return storage.ImportReceipt{}, err
	}
	defer s.endOperation()
	gate, recovering, err := s.beginImportCancel(key)
	if err != nil {
		return storage.ImportReceipt{}, err
	}
	settled := !recovering
	defer func() { s.finishImportCancel(key, gate, settled) }()
	native := s.meta.(metastore.NativeFileTransfer)
	receipt, err := native.QueryImportAction(ctx, key)
	if err != nil && !importReceiptFailureOnly(receipt, err) {
		return receipt, err
	}
	if receipt.Outcome.Terminal() {
		if checkErr := receipt.Check(); checkErr != nil || receipt.Action != key {
			return storage.ImportReceipt{}, fmt.Errorf("invalid cancellation receipt: %w", syscall.EIO)
		}
		settled = true
		return receipt, nil
	}
	if err != nil {
		return receipt, err
	}
	if receipt.Outcome != storage.DurableActionPending || receipt.Action != key || receipt.Check() != nil {
		return storage.ImportReceipt{}, fmt.Errorf("native import ownership is unknown: %w", syscall.EIO)
	}
	settled = false
	if err := native.AbandonImport(ctx, key); err != nil {
		return storage.ImportReceipt{Action: key, Fingerprint: receipt.Fingerprint, Outcome: storage.DurableActionUnknown}, err
	}
	receipt, err = native.QueryImportAction(ctx, key)
	if err != nil && !importReceiptFailureOnly(receipt, err) {
		return receipt, err
	}
	if !receipt.Outcome.Terminal() {
		if err == nil {
			err = fmt.Errorf("import cancellation settlement is unknown: %w", syscall.EIO)
		}
		return receipt, err
	}
	if receipt.Action != key || receipt.Check() != nil {
		return storage.ImportReceipt{}, fmt.Errorf("invalid cancellation receipt: %w", syscall.EIO)
	}
	settled = true
	s.sweepAfterMutation()
	return receipt, nil
}
