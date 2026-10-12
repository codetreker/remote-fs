package httprest

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"reflect"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.FileTransfer = (*Storage)(nil)

func (s *Storage) CheckFileTransfer() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.silence)
	defer cancel()
	_, err := s.transferCapabilities(ctx)
	return err
}

func (s *Storage) transferCapabilities(ctx context.Context) (transferCapabilities, error) {
	response, err := s.transferCall(ctx, transferRequest{Op: "check"})
	if err != nil {
		return transferCapabilities{}, err
	}
	return *response.Capabilities, nil
}

func (s *Storage) BeginImport(ctx context.Context, command storage.ImportCommand) (storage.FileImport, error) {
	if err := command.Check(); err != nil {
		return nil, err
	}
	capabilities, err := s.transferCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if command.DeclaredBytes > capabilities.MaxBytes {
		return nil, syscall.EFBIG
	}
	response, err := s.transferCall(ctx, transferRequest{Op: "begin", Command: &command})
	if err != nil {
		return nil, err
	}
	if response.DeclaredBytes == nil || *response.DeclaredBytes != command.DeclaredBytes {
		return nil, unreachable(Request{Op: OpFileTransfer}, errors.New("import admission changed its declared length"))
	}
	return &remoteImport{storage: s, token: response.Token, action: command.Action, command: &command, replication: capabilities.Replication}, nil
}

func (s *Storage) ResumeImport(ctx context.Context, action storage.ActionKey) (storage.FileImport, error) {
	if err := action.Check(); err != nil {
		return nil, err
	}
	capabilities, err := s.transferCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	response, err := s.transferCall(ctx, transferRequest{Op: "resume", Action: &action})
	if err != nil {
		return nil, err
	}
	return &remoteImport{storage: s, token: response.Token, action: action, replication: capabilities.Replication}, nil
}

func (s *Storage) QueryImport(ctx context.Context, action storage.ActionKey) (storage.ImportReceipt, error) {
	return s.importAction(ctx, "query", action)
}

func (s *Storage) CancelImport(ctx context.Context, action storage.ActionKey) (storage.ImportReceipt, error) {
	return s.importAction(ctx, "cancel-action", action)
}

func (s *Storage) importAction(ctx context.Context, op string, action storage.ActionKey) (storage.ImportReceipt, error) {
	if err := action.Check(); err != nil {
		return storage.ImportReceipt{}, err
	}
	response, err := s.transferCall(ctx, transferRequest{Op: op, Action: &action})
	if response.Receipt == nil {
		return storage.ImportReceipt{}, err
	}
	receipt, receiptErr := transferReceiptStorage(response.Receipt)
	if receiptErr != nil || receipt.Action != action {
		return storage.ImportReceipt{}, unreachable(Request{Op: OpFileTransferControl}, errors.New("import action returned an invalid or unrelated receipt"))
	}
	return receipt, err
}

func (s *Storage) ExportFile(ctx context.Context, command storage.ExportCommand) (storage.FileExport, error) {
	if err := command.Check(); err != nil {
		return nil, err
	}
	response, err := s.transferCall(ctx, transferRequest{Op: "export", Export: &command})
	if err != nil {
		return nil, err
	}
	if response.Info.Attr.ID != command.NodeID {
		return nil, unreachable(Request{Op: OpFileTransfer}, errors.New("export captured a different node"))
	}
	info := storage.ExportInfo{Attr: response.Info.Attr.Storage(), DigestAvailable: true}
	copy(info.Digest[:], response.Info.Digest)
	return &remoteExport{storage: s, token: response.Token, info: info}, nil
}

func (s *Storage) transferCall(ctx context.Context, request transferRequest) (transferResponse, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return transferResponse{}, err
	}
	op := OpFileTransferControl
	if request.Op == "begin" || request.Op == "export" {
		op = OpFileTransfer
	}
	answer, err := s.call(ctx, Request{Op: op}, encoded)
	if err != nil {
		return transferResponse{}, err
	}
	defer answer.release()
	response, err := decodeTransferResponse(answer.content, request.Op)
	if err != nil || response.Error == nil {
		return response, err
	}
	if response.Settlement != nil {
		settlement := &storage.CloseSettlementError{
			State:       response.Settlement.State,
			SemanticErr: s.transferError(response.Settlement.Semantic),
			Cause:       s.transferError(response.Settlement.Cause),
		}
		if err := settlement.Check(); err != nil {
			return transferResponse{}, unreachable(Request{Op: OpFileTransfer}, err)
		}
		return response, settlement
	}
	return response, s.transferError(response.Error)
}

func (s *Storage) transferError(failure *ErrorResponse) error {
	if failure == nil {
		return nil
	}
	encoded, err := json.Marshal(failure)
	if err != nil {
		return unreachable(Request{Op: OpFileTransfer}, err)
	}
	return s.storageError(Request{Op: OpFileTransfer}, encoded)
}

func decodeTransferResponse(content []byte, op string) (transferResponse, error) {
	var response transferResponse
	if err := decodeFileJSON(content, &response); err != nil {
		return transferResponse{}, unreachable(Request{Op: OpFileTransfer}, err)
	}
	if err := validateTransferResponse(response, op); err != nil {
		return transferResponse{}, unreachable(Request{Op: OpFileTransfer}, err)
	}
	return response, nil
}

func validateTransferResponse(response transferResponse, op string) error {
	expected := transferResponse{}
	switch op {
	case "check":
		expected.Capabilities = response.Capabilities
		if response.Capabilities == nil || response.Capabilities.MaxBytes <= 0 {
			return errors.New("transfer capabilities carry no positive maximum length")
		}
	case "begin", "resume":
		expected.Token, expected.DeclaredBytes = response.Token, response.DeclaredBytes
		if !validFileCapability(response.Token) || response.DeclaredBytes != nil && *response.DeclaredBytes < 0 {
			return errors.New("import admission carries an invalid token or length")
		}
	case "receive":
		expected.Seal, expected.DeclaredBytes = response.Seal, response.DeclaredBytes
		if response.Seal == nil || response.Seal.Bytes < 0 || response.DeclaredBytes != nil && *response.DeclaredBytes != response.Seal.Bytes {
			return errors.New("import data response carries no valid content seal")
		}
	case "query", "cancel-action", "publish":
		expected.Receipt, expected.Error = response.Receipt, response.Error
		if _, err := transferReceiptStorage(response.Receipt); err != nil {
			return err
		}
		if (response.Receipt.Outcome == storage.DurableActionUnknown || response.Receipt.Outcome == storage.DurableActionHistoryExpired) && response.Error == nil {
			return errors.New("undetermined import result carries no error")
		}
		if response.Receipt.Outcome == storage.DurableActionCompleted {
			expected.Barrier = response.Barrier
		}
		if op == "publish" && response.Receipt.Outcome.Terminal() {
			expected.Close, expected.Settlement = response.Close, response.Settlement
		}
	case "export":
		expected.Token, expected.Info = response.Token, response.Info
		if !validFileCapability(response.Token) || response.Info == nil || response.Info.Attr == nil || response.Info.Attr.Kind != storage.NodeRegular || len(response.Info.Digest) != sha256.Size {
			return errors.New("export admission carries no verified regular-file content")
		}
	case "cancel", "detach", "close-export":
		expected.Close, expected.Error, expected.Settlement = response.Close, response.Error, response.Settlement
		if response.Close == nil {
			return errors.New("transfer close response carries an invalid release result")
		}
	default:
		return errors.New("unknown transfer response operation")
	}
	if !reflect.DeepEqual(response, expected) {
		return errors.New("transfer response carries unrelated results")
	}
	if response.Error != nil {
		if err := validateCapabilityFactError(response.Error); err != nil {
			return err
		}
	}
	if response.Close != nil && (response.Close.BarrierPending || response.Error == nil && (!response.Close.Released || !response.Close.Determined)) {
		return errors.New("transfer response carries an invalid release result")
	}
	if response.Settlement != nil {
		settlement := response.Settlement
		if response.Close == nil || !response.Close.Released || response.Error == nil || settlement.State != storage.CloseSettlementPending && settlement.State != storage.CloseSettlementUnknown || settlement.Cause == nil {
			return errors.New("transfer response carries an invalid settlement marker")
		}
		if err := validateCapabilityFactError(settlement.Cause); err != nil {
			return err
		}
		if settlement.Semantic != nil {
			if err := validateCapabilityFactError(settlement.Semantic); err != nil {
				return err
			}
		}
	}
	return nil
}

func transferReceiptStorage(wire *transferReceipt) (storage.ImportReceipt, error) {
	if wire == nil {
		return storage.ImportReceipt{}, errors.New("transfer response carries no action receipt")
	}
	receipt := storage.ImportReceipt{Action: wire.Action, Fingerprint: wire.Fingerprint, Outcome: wire.Outcome}
	if wire.Attr != nil {
		receipt.Attr = wire.Attr.Storage()
	}
	if wire.Failure != "" {
		var ok bool
		receipt.Failure, ok = storage.ErrnoByName(wire.Failure)
		if !ok || receipt.Failure == 0 {
			return storage.ImportReceipt{}, errors.New("import receipt carries an unknown failure")
		}
	}
	return receipt, receipt.Check()
}

type remoteImport struct {
	storage       *Storage
	token         string
	action        storage.ActionKey
	command       *storage.ImportCommand
	replication   bool
	mu            sync.Mutex
	closed        *storage.ReferenceCloseResult
	detached      bool
	cleanupErr    error
	released      *storage.ReferenceCloseResult
	publishMu     sync.Mutex
	published     *storage.ImportReceipt
	barrier       *MutationBarrier
	queryOriginal bool
}

func (f *remoteImport) Receive(ctx context.Context, source io.Reader) (storage.ImportSeal, error) {
	f.mu.Lock()
	closed := f.closed != nil || f.detached
	f.mu.Unlock()
	if closed {
		return storage.ImportSeal{}, syscall.ESTALE
	}
	if source == nil {
		return storage.ImportSeal{}, syscall.EINVAL
	}
	// Durable resume returns sealed work. Only its original admission carries
	// enough content identity to receive and verify another stream.
	if f.command == nil {
		return storage.ImportSeal{}, syscall.EOPNOTSUPP
	}
	reader := &transferSealingReader{source: source, remaining: f.command.DeclaredBytes, digest: f.command.Digest, hash: sha256.New()}
	request := Request{Op: OpFileTransferData, Path: f.token}
	response, release, err := f.storage.transferStream(ctx, request, reader)
	if err != nil {
		return storage.ImportSeal{}, err
	}
	defer release()
	content, err := readWhole(response, f.storage.maxBodyBytes)
	if err != nil {
		return storage.ImportSeal{}, operationFailure(request, err, true)
	}
	result, err := decodeTransferResponse(content, "receive")
	if err != nil {
		return storage.ImportSeal{}, err
	}
	if err := reader.check(); err != nil {
		return storage.ImportSeal{}, err
	}
	if result.Seal.Bytes != f.command.DeclaredBytes || result.Seal.Digest != f.command.Digest {
		return storage.ImportSeal{}, unreachable(request, errors.New("import returned a different content seal"))
	}
	return *result.Seal, nil
}

func (f *remoteImport) Publish(ctx context.Context) (storage.ImportReceipt, error) {
	receipt, _, err := f.PublishWithBarrier(ctx)
	return receipt, err
}

func (f *remoteImport) PublishWithBarrier(ctx context.Context) (storage.ImportReceipt, *MutationBarrier, error) {
	f.publishMu.Lock()
	defer f.publishMu.Unlock()
	f.mu.Lock()
	detached := f.detached
	f.mu.Unlock()
	if detached {
		return storage.ImportReceipt{}, nil, syscall.ESTALE
	}
	if f.published != nil {
		receipt := *f.published
		receipt.Attr = receipt.Attr.Clone()
		var barrier *MutationBarrier
		if f.barrier != nil {
			captured := *f.barrier
			barrier = &captured
		}
		return receipt, barrier, nil
	}
	request := transferRequest{Op: "publish", Token: f.token}
	if f.queryOriginal {
		request = transferRequest{Op: "query", Action: &f.action}
	}
	response, err := f.storage.transferCall(ctx, request)
	if response.Receipt == nil {
		return storage.ImportReceipt{}, nil, f.preserveCleanupError(err)
	}
	receipt, receiptErr := transferReceiptStorage(response.Receipt)
	if receiptErr != nil || receipt.Action != f.action || f.command != nil && receipt.Fingerprint != f.command.Fingerprint {
		return storage.ImportReceipt{}, nil, f.preserveCleanupError(unreachable(Request{Op: OpFileTransfer}, errors.New("publication returned an invalid or unrelated receipt")))
	}
	if receipt.Outcome == storage.DurableActionCompleted {
		if f.command != nil && receipt.Attr.Size != f.command.DeclaredBytes {
			return storage.ImportReceipt{}, nil, f.preserveCleanupError(unreachable(Request{Op: OpFileTransfer}, errors.New("publication receipt changed the imported size")))
		}
		if err == nil && f.replication && response.Barrier == nil {
			err = unreachable(Request{Op: OpFileTransfer}, errors.New("publication returned no replication barrier"))
		}
	}
	if receipt.Outcome.Terminal() {
		f.queryOriginal = true
	}
	f.mu.Lock()
	if response.Close != nil {
		result := response.Close.storage()
		if transferCloseSettled(result, err) {
			f.closed, f.cleanupErr, f.released = &result, nil, nil
		} else if f.closed == nil {
			f.cleanupErr = err
			if result.Released {
				f.released = &result
			}
		}
	}
	if request.Op == "query" && f.closed == nil && f.cleanupErr != nil {
		err = errors.Join(err, f.cleanupErr)
	}
	settled := f.closed != nil
	f.mu.Unlock()
	if receipt.Outcome == storage.DurableActionCompleted && err == nil && settled {
		captured := receipt
		captured.Attr = captured.Attr.Clone()
		f.published = &captured
		if response.Barrier != nil {
			barrier := *response.Barrier
			f.barrier = &barrier
		}
	}
	return receipt, response.Barrier, err
}

func (f *remoteImport) preserveCleanupError(err error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed == nil && f.cleanupErr != nil {
		return errors.Join(err, f.cleanupErr)
	}
	return err
}

func (f *remoteImport) Cancel(ctx context.Context) (storage.ReferenceCloseResult, error) {
	return f.close(ctx, "cancel")
}

func (f *remoteImport) Detach(ctx context.Context) (storage.ReferenceCloseResult, error) {
	return f.close(ctx, "detach")
}

func (f *remoteImport) close(ctx context.Context, op string) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	if f.detached && op == "cancel" {
		result := *f.closed
		f.mu.Unlock()
		return result, syscall.ESTALE
	}
	if f.closed != nil {
		result := *f.closed
		if op == "detach" {
			f.detached = true
		}
		f.mu.Unlock()
		return result, nil
	}
	f.mu.Unlock()
	response, err := f.storage.transferCall(ctx, transferRequest{Op: op, Token: f.token})
	f.mu.Lock()
	defer f.mu.Unlock()
	if response.Close == nil {
		if f.released != nil {
			var operation *operationError
			var previous *storage.CloseSettlementError
			if errors.As(err, &operation) && operation.unknown && errors.As(f.cleanupErr, &previous) {
				f.cleanupErr = &storage.CloseSettlementError{State: storage.CloseSettlementUnknown, SemanticErr: previous.SemanticErr, Cause: err}
				return *f.released, f.cleanupErr
			}
			return *f.released, errors.Join(f.cleanupErr, err)
		}
		return storage.ReferenceCloseResult{}, err
	}
	result := response.Close.storage()
	if transferCloseSettled(result, err) {
		f.closed, f.cleanupErr, f.released = &result, nil, nil
		if op == "detach" {
			f.detached = true
		}
	} else if f.closed == nil {
		var current, previous *storage.CloseSettlementError
		errors.As(err, &current)
		errors.As(f.cleanupErr, &previous)
		if current != nil || previous == nil {
			f.cleanupErr = err
		} else {
			err = errors.Join(err, f.cleanupErr)
		}
		if result.Released {
			f.released = &result
		} else if f.released != nil {
			result = *f.released
		}
	}
	return result, err
}

type remoteExport struct {
	storage *Storage
	token   string
	info    storage.ExportInfo
	mu      sync.Mutex
	closed  *storage.ReferenceCloseResult
}

func (f *remoteExport) Info() storage.ExportInfo {
	info := f.info
	info.Attr = info.Attr.Clone()
	return info
}

func (f *remoteExport) WriteTo(ctx context.Context, destination io.Writer) error {
	if destination == nil {
		return syscall.EINVAL
	}
	request := Request{Op: OpFileTransferExport, Path: f.token}
	response, release, err := f.storage.transferStream(ctx, request, nil)
	if err != nil {
		return err
	}
	defer release()
	if response.ContentLength >= 0 && response.ContentLength != f.info.Attr.Size {
		return unreachable(request, errors.New("export stream changed its captured length"))
	}
	_, err = storage.CopyTransferContent(ctx, destination, response.Body, f.info.Attr.Size, f.info.Digest)
	return err
}

func (f *remoteExport) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	f.mu.Lock()
	if f.closed != nil {
		result := *f.closed
		f.mu.Unlock()
		return result, nil
	}
	f.mu.Unlock()
	response, err := f.storage.transferCall(ctx, transferRequest{Op: "close-export", Token: f.token})
	if response.Close == nil {
		return storage.ReferenceCloseResult{}, err
	}
	result := response.Close.storage()
	if transferCloseSettled(result, err) {
		f.mu.Lock()
		f.closed = &result
		f.mu.Unlock()
	}
	return result, err
}

func transferCloseSettled(result storage.ReferenceCloseResult, err error) bool {
	var unsettled *storage.CloseSettlementError
	return result.Released && result.Determined && !errors.As(err, &unsettled)
}

func (s *Storage) transferStream(ctx context.Context, request Request, content io.Reader) (*http.Response, func(), error) {
	release, err := s.responses.acquire(ctx, retainedResponseMultiplier*s.maxBodyBytes)
	if err != nil {
		return nil, nil, operationFailure(request, err, true)
	}
	retained := false
	defer func() {
		if !retained {
			release()
		}
	}()
	u, err := request.URL(s.base)
	if err != nil {
		return nil, nil, unreachable(request, err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, request.Method(), u.String(), content)
	if err != nil {
		return nil, nil, unreachable(request, err)
	}
	if content != nil {
		httpRequest.Header.Set("Content-Type", request.ContentType())
	}
	response, err := s.http.Do(httpRequest)
	if err != nil {
		return nil, nil, operationFailure(request, err, true)
	}
	closeResponse := func() {
		_ = response.Body.Close()
		release()
	}
	if response.Header.Get(HeaderProtocol) != Version {
		_ = response.Body.Close()
		return nil, nil, unreachable(request, errors.New("transfer response carries no matching protocol version"))
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		if response.StatusCode == StatusStorageError {
			body, err := readWhole(response, s.maxBodyBytes)
			if err != nil {
				return nil, nil, operationFailure(request, err, true)
			}
			return nil, nil, s.storageError(request, body)
		}
		return nil, nil, unreachable(request, fmt.Errorf("the transfer server answered %s", response.Status))
	}
	retained = true
	return response, closeResponse, nil
}

type transferSealingReader struct {
	source     io.Reader
	remaining  int64
	digest     [sha256.Size]byte
	hash       hash.Hash
	mu         sync.Mutex
	verified   bool
	failure    error
	emptyReads int
}

func (r *transferSealingReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	limit := int64(len(buffer))
	if limit > r.remaining {
		limit = r.remaining + 1
	}
	n, err := r.source.Read(buffer[:limit])
	if n < 0 || int64(n) > limit {
		n, err = 0, syscall.EIO
	} else if int64(n) > r.remaining {
		n, err = 0, syscall.EFBIG
	} else {
		_, _ = r.hash.Write(buffer[:n])
		r.remaining -= int64(n)
	}
	if n == 0 && err == nil {
		r.emptyReads++
		if r.emptyReads == 100 {
			err = io.ErrNoProgress
		}
	} else {
		r.emptyReads = 0
	}
	if errors.Is(err, io.EOF) {
		var actual [sha256.Size]byte
		copy(actual[:], r.hash.Sum(nil))
		if r.remaining != 0 {
			err = syscall.EINVAL
		} else if actual != r.digest {
			err = syscall.EIO
		} else {
			r.mu.Lock()
			r.verified = true
			r.mu.Unlock()
		}
	}
	if err != nil && !errors.Is(err, io.EOF) {
		r.mu.Lock()
		r.failure = err
		r.mu.Unlock()
	}
	return n, err
}

func (r *transferSealingReader) check() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure != nil {
		return r.failure
	}
	if !r.verified {
		return errors.New("transfer server acknowledged a stream before its content was verified")
	}
	return nil
}
