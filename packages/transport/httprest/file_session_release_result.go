package httprest

import (
	"context"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

const opFileSessionReleaseResult storage.Operation = "file.session-release-result"

func (s *servedFileSession) observeSessionRelease(result storage.ReferenceCloseResult, err error) {
	var unsettled *storage.CloseSettlementError
	confirmed := result.Released && result.Check(err) == nil && !errors.As(err, &unsettled)
	s.mu.Lock()
	s.releaseFact = s.releaseFact || s.inlineSettlement && confirmed
	s.mu.Unlock()
}

func (h *Handler) sessionReleaseResult(ctx context.Context, id string) (fileResponse, error) {
	h.files.mu.Lock()
	terminal, closed := h.files.terminalCloses[id], h.files.closed
	h.files.mu.Unlock()
	if closed {
		return fileResponse{}, syscall.EIO
	}
	if terminal == nil {
		return fileResponse{}, syscall.ESTALE
	}
	terminal.mu.Lock()
	valid := time.Now().Before(terminal.expires)
	confirmed, epoch, releaseErr := terminal.releaseFact, terminal.epoch, terminal.releaseErr
	terminal.mu.Unlock()
	if !valid {
		return fileResponse{}, syscall.ESTALE
	}
	if !confirmed {
		return fileResponse{}, syscall.EOPNOTSUPP
	}
	response := fileResponse{Epoch: epoch, CloseResult: referenceCloseResultOf(storage.ReferenceCloseResult{Released: true, Determined: true})}
	response, err := h.finishFileMutation(ctx, response)
	return response, errors.Join(releaseErr, err)
}

func (s *remoteFileSession) sessionReleaseResult(ctx context.Context) (fileResponse, error) {
	return s.storage.fileCall(ctx, fileRequest{Op: opFileSessionReleaseResult, Session: s.id})
}
