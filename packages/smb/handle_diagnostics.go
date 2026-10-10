package smb

import (
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const diagnosticOwnerBytes = 256

type HandleOwnerStatus struct {
	FileID       wire.FileID
	SessionID    uint64
	TreeID       uint32
	NodeID       uint64
	OpenAction   storage.FileActionID
	CloseAttempt storage.CloseAttempt
	State        string
	LastStatus   uint32
}

func (h *fileHandle) recordOpenDiagnostic() {
	prior := h.diagnostic.Load()
	if prior == nil {
		return
	}
	current := *prior
	current.OpenAction = h.action
	h.diagnostic.Store(&current)
}

func (h *fileHandle) recordCloseDiagnostic() {
	prior := h.diagnostic.Load()
	if prior == nil {
		return
	}
	current := *prior
	current.CloseAttempt = *h.attempt
	h.diagnostic.Store(&current)
}

// HandleOwners contains only opaque lifetime and action identifiers. Every
// potential cleanup record is charged before CREATE can produce an effect.
func (s *Server) HandleOwners() []HandleOwnerStatus {
	s.handleMu.Lock()
	defer s.handleMu.Unlock()
	result := make([]HandleOwnerStatus, 0, len(s.handleOwners))
	for h := range s.handleOwners {
		snapshot := h.diagnostic.Load()
		if snapshot == nil {
			continue
		}
		owner := *snapshot
		owner.LastStatus = h.lastStatus.Load()
		switch handleState(h.state.Load()) {
		case handleReserved:
			owner.State = "opening"
		case handleLive:
			owner.State = "live"
		case handleCleanupOnly:
			owner.State = "cleanup-only"
		case handleBarrierOnly:
			owner.State = "barrier-only"
		}
		result = append(result, owner)
	}
	return result
}
