package httprest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

func fileAttrResult(op storage.Operation) bool {
	switch op {
	case storage.OpFileStat, storage.OpFileStatNode, storage.OpFileSetNodeAttr, storage.OpFileRead, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef, storage.OpFileLookupAt, storage.OpFileMutateName, storage.OpFileState, storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink, storage.OpFileMutate:
		return true
	}
	return false
}

func (h *Handler) attrResultBudget(req fileRequest) storage.AttrResultBudget {
	limit := min(req.ResultBytes, h.maxBodyBytes)
	return func(attr storage.Attr, metadataBytes int64) error {
		response := fileResponse{Epoch: math.MaxUint64, Data: []byte{}}
		extra := int64(0)
		switch req.Op {
		case storage.OpFileState, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink:
			response.State = &referenceState{Attr: AttrOf(attr), Detached: false, PendingUnlink: false, PendingGeneration: make([]byte, storage.MaxObservationTokenBytes)}
			if attr.Kind == storage.NodeSymlink {
				if attr.Size < 0 || attr.Size > storage.MaxLinkTargetBytes {
					return syscall.EFBIG
				}
				extra = int64(len(`,"linkTarget":""`)) + int64(base64.StdEncoding.EncodedLen(int(attr.Size)))
			}
		case storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata:
			response.Metadata = &OpaquePayload{Version: make([]byte, storage.MaxObservationTokenBytes), Data: []byte{}}
			extra = int64(base64.StdEncoding.EncodedLen(len(req.Payload)))
		default:
			response.Attr = AttrOf(attr)
		}
		if req.Op == storage.OpFileOpenAt || req.Op == storage.OpFileOpenNodeRef || req.Op == storage.OpFileOpenChildRef {
			response.File = strings.Repeat("f", 64)
			response.Outcome = storage.Replaced
			response.Capabilities = &fileCapabilities{}
		}
		if req.Op == storage.OpFileRead {
			length := min(int64(req.Length), max(int64(0), attr.Size-req.Offset))
			if length < 0 || length > limit {
				return syscall.EFBIG
			}
			extra += int64(base64.StdEncoding.EncodedLen(int(length)))
		}
		if fileMutation(req.Op) && h.log != nil {
			response.Barrier = &MutationBarrier{Incarnation: strings.Repeat("\x01", int(h.maxIncarnationBytes)), Position: math.MaxInt64}
		}
		encoded, err := json.Marshal(response)
		if err != nil {
			return fmt.Errorf("cannot size file result: %w", err)
		}
		// Metadata remains unloaded here. Its canonical envelope bounds JSON fields
		// and base64 payloads without interpreting any client namespace.
		metadataCharge, err := metadataResultBytes(metadataBytes, 0)
		if err != nil {
			return err
		}
		charge := int64(len(encoded)) + extra + metadataCharge
		if charge > limit {
			return fmt.Errorf("file result exceeds its %d-byte response bound: %w", limit, syscall.EFBIG)
		}
		return nil
	}
}
