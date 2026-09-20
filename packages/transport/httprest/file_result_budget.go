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
	case storage.OpFileStat, storage.OpFileStatNode, storage.OpFileSetNodeAttr,
		storage.OpFileRead, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr,
		storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef,
		storage.OpFileLookupAt, storage.OpFileMutateName, storage.OpFileState,
		storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink, storage.OpFileMutate:
		return true
	}
	return false
}

func fileBoundedResult(op storage.Operation) bool {
	return fileAttrResult(op) || op == storage.OpFileSetNodeMetadata || op == storage.OpFileSetMetadata ||
		op == storage.OpFileReadDirNode || op == storage.OpFileObserveDirectoryMetadata || op == storage.OpFileObserveName
}

func fileResponseLimit(req fileRequest, maximum int64) int64 {
	if fileBoundedResult(req.Op) {
		return min(req.ResultBytes, fileOperationLimit(req.Op, maximum))
	}
	if fileControl(req.Op) {
		return min(maximum, MaxFileControlBytes)
	}
	return maximum
}

func fileOperationLimit(operation storage.Operation, maximum int64) int64 {
	if fileControl(operation) {
		return min(maximum, MaxFileControlBytes)
	}
	return maximum
}

func metadataResponseBound(payloadBytes int, barrier bool, maxIncarnationBytes int64) (int64, error) {
	response := fileResponse{Epoch: math.MaxUint64, Data: []byte{}, Metadata: &OpaquePayload{
		Version: make([]byte, storage.MaxObservationTokenBytes), Data: []byte{},
	}}
	if barrier {
		response.Barrier = &MutationBarrier{Incarnation: strings.Repeat("\x01", int(maxIncarnationBytes)), Position: math.MaxInt64}
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return 0, err
	}
	return int64(len(encoded)) + int64(base64.StdEncoding.EncodedLen(payloadBytes)), nil
}

func (h *Handler) attrResultBudget(req fileRequest) storage.AttrResultBudget {
	limit := min(req.ResultBytes, fileOperationLimit(req.Op, h.maxBodyBytes))
	return func(attr storage.Attr, metadataBytes int64) error {
		response := fileResponse{Epoch: math.MaxUint64, Data: []byte{}}
		extra := int64(0)
		switch req.Op {
		case storage.OpFileState, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink:
			response.State = &referenceState{Attr: AttrOf(attr), PendingGeneration: make([]byte, storage.MaxObservationTokenBytes)}
			if attr.Kind == storage.NodeSymlink {
				if attr.Size < 0 || attr.Size > storage.MaxLinkTargetBytes {
					return syscall.EFBIG
				}
				extra = int64(len(`,"linkTarget":""`)) + int64(base64.StdEncoding.EncodedLen(int(attr.Size)))
			}
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
			extra = int64(base64.StdEncoding.EncodedLen(int(length)))
		}
		if fileMutation(req.Op) && h.log != nil {
			response.Barrier = &MutationBarrier{Incarnation: strings.Repeat("\x01", int(h.maxIncarnationBytes)), Position: math.MaxInt64}
		}
		encoded, err := json.Marshal(response)
		if err != nil {
			return fmt.Errorf("cannot size file result: %w", err)
		}
		metadataCharge, err := metadataResultBytes(metadataBytes)
		if err != nil {
			return err
		}
		if int64(len(encoded))+extra+metadataCharge > limit {
			return fmt.Errorf("file result exceeds its %d-byte response bound: %w", limit, syscall.EFBIG)
		}
		if partialFileResult(req, response) != nil {
			failure, err := json.Marshal(ErrorResponse{Errno: "EIO", Message: boundedErrorDetail, FileResult: &response})
			if err != nil {
				return fmt.Errorf("cannot size partial file result: %w", err)
			}
			errorLimit := fileOperationLimit(req.Op, h.maxBodyBytes)
			if int64(len(failure))+extra+metadataCharge > errorLimit {
				return fmt.Errorf("partial file result exceeds its %d-byte response bound: %w", errorLimit, syscall.EFBIG)
			}
		}
		return nil
	}
}
