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
		storage.OpFileRead, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr:
		return true
	}
	return false
}

func fileBoundedResult(op storage.Operation) bool {
	return fileAttrResult(op) || op == storage.OpFileSetNodeMetadata || op == storage.OpFileSetMetadata
}

func fileResponseLimit(req fileRequest, maximum int64) int64 {
	if fileBoundedResult(req.Op) {
		return min(req.ResultBytes, maximum)
	}
	if fileControl(req.Op) {
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
	limit := min(req.ResultBytes, h.maxBodyBytes)
	return func(attr storage.Attr, metadataBytes int64) error {
		response := fileResponse{Epoch: math.MaxUint64, Data: []byte{}, Attr: AttrOf(attr)}
		extra := int64(0)
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
		return nil
	}
}
