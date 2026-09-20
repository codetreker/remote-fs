package httprest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/codetreker/remote-fs/packages/storage"
	"io"
	"math"
	"reflect"
	"unicode/utf8"
)

func decodeFileJSON(data []byte, target any) error {
	if !utf8.Valid(data) {
		return errors.New("file JSON must be UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := checkTypedJSON(decoder, reflect.TypeOf(target).Elem(), len(data)); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("file JSON contains trailing content")
	}
	return json.Unmarshal(data, target)
}

func validFileCapability(cap string) bool {
	if len(cap) != 64 {
		return false
	}
	for _, c := range cap {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validateFileRequest(r fileRequest) error {
	expected := fileRequest{Op: r.Op, Session: r.Session, Action: r.Action, Path: []byte{}, Data: []byte{}}
	if r.Op == storage.OpFileSessionOpen {
		if r.Session != "" {
			return errors.New("new file session names a previous session")
		}
	} else if !validFileCapability(r.Session) {
		return errors.New("invalid file session capability")
	}
	if fileActionRequired(r.Op) || r.Action != "" && (r.Op == storage.OpFileClose || r.Op == storage.OpFileSessionClose) {
		if _, err := r.Action.Epoch(); err != nil {
			return err
		}
	} else if r.Action != "" {
		return errors.New("operation does not accept an action identity")
	}
	reference := false
	switch r.Op {
	case storage.OpFileSessionOpen:
		expected.Options = r.Options
	case storage.OpFileStatus, storage.OpFileRenew, storage.OpFileSessionClose:
	case storage.OpFileQueryAction:
		expected.FileAction = r.FileAction
	case storage.OpFileQueryDeleteIntent:
		expected.DeleteIntent = r.DeleteIntent
	case storage.OpFileAcknowledgeDeleteIntent:
		expected.Acknowledge = r.Acknowledge
	case storage.OpFileOpen:
		expected.Path = r.Path
		expected.Open = r.Open
	case storage.OpFileOpenNode:
		expected.Node = r.Node
		expected.Open = r.Open
	case storage.OpFileStatNode:
		expected.Node = r.Node
	case storage.OpFileSetNodeAttr:
		expected.Node = r.Node
		expected.Change = r.Change
	case storage.OpFileStat, storage.OpFileSync, storage.OpFileClose, storage.OpFileAck, storage.OpFileState, storage.OpFileScope:
		reference = true
	case storage.OpFileRead:
		reference = true
		expected.Offset = r.Offset
		expected.Length = r.Length
	case storage.OpFileWrite:
		reference = true
		expected.Offset = r.Offset
		expected.Data = r.Data
	case storage.OpFileTruncate:
		reference = true
		expected.Offset = r.Offset
	case storage.OpFileSetAttr:
		reference = true
		expected.Change = r.Change
	case storage.OpFileOpenAt:
		expected.Child = r.Child
		expected.OpenAt = r.OpenAt
	case storage.OpFileOpenNodeRef:
		expected.Node = r.Node
		expected.NodeRef = r.NodeRef
	case storage.OpFileOpenChildRef:
		expected.Child = r.Child
		expected.NodeRef = r.NodeRef
	case storage.OpFileLookupAt:
		expected.Child = r.Child
	case storage.OpFileMutateName:
		expected.Name = r.Name
	case storage.OpFileSetNodeMetadata:
		expected.Node = r.Node
		expected.Namespace = r.Namespace
		expected.Version = r.Version
		expected.Payload = r.Payload
	case storage.OpFileSetMetadata:
		reference = true
		expected.Namespace = r.Namespace
		expected.Version = r.Version
		expected.Payload = r.Payload
	case storage.OpFileNewUseOwner:
		expected.Node = r.Node
		expected.Scope = r.Scope
		expected.OwnerOptions = r.OwnerOptions
	case storage.OpFileRetireUseOwner:
		expected.Owner = r.Owner
	case storage.OpFileRangeGetConflict:
		expected.Owner = r.Owner
		expected.Commands = r.Commands
	case storage.OpFileRangeApply:
		expected.Owner = r.Owner
		expected.Commands = r.Commands
		expected.LockID = r.LockID
	case storage.OpFileRangeQuery, storage.OpFileRangeCancel:
		expected.Owner = r.Owner
		expected.LockID = r.LockID
	case storage.OpFileRangeDrop:
		expected.Owner = r.Owner
		expected.Domain = r.Domain
	case storage.OpFileSetPendingUnlink:
		reference = true
		expected.Pending = r.Pending
	case storage.OpFileClearPendingUnlink:
		reference = true
		expected.ClearPending = r.ClearPending
	case storage.OpFileMutate:
		reference = true
		expected.Mutation = r.Mutation
	default:
		return errors.New("unknown retained file operation")
	}
	if fileBoundedResult(r.Op) {
		expected.ResultBytes = r.ResultBytes
	}
	if reference {
		expected.File = r.File
		if !validFileCapability(r.File) {
			return errors.New("file operation has no reference capability")
		}
	}
	if !reflect.DeepEqual(r, expected) {
		return errors.New("file operation carries unrelated operands")
	}
	if action := semanticFileAction(r); action != "" && r.Action != action {
		return errors.New("file operation action identity differs from its semantic action")
	}
	switch r.Op {
	case storage.OpFileSetAttr, storage.OpFileSetNodeAttr:
		if r.Change == nil {
			return errors.New("attribute operation carries no change")
		}
	case storage.OpFileOpenAt:
		if r.Child == nil || r.OpenAt == nil {
			return errors.New("atomic open carries no target or options")
		}
	case storage.OpFileOpenNodeRef:
		if r.NodeRef == nil {
			return errors.New("reference open carries no options")
		}
	case storage.OpFileOpenChildRef:
		if r.Child == nil || r.NodeRef == nil {
			return errors.New("reference open carries no target or options")
		}
	case storage.OpFileLookupAt:
		if r.Child == nil {
			return errors.New("lookup carries no child")
		}
	case storage.OpFileMutateName:
		if r.Name == nil {
			return errors.New("name operation carries no command")
		}
	case storage.OpFileNewUseOwner:
		if r.Scope == nil {
			return errors.New("owner enrollment carries no reference scope")
		}
	case storage.OpFileSetPendingUnlink:
		if r.Pending == nil {
			return errors.New("pending unlink carries no command")
		}
	case storage.OpFileClearPendingUnlink:
		if r.ClearPending == nil {
			return errors.New("pending unlink clear carries no command")
		}
	case storage.OpFileMutate:
		if r.Mutation == nil {
			return errors.New("conditional mutation carries no command")
		}
	case storage.OpFileAcknowledgeDeleteIntent:
		if r.Acknowledge == nil {
			return errors.New("delete intent acknowledgement carries no command")
		}
	}
	return nil
}

func validateFileResponse(req fileRequest, r fileResponse) error {
	if r.Epoch == 0 {
		return errors.New("file response carries no action epoch")
	}
	expected := fileResponse{Epoch: r.Epoch, Data: []byte{}}
	if r.Retry {
		if !fileActionRequired(req.Op) {
			return errors.New("unexpected file action retry receipt")
		}
		expected.Retry = true
	} else {
		switch req.Op {
		case storage.OpFileSessionOpen:
			expected.Session = r.Session
			expected.Status = r.Status
			expected.Capabilities = r.Capabilities
		case storage.OpFileStatus, storage.OpFileRenew:
			expected.Status = r.Status
		case storage.OpFileQueryAction:
			expected.ActionReceipt = r.ActionReceipt
		case storage.OpFileQueryDeleteIntent:
			expected.DeleteStatus = r.DeleteStatus
		case storage.OpFileOpen, storage.OpFileOpenNode:
			expected.Node = r.Node
			expected.File = r.File
			expected.Barrier = r.Barrier
			expected.Capabilities = r.Capabilities
		case storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
			expected.File = r.File
			expected.Barrier = r.Barrier
			expected.Attr = r.Attr
			expected.Outcome = r.Outcome
			expected.Capabilities = r.Capabilities
		case storage.OpFileStat, storage.OpFileStatNode, storage.OpFileLookupAt:
			expected.Attr = r.Attr
		case storage.OpFileRead:
			expected.Attr = r.Attr
			expected.Data = r.Data
		case storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileSetNodeAttr, storage.OpFileMutate:
			expected.Attr = r.Attr
			expected.Barrier = r.Barrier
		case storage.OpFileMutateName:
			expected.Attr = r.Attr
			expected.Barrier = r.Barrier
		case storage.OpFileSync, storage.OpFileClose, storage.OpFileSessionClose:
			expected.Barrier = r.Barrier
		case storage.OpFileScope:
			expected.Scope = r.Scope
		case storage.OpFileState:
			expected.State = r.State
		case storage.OpFileNewUseOwner:
			expected.Owner = r.Owner
		case storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata:
			expected.Metadata = r.Metadata
			expected.Barrier = r.Barrier
		case storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink:
			expected.State = r.State
			expected.Barrier = r.Barrier
		case storage.OpFileRangeGetConflict:
			expected.Conflict = r.Conflict
		case storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel:
			expected.Attempt = r.Attempt
		case storage.OpFileAck, storage.OpFileAcknowledgeDeleteIntent, storage.OpFileRetireUseOwner, storage.OpFileRangeDrop:
		default:
			return errors.New("unknown file response variant")
		}
	}
	if !reflect.DeepEqual(r, expected) {
		return errors.New("file response carries unrelated result fields")
	}
	if r.Retry {
		return nil
	}
	switch req.Op {
	case storage.OpFileSessionOpen:
		if !validFileCapability(r.Session) || r.Status == nil || r.Capabilities == nil {
			return errors.New("file session response has no valid capability or status")
		}
	case storage.OpFileStatus, storage.OpFileRenew:
		if r.Status == nil {
			return errors.New("file response carries no status")
		}
	case storage.OpFileQueryAction:
		if r.ActionReceipt == nil || r.ActionReceipt.Check() != nil || r.ActionReceipt.Action != req.FileAction {
			return errors.New("file action query returned an invalid receipt")
		}
	case storage.OpFileQueryDeleteIntent:
		if r.DeleteStatus == nil || r.DeleteStatus.ID != req.DeleteIntent {
			return errors.New("delete intent query returned an invalid status")
		}
		if _, err := r.DeleteStatus.storage(); err != nil {
			return err
		}
	case storage.OpFileOpen, storage.OpFileOpenNode:
		if !validFileCapability(r.File) || r.Capabilities == nil {
			return errors.New("file open response carries no reference capability")
		}
	case storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		if !validFileCapability(r.File) || r.Attr == nil || r.Capabilities == nil || r.Outcome < storage.Opened || r.Outcome > storage.Replaced {
			return errors.New("atomic open response is incomplete")
		}
	case storage.OpFileRead, storage.OpFileStat, storage.OpFileStatNode, storage.OpFileLookupAt, storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileSetNodeAttr, storage.OpFileMutate:
		if r.Attr == nil {
			return errors.New("file response carries no attributes")
		}
	case storage.OpFileMutateName:
		if req.Name == nil || nameResultNeedsAttr(req.Name.Kind) && r.Attr == nil {
			return errors.New("name response carries no required attributes")
		}
	case storage.OpFileState, storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink:
		if r.State == nil {
			return errors.New("reference state is incomplete")
		}
	case storage.OpFileScope:
		if r.Scope == nil {
			return errors.New("reference scope is absent")
		}
		if err := r.Scope.Check(); err != nil {
			return err
		}
		if !utf8.ValidString(r.Scope.Token) {
			return errors.New("reference scope is not valid UTF-8")
		}
	case storage.OpFileNewUseOwner:
		if r.Owner == 0 {
			return errors.New("owner enrollment carries no owner")
		}
	case storage.OpFileSetNodeMetadata, storage.OpFileSetMetadata:
		if r.Metadata == nil {
			return errors.New("metadata result is absent")
		}
	case storage.OpFileRangeGetConflict:
		if r.Conflict == nil {
			return errors.New("range query has no conflict result")
		}
	case storage.OpFileRangeApply, storage.OpFileRangeQuery, storage.OpFileRangeCancel:
		if r.Attempt == nil {
			return errors.New("range response has no attempt result")
		}
	}
	if r.Status != nil {
		s := r.Status
		if s.Epoch == "" || len(s.Epoch) > MaxLockCapabilityBytes || s.Revision == 0 || s.ActionEpoch == 0 || s.Remaining < 0 || s.HistoryRemaining < 0 {
			return errors.New("invalid file session status")
		}
	}
	if r.Attr != nil {
		if err := r.Attr.check(); err != nil {
			return err
		}
		switch req.Op {
		case storage.OpFileOpenAt, storage.OpFileRead, storage.OpFileWrite, storage.OpFileTruncate:
			if r.Attr.Kind != storage.NodeRegular {
				return errors.New("file operation returned nonregular attributes")
			}
		}
	}
	if r.State != nil {
		if err := validateReferenceState(r.State); err != nil {
			return err
		}
	}
	if r.Metadata != nil {
		if err := storage.CheckMetadata(map[string]storage.OpaquePayload{req.Namespace: {Version: r.Metadata.Version, Data: r.Metadata.Data}}); err != nil {
			return err
		}
	}
	if r.Conflict != nil {
		if err := validateFileConflict(*r.Conflict); err != nil {
			return err
		}
	}
	if r.Attempt != nil {
		if err := validateFileAttempt(req, *r.Attempt); err != nil {
			return err
		}
	}
	return nil
}

func validatePartialFileResponse(req fileRequest, response fileResponse) error {
	if response.Epoch == 0 || response.Retry || response.Session != "" || response.Status != nil || len(response.Data) != 0 || response.Conflict != nil || response.Attempt != nil || response.Scope != nil || response.Owner != 0 || response.Metadata != nil {
		return errors.New("partial file result carries unrelated fields")
	}
	expected := fileResponse{Epoch: response.Epoch, Data: []byte{}}
	switch req.Op {
	case storage.OpFileQueryAction:
		expected.ActionReceipt = response.ActionReceipt
		if response.ActionReceipt == nil || response.ActionReceipt.Check() != nil || response.ActionReceipt.Action != req.FileAction {
			return errors.New("partial file action receipt is invalid")
		}
	case storage.OpFileQueryDeleteIntent:
		expected.DeleteStatus = response.DeleteStatus
		if response.DeleteStatus == nil || response.DeleteStatus.ID != req.DeleteIntent {
			return errors.New("partial delete intent status is invalid")
		}
		if _, err := response.DeleteStatus.storage(); err != nil {
			return err
		}
	case storage.OpFileOpenAt, storage.OpFileOpenNodeRef, storage.OpFileOpenChildRef:
		expected.File = response.File
		expected.Attr = response.Attr
		expected.Outcome = response.Outcome
		expected.Capabilities = response.Capabilities
		expected.Barrier = response.Barrier
		if response.File != "" && (!validFileCapability(response.File) || response.Capabilities == nil) {
			return errors.New("partial open result carries an invalid reference")
		}
		if response.Attr != nil {
			if err := response.Attr.check(); err != nil {
				return err
			}
		}
		if response.Outcome != 0 && (response.Outcome < storage.Opened || response.Outcome > storage.Replaced) {
			return errors.New("partial open result carries an invalid outcome")
		}
	case storage.OpFileMutateName, storage.OpFileMutate:
		expected.Attr = response.Attr
		expected.Barrier = response.Barrier
		if response.Attr != nil {
			if err := response.Attr.check(); err != nil {
				return err
			}
		}
	case storage.OpFileSetPendingUnlink, storage.OpFileClearPendingUnlink:
		expected.State = response.State
		expected.Barrier = response.Barrier
		if response.State != nil {
			if err := validateReferenceState(response.State); err != nil {
				return err
			}
		}
	default:
		return errors.New("operation cannot return a partial file result")
	}
	if !reflect.DeepEqual(response, expected) {
		return errors.New("partial file result carries unrelated fields")
	}
	return nil
}

func validateReferenceState(state *referenceState) error {
	if state.Attr == nil {
		return errors.New("reference state is incomplete")
	}
	if err := state.Attr.check(); err != nil {
		return err
	}
	if state.Detached && state.PendingUnlink {
		return errors.New("detached reference cannot remain pending unlink")
	}
	if state.PendingUnlink != (len(state.PendingGeneration) != 0) || len(state.PendingGeneration) > storage.MaxObservationTokenBytes {
		return errors.New("reference state carries inconsistent pending generation")
	}
	if len(state.LinkTarget) > storage.MaxLinkTargetBytes ||
		state.Attr.Kind == storage.NodeSymlink && (len(state.LinkTarget) == 0 || int64(len(state.LinkTarget)) != state.Attr.Size) ||
		state.Attr.Kind != storage.NodeSymlink && len(state.LinkTarget) != 0 {
		return errors.New("reference state carries an invalid link target")
	}
	return nil
}

func validateFileConflict(c storage.RangeConflict) error {
	if !c.Found {
		if c != (storage.RangeConflict{}) {
			return errors.New("absent conflict carries owner or range state")
		}
		return nil
	}
	if c.Mode != storage.RangeShared && c.Mode != storage.RangeExclusive {
		return errors.New("range conflict has an invalid mode")
	}
	return c.Range.Check()
}

func validateFileAttempt(req fileRequest, a storage.RangeAttempt) error {
	if a.Request != req.LockID {
		return errors.New("range result identifies a different action")
	}
	if len(a.Commands) == 0 || len(a.Commands) > storage.MaxRangeCommands || len(a.Claims) > storage.MaxRangeClaims || len(a.Effects) > storage.MaxRangeEffects {
		return errors.New("range result has an invalid batch size")
	}
	for _, command := range a.Commands {
		if err := command.Check(); err != nil {
			return err
		}
	}
	if req.Op == storage.OpFileRangeApply && !reflect.DeepEqual(a.Commands, req.Commands) {
		return errors.New("range result identifies a different intent")
	}
	for _, claim := range a.Claims {
		if err := claim.Check(); err != nil {
			return err
		}
	}
	for _, effect := range a.Effects {
		if err := effect.Command.Check(); err != nil {
			return err
		}
		if effect.Claim != "" {
			if err := effect.Claim.Check(); err != nil {
				return err
			}
		}
	}
	if err := validateFileConflict(a.Conflict); err != nil {
		return err
	}
	if a.HistoryRemaining < 0 {
		return errors.New("range history lifetime is negative")
	}
	if a.State == storage.Rejected {
		if a.FailedAt != nil {
			if *a.FailedAt < 0 || *a.FailedAt >= len(a.Commands) {
				return errors.New("range result has an invalid failing command position")
			}
		} else {
			switch a.Rejection {
			case storage.RangeExhausted, storage.RangeTooLarge:
			default:
				return errors.New("range rejection omits its failing command position")
			}
		}
	} else if a.FailedAt != nil {
		return errors.New("range result has an invalid failing command position")
	}
	acquires, waits := false, false
	for _, command := range a.Commands {
		acquires = acquires || command.Edit == storage.Replace || command.Edit == storage.AddExact
		waits = waits || command.Wait
	}
	switch a.State {
	case storage.Pending:
		if a.EverGranted || a.Rejection != "" || !waits {
			return errors.New("inconsistent pending range result")
		}
	case storage.Granted:
		if !acquires || !a.EverGranted || a.Rejection != "" {
			return errors.New("inconsistent granted range result")
		}
	case storage.Released:
		if a.Rejection != "" || a.EverGranted != acquires {
			return errors.New("inconsistent released range result")
		}
	case storage.Cancelled:
		if a.EverGranted || a.Rejection != "" {
			return errors.New("inconsistent cancelled range result")
		}
	case storage.Rejected:
		if a.EverGranted {
			return errors.New("rejected range result claims a grant")
		}
		switch a.Rejection {
		case storage.RangeBlocked, storage.RangeNotHeld, storage.RangeExhausted, storage.RangeDeadlock, storage.RangeInvalid, storage.RangeUnsupported, storage.RangeExpired, storage.RangeTooLarge:
		default:
			return errors.New("range rejection code is unknown")
		}
	default:
		return fmt.Errorf("unknown range action state %d", a.State)
	}
	return validateRangeEffects(a)
}

var fileReadEnvelopeBytes = func() int64 {
	worstTime := Time{UnixSec: math.MinInt64, Nanos: 999999999}
	response := fileResponse{Epoch: math.MaxUint64, Data: []byte{}, Attr: &Attr{ID: math.MaxUint64, Kind: storage.NodeRegular, Size: math.MaxInt64, AccessTime: worstTime, ModTime: worstTime}}
	encoded, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}
	return int64(len(encoded))
}()

func fileReadLimit(limit int64) int64 {
	if limit < fileReadEnvelopeBytes {
		return -1
	}
	return (limit - fileReadEnvelopeBytes) / 4 * 3
}
