package httprest

import (
	"bytes"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"math"
	"reflect"
	"unicode/utf8"
)

func decodeFileJSON(data []byte, target any) error { return decodeMessageJSON(data, target, len(data)) }
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
	expected := fileRequest{Op: r.Op, Session: r.Session, Action: r.Action}
	if r.Op == storage.OpFileState || r.Op == storage.OpFileSessionOpen {
		if r.Session != "" {
			return errors.New("session creation cannot name an existing session")
		}
	} else if !validFileCapability(r.Session) {
		return errors.New("invalid file session capability")
	}
	if fileActionRequired(r.Op) {
		if _, err := r.Action.Epoch(); err != nil {
			return err
		}
	} else if r.Action != "" {
		return errors.New("operation does not accept an action")
	}
	reference := false
	var checked error
	switch r.Op {
	case storage.OpFileState, storage.OpFileStatus, storage.OpFileRenew, storage.OpFileSessionClose, storage.OpFileQueryAction, storage.OpFileCancelAction:
	case storage.OpFileSessionOpen:
		if r.Options == nil {
			return errors.New("missing session options")
		}
		expected.Options = r.Options
		checked = r.Options.Check()
	case storage.OpFileReference:
		reference = true
	case storage.OpFileStatNode:
		if r.Node == 0 || r.Observation == nil {
			return errors.New("missing node observation")
		}
		expected.Node = r.Node
		expected.Observation = r.Observation
	case storage.OpFileSetNodeAttr:
		if r.Node == 0 || r.Change == nil {
			return errors.New("missing node change")
		}
		expected.Node = r.Node
		expected.Change = r.Change
		_, checked = r.Change.Storage()
	case storage.OpFileRetain:
		if r.Retain == nil {
			return errors.New("missing retain request")
		}
		expected.Retain = r.Retain
		_, checked = r.Retain.storage()
	case storage.OpFileRetainAt:
		if r.RetainAt == nil {
			return errors.New("missing retain-at request")
		}
		expected.RetainAt = r.RetainAt
		_, checked = r.RetainAt.storage()
	case storage.OpFileCreateAndRetainAt, storage.OpFileReplaceAndRetainAt:
		if r.Create == nil {
			return errors.New("missing create request")
		}
		expected.Create = r.Create
		var native storage.CreateAndRetainRequest
		native, checked = r.Create.storage()
		if checked == nil {
			if r.Op == storage.OpFileCreateAndRetainAt {
				checked = native.CheckCreate()
			} else {
				checked = native.CheckReplace()
			}
		}
	case storage.OpFileResetAndRetainAt:
		if r.Reset == nil {
			return errors.New("missing reset request")
		}
		expected.Reset = r.Reset
		_, checked = r.Reset.storage()
	case storage.OpFileStat:
		reference = true
		if r.Observation == nil {
			return errors.New("missing observation options")
		}
		expected.Observation = r.Observation
	case storage.OpFileCheckObservation:
		reference = true
		if r.Check == nil {
			return errors.New("missing observation condition")
		}
		expected.Check = r.Check
		checked = r.Check.Check()
	case storage.OpFileRead:
		reference = true
		if r.Read == nil || r.Read.Offset < 0 || r.Read.Length < 0 || int64(r.Read.Length) > math.MaxInt64-r.Read.Offset {
			return errors.New("invalid read interval")
		}
		expected.Read = r.Read
	case storage.OpFileWrite:
		reference = true
		if r.Write == nil || r.Write.Offset < 0 || int64(len(r.Write.Data)) > math.MaxInt64-r.Write.Offset {
			return errors.New("invalid write interval")
		}
		expected.Write = r.Write
		checked = r.Write.storage().Check()
	case storage.OpFileTruncate:
		reference = true
		if r.Truncate == nil || r.Truncate.Size < 0 {
			return errors.New("invalid truncate size")
		}
		expected.Truncate = r.Truncate
	case storage.OpFileSetAttr:
		reference = true
		if r.Change == nil {
			return errors.New("missing attribute change")
		}
		expected.Change = r.Change
		_, checked = r.Change.Storage()
	case storage.OpFileSetKind:
		reference = true
		if r.Kind == nil {
			return errors.New("missing kind change")
		}
		expected.Kind = r.Kind
		_, checked = r.Kind.storage()
	case storage.OpFileLookupAt:
		reference = true
		expected.Name = r.Name
		checked = storage.CheckEntryName(r.Name)
	case storage.OpFileListAt:
		reference = true
		if r.List == nil {
			return errors.New("missing directory page request")
		}
		expected.List = r.List
		checked = r.List.Check()
	case storage.OpFileRename:
		reference = true
		if r.Rename == nil {
			return errors.New("missing rename request")
		}
		expected.Rename = r.Rename
		_, checked = r.Rename.storage()
	case storage.OpFileReplaceClaim:
		reference = true
		if r.Claim == nil {
			return errors.New("missing claim")
		}
		expected.Claim = r.Claim
		checked = r.Claim.Check()
	case storage.OpFilePrepareRemoval:
		reference = true
		if r.Prepare == nil {
			return errors.New("missing prepared removal")
		}
		expected.Prepare = r.Prepare
		checked = r.Prepare.Check()
	case storage.OpFileCancelPrepared:
		reference = true
		if r.Intent == nil || *r.Intent == 0 {
			return errors.New("missing removal intent")
		}
		expected.Intent = r.Intent
	case storage.OpFileDrainEntry:
		reference = true
		if r.Drain == nil {
			return errors.New("missing drain request")
		}
		expected.Drain = r.Drain
		checked = r.Drain.Check()
	case storage.OpFileCancelDrain:
		reference = true
		if r.CancelDrain == nil {
			return errors.New("missing drain cancellation")
		}
		expected.CancelDrain = r.CancelDrain
		checked = r.CancelDrain.Check()
	case storage.OpFileRangeSnapshot, storage.OpFileRetireRanges:
		reference = true
		if r.Owner == nil || r.Scope == nil {
			return errors.New("missing range owner or scope")
		}
		expected.Owner = r.Owner
		expected.Scope = r.Scope
		checked = r.Scope.Check()
	case storage.OpFileRetireRangeOwner:
		if r.Owner == nil {
			return errors.New("missing range owner")
		}
		expected.Owner = r.Owner
	case storage.OpFileReplaceRanges:
		reference = true
		if r.Ranges == nil {
			return errors.New("missing range replacement")
		}
		expected.Ranges = r.Ranges
		checked = r.Ranges.Check()
	case storage.OpFileWaitRanges:
		reference = true
		if r.Wait == nil {
			return errors.New("missing range wait")
		}
		expected.Wait = r.Wait
		checked = r.Wait.Check()
	case storage.OpFileSync, storage.OpFileClose:
		reference = true
	default:
		return errors.New("unknown retained file operation")
	}
	if reference {
		if r.Reference == 0 {
			return errors.New("missing file reference")
		}
		expected.Reference = r.Reference
	}
	if !reflect.DeepEqual(r, expected) {
		return errors.New("file request carries unrelated operands")
	}
	return checked
}

func validateFileResponse(req fileRequest, r fileResponse) error {
	expected := fileResponse{Barrier: r.Barrier}
	switch req.Op {
	case storage.OpFileState:
		if r.State == nil || r.State.VolumeIdentity == "" || len(r.State.VolumeIdentity) > MaxLockCapabilityBytes || !utf8.ValidString(r.State.VolumeIdentity) || r.State.RootID == 0 || r.State.MaxEventBytes <= 0 {
			return errors.New("invalid file volume state")
		}
		expected.State = r.State
	case storage.OpFileSessionOpen, storage.OpFileStatus, storage.OpFileRenew:
		if r.Status == nil || r.Status.Epoch == "" || len(r.Status.Epoch) > MaxLockCapabilityBytes || !utf8.ValidString(r.Status.Epoch) || r.Status.ActionEpoch == 0 || r.Status.Revision == 0 || r.Status.Remaining < 0 || r.Status.HistoryRemaining < 0 {
			return errors.New("invalid file session status")
		}
		expected.Status = r.Status
		if req.Op == storage.OpFileSessionOpen {
			if !validFileCapability(r.Session) || r.Status.Retired || r.Status.Fenced || r.Status.Remaining <= 0 {
				return errors.New("session enrollment did not return live ownership")
			}
			expected.Session = r.Session
		}
	case storage.OpFileReference:
		if r.Reference != req.Reference || r.Node == 0 {
			return errors.New("reference resolution changed identity")
		}
		expected.Reference = r.Reference
		expected.Node = r.Node
	case storage.OpFileStatNode, storage.OpFileStat, storage.OpFileCheckObservation, storage.OpFileRead:
		if r.Observation == nil {
			return errors.New("file response has no observation")
		}
		observation, err := r.Observation.storage()
		if err != nil {
			return err
		}
		expected.Observation = r.Observation
		if req.Op == storage.OpFileStatNode && observation.Attr.ID != req.Node {
			return errors.New("node observation changed identity")
		}
		if req.Observation != nil {
			if req.Observation.IncludeLocation != (observation.Location != nil) {
				return errors.New("location response does not match its request")
			}
			if !req.Observation.IncludeLinkTarget && len(observation.LinkTarget) != 0 {
				return errors.New("unsolicited link target")
			}
			if req.Observation.IncludeLinkTarget && observation.Attr.Kind == storage.NodeSymlink && int64(len(observation.LinkTarget)) != observation.Attr.Size {
				return errors.New("link target response is incomplete")
			}
		}

		if req.Op == storage.OpFileCheckObservation {
			if observation.Attr.MetadataRevision != req.Check.MetadataRevision || observation.Attr.DirectoryRevision != req.Check.DirectoryRevision || observation.Location == nil || !fileLocationsEqual(*observation.Location, req.Check.Location) {
				return errors.New("observation confirmation contradicts its requested condition")
			}
		}
		if req.Op == storage.OpFileRead {
			if observation.Attr.Kind != storage.NodeRegular {
				return errors.New("read returned a non-regular node")
			}
			expectedBytes := min(int64(req.Read.Length), max(observation.Attr.Size-req.Read.Offset, 0))
			if int64(len(r.Data)) > expectedBytes || expectedBytes > 0 && len(r.Data) == 0 {
				return errors.New("read exceeds its interval or invents end of file")
			}
			expected.Data = r.Data
		}
	case storage.OpFileLookupAt:
		if r.Lookup == nil {
			return errors.New("lookup response has no captured entry")
		}
		if _, err := r.Lookup.storage(req.Name); err != nil {
			return err
		}
		expected.Lookup = r.Lookup
	case storage.OpFileListAt:
		if r.Page == nil {
			return errors.New("directory response has no page")
		}
		page, err := r.Page.storage()
		if err != nil {
			return err
		}
		if err := page.Check(); err != nil {
			return err
		}
		if len(page.Entries) > req.List.MaxEntries || req.List.Revision != 0 && page.Revision != req.List.Revision {
			return errors.New("directory page exceeds requested bounds or changed revision")
		}
		if !req.List.Cursor.IsZero() && (page.ParentID != req.List.Cursor.ParentID || page.Revision != req.List.Cursor.Revision || len(page.Entries) > 0 && bytes.Compare(page.Entries[0].Name, req.List.Cursor.After) <= 0) {
			return errors.New("directory page does not follow the requested cursor")
		}
		used := storage.DirectoryPageBaseBytes
		for _, entry := range r.Page.Entries {
			charge, err := storage.DirectoryEntryBytes(len(entry.Name), len(entry.Attr.Metadata))
			if err != nil {
				return err
			}
			used += charge
		}
		if used > req.List.MaxBytes {
			return errors.New("directory page exceeds requested byte budget")
		}
		expected.Page = r.Page
	case storage.OpFileRangeSnapshot:
		if r.Ranges == nil {
			return errors.New("range response has no snapshot")
		}
		if r.Ranges.Revision == 0 || r.Ranges.Available < 0 || r.Ranges.OwnerAvailable < 0 || len(r.Ranges.Own) > storage.MaxRangeAcquisitions || len(r.Ranges.Own)+len(r.Ranges.Other) > storage.MaxRangeSnapshotRanges {
			return errors.New("invalid range snapshot")
		}
		for _, a := range r.Ranges.Own {
			if err := a.Check(); err != nil {
				return err
			}
		}
		for _, a := range r.Ranges.Other {
			if a.Owner.Session == "" || len(a.Owner.Session) > storage.MaxFileSessionIDBytes || !utf8.ValidString(a.Owner.Session) {
				return errors.New("invalid range owner session")
			}
			if err := a.Range.Check(); err != nil {
				return err
			}
		}
		expected.Ranges = r.Ranges
	case storage.OpFileSync:
	default:
		if !fileActionRequired(req.Op) || r.Receipt == nil {
			return errors.New("action response has no receipt")
		}
		if err := validateFileReceipt(req, *r.Receipt); err != nil {
			return err
		}
		expected.Receipt = r.Receipt
	}
	if !reflect.DeepEqual(r, expected) {
		return errors.New("file response carries unrelated fields")
	}
	return nil
}
func validateFileReceipt(req fileRequest, r fileReceipt) error {
	if r.Observation != nil && r.Observation.Attr == nil {
		return errors.New("receipt observation has no attributes")
	}
	if r.State == storage.FileActionRetired {
		if (req.Op != storage.OpFileClose && req.Op != storage.OpFileSessionClose) || r.Operation != req.Op || r.Action != "" || r.Reference != req.Reference || r.Effects != 0 || r.HistoryRemaining != 0 || r.Errno != "" || r.Observation != nil || r.Conflict != nil || r.RangeRevision != 0 || r.Removal != (storage.RemovalStatus{}) {
			return errors.New("invalid terminal retirement fact")
		}
		return nil
	}

	if r.Action != req.Action {
		return errors.New("receipt changed the action identity")
	}
	if _, err := r.Action.Epoch(); err != nil {
		return err
	}
	if req.Op != storage.OpFileQueryAction && req.Op != storage.OpFileCancelAction && r.Operation != req.Op {
		return errors.New("receipt changed the operation")
	}
	unknownLookup := (req.Op == storage.OpFileQueryAction || req.Op == storage.OpFileCancelAction) && r.State == storage.FileActionUnknown && r.Operation == ""
	if !unknownLookup && (!fileActionRequired(r.Operation) || r.Operation == storage.OpFileQueryAction || r.Operation == storage.OpFileCancelAction) {
		return errors.New("receipt does not identify a native action")
	}
	if r.State == storage.FileActionUnknown && r.Errno != "EIO" || r.State == storage.FileActionPending && r.Errno != "" || r.State == storage.FileActionNotApplied && r.Errno == "" {
		return errors.New("action state contradicts its error classification")
	}
	if r.State < storage.FileActionPending || r.State > storage.FileActionRetired || r.HistoryRemaining < 0 {
		return errors.New("invalid action state or history")
	}
	const effects = storage.EffectRetained | storage.EffectCreated | storage.EffectContentChanged | storage.EffectMetadataChanged | storage.EffectEntryMoved | storage.EffectEntryDetached | storage.EffectClaimChanged | storage.EffectRangesChanged | storage.EffectPreparedChanged | storage.EffectDrainChanged | storage.EffectReferenceRetired
	if r.Effects & ^effects != 0 {
		return errors.New("receipt has unknown effects")
	}
	if (r.State == storage.FileActionNotApplied || r.State == storage.FileActionUnknown || r.State == storage.FileActionPending || r.State == storage.FileActionRetired) && r.Effects != 0 {
		return errors.New("unconfirmed action claims effects")
	}
	if r.State == storage.FileActionRetired && r.Operation != storage.OpFileClose && r.Operation != storage.OpFileSessionClose {
		return errors.New("retired identity is not a terminal cleanup")
	}
	if req.Reference != 0 && r.Reference != 0 && r.Reference != req.Reference {
		return errors.New("receipt changed the retained reference identity")
	}

	expectedNode := req.Node
	switch req.Op {
	case storage.OpFileRetain:
		if req.Retain != nil {
			expectedNode = req.Retain.NodeID
		}
	case storage.OpFileRetainAt:
		if req.RetainAt != nil {
			expectedNode = req.RetainAt.Target.ExpectedNodeID
		}
	case storage.OpFileResetAndRetainAt:
		if req.Reset != nil {
			expectedNode = req.Reset.Target.ExpectedNodeID
		}
	}
	if expectedNode != 0 && r.Observation != nil && r.Observation.Attr.ID != expectedNode {
		return errors.New("receipt changed the requested node identity")
	}
	if r.State == storage.FileActionCompleted && r.Errno == "" {
		switch r.Operation {
		case storage.OpFileRetain, storage.OpFileRetainAt, storage.OpFileCreateAndRetainAt, storage.OpFileResetAndRetainAt, storage.OpFileReplaceAndRetainAt:
			if r.Effects&storage.EffectRetained == 0 || r.Reference == 0 || r.Observation == nil {
				return errors.New("successful retain has no retained reference observation")
			}
		}
	}
	if r.Effects&storage.EffectRetained != 0 && (r.Reference == 0 || r.Observation == nil) {
		return errors.New("retained effect has no reference observation")
	}
	if err := validateFileConflict(r.Conflict); err != nil {
		return err
	}
	_, err := r.storage()
	return err
}

func validateFileConflict(c *fileConflict) error {
	if c != nil {
		if c.Kind < storage.ConflictRevision || c.Kind > storage.ConflictDeadlock {
			return errors.New("unknown conflict kind")
		}
		if err := c.Claim.Check(); err != nil {
			return err
		}
		if c.Range != nil {
			if c.Range.Owner.Session == "" || len(c.Range.Owner.Session) > storage.MaxFileSessionIDBytes || !utf8.ValidString(c.Range.Owner.Session) {
				return errors.New("invalid conflict owner session")
			}
			if err := c.Range.Range.Check(); err != nil {
				return err
			}
		}
	}
	return nil
}

func fileLocationsEqual(a, b storage.EntryLocation) bool {
	if a.State != b.State || a.RootNodeID != b.RootNodeID || a.NodeID != b.NodeID || len(a.Ancestors) != len(b.Ancestors) {
		return false
	}
	for i, x := range a.Ancestors {
		y := b.Ancestors[i]
		if x.ParentID != y.ParentID || x.DirectoryRevision != y.DirectoryRevision || x.EntryID != y.EntryID || x.NodeID != y.NodeID || !bytes.Equal(x.Name, y.Name) {
			return false
		}
	}
	return true
}
