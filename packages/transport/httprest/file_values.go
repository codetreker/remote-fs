package httprest

import (
	"encoding/json"
	"errors"
	"github.com/codetreker/remote-fs/packages/storage"
	"reflect"
	"syscall"
	"time"
)

func wireBytes(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}
func optionalTimeOf(t *time.Time) *Time {
	if t == nil {
		return nil
	}
	value := TimeOf(*t)
	return &value
}
func optionalStorageTime(t *Time) *time.Time {
	if t == nil {
		return nil
	}
	value := t.Time()
	return &value
}

func fileInitialOf(n storage.NodeInitial) (fileInitial, error) {
	metadata, err := storage.EncodeMetadata(n.Metadata)
	if err != nil {
		return fileInitial{}, err
	}
	return fileInitial{Kind: n.Kind, Metadata: metadata, LinkTarget: wireBytes(n.LinkTarget), AccessTime: optionalTimeOf(n.AccessTime), ModTime: optionalTimeOf(n.ModTime), CreationTime: optionalTimeOf(n.CreationTime), ChangeTime: optionalTimeOf(n.ChangeTime)}, nil
}
func (n fileInitial) storage() (storage.NodeInitial, error) {
	for _, instant := range []*Time{n.AccessTime, n.ModTime, n.CreationTime, n.ChangeTime} {
		if instant != nil && (instant.Nanos < 0 || instant.Nanos >= 1e9) {
			return storage.NodeInitial{}, errors.New("initial node instant has invalid nanoseconds")
		}
	}
	metadata, err := storage.DecodeMetadata(n.Metadata)
	if err != nil {
		return storage.NodeInitial{}, err
	}
	value := storage.NodeInitial{Kind: n.Kind, Metadata: metadata, LinkTarget: n.LinkTarget, AccessTime: optionalStorageTime(n.AccessTime), ModTime: optionalStorageTime(n.ModTime), CreationTime: optionalStorageTime(n.CreationTime), ChangeTime: optionalStorageTime(n.ChangeTime)}
	return value, value.Check()
}
func fileCreateOf(r storage.CreateAndRetainRequest) (*fileCreateRequest, error) {
	initial, err := fileInitialOf(r.Initial)
	if err != nil {
		return nil, err
	}
	return &fileCreateRequest{Target: fileTargetOf(r.Target), Initial: initial, Claim: r.Claim, Prepared: r.Prepared}, nil
}
func (r fileCreateRequest) storage() (storage.CreateAndRetainRequest, error) {
	initial, err := r.Initial.storage()
	if err != nil {
		return storage.CreateAndRetainRequest{}, err
	}
	value := storage.CreateAndRetainRequest{Target: r.Target.storage(), Initial: initial, Claim: r.Claim, Prepared: r.Prepared}
	return value, value.Check()
}
func fileResetOf(r storage.ResetAndRetainRequest) (*fileResetRequest, error) {
	change, err := AttrChangeOf(r.Change)
	if err != nil {
		return nil, err
	}
	return &fileResetRequest{Target: fileTargetOf(r.Target), ExpectedRevision: r.ExpectedRevision, Change: *change, Claim: r.Claim, Prepared: r.Prepared}, nil
}
func (r fileResetRequest) storage() (storage.ResetAndRetainRequest, error) {
	change, err := r.Change.Storage()
	if err != nil {
		return storage.ResetAndRetainRequest{}, err
	}
	value := storage.ResetAndRetainRequest{Target: r.Target.storage(), ExpectedRevision: r.ExpectedRevision, Change: change, Claim: r.Claim, Prepared: r.Prepared}
	return value, value.Check()
}
func fileKindOf(r storage.SetKindRequest) (*fileKindRequest, error) {
	metadata, err := storage.EncodeMetadata(r.Metadata)
	if err != nil {
		return nil, err
	}
	return &fileKindRequest{Owner: r.Owner, Witness: r.Witness, ExpectedRevision: r.ExpectedRevision, Kind: r.Kind, Metadata: metadata, LinkTarget: wireBytes(r.LinkTarget)}, nil
}
func (r fileKindRequest) storage() (storage.SetKindRequest, error) {
	metadata, err := storage.DecodeMetadata(r.Metadata)
	if err != nil {
		return storage.SetKindRequest{}, err
	}
	value := storage.SetKindRequest{Owner: r.Owner, Witness: r.Witness, ExpectedRevision: r.ExpectedRevision, Kind: r.Kind, Metadata: metadata, LinkTarget: r.LinkTarget}
	return value, value.Check()
}
func fileObservationOf(o storage.FileObservation) (*fileObservation, error) {
	attr, err := AttrOf(o.Attr)
	if err != nil {
		return nil, err
	}
	return &fileObservation{Attr: attr, Removal: o.Removal, Location: o.Location, LinkTarget: wireBytes(o.LinkTarget)}, nil
}
func (o fileObservation) storage() (storage.FileObservation, error) {
	if err := o.Removal.Check(); err != nil {
		return storage.FileObservation{}, err
	}
	if o.Attr == nil {
		return storage.FileObservation{}, errors.New("file observation has no attributes")
	}
	attr, err := o.Attr.Storage()
	if err != nil {
		return storage.FileObservation{}, err
	}
	value := storage.FileObservation{Attr: attr, Removal: o.Removal, Location: o.Location, LinkTarget: o.LinkTarget}
	if o.Location != nil {
		if err := o.Location.Check(); err != nil {
			return storage.FileObservation{}, err
		}
		if o.Location.NodeID != attr.ID {
			return storage.FileObservation{}, errors.New("file location changed node identity")
		}
	}
	if len(o.LinkTarget) > storage.MaxLinkTargetBytes || len(o.LinkTarget) > 0 && (attr.Kind != storage.NodeSymlink || int64(len(o.LinkTarget)) != attr.Size) {
		return storage.FileObservation{}, errors.New("invalid file link target")
	}
	return value, nil
}
func fileReceiptOf(r storage.FileActionReceipt) (*fileReceipt, error) {
	var errno string
	if r.Errno != 0 {
		var ok bool
		errno, ok = storage.ErrnoName(r.Errno)
		if !ok {
			return nil, errors.New("file receipt carries unnamed errno")
		}
	}
	var observation *fileObservation
	if r.Observation.Attr.ID != 0 {
		var err error
		observation, err = fileObservationOf(r.Observation)
		if err != nil {
			return nil, err
		}
	}
	return &fileReceipt{Action: r.Action, Operation: r.Operation, State: r.State, Effects: r.Effects, Reference: r.Reference, Observation: observation, RangeRevision: r.RangeRevision, Removal: r.Removal, Errno: errno, Conflict: fileConflictOf(r.Conflict), HistoryRemaining: int64(r.HistoryRemaining)}, nil
}
func (r fileReceipt) storage() (storage.FileActionReceipt, error) {
	if err := r.Removal.Check(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	var errno syscall.Errno
	if r.Errno != "" {
		var ok bool
		errno, ok = storage.ErrnoByName(r.Errno)
		if !ok {
			return storage.FileActionReceipt{}, errors.New("file receipt carries unknown errno")
		}
	}
	value := storage.FileActionReceipt{Action: r.Action, Operation: r.Operation, State: r.State, Effects: r.Effects, Reference: r.Reference, RangeRevision: r.RangeRevision, Removal: r.Removal, Errno: errno, Conflict: r.Conflict.storage(), HistoryRemaining: time.Duration(r.HistoryRemaining)}
	if r.Observation != nil {
		var err error
		value.Observation, err = r.Observation.storage()
		if err != nil {
			return storage.FileActionReceipt{}, err
		}
	}
	return value, nil
}
func fileDirectoryPageOf(p storage.DirectoryPage) (*fileDirectoryPage, error) {
	entries := make([]fileDirectoryEntry, 0, len(p.Entries))
	for _, e := range p.Entries {
		attr, err := AttrOf(e.Attr)
		if err != nil {
			return nil, err
		}
		entries = append(entries, fileDirectoryEntry{EntryID: e.EntryID, Name: wireBytes(e.Name), Attr: attr})
	}
	next := p.Next
	next.After = wireBytes(next.After)
	return &fileDirectoryPage{ParentID: p.ParentID, Revision: p.Revision, Entries: entries, Next: next, Done: p.Done}, nil
}
func (p fileDirectoryPage) storage() (storage.DirectoryPage, error) {
	entries := make([]storage.DirectoryEntry, 0, len(p.Entries))
	for _, e := range p.Entries {
		if e.Attr == nil {
			return storage.DirectoryPage{}, errors.New("directory entry has no attributes")
		}
		attr, err := e.Attr.Storage()
		if err != nil {
			return storage.DirectoryPage{}, err
		}
		entries = append(entries, storage.DirectoryEntry{EntryID: e.EntryID, Name: e.Name, Attr: attr})
	}
	return storage.DirectoryPage{ParentID: p.ParentID, Revision: p.Revision, Entries: entries, Next: p.Next, Done: p.Done}, nil
}

func fileRetainOf(r storage.RetainRequest) *fileRetainRequest {
	return &fileRetainRequest{NodeID: r.NodeID, ExpectedMetadataRevision: r.ExpectedMetadataRevision, Claim: r.Claim, Witness: r.Witness, Prepared: r.Prepared}
}
func (r fileRetainRequest) storage() (storage.RetainRequest, error) {
	value := storage.RetainRequest{NodeID: r.NodeID, ExpectedMetadataRevision: r.ExpectedMetadataRevision, Claim: r.Claim, Witness: r.Witness, Prepared: r.Prepared}
	return value, value.Check()
}
func fileRetainAtOf(r storage.RetainAtRequest) *fileRetainAtRequest {
	return &fileRetainAtRequest{Target: fileTargetOf(r.Target), Claim: r.Claim, Prepared: r.Prepared}
}
func (r fileRetainAtRequest) storage() (storage.RetainAtRequest, error) {
	value := storage.RetainAtRequest{Target: r.Target.storage(), Claim: r.Claim, Prepared: r.Prepared}
	return value, value.Check()
}
func (r fileReadRequest) storage() storage.FileReadRequest {
	return storage.FileReadRequest{Offset: r.Offset, Length: r.Length, Owner: r.Owner}
}
func (r fileWriteRequest) storage() storage.FileWriteRequest {
	return storage.FileWriteRequest{Offset: r.Offset, Data: r.Data, Owner: r.Owner, ExpectedSize: r.ExpectedSize}
}
func (r fileTruncateRequest) storage() storage.FileTruncateRequest {
	return storage.FileTruncateRequest{Size: r.Size, Owner: r.Owner}
}
func fileConflictOf(c *storage.FileConflict) *fileConflict {
	if c == nil {
		return nil
	}
	return &fileConflict{Kind: c.Kind, NodeID: c.NodeID, EntryID: c.EntryID, Revision: c.Revision, Claim: c.Claim, Range: c.Range}
}
func (c *fileConflict) storage() *storage.FileConflict {
	if c == nil {
		return nil
	}
	return &storage.FileConflict{Kind: c.Kind, NodeID: c.NodeID, EntryID: c.EntryID, Revision: c.Revision, Claim: c.Claim, Range: c.Range}
}

// Empty native byte and sequence values have one wire representation. Optional
// pointers retain their absence; caller-owned slices are never rewritten.
func marshalFileJSON(value any) ([]byte, error) {
	return json.Marshal(fileJSONValue(reflect.ValueOf(value)).Interface())
}
func fileJSONValue(value reflect.Value) reflect.Value {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() {
			return value
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(fileJSONValue(value.Elem()))
		return out
	case reflect.Struct:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.NumField(); i++ {
			out.Field(i).Set(fileJSONValue(value.Field(i)))
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return reflect.MakeSlice(value.Type(), 0, 0)
		}
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return value
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(fileJSONValue(value.Index(i)))
		}
		return out
	default:
		return value
	}
}

func fileTargetOf(t storage.EntryTarget) fileEntryTarget {
	return fileEntryTarget{Parent: t.Parent, ParentID: t.ParentID, Name: wireBytes(t.Name), DirectoryRevision: t.DirectoryRevision, ExpectedEntryID: t.ExpectedEntryID, ExpectedNodeID: t.ExpectedNodeID, ExpectedMetadataRevision: t.ExpectedMetadataRevision, Witness: t.Witness}
}
func (t fileEntryTarget) storage() storage.EntryTarget {
	return storage.EntryTarget{Parent: t.Parent, ParentID: t.ParentID, Name: t.Name, DirectoryRevision: t.DirectoryRevision, ExpectedEntryID: t.ExpectedEntryID, ExpectedNodeID: t.ExpectedNodeID, ExpectedMetadataRevision: t.ExpectedMetadataRevision, Witness: t.Witness}
}
func fileRenameOf(r storage.RenameRequest) *fileRenameRequest {
	return &fileRenameRequest{NewName: r.NewName, Source: fileTargetOf(r.Source), Destination: fileTargetOf(r.Destination)}
}
func (r fileRenameRequest) storage() (storage.RenameRequest, error) {
	value := storage.RenameRequest{NewName: r.NewName, Source: r.Source.storage(), Destination: r.Destination.storage()}
	return value, value.Check()
}
func fileEntryLookupOf(l storage.EntryLookup) (*fileEntryLookup, error) {
	if err := l.Check(l.Name); err != nil {
		return nil, err
	}
	var attr *Attr
	if l.Found {
		var err error
		attr, err = AttrOf(l.Attr)
		if err != nil {
			return nil, err
		}
	}
	return &fileEntryLookup{ParentID: l.ParentID, DirectoryRevision: l.DirectoryRevision, Name: wireBytes(l.Name), Found: l.Found, EntryID: l.EntryID, Attr: attr}, nil
}
func (l fileEntryLookup) storage(name []byte) (storage.EntryLookup, error) {
	result := storage.EntryLookup{ParentID: l.ParentID, DirectoryRevision: l.DirectoryRevision, Name: l.Name, Found: l.Found, EntryID: l.EntryID}
	if l.Found {
		if l.Attr == nil {
			return storage.EntryLookup{}, errors.New("present lookup has no attributes")
		}
		var err error
		result.Attr, err = l.Attr.Storage()
		if err != nil {
			return storage.EntryLookup{}, err
		}
	} else if l.Attr != nil {
		return storage.EntryLookup{}, errors.New("missing lookup carries node attributes")
	}
	return result, result.Check(name)
}
