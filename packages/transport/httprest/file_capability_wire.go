package httprest

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"

	"github.com/codetreker/remote-fs/packages/storage"
)

type AtomicFileOpenerWithBarrier interface {
	storage.AtomicFileOpener
	OpenAtWithBarrier(context.Context, storage.ChildName, storage.OpenAtOptions) (storage.OpenResult, *MutationBarrier, error)
}

type NamespaceAccessWithBarrier interface {
	storage.NamespaceAccess
	MutateNameWithBarrier(context.Context, storage.NameCommand) (storage.NameResult, *MutationBarrier, error)
}

type NodeReferencesWithBarrier interface {
	storage.NodeReferences
	OpenNodeRefWithBarrier(context.Context, uint64, storage.NodeRefOptions) (storage.NodeOpenResult, *MutationBarrier, error)
	OpenChildRefWithBarrier(context.Context, storage.ChildName, storage.NodeRefOptions) (storage.NodeOpenResult, *MutationBarrier, error)
}

type NodeReferenceWithBarrier interface {
	storage.NodeReference
	CloseWithBarrier(context.Context) (*MutationBarrier, error)
	SetAttrWithBarrier(context.Context, storage.AttrChange) (storage.Attr, *MutationBarrier, error)
}

// MetadataAccessWithBarrier exposes the replication point that follows a
// successful node metadata replacement.
type MetadataAccessWithBarrier interface {
	storage.MetadataAccess
	SetMetadataWithBarrier(context.Context, uint64, string, []byte, []byte) (storage.OpaquePayload, *MutationBarrier, error)
}

// ReferenceMetadataAccessWithBarrier is the retained-reference equivalent.
type ReferenceMetadataAccessWithBarrier interface {
	storage.ReferenceMetadataAccess
	SetMetadataWithBarrier(context.Context, string, []byte, []byte) (storage.OpaquePayload, *MutationBarrier, error)
}

type DeleteIntentWithBarrier interface {
	storage.DeleteIntent
	SetPendingUnlinkWithBarrier(context.Context, storage.PendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error)
	ClearPendingUnlinkWithBarrier(context.Context, storage.ClearPendingUnlinkCommand) (storage.ReferenceState, *MutationBarrier, error)
}

type ConditionalFileMutationWithBarrier interface {
	storage.ConditionalFileMutation
	MutateFileWithBarrier(context.Context, storage.FileMutation) (storage.Attr, *MutationBarrier, error)
}

type initialFields struct {
	LinkTarget canonicalBytes             `json:"linkTarget,omitempty"`
	Attr       AttrChange                 `json:"attr"`
	Metadata   map[string]metadataPayload `json:"metadata,omitempty"`
}

type initialState struct {
	OnCreate  initialFields `json:"onCreate"`
	OnReset   initialFields `json:"onReset"`
	OnReplace initialFields `json:"onReplace"`
}

type childCondition struct {
	State            storage.ExpectedChild      `json:"state"`
	NodeID           uint64                     `json:"nodeId"`
	ExpectedMetadata map[string]metadataVersion `json:"expectedMetadata,omitempty"`
}

func childConditionOf(value storage.ChildCondition) childCondition {
	return childCondition{State: value.State, NodeID: value.NodeID, ExpectedMetadata: metadataVersionsOf(value.ExpectedMetadata)}
}

func (value childCondition) storage() storage.ChildCondition {
	return storage.ChildCondition{State: value.State, NodeID: value.NodeID, ExpectedMetadata: metadataVersionsStorage(value.ExpectedMetadata)}
}

type childName struct {
	Parent  storage.DirectoryTarget `json:"parent"`
	RawLeaf canonicalBytes          `json:"rawLeaf"`
}

func childNameOf(value storage.ChildName) *childName {
	return &childName{Parent: value.Parent, RawLeaf: canonicalBytes(bytes.Clone(value.RawLeaf))}
}

func (value childName) storage() storage.ChildName {
	return storage.ChildName{Parent: value.Parent, RawLeaf: bytes.Clone(value.RawLeaf)}
}

type renameTarget struct {
	Parent       storage.DirectoryTarget `json:"parent"`
	ObservedLeaf canonicalBytes          `json:"observedLeaf"`
	Expected     childCondition          `json:"expected"`
	OutputLeaf   canonicalBytes          `json:"outputLeaf"`
}

func renameTargetOf(value *storage.RenameTarget) *renameTarget {
	if value == nil {
		return nil
	}
	return &renameTarget{Parent: value.Parent, ObservedLeaf: canonicalBytes(bytes.Clone(value.ObservedLeaf)), Expected: childConditionOf(value.Expected), OutputLeaf: canonicalBytes(bytes.Clone(value.OutputLeaf))}
}

func (value renameTarget) storage() storage.RenameTarget {
	return storage.RenameTarget{Parent: value.Parent, ObservedLeaf: bytes.Clone(value.ObservedLeaf), Expected: value.Expected.storage(), OutputLeaf: bytes.Clone(value.OutputLeaf)}
}

type closeIntent struct {
	ID               storage.DeleteIntentID     `json:"id"`
	Trigger          storage.CloseTrigger       `json:"trigger"`
	Condition        storage.UnlinkCondition    `json:"condition"`
	ExpectedMetadata map[string]metadataVersion `json:"expectedMetadata,omitempty"`
	Uses             []storage.TargetUse        `json:"uses,omitempty"`
}

func closeIntentOf(value *storage.CloseIntent) *closeIntent {
	if value == nil {
		return nil
	}
	return &closeIntent{ID: value.ID, Trigger: value.Trigger, Condition: value.Condition, ExpectedMetadata: metadataVersionsOf(value.ExpectedMetadata), Uses: value.Uses}
}

func (value closeIntent) storage() storage.CloseIntent {
	return storage.CloseIntent{ID: value.ID, Trigger: value.Trigger, Condition: value.Condition, ExpectedMetadata: metadataVersionsStorage(value.ExpectedMetadata), Uses: value.Uses}
}

func metadataPayloadsOf(values map[string][]byte) map[string]metadataPayload {
	if values == nil {
		return nil
	}
	result := make(map[string]metadataPayload, len(values))
	for name, value := range values {
		result[strings.Clone(name)] = metadataPayload(bytes.Clone(value))
	}
	return result
}

func metadataPayloadsStorage(values map[string]metadataPayload) map[string][]byte {
	if values == nil {
		return nil
	}
	result := make(map[string][]byte, len(values))
	for name, value := range values {
		result[strings.Clone(name)] = bytes.Clone(value)
	}
	return result
}

func metadataVersionsOf(values map[string][]byte) map[string]metadataVersion {
	if values == nil {
		return nil
	}
	result := make(map[string]metadataVersion, len(values))
	for name, value := range values {
		result[strings.Clone(name)] = metadataVersion(bytes.Clone(value))
	}
	return result
}

func metadataVersionsStorage(values map[string]metadataVersion) map[string][]byte {
	if values == nil {
		return nil
	}
	result := make(map[string][]byte, len(values))
	for name, value := range values {
		result[strings.Clone(name)] = bytes.Clone(value)
	}
	return result
}

func initialFieldsOf(value storage.InitialFields) initialFields {
	return initialFields{LinkTarget: canonicalBytes(bytes.Clone(value.LinkTarget)), Attr: *AttrChangeOf(value.Attr), Metadata: metadataPayloadsOf(value.Metadata)}
}

func (value initialFields) storage() storage.InitialFields {
	return storage.InitialFields{LinkTarget: bytes.Clone(value.LinkTarget), Attr: value.Attr.Storage(), Metadata: metadataPayloadsStorage(value.Metadata)}
}

func initialStateOf(value storage.InitialState) initialState {
	return initialState{OnCreate: initialFieldsOf(value.OnCreate), OnReset: initialFieldsOf(value.OnReset), OnReplace: initialFieldsOf(value.OnReplace)}
}

func (value initialState) storage() storage.InitialState {
	return storage.InitialState{OnCreate: value.OnCreate.storage(), OnReset: value.OnReset.storage(), OnReplace: value.OnReplace.storage()}
}

type openAtOptions struct {
	Read        bool                   `json:"read"`
	Write       bool                   `json:"write"`
	Create      bool                   `json:"create"`
	Exclusive   bool                   `json:"exclusive"`
	Target      childCondition         `json:"target"`
	Action      storage.FileActionID   `json:"action"`
	Use         storage.UseClaim       `json:"use"`
	Existing    storage.ExistingEffect `json:"existing"`
	Initial     initialState           `json:"initial"`
	CloseIntent *closeIntent           `json:"closeIntent,omitempty"`
}

func openAtOptionsOf(value storage.OpenAtOptions) *openAtOptions {
	return &openAtOptions{Read: value.Read, Write: value.Write, Create: value.Create, Exclusive: value.Exclusive, Target: childConditionOf(value.Target), Action: value.Action, Use: value.Use, Existing: value.Existing, Initial: initialStateOf(value.Initial), CloseIntent: closeIntentOf(value.CloseIntent)}
}

func (value openAtOptions) storage() storage.OpenAtOptions {
	result := storage.OpenAtOptions{Read: value.Read, Write: value.Write, Create: value.Create, Exclusive: value.Exclusive, Target: value.Target.storage(), Action: value.Action, Use: value.Use, Existing: value.Existing, Initial: value.Initial.storage()}
	if value.CloseIntent != nil {
		intent := value.CloseIntent.storage()
		result.CloseIntent = &intent
	}
	return result
}

type nodeRefOptions struct {
	Kind           storage.NodeKind            `json:"kind"`
	Target         childCondition              `json:"target"`
	Action         storage.FileActionID        `json:"action"`
	Use            storage.UseClaim            `json:"use"`
	MetadataAccess storage.MetadataPermissions `json:"metadataAccess"`
	Create         bool                        `json:"create"`
	Exclusive      bool                        `json:"exclusive"`
	Initial        initialState                `json:"initial"`
	CloseIntent    *closeIntent                `json:"closeIntent,omitempty"`
}

func nodeRefOptionsOf(value storage.NodeRefOptions) *nodeRefOptions {
	return &nodeRefOptions{Kind: value.Kind, Target: childConditionOf(value.Target), Action: value.Action, Use: value.Use, MetadataAccess: value.MetadataAccess, Create: value.Create, Exclusive: value.Exclusive, Initial: initialStateOf(value.InitialState), CloseIntent: closeIntentOf(value.CloseIntent)}
}

func (value nodeRefOptions) storage() storage.NodeRefOptions {
	result := storage.NodeRefOptions{Kind: value.Kind, Target: value.Target.storage(), Action: value.Action, Use: value.Use, MetadataAccess: value.MetadataAccess, Create: value.Create, Exclusive: value.Exclusive, InitialState: value.Initial.storage()}
	if value.CloseIntent != nil {
		intent := value.CloseIntent.storage()
		result.CloseIntent = &intent
	}
	return result
}

type nameCommand struct {
	Kind        storage.NameOperation `json:"kind"`
	Action      storage.FileActionID  `json:"action"`
	Name        childName             `json:"name"`
	Target      childCondition        `json:"target"`
	Destination *renameTarget         `json:"destination,omitempty"`
	Initial     initialFields         `json:"initial"`
	Uses        []storage.TargetUse   `json:"uses,omitempty"`
}

func nameCommandOf(value storage.NameCommand) *nameCommand {
	return &nameCommand{Kind: value.Kind, Action: value.Action, Name: *childNameOf(value.Name), Target: childConditionOf(value.Target), Destination: renameTargetOf(value.Destination), Initial: initialFieldsOf(value.Initial), Uses: value.Uses}
}

func (value nameCommand) storage() storage.NameCommand {
	result := storage.NameCommand{Kind: value.Kind, Action: value.Action, Name: value.Name.storage(), Target: value.Target.storage(), Initial: value.Initial.storage(), Uses: value.Uses}
	if value.Destination != nil {
		destination := value.Destination.storage()
		result.Destination = &destination
	}
	return result
}

func nameResultNeedsAttr(operation storage.NameOperation) bool {
	switch operation {
	case storage.NameCreate, storage.NameMkdir, storage.NameRename, storage.NameSymlink:
		return true
	default:
		return false
	}
}

// metadataUpdate is request-side CAS state. Its empty version requires absence.
type metadataUpdate struct {
	ExpectedVersion metadataVersion `json:"version"`
	Data            metadataPayload `json:"data"`
}

func metadataUpdatesOf(values map[string]storage.OpaquePayload) map[string]metadataUpdate {
	if values == nil {
		return nil
	}
	result := make(map[string]metadataUpdate, len(values))
	for name, value := range values {
		result[strings.Clone(name)] = metadataUpdate{ExpectedVersion: metadataVersion(bytes.Clone(value.Version)), Data: metadataPayload(bytes.Clone(value.Data))}
	}
	return result
}

func metadataUpdatesStorage(values map[string]metadataUpdate) map[string]storage.OpaquePayload {
	if values == nil {
		return nil
	}
	result := make(map[string]storage.OpaquePayload, len(values))
	for name, value := range values {
		result[strings.Clone(name)] = storage.OpaquePayload{Version: bytes.Clone(value.ExpectedVersion), Data: bytes.Clone(value.Data)}
	}
	return result
}

type fileMutationOptions struct {
	Action           storage.FileActionID       `json:"action"`
	Offset           int64                      `json:"offset"`
	Data             canonicalBytes             `json:"data,omitempty"`
	ExpectedSize     *int64                     `json:"expectedSize,omitempty"`
	ExpectedMetadata map[string]metadataVersion `json:"expectedMetadata,omitempty"`
	Kind             storage.FileMutationKind   `json:"kind"`
	Size             int64                      `json:"size"`
	Attr             AttrChange                 `json:"attr"`
	Metadata         map[string]metadataUpdate  `json:"metadata,omitempty"`
	Uses             []storage.TargetUse        `json:"uses,omitempty"`
}

func fileMutationOf(value storage.FileMutation) *fileMutationOptions {
	return &fileMutationOptions{Action: value.Action, Offset: value.Offset, Data: canonicalBytes(bytes.Clone(value.Data)), ExpectedSize: value.ExpectedSize, ExpectedMetadata: metadataVersionsOf(value.ExpectedMetadata), Kind: value.Kind, Size: value.Size, Attr: *AttrChangeOf(value.Attr), Metadata: metadataUpdatesOf(value.Metadata), Uses: value.Uses}
}

func (value fileMutationOptions) storage() storage.FileMutation {
	return storage.FileMutation{Action: value.Action, Offset: value.Offset, Data: bytes.Clone(value.Data), ExpectedSize: value.ExpectedSize, ExpectedMetadata: metadataVersionsStorage(value.ExpectedMetadata), Kind: value.Kind, Size: value.Size, Attr: value.Attr.Storage(), Metadata: metadataUpdatesStorage(value.Metadata), Uses: value.Uses}
}

type pendingUnlinkCommand struct {
	Action           storage.FileActionID       `json:"action"`
	Condition        storage.UnlinkCondition    `json:"condition"`
	ExpectedMetadata map[string]metadataVersion `json:"expectedMetadata,omitempty"`
	Uses             []storage.TargetUse        `json:"uses,omitempty"`
}

func pendingUnlinkCommandOf(value storage.PendingUnlinkCommand) *pendingUnlinkCommand {
	return &pendingUnlinkCommand{Action: value.Action, Condition: value.Condition, ExpectedMetadata: metadataVersionsOf(value.ExpectedMetadata), Uses: value.Uses}
}

func (value pendingUnlinkCommand) storage() storage.PendingUnlinkCommand {
	return storage.PendingUnlinkCommand{Action: value.Action, Condition: value.Condition, ExpectedMetadata: metadataVersionsStorage(value.ExpectedMetadata), Uses: value.Uses}
}

type clearPendingUnlinkCommand struct {
	Action     storage.FileActionID `json:"action"`
	Generation metadataVersion      `json:"generation"`
	Uses       []storage.TargetUse  `json:"uses,omitempty"`
}

func clearPendingUnlinkCommandOf(value storage.ClearPendingUnlinkCommand) *clearPendingUnlinkCommand {
	return &clearPendingUnlinkCommand{Action: value.Action, Generation: metadataVersion(bytes.Clone(value.Generation)), Uses: value.Uses}
}

func (value clearPendingUnlinkCommand) storage() storage.ClearPendingUnlinkCommand {
	return storage.ClearPendingUnlinkCommand{Action: value.Action, Generation: bytes.Clone(value.Generation), Uses: value.Uses}
}

type referenceState struct {
	LinkTarget        canonicalBytes  `json:"linkTarget,omitempty"`
	Attr              *Attr           `json:"attr"`
	Detached          bool            `json:"detached"`
	PendingUnlink     bool            `json:"pendingUnlink"`
	PendingGeneration metadataVersion `json:"pendingGeneration"`
}

type deleteIntentStatus struct {
	ID      storage.DeleteIntentID      `json:"id"`
	NodeID  uint64                      `json:"nodeId"`
	Outcome storage.DeleteIntentOutcome `json:"outcome"`
	Failure string                      `json:"failure,omitempty"`
}

type acknowledgeDeleteIntentCommand struct {
	Action storage.FileActionID   `json:"action"`
	Intent storage.DeleteIntentID `json:"intent"`
}

func acknowledgeDeleteIntentCommandOf(value storage.AcknowledgeDeleteIntentCommand) *acknowledgeDeleteIntentCommand {
	return &acknowledgeDeleteIntentCommand{Action: value.Action, Intent: value.Intent}
}

func (value acknowledgeDeleteIntentCommand) storage() storage.AcknowledgeDeleteIntentCommand {
	return storage.AcknowledgeDeleteIntentCommand{Action: value.Action, Intent: value.Intent}
}

func deleteIntentStatusOf(value storage.DeleteIntentStatus) (*deleteIntentStatus, error) {
	result := &deleteIntentStatus{ID: value.ID, NodeID: value.NodeID, Outcome: value.Outcome}
	if value.Failure != 0 {
		name, ok := storage.ErrnoName(value.Failure)
		if !ok {
			return nil, errors.New("delete intent status carries an unknown failure")
		}
		result.Failure = name
	}
	return result, nil
}

func (value deleteIntentStatus) storage() (storage.DeleteIntentStatus, error) {
	result := storage.DeleteIntentStatus{ID: value.ID, NodeID: value.NodeID, Outcome: value.Outcome}
	if value.Failure != "" {
		errno, ok := storage.ErrnoByName(value.Failure)
		if !ok {
			return storage.DeleteIntentStatus{}, errors.New("delete intent status carries an unknown failure")
		}
		result.Failure = errno
	}
	if err := result.Check(); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	return result, nil
}

func referenceStateOf(value storage.ReferenceState) *referenceState {
	return &referenceState{LinkTarget: canonicalBytes(bytes.Clone(value.LinkTarget)), Attr: AttrOf(value.Attr), Detached: value.Detached, PendingUnlink: value.PendingUnlink, PendingGeneration: metadataVersion(bytes.Clone(value.PendingGeneration))}
}

func (value referenceState) storage() storage.ReferenceState {
	return storage.ReferenceState{LinkTarget: bytes.Clone(value.LinkTarget), Attr: value.Attr.Storage(), Detached: value.Detached, PendingUnlink: value.PendingUnlink, PendingGeneration: bytes.Clone(value.PendingGeneration)}
}

// metadataVersion is request-side CAS state. Empty means the namespace must be
// absent; response-side OpaquePayload deliberately rejects an empty version.
type metadataVersion []byte
type metadataPayload []byte
type canonicalBytes []byte

func encodeCanonicalBytes(value []byte) ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString(value))
}

func (v metadataVersion) MarshalJSON() ([]byte, error) { return encodeCanonicalBytes(v) }
func (p metadataPayload) MarshalJSON() ([]byte, error) { return encodeCanonicalBytes(p) }
func (v canonicalBytes) MarshalJSON() ([]byte, error)  { return encodeCanonicalBytes(v) }

func decodeCanonicalBytes(data []byte, maximum int, field string) ([]byte, error) {
	if string(data) == "null" {
		return nil, errors.New(field + " cannot be null")
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return nil, err
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, errors.New(field + " requires canonical base64")
	}
	if len(decoded) > maximum {
		return nil, errors.New(field + " exceeds its bound")
	}
	if len(decoded) == 0 {
		return nil, nil
	}
	return decoded, nil
}

func (v *metadataVersion) UnmarshalJSON(data []byte) error {
	decoded, err := decodeCanonicalBytes(data, storage.MaxObservationTokenBytes, "metadata version")
	if err != nil {
		return err
	}
	*v = metadataVersion(decoded)
	return nil
}

func (p *metadataPayload) UnmarshalJSON(data []byte) error {
	decoded, err := decodeCanonicalBytes(data, storage.MaxMetadataValueBytes, "metadata payload")
	if err != nil {
		return err
	}
	*p = metadataPayload(decoded)
	return nil
}

func (v *canonicalBytes) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		return errors.New("bytes cannot be null")
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return err
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return errors.New("bytes require canonical base64")
	}
	*v = canonicalBytes(decoded)
	return nil
}
