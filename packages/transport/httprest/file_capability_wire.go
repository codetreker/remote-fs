package httprest

import (
	"context"
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
type MetadataAccessWithBarrier interface {
	storage.MetadataAccess
	SetMetadataWithBarrier(context.Context, uint64, string, []byte, []byte) (storage.OpaquePayload, *MutationBarrier, error)
}
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
	LinkTarget []byte            `json:"linkTarget,omitempty"`
	Attr       AttrChange        `json:"attr"`
	Metadata   map[string][]byte `json:"metadata,omitempty"`
}
type initialState struct {
	OnCreate  initialFields `json:"onCreate"`
	OnReset   initialFields `json:"onReset"`
	OnReplace initialFields `json:"onReplace"`
}

func initialFieldsOf(v storage.InitialFields) initialFields {
	return initialFields{LinkTarget: v.LinkTarget, Attr: *AttrChangeOf(v.Attr), Metadata: initialMetadataOf(v.Metadata)}
}
func initialMetadataOf(values map[string][]byte) map[string][]byte {
	if values == nil {
		return nil
	}
	result := make(map[string][]byte, len(values))
	for key, value := range values {
		result[key] = append([]byte{}, value...)
	}
	return result
}
func (v initialFields) storage() storage.InitialFields {
	return storage.InitialFields{LinkTarget: v.LinkTarget, Attr: v.Attr.Storage(), Metadata: v.Metadata}
}
func initialStateOf(v storage.InitialState) initialState {
	return initialState{OnCreate: initialFieldsOf(v.OnCreate), OnReset: initialFieldsOf(v.OnReset), OnReplace: initialFieldsOf(v.OnReplace)}
}
func (v initialState) storage() storage.InitialState {
	return storage.InitialState{OnCreate: v.OnCreate.storage(), OnReset: v.OnReset.storage(), OnReplace: v.OnReplace.storage()}
}

type openAtOptions struct {
	Read        bool                     `json:"read"`
	Write       bool                     `json:"write"`
	Create      bool                     `json:"create"`
	Exclusive   bool                     `json:"exclusive"`
	Target      storage.ChildCondition   `json:"target"`
	Guards      *storage.NamespaceGuards `json:"guards,omitempty"`
	Use         storage.UseClaim         `json:"use"`
	Existing    storage.ExistingEffect   `json:"existing"`
	Initial     initialState             `json:"initial"`
	CloseIntent *storage.CloseIntent     `json:"closeIntent,omitempty"`
}

func openAtOptionsOf(v storage.OpenAtOptions) *openAtOptions {
	return &openAtOptions{Read: v.Read, Write: v.Write, Create: v.Create, Exclusive: v.Exclusive, Target: v.Target, Guards: v.Guards, Use: v.Use, Existing: v.Existing, Initial: initialStateOf(v.Initial), CloseIntent: v.CloseIntent}
}
func (v openAtOptions) storage() storage.OpenAtOptions {
	return storage.OpenAtOptions{Read: v.Read, Write: v.Write, Create: v.Create, Exclusive: v.Exclusive, Target: v.Target, Guards: v.Guards, Use: v.Use, Existing: v.Existing, Initial: v.Initial.storage(), CloseIntent: v.CloseIntent}
}

type nodeRefOptions struct {
	Kind           storage.NodeKind            `json:"kind"`
	Target         storage.ChildCondition      `json:"target"`
	Guards         *storage.NamespaceGuards    `json:"guards,omitempty"`
	Use            storage.UseClaim            `json:"use"`
	MetadataAccess storage.MetadataPermissions `json:"metadataAccess"`
	Create         bool                        `json:"create"`
	Exclusive      bool                        `json:"exclusive"`
	Initial        initialState                `json:"initial"`
	CloseIntent    *storage.CloseIntent        `json:"closeIntent,omitempty"`
}

func nodeRefOptionsOf(v storage.NodeRefOptions) *nodeRefOptions {
	return &nodeRefOptions{Kind: v.Kind, Target: v.Target, Guards: v.Guards, Use: v.Use, MetadataAccess: v.MetadataAccess, Create: v.Create, Exclusive: v.Exclusive, Initial: initialStateOf(v.InitialState), CloseIntent: v.CloseIntent}
}
func (v nodeRefOptions) storage() storage.NodeRefOptions {
	return storage.NodeRefOptions{Kind: v.Kind, Target: v.Target, Guards: v.Guards, Use: v.Use, MetadataAccess: v.MetadataAccess, Create: v.Create, Exclusive: v.Exclusive, InitialState: v.Initial.storage(), CloseIntent: v.CloseIntent}
}

type nameCommand struct {
	Kind        storage.NameOperation    `json:"kind"`
	Name        storage.ChildName        `json:"name"`
	Target      storage.ChildCondition   `json:"target"`
	Destination *storage.RenameTarget    `json:"destination,omitempty"`
	Initial     initialFields            `json:"initial"`
	Guards      *storage.NamespaceGuards `json:"guards,omitempty"`
	Uses        []storage.TargetUse      `json:"uses,omitempty"`
}

func nameCommandOf(v storage.NameCommand) *nameCommand {
	return &nameCommand{Kind: v.Kind, Name: v.Name, Target: v.Target, Destination: v.Destination, Initial: initialFieldsOf(v.Initial), Guards: v.Guards, Uses: v.Uses}
}
func (v nameCommand) storage() storage.NameCommand {
	return storage.NameCommand{Kind: v.Kind, Name: v.Name, Target: v.Target, Destination: v.Destination, Initial: v.Initial.storage(), Guards: v.Guards, Uses: v.Uses}
}

type fileMutationOptions struct {
	Offset           int64                    `json:"offset"`
	Data             []byte                   `json:"data,omitempty"`
	ExpectedSize     *int64                   `json:"expectedSize,omitempty"`
	ExpectedMetadata map[string][]byte        `json:"expectedMetadata,omitempty"`
	Kind             storage.FileMutationKind `json:"kind"`
	Size             int64                    `json:"size"`
	Attr             AttrChange               `json:"attr"`
	Metadata         map[string]OpaquePayload `json:"metadata,omitempty"`
	Guards           *storage.NamespaceGuards `json:"guards,omitempty"`
	Uses             []storage.TargetUse      `json:"uses,omitempty"`
}

func fileMutationOf(v storage.FileMutation) *fileMutationOptions {
	return &fileMutationOptions{Offset: v.Offset, Data: v.Data, ExpectedSize: v.ExpectedSize, ExpectedMetadata: initialMetadataOf(v.ExpectedMetadata), Kind: v.Kind, Size: v.Size, Attr: *AttrChangeOf(v.Attr), Metadata: metadataOf(v.Metadata), Guards: v.Guards, Uses: v.Uses}
}
func (v fileMutationOptions) storage() storage.FileMutation {
	return storage.FileMutation{Offset: v.Offset, Data: v.Data, ExpectedSize: v.ExpectedSize, ExpectedMetadata: v.ExpectedMetadata, Kind: v.Kind, Size: v.Size, Attr: v.Attr.Storage(), Metadata: metadataStorage(v.Metadata), Guards: v.Guards, Uses: v.Uses}
}

type observedDirectory struct {
	Observation storage.DirectoryObservation `json:"observation"`
	Entries     []observedEntry              `json:"entries"`
}
type observedEntry struct {
	RawLeaf []byte `json:"rawLeaf"`
	Attr    *Attr  `json:"attr"`
}

func observedDirectoryOf(v storage.ObservedDirectory) *observedDirectory {
	out := &observedDirectory{Observation: v.Observation, Entries: make([]observedEntry, 0, len(v.Entries))}
	for _, e := range v.Entries {
		out.Entries = append(out.Entries, observedEntry{RawLeaf: e.RawLeaf, Attr: AttrOf(e.Attr)})
	}
	return out
}
func (v observedDirectory) storage() storage.ObservedDirectory {
	out := storage.ObservedDirectory{Observation: v.Observation, Entries: make([]storage.ObservedEntry, 0, len(v.Entries))}
	for _, e := range v.Entries {
		out.Entries = append(out.Entries, storage.ObservedEntry{RawLeaf: e.RawLeaf, Attr: e.Attr.Storage()})
	}
	return out
}

type referenceState struct {
	LinkTarget        []byte `json:"linkTarget,omitempty"`
	Attr              *Attr  `json:"attr"`
	Detached          bool   `json:"detached"`
	PendingUnlink     bool   `json:"pendingUnlink"`
	PendingGeneration []byte `json:"pendingGeneration"`
}

func referenceStateOf(v storage.ReferenceState) *referenceState {
	return &referenceState{LinkTarget: v.LinkTarget, Attr: AttrOf(v.Attr), Detached: v.Detached, PendingUnlink: v.PendingUnlink, PendingGeneration: append([]byte{}, v.PendingGeneration...)}
}
func (v referenceState) storage() storage.ReferenceState {
	return storage.ReferenceState{LinkTarget: v.LinkTarget, Attr: v.Attr.Storage(), Detached: v.Detached, PendingUnlink: v.PendingUnlink, PendingGeneration: v.PendingGeneration}
}

type fileCapabilities struct {
	AtomicOpen  bool `json:"atomicOpen"`
	Namespace   bool `json:"namespace"`
	References  bool `json:"references"`
	Metadata    bool `json:"metadata"`
	Owners      bool `json:"owners"`
	Ranges      bool `json:"ranges"`
	State       bool `json:"state"`
	Scope       bool `json:"scope"`
	Delete      bool `json:"delete"`
	Conditional bool `json:"conditional"`
}
