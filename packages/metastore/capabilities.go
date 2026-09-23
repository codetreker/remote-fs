package metastore

import (
	"context"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/storage"
)

// OpenResult captures the original atomic result. A nonnil File remains owned
// by the caller even when Error is nonnil and must be closed or retained for
// cleanup; callers must not reconstruct Attr or Outcome with a later Stat.
type OpenResult struct {
	File    File
	State   FileState
	Outcome storage.OpenOutcome
}

type AtomicFileOpener interface {
	CheckAtomicFileOpen() error
	OpenAt(context.Context, storage.ChildSelection, storage.OpenAtOptions) (OpenResult, error)
}

// NodeReference shares native retention and session lifetime without granting
// regular-file byte methods.
type NodeReference interface {
	storage.ScopedReference
	storage.ReferenceMetadataAccess
	ReferenceStateAccess
	Order(context.Context, func() error) error
	Node(context.Context) (FileState, error)
	SetAttr(context.Context, storage.AttrChange) (FileState, error)
	Retire(context.Context) error
	DropUse(context.Context) error
	Close(context.Context) error
	CloseWithResult(context.Context) (storage.ReferenceCloseResult, error)
}

type NodeOpenResult struct {
	Reference NodeReference
	State     FileState
	Outcome   storage.OpenOutcome
}

type NodeReferences interface {
	CheckNodeReferences() error
	OpenNodeRef(context.Context, uint64, storage.NodeRefOptions) (NodeOpenResult, error)
	OpenChildRef(context.Context, storage.ChildSelection, storage.NodeRefOptions) (NodeOpenResult, error)
}

type NamespaceAccess interface {
	CheckNamespaceAccess() error
	LookupAt(context.Context, storage.ChildName) (storage.Attr, error)
	MutateName(context.Context, storage.NameCommand) (storage.NameResult, error)
}

type DirectoryReader interface {
	CheckDirectoryRead() error
	ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error)
	ReadDirNodeBounded(context.Context, storage.DirectoryTarget, *storage.ListResult) (storage.DirectoryObservation, error)
}

type ReferenceState struct {
	State             FileState
	LinkTarget        []byte
	PendingUnlink     bool
	PendingGeneration []byte
}

type ReferenceStateAccess interface {
	CheckReferenceState() error
	State(context.Context) (ReferenceState, error)
}

// ConditionalFileMutation applies attribute-only changes directly and publishes
// a staged object under the same final condition checks for data changes.
type ConditionalFileMutation interface {
	CheckConditionalFileMutation() error
	MutateFile(context.Context, storage.FileMutation) (FileState, error)
	CommitMutation(context.Context, storage.FileMutation, uint64, Object) (FileState, error)
}

type DeleteIntent interface {
	CheckDeleteIntent() error
	SetPendingUnlink(context.Context, storage.PendingUnlinkCommand) (ReferenceState, error)
	ClearPendingUnlink(context.Context, storage.ClearPendingUnlinkCommand) (ReferenceState, error)
}

type referenceSessionKey struct{}

// WithReferenceSession binds native capability calls to the caller's existing
// advisory session. It does not create a second owner or extend its lifetime.
func WithReferenceSession(ctx context.Context, session *advisory.Session) context.Context {
	return context.WithValue(ctx, referenceSessionKey{}, session)
}

func ReferenceSession(ctx context.Context) *advisory.Session {
	session, _ := ctx.Value(referenceSessionKey{}).(*advisory.Session)
	return session
}

type FileAccess struct {
	Uses           storage.Uses
	Offset, Length int64
	Truncate       bool
	Append         bool
	Size           int64
}

func (a FileAccess) Check() error {
	if a.Uses != storage.ReadData && a.Uses != storage.WriteData {
		return syscall.EINVAL
	}
	if a.Offset < 0 || a.Length < 0 || a.Length > math.MaxInt64-a.Offset || a.Size < 0 {
		return syscall.EINVAL
	}
	if a.Append && (a.Uses != storage.WriteData || a.Truncate || a.Offset != 0 || a.Size != 0) {
		return syscall.EINVAL
	}
	if a.Truncate {
		if a.Uses != storage.WriteData || a.Offset != 0 || a.Length != 0 {
			return syscall.EINVAL
		}
	} else if a.Size != 0 {
		return syscall.EINVAL
	}
	return nil
}

type fileAccessKey struct{}

func WithFileAccess(ctx context.Context, access FileAccess) context.Context {
	return context.WithValue(ctx, fileAccessKey{}, access)
}

func FileAccessFrom(ctx context.Context) (FileAccess, bool) {
	access, ok := ctx.Value(fileAccessKey{}).(FileAccess)
	return access, ok
}
