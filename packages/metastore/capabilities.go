package metastore

import (
	"context"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/storage"
)

// OpenResult captures the original atomic outcome. A nonnil File remains owned
// by the caller even with an error and must be retired and closed or retained
// for cleanup; it must never be delivered as a successful application open.
type OpenResult struct {
	File    File
	State   FileState
	Outcome storage.OpenOutcome
}
type AtomicFileOpener interface {
	CheckAtomicFileOpen() error
	OpenAt(context.Context, storage.ChildName, storage.OpenAtOptions) (OpenResult, error)
}

// NodeReference shares the retained-file registry and physical pin lifetime but
// grants no byte methods. Its Node result is captured from one authoritative state.
type NodeReference interface {
	Node(context.Context) (FileState, error)
	SetAttr(context.Context, storage.AttrChange) (FileState, error)
	Retire(context.Context) error
	Close(context.Context) error
}

// A nonnil Reference transfers cleanup responsibility on success and on error.
type NodeOpenResult struct {
	Reference NodeReference
	State     FileState
	Outcome   storage.OpenOutcome
}
type NodeReferences interface {
	CheckNodeReferences() error
	OpenNodeRef(context.Context, uint64, storage.NodeRefOptions) (NodeOpenResult, error)
	OpenChildRef(context.Context, storage.ChildName, storage.NodeRefOptions) (NodeOpenResult, error)
}
type NamespaceAccess interface {
	CheckNamespaceAccess() error
	LookupAt(context.Context, storage.ChildName) (storage.Attr, error)
	ReadDirNode(context.Context, storage.DirectoryTarget) (storage.ObservedDirectory, error)
	ReadDirNodeBounded(context.Context, storage.DirectoryTarget, *storage.ListResult) (storage.DirectoryObservation, error)
	MutateName(context.Context, storage.NameCommand) (storage.NameResult, error)
}
type ReferenceState struct {
	State             FileState
	PendingUnlink     bool
	PendingGeneration []byte
}
type ReferenceStateAccess interface {
	CheckReferenceState() error
	State(context.Context) (ReferenceState, error)
}

// Attribute mutations need no content object. CommitMutation publishes an
// existing staged object under the original revision and explicit conditions;
// the objectstore layer owns materialization, reservation and bounded retries.
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

// WithReferenceSession binds native calls to the existing object's advisory
// session. It does not create ownership or another session lifetime.
func WithReferenceSession(ctx context.Context, session *advisory.Session) context.Context {
	return context.WithValue(ctx, referenceSessionKey{}, session)
}
func ReferenceSession(ctx context.Context) *advisory.Session {
	session, _ := ctx.Value(referenceSessionKey{}).(*advisory.Session)
	return session
}

// FileAccess describes the requested data extent, not a replacement object's
// staged whole body. It conveys no scope, identity or authorization exemption.
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
