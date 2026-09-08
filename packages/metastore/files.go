package metastore

import (
	"context"

	"github.com/codetreker/remote-fs/packages/advisory"
	"github.com/codetreker/remote-fs/packages/storage"
)

// FileStore retains regular-file identities independently of namespace entries.
// Opens and requested creation/truncation are atomic with retention. Usage includes
// detached files and remains available when the namespace has no allowance.
type FileStore interface {
	// CheckFileStore reports the constructor-selected retention capability without
	// waiting for I/O or publication. Operations verify current exclusive ownership.
	CheckFileStore() error
	Advisory(context.Context) (*advisory.Coordinator, error)
	OpenFile(context.Context, string, storage.FileOpenOptions) (File, error)
	OpenNode(context.Context, uint64, storage.FileOpenOptions) (File, error)
	StatNode(context.Context, uint64) (Node, error)
	SetNodeAttr(context.Context, uint64, storage.AttrChange) (Node, error)
	Usage(context.Context) (int64, error)
}

// FileState captures attributes and content identity from one committed revision.
// Revision changes on every content publication, including empty-to-empty writes.
// Detached files have no namespace entry and produce no namespace change events.
type FileState struct {
	Node
	Revision uint64
	Detached bool
}

// File is a retained native regular file. The object layer must drain admitted
// materialization and upload operations before Close releases physical retention.
// Calls are concurrent-safe. A retired reference returns ESTALE for further work.
type File interface {
	Node(context.Context) (FileState, error)
	Reserve(context.Context, int64) (Key, error)
	// Commit checks logical liveness and expected revision under final publication
	// ordering. EAGAIN means the revision changed and the object remains reserved.
	// Strong mutation scope and publication accounting are carried in context.
	Commit(context.Context, uint64, Object) (FileState, error)
	SetAttr(context.Context, storage.AttrChange) (FileState, error)
	// Retire fences publication before an external caller starts draining I/O.
	// The physical pin and quota survive until Close has a known durable result.
	Retire(context.Context) error
	// Close is idempotent and retires the reference before releasing its pin.
	// Last-close accounting uses its context and the actual remaining file size.
	Close(context.Context) error
}

type filePublicationGuardKey struct{}
type filePublicationGuard struct {
	check    func() error
	previous *filePublicationGuard
}

// WithFilePublicationGuard adds a bounded lifetime check at final native
// publication admission. Checks must perform no I/O and release internal locks
// before returning. Retirement must also fence native references before draining
// operations; a context check alone does not retain or retire an object.
func WithFilePublicationGuard(ctx context.Context, check func() error) context.Context {
	if check == nil {
		panic("metastore: nil file publication guard")
	}
	previous, _ := ctx.Value(filePublicationGuardKey{}).(*filePublicationGuard)
	return context.WithValue(ctx, filePublicationGuardKey{}, &filePublicationGuard{check: check, previous: previous})
}

// CheckFilePublication validates all attached lifetime checks while the native
// publication gate is held, immediately before the final mutation permit.
func CheckFilePublication(ctx context.Context) error {
	guard, _ := ctx.Value(filePublicationGuardKey{}).(*filePublicationGuard)
	for ; guard != nil; guard = guard.previous {
		if err := guard.check(); err != nil {
			return err
		}
	}
	return nil
}
