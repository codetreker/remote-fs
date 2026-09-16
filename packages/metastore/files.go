package metastore

import (
	"context"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// FileStore orders retained references and transient path operations in one
// authority. Content materialization shares its volume-wide admission budget.
type FileStore interface {
	CheckFileStore() error
	FileState(context.Context) (storage.FileVolumeState, error)
	NewFileSession(context.Context, storage.FileSessionOptions) (FileSession, storage.FileSessionStatus, error)
	FileOperationLimits() (int64, int, time.Duration)
	AcquireMaterialization(context.Context, int64) (func(), error)
	Usage(context.Context) (int64, error)
}

// FileState captures the exact immutable object and metadata at one revision.
type FileState struct {
	Node
	Revision uint64
	Detached bool
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
