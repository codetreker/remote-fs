package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type exportReference struct {
	store    *Store
	file     *retainedFile
	key      metastore.Key
	released bool
}

func (s *Store) CaptureExport(ctx context.Context, c storage.ExportCommand) (metastore.ExportCapture, error) {
	if err := c.Check(); err != nil {
		return metastore.ExportCapture{}, err
	}
	if c.NodeID > math.MaxInt64 {
		return metastore.ExportCapture{}, syscall.ESTALE
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return metastore.ExportCapture{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileOwnership(); err != nil {
		return metastore.ExportCapture{}, err
	}
	if s.fileDomain.files >= s.fileDomain.maxFiles || s.exportPins >= s.fileDomain.maxFiles {
		return metastore.ExportCapture{}, syscall.EAGAIN
	}
	scope, err := newReferenceScope()
	if err != nil {
		return metastore.ExportCapture{}, err
	}
	f := &retainedFile{store: s, id: int64(c.NodeID), scope: scope, session: metastore.ReferenceSession(ctx), read: true, metadata: storage.ReadMetadata, active: true,
		use: (storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}).EffectiveUse()}
	var result metastore.ExportCapture
	claimed := false
	err = s.inspect(ctx, func(tx *sql.Tx) error {
		var original storage.UseScope
		if c.Scope != nil {
			provided, err := s.resolveUseScope(ctx, *c.Scope, c.NodeID, storage.ReadData)
			if err != nil {
				return err
			}
			if provided.metadata&storage.ReadMetadata == 0 {
				return syscall.EBADF
			}
			original = provided.scope
		}
		state, err := s.fileState(ctx, tx, int64(c.NodeID))
		if err != nil {
			return err
		}
		if state.Kind != storage.NodeRegular {
			return nodeKindMismatch(state.Kind, storage.NodeRegular)
		}
		if c.Scope == nil && state.Detached {
			return syscall.ESTALE
		}
		if err := s.checkByteExtent(ctx, c.NodeID, original, 0, state.Size, storage.ReadData); err != nil {
			return err
		}
		if err := s.fileDomain.coordinator.AddUse(ctx, c.NodeID, scope, f.use); err != nil {
			return err
		}
		claimed = true
		result.State = state
		result.Key = state.Content
		if state.Content == "" {
			if state.Size != 0 {
				return syscall.EIO
			}
			result.Digest = sha256.Sum256(nil)
			result.DigestAvailable = true
			return nil
		}
		var size int64
		var objectState int
		if err := tx.QueryRowContext(ctx, `SELECT size,state FROM objects WHERE volume=? AND key=?`, s.volume, string(state.Content)).Scan(&size, &objectState); err != nil {
			return err
		}
		if size != state.Size || objectState != stateReferenced {
			return syscall.EIO
		}
		return nil
	})
	if err != nil {
		if claimed {
			err = errors.Join(err, s.fileDomain.coordinator.DropUse(context.WithoutCancel(ctx), c.NodeID, scope))
		}
		return metastore.ExportCapture{}, err
	}
	s.retainFileLocked(f)
	s.exportPins++
	if result.Key != "" {
		s.coordinator.objectPinsMu.Lock()
		s.coordinator.objectPins[result.Key]++
		s.coordinator.objectPinsMu.Unlock()
	}
	result.Reference = &exportReference{store: s, file: f, key: result.Key}
	return result, nil
}

func (e *exportReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	// Retention survives a failed reference close. Exact-key release is ordered
	// after native settlement, so concurrent GC cannot cross either responsibility.
	settled, err := e.file.CloseWithResult(ctx)
	if !settled.Released || !settled.Determined {
		return settled, err
	}
	s := e.store
	if acquireErr := s.coordinator.commit.acquire(ctx); acquireErr != nil {
		return storage.ReferenceCloseResult{}, errors.Join(err, acquireErr)
	}
	defer s.coordinator.commit.release()
	if e.released {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, err
	}
	if s.exportPins < 1 {
		return storage.ReferenceCloseResult{}, errors.Join(err, syscall.EIO)
	}
	if e.key != "" {
		s.coordinator.objectPinsMu.Lock()
		defer s.coordinator.objectPinsMu.Unlock()
		count := s.coordinator.objectPins[e.key]
		if count < 1 {
			return storage.ReferenceCloseResult{}, errors.Join(err, syscall.EIO)
		}
		if count == 1 {
			delete(s.coordinator.objectPins, e.key)
		} else {
			s.coordinator.objectPins[e.key] = count - 1
		}
	}
	s.exportPins--
	e.released = true
	return storage.ReferenceCloseResult{Released: true, Determined: true}, err
}
