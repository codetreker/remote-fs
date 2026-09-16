package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/internal/fileaccess"
	"github.com/codetreker/remote-fs/packages/internal/filebudget"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/storage"
)

func accessLimits(o storage.FileServiceOptions) fileaccess.Limits {
	return fileaccess.Limits{MaxClaims: o.MaxClaims, MaxOwners: o.MaxOwners, MaxRanges: o.MaxRanges,
		MaxOwnerRanges: o.MaxOwnerRanges, MaxSetRanges: o.MaxSetRanges, MaxSnapshotRanges: o.MaxSnapshotRanges,
		MaxWaits: o.MaxWaits, MaxWaitRanges: o.MaxWaitRanges, MaxDependencies: o.MaxDependencies, MaxWork: o.MaxWork}
}

func contentLimits(o storage.FileServiceOptions) filebudget.Config {
	return filebudget.Config{MaxMaterializedBytes: o.MaxMaterializedBytes, MaxMaterializations: o.MaxMaterializations,
		MaxFileBytes: o.MaxFileBytes, MaxFileAttempts: o.MaxFileAttempts, FileOperationTimeout: o.FileOperationTimeout}
}

type fileDomain struct {
	memberships                            map[*fileIOMembership]struct{}
	recoveryPending                        bool
	recoveryStore                          *Store
	recoveryTimer                          *time.Timer
	recoveryContext                        context.Context
	recoveryStopped                        atomic.Bool
	config                                 storage.FileServiceOptions
	accessConfig                           fileaccess.Limits
	contentConfig                          filebudget.Config
	access                                 *fileaccess.Coordinator
	budget                                 *filebudget.Budget
	sessions                               map[*fileSession]struct{}
	stores, maxFiles, files, activeIO      int
	nextSession, nextReference, nextIntent uint64
	authority                              string
	recoveryUntil                          time.Time
}

func allocateFileIdentity(counter *uint64) (uint64, error) {
	if *counter == math.MaxUint64 {
		return 0, syscall.EOVERFLOW
	}
	*counter++
	return *counter, nil
}

func (d *fileDomain) allocateSession() (uint64, error) { return allocateFileIdentity(&d.nextSession) }
func (d *fileDomain) allocateReference() (uint64, error) {
	return allocateFileIdentity(&d.nextReference)
}
func (d *fileDomain) allocateIntent() (uint64, error) { return allocateFileIdentity(&d.nextIntent) }

func (s *Store) attachFileDomain(options Options) error {
	d := s.coordinator.domains[s.volume]
	if d == nil {
		access, err := fileaccess.New(accessLimits(options.Files))
		if err != nil {
			return err
		}
		budget, err := filebudget.New(contentLimits(options.Files))
		if err != nil {
			return err
		}
		var nonce [16]byte
		rand.Read(nonce[:])
		d = &fileDomain{config: options.Files, accessConfig: accessLimits(options.Files), contentConfig: contentLimits(options.Files),
			access: access, budget: budget, sessions: make(map[*fileSession]struct{}), memberships: make(map[*fileIOMembership]struct{}), maxFiles: options.MaxRetainedFiles,
			authority: hex.EncodeToString(nonce[:])}
		s.coordinator.domains[s.volume] = d
	} else if d.config != options.Files || d.maxFiles != options.MaxRetainedFiles {
		return fmt.Errorf("shared volume file limits differ from its active owner: %w", syscall.EINVAL)
	}
	d.stores++
	s.fileDomain = d
	return nil
}

func (s *Store) releaseFileDomain() {
	if s.fileDomain == nil {
		return
	}
	s.fileDomain.stores--
	if s.fileDomain.stores == 0 {
		s.fileDomain.recoveryStopped.Store(true)
		if s.fileDomain.recoveryTimer != nil {
			s.fileDomain.recoveryTimer.Stop()
		}
		s.fileDomain.access.Close()
		delete(s.coordinator.domains, s.volume)
	}
	s.fileDomain = nil
}

func (s *Store) FileOperationLimits() (int64, int, time.Duration) {
	return s.fileDomain.budget.FileOperationLimits()
}

func (s *Store) AcquireMaterialization(ctx context.Context, size int64) (func(), error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.coordinator.commit.release()
	if s.files == nil || s.fileDomain == nil {
		return nil, syscall.ESTALE
	}
	if s.leaseOwner == nil || !s.leaseOwner.Exclusive() {
		return nil, syscall.EOPNOTSUPP
	}
	if s.leaseOwner.FD() < 0 {
		return nil, syscall.ESTALE
	}
	if err := s.coordinator.healthy(); err != nil {
		return nil, err
	}
	return s.fileDomain.budget.AcquireMaterialization(ctx, size)
}

func (s *Store) checkFileAuthority(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkFileOwnership(); err != nil {
		return err
	}
	if err := s.coordinator.healthy(); err != nil {
		return err
	}
	if s.fileLeaseRecovery == nil {
		return syscall.EOPNOTSUPP
	}
	if time.Now().Before(s.fileDomain.recoveryUntil) || s.fileDomain.recoveryPending {
		return syscall.EAGAIN
	}
	return nil
}

func (s *Store) FileState(ctx context.Context) (storage.FileVolumeState, error) {
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.FileVolumeState{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkFileAuthority(ctx); err != nil {
		return storage.FileVolumeState{}, err
	}
	var identity string
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		state, err := dbstate.Read(ctx, tx)
		if err != nil {
			return err
		}
		identity = state.DatabaseID + ":" + strconv.FormatInt(s.volume, 10)
		return nil
	})
	return storage.FileVolumeState{VolumeIdentity: identity, RootID: uint64(s.root), MaxEventBytes: metastore.MaxChangePayloadBytes}, err
}
