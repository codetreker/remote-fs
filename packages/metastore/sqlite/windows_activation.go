package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"strconv"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (d *windowsDomain) rotateActivation() {
	now := time.Now()
	if !now.Before(d.activationRotates) {
		d.activationEpoch++
		d.activationRotates = now.Add(time.Minute)
	}
	for id, a := range d.activations {
		if !now.Before(a.expires) {
			delete(d.activations, id)
		}
	}
}

func (s *Store) WindowsState(ctx context.Context) (storage.WindowsState, error) {
	if err := s.CheckWindowsStore(); err != nil {
		return storage.WindowsState{}, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.WindowsState{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkWindowsLocked(ctx); err != nil {
		return storage.WindowsState{}, err
	}
	d := s.fileDomain.windows
	d.rotateActivation()
	var version uint32
	var identity string
	err := s.inspect(ctx, func(tx *sql.Tx) error {
		var err error
		version, err = s.windowsNameVersion(ctx, tx)
		if err != nil {
			return err
		}
		state, err := dbstate.Read(ctx, tx)
		if err != nil {
			return err
		}
		identity = state.DatabaseID + ":" + strconv.FormatInt(s.volume, 10)
		return nil
	})
	label := sha256.Sum256([]byte(identity))
	return storage.WindowsState{VolumeIdentity: identity, VolumeSerial: binary.LittleEndian.Uint64(label[:8]), Enabled: version != 0, ActionEpoch: d.activationEpoch, MaxEventBytes: metastore.MaxChangePayloadBytes}, err
}

func (s *Store) EnableWindows(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	epoch, err := id.Epoch()
	if err != nil {
		return storage.WindowsActivation{}, err
	}
	if err := s.CheckWindowsStore(); err != nil {
		return storage.WindowsActivation{}, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.WindowsActivation{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkWindowsLocked(ctx); err != nil {
		return storage.WindowsActivation{}, err
	}
	d := s.fileDomain.windows
	d.rotateActivation()
	if old, ok := d.activations[id]; ok {
		old.result.HistoryRemaining = max(time.Until(old.expires), 0)
		return old.result, old.err
	}
	if epoch != d.activationEpoch {
		return storage.WindowsActivation{}, syscall.ESTALE
	}
	if len(d.activations) >= s.fileDomain.config.MaxRequests {
		return storage.WindowsActivation{}, syscall.EAGAIN
	}
	result := storage.WindowsActivation{Action: id, State: storage.WindowsActionPending}
	err = s.mutateTransactionLocked(ctx, ctx, nil, func(tx *sql.Tx) error { return s.activateWindowsLocked(ctx, tx) })
	if s.coordinator.healthy() == nil {
		result.State = storage.WindowsActionCompleted
		result.Enabled = err == nil
		if err != nil {
			result.State = storage.WindowsActionRejected
			errors.As(err, &result.Errno)
		}
	}
	expires := time.Now().Add(time.Minute)
	result.HistoryRemaining = time.Minute
	d.activations[id] = windowsActivationRecord{result: result, err: err, expires: expires}
	return result, err
}

func (s *Store) QueryWindowsActivation(ctx context.Context, id storage.WindowsActionID) (storage.WindowsActivation, error) {
	if _, err := id.Epoch(); err != nil {
		return storage.WindowsActivation{}, err
	}
	if err := s.CheckWindowsStore(); err != nil {
		return storage.WindowsActivation{}, err
	}
	if err := s.coordinator.commit.acquire(ctx); err != nil {
		return storage.WindowsActivation{}, err
	}
	defer s.coordinator.commit.release()
	if err := s.checkWindowsLocked(ctx); err != nil {
		return storage.WindowsActivation{}, err
	}
	d := s.fileDomain.windows
	d.rotateActivation()
	a, ok := d.activations[id]
	if !ok {
		return storage.WindowsActivation{}, syscall.ESTALE
	}
	a.result.HistoryRemaining = max(time.Until(a.expires), 0)
	return a.result, a.err
}
