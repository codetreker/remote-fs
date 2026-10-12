package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (l *Lifecycle) RetireNamespaces(ctx context.Context, actor management.ID) (int64, error) {
	release, err := l.lockOperation(ctx, actor)
	if err != nil {
		return 0, err
	}
	defer release()
	candidate, err := l.config.Management.RetirementCandidate(ctx, actor)
	if err != nil {
		return 0, err
	}
	if candidate.Floor <= candidate.ExpectedRetiredBefore {
		return candidate.ExpectedRetiredBefore, nil
	}
	if candidate.Floor == math.MaxInt64 {
		return 0, syscall.EOVERFLOW
	}
	evidence := sha256.New()
	var beforeBytes [8]byte
	binary.BigEndian.PutUint64(beforeBytes[:], uint64(candidate.Floor)+1)
	_, _ = evidence.Write(beforeBytes[:])
	for _, id := range candidate.VolumeIDs {
		volume, err := l.config.Management.GetVolume(ctx, actor, id)
		if err != nil {
			return 0, err
		}
		before := uint64(candidate.Floor) + 1
		if volume.State == management.VolumeDeleted {
			// A completed delete has settled the entire native authority and leaves a
			// permanent management tombstone; reopening its removed root is forbidden.
			_, _ = evidence.Write([]byte(id))
			_, _ = evidence.Write([]byte{1})
			continue
		}
		lease, err := l.config.Hooks.Acquire(ctx, volume)
		if err != nil {
			return 0, err
		}
		if lease.Store == nil || lease.Release == nil {
			return 0, syscall.EIO
		}
		retiredErr := lease.Store.RetireActionNamespaces(ctx, storage.ActionNamespaceRetirement{SubjectID: string(actor), Before: before, RetainAfter: candidate.Cutoff})
		releaseErr := lease.Release()
		if err := errors.Join(retiredErr, releaseErr); err != nil {
			return 0, err
		}
		_, _ = evidence.Write([]byte(id))
		_, _ = evidence.Write([]byte{0})
	}
	proof, err := json.Marshal(struct {
		Digest         [32]byte `json:"digest"`
		NativeDigest   []byte   `json:"nativeDigest"`
		AuthorityCount int      `json:"authorityCount"`
		CutoffMillis   string   `json:"cutoffMillis"`
	}{candidate.Digest, evidence.Sum(nil), len(candidate.VolumeIDs), strconv.FormatInt(candidate.Cutoff.UnixMilli(), 10)})
	if err != nil {
		return 0, err
	}
	return l.config.Management.ConfirmRetirement(ctx, actor, candidate, candidate.NativeRevision, proof)
}

func (l *Lifecycle) RetireAllNamespaces(ctx context.Context) error {
	var after management.ID
	var failures []error
	for {
		actors, err := l.config.Management.ListNamespaceActors(ctx, after, 50)
		if err != nil {
			return errors.Join(append(failures, err)...)
		}
		if len(actors) == 0 {
			return errors.Join(failures...)
		}
		for _, actor := range actors {
			if _, err := l.RetireNamespaces(ctx, actor); err != nil {
				failures = append(failures, err)
			}
			after = actor
		}
	}
}
