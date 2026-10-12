package storage

import (
	"context"
	"strings"
	"syscall"
	"time"
)

const MaxFileIdentityBytes = 128

type VolumeID string
type AuthorityIncarnation string

// BackendIdentity identifies the durable volume and its current shared ordering
// authority. The getter fails when ownership, health, or the live root cannot be
// verified. A path, authorization label, or per-session nonce is not a Volume.
type BackendIdentity interface {
	CheckBackendIdentity() error
	BackendIdentity(context.Context) (BackendIdentityResult, error)
}

type BackendIdentityResult struct {
	Volume     VolumeID
	Authority  AuthorityIncarnation
	RootNodeID uint64
}

// FileSessionIdentity binds a session to the same backend descriptor used by
// its retained references. SessionEpoch must equal that session's Status.Epoch.
type FileSessionIdentity interface {
	CheckFileSessionIdentity() error
	FileSessionIdentity(context.Context) (FileSessionIdentityResult, error)
}

type FileSessionIdentityResult struct {
	Backend      BackendIdentityResult
	SessionEpoch string
}

// StableReferenceIdentity promises that OpenAt, OpenNodeRef, and OpenChildRef
// return references implementing ReferenceIdentity, including error results
// that transfer cleanup ownership. The identity stays nonzero and immutable
// after retirement. Check verifies the complete implementation chain before effects.
type StableReferenceIdentity interface {
	CheckStableReferenceIdentity() error
}

// OpenMetadataAccess promises independent metadata permissions and the complete
// ReferenceMetadataAccess contract for OpenAt, OpenNodeRef and OpenChildRef.
// Zero grants neither Stat nor arbitrary namespace observation or mutation;
// byte rights and Uses do not confer metadata rights. An enrolled content effect
// can be observed through its separate selector. Atomic open results retain Attr.
type OpenMetadataAccess interface {
	CheckOpenMetadataAccess() error
}

func (r BackendIdentityResult) Check() error {
	if !canonicalFileIdentity(string(r.Volume)) || !canonicalFileIdentity(string(r.Authority)) || r.RootNodeID == 0 {
		return syscall.EINVAL
	}
	return nil
}

func (r FileSessionIdentityResult) Check() error {
	if err := r.Backend.Check(); err != nil {
		return err
	}
	if !canonicalFileIdentity(r.SessionEpoch) {
		return syscall.EINVAL
	}
	return nil
}

func canonicalFileIdentity(value string) bool {
	return len(value) > 0 && len(value) <= MaxFileIdentityBytes &&
		strings.Trim(value, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:/-") == ""
}

// VolumeCreatedAt discovers the optional immutable creation fact without changing
// identity capabilities. The source must report its recorded volume creation,
// not a root attribute or an estimate; historical unknown facts fail explicitly.
// A successful result is canonical UTC; a nil error distinguishes a known year-1
// value from an unknown fact. Current session health and retained-reference
// liveness are checked by the caller.
func VolumeCreatedAt(ctx context.Context, source any) (time.Time, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	getter, ok := source.(interface {
		VolumeCreatedAt(context.Context) (time.Time, error)
	})
	if !ok {
		return time.Time{}, syscall.EOPNOTSUPP
	}
	created, err := getter.VolumeCreatedAt(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, err
	}
	return created.UTC(), nil
}
