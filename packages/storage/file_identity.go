package storage

import (
	"context"
	"strings"
	"syscall"
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

// OpenMetadataAccess promises independent metadata permissions for OpenAt.
// Zero grants neither Stat nor metadata mutation; byte rights and Uses do not
// confer metadata rights. The original atomic OpenResult still contains Attr.
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
