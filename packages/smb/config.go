package smb

import (
	"errors"
	"log/slog"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

var (
	ErrConfig  = errors.New("invalid SMB configuration")
	ErrBusy    = errors.New("SMB export is busy")
	ErrStopped = errors.New("SMB server has stopped")
)

type Config struct {
	Authenticator     Authenticator
	AuthorizeIdentity IdentityAuthorizer
	Authorize         authz.Authorizer
	Limits            Limits
	Logger            *slog.Logger
}

// Limits bounds every resource retained by the endpoint. Callers select the
// defaults explicitly; an omitted individual field never means unbounded.
type Limits struct {
	MaxExports, MaxConnections, MaxSessions, MaxTrees, MaxOpens int
	MaxRequests, MaxCompound, MaxContexts                       int
	MaxFrameBytes, MaxIOBytes, MaxTokenBytes                    int
	MaxOpenResultBytes, MaxDirectoryBytes                       int64
	MaxDirectoryEntries                                         int
	HandshakeTimeout, RequestTimeout, CleanupTimeout            time.Duration
	FileSession                                                 storage.FileSessionOptions
}

func DefaultLimits() Limits {
	return Limits{
		MaxExports: 32, MaxConnections: 16, MaxSessions: 16, MaxTrees: 32, MaxOpens: 1024,
		MaxRequests: 128, MaxCompound: 32, MaxContexts: 16,
		MaxFrameBytes: 2 << 20, MaxIOBytes: 1 << 20, MaxTokenBytes: 65535,
		MaxOpenResultBytes: 8 << 20, MaxDirectoryBytes: 8 << 20,
		MaxDirectoryEntries: storage.MaxDirectoryEntries,
		HandshakeTimeout:    30 * time.Second, RequestTimeout: time.Minute,
		CleanupTimeout: 30 * time.Second, FileSession: storage.DefaultFileSessionOptions(),
	}
}

func (l Limits) check() error {
	if l.MaxExports < 1 || l.MaxConnections < 1 || l.MaxSessions < 1 || l.MaxTrees < 1 || l.MaxOpens < 1 ||
		l.MaxRequests < 1 || l.MaxRequests > 65535 || l.MaxCompound < 1 || l.MaxCompound > 128 || l.MaxCompound > l.MaxRequests ||
		l.MaxContexts < 1 || l.MaxIOBytes < 65536 || l.MaxIOBytes > 8<<20 ||
		l.MaxFrameBytes < l.MaxIOBytes+65536 || l.MaxFrameBytes > 0xffffff ||
		l.MaxTokenBytes < 1 || l.MaxTokenBytes > 65535 || l.MaxOpenResultBytes < maxOpenResultCharge() ||
		l.MaxDirectoryBytes < 256 || l.MaxDirectoryEntries < 1 || l.MaxDirectoryEntries > storage.MaxDirectoryEntries ||
		l.HandshakeTimeout <= 0 || l.RequestTimeout <= 0 || l.CleanupTimeout <= 0 {
		return ErrConfig
	}
	return l.FileSession.Check()
}

type Share struct {
	Name string
	// Volume is the trusted host-selected identity used for authorization. It
	// must remain stable when a share is renamed or republished.
	Volume            string
	DeleteIntentOwner storage.DeleteIntentOwner
	Backend           storage.FileStorage
}

// Status reports resources still owned, including cleanup-only owners.
type Status struct {
	Serving, Stopping, Stopped                                 bool
	Exports, StoppingExports, Connections, RetainedConnections int
	Sessions, ExpiredSessions, Trees, PendingRequests          int
	OpenReservations, InstalledHandles, CleanupPendingHandles  int
	ActiveFileOperations                                       int
	OpenResultBytes, DirectoryEntries, DirectoryBytes          int64
	DeleteIntents                                              int
	RecoveryPendingAuthorities                                 int
	FencedAuthorities, CleanupFailures                         int
}
