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
	Authenticator Authenticator
	Authorize     authz.Authorizer
	Limits        Limits
	Logger        *slog.Logger
}

// Limits bounds allocation before protocol input is decoded. Select defaults
// explicitly so an omitted limit never means unbounded resource retention.
type Limits struct {
	MaxExports, MaxConnections, MaxSessions, MaxTrees, MaxOpens      int
	MaxRequests, MaxCompound, MaxContexts, MaxFrameBytes, MaxIOBytes int
	MaxTokenBytes                                                    int
	MaxDirectoryBytes                                                int64
	MaxNotifyEvents                                                  int
	MaxNotifyBytes                                                   int64
	HandshakeTimeout, RequestTimeout, CleanupTimeout                 time.Duration
	FileSession                                                      storage.FileSessionOptions
}

func DefaultLimits() Limits {
	return Limits{MaxExports: 32, MaxConnections: 16, MaxSessions: 16, MaxTrees: 32, MaxOpens: 1024,
		MaxRequests: 128, MaxCompound: 32, MaxContexts: 16, MaxFrameBytes: 2 << 20,
		MaxIOBytes: 1 << 20, MaxTokenBytes: 65535, MaxDirectoryBytes: 8 << 20, MaxNotifyEvents: 1024, MaxNotifyBytes: 8 << 20,
		HandshakeTimeout: 30 * time.Second, RequestTimeout: time.Minute, CleanupTimeout: 30 * time.Second,
		FileSession: storage.DefaultFileSessionOptions()}
}

func (l Limits) check() error {
	if l.MaxExports < 1 || l.MaxConnections < 1 || l.MaxSessions < 1 || l.MaxTrees < 1 || l.MaxOpens < 1 ||
		l.MaxRequests < 1 || l.MaxRequests > 65535 || l.MaxCompound < 1 || l.MaxCompound > 128 || l.MaxCompound > l.MaxRequests ||
		l.MaxContexts < 1 || l.MaxIOBytes < 65536 || l.MaxIOBytes > 8<<20 ||
		l.MaxFrameBytes < l.MaxIOBytes+65536 || l.MaxFrameBytes > 0xffffff ||
		l.MaxNotifyEvents < 1 || l.MaxNotifyBytes < 1 || l.MaxTokenBytes < 1 || l.MaxTokenBytes > 65535 || l.MaxDirectoryBytes < 1 ||
		l.HandshakeTimeout <= 0 || l.RequestTimeout <= 0 || l.CleanupTimeout <= 0 {
		return ErrConfig
	}
	return l.FileSession.Check()
}

type Share struct {
	Name, Volume string
	Backend      storage.FileStorage
}

// Status reports owned resources, including resources awaiting confirmed cleanup.
type Status struct {
	Serving, Stopping, Stopped                                 bool
	Exports, StoppingExports, Connections, RetainedConnections int
	Sessions, Trees, PendingRequests                           int
	OpenReservations, InstalledHandles, CleanupPendingHandles  int
	FencedAuthorities, CleanupFailures                         int
}
