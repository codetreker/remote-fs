package sqlite

import (
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/nativelease"
)

// LeaseAnchorConfig binds one witness location to an already exclusively owned native
// file or directory. BindingFD must remain owned until all SQLite handles close cleanly.
// RecoveryStart is captured after that ownership was acquired. Initialize permits creating
// a new durable intent; an existing READY intent still requires its accepted witness.
type LeaseAnchorConfig struct {
	Directory     string
	Name          string
	Identity      string
	BindingFD     int
	RecoveryStart time.Time
	Initialize    bool
}

// LeaseAnchor holds a durable initialization intent and an independent accepted witness.
// Its descriptors pin the native binding, but it does not acquire or release flock itself.
// Close must be called only after the caller has proved every writable database handle closed.
type LeaseAnchor nativelease.Anchor

// OpenLeaseAnchor validates existing evidence before making it available to SQLite.
// Missing evidence on an ordinary Open, malformed records, and changed native bindings
// return EIO. Only a matching durable initialization intent can resume an interrupted setup.
// Every open rejects known remote filesystems and probes local xattr, exclusive flock,
// atomic rename, and file/directory fsync support before creating or accepting lease state.
func OpenLeaseAnchor(config LeaseAnchorConfig) (*LeaseAnchor, error) {
	anchor, err := nativelease.Open(nativelease.Config(config))
	return (*LeaseAnchor)(anchor), err
}
func (a *LeaseAnchor) StateID() string          { return (*nativelease.Anchor)(a).StateID() }
func (a *LeaseAnchor) RecoveryStart() time.Time { return (*nativelease.Anchor)(a).RecoveryStart() }
func (a *LeaseAnchor) Initializing() bool       { return (*nativelease.Anchor)(a).Initializing() }
func (a *LeaseAnchor) Load() (LeaseEvidence, bool, error) {
	evidence, found, err := (*nativelease.Anchor)(a).Load()
	return LeaseEvidence(evidence), found, err
}
func (a *LeaseAnchor) Advance(next LeaseEvidence) error {
	return (*nativelease.Anchor)(a).Advance(nativelease.Evidence(next))
}
func (a *LeaseAnchor) Complete() error { return (*nativelease.Anchor)(a).Complete() }
func (a *LeaseAnchor) Close() error    { return (*nativelease.Anchor)(a).Close() }
