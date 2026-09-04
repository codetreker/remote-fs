// Package localdisk implements objectstore.Objects with immutable files on one local
// filesystem.
//
// The root is an owned storage directory, not a directory of user-visible files. Open
// places a durable format marker and an exclusive lifetime lock in it. Object names are
// derived without interpreting keys as paths, and every read verifies the envelope that
// binds the bytes to both the key and this particular store.
//
// Open rejects known remote filesystem types. An unknown type is accepted only after the
// publication probe succeeds; honest local flock, link, and fsync semantics remain a
// deployment requirement because no runtime probe can simulate a crash.
package localdisk

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

const (
	// InitializationMarkerName is reserved for a composite store's durable initialization
	// intent. Open creates the bootstrap marker when CompositeInitialization requests it.
	// The marker survives FORMAT so the composite can bind it to the store and workspace;
	// the composite removes it only after its own READY state is durable.
	InitializationMarkerName = "LOCALSTORE.init"

	// MaxKeyBytes keeps the injective key encoding inside one Linux pathname component.
	// Base64 expands 120 bytes to 160; the prefixes used by final and staging files leave
	// ample room beneath NAME_MAX's 255-byte limit on supported local filesystems.
	MaxKeyBytes = 120

	DefaultMaxObjectBytes          = int64(1024 * 1024 * 1024)
	DefaultMaxInFlightOperations   = 64
	DefaultMaxInFlightBytes        = int64(2 * 1024 * 1024 * 1024)
	DefaultMaintenanceReserveBytes = int64(64 * 1024 * 1024)
	DefaultMaxRecoveryEntries      = 4096
)

// Options bounds the resources an open store can retain while serving requests. Zero
// selects the corresponding Default value, including for MaintenanceReserveBytes.
type Options struct {
	// MaxObjectBytes limits one payload; the envelope and key are additional bytes.
	MaxObjectBytes int64
	// MaxInFlightOperations includes reads, writes, deletes, and capacity queries. Status
	// and root verification share one separate serialized control slot.
	MaxInFlightOperations int
	// MaxInFlightBytes covers encoded writes and allocated read payloads.
	MaxInFlightBytes int64
	// MaintenanceReserveBytes is unavailable to object payloads.
	MaintenanceReserveBytes int64
	// MaxRecoveryEntries bounds crash-residue records examined by Open.
	MaxRecoveryEntries int
	// CompositeInitialization creates a durable composite initialization intent under the
	// lifetime lock when FORMAT is absent.
	CompositeInitialization bool
}

type limits struct {
	maxObjectBytes          int64
	maxInFlightOperations   int
	maxInFlightBytes        int64
	maintenanceReserveBytes int64
	maxRecoveryEntries      int
}

// Validate checks option relationships without opening or modifying storage. Zero-valued
// fields are accepted and resolved to the exported defaults.
func (o Options) Validate() error {
	_, err := o.settle()
	return err
}

func (o Options) settle() (limits, error) {
	settled := limits{
		maxObjectBytes:          o.MaxObjectBytes,
		maxInFlightOperations:   o.MaxInFlightOperations,
		maxInFlightBytes:        o.MaxInFlightBytes,
		maintenanceReserveBytes: o.MaintenanceReserveBytes,
		maxRecoveryEntries:      o.MaxRecoveryEntries,
	}
	if settled.maxObjectBytes == 0 {
		settled.maxObjectBytes = DefaultMaxObjectBytes
	}
	if settled.maxInFlightOperations == 0 {
		settled.maxInFlightOperations = DefaultMaxInFlightOperations
	}
	if settled.maxInFlightBytes == 0 {
		settled.maxInFlightBytes = DefaultMaxInFlightBytes
	}
	if settled.maintenanceReserveBytes == 0 {
		settled.maintenanceReserveBytes = DefaultMaintenanceReserveBytes
	}
	if settled.maxRecoveryEntries == 0 {
		settled.maxRecoveryEntries = DefaultMaxRecoveryEntries
	}

	if settled.maxObjectBytes < 0 || settled.maxInFlightOperations < 0 || settled.maxInFlightBytes < 0 ||
		settled.maintenanceReserveBytes < 0 || settled.maxRecoveryEntries < 0 {
		return limits{}, fmt.Errorf("local-disk object limits must be positive: %w", syscall.EINVAL)
	}
	if settled.maxRecoveryEntries < settled.maxInFlightOperations {
		return limits{}, fmt.Errorf("the recovery-record limit %d is smaller than the operation limit %d: %w",
			settled.maxRecoveryEntries, settled.maxInFlightOperations, syscall.EINVAL)
	}
	if settled.maxObjectBytes > math.MaxInt64-fixedEnvelopeBytes-MaxKeyBytes {
		return limits{}, fmt.Errorf("the per-object limit %d leaves no room for an object envelope: %w",
			settled.maxObjectBytes, syscall.EOVERFLOW)
	}
	largestOperation := settled.maxObjectBytes + fixedEnvelopeBytes + MaxKeyBytes
	if settled.maxInFlightBytes < largestOperation {
		return limits{}, fmt.Errorf("the in-flight byte limit %d is smaller than the largest encoded object %d: %w",
			settled.maxInFlightBytes, largestOperation, syscall.EINVAL)
	}
	return settled, nil
}

// ID is the durable identity of one object store.
type ID [16]byte

// CompositeInitializationState describes the initialization intent observed or created
// while opening this store.
type CompositeInitializationState uint8

const (
	// NoCompositeInitialization means Open did not create or adopt the composite intent.
	// It does not assert that a higher-level READY marker exists. This is also returned
	// when CompositeInitialization is disabled, even if raw Open validated an existing
	// pre-FORMAT initialization marker.
	NoCompositeInitialization CompositeInitializationState = iota
	// CompositeInitializationStarted means this Open durably created the intent before
	// publishing FORMAT.
	CompositeInitializationStarted
	// CompositeInitializationResumed means CompositeInitialization was enabled and Open
	// found an existing valid intent, whether FORMAT already existed or not.
	CompositeInitializationResumed
)

func newID() (ID, error) {
	var id ID
	if _, err := rand.Read(id[:]); err != nil {
		return ID{}, fmt.Errorf("generate the object-store ID: %w", err)
	}
	// RFC 9562 UUID version 4 and the RFC variant make the on-disk identifier usable by
	// tools that understand UUIDs without giving those bits any storage semantics.
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	return id, nil
}

func validStoreID(id ID) bool {
	return id != (ID{}) && id[6]>>4 == 4 && id[8]>>6 == 2
}

// String renders the ID in canonical UUID form.
func (id ID) String() string {
	var encoded [36]byte
	hex.Encode(encoded[0:8], id[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], id[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], id[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], id[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], id[10:16])
	return string(encoded[:])
}

// Objects is one exclusively opened local-disk object store.
type Objects struct {
	rootPath                string
	rootFD                  int
	lockFD                  int
	objectsFD               int
	id                      ID
	newlyInitialized        bool
	compositeInitialization CompositeInitializationState
	filesystem              filesystemIdentity
	limits                  limits
	ops                     fileOperations
	gate                    *admission
	health                  *healthState
	capacity                *physicalCapacity
	keys                    *keyLocker
	recoveryRecords         atomic.Int64

	closeMu   sync.Mutex
	closeDone chan struct{}
	closeErr  error
}

var _ objectstore.Objects = (*Objects)(nil)

// Open exclusively opens an existing owner-only directory. It initializes an empty root
// or a recognized interrupted initialization. Once FORMAT exists, mandatory control
// state must remain valid. Open validates shards plus entries named by recovery records;
// ordinary final objects are validated when Get, Put, or Delete opens them. A shard or
// object that was never created remains legitimate absence.
func Open(ctx context.Context, root string, options Options) (*Objects, error) {
	return open(ctx, root, options, systemFileOperations)
}

func open(ctx context.Context, root string, options Options, ops fileOperations) (_ *Objects, returned error) {
	if err := checkContext(ctx, "open object store", ""); err != nil {
		return nil, err
	}
	settled, err := options.settle()
	if err != nil {
		return nil, err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve object-store root %q: %w", root, err)
	}

	rootFD, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open object-store root", Path: root, Err: err}
	}
	defer func() {
		if returned != nil {
			_ = unix.Close(rootFD)
		}
	}()
	if err := requirePrivateDirectory(rootFD, "object-store root", root); err != nil {
		return nil, err
	}
	rootFilesystem, err := identifyFilesystem(rootFD, ops)
	if err != nil {
		return nil, &os.PathError{Op: "identify local object-store filesystem", Path: root, Err: err}
	}
	if err := rejectRemoteFilesystem(rootFilesystem); err != nil {
		return nil, &os.PathError{Op: "validate local object-store filesystem", Path: root, Err: err}
	}
	if err := unix.Flock(rootFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			err = syscall.EBUSY
		}
		return nil, &os.PathError{Op: "lock object-store root", Path: root, Err: err}
	}

	initialized, err := hasManifest(rootFD)
	if err != nil {
		return nil, rootFailure(root, "inspect FORMAT", err)
	}
	if !initialized {
		if err := requireRecognizedInitialRoot(rootFD, root, rootFilesystem, ops); err != nil {
			return nil, err
		}
	}
	compositeInitialization, err := prepareCompositeInitialization(
		rootFD, root, initialized, options.CompositeInitialization, rootFilesystem, ops)
	if err != nil {
		return nil, err
	}

	lockFD, err := openOwnerLock(rootFD, root, initialized, rootFilesystem, ops)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returned != nil {
			_ = unix.Close(lockFD)
		}
	}()

	var id ID
	if initialized {
		id, err = readManifest(rootFD, rootFilesystem, ops)
		if err == nil {
			err = cleanupManifestStage(rootFD, rootFilesystem, ops)
		}
	} else {
		id, err = partialObjectsID(rootFD, root, rootFilesystem, ops)
		if errors.Is(err, syscall.ENOENT) {
			id, err = newID()
		}
	}
	if err != nil {
		return nil, rootFailure(root, "open FORMAT", err)
	}

	objectsFD, err := openObjectsDirectory(rootFD, root, initialized, id, rootFilesystem, ops)
	if err != nil {
		return nil, err
	}
	defer func() {
		if returned != nil {
			_ = unix.Close(objectsFD)
		}
	}()
	if err := requireNameCapacity(objectsFD, ops); err != nil {
		return nil, rootFailure(root, "check object filename capacity", err)
	}

	if err := probePublication(objectsFD, rootFilesystem, ops); err != nil {
		return nil, rootFailure(root, "probe atomic object publication", err)
	}
	if !initialized {
		if err := writeManifest(rootFD, id, rootFilesystem, ops); err != nil {
			return nil, rootFailure(root, "write FORMAT", err)
		}
	}
	if err := recoverStaging(ctx, objectsFD, id, settled, rootFilesystem, ops); err != nil {
		return nil, rootFailure(root, "recover staged objects", err)
	}

	store := &Objects{
		rootPath:                root,
		rootFD:                  rootFD,
		lockFD:                  lockFD,
		objectsFD:               objectsFD,
		id:                      id,
		newlyInitialized:        !initialized,
		compositeInitialization: compositeInitialization,
		filesystem:              rootFilesystem,
		limits:                  settled,
		ops:                     ops,
		gate:                    newAdmission(settled.maxInFlightOperations, settled.maxInFlightBytes),
		health:                  &healthState{},
		capacity:                &physicalCapacity{},
	}
	store.keys = newKeyLocker()
	return store, nil
}

// ID returns the identity persisted in FORMAT.
func (o *Objects) ID() ID { return o.id }

// NewlyInitialized reports whether this Open published FORMAT for a root that lacked it.
func (o *Objects) NewlyInitialized() bool { return o.newlyInitialized }

// CompositeInitializationState returns the immutable initialization intent state observed
// while Open held the root lock.
func (o *Objects) CompositeInitializationState() CompositeInitializationState {
	return o.compositeInitialization
}

// VerifyRootPath proves, at one instant, that the configured pathname still names the
// directory whose descriptor and lifetime lock this store holds. It is not a rename
// lease. A composite opening a pathname-based dependency must also validate that the full
// ancestor chain cannot be renamed by principals outside the store owner's trust boundary;
// same-UID and administrative mutation remain trusted deployment actions.
func (o *Objects) VerifyRootPath() error {
	ticket, err := o.gate.acquireControl(context.Background())
	if err != nil {
		return fmt.Errorf("verify object-store root path: %w", err)
	}
	defer ticket.release()
	var pinned unix.Stat_t
	if err := unix.Fstat(o.rootFD, &pinned); err != nil {
		return fmt.Errorf("stat pinned object-store root: %v: %w", err, syscall.EIO)
	}
	var named unix.Statx_t
	mask := unix.STATX_TYPE | unix.STATX_INO | unix.STATX_MNT_ID
	if err := o.ops.statx(unix.AT_FDCWD, o.rootPath,
		unix.AT_SYMLINK_NOFOLLOW|unix.AT_STATX_SYNC_AS_STAT, mask, &named); err != nil {
		return fmt.Errorf("stat configured object-store root: %v: %w", err, syscall.EIO)
	}
	if named.Mask&uint32(mask) != uint32(mask) {
		return fmt.Errorf("configured object-store root did not report type, inode, and mount identity: %w", syscall.EIO)
	}
	if named.Mode&unix.S_IFMT != unix.S_IFDIR || named.Ino != pinned.Ino || named.Mnt_id != o.filesystem.mountID {
		return fmt.Errorf("configured object-store root no longer names the locked directory: %w", syscall.EIO)
	}
	return nil
}

// Close refuses new operations, waits for admitted operations to leave, and releases the
// directory descriptors and lifetime locks. Concurrent calls receive the same result.
func (o *Objects) Close() error {
	o.closeMu.Lock()
	if o.closeDone != nil {
		done := o.closeDone
		o.closeMu.Unlock()
		<-done
		return o.closeErr
	}
	o.closeDone = make(chan struct{})
	done := o.closeDone
	o.closeMu.Unlock()

	o.gate.closeAndWait()
	err := errors.Join(
		closeFD("objects directory", o.objectsFD),
		closeFD("owner lock", o.lockFD),
		closeFD("object-store root", o.rootFD),
	)

	o.closeMu.Lock()
	o.closeErr = err
	close(done)
	o.closeMu.Unlock()
	return err
}

func closeFD(what string, fd int) error {
	if err := unix.Close(fd); err != nil {
		return fmt.Errorf("close %s: %w", what, err)
	}
	return nil
}

// Available reports a conservative payload ceiling derived from statfs after the
// maintenance reserve, active publications, and worst-case envelope/key overhead.
func (o *Objects) Available(ctx context.Context) (int64, error) {
	space, err := o.Space(ctx)
	if err != nil {
		return 0, err
	}
	return space.Avail, nil
}
