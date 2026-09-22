package smb

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

const handleIDAttempts = 16

type handleReference interface {
	Stat(context.Context) (storage.Attr, error)
	SetAttr(context.Context, storage.AttrChange) (storage.Attr, error)
	Close(context.Context) error
}

type directoryCursor struct {
	pattern       string
	observation   storage.DirectoryObservation
	entries       []storage.ObservedEntry
	next          int
	started       bool
	entryCharge   int64
	byteCharge    int64
	patternCharge int64
}

type directorySnapshotReservation struct {
	handle         *fileHandle
	mu             sync.Mutex
	entries, bytes int64
	finished       bool
}

type deleteIntentCleanup struct {
	cleanup     cleanupGate
	action      storage.FileActionID
	actionEpoch uint64
	terminal    bool
}

func checkSMBFileSession(session storage.FileSession) error {
	opener, ok := session.(storage.AtomicFileOpener)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	namespace, ok := session.(storage.NamespaceAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	directories, ok := session.(storage.DirectoryReader)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	directoryMetadata, ok := session.(storage.DirectoryMetadataObserver)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	references, ok := session.(storage.NodeReferences)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	actions, ok := session.(storage.FileActions)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	for _, check := range []func() error{
		opener.CheckAtomicFileOpen,
		namespace.CheckNamespaceAccess,
		directories.CheckDirectoryRead,
		directoryMetadata.CheckDirectoryMetadataObservation,
		references.CheckNodeReferences,
		actions.CheckFileActions,
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func checkSMBReference(reference handleReference) error {
	scoped, ok := reference.(storage.ScopedReference)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	state, ok := reference.(storage.ReferenceStateAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	name, ok := reference.(storage.ReferenceNameObserver)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	metadata, ok := reference.(storage.ReferenceMetadataAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	deletion, ok := reference.(storage.DeleteIntent)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	mutation, ok := reference.(storage.ConditionalFileMutation)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	for _, check := range []func() error{
		scoped.CheckScopedReference,
		state.CheckReferenceState,
		name.CheckReferenceNameObservation,
		metadata.CheckMetadataAccess,
		deletion.CheckDeleteIntent,
		mutation.CheckConditionalFileMutation,
	} {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

func (a *authoritySession) retainDeleteIntent(intent storage.DeleteIntentID) error {
	if intent == "" {
		return nil
	}
	if err := intent.Check(); err != nil {
		return err
	}
	a.deleteMu.Lock()
	defer a.deleteMu.Unlock()
	if a.deleteIntents == nil {
		a.deleteIntents = make(map[storage.DeleteIntentID]*deleteIntentCleanup)
	}
	if _, exists := a.deleteIntents[intent]; exists {
		return nil
	}
	if len(a.deleteIntents) >= a.export.server.config.Limits.MaxOpens {
		return syscall.ENOMEM
	}
	a.deleteIntents[intent] = &deleteIntentCleanup{}
	a.recoveryPending = true
	return nil
}

func (a *authoritySession) reconcileDeleteIntent(ctx context.Context, intent storage.DeleteIntentID, cleanup *deleteIntentCleanup) error {
	if cleanup == nil {
		a.deleteMu.Lock()
		cleanup = a.deleteIntents[intent]
		a.deleteMu.Unlock()
		if cleanup == nil {
			return syscall.EIO
		}
	}
	actions, epoch, err := a.cleanupActions(ctx, a.raw)
	if err != nil {
		return err
	}
	return a.reconcileDeleteIntentWith(ctx, actions, epoch, intent, cleanup, nil)
}

func (a *authoritySession) reconcileDeleteIntentWith(ctx context.Context, actions storage.FileActions, epoch uint64, intent storage.DeleteIntentID, cleanup *deleteIntentCleanup, known *storage.DeleteIntentStatus) error {
	return cleanup.cleanup.run(ctx, func() error {
		ctx = WithPrincipal(ctx, a.principal)
		if cleanup.terminal && (cleanup.action == "" || cleanup.actionEpoch != epoch) {
			action, err := storage.NewFileActionID(epoch)
			if err != nil {
				return err
			}
			cleanup.action = action
			cleanup.actionEpoch = epoch
		}
		if !cleanup.terminal {
			var status storage.DeleteIntentStatus
			if known != nil {
				status = *known
			} else {
				if err := a.export.server.config.Authorize.Authorize(ctx, authz.AccessRequest{
					Volume: a.export.share.Volume, Operation: storage.OpFileQueryDeleteIntent,
				}); err != nil {
					return err
				}
				var err error
				status, err = actions.QueryDeleteIntent(ctx, a.export.share.DeleteIntentOwner, intent)
				if err != nil {
					return err
				}
			}
			if err := status.Check(); err != nil || status.ID != intent {
				return syscall.EIO
			}
			switch status.Outcome {
			case storage.DeleteIntentCompleted, storage.DeleteIntentNotExecuted:
				action, err := storage.NewFileActionID(epoch)
				if err != nil {
					return err
				}
				cleanup.action = action
				cleanup.actionEpoch = epoch
				cleanup.terminal = true
			case storage.DeleteIntentCleanupFailed:
				return status.Failure
			case storage.DeleteIntentArmed, storage.DeleteIntentPending:
				return syscall.EAGAIN
			case storage.DeleteIntentUnknown, storage.DeleteIntentRetired:
				return syscall.EIO
			default:
				return syscall.EIO
			}
		}
		if err := a.export.server.config.Authorize.Authorize(ctx, authz.AccessRequest{
			Volume: a.export.share.Volume, Operation: storage.OpFileAcknowledgeDeleteIntent,
		}); err != nil {
			return err
		}
		if err := actions.AcknowledgeDeleteIntent(ctx, storage.AcknowledgeDeleteIntentCommand{
			Action: cleanup.action, Owner: a.export.share.DeleteIntentOwner, Intent: intent,
		}); err != nil {
			return err
		}
		a.deleteMu.Lock()
		if a.deleteIntents[intent] == cleanup {
			delete(a.deleteIntents, intent)
		}
		a.deleteMu.Unlock()
		return nil
	})
}

func (a *authoritySession) cleanupActions(ctx context.Context, session storage.FileSession) (storage.FileActions, uint64, error) {
	actions, ok := session.(storage.FileActions)
	if !ok {
		return nil, 0, syscall.EOPNOTSUPP
	}
	if err := actions.CheckFileActions(); err != nil {
		return nil, 0, err
	}
	ctx = WithPrincipal(ctx, a.principal)
	if err := a.export.server.config.Authorize.Authorize(ctx, authz.AccessRequest{
		Volume: a.export.share.Volume, Operation: storage.OpFileStatus,
	}); err != nil {
		return nil, 0, err
	}
	status, err := session.Status(ctx)
	if err != nil {
		return nil, 0, err
	}
	if status.Epoch == "" || status.ActionEpoch == 0 || status.Revision == 0 || status.Remaining <= 0 || status.Fenced || status.Retired {
		return nil, 0, syscall.EIO
	}
	return actions, status.ActionEpoch, nil
}

func (a *authoritySession) scanDeleteIntents(ctx context.Context, session storage.FileSession) error {
	actions, epoch, err := a.cleanupActions(ctx, session)
	if err != nil {
		return err
	}
	return a.scanDeleteIntentsWith(ctx, actions, epoch)
}

func (a *authoritySession) scanDeleteIntentsWith(ctx context.Context, actions storage.FileActions, epoch uint64) error {
	owner := a.export.share.DeleteIntentOwner
	var cursor storage.DeleteIntentCursor
	var errs []error
	sawPending := false
	for {
		if err := a.export.server.config.Authorize.Authorize(ctx, authz.AccessRequest{
			Volume: a.export.share.Volume, Operation: storage.OpFileListDeleteIntents,
		}); err != nil {
			return errors.Join(errors.Join(errs...), err)
		}
		page, err := actions.ListDeleteIntents(ctx, owner, cursor, storage.MaxDeleteIntentPageEntries)
		if err != nil {
			return errors.Join(errors.Join(errs...), err)
		}
		if err := page.Check(owner, cursor, storage.MaxDeleteIntentPageEntries); err != nil {
			return errors.Join(errors.Join(errs...), syscall.EIO)
		}
		for index := range page.Intents {
			status := page.Intents[index]
			switch status.Outcome {
			case storage.DeleteIntentCompleted, storage.DeleteIntentNotExecuted:
				if err := a.retainDeleteIntent(status.ID); err != nil {
					errs = append(errs, err)
					continue
				}
				a.deleteMu.Lock()
				cleanup := a.deleteIntents[status.ID]
				a.deleteMu.Unlock()
				if err := a.reconcileDeleteIntentWith(ctx, actions, epoch, status.ID, cleanup, &status); err != nil {
					errs = append(errs, err)
				}
			case storage.DeleteIntentCleanupFailed, storage.DeleteIntentArmed, storage.DeleteIntentPending:
				sawPending = true
			default:
				errs = append(errs, syscall.EIO)
			}
		}
		if len(page.Intents) == 0 {
			break
		}
		cursor = page.Next
	}
	a.deleteMu.Lock()
	a.recoveryPending = sawPending || len(a.deleteIntents) != 0
	remaining := make(map[storage.DeleteIntentID]*deleteIntentCleanup, len(a.deleteIntents))
	for intent, cleanup := range a.deleteIntents {
		remaining[intent] = cleanup
	}
	a.deleteMu.Unlock()
	for intent, cleanup := range remaining {
		if cleanup.terminal {
			if err := a.reconcileDeleteIntentWith(ctx, actions, epoch, intent, cleanup, nil); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (a *authoritySession) recoverDeleteIntents(ctx context.Context) error {
	a.deleteMu.Lock()
	pending := a.recoveryPending || len(a.deleteIntents) != 0
	session := a.recoverySession
	a.deleteMu.Unlock()
	if !pending && session == nil {
		return nil
	}
	ctx = WithPrincipal(ctx, a.principal)
	if session == nil {
		if err := a.export.server.config.Authorize.Authorize(ctx, authz.AccessRequest{
			Volume: a.export.share.Volume, Operation: storage.OpFileSessionOpen,
		}); err != nil {
			return err
		}
		opened, err := a.export.share.Backend.NewFileSession(ctx, a.export.server.config.Limits.FileSession)
		if err != nil {
			return err
		}
		if opened == nil {
			return syscall.EIO
		}
		a.deleteMu.Lock()
		a.recoverySession = opened
		session = opened
		a.deleteMu.Unlock()
	}
	scanErr := a.scanDeleteIntents(ctx, session)
	closeAuthErr := a.export.server.config.Authorize.Authorize(ctx, authz.AccessRequest{
		Volume: a.export.share.Volume, Operation: storage.OpFileSessionClose,
	})
	var closeErr error
	if closeAuthErr == nil {
		closeErr = session.Close(ctx)
		if closeErr == nil {
			a.deleteMu.Lock()
			if a.recoverySession == session {
				a.recoverySession = nil
			}
			a.deleteMu.Unlock()
		}
	}
	a.deleteMu.Lock()
	pending = a.recoveryPending || len(a.deleteIntents) != 0 || a.recoverySession != nil
	a.deleteMu.Unlock()
	if pending {
		scanErr = errors.Join(scanErr, syscall.EAGAIN)
	}
	return errors.Join(scanErr, closeAuthErr, closeErr)
}

type fileWorkGate struct {
	mu     sync.Mutex
	fenced bool
	active int
	limit  int
	wake   chan struct{}
}

func (g *fileWorkGate) begin() (func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.fenced {
		return nil, syscall.EIO
	}
	if g.active >= g.limit {
		return nil, syscall.EAGAIN
	}
	g.active++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.active--
			g.notifyLocked()
			g.mu.Unlock()
		})
	}, nil
}

func (g *fileWorkGate) fence() {
	g.mu.Lock()
	g.fenced = true
	g.notifyLocked()
	g.mu.Unlock()
}

func (g *fileWorkGate) wait(ctx context.Context) error {
	for {
		g.mu.Lock()
		if g.active == 0 {
			g.mu.Unlock()
			return nil
		}
		wake := g.wakeLocked()
		g.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *fileWorkGate) count() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active
}

func (g *fileWorkGate) wakeLocked() chan struct{} {
	if g.wake == nil {
		g.wake = make(chan struct{})
	}
	return g.wake
}

func (g *fileWorkGate) notifyLocked() {
	if g.wake != nil {
		close(g.wake)
		g.wake = make(chan struct{})
	}
}

type fileHandle struct {
	registry                   *handleRegistry
	authority                  *authoritySession
	reference                  handleReference
	file                       storage.File
	nodeID                     uint64
	grantedAccess, shareAccess uint32
	writeThrough               bool
	scope                      *storage.UseScope
	deleteIntent               storage.DeleteIntentID
	directoryMu                sync.Mutex
	cursorMu                   sync.Mutex
	cursor                     directoryCursor
	pendingCursor              *directorySnapshotReservation
	mu                         sync.Mutex
	cleanup                    cleanupGate
	retiring, closed           bool
	referenceClosed            bool
	closeResult                error
	closeAttr                  storage.Attr
	closeAttrSet               bool
	active                     int
	wake                       chan struct{}
}

func (h *fileHandle) beginDirectorySnapshot() (*directorySnapshotReservation, error) {
	h.cursorMu.Lock()
	if h.pendingCursor != nil {
		h.cursorMu.Unlock()
		return nil, syscall.EAGAIN
	}
	reservation := &directorySnapshotReservation{handle: h}
	h.pendingCursor = reservation
	h.cursorMu.Unlock()
	if err := reservation.reserve(0, 256); err != nil {
		h.cursorMu.Lock()
		if h.pendingCursor == reservation {
			h.pendingCursor = nil
		}
		h.cursorMu.Unlock()
		return nil, err
	}
	return reservation, nil
}

// resetDirectorySnapshot atomically replaces the retained snapshot with the
// pattern that a new authoritative capture will use. The caller serializes
// directory commands and holds a handle borrow, so cleanup cannot race this
// transition.
func (h *fileHandle) resetDirectorySnapshot(pattern string, started bool) error {
	charge := int64(len(pattern))
	registry := h.registry
	registry.mu.Lock()
	server := registry.tree.export.server
	server.mu.Lock()
	h.cursorMu.Lock()
	if h.pendingCursor != nil {
		h.cursorMu.Unlock()
		server.mu.Unlock()
		registry.mu.Unlock()
		return syscall.EAGAIN
	}
	releasedEntries := h.cursor.entryCharge
	releasedBytes := h.cursor.byteCharge + h.cursor.patternCharge
	newRegistryBytes := registry.directoryBytes - releasedBytes + charge
	newExportBytes := registry.tree.export.directoryBytes - releasedBytes + charge
	if charge > registry.limits.MaxDirectoryBytes ||
		newRegistryBytes > registry.limits.MaxDirectoryBytes ||
		newExportBytes > registry.limits.MaxDirectoryBytes {
		h.cursorMu.Unlock()
		server.mu.Unlock()
		registry.mu.Unlock()
		return syscall.ENOMEM
	}
	registry.directoryEntries -= releasedEntries
	registry.directoryBytes = newRegistryBytes
	registry.tree.export.directoryEntries -= releasedEntries
	registry.tree.export.directoryBytes = newExportBytes
	h.cursor = directoryCursor{pattern: pattern, started: started, patternCharge: charge}
	h.cursorMu.Unlock()
	server.mu.Unlock()
	registry.mu.Unlock()
	return nil
}

func (h *fileHandle) setDirectoryPattern(pattern string, started bool) error {
	return h.resetDirectorySnapshot(pattern, started)
}

func (r *directorySnapshotReservation) entryBudget(_ int, nameBytes, metadataBytes int64, _ storage.Attr) (int64, error) {
	charge, err := storage.ObservedEntryBytes(nameBytes, metadataBytes)
	if err != nil {
		return 0, err
	}
	if nameBytes > (int64(^uint64(0)>>1)-charge-128)/2 {
		return 0, syscall.EOVERFLOW
	}
	charge += 128 + 2*nameBytes
	if err := r.reserve(1, charge); err != nil {
		return 0, err
	}
	return charge, nil
}

func (r *directorySnapshotReservation) reserve(entries, bytes int64) error {
	if entries < 0 || bytes < 0 || entries == 0 && bytes == 0 {
		return syscall.EINVAL
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return syscall.EBADF
	}
	registry := r.handle.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	server := registry.tree.export.server
	server.mu.Lock()
	defer server.mu.Unlock()
	if r.entries+entries > int64(registry.limits.MaxDirectoryEntries) ||
		r.bytes+bytes > registry.limits.MaxDirectoryBytes ||
		registry.directoryEntries+entries > int64(registry.limits.MaxDirectoryEntries) ||
		registry.directoryBytes+bytes > registry.limits.MaxDirectoryBytes ||
		registry.tree.export.directoryEntries+entries > int64(registry.limits.MaxDirectoryEntries) ||
		registry.tree.export.directoryBytes+bytes > registry.limits.MaxDirectoryBytes {
		return syscall.ENOMEM
	}
	r.entries += entries
	r.bytes += bytes
	registry.directoryEntries += entries
	registry.directoryBytes += bytes
	registry.tree.export.directoryEntries += entries
	registry.tree.export.directoryBytes += bytes
	return nil
}

// commit settles a completed capture. bytes excludes the pattern already
// charged by resetDirectorySnapshot.
func (r *directorySnapshotReservation) commit(cursor directoryCursor, entries, bytes int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished || entries < 0 || bytes < 0 || entries > r.entries || bytes > r.bytes {
		return syscall.EINVAL
	}
	handle := r.handle
	registry := handle.registry
	registry.mu.Lock()
	server := registry.tree.export.server
	server.mu.Lock()
	handle.cursorMu.Lock()
	if handle.pendingCursor != r {
		handle.cursorMu.Unlock()
		server.mu.Unlock()
		registry.mu.Unlock()
		return syscall.EBADF
	}
	if cursor.pattern != handle.cursor.pattern {
		handle.cursorMu.Unlock()
		server.mu.Unlock()
		registry.mu.Unlock()
		return syscall.EINVAL
	}
	releasedEntries := r.entries - entries + handle.cursor.entryCharge
	releasedBytes := r.bytes - bytes + handle.cursor.byteCharge
	registry.directoryEntries -= releasedEntries
	registry.directoryBytes -= releasedBytes
	registry.tree.export.directoryEntries -= releasedEntries
	registry.tree.export.directoryBytes -= releasedBytes
	cursor.entryCharge = entries
	cursor.byteCharge = bytes
	cursor.patternCharge = handle.cursor.patternCharge
	handle.cursor = cursor
	handle.pendingCursor = nil
	r.finished = true
	handle.cursorMu.Unlock()
	server.mu.Unlock()
	registry.mu.Unlock()
	return nil
}

func (h *fileHandle) reserveDirectoryOutput(bytes int64) (func(), error) {
	if bytes < 0 {
		return nil, syscall.EINVAL
	}
	registry := h.registry
	registry.mu.Lock()
	server := registry.tree.export.server
	server.mu.Lock()
	if registry.directoryBytes+bytes > registry.limits.MaxDirectoryBytes ||
		registry.tree.export.directoryBytes+bytes > registry.limits.MaxDirectoryBytes {
		server.mu.Unlock()
		registry.mu.Unlock()
		return nil, syscall.ENOMEM
	}
	registry.directoryBytes += bytes
	registry.tree.export.directoryBytes += bytes
	server.mu.Unlock()
	registry.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			registry.mu.Lock()
			server.mu.Lock()
			registry.directoryBytes -= bytes
			registry.tree.export.directoryBytes -= bytes
			server.mu.Unlock()
			registry.mu.Unlock()
		})
	}, nil
}

func (r *directorySnapshotReservation) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finished {
		return
	}
	handle := r.handle
	registry := handle.registry
	registry.mu.Lock()
	server := registry.tree.export.server
	server.mu.Lock()
	handle.cursorMu.Lock()
	registry.directoryEntries -= r.entries
	registry.directoryBytes -= r.bytes
	registry.tree.export.directoryEntries -= r.entries
	registry.tree.export.directoryBytes -= r.bytes
	if handle.pendingCursor == r {
		handle.pendingCursor = nil
	}
	r.finished = true
	handle.cursorMu.Unlock()
	server.mu.Unlock()
	registry.mu.Unlock()
}

func (h *fileHandle) clearDirectoryCursor() {
	h.cursorMu.Lock()
	pending := h.pendingCursor
	h.cursorMu.Unlock()
	if pending != nil {
		pending.release()
	}
	registry := h.registry
	registry.mu.Lock()
	server := registry.tree.export.server
	server.mu.Lock()
	h.cursorMu.Lock()
	registry.directoryEntries -= h.cursor.entryCharge
	registry.directoryBytes -= h.cursor.byteCharge + h.cursor.patternCharge
	registry.tree.export.directoryEntries -= h.cursor.entryCharge
	registry.tree.export.directoryBytes -= h.cursor.byteCharge + h.cursor.patternCharge
	h.cursor = directoryCursor{}
	h.cursorMu.Unlock()
	server.mu.Unlock()
	registry.mu.Unlock()
}

type handleRegistry struct {
	tree                                              *tree
	limits                                            Limits
	work                                              fileWorkGate
	mu                                                sync.Mutex
	handles                                           map[wire.FileID]*fileHandle
	reservations                                      map[*openReservation]struct{}
	reservedIDs                                       map[wire.FileID]struct{}
	slots                                             int
	openResultBytes, directoryEntries, directoryBytes int64
	retired                                           bool
	random                                            func([]byte) (int, error)
}

type openReservation struct {
	registry *handleRegistry
	id       wire.FileID

	mu              sync.Mutex
	reference       handleReference
	file            storage.File
	aux             []handleReference
	attr            storage.Attr
	outcome         storage.OpenOutcome
	scope           *storage.UseScope
	deleteIntent    storage.DeleteIntentID
	intentRetained  bool
	writeThrough    bool
	attached        bool
	installed       bool
	finished        bool
	cleanupOnly     bool
	settled         bool
	resultErr       error
	responseDone    chan struct{}
	cleanup         cleanupGate
	charge          int64
	referenceClosed bool
	closeResult     error
}

func newHandleRegistry(tree *tree, limits Limits) *handleRegistry {
	return &handleRegistry{
		tree: tree, limits: limits, work: fileWorkGate{limit: limits.MaxRequests},
		handles: make(map[wire.FileID]*fileHandle), reservations: make(map[*openReservation]struct{}),
		reservedIDs: make(map[wire.FileID]struct{}), random: rand.Read,
	}
}

func maxOpenResultCharge() int64 {
	charge, err := storage.MetadataRetentionBytes(storage.MaxMetadataBytes)
	if err != nil {
		panic(err)
	}
	return charge + 512
}

func (t *tree) beginFileWork() (func(), error) {
	if t.files == nil || t.session == nil {
		return nil, syscall.EBADF
	}
	t.session.mu.Lock()
	defer t.session.mu.Unlock()
	if t.session.retired || t.session.trees[t.id] != t {
		return nil, syscall.EBADF
	}
	return t.files.work.begin()
}

func (r *handleRegistry) get(id wire.FileID) *fileHandle {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.handles[id]
}

func (r *handleRegistry) reserve() (*openReservation, error) {
	return r.reserveResult(false)
}

func (r *handleRegistry) reserveResponse() (*openReservation, error) {
	return r.reserveResult(true)
}

func (r *handleRegistry) reserveResult(retainResponse bool) (*openReservation, error) {
	authority := r.tree.authority
	authority.installMu.RLock()
	defer authority.installMu.RUnlock()
	if authority.stopping || authority.closed || !time.Now().Before(authority.deadline) {
		return nil, syscall.EIO
	}

	charge := maxOpenResultCharge()

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.slots >= r.limits.MaxOpens || charge > r.limits.MaxOpenResultBytes-r.openResultBytes {
		return nil, syscall.ENOMEM
	}
	server := r.tree.export.server
	server.mu.Lock()
	defer server.mu.Unlock()
	if r.tree.export.opens >= r.limits.MaxOpens ||
		charge > r.limits.MaxOpenResultBytes-r.tree.export.openResultBytes {
		return nil, syscall.ENOMEM
	}
	id, err := r.newFileIDLocked()
	if err != nil {
		return nil, err
	}

	reservation := &openReservation{
		registry: r, id: id,
		charge: charge,
	}
	if retainResponse {
		reservation.responseDone = make(chan struct{})
	}
	r.slots++
	r.openResultBytes += charge
	r.reservations[reservation] = struct{}{}
	r.reservedIDs[id] = struct{}{}
	r.tree.export.opens++
	r.tree.export.openResultBytes += charge
	return reservation, nil
}

func (r *handleRegistry) newFileIDLocked() (wire.FileID, error) {
	for range handleIDAttempts {
		var id wire.FileID
		if _, err := r.random(id[:]); err != nil {
			return wire.FileID{}, err
		}
		if id == (wire.FileID{}) {
			continue
		}
		_, handleExists := r.handles[id]
		_, reservationExists := r.reservedIDs[id]
		if !handleExists && !reservationExists {
			return id, nil
		}
	}
	return wire.FileID{}, syscall.EAGAIN
}

func (p *openReservation) attachFile(result storage.OpenResult) {
	p.attach(result.File, result.File, result.Attr, result.Outcome)
}

func (p *openReservation) attachNode(result storage.NodeOpenResult) {
	p.attach(result.Reference, nil, result.Attr, result.Outcome)
}

func (p *openReservation) adoptAux(reference handleReference) {
	if reference == nil {
		panic("SMB open reservation adopted a nil reference")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished || p.installed {
		panic("SMB open reservation adopted a reference after ownership settled")
	}
	p.aux = append(p.aux, reference)
}

func (p *openReservation) attach(reference handleReference, file storage.File, attr storage.Attr, outcome storage.OpenOutcome) {
	p.mu.Lock()
	settled := p.settled
	p.mu.Unlock()
	metadataBytes, err := storage.MetadataSize(attr.Metadata)
	if err == nil && !settled {
		err = p.settleResult(attr, int64(metadataBytes))
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.attached {
		panic("SMB open reservation attached more than once")
	}
	p.reference = reference
	p.file = file
	p.attr = attr
	p.outcome = outcome
	p.attached = true
	if err != nil {
		p.resultErr = err
		p.cleanupOnly = reference != nil
	}
}

func (p *openReservation) resultBudget(attr storage.Attr, metadataBytes int64) error {
	return p.settleResult(attr, metadataBytes)
}

func (p *openReservation) settleResult(_ storage.Attr, metadataBytes int64) error {
	charge, err := storage.MetadataRetentionBytes(metadataBytes)
	if err != nil {
		return err
	}
	charge += 512
	registry := p.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished || p.settled {
		return syscall.EINVAL
	}
	if charge > p.charge {
		return syscall.EFBIG
	}
	delta := p.charge - charge
	p.charge = charge
	p.settled = true
	registry.openResultBytes -= delta
	server := registry.tree.export.server
	server.mu.Lock()
	registry.tree.export.openResultBytes -= delta
	server.mu.Unlock()
	return nil
}

func (p *openReservation) setScope(scope storage.UseScope) {
	p.mu.Lock()
	defer p.mu.Unlock()
	copy := scope
	p.scope = &copy
}

func (p *openReservation) setDeleteIntent(intent storage.DeleteIntentID) {
	p.mu.Lock()
	p.deleteIntent = intent
	p.mu.Unlock()
	if intent != "" {
		p.registry.tree.authority.deleteMu.Lock()
		p.registry.tree.authority.deleteProduced = true
		p.registry.tree.authority.deleteMu.Unlock()
	}
}

func (p *openReservation) discardDeleteIntent() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.intentRetained {
		panic("SMB open reservation discarded retained delete intent")
	}
	p.deleteIntent = ""
}

func (p *openReservation) setWriteThrough(enabled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writeThrough = enabled
}

func (p *openReservation) install(access, share uint32) (wire.FileID, error) {
	registry := p.registry
	authority := registry.tree.authority
	authority.installMu.RLock()
	defer authority.installMu.RUnlock()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished || p.installed || !p.attached || p.reference == nil || p.attr.ID == 0 {
		return wire.FileID{}, syscall.EINVAL
	}
	if p.resultErr != nil {
		p.cleanupOnly = true
		return wire.FileID{}, p.resultErr
	}
	if authority.stopping || authority.closed || !time.Now().Before(authority.deadline) {
		p.cleanupOnly = true
		return wire.FileID{}, syscall.EIO
	}
	if p.deleteIntent != "" && !p.intentRetained {
		if err := authority.retainDeleteIntent(p.deleteIntent); err != nil {
			p.cleanupOnly = true
			return wire.FileID{}, err
		}
		p.intentRetained = true
	}
	registry.handles[p.id] = &fileHandle{
		registry: registry, authority: authority, reference: p.reference, file: p.file,
		nodeID: p.attr.ID, grantedAccess: access, shareAccess: share,
		writeThrough: p.writeThrough, scope: p.scope, deleteIntent: p.deleteIntent, wake: make(chan struct{}),
	}
	p.reference = nil
	p.file = nil
	p.scope = nil
	p.installed = true
	delete(registry.reservedIDs, p.id)
	return p.id, nil
}

// The original open result remains charged until response construction no
// longer reads it, including when cleanup begins concurrently.
func (p *openReservation) releaseResponse() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.responseDone != nil {
		close(p.responseDone)
		p.responseDone = nil
	}
}

func (p *openReservation) finish(ctx context.Context) error {
	return p.cleanup.run(ctx, func() error {
		p.mu.Lock()
		responseDone := p.responseDone
		p.mu.Unlock()
		if responseDone != nil {
			select {
			case <-responseDone:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		p.mu.Lock()
		if p.finished {
			p.mu.Unlock()
			return nil
		}
		reference, installed, intent := p.reference, p.installed, p.deleteIntent
		referenceClosed, closeResult := p.referenceClosed, p.closeResult
		aux := append([]handleReference(nil), p.aux...)
		p.mu.Unlock()

		authorityClosed := p.registry.tree.authority.isClosed()
		var semanticErrs, cleanupErrs []error
		if !installed && intent != "" && !authorityClosed {
			if err := p.ensureDeleteIntentRetained(); err != nil {
				p.mu.Lock()
				p.cleanupOnly = true
				p.mu.Unlock()
				return err
			}
		}
		if !installed && reference != nil && !referenceClosed && !authorityClosed {
			result, err := storage.CloseReference(ctx, reference)
			if !result.Released && err == nil {
				err = syscall.EIO
			}
			closeResult = err
			if result.Released {
				p.mu.Lock()
				p.referenceClosed = true
				p.closeResult = closeResult
				p.mu.Unlock()
				if err != nil {
					semanticErrs = append(semanticErrs, err)
				}
			} else {
				cleanupErrs = append(cleanupErrs, err)
			}
		} else if !installed && reference != nil && authorityClosed {
			p.mu.Lock()
			p.referenceClosed = true
			p.mu.Unlock()
		}
		p.mu.Lock()
		referenceClosed, closeResult = p.referenceClosed, p.closeResult
		p.mu.Unlock()
		if !installed && (reference == nil || referenceClosed) && intent != "" && !authorityClosed {
			if err := p.registry.tree.authority.reconcileDeleteIntent(ctx, intent, nil); err != nil {
				cleanupErrs = append(cleanupErrs, err)
			}
		}
		failedAux := make([]bool, len(aux))
		for index := len(aux) - 1; index >= 0; index-- {
			var result storage.ReferenceCloseResult
			var err error
			if !authorityClosed {
				result, err = storage.CloseReference(ctx, aux[index])
			} else {
				result.Released = true
			}
			if !result.Released && err == nil {
				err = syscall.EIO
			}
			if !result.Released {
				cleanupErrs = append(cleanupErrs, err)
				failedAux[index] = true
			} else if err != nil {
				semanticErrs = append(semanticErrs, err)
			}
		}
		if err := errors.Join(cleanupErrs...); err != nil {
			remainingAux := make([]handleReference, 0, len(aux))
			for index, reference := range aux {
				if failedAux[index] {
					remainingAux = append(remainingAux, reference)
				}
			}
			p.mu.Lock()
			p.aux = remainingAux
			p.cleanupOnly = true
			p.mu.Unlock()
			return errors.Join(errors.Join(semanticErrs...), err)
		}

		registry := p.registry
		registry.mu.Lock()
		p.mu.Lock()
		if !p.finished {
			p.reference = nil
			p.file = nil
			p.aux = nil
			p.scope = nil
			p.finished = true
			p.cleanupOnly = false
			delete(registry.reservations, p)
			delete(registry.reservedIDs, p.id)
			charge := p.charge
			registry.openResultBytes -= charge
			p.charge = 0
			server := registry.tree.export.server
			server.mu.Lock()
			registry.tree.export.openResultBytes -= charge
			server.mu.Unlock()
			if !p.installed || registry.handles[p.id] == nil {
				registry.slots--
				server.mu.Lock()
				registry.tree.export.opens--
				server.mu.Unlock()
			}
		}
		p.mu.Unlock()
		registry.mu.Unlock()
		return errors.Join(semanticErrs...)
	})
}

func (p *openReservation) ensureDeleteIntentRetained() error {
	p.mu.Lock()
	if p.deleteIntent == "" || p.intentRetained {
		p.mu.Unlock()
		return nil
	}
	intent := p.deleteIntent
	p.mu.Unlock()
	if err := p.registry.tree.authority.retainDeleteIntent(intent); err != nil {
		return err
	}
	p.mu.Lock()
	p.intentRetained = true
	p.mu.Unlock()
	return nil
}

func (h *fileHandle) borrow() (func(), error) {
	authority := h.authority
	authority.installMu.RLock()
	h.registry.mu.Lock()
	h.mu.Lock()
	if authority.stopping || authority.closed || !time.Now().Before(authority.deadline) || h.retiring || h.closed {
		h.mu.Unlock()
		h.registry.mu.Unlock()
		authority.installMu.RUnlock()
		return nil, syscall.EBADF
	}
	h.active++
	h.mu.Unlock()
	h.registry.mu.Unlock()
	authority.installMu.RUnlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			h.active--
			h.notifyLocked()
			h.mu.Unlock()
		})
	}, nil
}

func (h *fileHandle) close(ctx context.Context) error {
	_, err := h.closeWithAttr(ctx, false)
	return err
}

func (h *fileHandle) closeWithAttr(ctx context.Context, capture bool) (storage.Attr, error) {
	err := h.cleanup.run(ctx, func() error {
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return nil
		}
		h.retiring = true
		h.mu.Unlock()
		if err := h.waitIdle(ctx); err != nil {
			return err
		}

		h.mu.Lock()
		referenceClosed, closeResult, intent := h.referenceClosed, h.closeResult, h.deleteIntent
		h.mu.Unlock()
		authorityClosed := h.authority.isClosed()
		var captureErr error
		var cleanupErrs []error
		if capture && !referenceClosed && !authorityClosed {
			attr, err := h.reference.Stat(ctx)
			if err != nil {
				captureErr = err
			} else {
				h.mu.Lock()
				h.closeAttr = attr
				h.closeAttrSet = true
				h.mu.Unlock()
			}
		}
		if !referenceClosed && !authorityClosed {
			result, err := storage.CloseReference(ctx, h.reference)
			if !result.Released && err == nil {
				err = syscall.EIO
			}
			closeResult = err
			if result.Released {
				h.mu.Lock()
				h.referenceClosed = true
				h.closeResult = closeResult
				h.mu.Unlock()
				captureErr = errors.Join(captureErr, closeResult)
			} else {
				cleanupErrs = append(cleanupErrs, closeResult)
			}
		} else if authorityClosed {
			h.mu.Lock()
			h.referenceClosed = true
			h.mu.Unlock()
		}
		h.mu.Lock()
		referenceClosed = h.referenceClosed
		closeResult = h.closeResult
		h.mu.Unlock()
		if referenceClosed && intent != "" && !authorityClosed {
			if err := h.authority.reconcileDeleteIntent(ctx, intent, nil); err != nil {
				cleanupErrs = append(cleanupErrs, err)
			}
		}
		if err := errors.Join(cleanupErrs...); err != nil {
			return errors.Join(captureErr, err)
		}
		h.mu.Lock()
		h.closed = true
		h.reference = nil
		h.file = nil
		h.scope = nil
		h.deleteIntent = ""
		h.mu.Unlock()
		h.clearDirectoryCursor()
		return captureErr
	})
	h.mu.Lock()
	attr := h.closeAttr
	h.mu.Unlock()
	return attr, err
}

func (h *fileHandle) waitIdle(ctx context.Context) error {
	for {
		h.mu.Lock()
		if h.active == 0 {
			h.mu.Unlock()
			return nil
		}
		wake := h.wake
		h.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (h *fileHandle) notifyLocked() {
	close(h.wake)
	h.wake = make(chan struct{})
}

func (h *fileHandle) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

func (r *handleRegistry) fence() {
	r.mu.Lock()
	if !r.retired {
		r.retired = true
	}
	r.mu.Unlock()
	r.work.fence()
}

func (r *handleRegistry) close(ctx context.Context) error {
	r.fence()
	if err := r.work.wait(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	reservations := make([]*openReservation, 0, len(r.reservations))
	for reservation := range r.reservations {
		reservations = append(reservations, reservation)
	}
	handles := make(map[wire.FileID]*fileHandle, len(r.handles))
	for id, handle := range r.handles {
		handles[id] = handle
	}
	r.mu.Unlock()

	var errs []error
	for _, reservation := range reservations {
		if err := reservation.finish(ctx); err != nil {
			reservation.mu.Lock()
			finished := reservation.finished
			reservation.mu.Unlock()
			if !finished {
				errs = append(errs, err)
			}
		}
	}
	for id, handle := range handles {
		if err := r.closeID(ctx, id, handle); err != nil && !handle.isClosed() {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (r *handleRegistry) closeID(ctx context.Context, id wire.FileID, handle *fileHandle) error {
	_, err := r.closeIDWithAttr(ctx, id, handle, false)
	return err
}

func (r *handleRegistry) closeIDWithAttr(ctx context.Context, id wire.FileID, handle *fileHandle, capture bool) (storage.Attr, error) {
	if handle == nil {
		return storage.Attr{}, syscall.EBADF
	}
	attr, err := handle.closeWithAttr(ctx, capture)
	if err != nil && !handle.isClosed() {
		return attr, err
	}
	r.mu.Lock()
	if r.handles[id] == handle {
		delete(r.handles, id)
		reservationOwnsSlot := false
		for reservation := range r.reservations {
			reservation.mu.Lock()
			owns := reservation.id == id && reservation.installed && !reservation.finished
			reservation.mu.Unlock()
			if owns {
				reservationOwnsSlot = true
				break
			}
		}
		if !reservationOwnsSlot {
			r.slots--
			server := r.tree.export.server
			server.mu.Lock()
			r.tree.export.opens--
			server.mu.Unlock()
		} else {
			r.reservedIDs[id] = struct{}{}
		}
	}
	r.mu.Unlock()
	return attr, err
}

func (r *handleRegistry) addStatus(status *Status) {
	status.ActiveFileOperations += r.work.count()
	r.mu.Lock()
	status.OpenReservations += len(r.reservations)
	status.InstalledHandles += len(r.handles)
	status.OpenResultBytes += r.openResultBytes
	status.DirectoryEntries += r.directoryEntries
	status.DirectoryBytes += r.directoryBytes
	retired := r.retired
	reservations := make([]*openReservation, 0, len(r.reservations))
	for reservation := range r.reservations {
		reservations = append(reservations, reservation)
	}
	handles := make([]*fileHandle, 0, len(r.handles))
	for _, handle := range r.handles {
		handles = append(handles, handle)
	}
	r.mu.Unlock()
	for _, reservation := range reservations {
		reservation.mu.Lock()
		if retired || reservation.cleanupOnly {
			status.CleanupPendingHandles++
		}
		reservation.mu.Unlock()
	}
	for _, handle := range handles {
		handle.mu.Lock()
		if retired || handle.retiring {
			status.CleanupPendingHandles++
		}
		handle.mu.Unlock()
	}
}
