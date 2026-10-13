package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Native output structures retain the SDK's LARGE_INTEGER alignment.
// https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_standard_info
type NativeProbeFileStandardInfo struct {
	AllocationSize, EndOfFile int64
	NumberOfLinks             uint32
	DeletePending, Directory  uint8
}
type NativeProbeFileIDInfo struct {
	VolumeSerial uint64
	FileID       [16]byte
}

func TestNativeProbeInformationBufferLayout(t *testing.T) {
	var standard NativeProbeFileStandardInfo
	if unsafe.Sizeof(standard) != 24 || unsafe.Alignof(standard) != 8 || unsafe.Offsetof(standard.NumberOfLinks) != 16 || unsafe.Offsetof(standard.DeletePending) != 20 || unsafe.Offsetof(standard.Directory) != 21 || uintptr(unsafe.Pointer(&standard))%8 != 0 {
		t.Fatal("invalid FILE_STANDARD_INFO native layout or alignment")
	}
	var identity NativeProbeFileIDInfo
	if unsafe.Sizeof(identity) != 24 || unsafe.Alignof(identity) != 8 || unsafe.Offsetof(identity.FileID) != 8 || uintptr(unsafe.Pointer(&identity))%8 != 0 {
		t.Fatal("invalid FILE_ID_INFO native layout or alignment")
	}
}

// NewNativeDeleteProbeBackend models only the native probe's mutable namespace.
// Persistence and process retirement are exercised against SQLite separately.
func NewNativeDeleteProbeBackend() *nativeDeleteBackend {
	b := NewNativeDirectoryProbeBackend().(*nativeDirectoryBackend)
	b.views[1] = append(b.views[1], storage.Entry{Name: "empty", Attr: b.views[1][2].Attr.Clone()})
	b.views[1][4].Attr.ID = 7
	b.views[7] = nil
	return &nativeDeleteBackend{nativeDirectoryBackend: b, refs: make(map[*nativeDeleteReference]struct{}),
		intents: make(map[storage.DeleteIntentID]*nativeDeleteIntent), actions: make(map[storage.FileActionID]nativeDeleteAction), nextNode: 100,
		ordinary: make(map[uint64]bool), generations: make(map[uint64]uint64), revision: 1}
}

type nativeDeleteIntent struct {
	storage.DeleteIntentStatus
	owner  storage.DeleteIntentOwner
	cursor storage.DeleteIntentCursor
}

type nativeDeleteAction struct {
	storage.FileActionReceipt
	failure error
}

type nativeDeleteBackend struct {
	*nativeDirectoryBackend
	mu           sync.Mutex
	refs         map[*nativeDeleteReference]struct{}
	intents      map[storage.DeleteIntentID]*nativeDeleteIntent
	actions      map[storage.FileActionID]nativeDeleteAction
	sequence     storage.DeleteIntentCursor
	nextNode     uint64
	acknowledged []storage.DeleteIntentStatus
	ordinary     map[uint64]bool
	generations  map[uint64]uint64
	revision     uint64
}

func (b *nativeDeleteBackend) NewFileSession(ctx context.Context, options storage.FileSessionOptions) (storage.FileSession, error) {
	raw, err := b.nativeDirectoryBackend.NewFileSession(ctx, options)
	if err != nil {
		return nil, err
	}
	return &nativeDeleteSession{nativeDirectorySession: raw.(*nativeDirectorySession), mutable: b}, nil
}

type nativeDeleteSession struct {
	*nativeDirectorySession
	mutable *nativeDeleteBackend
}

func (s *nativeDeleteSession) ObserveDirectoryMetadata(ctx context.Context, target storage.DirectoryTarget, options storage.DirectoryMetadataOptions, result *storage.ListResult) (storage.DirectoryMetadataObservation, error) {
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	if err := options.Check(); err != nil {
		result.Fail(err)
		return storage.DirectoryMetadataObservation{}, err
	}
	if err := s.checkGuards(options.Guards); err != nil {
		result.Fail(err)
		return storage.DirectoryMetadataObservation{}, err
	}
	observation, err := s.directory(ctx, target, result)
	out := storage.DirectoryMetadataObservation{Observation: observation}
	if err == nil && options.IncludeName {
		name, nameErr := s.mutable.name(ctx, target.NodeID)
		if nameErr != nil {
			result.Fail(nameErr)
			return storage.DirectoryMetadataObservation{}, nameErr
		}
		out.Name = &name
	}
	return out, err
}

func (s *nativeDeleteSession) ReadDirNodeBounded(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (storage.DirectoryObservation, error) {
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	return s.directory(ctx, target, result)
}

func (s *nativeDeleteSession) directory(ctx context.Context, target storage.DirectoryTarget, result *storage.ListResult) (out storage.DirectoryObservation, returned error) {
	defer func() {
		if returned != nil {
			result.Fail(returned)
		}
	}()
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if err := target.Check(); err != nil {
		return out, err
	}
	if target.Scope != nil {
		s.refMu.Lock()
		ref := s.references[target.Scope.Token]
		valid := ref != nil && ref.id == target.NodeID && !ref.closed
		allowed := valid && ref.use.Uses&storage.ReadEntries != 0
		s.refMu.Unlock()
		if !valid {
			return out, storage.ErrInvalidScope
		}
		if !allowed {
			return out, syscall.EACCES
		}
	}
	entries, found := s.backend.views[target.NodeID]
	if !found {
		return out, syscall.ENOTDIR
	}
	for _, entry := range entries {
		if err := result.Add(entry); err != nil {
			return out, err
		}
	}
	return storage.DirectoryObservation{ParentID: target.NodeID, Revision: s.mutable.revisionToken()}, nil
}

func (s *nativeDeleteSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if id != 1 || options.CloseIntent != nil {
		return storage.NodeOpenResult{}, syscall.EINVAL
	}
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	opened, err := s.nativeDirectorySession.OpenNodeRef(ctx, id, options)
	if err != nil {
		return opened, err
	}
	ref := &nativeDeleteReference{nativeDirectoryReference: opened.Reference.(*nativeDirectoryReference), owner: s}
	s.mutable.refs[ref] = struct{}{}
	opened.Reference = ref
	return opened, nil
}

func (s *nativeDeleteSession) OpenChildRef(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if err := options.Check(); err != nil {
		return storage.NodeOpenResult{}, err
	}
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	ref, attr, outcome, err := s.open(ctx, selection, options)
	return storage.NodeOpenResult{Reference: ref, Attr: attr, Outcome: outcome}, err
}

func (s *nativeDeleteSession) OpenAt(ctx context.Context, selection storage.ChildSelection, options storage.OpenAtOptions) (storage.OpenResult, error) {
	if err := options.Check(); err != nil {
		return storage.OpenResult{}, err
	}
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	if options.Existing != storage.Keep && options.Existing != storage.ResetContent {
		return storage.OpenResult{}, syscall.EOPNOTSUPP
	}
	ref, attr, outcome, err := s.open(ctx, selection, storage.NodeRefOptions{Kind: storage.NodeRegular,
		Target: options.Target, Action: options.Action, Use: options.Use, MetadataAccess: options.MetadataAccess,
		Create: options.Create, Exclusive: options.Exclusive, InitialState: options.Initial, CloseIntent: options.CloseIntent})
	if err != nil {
		return storage.OpenResult{}, err
	}
	if options.Existing == storage.ResetContent && outcome == storage.Opened {
		attr.Size = 0
		attr.AllocationSize = 0
		attr.Metadata = cloneNativeInitialMetadata(options.Initial.OnReset.Metadata)
		s.mutable.updateAttr(attr)
		outcome = storage.Reset
	}
	s.mutable.actions[options.Action] = nativeDeleteAction{FileActionReceipt: storage.FileActionReceipt{Action: options.Action, Operation: storage.OpFileOpenAt, Outcome: storage.FileActionCompleted}}
	return storage.OpenResult{File: &nativeDeleteFile{nativeDeleteReference: ref, readable: options.Read, writable: options.Write}, Attr: attr, Outcome: outcome}, nil
}

func (s *nativeDeleteSession) open(ctx context.Context, selection storage.ChildSelection, options storage.NodeRefOptions) (*nativeDeleteReference, storage.Attr, storage.OpenOutcome, error) {
	if err := ctx.Err(); err != nil {
		return nil, storage.Attr{}, 0, err
	}
	if err := s.checkGuards(selection.Guards); err != nil {
		return nil, storage.Attr{}, 0, err
	}
	b := s.mutable
	parent, name := selection.Name.Parent.NodeID, string(selection.Name.RawLeaf)
	entries, found := b.views[parent]
	if !found {
		return nil, storage.Attr{}, 0, syscall.ENOTDIR
	}
	var attr storage.Attr
	for _, entry := range entries {
		if entry.Name == name {
			attr = entry.Attr.Clone()
			break
		}
	}
	if options.Target.State == storage.Absent && attr.ID != 0 || options.Target.State == storage.SameNode && attr.ID != options.Target.NodeID {
		return nil, storage.Attr{}, 0, storage.ErrConditionConflict
	}
	if attr.ID != 0 && options.Exclusive {
		return nil, storage.Attr{}, 0, syscall.EEXIST
	}
	if attr.ID != 0 && attr.Kind != options.Kind {
		return nil, storage.Attr{}, 0, syscall.EINVAL
	}
	for key, expected := range options.Target.ExpectedMetadata {
		actual, exists := attr.Metadata[key]
		if len(expected) == 0 && exists || len(expected) != 0 && (!exists || !bytes.Equal(actual.Version, expected)) {
			return nil, storage.Attr{}, 0, storage.ErrConditionConflict
		}
	}
	for _, intent := range b.intents {
		if intent.NodeID == attr.ID && intent.Outcome == storage.DeleteIntentPending {
			return nil, storage.Attr{}, 0, storage.ErrPendingDelete
		}
	}
	if b.ordinary[attr.ID] {
		return nil, storage.Attr{}, 0, storage.ErrPendingDelete
	}
	for ref := range b.refs {
		if ref.id == attr.ID && (ref.use.Deny&options.Use.Uses != 0 || options.Use.Deny&ref.use.Uses != 0) {
			return nil, storage.Attr{}, 0, storage.ErrUseConflict
		}
	}
	if options.CloseIntent != nil {
		if options.Use.Uses&storage.DeleteName == 0 || options.CloseIntent.Trigger != storage.OnReferenceClose {
			return nil, storage.Attr{}, 0, syscall.EACCES
		}
		if _, exists := b.intents[options.CloseIntent.ID]; exists {
			return nil, storage.Attr{}, 0, syscall.EEXIST
		}
	}
	outcome := storage.Opened
	if attr.ID == 0 {
		if !options.Create {
			return nil, storage.Attr{}, 0, syscall.ENOENT
		}
		instant := time.Unix(1700000000, 0).UTC()
		attr = storage.Attr{ID: b.nextNode + 1, Kind: options.Kind, AllocationKnown: true, BirthTime: &instant, ChangeTime: &instant, AccessTime: instant, ModTime: instant}
		if options.InitialState.OnCreate.Metadata != nil {
			attr.Metadata = make(map[string]storage.OpaquePayload)
			for key, value := range options.InitialState.OnCreate.Metadata {
				attr.Metadata[key] = storage.OpaquePayload{Version: []byte("native-create"), Data: append([]byte(nil), value...)}
			}
		}
		outcome = storage.Created
	}
	metadataBytes, err := storage.MetadataSize(attr.Metadata)
	if err != nil {
		return nil, storage.Attr{}, 0, err
	}
	scalar := attr
	scalar.Metadata = nil
	if err := storage.CheckAttrResultBudget(ctx, scalar, int64(metadataBytes)); err != nil {
		return nil, storage.Attr{}, 0, err
	}
	if outcome == storage.Created {
		b.nextNode = attr.ID
		b.views[parent] = append(entries, storage.Entry{Name: name, Attr: attr})
		if options.Kind == storage.NodeDirectory {
			b.views[attr.ID] = nil
		}
		b.revision++
	}
	s.refMu.Lock()
	s.sequence++
	token := fmt.Sprintf("native-delete-%d", s.sequence)
	base := &nativeDirectoryReference{session: s.nativeDirectorySession, id: attr.ID, attr: attr, token: token, metadata: options.MetadataAccess, use: options.Use}
	s.references[token] = base
	s.refMu.Unlock()
	ref := &nativeDeleteReference{nativeDirectoryReference: base, owner: s, parent: parent, name: name}
	b.refs[ref] = struct{}{}
	if intent := options.CloseIntent; intent != nil {
		b.sequence++
		ref.intent = intent.ID
		b.intents[intent.ID] = &nativeDeleteIntent{DeleteIntentStatus: storage.DeleteIntentStatus{ID: intent.ID, NodeID: attr.ID, Outcome: storage.DeleteIntentArmed}, owner: intent.Owner, cursor: b.sequence}
	}
	b.actions[options.Action] = nativeDeleteAction{FileActionReceipt: storage.FileActionReceipt{Action: options.Action, Operation: storage.OpFileOpenChildRef, Outcome: storage.FileActionCompleted}}
	return ref, attr.Clone(), outcome, nil
}

func (s *nativeDeleteSession) QueryFileAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if result, found := s.mutable.actions[id]; found {
		return result.FileActionReceipt, nil
	}
	return storage.FileActionReceipt{Action: id, Outcome: storage.FileActionUnknown}, nil
}

func (s *nativeDeleteSession) QueryDeleteIntent(ctx context.Context, owner storage.DeleteIntentOwner, id storage.DeleteIntentID) (storage.DeleteIntentStatus, error) {
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.DeleteIntentStatus{}, err
	}
	if intent := s.mutable.intents[id]; intent != nil && intent.owner == owner {
		return intent.DeleteIntentStatus, nil
	}
	return storage.DeleteIntentStatus{ID: id, Outcome: storage.DeleteIntentUnknown}, nil
}

func (s *nativeDeleteSession) ListDeleteIntents(ctx context.Context, owner storage.DeleteIntentOwner, after storage.DeleteIntentCursor, limit int) (storage.DeleteIntentPage, error) {
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.DeleteIntentPage{}, err
	}
	if limit <= 0 || limit > storage.MaxDeleteIntentPageEntries {
		return storage.DeleteIntentPage{}, syscall.EINVAL
	}
	var ordered []*nativeDeleteIntent
	for _, intent := range s.mutable.intents {
		if intent.owner == owner && intent.cursor > after {
			ordered = append(ordered, intent)
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].cursor < ordered[j].cursor })
	page := storage.DeleteIntentPage{Next: after}
	for _, intent := range ordered[:min(limit, len(ordered))] {
		page.Intents = append(page.Intents, intent.DeleteIntentStatus)
		page.Next = intent.cursor
	}
	return page, nil
}

func (s *nativeDeleteSession) AcknowledgeDeleteIntent(ctx context.Context, command storage.AcknowledgeDeleteIntentCommand) error {
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := command.Action.Check(); err != nil {
		return err
	}
	if intent := s.mutable.intents[command.Intent]; intent != nil {
		if intent.owner != command.Owner {
			return syscall.EACCES
		}
		if intent.Outcome != storage.DeleteIntentCompleted && intent.Outcome != storage.DeleteIntentNotExecuted {
			return syscall.EBUSY
		}
		s.mutable.acknowledged = append(s.mutable.acknowledged, intent.DeleteIntentStatus)
		delete(s.mutable.intents, command.Intent)
	}
	s.mutable.actions[command.Action] = nativeDeleteAction{FileActionReceipt: storage.FileActionReceipt{Action: command.Action, Operation: storage.OpFileAcknowledgeDeleteIntent, Outcome: storage.FileActionCompleted}}
	return nil
}

func (b *nativeDeleteBackend) ProbeDeleteOutcomes() []storage.DeleteIntentStatus {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]storage.DeleteIntentStatus(nil), b.acknowledged...)
	for _, intent := range b.intents {
		out = append(out, intent.DeleteIntentStatus)
	}
	return out
}

func (s *nativeDeleteSession) Close(ctx context.Context) error {
	_, err := s.CloseWithResult(ctx)
	return err
}
func (s *nativeDeleteSession) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	s.mutable.mu.Lock()
	var refs []*nativeDeleteReference
	for ref := range s.mutable.refs {
		if ref.owner == s {
			refs = append(refs, ref)
		}
	}
	s.mutable.mu.Unlock()
	var returned error
	for _, ref := range refs {
		_, err := ref.CloseWithResult(ctx)
		returned = errors.Join(returned, err)
	}
	result, err := s.endpointFileSession.CloseWithResult(ctx)
	result.Determined = result.Released
	return result, errors.Join(returned, err)
}

type nativeDeleteReference struct {
	*nativeDirectoryReference
	owner    *nativeDeleteSession
	parent   uint64
	name     string
	intent   storage.DeleteIntentID
	closeErr error
}

func (r *nativeDeleteReference) State(ctx context.Context, projection storage.StateProjection) (storage.ReferenceState, error) {
	b := r.owner.mutable
	b.mu.Lock()
	defer b.mu.Unlock()
	state, err := r.nativeDirectoryReference.State(ctx, projection)
	if err != nil {
		return state, err
	}
	if projection == storage.StateScalars {
		state.Attr.Metadata = nil
	}
	for _, intent := range b.intents {
		if intent.NodeID == r.id && intent.Outcome == storage.DeleteIntentPending {
			state.PendingUnlink = true
			state.PendingGeneration = b.pendingToken(r.id)
		}
	}
	state.PendingUnlink = state.PendingUnlink || b.ordinary[r.id]
	if state.PendingUnlink {
		state.PendingGeneration = b.pendingToken(r.id)
	}
	state.Detached = b.binding(r.id).State == storage.NameDetached
	return state, nil
}
func (r *nativeDeleteReference) Stat(ctx context.Context) (storage.Attr, error) {
	state, err := r.State(ctx, storage.StateFull)
	return state.Attr, err
}
func (r *nativeDeleteReference) Close(ctx context.Context) error {
	_, err := r.CloseWithResult(ctx)
	return err
}
func (r *nativeDeleteReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	b := r.owner.mutable
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	if _, active := b.refs[r]; !active {
		return storage.ReferenceCloseResult{Released: true, Determined: true}, r.closeErr
	}
	result, err := r.nativeDirectoryReference.CloseWithResult(ctx)
	if err != nil {
		return result, err
	}
	delete(b.refs, r)
	if intent := b.intents[r.intent]; intent != nil && intent.Outcome == storage.DeleteIntentArmed {
		if r.attr.Kind == storage.NodeDirectory && len(b.views[r.id]) != 0 {
			intent.Outcome = storage.DeleteIntentNotExecuted
			r.closeErr = syscall.ENOTEMPTY
		} else {
			if !b.pending(r.id) {
				b.generations[r.id]++
			}
			intent.Outcome = storage.DeleteIntentPending
		}
	}
	for _, intent := range b.intents {
		if intent.NodeID != r.id || intent.Outcome != storage.DeleteIntentPending {
			continue
		}
		remaining := false
		for ref := range b.refs {
			if ref.id == r.id {
				remaining = true
			}
		}
		if !remaining {
			entries := b.views[r.parent]
			for i, entry := range entries {
				if entry.Attr.ID == r.id && entry.Name == r.name {
					b.views[r.parent] = append(entries[:i], entries[i+1:]...)
					break
				}
			}
			delete(b.views, r.id)
			b.revision++
			intent.Outcome = storage.DeleteIntentCompleted
		}
	}
	if b.ordinary[r.id] && !b.hasReference(r.id) {
		b.unlink(r.id)
		delete(b.ordinary, r.id)
	}
	return result, r.closeErr
}
func (r *nativeDeleteReference) CloseWithAction(ctx context.Context, attempt storage.CloseAttempt) (storage.ReferenceCloseResult, error) {
	if err := attempt.Check(); err != nil {
		return storage.ReferenceCloseResult{}, err
	}
	r.session.refMu.Lock()
	if r.attempt != nil && *r.attempt != attempt {
		r.session.refMu.Unlock()
		return storage.ReferenceCloseResult{}, syscall.EINVAL
	}
	copy := attempt
	r.attempt = &copy
	r.session.refMu.Unlock()
	return r.CloseWithResult(ctx)
}

type nativeDeleteFile struct {
	*nativeDeleteReference
	readable bool
	writable bool
}

func (*nativeDeleteFile) CheckConditionalFileMutation() error { return nil }
func (f *nativeDeleteFile) MutateFile(ctx context.Context, command storage.FileMutation) (storage.Attr, error) {
	if err := command.Check(); err != nil {
		return storage.Attr{}, err
	}
	if _, err := f.State(ctx, storage.StateScalars); err != nil {
		return storage.Attr{}, err
	}
	return storage.Attr{}, syscall.EACCES
}

func (f *nativeDeleteFile) ReadAt(ctx context.Context, offset int64, length int) (storage.FileRead, error) {
	if !f.readable {
		return storage.FileRead{}, syscall.EBADF
	}
	if offset < 0 || length < 0 {
		return storage.FileRead{}, syscall.EINVAL
	}
	state, err := f.State(ctx, storage.StateScalars)
	return storage.FileRead{Attr: state.Attr}, err
}
func (*nativeDeleteFile) WriteAt(context.Context, int64, []byte) (storage.Attr, error) {
	return storage.Attr{}, syscall.EOPNOTSUPP
}
func (*nativeDeleteFile) Truncate(context.Context, int64) (storage.Attr, error) {
	return storage.Attr{}, syscall.EOPNOTSUPP
}
func (f *nativeDeleteFile) Sync(ctx context.Context) error {
	if !f.readable && !f.writable {
		return syscall.EBADF
	}
	_, err := f.State(ctx, storage.StateScalars)
	return err
}

func TestNativeDeleteProbeFixture(t *testing.T) {
	newSession := func(t *testing.T) (*nativeDeleteBackend, *nativeDeleteSession) {
		t.Helper()
		backend := NewNativeDeleteProbeBackend()
		raw, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := raw.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		return backend, raw.(*nativeDeleteSession)
	}
	options := func(t *testing.T, id uint64, kind storage.NodeKind, claim storage.UseClaim, intent bool) storage.NodeRefOptions {
		t.Helper()
		action, err := storage.NewFileActionID(1)
		if err != nil {
			t.Fatal(err)
		}
		out := storage.NodeRefOptions{Kind: kind, Target: storage.ChildCondition{State: storage.SameNode, NodeID: id}, Action: action, Use: claim, MetadataAccess: storage.ReadMetadata}
		if intent {
			id, err := storage.NewDeleteIntentID()
			if err != nil {
				t.Fatal(err)
			}
			condition := storage.UnlinkFile
			if kind == storage.NodeDirectory {
				condition = storage.UnlinkIfEmpty
			}
			out.CloseIntent = &storage.CloseIntent{ID: id, Owner: "native-probe-owner", Trigger: storage.OnReferenceClose, Condition: condition}
		}
		return out
	}
	selection := func(name string) storage.ChildSelection {
		return storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte(name)}}
	}
	t.Run("pending-shared-reference-and-terminal-ack", func(t *testing.T) {
		b, s := newSession(t)
		readerOpts := options(t, 3, storage.NodeRegular, storage.UseClaim{Uses: storage.ReadData}, false)
		reader, err := s.OpenAt(t.Context(), selection("alpha.txt"), storage.OpenAtOptions{Read: true, Existing: storage.Keep, Target: readerOpts.Target, Action: readerOpts.Action, Use: readerOpts.Use, MetadataAccess: readerOpts.MetadataAccess})
		if err != nil {
			t.Fatal(err)
		}
		initiatorOpts := options(t, 3, storage.NodeRegular, storage.UseClaim{Uses: storage.DeleteName}, true)
		initiator, err := s.OpenChildRef(t.Context(), selection("alpha.txt"), initiatorOpts)
		if err != nil {
			t.Fatal(err)
		}
		intent := initiatorOpts.CloseIntent
		if err := s.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Action: options(t, 3, storage.NodeRegular, storage.UseClaim{}, false).Action, Owner: intent.Owner, Intent: intent.ID}); !errors.Is(err, syscall.EBUSY) {
			t.Fatalf("live ACK=%v", err)
		}
		if err := initiator.Reference.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		state, err := reader.File.(*nativeDeleteFile).State(t.Context(), storage.StateScalars)
		if err != nil || !state.PendingUnlink || state.CheckProjection(3, storage.StateScalars) != nil {
			t.Fatalf("pending state=%+v error=%v", state, err)
		}
		if _, err := reader.File.ReadAt(t.Context(), 0, 1); err != nil {
			t.Fatal(err)
		}
		readerOpts.Action = options(t, 3, storage.NodeRegular, storage.UseClaim{}, false).Action
		if _, err := s.OpenChildRef(t.Context(), selection("alpha.txt"), readerOpts); !errors.Is(err, storage.ErrPendingDelete) {
			t.Fatalf("pending open=%v", err)
		}
		if err := reader.File.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		status, err := s.QueryDeleteIntent(t.Context(), intent.Owner, intent.ID)
		if err != nil || status.Outcome != storage.DeleteIntentCompleted {
			t.Fatalf("terminal status=%+v error=%v", status, err)
		}
		page, err := s.ListDeleteIntents(t.Context(), intent.Owner, 0, 1)
		if err != nil || len(page.Intents) != 1 || page.Next == 0 {
			t.Fatalf("intent page=%+v error=%v", page, err)
		}
		if other, err := s.ListDeleteIntents(t.Context(), "foreign", 0, 1); err != nil || len(other.Intents) != 0 {
			t.Fatalf("foreign owner page=%+v error=%v", other, err)
		}
		if err := s.AcknowledgeDeleteIntent(t.Context(), storage.AcknowledgeDeleteIntentCommand{Action: options(t, 3, storage.NodeRegular, storage.UseClaim{}, false).Action, Owner: intent.Owner, Intent: intent.ID}); err != nil {
			t.Fatal(err)
		}
		if status, err := s.QueryDeleteIntent(t.Context(), intent.Owner, intent.ID); err != nil || status.Outcome != storage.DeleteIntentUnknown {
			t.Fatalf("ack status=%+v error=%v", status, err)
		}
		if outcomes := b.ProbeDeleteOutcomes(); len(outcomes) != 1 || outcomes[0].Outcome != storage.DeleteIntentCompleted {
			t.Fatalf("observed terminal=%+v", outcomes)
		}
	})
	t.Run("nonempty-directory", func(t *testing.T) {
		_, s := newSession(t)
		opts := options(t, 2, storage.NodeDirectory, storage.UseClaim{Uses: storage.DeleteName}, true)
		opened, err := s.OpenChildRef(t.Context(), selection("folder"), opts)
		if err != nil {
			t.Fatal(err)
		}
		result, err := opened.Reference.CloseWithResult(t.Context())
		if !result.Released || !result.Determined || !errors.Is(err, syscall.ENOTEMPTY) {
			t.Fatalf("close=%+v error=%v", result, err)
		}
		status, err := s.QueryDeleteIntent(t.Context(), opts.CloseIntent.Owner, opts.CloseIntent.ID)
		if err != nil || status.Outcome != storage.DeleteIntentNotExecuted {
			t.Fatalf("nonempty status=%+v error=%v", status, err)
		}
	})
	t.Run("exclusive-claim-and-canceled-open", func(t *testing.T) {
		_, s := newSession(t)
		opts := options(t, 3, storage.NodeRegular, storage.UseClaim{Uses: storage.DeleteName, Deny: storage.AllUses}, true)
		opened, err := s.OpenChildRef(t.Context(), selection("alpha.txt"), opts)
		if err != nil {
			t.Fatal(err)
		}
		conflicting := options(t, 3, storage.NodeRegular, storage.UseClaim{Uses: storage.ReadData}, false)
		if _, err := s.OpenChildRef(t.Context(), selection("alpha.txt"), conflicting); !errors.Is(err, storage.ErrUseConflict) {
			t.Fatalf("exclusive share=%v", err)
		}
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := s.OpenChildRef(canceled, selection("alpha.txt"), conflicting); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled open=%v", err)
		}
		if err := opened.Reference.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("session-retirement-closes-armed-reference", func(t *testing.T) {
		_, s := newSession(t)
		opts := options(t, 7, storage.NodeDirectory, storage.UseClaim{Uses: storage.DeleteName}, true)
		if _, err := s.OpenChildRef(t.Context(), selection("empty"), opts); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		status, err := s.QueryDeleteIntent(t.Context(), opts.CloseIntent.Owner, opts.CloseIntent.ID)
		if err != nil || status.Outcome != storage.DeleteIntentCompleted {
			t.Fatalf("retired intent=%+v error=%v", status, err)
		}
	})
	t.Run("created-file-preserves-bounded-metadata", func(t *testing.T) {
		_, s := newSession(t)
		opts := options(t, 0, storage.NodeRegular, storage.UseClaim{Uses: storage.ReadData | storage.DeleteName}, true)
		opts.Target = storage.ChildCondition{State: storage.Absent}
		metadata, err := withWindowsMetadata(nil, windowsMetadata{Attributes: dosArchive})
		if err != nil {
			t.Fatal(err)
		}
		opened, err := s.OpenAt(t.Context(), selection("created.txt"), storage.OpenAtOptions{Read: true, Create: true, Exclusive: true,
			Existing: storage.Keep, Target: opts.Target, Action: opts.Action, Use: opts.Use, MetadataAccess: opts.MetadataAccess,
			Initial: storage.InitialState{OnCreate: storage.InitialFields{Metadata: metadata}}, CloseIntent: opts.CloseIntent})
		if err != nil || opened.Outcome != storage.Created || opened.Attr.ID == 0 {
			t.Fatalf("created result=%+v error=%v", opened, err)
		}
		windows, err := decodeWindowsMetadata(opened.Attr.Metadata)
		if err != nil || windows.Attributes != dosArchive {
			t.Fatalf("created metadata=%+v error=%v", windows, err)
		}
		result, err := storage.NewListResult(65536, 0, func(_ int, name, metadata int64, _ storage.Attr) (int64, error) {
			return storage.ObservedEntryBytes(name, metadata)
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{NodeID: 1}, result); err != nil {
			t.Fatal(err)
		}
		entries, err := result.Entries()
		if err != nil || len(entries) != 6 {
			t.Fatalf("created namespace entries=%+v error=%v", entries, err)
		}
		if err := opened.File.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestNativeDeleteProbeByteCreateUsesEndpointContract(t *testing.T) {
	backend := NewNativeDeleteProbeBackend()
	server, err := New(endpointConfig())
	if err != nil {
		t.Fatal(err)
	}
	owner, err := storage.NewDeleteIntentOwner()
	if err != nil {
		t.Fatal(err)
	}
	export, err := server.Publish(Share{Name: "native-probe", Volume: "native-probe", BackendVolume: "test-volume", RootNodeID: 1, Backend: backend, DeleteIntentOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	principal := testPrincipal("0123456789abcdef")
	ctx := WithPrincipal(t.Context(), principal)
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	c := newConnection(server, nil)
	s := &session{id: 21, principal: principal, trees: make(map[uint32]*tree)}
	c.sessions[s.id] = s
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(WithPrincipal(context.Background(), principal), time.Second)
		defer cancel()
		if err := c.closeOwnedExport(cleanup, export); err != nil {
			t.Error(err)
		}
		if err := export.Unpublish(cleanup); err != nil {
			t.Error(err)
		}
		c.cancel()
	})
	header := wire.Header{}
	if _, status := c.connectVolume(ctx, s, export.key, &header); status != statusOK {
		t.Fatalf("native fixture TREE_CONNECT=%#x", status)
	}
	tr := s.trees[header.TreeID]

	for _, test := range []struct {
		name                                string
		disposition, access, share, options uint32
	}{
		{"exclusive.txt", 2, accessGenericRead | accessDelete, 0, createDeleteOnClose},
		{"beta.bin", 1, accessGenericRead, 7, 0},
	} {
		body, status, id := c.createFile(ctx, s, tr, nativeProbeCreateRequest(test.name, test.disposition, test.access, test.share, test.options))
		if status != statusOK || len(body) != 88 || id == (wire.FileID{}) {
			t.Fatalf("byte CREATE %q status=%#x body=%d id=%x", test.name, status, len(body), id)
		}
		mutation := tr.findFileHandle(id).file.(storage.ConditionalFileMutation)
		action, err := storage.NewFileActionID(1)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := mutation.MutateFile(ctx, storage.FileMutation{Kind: storage.MutateTruncate, Action: action, Size: 1}); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("readonly mutation %q=%v", test.name, err)
		}
		closeBody := make([]byte, 24)
		binary.LittleEndian.PutUint16(closeBody, 24)
		copy(closeBody[8:], id[:])
		if _, status := c.closeFile(ctx, s, tr, wire.Request{Header: wire.Header{Command: wire.Close}, Body: closeBody}, wire.FileID{}); status != statusOK {
			t.Fatalf("byte CLOSE %q=%#x", test.name, status)
		}
		if test.name == "exclusive.txt" {
			if _, status, id := c.createFile(ctx, s, tr, nativeProbeCreateRequest(test.name, 1, accessReadAttr, 7, createBackupIntent)); status != statusNameNotFound || id != (wire.FileID{}) {
				t.Fatalf("deleted metadata-only OPEN_EXISTING status=%#x id=%x", status, id)
			}
		}
	}
	_, status, metadataID := c.createFile(ctx, s, tr, nativeProbeCreateRequest("beta.bin", 1, accessReadAttr, 7, createBackupIntent))
	if status != statusOK || metadataID == (wire.FileID{}) {
		t.Fatalf("existing metadata-only OPEN_EXISTING status=%#x id=%x", status, metadataID)
	}
	closeMetadata := make([]byte, 24)
	binary.LittleEndian.PutUint16(closeMetadata, 24)
	copy(closeMetadata[8:], metadataID[:])
	if _, status := c.closeFile(ctx, s, tr, wire.Request{Header: wire.Header{Command: wire.Close}, Body: closeMetadata}, wire.FileID{}); status != statusOK {
		t.Fatalf("metadata-only CLOSE=%#x", status)
	}
	if _, status, _ := c.createFile(ctx, s, tr, nativeProbeCreateRequest("without-delete.txt", 2, accessReadAttr, 7, createDeleteOnClose)); status != statusDenied {
		t.Fatalf("missing DELETE CREATE=%#x", status)
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	for _, entry := range backend.views[1] {
		if entry.Name == "exclusive.txt" || entry.Name == "without-delete.txt" {
			t.Fatalf("native contract retained unexpected name %q", entry.Name)
		}
	}
}

func nativeProbeCreateRequest(name string, disposition, access, share, options uint32) wire.Request {
	encoded := wire.EncodeUTF16(name)
	packet := make([]byte, 64+max(57, 56+len(encoded)))
	body := packet[64:]
	binary.LittleEndian.PutUint16(body, 57)
	binary.LittleEndian.PutUint32(body[4:], 2)
	binary.LittleEndian.PutUint32(body[24:], access)
	binary.LittleEndian.PutUint32(body[32:], share)
	binary.LittleEndian.PutUint32(body[36:], disposition)
	binary.LittleEndian.PutUint32(body[40:], options)
	binary.LittleEndian.PutUint16(body[44:], 120)
	binary.LittleEndian.PutUint16(body[46:], uint16(len(encoded)))
	copy(packet[120:], encoded)
	return wire.Request{Header: wire.Header{Command: wire.Create}, Body: body, Packet: packet}
}

func TestNativeDeleteProbeSignedPendingStandardInformation(t *testing.T) {
	server, _, client := startProtocolServer(t, DefaultLimits())
	ctx := WithPrincipal(t.Context(), testPrincipal("0123456789abcdef"))
	var original *Export
	server.mu.Lock()
	for _, export := range server.exports {
		if export.share.Name == "data" {
			original = export
		}
	}
	server.mu.Unlock()
	if original == nil {
		t.Fatal("protocol harness data export missing")
	}
	if err := original.Unpublish(ctx); err != nil {
		t.Fatal(err)
	}
	backend := NewNativeDeleteProbeBackend()
	owner, err := storage.NewDeleteIntentOwner()
	if err != nil {
		t.Fatal(err)
	}
	export, err := server.Publish(Share{Name: "data", Volume: "volume", BackendVolume: "test-volume", RootNodeID: 1, Backend: backend, DeleteIntentOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	session, key := authenticateProtocol(t, client)
	tree := connectProtocolTree(t, client, key, session, 3, "data")
	message := uint64(4)
	exchange := func(command uint16, body []byte) []byte {
		t.Helper()
		header := wire.Header{Command: command, MessageID: message, SessionID: session, TreeID: tree, Credits: 1, CreditCharge: 1}
		sendFrame(t, client, signedRequest(t, key, header, body))
		response := readFrame(t, client)
		actual, err := wire.ParseHeader(response)
		if err != nil || actual.Command != command || actual.MessageID != message || actual.SessionID != session || actual.TreeID != tree || actual.Flags&(wire.FlagResponse|wire.FlagSigned) != (wire.FlagResponse|wire.FlagSigned) || actual.Status != statusOK || key.Verify(response) != nil {
			t.Fatalf("signed command%d response=%+v error=%v", command, actual, err)
		}
		message++
		return response[wire.HeaderSize:]
	}
	open := func(access, options uint32) wire.FileID {
		t.Helper()
		body := exchange(wire.Create, nativeProbeCreateRequest("beta.bin", 1, access, 7, options).Body)
		if len(body) != 88 {
			t.Fatalf("CREATE body bytes=%d", len(body))
		}
		var id wire.FileID
		copy(id[:], body[64:80])
		return id
	}
	close := func(id wire.FileID) {
		body := make([]byte, 24)
		binary.LittleEndian.PutUint16(body, 24)
		copy(body[8:], id[:])
		exchange(wire.Close, body)
	}
	reader := open(accessGenericRead, 0)
	initiator := open(accessReadAttr|accessDelete, createDeleteOnClose)
	close(initiator)
	found := false
	for _, intent := range backend.ProbeDeleteOutcomes() {
		if intent.NodeID == 4 && intent.Outcome == storage.DeleteIntentPending {
			found = true
		}
	}
	if !found {
		t.Fatal("signed initiator CLOSE did not establish Pending")
	}
	query := queryInfoRequestForTest(reader, wire.FileStandardInformation, 24)
	actual := exchange(wire.QueryInfo, query.Body)
	expected, err := wire.QueryInfoResponseBody(wire.FileStandardInformationBody(wire.FileStandardInfo{DeletePending: true}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("signed pending standard body=%x expected=%x", actual, expected)
	}
	close(reader)
	completed := false
	for _, intent := range backend.ProbeDeleteOutcomes() {
		if intent.NodeID == 4 && intent.Outcome == storage.DeleteIntentCompleted {
			completed = true
		}
	}
	if !completed {
		t.Fatal("last signed CLOSE did not complete original delete")
	}
}
