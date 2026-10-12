package smb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// NewNativeDeleteProbeBackend models only the native probe's mutable namespace.
// Persistence and process retirement are exercised against SQLite separately.
func NewNativeDeleteProbeBackend() *nativeDeleteBackend {
	b := NewNativeDirectoryProbeBackend().(*nativeDirectoryBackend)
	b.views[1] = append(b.views[1], storage.Entry{Name: "empty", Attr: b.views[1][2].Attr.Clone()})
	b.views[1][4].Attr.ID = 7
	b.views[7] = nil
	return &nativeDeleteBackend{nativeDirectoryBackend: b, refs: make(map[*nativeDeleteReference]struct{}),
		intents: make(map[storage.DeleteIntentID]*nativeDeleteIntent), actions: make(map[storage.FileActionID]storage.FileActionReceipt), nextNode: 100}
}

type nativeDeleteIntent struct {
	storage.DeleteIntentStatus
	owner  storage.DeleteIntentOwner
	cursor storage.DeleteIntentCursor
}

type nativeDeleteBackend struct {
	*nativeDirectoryBackend
	mu           sync.Mutex
	refs         map[*nativeDeleteReference]struct{}
	intents      map[storage.DeleteIntentID]*nativeDeleteIntent
	actions      map[storage.FileActionID]storage.FileActionReceipt
	sequence     storage.DeleteIntentCursor
	nextNode     uint64
	acknowledged []storage.DeleteIntentStatus
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
	if options.IncludeName {
		result.Fail(syscall.EOPNOTSUPP)
		return storage.DirectoryMetadataObservation{}, syscall.EOPNOTSUPP
	}
	if err := s.checkGuards(options.Guards); err != nil {
		result.Fail(err)
		return storage.DirectoryMetadataObservation{}, err
	}
	observation, err := s.directory(ctx, target, result)
	return storage.DirectoryMetadataObservation{Observation: observation}, err
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
	return storage.DirectoryObservation{ParentID: target.NodeID, Revision: []byte("native-probe-one")}, nil
}

func (s *nativeDeleteSession) OpenNodeRef(ctx context.Context, id uint64, options storage.NodeRefOptions) (storage.NodeOpenResult, error) {
	if id != 1 || options.CloseIntent != nil {
		return storage.NodeOpenResult{}, syscall.EINVAL
	}
	return s.nativeDirectorySession.OpenNodeRef(ctx, id, options)
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
	if options.Existing != storage.Keep {
		return storage.OpenResult{}, syscall.EOPNOTSUPP
	}
	ref, attr, outcome, err := s.open(ctx, selection, storage.NodeRefOptions{Kind: storage.NodeRegular,
		Target: options.Target, Action: options.Action, Use: options.Use, MetadataAccess: options.MetadataAccess,
		Create: options.Create, Exclusive: options.Exclusive, InitialState: options.Initial, CloseIntent: options.CloseIntent})
	if err != nil {
		return storage.OpenResult{}, err
	}
	s.mutable.actions[options.Action] = storage.FileActionReceipt{Action: options.Action, Operation: storage.OpFileOpenAt, Outcome: storage.FileActionCompleted}
	return storage.OpenResult{File: &nativeDeleteFile{nativeDeleteReference: ref, readable: options.Read}, Attr: attr, Outcome: outcome}, nil
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
	b.actions[options.Action] = storage.FileActionReceipt{Action: options.Action, Operation: storage.OpFileOpenChildRef, Outcome: storage.FileActionCompleted}
	return ref, attr.Clone(), outcome, nil
}

func (s *nativeDeleteSession) QueryFileAction(ctx context.Context, id storage.FileActionID) (storage.FileActionReceipt, error) {
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.FileActionReceipt{}, err
	}
	if result, found := s.mutable.actions[id]; found {
		return result, nil
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
	s.mutable.actions[command.Action] = storage.FileActionReceipt{Action: command.Action, Operation: storage.OpFileAcknowledgeDeleteIntent, Outcome: storage.FileActionCompleted}
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
			if projection == storage.StateFull {
				state.PendingGeneration = []byte("native-pending")
			}
		}
	}
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
			intent.Outcome = storage.DeleteIntentCompleted
		}
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
