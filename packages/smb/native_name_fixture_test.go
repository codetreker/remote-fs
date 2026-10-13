package smb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func NewNativeNameProbeBackend() *nativeDeleteBackend {
	b := NewNativeDeleteProbeBackend()
	attr := b.views[1][0].Attr.Clone()
	attr.ID, attr.Size, attr.AllocationSize = 8, 9, 4096
	b.views[1] = append(b.views[1], storage.Entry{Name: "supersede.txt", Attr: attr})
	return b
}

func (b *nativeDeleteBackend) revisionToken() []byte {
	return []byte(fmt.Sprintf("native-mutable-%d", b.revision))
}

func (b *nativeDeleteBackend) binding(id uint64) storage.NameObservation {
	out := storage.NameObservation{NodeID: id, State: storage.NameDetached}
	if id == b.root.ID {
		out.State = storage.NameRoot
		return out
	}
	for parent, entries := range b.views {
		for _, entry := range entries {
			if entry.Attr.ID == id {
				return storage.NameObservation{NodeID: id, State: storage.NameLinked, ParentID: parent, RawLeaf: []byte(entry.Name)}
			}
		}
	}
	return out
}

func (b *nativeDeleteBackend) name(ctx context.Context, id uint64) (storage.NameObservation, error) {
	scalar := storage.NameObservation{NodeID: id, State: storage.NameDetached}
	leaf := ""
	if id == b.root.ID {
		scalar.State = storage.NameRoot
	} else {
		for parent, entries := range b.views {
			for _, entry := range entries {
				if entry.Attr.ID == id {
					scalar.State, scalar.ParentID, leaf = storage.NameLinked, parent, entry.Name
				}
			}
		}
	}
	if _, err := storage.CheckNameObservationBudget(ctx, scalar, int64(len(leaf))); err != nil {
		return storage.NameObservation{}, err
	}
	if scalar.State == storage.NameLinked {
		scalar.RawLeaf = []byte(leaf)
	}
	return scalar, nil
}

func (s *nativeDeleteSession) checkGuards(guards *storage.NamespaceGuards) error {
	if guards == nil {
		return nil
	}
	if err := guards.Check(); err != nil {
		return err
	}
	if guards.RootID != 0 && guards.RootID != s.mutable.root.ID {
		return storage.ErrConditionConflict
	}
	for _, guard := range guards.Directories {
		if _, ok := s.mutable.views[guard.ParentID]; !ok || !bytes.Equal(guard.Revision, s.mutable.revisionToken()) {
			return storage.ErrConditionConflict
		}
	}
	for _, edge := range guards.Edges {
		found := false
		for _, entry := range s.mutable.views[edge.ParentID] {
			found = found || entry.Name == string(edge.RawLeaf) && entry.Attr.ID == edge.ChildID
		}
		if !found {
			return storage.ErrConditionConflict
		}
	}
	return nil
}

func (*nativeDeleteSession) CheckNamespaceAccess() error { return nil }
func (s *nativeDeleteSession) LookupAt(ctx context.Context, name storage.ChildName) (storage.Attr, error) {
	s.mutable.mu.Lock()
	defer s.mutable.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.Attr{}, err
	}
	for _, entry := range s.mutable.views[name.Parent.NodeID] {
		if entry.Name == string(name.RawLeaf) {
			return entry.Attr.Clone(), nil
		}
	}
	return storage.Attr{}, syscall.ENOENT
}

func nativeCondition(attr storage.Attr, expected storage.ChildCondition) error {
	if expected.State == storage.Absent && attr.ID != 0 || expected.State == storage.SameNode && attr.ID != expected.NodeID {
		return storage.ErrConditionConflict
	}
	for key, token := range expected.ExpectedMetadata {
		if !bytes.Equal(attr.Metadata[key].Version, token) {
			return storage.ErrConditionConflict
		}
	}
	return nil
}

func (b *nativeDeleteBackend) use(id uint64, uses []storage.TargetUse) error {
	claimed := false
	for ref := range b.refs {
		if ref.id != id {
			continue
		}
		if ref.use.Deny&storage.DeleteName != 0 {
			return storage.ErrUseConflict
		}
		for _, use := range uses {
			if use.NodeID == id && use.Scope.Token == ref.token && ref.use.Uses&storage.DeleteName != 0 {
				claimed = true
			}
		}
	}
	if !claimed {
		return syscall.EACCES
	}
	return nil
}

func (b *nativeDeleteBackend) attr(id uint64) storage.Attr {
	if id == b.root.ID {
		return b.root.Clone()
	}
	for _, entries := range b.views {
		for _, entry := range entries {
			if entry.Attr.ID == id {
				return entry.Attr.Clone()
			}
		}
	}
	for ref := range b.refs {
		if ref.id == id {
			return ref.attr.Clone()
		}
	}
	return storage.Attr{}
}

func (b *nativeDeleteBackend) updateAttr(attr storage.Attr) {
	for parent, entries := range b.views {
		for i := range entries {
			if entries[i].Attr.ID == attr.ID {
				b.views[parent][i].Attr = attr.Clone()
			}
		}
	}
	for ref := range b.refs {
		if ref.id == attr.ID {
			ref.attr = attr.Clone()
		}
	}
	b.revision++
}

func cloneNativeInitialMetadata(values map[string][]byte) map[string]storage.OpaquePayload {
	if values == nil {
		return nil
	}
	out := make(map[string]storage.OpaquePayload, len(values))
	for key, value := range values {
		out[key] = storage.OpaquePayload{Version: []byte("native-reset"), Data: bytes.Clone(value)}
	}
	return out
}

func (*nativeDeleteReference) CheckReferenceNameObservation() error { return nil }
func (r *nativeDeleteReference) ObserveName(ctx context.Context, guards *storage.NamespaceGuards) (storage.NameObservation, error) {
	b := r.owner.mutable
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.closed {
		return storage.NameObservation{}, syscall.EBADF
	}
	if err := r.owner.checkGuards(guards); err != nil {
		return storage.NameObservation{}, err
	}
	return b.name(ctx, r.id)
}

func (s *nativeDeleteSession) MutateName(ctx context.Context, command storage.NameCommand) (storage.NameResult, error) {
	b := s.mutable
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.NameResult{}, err
	}
	if err := command.Check(); err != nil {
		return storage.NameResult{}, err
	}
	if command.Kind != storage.NameRename {
		return storage.NameResult{}, syscall.EOPNOTSUPP
	}
	if err := s.checkGuards(command.Guards); err != nil {
		return storage.NameResult{}, err
	}
	id := command.Target.NodeID
	attr, binding := b.attr(id), b.binding(id)
	if binding.State != storage.NameLinked || binding.ParentID != command.Name.Parent.NodeID || !bytes.Equal(binding.RawLeaf, command.Name.RawLeaf) {
		return storage.NameResult{}, storage.ErrConditionConflict
	}
	if err := nativeCondition(attr, command.Target); err != nil {
		return storage.NameResult{}, err
	}
	if b.pending(id) {
		return storage.NameResult{}, storage.ErrPendingDelete
	}
	if err := b.use(id, command.Uses); err != nil {
		return storage.NameResult{}, err
	}
	if command.RequireNoOpenDescendants {
		for ref := range b.refs {
			candidate := ref.id
			for depth := 0; candidate != id && candidate != 1 && depth < 32; depth++ {
				name := b.binding(candidate)
				if name.State != storage.NameLinked {
					break
				}
				candidate = name.ParentID
			}
			if ref.id != id && candidate == id {
				return storage.NameResult{}, syscall.EACCES
			}
		}
	}
	destination := command.Destination
	var target storage.Attr
	for _, entry := range b.views[destination.Parent.NodeID] {
		if entry.Name == string(destination.ObservedLeaf) {
			target = entry.Attr
		}
		if entry.Name == string(destination.OutputLeaf) && entry.Attr.ID != id && entry.Attr.ID != destination.Expected.NodeID {
			return storage.NameResult{}, storage.ErrConditionConflict
		}
	}
	if err := nativeCondition(target, destination.Expected); err != nil {
		return storage.NameResult{}, err
	}
	if target.ID != 0 && target.ID != id {
		if b.pending(target.ID) {
			return storage.NameResult{}, storage.ErrPendingDelete
		}
		for ref := range b.refs {
			if ref.id == target.ID && ref.use.Deny&storage.DeleteName != 0 {
				return storage.NameResult{}, storage.ErrUseConflict
			}
		}
		b.unlink(target.ID)
	}
	b.unlink(id)
	b.views[destination.Parent.NodeID] = append(b.views[destination.Parent.NodeID], storage.Entry{Name: string(destination.OutputLeaf), Attr: attr.Clone()})
	for ref := range b.refs {
		if ref.id == id {
			ref.parent, ref.name = destination.Parent.NodeID, string(destination.OutputLeaf)
		}
	}
	b.actions[command.Action] = storage.FileActionReceipt{Action: command.Action, Operation: storage.OpFileMutateName, Outcome: storage.FileActionCompleted}
	return storage.NameResult{Attr: &attr}, nil
}

func (b *nativeDeleteBackend) unlink(id uint64) {
	for parent, entries := range b.views {
		for i, entry := range entries {
			if entry.Attr.ID == id {
				b.views[parent] = append(entries[:i], entries[i+1:]...)
				b.revision++
				return
			}
		}
	}
}

func (b *nativeDeleteBackend) hasReference(id uint64) bool {
	for ref := range b.refs {
		if ref.id == id {
			return true
		}
	}
	return false
}

func (b *nativeDeleteBackend) pending(id uint64) bool {
	if b.ordinary[id] {
		return true
	}
	for _, intent := range b.intents {
		if intent.NodeID == id && intent.Outcome == storage.DeleteIntentPending {
			return true
		}
	}
	return false
}

func (b *nativeDeleteBackend) pendingToken(id uint64) []byte {
	return []byte(fmt.Sprintf("native-pending-%d", b.generations[id]))
}

func (b *nativeDeleteBackend) ProbeNodeState(id uint64) storage.ReferenceState {
	b.mu.Lock()
	defer b.mu.Unlock()
	state := storage.ReferenceState{Attr: b.attr(id), Detached: b.binding(id).State == storage.NameDetached, PendingUnlink: b.pending(id)}
	if state.PendingUnlink {
		state.PendingGeneration = b.pendingToken(id)
	}
	return state
}

func (*nativeDeleteReference) CheckDeleteIntent() error { return nil }
func (r *nativeDeleteReference) pendingState() storage.ReferenceState {
	b := r.owner.mutable
	state := storage.ReferenceState{Attr: b.attr(r.id), Detached: b.binding(r.id).State == storage.NameDetached, PendingUnlink: b.pending(r.id)}
	if state.PendingUnlink {
		state.PendingGeneration = b.pendingToken(r.id)
	}
	return state
}

func (r *nativeDeleteReference) SetPendingUnlink(ctx context.Context, command storage.PendingUnlinkCommand) (storage.ReferenceState, error) {
	b := r.owner.mutable
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := command.Check(); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := b.use(r.id, command.Uses); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := nativeCondition(b.attr(r.id), storage.ChildCondition{State: storage.SameNode, NodeID: r.id, ExpectedMetadata: command.ExpectedMetadata}); err != nil {
		return storage.ReferenceState{}, err
	}
	if command.Condition == storage.UnlinkIfEmpty && len(b.views[r.id]) != 0 {
		return storage.ReferenceState{}, syscall.ENOTEMPTY
	}
	if !b.ordinary[r.id] {
		b.generations[r.id]++
		b.ordinary[r.id] = true
	}
	b.actions[command.Action] = storage.FileActionReceipt{Action: command.Action, Operation: storage.OpFileSetPendingUnlink, Outcome: storage.FileActionCompleted}
	return r.pendingState(), nil
}

func (r *nativeDeleteReference) ClearPendingUnlink(ctx context.Context, command storage.ClearPendingUnlinkCommand) (storage.ReferenceState, error) {
	b := r.owner.mutable
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := command.Check(); err != nil {
		return storage.ReferenceState{}, err
	}
	if err := b.use(r.id, command.Uses); err != nil {
		return storage.ReferenceState{}, err
	}
	state := r.pendingState()
	if !bytes.Equal(command.Generation, state.PendingGeneration) {
		return storage.ReferenceState{}, storage.ErrConditionConflict
	}
	delete(b.ordinary, r.id)
	b.actions[command.Action] = storage.FileActionReceipt{Action: command.Action, Operation: storage.OpFileClearPendingUnlink, Outcome: storage.FileActionCompleted}
	return r.pendingState(), nil
}

func TestNativeNameProbeFixture(t *testing.T) {
	backend := NewNativeDeleteProbeBackend()
	raw, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := raw.(*nativeDeleteSession)
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	action := func() storage.FileActionID {
		id, err := storage.NewFileActionID(1)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	opened, err := session.OpenChildRef(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("alpha.txt")}},
		storage.NodeRefOptions{Kind: storage.NodeRegular, Action: action(), Target: storage.ChildCondition{State: storage.SameNode, NodeID: 3}, Use: storage.UseClaim{Uses: storage.DeleteName}})
	if err != nil {
		t.Fatal(err)
	}
	ref := opened.Reference.(*nativeDeleteReference)
	scope, err := ref.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	uses := []storage.TargetUse{{NodeID: 3, Scope: scope}}
	name, err := ref.ObserveName(t.Context(), nil)
	if err != nil || name.ParentID != 1 || string(name.RawLeaf) != "alpha.txt" {
		t.Fatalf("fixture original binding=%+v error=%v", name, err)
	}
	guards := &storage.NamespaceGuards{RootID: 1, Directories: []storage.DirectoryObservation{{ParentID: 1, Revision: backend.revisionToken()}},
		Edges: []storage.ObservedEdge{{ParentID: 1, ChildID: 3, RawLeaf: []byte("alpha.txt")}}}
	command := storage.NameCommand{Kind: storage.NameRename, Action: action(), Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte("alpha.txt")},
		Target: storage.ChildCondition{State: storage.SameNode, NodeID: 3}, Uses: uses, Guards: guards,
		Destination: &storage.RenameTarget{Parent: storage.DirectoryTarget{NodeID: 1}, ObservedLeaf: []byte("renamed.txt"), OutputLeaf: []byte("renamed.txt"), Expected: storage.ChildCondition{State: storage.Absent}}}
	if _, err := session.MutateName(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	if _, err := ref.ObserveName(t.Context(), guards); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale fixture guards survived rename: %v", err)
	}
	name, err = ref.ObserveName(t.Context(), nil)
	if err != nil || name.NodeID != 3 || string(name.RawLeaf) != "renamed.txt" {
		t.Fatalf("fixture current binding=%+v error=%v", name, err)
	}
	state, err := ref.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Action: action(), Condition: storage.UnlinkFile, Uses: uses})
	if err != nil || !state.PendingUnlink || len(state.PendingGeneration) == 0 {
		t.Fatalf("fixture SET=%+v error=%v", state, err)
	}
	scalars, err := ref.State(t.Context(), storage.StateScalars)
	if err != nil || scalars.CheckProjection(3, storage.StateScalars) != nil || !bytes.Equal(scalars.PendingGeneration, state.PendingGeneration) {
		t.Fatalf("fixture scalar token=%+v error=%v", scalars, err)
	}
	if _, err := ref.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: action(), Uses: uses}); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("fixture nil CAS cleared pending: %v", err)
	}
	if _, err := ref.ClearPendingUnlink(t.Context(), storage.ClearPendingUnlinkCommand{Action: action(), Generation: state.PendingGeneration, Uses: uses}); err != nil {
		t.Fatal(err)
	}
	second, err := ref.SetPendingUnlink(t.Context(), storage.PendingUnlinkCommand{Action: action(), Condition: storage.UnlinkFile, Uses: uses})
	if err != nil || bytes.Equal(state.PendingGeneration, second.PendingGeneration) {
		t.Fatalf("fixture reused pending episode=%+v error=%v", second, err)
	}
	if err := ref.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if backend.binding(3).State != storage.NameDetached {
		t.Fatal("ordinary last-reference close did not detach renamed object")
	}
}
