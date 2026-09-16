package smb

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

type mutationClientFile struct {
	*clientTestFile
	onRead        func(storage.FileReadRequest) (storage.FileRead, error)
	onWrite       func(storage.FileWriteRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onTruncate    func(storage.FileTruncateRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onSetAttr     func(storage.AttrChange, storage.FileActionID) (storage.FileActionReceipt, error)
	onDrain       func(storage.DrainEntryRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onCancelDrain func(storage.CancelDrainRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onRename      func(storage.RenameRequest, storage.FileActionID) (storage.FileActionReceipt, error)
	onSetKind     func(storage.SetKindRequest, storage.FileActionID) (storage.FileActionReceipt, error)
}

func (f *mutationClientFile) ReadAt(_ context.Context, r storage.FileReadRequest) (storage.FileRead, error) {
	return f.onRead(r)
}
func (f *mutationClientFile) WriteAt(_ context.Context, r storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.onWrite(r, id)
}
func (f *mutationClientFile) Truncate(_ context.Context, r storage.FileTruncateRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.onTruncate(r, id)
}
func (f *mutationClientFile) SetAttr(_ context.Context, r storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.onSetAttr(r, id)
}
func (f *mutationClientFile) DrainEntry(_ context.Context, r storage.DrainEntryRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.onDrain(r, id)
}
func (f *mutationClientFile) CancelDrain(_ context.Context, r storage.CancelDrainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.onCancelDrain(r, id)
}
func (f *mutationClientFile) Rename(_ context.Context, r storage.RenameRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.onRename(r, id)
}
func (f *mutationClientFile) SetKind(_ context.Context, r storage.SetKindRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
	return f.onSetKind(r, id)
}
func mutationReceipt(id storage.FileActionID) storage.FileActionReceipt {
	return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted}
}
func mutationFile(t *testing.T) (*clientFile, *mutationClientFile, *clientSession, *clientTestSession, *clientTestFile, *clientTestFile) {
	t.Helper()
	session, raw, root, parent, child := newClientTestSession(t)
	native := &mutationClientFile{clientTestFile: child}
	return &clientFile{session: session, raw: native, access: windowsAllAccess, owner: 91}, native, session, raw, root, parent
}
func setTestDOS(t *testing.T, attr *storage.Attr, metadata windowsMetadata) {
	t.Helper()
	payload, err := encodeWindowsMetadata(metadata)
	if err != nil {
		t.Fatal(err)
	}
	attr.Metadata, err = attr.Metadata.With(storage.OpaqueMetadata{Key: windowsMetadataKey, Version: windowsMetadataVersion, Data: payload})
	if err != nil {
		t.Fatal(err)
	}
}

func TestClientIdentityIOCarriesRangeOwnerWithoutNameLookup(t *testing.T) {
	file, native, _, _, root, _ := mutationFile(t)
	root.entries = append(root.entries, storage.DirectoryEntry{EntryID: 98, Name: []byte("bad:name"), Attr: storage.Attr{ID: 99, Kind: storage.NodeRegular}})
	native.observation.Location.Ancestors[0].Name = []byte("unrepresentable:name")
	calls := 0
	native.onRead = func(r storage.FileReadRequest) (storage.FileRead, error) {
		calls++
		if r.Offset != 4 || r.Length != 3 || r.Owner == nil || *r.Owner != 91 {
			t.Fatal(r)
		}
		return storage.FileRead{Attr: native.observation.Attr, Data: []byte("new")}, nil
	}
	native.onWrite = func(r storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		if r.Offset != 8 || string(r.Data) != "bytes" || r.Owner == nil || *r.Owner != 91 {
			t.Fatal(r)
		}
		return mutationReceipt(id), nil
	}
	native.onTruncate = func(r storage.FileTruncateRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		if r.Size != 10 || r.Owner == nil || *r.Owner != 91 {
			t.Fatal(r)
		}
		return mutationReceipt(id), nil
	}
	read, err := file.ReadAt(t.Context(), 4, 3)
	if err != nil || string(read.Data) != "new" {
		t.Fatal(read, err)
	}
	if _, err = file.WriteAt(t.Context(), 8, []byte("bytes"), clientTestAction(t)); err != nil {
		t.Fatal(err)
	}
	if _, err = file.Truncate(t.Context(), 10, clientTestAction(t)); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || len(native.stats) != 0 || len(root.lists) != 0 {
		t.Fatalf("identity operations touched names: calls=%d stats=%v lists=%v", calls, native.stats, root.lists)
	}
}

func TestClientIdentityIOEnforcesRightsAndPropagatesErrors(t *testing.T) {
	file, native, session, _, _, _ := mutationFile(t)
	operations := []func() error{func() error { _, e := file.ReadAt(t.Context(), 0, 1); return e }, func() error { _, e := file.WriteAt(t.Context(), 0, []byte("x"), clientTestAction(t)); return e }, func() error { _, e := file.Truncate(t.Context(), 0, clientTestAction(t)); return e }}
	file.access = windowsReadAttributes
	for _, operation := range operations {
		if err := operation(); !errors.Is(err, syscall.EACCES) {
			t.Fatal(err)
		}
	}
	file.access = windowsAllAccess
	cause := errors.New("authority unavailable")
	session.backend.authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return cause })
	for _, operation := range operations {
		if err := operation(); !errors.Is(err, cause) {
			t.Fatal(err)
		}
	}
	session.backend.authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
	native.onRead = func(storage.FileReadRequest) (storage.FileRead, error) { return storage.FileRead{}, cause }
	native.onWrite = func(storage.FileWriteRequest, storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{}, cause
	}
	native.onTruncate = func(storage.FileTruncateRequest, storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{}, cause
	}
	for _, operation := range operations {
		if err := operation(); !errors.Is(err, cause) {
			t.Fatal(err)
		}
	}
}

func TestClientSetAttrPreservesForeignMetadataAndDirectoryHint(t *testing.T) {
	file, native, _, _, _, _ := mutationFile(t)
	native.observation.Attr.Kind = storage.NodeSymlink
	native.observation.Attr.Metadata = storage.Metadata{{Key: "business", Version: 9, Data: []byte("opaque")}}
	setTestDOS(t, &native.observation.Attr, windowsMetadata{Attributes: dosHidden, DirectorySymlink: true})
	before := native.observation.Attr.Metadata.Clone()
	stamp := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	attributes := uint32(dosArchive)
	native.onSetAttr = func(r storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
		if r.ExpectedRevision != 7 || r.Metadata == nil || r.CreationTime == nil || !r.CreationTime.Equal(stamp) || r.ChangeTime == nil || !r.ChangeTime.Equal(stamp) {
			t.Fatal(r)
		}
		foreign, _ := r.Metadata.Get("business")
		if foreign.Version != 9 || string(foreign.Data) != "opaque" {
			t.Fatal(foreign)
		}
		own, present := r.Metadata.Get(windowsMetadataKey)
		decoded, err := decodeWindowsMetadata(own.Data, own.Version, present)
		if err != nil || decoded.Attributes != dosArchive || !decoded.DirectorySymlink {
			t.Fatal(decoded, err)
		}
		return mutationReceipt(id), nil
	}
	_, err := file.SetAttr(t.Context(), windowsAttrChange{DOSAttributes: &attributes, CreationTime: &stamp, ChangeTime: &stamp}, clientTestAction(t))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, native.observation.Attr.Metadata) || len(native.stats) != 1 || native.stats[0].IncludeLocation {
		t.Fatal("metadata update modified input or observed names")
	}
}

func TestClientDrainAndCancelPreservePreparedIntent(t *testing.T) {
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory} {
		t.Run(map[storage.NodeKind]string{storage.NodeRegular: "file", storage.NodeDirectory: "directory"}[kind], func(t *testing.T) {
			file, native, _, _, _, _ := mutationFile(t)
			native.observation.Attr.Kind = kind
			native.observation.Removal = storage.RemovalStatus{Prepared: true, IntentID: 4, EntryID: 23, State: storage.EntryDraining, Generation: 8}
			drains, cancels := 0, 0
			native.onDrain = func(r storage.DrainEntryRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				drains++
				expected := storage.RemovalFile
				if kind == storage.NodeDirectory {
					expected = storage.RemovalIfEmpty
				}
				if r.ExpectedMetadataRevision != 7 || r.Condition != expected || r.Entry.EntryID != 23 || !reflect.DeepEqual(r.Witness, *native.observation.Location) {
					t.Fatal(r)
				}
				return mutationReceipt(id), nil
			}
			native.onCancelDrain = func(r storage.CancelDrainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				cancels++
				if r.EntryID != 23 || r.Generation != 8 {
					t.Fatal(r)
				}
				return mutationReceipt(id), nil
			}
			if _, err := file.SetDeletePending(t.Context(), true, clientTestAction(t)); err != nil {
				t.Fatal(err)
			}
			if _, err := file.SetDeletePending(t.Context(), false, clientTestAction(t)); err != nil {
				t.Fatal(err)
			}
			if drains != 1 || cancels != 1 || !native.observation.Removal.Prepared || native.observation.Removal.IntentID != 4 {
				t.Fatalf("prepared ownership changed: %+v", native.observation.Removal)
			}
		})
	}
}

func TestClientDrainEnforcesReadonlyAndNativeEmptyCheck(t *testing.T) {
	file, native, _, _, _, _ := mutationFile(t)
	setTestDOS(t, &native.observation.Attr, windowsMetadata{Attributes: dosReadOnly})
	if _, err := file.SetDeletePending(t.Context(), true, clientTestAction(t)); !errors.Is(err, syscall.EACCES) {
		t.Fatal(err)
	}
	native.observation.Attr.Kind = storage.NodeDirectory
	native.onDrain = func(r storage.DrainEntryRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		if r.Condition != storage.RemovalIfEmpty {
			t.Fatal(r)
		}
		return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.ENOTEMPTY}, syscall.ENOTEMPTY
	}
	if _, err := file.SetDeletePending(t.Context(), true, clientTestAction(t)); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatal(err)
	}
	file.access = windowsReadAttributes
	if _, err := file.SetDeletePending(t.Context(), false, clientTestAction(t)); !errors.Is(err, syscall.EACCES) {
		t.Fatal(err)
	}
}

func TestClientReadLinkUsesAtomicTargetAndLocation(t *testing.T) {
	file, native, _, _, _, _ := mutationFile(t)
	native.observation.Attr.Kind = storage.NodeSymlink
	native.observation.LinkTarget = []byte("../target")
	info, err := file.ReadLink(t.Context())
	if err != nil || info.Target != "../target" || info.Location.Path != "Parent/Report.TXT" {
		t.Fatal(info, err)
	}
	if len(native.stats) != 1 || !native.stats[0].IncludeLocation || !native.stats[0].IncludeLinkTarget || len(native.checks) != 1 || native.checks[0].MetadataRevision != 7 {
		t.Fatal(native.stats, native.checks)
	}
	native.checkErr = syscall.EAGAIN
	if _, err = file.ReadLink(t.Context()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	native.checkErr = nil
	for _, target := range []string{"../../escape", "C:/outside", "//server/share", `\\server\share`} {
		native.observation.LinkTarget = []byte(target)
		if _, err = file.ReadLink(t.Context()); err == nil {
			t.Fatalf("outside target %q accepted", target)
		}
	}
	native.observation.Attr.Kind = storage.NodeRegular
	if _, err = file.ReadLink(t.Context()); windowsFailureOf(err) != windowsNotReparsePoint {
		t.Fatal(err)
	}
}

func TestClientSetLinkPinsNativeConversionAndPreservesForeignMetadata(t *testing.T) {
	file, native, _, _, _, _ := mutationFile(t)
	native.observation.Attr.Kind = storage.NodeDirectory
	native.observation.Attr.Size = 0
	native.observation.Attr.Metadata = storage.Metadata{{Key: "business", Version: 3, Data: []byte("opaque")}}
	setTestDOS(t, &native.observation.Attr, windowsMetadata{Attributes: dosHidden})
	calls := 0
	native.onSetKind = func(r storage.SetKindRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		if r.Kind != storage.NodeSymlink || r.ExpectedRevision != 7 || string(r.LinkTarget) != "../target" || r.Witness == nil || !reflect.DeepEqual(*r.Witness, *native.observation.Location) {
			t.Fatal(r)
		}
		foreign, _ := r.Metadata.Get("business")
		if string(foreign.Data) != "opaque" || foreign.Version != 3 {
			t.Fatal(foreign)
		}
		own, present := r.Metadata.Get(windowsMetadataKey)
		decoded, err := decodeWindowsMetadata(own.Data, own.Version, present)
		if err != nil || !decoded.DirectorySymlink || decoded.Attributes != dosHidden {
			t.Fatal(decoded, err)
		}
		return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EBUSY}, syscall.EBUSY
	}
	if _, err := file.SetLink(t.Context(), "../target", clientTestAction(t)); !errors.Is(err, syscall.EBUSY) || calls != 1 {
		t.Fatalf("sole reference rejection ignored: %v calls=%d", err, calls)
	}
	if _, err := file.SetLink(t.Context(), "../../outside", clientTestAction(t)); err == nil || calls != 1 {
		t.Fatal(err, calls)
	}
	file.access = windowsReadAttributes
	if _, err := file.SetLink(t.Context(), "../target", clientTestAction(t)); !errors.Is(err, syscall.EACCES) {
		t.Fatal(err)
	}
}

func TestClientDispositionFalseConfirmsActivePreparedWithoutCancellation(t *testing.T) {
	file, native, _, _, _, _ := mutationFile(t)
	native.observation.Removal = storage.RemovalStatus{Prepared: true, IntentID: 4, EntryID: 23, State: storage.EntryActive}
	result, err := file.SetDeletePending(t.Context(), false, clientTestAction(t))
	if err != nil || result.State != windowsActionCompleted || result.Receipt.Action != "" || len(native.checks) != 1 || !native.observation.Removal.Prepared {
		t.Fatalf("result=%+v err=%v checks=%v removal=%+v", result, err, native.checks, native.observation.Removal)
	}
	native.checkErr = syscall.EAGAIN
	if _, err = file.SetDeletePending(t.Context(), false, clientTestAction(t)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	native.checkErr = nil
	native.onStat = func(options storage.ObservationOptions) (storage.FileObservation, error) {
		before := native.observation.Clone()
		native.observation.Removal.State = storage.EntryDraining
		native.observation.Removal.Generation = 17
		return before, nil
	}
	cancels := 0
	native.onCancelDrain = func(r storage.CancelDrainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		cancels++
		if r.Generation != 17 || r.EntryID != 23 {
			t.Fatal(r)
		}
		return mutationReceipt(id), nil
	}
	if _, err = file.SetDeletePending(t.Context(), false, clientTestAction(t)); err != nil || cancels != 1 || !native.observation.Removal.Prepared {
		t.Fatal(err, cancels, native.observation.Removal)
	}
}

func TestClientMetadataErrorsDoNotSubmitMutation(t *testing.T) {
	file, native, session, _, _, _ := mutationFile(t)
	attributes := uint32(dosArchive)
	operation := func() error {
		_, err := file.SetAttr(t.Context(), windowsAttrChange{DOSAttributes: &attributes}, clientTestAction(t))
		return err
	}
	file.access = windowsReadAttributes
	if err := operation(); !errors.Is(err, syscall.EACCES) {
		t.Fatal(err)
	}
	file.access = windowsAllAccess
	cause := errors.New("metadata unavailable")
	native.onStat = func(storage.ObservationOptions) (storage.FileObservation, error) {
		return storage.FileObservation{}, cause
	}
	if err := operation(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	native.onStat = nil
	native.observation.Attr.Metadata = storage.Metadata{{Key: windowsMetadataKey, Version: 2, Data: make([]byte, 8)}}
	if err := operation(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatal(err)
	}
	native.observation.Attr.Metadata = nil
	attributes = dosDirectory
	if err := operation(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	session.backend.authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return cause })
	if err := operation(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
}

func TestClientRenameUsesCurrentSourceLocationAndDestinationGuard(t *testing.T) {
	file, native, _, _, root, parent := mutationFile(t)
	current := storage.EntryCondition{ParentID: 1, DirectoryRevision: 31, EntryID: 23, NodeID: 3, Name: []byte("Moved.TXT")}
	native.observation.Location = &storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 3, Ancestors: []storage.EntryCondition{current}}
	root.entries = append(root.entries, storage.DirectoryEntry{EntryID: 23, Name: []byte("Moved.TXT"), Attr: native.observation.Attr.Clone()})
	parent.entries = []storage.DirectoryEntry{{EntryID: 44, Name: []byte("Destination.TXT"), Attr: storage.Attr{ID: 4, Kind: storage.NodeRegular, MetadataRevision: 19}}}
	native.onRename = func(r storage.RenameRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		if r.Source.ParentID != 1 || r.Source.Parent != root.ref || string(r.Source.Name) != "Moved.TXT" || r.Source.ExpectedEntryID != 23 || r.Source.ExpectedMetadataRevision != 7 {
			t.Fatalf("stale source target: %+v", r.Source)
		}
		if r.Destination.ExpectedEntryID != 44 || r.Destination.ExpectedNodeID != 4 || r.Destination.ExpectedMetadataRevision != 19 || r.Destination.DirectoryRevision != 41 || string(r.Destination.Name) != "Destination.TXT" {
			t.Fatal(r.Destination)
		}
		return mutationReceipt(id), nil
	}
	request := windowsRenameRequest{Destination: windowsLookup{ParentID: 2, ParentReference: 22, Name: "destination.txt"}, Replace: true}
	if _, err := file.Rename(t.Context(), request, clientTestAction(t)); err != nil {
		t.Fatal(err)
	}
	setTestDOS(t, &parent.entries[0].Attr, windowsMetadata{Attributes: dosReadOnly})
	if _, err := file.Rename(t.Context(), request, clientTestAction(t)); !errors.Is(err, syscall.EACCES) {
		t.Fatal(err)
	}
	request.Replace = false
	if _, err := file.Rename(t.Context(), request, clientTestAction(t)); !errors.Is(err, syscall.EEXIST) {
		t.Fatal(err)
	}
}
