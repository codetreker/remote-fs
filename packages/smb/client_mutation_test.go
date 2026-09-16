package smb

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestClientConditionalMetadataRetriesOnlyKnownRevision(t *testing.T) {
	for _, unknown := range []bool{false, true} {
		t.Run(map[bool]string{false: "known", true: "unknown"}[unknown], func(t *testing.T) {
			f, native, session, raw, _, _ := mutationFile(t)
			original := clientTestAction(t)
			var actions []storage.FileActionID
			native.onSetAttr = func(change storage.AttrChange, id storage.FileActionID) (storage.FileActionReceipt, error) {
				actions = append(actions, id)
				if len(actions) == 1 {
					if change.ExpectedRevision != 7 {
						t.Fatal(change)
					}
					native.observation.Attr.MetadataRevision = 8
					native.observation.Attr.Metadata = storage.Metadata{{Key: "foreign.owner", Version: 2, Data: []byte("new")}}
					return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision}}, syscall.EAGAIN
				}
				foreign, ok := change.Metadata.Get("foreign.owner")
				if change.ExpectedRevision != 8 || !ok || string(foreign.Data) != "new" {
					t.Fatal(change)
				}
				if unknown {
					return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EIO
				}
				return mutationReceipt(id), nil
			}
			hidden := uint32(dosHidden)
			result, err := f.SetAttr(t.Context(), windowsAttrChange{DOSAttributes: &hidden}, original)
			if len(actions) != 2 || actions[0] != original || actions[1] == original {
				t.Fatal(actions, err)
			}
			if unknown {
				if !errors.Is(err, syscall.EIO) {
					t.Fatal(err)
				}
				raw.onQuery = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
					if id != actions[1] {
						t.Fatal(id)
					}
					r := mutationReceipt(id)
					r.HistoryRemaining = time.Minute
					return r, nil
				}
				result, err = session.QueryAction(t.Context(), original)
			}
			if err != nil || result.Action != original || result.Receipt.Action != actions[1] {
				t.Fatal(result, err)
			}
		})
	}
}
func TestClientConditionalLocalFailureDoesNotRetainHistory(t *testing.T) {
	f, native, session, _, _, _ := mutationFile(t)
	native.onStat = func(storage.ObservationOptions) (storage.FileObservation, error) {
		return storage.FileObservation{}, syscall.EAGAIN
	}
	if _, err := f.SetAttr(t.Context(), windowsAttrChange{}, clientTestAction(t)); !errors.Is(err, syscall.EAGAIN) || len(native.stats) != 8 || len(session.actions) != 0 {
		t.Fatal(err, len(native.stats), len(session.actions))
	}
	native.onStat = func(storage.ObservationOptions) (storage.FileObservation, error) {
		return storage.FileObservation{}, errors.Join(syscall.EAGAIN, syscall.EIO)
	}
	native.stats = nil
	if _, err := f.SetAttr(t.Context(), windowsAttrChange{}, clientTestAction(t)); !errors.Is(err, syscall.EIO) || len(native.stats) != 1 {
		t.Fatal(err, len(native.stats))
	}
}
func TestClientStoppedSymlinkUsesAtomicReceiptAndRetiresProbe(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "lost reply"}[lost], func(t *testing.T) {
			s, raw, _, parent, link := newClientTestSession(t)
			link.observation.Attr.Kind = storage.NodeSymlink
			link.observation.LinkTarget = []byte("../target")
			link.observation.Attr.Size = int64(len(link.observation.LinkTarget))
			setTestDOS(t, &link.observation.Attr, windowsMetadata{Attributes: dosReadOnly})
			parent.entries[0].Attr = link.observation.Attr.Clone()
			closed, calls := 0, 0
			link.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				closed++
				return mutationReceipt(id), nil
			}
			raw.onRetainAt = func(r storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				calls++
				if r.Claim != (storage.AccessClaim{}) || r.Prepared != nil {
					t.Fatal(r)
				}
				if lost {
					return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EIO
				}
				return clientTestReceipt(id, storage.OpFileRetainAt, link), nil
			}
			raw.onQuery = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return clientTestReceipt(id, storage.OpFileRetainAt, link), nil
			}
			request := clientTestOpenRequest()
			request.Disposition = windowsOverwrite
			request.Access = windowsWriteData
			d := newFileDispatcher(s.backend, s, 17, DefaultLimits())
			result, err := d.open(t.Context(), request, clientTestAction(t))
			var symlink *windowsSymlinkError
			if !errors.As(err, &symlink) || symlink.Target != "../target" || symlink.Location.Path != "Parent/Report.TXT" || result.File != nil || calls != 1 || closed != 1 {
				t.Fatal(result, err, calls, closed)
			}
		})
	}
}
func TestClientFailedRetentionRetiresReturnedReference(t *testing.T) {
	s, raw, _, _, file := newClientTestSession(t)
	closed := 0
	file.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		closed++
		return mutationReceipt(id), nil
	}
	raw.onRetainAt = func(_ storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		r := clientTestReceipt(id, storage.OpFileRetainAt, file)
		r.Errno = syscall.EACCES
		return r, syscall.EACCES
	}
	result, err := s.Open(t.Context(), clientTestOpenRequest(), clientTestAction(t))
	if !errors.Is(err, syscall.EACCES) || result.File != nil || closed != 1 {
		t.Fatal(result, err, closed)
	}
}

func TestClientAppendOnlyUsesAtomicSizeCondition(t *testing.T) {
	file, native, _, _, root, _ := mutationFile(t)
	file.access = windowsAppendData
	native.onWrite = func(r storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		if r.ExpectedSize == nil || *r.ExpectedSize != 9 || r.Offset != 9 || r.Owner == nil || *r.Owner != 91 {
			t.Fatal(r)
		}
		return mutationReceipt(id), nil
	}
	if _, err := file.WriteAt(t.Context(), 9, []byte("append"), clientTestAction(t)); err != nil {
		t.Fatal(err)
	}
	if len(native.stats) != 0 || len(root.lists) != 0 {
		t.Fatal("append used racy observation", native.stats, root.lists)
	}
	if _, err := file.Truncate(t.Context(), 0, clientTestAction(t)); !errors.Is(err, syscall.EACCES) {
		t.Fatal(err)
	}
	file.access |= windowsWriteData
	native.onWrite = func(r storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		if r.ExpectedSize != nil {
			t.Fatal("ordinary write acquired append condition")
		}
		return mutationReceipt(id), nil
	}
	if _, err := file.WriteAt(t.Context(), 2, []byte("overwrite"), clientTestAction(t)); err != nil {
		t.Fatal(err)
	}
}
func TestClientCurrentRejectionDoesNotConsultHistoricalReceipt(t *testing.T) {
	session, raw, _, parent, child := newClientTestSession(t)
	parent.entries = nil
	queries := 0
	raw.onQuery = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		queries++
		return clientTestReceipt(id, storage.OpFileCreateAndRetainAt, child), nil
	}
	rejected := &storage.FileError{Code: syscall.EIO, NotAdmitted: true}
	raw.onCreate = func(_ storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{}, rejected
	}
	request := clientTestOpenRequest()
	request.Disposition = windowsCreate
	d := newFileDispatcher(session.backend, session, 17, DefaultLimits())
	opened, known, err := d.openOutcome(t.Context(), request, clientTestAction(t))
	if !errors.Is(err, syscall.EIO) || !known || opened.File != nil || queries != 0 || d.failed || len(session.actions) != 0 {
		t.Fatal(opened, known, err, queries, d.failed, len(session.actions))
	}
}
func TestClientDeniedReconciliationDoesNotResolveUnknownMutation(t *testing.T) {
	f, native, session, raw, _, _ := mutationFile(t)
	queries := 0
	native.onWrite = func(_ storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EIO
	}
	raw.onQuery = func(storage.FileActionID) (storage.FileActionReceipt, error) {
		queries++
		return storage.FileActionReceipt{}, &storage.FileError{Code: syscall.EACCES, NotAdmitted: true}
	}
	d := newFileDispatcher(session.backend, session, 17, DefaultLimits())
	id := clientTestAction(t)
	result, err := f.WriteAt(t.Context(), 0, []byte("x"), id)
	if status := d.mutationResult(t.Context(), id, result, err); status != fileIOError || queries != 1 || !d.failed {
		t.Fatal(status, queries, d.failed)
	}
}
func TestClientRawRejectedWriteCarriesNoMutationReceipt(t *testing.T) {
	f, native, session, _, _, _ := mutationFile(t)
	id := clientTestAction(t)
	native.onWrite = func(storage.FileWriteRequest, storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{}, &storage.FileError{Code: syscall.EIO, NotAdmitted: true}
	}
	result, err := f.WriteAt(t.Context(), 0, []byte("x"), id)
	d := newFileDispatcher(session.backend, session, 17, DefaultLimits())
	if status := d.mutationResult(t.Context(), id, result, err); status != fileIOError || d.failed || !result.notAdmitted || result.Receipt.Action != "" {
		t.Fatal(status, d.failed, result, err)
	}
}

func TestClientAppendConditionMapsOnlyKnownSizeFailure(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "reconciled"}[lost], func(t *testing.T) {
			f, native, s, raw, _, _ := mutationFile(t)
			f.access = windowsAppendData
			id := clientTestAction(t)
			failure := func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision, NodeID: 3}}, &storage.FileError{Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision, NodeID: 3}}
			}
			native.onWrite = func(r storage.FileWriteRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				if r.ExpectedSize == nil || *r.ExpectedSize != 3 {
					t.Fatal(r)
				}
				if lost {
					return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EIO
				}
				return failure(id)
			}
			r, err := f.WriteAt(t.Context(), 3, []byte("x"), id)
			if lost {
				if !errors.Is(err, syscall.EIO) {
					t.Fatal(err)
				}
				raw.onQuery = failure
				r, err = s.QueryAction(t.Context(), id)
			}
			if storage.ErrnoOf(err) != syscall.EACCES || !errors.Is(err, syscall.EAGAIN) || r.Errno != syscall.EACCES || r.Receipt.Errno != syscall.EAGAIN {
				t.Fatal(r, err)
			}
		})
	}
	if storage.ErrnoOf(&windowsError{Err: syscall.EINVAL}) != syscall.EINVAL {
		t.Fatal("default classification lost")
	}
}
func TestClientOverwriteKeepsRequiredAttributesAndArchive(t *testing.T) {
	s, raw, _, parent, file := newClientTestSession(t)
	setTestDOS(t, &file.observation.Attr, windowsMetadata{Attributes: dosHidden | dosSystem})
	parent.entries[0].Attr = file.observation.Attr.Clone()
	calls := 0
	raw.onReset = func(r storage.ResetAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		value, ok := r.Change.Metadata.Get(windowsMetadataKey)
		m, err := decodeWindowsMetadata(value.Data, value.Version, ok)
		if err != nil || m.Attributes != dosHidden|dosSystem|dosArchive || r.ExpectedRevision != 7 || r.Target.ExpectedMetadataRevision != 7 {
			t.Fatal(r, m, err)
		}
		return clientTestReceipt(id, storage.OpFileResetAndRetainAt, file), nil
	}
	request := clientTestOpenRequest()
	request.Access = windowsWriteData
	request.Disposition = windowsOverwrite
	request.DOSAttributes = dosNormal
	if _, err := s.Open(t.Context(), request, clientTestAction(t)); !errors.Is(err, syscall.EACCES) || calls != 0 {
		t.Fatal(err, calls)
	}
	request.DOSAttributes = dosHidden | dosSystem
	if _, err := s.Open(t.Context(), request, clientTestAction(t)); err != nil || calls != 1 {
		t.Fatal(err, calls)
	}
}

func TestClientClaimsPreserveMetadataOnlyAndDirectoryDeleteSharing(t *testing.T) {
	metadata := windowsOpenIntent{Access: windowsReadAttributes | windowsWriteAttributes | windowsReadSecurity | windowsSynchronize, Share: 0}
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory, storage.NodeSymlink} {
		if claim := windowsClaim(metadata, kind); claim != (storage.AccessClaim{}) {
			t.Fatal(kind, claim)
		}
	}
	directory := windowsOpenIntent{Access: windowsReadData | windowsWriteData, Share: windowsShareRead | windowsShareWrite}
	if claim := windowsClaim(directory, storage.NodeDirectory); claim != (storage.AccessClaim{Excludes: storage.RemoveEntry}) {
		t.Fatal(claim)
	}
	directory.Share = windowsShareAll
	if claim := windowsClaim(directory, storage.NodeDirectory); claim != (storage.AccessClaim{}) {
		t.Fatal(claim)
	}
}

func TestClientRenameSeparatesExactMatchAndRequestedSpelling(t *testing.T) {
	for _, same := range []bool{false, true} {
		t.Run(map[bool]string{false: "replacement", true: "case-only source"}[same], func(t *testing.T) {
			file, native, _, _, _, parent := mutationFile(t)
			name, old, entry, node := "OTHER.TXT", "Other.txt", storage.EntryID(24), uint64(4)
			if same {
				name, old, entry, node = "REPORT.TXT", "Report.TXT", 23, 3
			} else {
				parent.entries = append(parent.entries, storage.DirectoryEntry{EntryID: entry, Name: []byte(old), Attr: storage.Attr{ID: node, Kind: storage.NodeRegular, MetadataRevision: 9}})
			}
			calls := 0
			native.onRename = func(r storage.RenameRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				calls++
				if string(r.Destination.Name) != old || r.Destination.ExpectedEntryID != entry || r.Destination.ExpectedNodeID != node || string(r.NewName) != name || r.Source.ExpectedEntryID != 23 || r.Source.ExpectedNodeID != 3 || r.Destination.Witness == nil {
					t.Fatal(r)
				}
				return mutationReceipt(id), nil
			}
			request := windowsRenameRequest{Destination: windowsLookup{ParentID: 2, ParentReference: 22, Name: name}, Replace: !same}
			if _, err := file.Rename(t.Context(), request, clientTestAction(t)); err != nil || calls != 1 {
				t.Fatal(err, calls)
			}
			if !same {
				request.Replace = false
				if _, err := file.Rename(t.Context(), request, clientTestAction(t)); !errors.Is(err, syscall.EEXIST) || calls != 1 {
					t.Fatal(err, calls)
				}
			}
		})
	}
}

func TestClientSetLinkAcceptsEitherWritePermission(t *testing.T) {
	for _, access := range []windowsAccess{windowsWriteData, windowsWriteAttributes, windowsReadAttributes} {
		f, native, _, _, _, _ := mutationFile(t)
		f.access = access
		calls := 0
		native.onSetKind = func(r storage.SetKindRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
			calls++
			if r.Owner == nil || *r.Owner != f.owner {
				t.Fatal(r.Owner)
			}
			return mutationReceipt(id), nil
		}
		_, err := f.SetLink(t.Context(), "target", clientTestAction(t))
		if access == windowsReadAttributes {
			if !errors.Is(err, syscall.EACCES) || calls != 0 {
				t.Fatal(access, err, calls)
			}
		} else if err != nil || calls != 1 {
			t.Fatal(access, err, calls)
		}
	}
}
