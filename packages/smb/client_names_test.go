package smb

import (
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestClientDirectoryCapturesCompleteRevisionBeforeLookup(t *testing.T) {
	session, _, _, parent, child := newClientTestSession(t)
	first := storage.DirectoryEntry{EntryID: 20, Name: []byte("Alpha"), Attr: storage.Attr{ID: 20, Kind: storage.NodeRegular, MetadataRevision: 1}}
	calls := 0
	parent.onList = func(r storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
		calls++
		if r.Revision != 41 {
			t.Fatal(r)
		}
		if calls == 1 {
			return storage.DirectoryPage{ParentID: 2, Revision: 41, Entries: []storage.DirectoryEntry{first}, Next: storage.DirectoryCursor{ParentID: 2, Revision: 41, After: []byte("Alpha")}}, nil
		}
		if string(r.Cursor.After) != "Alpha" || r.Cursor.Revision != 41 {
			t.Fatal(r.Cursor)
		}
		return storage.DirectoryPage{ParentID: 2, Revision: 41, Entries: parent.entries, Done: true}, nil
	}
	target, observed, exists, err := session.lookup(t.Context(), windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt"})
	if err != nil || !exists || observed.Attr.ID != 3 || calls != 2 || string(target.Name) != "Report.TXT" || target.ExpectedEntryID != 23 || target.ExpectedMetadataRevision != child.observation.Attr.MetadataRevision {
		t.Fatalf("target=%+v observed=%+v exists=%v error=%v calls=%d", target, observed, exists, err, calls)
	}
	if len(parent.checks) != 1 || parent.checks[0].DirectoryRevision != 41 || !reflect.DeepEqual(parent.checks[0].Location, *parent.observation.Location) {
		t.Fatal(parent.checks)
	}
}

func TestClientDirectoryRejectsUnrepresentableAndAmbiguousSiblings(t *testing.T) {
	for _, bad := range [][]byte{[]byte("bad:name"), []byte("REPORT.txt"), {0xff}} {
		t.Run(string(bad), func(t *testing.T) {
			session, _, _, parent, _ := newClientTestSession(t)
			parent.entries = append(parent.entries, storage.DirectoryEntry{EntryID: 90, Name: bad, Attr: storage.Attr{ID: 90, Kind: storage.NodeRegular, MetadataRevision: 1}})
			if _, _, _, err := session.lookup(t.Context(), windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt"}); err == nil {
				t.Fatal("unrepresentable sibling ignored")
			}
			file := &clientFile{session: session, raw: parent, access: windowsReadData}
			result, err := newWindowsListResult(10000, 0, func(int, int64, windowsBasicAttr) (int64, error) { return 10, nil })
			if err != nil {
				t.Fatal(err)
			}
			if err = file.ListBounded(t.Context(), result); err == nil {
				t.Fatal("bad complete view produced listing")
			}
		})
	}
}

func TestClientDirectoryRejectsRevisionAndIncompletePages(t *testing.T) {
	for _, page := range []storage.DirectoryPage{{ParentID: 99, Revision: 41, Done: true}, {ParentID: 2, Revision: 42, Done: true}, {ParentID: 2, Revision: 41, Done: true, Next: storage.DirectoryCursor{ParentID: 2}}, {ParentID: 2, Revision: 41}, {ParentID: 2, Revision: 41, Next: storage.DirectoryCursor{ParentID: 2}}} {
		session, _, _, parent, _ := newClientTestSession(t)
		parent.onList = func(storage.DirectoryPageRequest) (storage.DirectoryPage, error) { return page, nil }
		if _, err := session.directory(t.Context(), parent, 41); !errors.Is(err, syscall.EIO) {
			t.Fatal(page, err)
		}
	}
	session, _, _, parent, _ := newClientTestSession(t)
	session.backend.limits.MaxDirectoryBytes = 255
	if _, err := session.directory(t.Context(), parent, 41); !errors.Is(err, syscall.ENOMEM) || len(parent.lists) != 0 {
		t.Fatal(err, parent.lists)
	}
	session.backend.limits.MaxDirectoryBytes = 512
	if _, err := session.directory(t.Context(), parent, 41); !errors.Is(err, syscall.ENOMEM) {
		t.Fatal(err)
	}
}

func TestClientNameObservationChecksWholeLocationAndDetachedIdentity(t *testing.T) {
	session, _, root, _, child := newClientTestSession(t)
	file := &clientFile{session: session, raw: child, access: windowsReadAttributes}
	attr, err := file.ObserveName(t.Context())
	if err != nil || attr.NameInfo.Path != "Parent/Report.TXT" || len(child.checks) != 1 {
		t.Fatal(attr, err, child.checks)
	}
	child.checkErr = syscall.EAGAIN
	if _, err = file.ObserveName(t.Context()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatal(err)
	}
	child.checkErr = nil
	child.observation.Location = &storage.EntryLocation{State: storage.LocationDetached, RootNodeID: 1, NodeID: 3}
	before := len(root.lists)
	attr, err = file.ObserveName(t.Context())
	if err != nil || attr.NameInfo.State != windowsNameDetached || attr.NameInfo.Path != "" || len(root.lists) != before {
		t.Fatal(attr, err)
	}
	file.raw = root
	attr, err = file.ObserveName(t.Context())
	if err != nil || attr.NameInfo.State != windowsNameRoot {
		t.Fatal(attr, err)
	}
}

func TestClientLookupPinnedProofRejectsChangedAncestor(t *testing.T) {
	session, _, _, parent, _ := newClientTestSession(t)
	proof := &nameProof{Observation: parent.observation.Clone(), Location: parent.observation.Location.Clone()}
	parent.checkErr = syscall.EAGAIN
	_, _, _, err := session.lookup(t.Context(), windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt", proof: proof})
	if !errors.Is(err, syscall.EAGAIN) || len(parent.stats) != 1 || len(parent.checks) != 1 || parent.checks[0].MetadataRevision != proof.Observation.Attr.MetadataRevision {
		t.Fatal(err, parent.stats, parent.checks)
	}
	beforeLists := len(parent.lists)
	parent.observation.Location.Ancestors[0].Name = []byte("Moved")
	if _, _, _, err = session.lookup(t.Context(), windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt", proof: proof}); !errors.Is(err, syscall.ESTALE) || len(parent.lists) != beforeLists {
		t.Fatal("changed ancestor was reinterpreted", err, parent.lists)
	}
	parent.observation.Location.Ancestors[0].Name = []byte("Parent")
	proof.Location.NodeID = 99
	if _, _, _, err = session.lookup(t.Context(), windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt", proof: proof}); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	if _, _, _, err = session.lookup(t.Context(), windowsLookup{ParentID: 99, ParentReference: 22, Name: "report.txt"}); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
	parent.checkErr = nil
	if _, _, _, err = session.lookup(t.Context(), windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt", ExpectedID: 99}); !errors.Is(err, syscall.ESTALE) {
		t.Fatal(err)
	}
}

func TestClientDirectoryTemporaryFailureRetainsAllCausesAndClosesOwner(t *testing.T) {
	session, raw, root, _, _ := newClientTestSession(t)
	viewFailure := errors.New("directory source unavailable")
	closeFailure := errors.New("reference retirement unavailable")
	root.onList = func(storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
		return storage.DirectoryPage{}, viewFailure
	}
	root.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.ENOSPC}, closeFailure
	}
	closed := 0
	raw.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		closed++
		return mutationReceipt(id), nil
	}
	_, _, _, err := session.lookup(t.Context(), windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt"})
	if !errors.Is(err, viewFailure) || !errors.Is(err, closeFailure) || !errors.Is(err, syscall.EIO) || closed != 1 {
		t.Fatalf("ownership or failure lost: %v closes=%d", err, closed)
	}
}

func TestClientTemporaryUnknownCloseQueriesExactAction(t *testing.T) {
	session, raw, root, _, _ := newClientTestSession(t)
	var closeID storage.FileActionID
	root.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		closeID = id
		return storage.FileActionReceipt{}, syscall.EIO
	}
	raw.onQuery = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		if id != closeID {
			t.Fatal("cleanup queried a new action")
		}
		return mutationReceipt(id), nil
	}
	if err := session.closeTemporary(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	if session.closed || closeID == "" {
		t.Fatal("known retirement fenced owner")
	}
}

func TestClientFailedRetainWithReferenceStillClosesSession(t *testing.T) {
	session, raw, _, _, _ := newClientTestSession(t)
	cause := errors.New("admission response lost")
	raw.onRetain = func(_ storage.RetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted, Reference: 11, Effects: storage.EffectRetained}, cause
	}
	closed := 0
	raw.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		closed++
		return mutationReceipt(id), nil
	}
	_, _, _, err := session.lookup(t.Context(), windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt"})
	if !errors.Is(err, cause) || !errors.Is(err, syscall.EIO) || closed != 1 {
		t.Fatal(err, closed)
	}
}

func TestClientListFailureInvalidatesAccumulatedPrefix(t *testing.T) {
	session, _, _, parent, _ := newClientTestSession(t)
	parent.entries = append(parent.entries, storage.DirectoryEntry{EntryID: 99, Name: []byte("second"), Attr: storage.Attr{ID: 99, Kind: storage.NodeRegular, MetadataRevision: 1, Metadata: storage.Metadata{{Key: windowsMetadataKey, Version: 2, Data: make([]byte, 8)}}}})
	file := &clientFile{session: session, raw: parent, access: windowsReadData}
	result, err := newWindowsListResult(10000, 0, func(int, int64, windowsBasicAttr) (int64, error) { return 10, nil })
	if err != nil {
		t.Fatal(err)
	}
	err = file.ListBounded(t.Context(), result)
	if !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatal(err)
	}
	entries, resultErr := result.Entries()
	if entries != nil || !errors.Is(resultErr, syscall.EOPNOTSUPP) {
		t.Fatalf("failed listing exposes prefix: %+v %v", entries, resultErr)
	}
}

func TestClientUnresolvableTemporaryReferenceRetainsCleanupOwnership(t *testing.T) {
	for _, kind := range []string{"reference error", "nil reference", "wrong node"} {
		t.Run(kind, func(t *testing.T) {
			session, raw, _, _, _ := newClientTestSession(t)
			closes := 0
			raw.onReference = func(id storage.FileReferenceID) (storage.File, error) {
				if id != 11 {
					return raw.files[id], nil
				}
				switch kind {
				case "reference error":
					return nil, syscall.EAGAIN
				case "nil reference":
					return nil, nil
				default:
					return &clientTestFile{ref: 11, observation: storage.FileObservation{Attr: storage.Attr{ID: 99, Kind: storage.NodeDirectory}}}, nil
				}
			}
			raw.onClose = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				closes++
				if closes == 1 {
					return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EBUSY
				}
				return mutationReceipt(id), nil
			}
			_, _, _, err := session.lookup(t.Context(), windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt"})
			var cleanup *clientCleanupError
			if !errors.As(err, &cleanup) || !errors.Is(err, syscall.EIO) || !errors.Is(err, syscall.EBUSY) || closes != 1 || session.closed {
				t.Fatal(err, closes, session.closed)
			}
			if err = session.Close(t.Context()); err != nil || !session.closed || closes != 2 {
				t.Fatal(err, closes, session.closed)
			}
		})
	}
}
