package smb

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestPlatformIntentRejectsInvalidAccessAndDisposition(t *testing.T) {
	valid := windowsOpenIntent{Disposition: windowsOpen, Share: windowsShareAll}
	if err := valid.Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*windowsOpenIntent){
		func(i *windowsOpenIntent) { i.Access = 1 << 31 }, func(i *windowsOpenIntent) { i.Share = 128 }, func(i *windowsOpenIntent) { i.Kind = 99 }, func(i *windowsOpenIntent) { i.Disposition = 0 }, func(i *windowsOpenIntent) { i.DeleteOnClose = true }, func(i *windowsOpenIntent) { i.Disposition = windowsOverwrite }, func(i *windowsOpenIntent) { i.Disposition = windowsSupersede }, func(i *windowsOpenIntent) {
			i.Kind = windowsDirectory
			i.Disposition = windowsOverwrite
			i.Access = windowsWriteData
		},
	} {
		intent := valid
		change(&intent)
		if !errors.Is(intent.Check(), syscall.EINVAL) {
			t.Fatalf("accepted %+v", intent)
		}
	}
	for _, intent := range []windowsOpenIntent{{Disposition: windowsOpen, DeleteOnClose: true, Access: windowsDelete}, {Disposition: windowsOverwrite, Access: windowsWriteData}, {Disposition: windowsSupersede, Access: windowsDelete}, {Disposition: windowsOpen, DeleteOnClose: true, MaximumAllowed: true}} {
		if err := intent.Check(); err != nil {
			t.Fatal(intent, err)
		}
	}
}

func TestPlatformLookupOpenAndRenameValidation(t *testing.T) {
	for _, lookup := range []windowsLookup{{ParentID: 1}, {ParentReference: 1}, {ExpectedID: 1}, {Name: "file"}, {ParentID: 1, Name: "a:b"}} {
		if lookup.Check() == nil {
			t.Fatal(lookup)
		}
	}
	lookup := windowsLookup{ParentID: 1, ParentReference: 1, Name: "file", ExpectedID: 2}
	if err := lookup.Check(); err != nil {
		t.Fatal(err)
	}
	for _, request := range []windowsOpenRequest{{windowsOpenIntent: windowsOpenIntent{Disposition: windowsCreate}}, {windowsOpenIntent: windowsOpenIntent{Disposition: windowsOpen, Kind: windowsRegularFile}}, {windowsOpenIntent: windowsOpenIntent{Disposition: windowsOpen}, DOSAttributes: dosDirectory}} {
		if request.Check() == nil {
			t.Fatal(request)
		}
	}
	if err := (windowsOpenRequest{windowsOpenIntent: windowsOpenIntent{Disposition: windowsOpen}}).Check(); err != nil {
		t.Fatal(err)
	}
	rename := windowsRenameRequest{Destination: windowsLookup{ParentID: 1, ParentReference: 1, Name: "new"}}
	if err := rename.Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*windowsRenameRequest){func(r *windowsRenameRequest) { r.Destination.Name = "" }, func(r *windowsRenameRequest) { r.Destination.ParentID = 0 }, func(r *windowsRenameRequest) { r.Destination.Name = "a:b" }} {
		r := rename
		change(&r)
		if r.Check() == nil {
			t.Fatal(r)
		}
	}
	bad := uint32(dosDirectory)
	if (windowsAttrChange{DOSAttributes: &bad}).Check() == nil {
		t.Fatal("derived DOS attribute accepted")
	}
}

func TestPlatformProjectionPreservesKnownTimesAndIdentity(t *testing.T) {
	stamp := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	for _, state := range []storage.LocationState{storage.LocationRoot, storage.LocationDetached} {
		location := storage.EntryLocation{State: state, RootNodeID: 1, NodeID: 1}
		if state == storage.LocationDetached {
			location.NodeID = 2
		}
		observation := storage.FileObservation{Attr: storage.Attr{ID: location.NodeID, Kind: storage.NodeDirectory, CreationTime: &stamp, ChangeTime: &stamp}, Location: &location, Removal: storage.RemovalStatus{State: storage.EntryDraining}}
		attr, err := projectWindowsAttr(observation, windowsMetadata{Attributes: dosNormal})
		if err != nil || !attr.CreationTime.Equal(stamp) || !attr.ChangeTime.Equal(stamp) || attr.DOSAttributes != dosDirectory || !attr.DeletePending || attr.Attr.CreationTime != nil || attr.Metadata != nil {
			t.Fatalf("attr=%+v err=%v", attr, err)
		}
		if err = attr.NameInfo.Check(); err != nil {
			t.Fatal(err)
		}
	}
	attr, err := projectWindowsAttr(storage.FileObservation{Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular}}, windowsMetadata{})
	if err != nil || !attr.CreationTime.IsZero() || !attr.ChangeTime.IsZero() || attr.NameInfo.State != 0 {
		t.Fatal(attr, err)
	}
	for _, observation := range []storage.FileObservation{{Attr: storage.Attr{ID: 1, Kind: 99}}, {Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular}, Location: &storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 1, NodeID: 1}}} {
		if _, err := projectWindowsAttr(observation, windowsMetadata{}); !errors.Is(err, syscall.EIO) {
			t.Fatal(observation, err)
		}
	}
	payload, err := encodeWindowsMetadata(windowsMetadata{DirectorySymlink: true})
	if err != nil {
		t.Fatal(err)
	}
	metadata := storage.Metadata{{Key: windowsMetadataKey, Version: windowsMetadataVersion, Data: payload}}
	if _, err := projectWindowsAttr(storage.FileObservation{Attr: storage.Attr{ID: 1, Kind: storage.NodeRegular, Metadata: metadata}}, windowsMetadata{}); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	attr, err = projectWindowsAttr(storage.FileObservation{Attr: storage.Attr{ID: 1, Kind: storage.NodeSymlink, Metadata: metadata}}, windowsMetadata{})
	if err != nil || attr.DOSAttributes&dosDirectory == 0 {
		t.Fatal(attr, err)
	}
}

func TestPlatformNameAndFailureClassification(t *testing.T) {
	for _, name := range []windowsNameInfo{{}, {State: windowsNameRoot, Path: "file"}, {State: windowsNameDetached, Path: "file"}, {State: windowsNameLinked}, {State: windowsNameLinked, Path: "bad:name"}} {
		if !errors.Is(name.Check(), syscall.EIO) {
			t.Fatal(name)
		}
	}
	if err := (windowsNameInfo{State: windowsNameLinked, Path: "dir/file"}).Check(); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		conflict storage.FileConflictKind
		failure  windowsFailure
	}{{storage.ConflictClaim, windowsSharingViolation}, {storage.ConflictRange, windowsLockConflict}, {storage.ConflictDraining, windowsDeletePending}} {
		err := &storage.FileError{Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: tt.conflict}}
		if windowsFailureOf(err) != tt.failure {
			t.Fatal(tt)
		}
	}
	first := &windowsError{Failure: windowsLockConflict, Err: syscall.EAGAIN}
	if first.Error() == "" || !errors.Is(first, syscall.EAGAIN) || windowsFailureOf(errors.Join(first, first)) != windowsLockConflict {
		t.Fatal(first)
	}
	if windowsFailureOf(errors.Join(first, &windowsError{Failure: windowsSharingViolation, Err: syscall.EACCES})) != "" {
		t.Fatal("ambiguous failure claimed one status")
	}
	if windowsFailureOf(errors.New("backend failed")) != "" {
		t.Fatal("invented platform failure")
	}
}
