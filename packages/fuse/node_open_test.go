package fuse

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type targetDirectory struct {
	storage.File
	lookup      storage.EntryLookup
	lookupError error
	requests    [][]byte
}

func (f *targetDirectory) Reference() storage.FileReferenceID { return 7 }
func (f *targetDirectory) LookupAt(_ context.Context, name []byte) (storage.EntryLookup, error) {
	f.requests = append(f.requests, append([]byte(nil), name...))
	return f.lookup, f.lookupError
}
func (f *targetDirectory) ListAt(context.Context, storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
	return storage.DirectoryPage{}, syscall.EACCES
}
func (f *targetDirectory) Stat(context.Context, storage.ObservationOptions) (storage.FileObservation, error) {
	return storage.FileObservation{}, syscall.EACCES
}
func directoryTargetFixture(name string) *targetDirectory {
	return &targetDirectory{lookup: storage.EntryLookup{ParentID: 1, DirectoryRevision: 9, Name: []byte(name)}}
}

func TestEntryTargetCapturesExactIdentityAndRevision(t *testing.T) {
	name := string([]byte{'a', 0xff})
	file := directoryTargetFixture(name)
	file.lookup.Found = true
	file.lookup.EntryID = 5
	file.lookup.Attr = storage.Attr{ID: 6, Kind: storage.NodeRegular, MetadataRevision: 11}
	target, attr, err := (&volume{}).entryTarget(t.Context(), file, 1, name)
	if err != nil {
		t.Fatal(err)
	}
	if target.Parent != 7 || target.ParentID != 1 || target.DirectoryRevision != 9 || target.ExpectedEntryID != 5 || target.ExpectedNodeID != 6 || target.ExpectedMetadataRevision != 11 || string(target.Name) != name || attr.MetadataRevision != 11 || target.Witness != nil {
		t.Fatalf("target=%+v attr=%+v", target, attr)
	}
	if len(file.requests) != 1 || string(file.requests[0]) != name {
		t.Fatalf("lookup requests=%v", file.requests)
	}
}

func TestEntryTargetDistinguishesAbsenceAndInvalidObservation(t *testing.T) {
	for _, test := range []struct {
		name  string
		alter func(*targetDirectory)
		want  error
	}{
		{"absent", func(*targetDirectory) {}, nil},
		{"lookup unavailable", func(f *targetDirectory) { f.lookupError = syscall.EACCES }, syscall.EACCES},
		{"not directory", func(f *targetDirectory) { f.lookupError = syscall.ENOTDIR }, syscall.ENOTDIR},
		{"missing revision", func(f *targetDirectory) { f.lookup.DirectoryRevision = 0 }, syscall.EIO},
		{"wrong parent", func(f *targetDirectory) { f.lookup.ParentID = 2 }, syscall.EIO},
		{"wrong name", func(f *targetDirectory) { f.lookup.Name = []byte("other") }, syscall.EIO},
		{"invented absent metadata", func(f *targetDirectory) { f.lookup.Attr.ID = 6 }, syscall.EIO},
		{"missing present node", func(f *targetDirectory) { f.lookup.Found = true; f.lookup.EntryID = 5 }, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := directoryTargetFixture("missing")
			test.alter(f)
			target, _, err := (&volume{}).entryTarget(t.Context(), f, 1, "missing")
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
			if err == nil && (target.ExpectedEntryID != 0 || target.ExpectedNodeID != 0 || target.ExpectedMetadataRevision != 0) {
				t.Fatalf("absence fabricated identity: %+v", target)
			}
		})
	}
}

func TestOpenRejectsDamagedPermissionsBeforeTruncation(t *testing.T) {
	parent := directoryTargetFixture("file")
	parent.lookup.Found = true
	parent.lookup.EntryID = 2
	parent.lookup.Attr = storage.Attr{ID: 3, Kind: storage.NodeRegular, MetadataRevision: 1, Metadata: storage.Metadata{{Key: posixMetadataKey, Version: 99}}}
	n := &node{volume: &volume{}, id: &identity{node: 1}}
	_, _, err := n.openAt(t.Context(), parent, 1, "file", openOptions{Read: true, Write: true, Truncate: true}, storage.NodeRegular)
	if errnoOf(err) != syscall.EIO {
		t.Fatalf("damaged permissions reached truncate: %v", err)
	}
}

type retainedTestFile struct {
	storage.File
	closes     int
	closeError error
}

func (f *retainedTestFile) Close(ctx context.Context, action storage.FileActionID) (storage.FileActionReceipt, error) {
	if ctx.Err() != nil {
		return storage.FileActionReceipt{}, ctx.Err()
	}
	f.closes++
	if f.closeError != nil {
		return storage.FileActionReceipt{Action: action, Operation: storage.OpFileClose, State: storage.FileActionNotApplied, Errno: errnoOf(f.closeError)}, f.closeError
	}
	return storage.FileActionReceipt{Action: action, Operation: storage.OpFileClose, State: storage.FileActionCompleted, Effects: storage.EffectReferenceRetired}, nil
}

type retainedTestSession struct {
	storage.FileSession
	file           *retainedTestFile
	referenceError error
}

func (s *retainedTestSession) Reference(ctx context.Context, id storage.FileReferenceID) (storage.File, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if s.referenceError != nil {
		return nil, s.referenceError
	}
	return s.file, nil
}
func retainedVolumeFixture() (*volume, *retainedTestSession) {
	session := &retainedTestSession{file: &retainedTestFile{}}
	return &volume{files: session, flushTimeout: time.Second, status: storage.FileSessionStatus{ActionEpoch: 1}, stop: make(chan struct{}), deadline: time.Now().Add(time.Minute)}, session
}

func TestInterruptedRetainClosesExactReferenceBeforeReturning(t *testing.T) {
	for _, effects := range []storage.FileEffects{storage.EffectRetained, storage.EffectRetained | storage.EffectCreated, storage.EffectRetained | storage.EffectContentChanged} {
		v, session := retainedVolumeFixture()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		receipt := storage.FileActionReceipt{State: storage.FileActionCompleted, Reference: 7, Effects: effects, Observation: storage.FileObservation{Attr: storage.Attr{ID: 9}}}
		file, _, err := v.retainedResult(ctx, receipt, nil)
		want := syscall.EINTR
		if effects != storage.EffectRetained {
			want = syscall.EIO
		}
		if file != nil || errnoOf(err) != want || !errors.Is(err, context.Canceled) || session.file.closes != 1 {
			t.Fatalf("effects %v: file=%v error=%v closes=%d", effects, file, err, session.file.closes)
		}

	}
}

func TestRetainedResultPreservesEffectsAndFencesUnknownOwnership(t *testing.T) {
	for _, test := range []struct {
		name                              string
		receipt                           storage.FileActionReceipt
		cause, referenceError, closeError error
		want                              syscall.Errno
		closes                            int
		fenced                            bool
	}{
		{name: "known rejection", cause: syscall.EACCES, want: syscall.EACCES},
		{name: "missing success identity", want: syscall.EIO, fenced: true},
		{name: "reference unavailable", receipt: storage.FileActionReceipt{Reference: 7, Effects: storage.EffectRetained}, referenceError: syscall.ESTALE, want: syscall.EIO, fenced: true},
		{name: "partial retained rejection", receipt: storage.FileActionReceipt{Reference: 7, Effects: storage.EffectRetained}, cause: syscall.EACCES, want: syscall.EACCES, closes: 1},
		{name: "partial cleanup failure", receipt: storage.FileActionReceipt{Reference: 7, Effects: storage.EffectRetained}, cause: syscall.EACCES, closeError: syscall.EACCES, want: syscall.EACCES, closes: 1, fenced: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			v, session := retainedVolumeFixture()
			session.referenceError = test.referenceError
			session.file.closeError = test.closeError
			file, _, err := v.retainedResult(t.Context(), test.receipt, test.cause)
			if file != nil || errnoOf(err) != test.want || session.file.closes != test.closes || (v.fault != nil) != test.fenced {
				t.Fatalf("file=%v error=%v closes=%d fault=%v", file, err, session.file.closes, v.fault)
			}
		})
	}
	v, session := retainedVolumeFixture()
	file, attr, err := v.retainedResult(t.Context(), storage.FileActionReceipt{Reference: 7, Effects: storage.EffectRetained, Observation: storage.FileObservation{Attr: storage.Attr{ID: 9}}}, nil)
	if err != nil || file != session.file || attr.ID != 9 || session.file.closes != 0 {
		t.Fatalf("retained success=%v/%+v/%v", file, attr, err)
	}
}

func TestOpenFlagsDeclareOnlyRequestedContentAccess(t *testing.T) {
	for _, test := range []struct {
		flags    uint32
		uses     storage.AccessUse
		truncate bool
		want     syscall.Errno
	}{
		{syscall.O_RDONLY, storage.ReadContent, false, 0},
		{syscall.O_WRONLY, storage.WriteContent, false, 0},
		{syscall.O_RDWR | syscall.O_TRUNC, storage.ReadContent | storage.WriteContent, true, 0},
		{syscall.O_RDONLY | syscall.O_TRUNC, 0, false, syscall.EINVAL},
		{3, 0, false, syscall.EINVAL},
	} {
		options, errno := fileOpenOptions(test.flags)
		if errno != test.want {
			t.Fatalf("flags %x error=%v", test.flags, errno)
		}
		if errno == 0 && (options.claim().Uses != test.uses || options.claim().Excludes != 0 || options.Truncate != test.truncate) {
			t.Fatalf("flags %x options=%+v", test.flags, options)
		}
	}
}
