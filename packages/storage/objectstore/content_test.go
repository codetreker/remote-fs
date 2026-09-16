package objectstore

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

type receiptContentFile struct {
	metastore.File
	result storage.FileActionReceipt
}

func (*receiptContentFile) Reference() storage.FileReferenceID { return 1 }
func (*receiptContentFile) BeginContent(context.Context, storage.FileActionID, [32]byte, storage.FileIO) (storage.FileActionReceipt, bool, error) {
	return storage.FileActionReceipt{State: storage.FileActionPending}, true, nil
}
func (*receiptContentFile) Capture(context.Context, storage.FileIO) (metastore.FileState, error) {
	return metastore.FileState{Node: metastore.Node{ID: 1, Kind: storage.NodeRegular}, Revision: 1}, nil
}
func (*receiptContentFile) Reserve(context.Context, int64) (metastore.Key, error) {
	return "stage", nil
}
func (f *receiptContentFile) CommitContent(context.Context, storage.FileActionID, uint64, metastore.Object) (storage.FileActionReceipt, error) {
	return f.result, syscall.EIO
}

type receiptObjects struct {
	BoundedObjects
	puts atomic.Int32
}

func (o *receiptObjects) Put(context.Context, string, []byte) ([]byte, error) {
	o.puts.Add(1)
	return []byte("digest"), nil
}

type receiptAuthority struct {
	*sessionFixtureAuthority
	abandoned atomic.Int32
}

func (a *receiptAuthority) Abandon(context.Context, metastore.Key) error {
	a.abandoned.Add(1)
	return nil
}

func TestContentReceiptPreservesObjectsWhosePublicationMayHaveCommitted(t *testing.T) {
	for _, test := range []struct {
		name   string
		result storage.FileActionReceipt
	}{
		{"unknown", storage.FileActionReceipt{State: storage.FileActionUnknown}},
		{"missing receipt", storage.FileActionReceipt{}},
		{"confirmed effect with later failure", storage.FileActionReceipt{State: storage.FileActionCompleted, Effects: storage.EffectContentChanged, Errno: syscall.EIO}},
	} {
		t.Run(test.name, func(t *testing.T) {
			native := &sessionFixtureNative{file: &receiptContentFile{result: test.result}, retired: make(chan struct{})}
			volume, session := sessionFixture(t, native)
			authority := &receiptAuthority{sessionFixtureAuthority: volume.meta.(*sessionFixtureAuthority)}
			volume.meta = authority
			objects := &receiptObjects{}
			volume.objects = objects
			file, err := session.Reference(t.Context(), 1)
			if err != nil {
				t.Fatal(err)
			}
			id, err := storage.NewFileActionID(1)
			if err != nil {
				t.Fatal(err)
			}
			result, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("x")}, id)
			if !errors.Is(err, syscall.EIO) || result.State != test.result.State || result.Effects != test.result.Effects {
				t.Fatalf("write=%+v,%v", result, err)
			}
			if objects.puts.Load() != 1 || authority.abandoned.Load() != 0 {
				t.Fatalf("uploads=%d, abandoned=%d", objects.puts.Load(), authority.abandoned.Load())
			}
		})
	}
}

type failedDiscardFile struct {
	*receiptContentFile
	rejected atomic.Int32
}

func (f *failedDiscardFile) CommitContent(context.Context, storage.FileActionID, uint64, metastore.Object) (storage.FileActionReceipt, error) {
	return storage.FileActionReceipt{State: storage.FileActionPending}, syscall.EAGAIN
}
func (f *failedDiscardFile) RejectContent(_ context.Context, id storage.FileActionID, cause error) (storage.FileActionReceipt, error) {
	f.rejected.Add(1)
	return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EIO}, cause
}

type failedDiscardAuthority struct{ *receiptAuthority }

func (a *failedDiscardAuthority) Abandon(context.Context, metastore.Key) error {
	a.abandoned.Add(1)
	return syscall.EIO
}

func TestFailedDiscardFinishesTheKnownUnpublishedContentAction(t *testing.T) {
	nativeFile := &failedDiscardFile{receiptContentFile: &receiptContentFile{}}
	native := &sessionFixtureNative{file: nativeFile, retired: make(chan struct{})}
	volume, session := sessionFixture(t, native)
	authority := &failedDiscardAuthority{&receiptAuthority{sessionFixtureAuthority: volume.meta.(*sessionFixtureAuthority)}}
	volume.meta = authority
	objects := &receiptObjects{}
	volume.objects = objects
	file, err := session.Reference(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	result, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("x")}, id)
	if !errors.Is(err, syscall.EIO) || result.State != storage.FileActionNotApplied {
		t.Fatalf("failed discard left action unresolved: %+v,%v", result, err)
	}
	if nativeFile.rejected.Load() != 1 || authority.abandoned.Load() != 1 || objects.puts.Load() != 1 {
		t.Fatalf("rejections=%d, abandon attempts=%d, uploads=%d", nativeFile.rejected.Load(), authority.abandoned.Load(), objects.puts.Load())
	}
}

type contentMembership struct{ close func(context.Context) error }

func (m contentMembership) Close(ctx context.Context) error {
	if m.close != nil {
		return m.close(ctx)
	}
	return nil
}
func (*receiptContentFile) AcquireIO(context.Context) (metastore.IOMembership, error) {
	return contentMembership{}, nil
}

type releaseFailureFile struct {
	*receiptContentFile
	released atomic.Int32
}

func (f *releaseFailureFile) AcquireIO(context.Context) (metastore.IOMembership, error) {
	return contentMembership{close: func(context.Context) error { f.released.Add(1); return syscall.EIO }}, nil
}
func (*releaseFailureFile) CommitContent(_ context.Context, id storage.FileActionID, _ uint64, _ metastore.Object) (storage.FileActionReceipt, error) {
	return storage.FileActionReceipt{Action: id, State: storage.FileActionCompleted, Effects: storage.EffectContentChanged, Observation: storage.FileObservation{Attr: storage.Attr{ID: 1, Kind: storage.NodeRegular, Size: 1}}}, nil
}
func (*releaseFailureFile) Sync(context.Context) (metastore.FileState, error) {
	return metastore.FileState{Node: metastore.Node{ID: 1, Kind: storage.NodeRegular}}, nil
}

func TestContentMembershipReleaseFailurePreservesTheOperationFacts(t *testing.T) {
	for _, operation := range []string{"write", "read", "sync"} {
		t.Run(operation, func(t *testing.T) {
			nativeFile := &releaseFailureFile{receiptContentFile: &receiptContentFile{}}
			native := &sessionFixtureNative{file: nativeFile, retired: make(chan struct{})}
			volume, session := sessionFixture(t, native)
			volume.objects = &receiptObjects{}
			file, err := session.Reference(t.Context(), 1)
			if err != nil {
				t.Fatal(err)
			}
			switch operation {
			case "write":
				id, err := storage.NewFileActionID(1)
				if err != nil {
					t.Fatal(err)
				}
				receipt, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("x")}, id)
				if !errors.Is(err, syscall.EIO) || receipt.State != storage.FileActionCompleted || receipt.Effects&storage.EffectContentChanged == 0 || receipt.Observation.Attr.Size != 1 {
					t.Fatalf("committed write facts lost during membership release: %+v,%v", receipt, err)
				}
			case "read":
				result, err := file.ReadAt(t.Context(), storage.FileReadRequest{Length: 1})
				if !errors.Is(err, syscall.EIO) || result.Attr.ID != 1 {
					t.Fatalf("captured read facts lost during membership release: %+v,%v", result, err)
				}
			case "sync":
				if err := file.Sync(t.Context()); !errors.Is(err, syscall.EIO) {
					t.Fatalf("sync release=%v", err)
				}
			}
			if nativeFile.released.Load() != 1 {
				t.Fatalf("membership releases=%d", nativeFile.released.Load())
			}
		})
	}
}

type scopedAdmissionFile struct {
	*receiptContentFile
	before  bool
	failure *storage.FileError
}

func (f *scopedAdmissionFile) BeginContent(context.Context, storage.FileActionID, [32]byte, storage.FileIO) (storage.FileActionReceipt, bool, error) {
	if f.before {
		return storage.FileActionReceipt{}, false, f.failure
	}
	return storage.FileActionReceipt{State: storage.FileActionPending}, true, nil
}
func (f *scopedAdmissionFile) CommitContent(context.Context, storage.FileActionID, uint64, metastore.Object) (storage.FileActionReceipt, error) {
	return storage.FileActionReceipt{}, f.failure
}

func TestContentAdmissionProofStaysScopedToThePublicInvocation(t *testing.T) {
	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before action", false: "after action"}[before], func(t *testing.T) {
			failure := &storage.FileError{NotAdmitted: true, Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictCapacity}}
			nativeFile := &scopedAdmissionFile{receiptContentFile: &receiptContentFile{}, before: before, failure: failure}
			native := &sessionFixtureNative{file: nativeFile, retired: make(chan struct{})}
			volume, session := sessionFixture(t, native)
			volume.objects = &receiptObjects{}
			file, err := session.Reference(t.Context(), 1)
			if err != nil {
				t.Fatal(err)
			}
			action, err := storage.NewFileActionID(1)
			if err != nil {
				t.Fatal(err)
			}
			_, err = file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("x")}, action)
			if storage.IsFileCallNotAdmitted(err) != before || !errors.Is(err, failure) || storage.ErrnoOf(err) != syscall.EAGAIN {
				t.Fatalf("admission phase lost: before=%v err=%v proof=%v", before, err, storage.IsFileCallNotAdmitted(err))
			}
			var fact *storage.FileError
			if !errors.As(err, &fact) || fact.Conflict == nil || fact.Conflict.Kind != storage.ConflictCapacity {
				t.Fatalf("admission scoping lost conflict facts: %v", err)
			}
		})
	}
}
