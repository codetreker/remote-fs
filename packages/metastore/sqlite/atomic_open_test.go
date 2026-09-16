package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func atomicOptions() storage.OpenAtOptions {
	return storage.OpenAtOptions{Read: true, Write: true, Create: true, Existing: storage.Keep,
		Target: storage.ChildCondition{State: storage.Any}, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}}
}

func atomicFile(t *testing.T, s *Store, name string, options storage.OpenAtOptions) metastore.OpenResult {
	t.Helper()
	result, err := s.OpenAt(t.Context(), namespaceName(s.root, name), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := result.File.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return result
}

func commitNativeBytes(t *testing.T, file metastore.File, body []byte) metastore.FileState {
	t.Helper()
	before, err := file.Node(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key, err := file.Reserve(t.Context(), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	state, err := file.Commit(t.Context(), before.Revision, metastore.Object{Key: key, Size: int64(len(body)), Digest: digest[:], ModTime: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestAtomicOpenCapturesSelectedInitialStateAndPreservesReplacementIdentity(t *testing.T) {
	s := pendingUnlinkStore(t)
	options := atomicOptions()
	createdAt := time.Unix(100, 20)
	options.Initial.OnCreate = storage.InitialFields{Attr: storage.AttrChange{ModTime: &createdAt}, Metadata: map[string][]byte{"test.value": []byte("created"), "test.foreign": []byte("kept")}}
	created := atomicFile(t, s.Store, "file", options)
	if created.Outcome != storage.Created || created.State.ID == 0 || !created.State.ModTime.Equal(createdAt) || !bytes.Equal(created.State.Metadata["test.value"].Data, []byte("created")) {
		t.Fatalf("create = %+v", created)
	}
	options.Initial.OnCreate.Metadata["test.value"] = []byte("unused")
	kept := atomicFile(t, s.Store, "file", options)
	if kept.Outcome != storage.Opened || kept.State.ID != created.State.ID || !bytes.Equal(kept.State.Metadata["test.value"].Data, []byte("created")) {
		t.Fatalf("keep applied create fields: %+v", kept)
	}
	commitNativeBytes(t, created.File, []byte("old"))
	resetAt := time.Unix(200, 30)
	options.Existing = storage.ResetContent
	options.Initial.OnReset = storage.InitialFields{Attr: storage.AttrChange{ModTime: &resetAt}, Metadata: map[string][]byte{"test.value": []byte("reset")}}
	reset := atomicFile(t, s.Store, "file", options)
	if reset.Outcome != storage.Reset || reset.State.ID != created.State.ID || reset.State.Size != 0 || !reset.State.ModTime.Equal(resetAt) ||
		!bytes.Equal(reset.State.Metadata["test.value"].Data, []byte("reset")) || !bytes.Equal(reset.State.Metadata["test.foreign"].Data, []byte("kept")) {
		t.Fatalf("reset = %+v", reset)
	}
	if !bytes.Equal(created.State.Metadata["test.value"].Data, []byte("created")) || created.State.Size != 0 {
		t.Fatal("later changes rewrote the captured open result")
	}
	commitNativeBytes(t, reset.File, []byte("held"))
	options.Existing = storage.ReplaceNode
	options.Use.Uses |= storage.DeleteName
	options.Initial.OnReset = storage.InitialFields{}
	options.Initial.OnReplace.Metadata = map[string][]byte{"test.value": []byte("replacement")}
	replaced := atomicFile(t, s.Store, "file", options)
	if replaced.Outcome != storage.Replaced || replaced.State.ID == created.State.ID || replaced.State.Size != 0 || len(replaced.State.Metadata) != 1 {
		t.Fatalf("replace = %+v", replaced)
	}
	if old, err := reset.File.Node(t.Context()); err != nil || !old.Detached || old.ID != created.State.ID || old.Size != 4 {
		t.Fatalf("displaced retained state = %+v, %v", old, err)
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 4 {
		t.Fatalf("retained replacement usage = %d, %v", used, err)
	}
	for _, old := range []metastore.File{created.File, kept.File, reset.File} {
		if err := old.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if used, err := s.Usage(t.Context()); err != nil || used != 0 {
		t.Fatalf("last displaced close usage = %d, %v", used, err)
	}
}

func TestAtomicOpenKnownBudgetRefusalRetainsNoReusedIdentity(t *testing.T) {
	s := pendingUnlinkStore(t)
	var refusedID uint64
	ctx := storage.WithAttrResultBudget(t.Context(), func(attr storage.Attr, _ int64) error {
		refusedID = attr.ID
		return syscall.EFBIG
	})
	options := atomicOptions()
	result, err := s.OpenAt(ctx, namespaceName(s.root, "refused"), options)
	if !errors.Is(err, syscall.EFBIG) || result.File != nil || result.State.ID != 0 || result.Outcome != 0 || refusedID == 0 || len(s.files) != 0 || len(s.coordinator.pins) != 0 {
		t.Fatalf("known refusal = %+v, %v; proposed=%d files=%d pins=%d", result, err, refusedID, len(s.files), len(s.coordinator.pins))
	}
	if _, err := s.Stat(t.Context(), "refused"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused open changed namespace: %v", err)
	}
	accepted := atomicFile(t, s.Store, "accepted", options)
	if uint64(accepted.State.ID) != refusedID || s.fileDomain.files != 1 {
		t.Fatalf("rollback retained an uncommitted allocation: id=%d refused=%d references=%d", accepted.State.ID, refusedID, s.fileDomain.files)
	}
}

func TestAtomicOpenUnknownAcceptanceRetainsClaimAndPreventsIdentityReuse(t *testing.T) {
	s, err := OpenLocking(t.Context(), lockingTestConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	acceptErr := errors.New("open acceptance unavailable")
	s.witness = &retainedFailureWitness{failure: acceptErr}
	options := atomicOptions()
	options.Use.Deny = storage.WriteData
	result, err := s.OpenAt(t.Context(), namespaceName(s.root, "unknown"), options)
	if !errors.Is(err, acceptErr) || result.File == nil || result.State.ID != 0 || result.Outcome != 0 {
		t.Fatalf("unknown open = %+v, %v", result, err)
	}
	file := result.File.(*retainedFile)
	if file.active || file.closed || s.fileDomain.files != 1 || s.coordinator.pins[retainedNode{s.volume, file.id}] != 1 {
		t.Fatal("unknown open released or reactivated its native identity")
	}
	if err := s.fileDomain.coordinator.CheckUse(t.Context(), uint64(file.id), storage.UseScope{}, storage.WriteData); !errors.Is(err, storage.ErrUseConflict) {
		t.Fatalf("unknown open discarded its claim: %v", err)
	}
	var before, after int64
	if err := s.write.QueryRowContext(t.Context(), `SELECT max(id) FROM nodes`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if next, err := s.OpenAt(t.Context(), namespaceName(s.root, "later"), atomicOptions()); !errors.Is(err, acceptErr) || next.File != nil {
		t.Fatalf("poisoned authority accepted another identity: %+v, %v", next, err)
	}
	if err := s.write.QueryRowContext(t.Context(), `SELECT max(id) FROM nodes`).Scan(&after); err != nil || after != before {
		t.Fatalf("unknown authority allocated a replacement identity: %d -> %d, %v", before, after, err)
	}
	if err := result.File.Close(t.Context()); !errors.Is(err, acceptErr) {
		t.Fatalf("unknown reference close = %v", err)
	}
	if err := s.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("unknown owner released the database: %v", err)
	}
	// Public cleanup must retain the hold; the fixture then models process teardown.
	if err := s.locks.Close(); err != nil && !errors.Is(err, acceptErr) {
		t.Fatal(err)
	}
	s.locks, s.witness, s.files = nil, nil, nil
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
}

type metadataOpenObservation struct {
	attr      storage.Attr
	directory storage.DirectoryObservation
	expected  map[string][]byte
}

type metadataConditionOpenResult struct {
	reference metastore.NodeReference
	state     metastore.FileState
	outcome   storage.OpenOutcome
}

type metadataConditionObject struct {
	key    string
	state  int
	size   int64
	digest []byte
}

type metadataConditionSnapshot struct {
	named      metastore.Node
	retained   metastore.ReferenceState
	used       int64
	position   metastore.Position
	files      int
	domainRefs int
	recovered  int64
	pins       map[retainedNode]int
	intents    int
	nodes      int
	generation int64
	highWater  int64
	objects    []metadataConditionObject
}

func captureMetadataConditionSnapshot(t *testing.T, s *Store, file metastore.File) metadataConditionSnapshot {
	t.Helper()
	var snapshot metadataConditionSnapshot
	var err error
	snapshot.named, err = s.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	snapshot.retained, err = file.(metastore.ReferenceStateAccess).State(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	snapshot.used, err = s.Usage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	snapshot.position, err = s.CommittedPosition(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.read.QueryRowContext(t.Context(), `SELECT generation,node_high_water,
		(SELECT count(*) FROM nodes WHERE volume=?),
		(SELECT count(*) FROM close_intents WHERE volume=?) FROM database_state`, s.volume, s.volume).Scan(
		&snapshot.generation, &snapshot.highWater, &snapshot.nodes, &snapshot.intents); err != nil {
		t.Fatal(err)
	}
	rows, err := s.read.QueryContext(t.Context(), `SELECT key,state,size,digest FROM objects WHERE volume=? ORDER BY key`, s.volume)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var object metadataConditionObject
		if err := rows.Scan(&object.key, &object.state, &object.size, &object.digest); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		snapshot.objects = append(snapshot.objects, object)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatal(err)
	}
	if err := s.coordinator.commit.acquire(t.Context()); err != nil {
		t.Fatal(err)
	}
	snapshot.files, snapshot.domainRefs = len(s.files), s.fileDomain.files
	snapshot.recovered = s.fileDomain.recoveredReferences
	snapshot.pins = make(map[retainedNode]int, len(s.coordinator.pins))
	for node, count := range s.coordinator.pins {
		snapshot.pins[node] = count
	}
	s.coordinator.commit.release()
	return snapshot
}

func runStaleMetadataOpen(t *testing.T, initiallyPresent bool, invoke func(context.Context, *Store, storage.ChildName, metadataOpenObservation) (metadataConditionOpenResult, error)) {
	t.Helper()
	config := lockingTestConfig(t)
	config.SQLite.Advisory.MaxOwners = 2
	s, err := OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	options := atomicOptions()
	options.Initial.OnCreate.Metadata = map[string][]byte{"test.unrelated": []byte("preserved")}
	if initiallyPresent {
		options.Initial.OnCreate.Metadata["test.attributes"] = []byte("v1")
	}
	base := atomicFile(t, s.Store, "file", options)
	commitNativeBytes(t, base.File, []byte("original content"))
	first, cancel := context.WithTimeout(nodeReferenceSessionContext(t, s.Store), 30*time.Second)
	second := nodeReferenceSessionContext(t, s.Store)
	type observationResult struct {
		observation metadataOpenObservation
		err         error
	}
	type openResult struct {
		result metadataConditionOpenResult
		err    error
	}
	observed := make(chan observationResult, 1)
	resume := make(chan struct{})
	completed := make(chan openResult, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		view, err := s.ReadDirNode(first, storage.DirectoryTarget{NodeID: uint64(s.root)})
		if err != nil {
			observed <- observationResult{err: err}
			return
		}
		var capture metadataOpenObservation
		for _, entry := range view.Entries {
			if bytes.Equal(entry.RawLeaf, []byte("file")) {
				capture = metadataOpenObservation{attr: entry.Attr, directory: view.Observation,
					expected: map[string][]byte{"test.attributes": bytes.Clone(entry.Attr.Metadata["test.attributes"].Version)}}
				break
			}
		}
		if capture.attr.ID == 0 {
			observed <- observationResult{err: fmt.Errorf("observed directory lost the test file")}
			return
		}
		observed <- observationResult{observation: capture}
		select {
		case <-resume:
		case <-first.Done():
			return
		}
		result, err := invoke(first, s.Store, namespaceName(s.root, "file"), capture)
		completed <- openResult{result: result, err: err}
	}()
	defer func() { cancel(); <-finished }()
	var capture metadataOpenObservation
	select {
	case ready := <-observed:
		if ready.err != nil {
			t.Fatal(ready.err)
		}
		capture = ready.observation
	case <-first.Done():
		t.Fatal(first.Err())
	}
	value := []byte("v2")
	if !initiallyPresent {
		value = []byte{}
	}
	updated, err := s.SetMetadata(second, capture.attr.ID, "test.attributes", capture.expected["test.attributes"], value)
	if err != nil || len(updated.Version) == 0 || bytes.Equal(updated.Version, capture.expected["test.attributes"]) {
		t.Fatalf("concurrent metadata publication=%+v error=%v", updated, err)
	}
	current, err := s.ReadDirNode(second, storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil || !bytes.Equal(current.Observation.Revision, capture.directory.Revision) || len(current.Entries) != 1 || current.Entries[0].Attr.ID != capture.attr.ID || !bytes.Equal(current.Entries[0].Attr.Metadata["test.attributes"].Version, updated.Version) {
		t.Fatalf("metadata update changed name identity or directory token: %+v error=%v", current, err)
	}
	before := captureMetadataConditionSnapshot(t, s.Store, base.File)
	close(resume)
	var attempt openResult
	select {
	case attempt = <-completed:
	case <-first.Done():
		t.Fatal(first.Err())
	}
	if attempt.result.reference != nil {
		t.Cleanup(func() {
			if err := attempt.result.reference.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	if !errors.Is(attempt.err, storage.ErrConditionConflict) || !reflect.DeepEqual(attempt.result, metadataConditionOpenResult{}) {
		t.Fatalf("stale metadata admission=%+v error=%v", attempt.result, attempt.err)
	}
	after := captureMetadataConditionSnapshot(t, s.Store, base.File)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("condition refusal changed state: before=%+v after=%+v", before, after)
	}
	if err := s.fileDomain.coordinator.CheckUse(t.Context(), capture.attr.ID, storage.UseScope{}, storage.DeleteName); err != nil {
		t.Fatalf("condition refusal left an admitted deny claim=%v", err)
	}
	// One spare use slot detects an enrollment that leaked without a retained reference.
	probe, err := s.OpenFile(t.Context(), "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatalf("condition refusal consumed the spare claim slot=%v", err)
	}
	if err := probe.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := base.File.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if node, err := s.Stat(t.Context(), "file"); err != nil || uint64(node.ID) != capture.attr.ID || !bytes.Equal(node.Metadata["test.attributes"].Version, updated.Version) {
		t.Fatalf("unrelated close consumed a refused close intent: %+v error=%v", node, err)
	}
}

func TestAtomicOpenMetadataConditionsRejectConcurrentChangeBeforeEveryEffect(t *testing.T) {
	for _, observed := range []struct {
		name    string
		present bool
	}{{"version", true}, {"absence", false}} {
		for _, effect := range []struct {
			name  string
			kind  storage.ExistingEffect
			armed bool
		}{{"keep", storage.Keep, false}, {"keep with close intent", storage.Keep, true}, {"reset", storage.ResetContent, false}, {"replace", storage.ReplaceNode, false}} {
			t.Run(observed.name+"/"+effect.name, func(t *testing.T) {
				runStaleMetadataOpen(t, observed.present, func(ctx context.Context, s *Store, name storage.ChildName, capture metadataOpenObservation) (metadataConditionOpenResult, error) {
					options := atomicOptions()
					options.Target = storage.ChildCondition{State: storage.SameNode, NodeID: capture.attr.ID}
					options.ExpectedMetadata = capture.expected
					options.Guards = &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{capture.directory}}
					options.Existing = effect.kind
					options.Use.Uses |= storage.DeleteName
					options.Use.Deny = storage.DeleteName
					if effect.armed {
						options.CloseIntent = &storage.CloseIntent{Trigger: storage.OnReferenceClose, Condition: storage.UnlinkFile}
					}
					switch effect.kind {
					case storage.ResetContent:
						options.Initial.OnReset.Metadata = map[string][]byte{"test.attributes": []byte("stale reset")}
					case storage.ReplaceNode:
						options.Initial.OnReplace.Metadata = map[string][]byte{"test.attributes": []byte("stale replacement")}
					}
					result, err := s.OpenAt(ctx, name, options)
					return metadataConditionOpenResult{reference: result.File, state: result.State, outcome: result.Outcome}, err
				})
			})
		}
	}
}

func TestAtomicOpenCurrentMetadataConditionsPreserveResetAndReplacementOwnership(t *testing.T) {
	for _, effect := range []struct {
		name string
		kind storage.ExistingEffect
	}{{"reset", storage.ResetContent}, {"replace", storage.ReplaceNode}} {
		t.Run(effect.name, func(t *testing.T) {
			s := pendingUnlinkStore(t)
			options := atomicOptions()
			options.Initial.OnCreate.Metadata = map[string][]byte{"test.attributes": []byte("v1"), "test.unrelated": []byte("kept")}
			original := atomicFile(t, s.Store, "file", options)
			before := commitNativeBytes(t, original.File, []byte("retained bytes"))
			current, err := s.SetMetadata(t.Context(), uint64(before.ID), "test.attributes", before.Metadata["test.attributes"].Version, []byte("v2"))
			if err != nil {
				t.Fatal(err)
			}
			options = atomicOptions()
			options.Target = storage.ChildCondition{State: storage.SameNode, NodeID: uint64(before.ID)}
			options.ExpectedMetadata = map[string][]byte{"test.attributes": current.Version}
			options.Existing = effect.kind
			options.Use.Uses |= storage.DeleteName
			initial := storage.InitialFields{Metadata: map[string][]byte{"test.attributes": []byte("accepted")}}
			if effect.kind == storage.ResetContent {
				options.Initial.OnReset = initial
			} else {
				options.Initial.OnReplace = initial
			}
			accepted := atomicFile(t, s.Store, "file", options)
			if accepted.State.Size != 0 || !bytes.Equal(accepted.State.Metadata["test.attributes"].Data, []byte("accepted")) {
				t.Fatalf("captured conditional open=%+v", accepted)
			}
			old, err := original.File.Node(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if effect.kind == storage.ResetContent {
				if accepted.Outcome != storage.Reset || accepted.State.ID != before.ID || old.Content != "" || old.Size != 0 || old.Detached || !bytes.Equal(accepted.State.Metadata["test.unrelated"].Data, []byte("kept")) {
					t.Fatalf("conditional reset=%+v retained=%+v", accepted, old)
				}
				if used, err := s.Usage(t.Context()); err != nil || used != 0 {
					t.Fatalf("reset usage=%d error=%v", used, err)
				}
			} else {
				if accepted.Outcome != storage.Replaced || accepted.State.ID == before.ID || old.ID != before.ID || !old.Detached || old.Content != before.Content || old.Size != before.Size || len(accepted.State.Metadata) != 1 || !bytes.Equal(old.Metadata["test.attributes"].Version, current.Version) {
					t.Fatalf("conditional replacement=%+v retained=%+v", accepted, old)
				}
				if used, err := s.Usage(t.Context()); err != nil || used != before.Size {
					t.Fatalf("retained replacement usage=%d error=%v", used, err)
				}
				if err := original.File.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
				if used, err := s.Usage(t.Context()); err != nil || used != 0 {
					t.Fatalf("released replacement usage=%d error=%v", used, err)
				}
			}
		})
	}
}

func TestAtomicOpenMetadataAbsenceAndUnrelatedNamespaceConditions(t *testing.T) {
	s := pendingUnlinkStore(t)
	base := atomicFile(t, s.Store, "file", atomicOptions())
	options := atomicOptions()
	options.Target = storage.ChildCondition{State: storage.SameNode, NodeID: uint64(base.State.ID)}
	options.ExpectedMetadata = map[string][]byte{"test.attributes": nil}
	absent := atomicFile(t, s.Store, "file", options)
	if absent.Outcome != storage.Opened || absent.State.ID != base.State.ID {
		t.Fatalf("matching absence=%+v", absent)
	}
	present, err := s.SetMetadata(t.Context(), uint64(base.State.ID), "test.attributes", nil, []byte{})
	if err != nil || len(present.Version) == 0 || len(present.Data) != 0 {
		t.Fatalf("present empty metadata=%+v error=%v", present, err)
	}
	if result, err := s.OpenAt(t.Context(), namespaceName(s.root, "file"), options); !errors.Is(err, storage.ErrConditionConflict) || result.File != nil || !reflect.DeepEqual(result.State, metastore.FileState{}) || result.Outcome != 0 {
		if result.File != nil {
			result.File.Close(context.Background())
		}
		t.Fatalf("present empty payload matched namespace absence: %+v error=%v", result, err)
	}
	view, err := s.ReadDirNode(t.Context(), storage.DirectoryTarget{NodeID: uint64(s.root)})
	if err != nil {
		t.Fatal(err)
	}
	options.ExpectedMetadata = map[string][]byte{"test.attributes": bytes.Clone(present.Version)}
	options.Guards = &storage.NamespaceGuards{Directories: []storage.DirectoryObservation{view.Observation}}
	if _, err := s.SetMetadata(t.Context(), uint64(base.State.ID), "test.unrelated", nil, []byte("changed independently")); err != nil {
		t.Fatal(err)
	}
	kept := atomicFile(t, s.Store, "file", options)
	if kept.Outcome != storage.Opened || kept.State.ID != base.State.ID || !bytes.Equal(kept.State.Metadata["test.attributes"].Version, present.Version) || len(kept.State.Metadata["test.attributes"].Data) != 0 || !bytes.Equal(kept.State.Metadata["test.unrelated"].Data, []byte("changed independently")) {
		t.Fatalf("unrelated namespace changed conditioned admission=%+v", kept)
	}
}
