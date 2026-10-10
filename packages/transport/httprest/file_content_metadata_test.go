package httprest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/storage"
)

func httpContentEffect() storage.ContentMetadataEffect {
	return storage.ContentMetadataEffect{Namespace: "content.test", PayloadBytes: 2, PresentPrefix: []byte{'T'}, AbsentPayload: []byte{'T', 0}, ClearMask: []byte{0, 2}, SetMask: []byte{0, 1}}
}
func httpContentOpen(t *testing.T, client *Storage, backend storage.Storage) (*remoteFileSession, *remoteFile) {
	t.Helper()
	if err := backend.Write(t.Context(), "content", []byte("old")); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	sessionValue, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	session := sessionValue.(*remoteFileSession)
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	options := storage.OpenAtOptions{Read: true, Write: true, Target: storage.ChildCondition{State: storage.Any}, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.ReadData | storage.WriteData}, Action: httpFileAction(t, session), ContentMetadataEffects: []storage.ContentMetadataEffect{httpContentEffect()}}
	opened, err := session.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("content")}}, options)
	if err != nil {
		t.Fatal(err)
	}
	return session, opened.File.(*remoteFile)
}
func TestHTTPContentMetadataByteOnlyReference(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	session, file := httpContentOpen(t, client, backend)
	if err := session.CheckOpenContentMetadata(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("public Stat=%v", err)
	}
	absent, err := file.ObserveContentMetadata(t.Context(), 0)
	if err != nil || absent.NodeID != file.node || absent.Value != nil {
		t.Fatalf("absence=%+v err=%v", absent, err)
	}
	action := httpFileAction(t, session)
	command := storage.FileMutation{Action: action, Kind: storage.MutateWriteAt, Data: []byte("new"), ExpectedMetadata: map[string][]byte{"content.test": {}}, ContentEffects: []uint16{0}}
	if _, err := file.MutateFile(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	observed, err := file.ObserveContentMetadata(t.Context(), 0)
	if err != nil || observed.Value == nil || !bytes.Equal(observed.Value.Data, []byte{'T', 1}) {
		t.Fatalf("observed=%+v err=%v", observed, err)
	}
	if _, err := file.ObserveContentMetadata(t.Context(), 1); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid effect=%v", err)
	}
	command.Action = httpFileAction(t, session)
	command.ExpectedMetadata = nil
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("missing CAS=%v", err)
	}
	command.ExpectedMetadata = map[string][]byte{"content.test": {}}
	command.Action = httpFileAction(t, session)
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, storage.ErrConditionConflict) {
		t.Fatalf("stale CAS=%v", err)
	}
	command.ExpectedMetadata = map[string][]byte{"content.test": observed.Value.Version}
	command.Action = httpFileAction(t, session)
	command.Data = nil
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("empty effect mutation=%v", err)
	}
	handler.files.mu.Lock()
	served := handler.files.sessions[session.id]
	handler.files.mu.Unlock()
	served.mu.Lock()
	sealed := storage.CloneContentMetadataEffects(served.files[file.id].contentEffects)
	served.mu.Unlock()
	file.contentEffects[0].SetMask[1] = 255
	if !reflect.DeepEqual(sealed, []storage.ContentMetadataEffect{httpContentEffect()}) {
		t.Fatalf("server descriptor changed=%+v", sealed)
	}
}

type changingContentPolicy struct {
	mu     sync.Mutex
	deny   bool
	mutate bool
	seen   []authz.AccessRequest
}

func (p *changingContentPolicy) Authorize(_ context.Context, request authz.AccessRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seen = append(p.seen, request.Clone())
	if p.deny && request.Operation == storage.OpFileSetMetadata && len(request.ContentMetadataEffects) != 0 {
		return authz.ErrDenied
	}
	if p.mutate && len(request.ContentMetadataEffects) != 0 {
		request.ContentMetadataEffects[0].SetMask[1] = 255
	}
	if p.mutate && len(request.Open.ContentMetadataEffects) != 0 {
		request.Open.ContentMetadataEffects[0].SetMask[1] = 255
	}
	return nil
}
func TestHTTPContentEffectCurrentPolicyAndSealedDescriptor(t *testing.T) {
	client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
	policy := &changingContentPolicy{mutate: true}
	handler.authorizer = policy
	session, file := httpContentOpen(t, client, backend)
	command := storage.FileMutation{Action: httpFileAction(t, session), Kind: storage.MutateWriteAt, Data: []byte("new"), ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"content.test": {}}}
	calls := loseFileResponses(t, client, storage.OpFileMutate, 1)
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EIO) {
		t.Fatalf("lost result=%v", err)
	}
	policy.mu.Lock()
	policy.deny = true
	policy.mu.Unlock()
	if _, err := file.MutateFile(t.Context(), command); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("revoked replay=%v", err)
	}
	session.mu.Lock()
	pending := len(session.explicitWrites)
	session.mu.Unlock()
	if pending != 1 {
		t.Fatalf("revoked replay discarded pending=%d", pending)
	}
	policy.mu.Lock()
	policy.deny = false
	policy.mu.Unlock()
	if _, err := file.MutateFile(t.Context(), command); err != nil {
		t.Fatal(err)
	}
	observed, err := file.ObserveContentMetadata(t.Context(), 0)
	if err != nil || !bytes.Equal(observed.Value.Data, []byte{'T', 1}) || calls.Load() != 3 {
		t.Fatalf("sealed replay observed=%+v err=%v calls=%d", observed, err, calls.Load())
	}
}
func TestHTTPContentMetadataStrictWire(t *testing.T) {
	values := contentMetadataEffectsOf([]storage.ContentMetadataEffect{httpContentEffect()})
	encoded, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	var decoded []contentMetadataEffect
	if err := decodeFileJSON(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, damage := range [][]byte{bytes.Replace(encoded, []byte(`"setMask":"AAE="`), []byte(`"setMask":"AAF="`), 1), bytes.Replace(encoded, []byte(`"namespace":"content.test"`), []byte(`"namespace":"content.test","extra":1`), 1)} {
		if err := decodeFileJSON(damage, &decoded); err == nil {
			t.Fatalf("damaged descriptor accepted %s", damage)
		}
	}
	observation := contentMetadataObservationOf(storage.ContentMetadataObservation{NodeID: 1})
	body, _ := json.Marshal(observation)
	var result contentMetadataObservation
	if err := decodeFileJSON(body, &result); err != nil || result.Value != nil {
		t.Fatalf("absence wire=%s err=%v", body, err)
	}

}

type effectsOnlyPolicy struct{}

func (effectsOnlyPolicy) Authorize(_ context.Context, request authz.AccessRequest) error {
	if request.Operation == storage.OpFileSetMetadata && len(request.ContentMetadataEffects) == 0 {
		return authz.ErrDenied
	}
	return nil
}
func TestHTTPContentEnrollmentDoesNotGrantArbitraryInitialMetadata(t *testing.T) {
	for _, existing := range []storage.ExistingEffect{storage.Keep, storage.ResetContent, storage.ReplaceNode} {
		t.Run(string(rune('0'+existing)), func(t *testing.T) {
			client, handler, backend := retainedHTTPFixture(t, DefaultFileLimits())
			if err := backend.Write(t.Context(), "original", []byte("old")); err != nil {
				t.Fatal(err)
			}
			before, err := backend.Stat(t.Context(), "original")
			if err != nil {
				t.Fatal(err)
			}
			root, err := backend.Stat(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			sessionValue, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			session := sessionValue.(*remoteFileSession)
			t.Cleanup(func() {
				if err := session.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			handler.authorizer = effectsOnlyPolicy{}
			fields := storage.InitialFields{Metadata: map[string][]byte{"arbitrary.secret": []byte("bad")}}
			options := storage.OpenAtOptions{Write: true, Create: true, Target: storage.ChildCondition{State: storage.Any}, Existing: existing, Use: storage.UseClaim{Uses: storage.WriteData}, Action: httpFileAction(t, session), ContentMetadataEffects: []storage.ContentMetadataEffect{httpContentEffect()}}
			name := "original"
			switch existing {
			case storage.Keep:
				name = "new"
				options.Initial.OnCreate = fields
			case storage.ResetContent:
				options.Initial.OnReset = fields
			case storage.ReplaceNode:
				options.Use.Uses |= storage.DeleteName
				options.Initial.OnReplace = fields
			}
			_, err = session.OpenAt(t.Context(), storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(name)}}, options)
			if !errors.Is(err, syscall.EACCES) {
				t.Fatalf("arbitrary initial metadata accepted=%v", err)
			}
			after, err := backend.Stat(t.Context(), "original")
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("original changed before=%+v after=%+v err=%v", before, after, err)
			}
			if name == "new" {
				if _, err := backend.Stat(t.Context(), "new"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("unauthorized creation=%v", err)
				}
			}
		})
	}
}
