//go:build linux

package smb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func seedIOWindowsMetadata(t *testing.T, fixture realSMBFixture, name string, attributes uint32) storage.Attr {
	t.Helper()
	attr, err := fixture.volume.Stat(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	data, err := encodeWindowsMetadata(windowsMetadata{Attributes: attributes})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.raw.(storage.MetadataAccess).SetMetadata(t.Context(), attr.ID, windowsMetadataKey, nil, data); err != nil {
		t.Fatal(err)
	}
	attr, err = fixture.volume.Stat(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	return attr
}

func requireIOContents(t *testing.T, fixture realSMBFixture, name, want string) storage.Attr {
	t.Helper()
	got, err := fixture.volume.Read(t.Context(), name)
	if err != nil || string(got) != want {
		t.Fatalf("contents=%q want=%q err=%v", got, want, err)
	}
	attr, err := fixture.volume.Stat(t.Context(), name)
	if err != nil || attr.Size != int64(len(want)) {
		t.Fatalf("captured length=%d want=%d err=%v", attr.Size, len(want), err)
	}
	return attr
}

func requireIOWrite(t *testing.T, fixture realSMBFixture, id [16]byte, offset uint64, data string) {
	t.Helper()
	ctx, finish := ioResponseContextForTest(t.Context())
	body, status := fixture.connection.writeFile(ctx, fixture.session, fixture.tree, writeRequestForTest(id, offset, []byte(data)))
	finish()
	if status != statusOK || len(body) != 16 || binary.LittleEndian.Uint16(body) != 17 || binary.LittleEndian.Uint32(body[4:]) != uint32(len(data)) {
		t.Fatalf("WRITE status=%#x body=%x", status, body)
	}
}

func TestFileIORealWriteOnlyAndAppendOnlyMetadataRights(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			for _, test := range []struct {
				name   string
				access uint32
			}{{"write-only", accessWriteData}, {"append-only", accessAppend}} {
				access := test.access
				t.Run(test.name, func(t *testing.T) {
					fixture := factory(t)
					fixture.tree.sessionID = fixture.session.id
					if err := fixture.volume.Write(t.Context(), "file", []byte("abcd")); err != nil {
						t.Fatal(err)
					}
					seed := seedIOWindowsMetadata(t, fixture, "file", dosHidden|dosSystem)
					_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("file", 1, access))
					if status != statusOK {
						t.Fatalf("WRITE-only CREATE status=%#x", status)
					}
					handle := fixture.tree.findFileHandle(id)
					if _, err := handle.file.Stat(t.Context()); !errors.Is(err, syscall.EBADF) {
						t.Fatalf("WRITE-only Stat gained metadata rights: %v", err)
					}
					now := time.Now()
					if _, err := handle.file.SetAttr(t.Context(), storage.AttrChange{ModTime: &now}); !errors.Is(err, syscall.EBADF) {
						t.Fatalf("WRITE-only SetAttr gained metadata rights: %v", err)
					}
					if _, err := handle.file.(storage.ReferenceMetadataAccess).SetMetadata(t.Context(), windowsMetadataKey, nil, []byte("unsealed")); !errors.Is(err, syscall.EBADF) {
						t.Fatalf("WRITE-only SetMetadata gained metadata rights: %v", err)
					}
					requireIOWrite(t, fixture, id, 1, "XY")
					want := "aXYd"
					if access == accessAppend {
						want = "abcdXY"
					}
					attr := requireIOContents(t, fixture, "file", want)
					flags, err := decodeWindowsMetadata(attr.Metadata)
					if err != nil || flags.Attributes != dosHidden|dosSystem|dosArchive || attr.ID != seed.ID {
						t.Fatalf("WRITE effect changed metadata/identity: %+v id=%d want=%d err=%v", flags, attr.ID, seed.ID, err)
					}
					if bytes.Equal(attr.Metadata[windowsMetadataKey].Version, seed.Metadata[windowsMetadataKey].Version) {
						t.Fatal("WRITE did not atomically advance ARCHIVE metadata")
					}
					if _, status := fixture.connection.readFile(t.Context(), fixture.session, fixture.tree, readRequestForTest(id, 0, 4, 0)); status != statusDenied {
						t.Fatalf("WRITE-only READ status=%#x", status)
					}
					flushCtx, finalizeFlush := ioResponseContextForTest(t.Context())
					_, status = fixture.connection.flushFile(flushCtx, fixture.session, fixture.tree, flushRequestForTest(id))
					finalizeFlush()
					if status != statusOK {
						t.Fatalf("WRITE-only FLUSH status=%#x", status)
					}
					closeFixtureHandle(t, fixture, id)
				})
			}
		})
	}
}

func TestFileIORealRangeGrowthAtomicAppendAndEmptyWrite(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			fixture.tree.sessionID = fixture.session.id
			if err := fixture.volume.Write(t.Context(), "file", []byte("abcd")); err != nil {
				t.Fatal(err)
			}
			seedIOWindowsMetadata(t, fixture, "file", dosNormal)
			_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("file", 1, accessReadData|accessWriteData))
			if status != statusOK {
				t.Fatalf("OPEN status=%#x", status)
			}
			requireIOWrite(t, fixture, id, 1, "XY")
			requireIOContents(t, fixture, "file", "aXYd")
			requireIOWrite(t, fixture, id, 6, "!")
			requireIOContents(t, fixture, "file", "aXYd\x00\x00!")
			requireIOWrite(t, fixture, id, math.MaxUint64, "tail")
			attr := requireIOContents(t, fixture, "file", "aXYd\x00\x00!tail")
			flags, err := decodeWindowsMetadata(attr.Metadata)
			if err != nil || flags.Attributes != dosArchive {
				t.Fatalf("NORMAL was not replaced by ARCHIVE: %+v %v", flags, err)
			}
			for _, offset := range []uint64{1000, math.MaxUint64} {
				before, err := fixture.volume.Stat(t.Context(), "file")
				if err != nil {
					t.Fatal(err)
				}
				requireIOWrite(t, fixture, id, offset, "")
				after, err := fixture.volume.Stat(t.Context(), "file")
				if err != nil || before.ID != after.ID || before.Size != after.Size || !before.ModTime.Equal(after.ModTime) || !reflect.DeepEqual(before.ChangeTime, after.ChangeTime) || !reflect.DeepEqual(before.Metadata, after.Metadata) {
					t.Fatalf("empty WRITE changed file: before=%+v after=%+v err=%v", before, after, err)
				}
			}
			body, status := fixture.connection.readFile(t.Context(), fixture.session, fixture.tree, readRequestForTest(id, 0, 64, 11))
			if status != statusOK || string(body[16:]) != "aXYd\x00\x00!tail" {
				t.Fatalf("READ latest WRITE status=%#x body=%x", status, body)
			}
			if _, status := fixture.connection.readFile(t.Context(), fixture.session, fixture.tree, readRequestForTest(id, 11, 64, 0)); status != statusEndOfFile {
				t.Fatalf("captured EOF status=%#x", status)
			}
			closeFixtureHandle(t, fixture, id)
		})
	}
}

func TestFileIORealWriteRechecksReadonlyAndMetadataPolicy(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			fixture.tree.sessionID = fixture.session.id
			if err := fixture.volume.Write(t.Context(), "file", []byte("body")); err != nil {
				t.Fatal(err)
			}
			before := seedIOWindowsMetadata(t, fixture, "file", dosHidden)
			_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("file", 1, accessWriteData))
			if status != statusOK {
				t.Fatalf("OPEN status=%#x", status)
			}
			fixture.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
				if request.Operation == storage.OpFileSetMetadata {
					return authz.ErrDenied
				}
				return nil
			})
			if _, status := fixture.connection.writeFile(t.Context(), fixture.session, fixture.tree, writeRequestForTest(id, 0, []byte("bad!"))); status != statusDenied {
				t.Fatalf("metadata-policy WRITE status=%#x", status)
			}
			after := requireIOContents(t, fixture, "file", "body")
			if !reflect.DeepEqual(before.Metadata, after.Metadata) || !before.ModTime.Equal(after.ModTime) {
				t.Fatal("policy denial changed content metadata/time")
			}
			requireIOWrite(t, fixture, id, 99, "")
			fixture.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
			current := after.Metadata[windowsMetadataKey]
			data, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosHidden | dosReadOnly})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.raw.(storage.MetadataAccess).SetMetadata(t.Context(), before.ID, windowsMetadataKey, current.Version, data); err != nil {
				t.Fatal(err)
			}
			if _, status := fixture.connection.writeFile(t.Context(), fixture.session, fixture.tree, writeRequestForTest(id, 0, []byte("bad!"))); status != statusDenied {
				t.Fatalf("late READONLY WRITE status=%#x", status)
			}
			after = requireIOContents(t, fixture, "file", "body")
			flags, err := decodeWindowsMetadata(after.Metadata)
			if err != nil || flags.Attributes != dosHidden|dosReadOnly {
				t.Fatalf("denied WRITE changed readonly: %+v %v", flags, err)
			}
			closeFixtureHandle(t, fixture, id)
		})
	}
}

type ioDropMutations struct {
	next     http.RoundTripper
	mu       sync.Mutex
	drop     bool
	actions  []storage.FileActionID
	payloads [][]byte
}

func (d *ioDropMutations) RoundTrip(request *http.Request) (*http.Response, error) {
	var command struct {
		Op       storage.Operation `json:"op"`
		Mutation *struct {
			Action storage.FileActionID `json:"action"`
			Data   []byte               `json:"data"`
		} `json:"mutation"`
	}
	if request.Body != nil {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		request.Body = io.NopCloser(bytes.NewReader(body))
		if err := json.Unmarshal(body, &command); err != nil {
			return nil, err
		}
	}
	response, err := d.next.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	drop := d.drop && command.Op == storage.OpFileMutate && command.Mutation != nil
	if drop {
		d.actions = append(d.actions, command.Mutation.Action)
		d.payloads = append(d.payloads, bytes.Clone(command.Mutation.Data))
	}
	d.mu.Unlock()
	if !drop {
		return response, nil
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil {
		return nil, errors.Join(readErr, closeErr)
	}
	return nil, errors.New("mutation response lost after authority dispatch")
}

func newDroppedWriteSMBFixture(t *testing.T) (realSMBFixture, *ioDropMutations) {
	t.Helper()
	fixture := newRealSMBFixture(t)
	options := httprest.DefaultHandlerOptions()
	options.Files = httprest.DefaultFileLimits()
	options.Files.Session = fixtureFileSessionOptions()
	handler, err := httprest.NewHandlerWithOptions(fixture.volume, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := httptest.NewServer(handler)
	t.Cleanup(func() {
		endpoint.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	drop := &ioDropMutations{next: endpoint.Client().Transport, drop: true}
	client, err := httprest.Dial(endpoint.URL, &http.Client{Transport: drop})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := client.NewFileSession(t.Context(), fixtureFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := raw.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	state, err := raw.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := raw.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	fixture.raw, fixture.state = raw, state
	fixture.tree.sessionID = fixture.session.id
	fixture.tree.export.share.Backend = client
	fixture.tree.authority = &authoritySession{raw: raw, identity: identity, export: fixture.tree.export, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining)}
	return fixture, drop
}

func TestFileIOHTTPUnknownWriteKeepsSiblingAndCleanupProgress(t *testing.T) {
	fixture, drop := newDroppedWriteSMBFixture(t)
	for name, body := range map[string]string{"unknown": "before", "sibling": "sibling body"} {
		if err := fixture.volume.Write(t.Context(), name, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	_, status, unknownID := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("unknown", 1, accessWriteData))
	if status != statusOK {
		t.Fatalf("unknown OPEN=%#x", status)
	}
	_, status, siblingID := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("sibling", 1, accessReadData))
	if status != statusOK {
		t.Fatalf("sibling OPEN=%#x", status)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	writeCtx, finalizeWrite := ioResponseContextForTest(ctx)
	_, status = fixture.connection.writeFile(writeCtx, fixture.session, fixture.tree, writeRequestForTest(unknownID, 0, []byte("after!")))
	finalizeWrite()
	if status != statusIO {
		t.Fatalf("lost WRITE response status=%#x", status)
	}
	observed := fixture.server.Status()
	if observed.UnknownWrites != 1 || observed.RetainedWriteBytes < 6 || len(observed.WriteFailures) != 1 || observed.WriteFailures[0].PayloadReleased {
		t.Fatalf("unknown WRITE not retained/observable: %+v", observed)
	}
	drop.mu.Lock()
	actions := append([]storage.FileActionID(nil), drop.actions...)
	payloads := append([][]byte(nil), drop.payloads...)
	drop.mu.Unlock()
	if len(actions) < 2 {
		t.Fatalf("Completed receipt was not followed by typed replay: actions=%v", actions)
	}
	for index := range actions {
		if actions[index] != actions[0] || string(payloads[index]) != "after!" {
			t.Fatalf("WRITE recovery changed action/payload: actions=%v payloads=%q", actions, payloads)
		}
	}
	body, status := fixture.connection.readFile(ctx, fixture.session, fixture.tree, readRequestForTest(siblingID, 0, 32, 0))
	if status != statusOK || string(body[16:]) != "sibling body" {
		t.Fatalf("unknown WRITE blocked sibling READ: status=%#x body=%x", status, body)
	}
	if _, err := fixture.raw.Renew(ctx); err != nil {
		t.Fatalf("unknown WRITE blocked session renewal: %v", err)
	}
	closeFixtureHandle(t, fixture, siblingID)
	if fixture.tree.authority.isClosed() {
		t.Fatal("sibling CLOSE retired authority holding unknown WRITE")
	}
	unknown := fixture.tree.findFileHandle(unknownID)
	if err := fixture.tree.closeFileHandle(ctx, unknown); err == nil {
		t.Fatal("unknown WRITE CLOSE hid original failure")
	}
	if fixture.tree.findFileHandle(unknownID) != nil {
		t.Fatal("exact released reference retained handle slot")
	}
	observed = fixture.server.Status()
	if observed.RetainedWriteBytes != 0 || len(observed.WriteFailures) != 1 {
		t.Fatalf("cleanup did not release payload or retain failure: %+v", observed)
	}
	fact := observed.WriteFailures[0]
	if fact.Execution != WriteExecutionUnknown || !fact.ReferenceReleased || !fact.ChainSettled || !fact.PayloadReleased || !fact.Terminal {
		t.Fatalf("close fabricated WRITE result or lacked terminal proof: %+v", fact)
	}
	if err := fixture.server.AcknowledgeWriteFailures([]WriteOwnerID{fact.Owner}); err != nil {
		t.Fatalf("terminal fact acknowledgment=%v", err)
	}
	if current := fixture.server.Status(); current.UnknownWrites != 0 || len(current.WriteFailures) != 0 {
		t.Fatalf("acknowledged failure remained charged: %+v", current)
	}
	drop.mu.Lock()
	calls := len(drop.actions)
	drop.drop = false
	drop.mu.Unlock()
	if _, status := fixture.connection.writeFile(ctx, fixture.session, fixture.tree, writeRequestForTest(unknownID, 0, []byte("wrong!"))); status != statusInvalidHandle {
		t.Fatalf("retired FileId WRITE=%#x", status)
	}
	drop.mu.Lock()
	finalCalls := len(drop.actions)
	drop.mu.Unlock()
	if finalCalls != calls {
		t.Fatal("retired FileId replayed a WRITE")
	}
	requireIOContents(t, fixture, "unknown", "after!")
}

func TestFileIOEffectAuthorizationUsesSelectedDescriptorsAndOwnsCopies(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			fixture.tree.sessionID = fixture.session.id
			if err := fixture.volume.Write(t.Context(), "file", []byte("body")); err != nil {
				t.Fatal(err)
			}
			before := seedIOWindowsMetadata(t, fixture, "file", dosHidden)
			var observeChecks, metadataChecks, writeChecks int
			fixture.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
				switch request.Operation {
				case storage.OpFileOpenAt:
					if !reflect.DeepEqual(request.Open.ContentMetadataEffects, windowsContentEffects()) {
						return authz.ErrDenied
					}
					request.Open.ContentMetadataEffects[0].SetMask[4] = byte(dosReadOnly)
				case storage.OpFileObserveContentMetadata, storage.OpFileSetMetadata, storage.OpFileWrite:
					if !reflect.DeepEqual(request.ContentMetadataEffects, windowsContentEffects()) {
						return authz.ErrDenied
					}
					switch request.Operation {
					case storage.OpFileObserveContentMetadata:
						observeChecks++
					case storage.OpFileSetMetadata:
						metadataChecks++
					case storage.OpFileWrite:
						writeChecks++
					}
					request.ContentMetadataEffects[0].Namespace = "policy.alias"
					request.ContentMetadataEffects[0].SetMask[4] = byte(dosReadOnly)
					request.ContentMetadataEffects[0].PresentPrefix[0] = 'X'
				}
				return nil
			})
			_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("file", 1, accessWriteData))
			if status != statusOK {
				t.Fatalf("selected descriptor OPEN=%#x", status)
			}
			handle := fixture.tree.findFileHandle(id)
			if !reflect.DeepEqual(handle.contentEffects, windowsContentEffects()) {
				t.Fatal("authorization callback aliased enrolled handle descriptor")
			}
			requireIOWrite(t, fixture, id, 0, "good")
			current := requireIOContents(t, fixture, "file", "good")
			flags, err := decodeWindowsMetadata(current.Metadata)
			if err != nil || flags.Attributes != dosHidden|dosArchive || current.ID != before.ID || observeChecks < 2 || metadataChecks < 2 || writeChecks < 2 {
				t.Fatalf("sealed write after policy alias: flags=%+v obs=%d metadata=%d writes=%d err=%v", flags, observeChecks, metadataChecks, writeChecks, err)
			}
			for _, denied := range []storage.Operation{storage.OpFileObserveContentMetadata, storage.OpFileSetMetadata} {
				fixture.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
					if request.Operation == denied {
						if !reflect.DeepEqual(request.ContentMetadataEffects, windowsContentEffects()) {
							t.Error("selected descriptor missing from metadata authorization")
						}
						return authz.ErrDenied
					}
					return nil
				})
				if _, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("file", 1, accessWriteData)); status != statusDenied {
					t.Fatalf("denied %s allowed OPEN: %#x", denied, status)
				}
				if _, status := fixture.connection.writeFile(t.Context(), fixture.session, fixture.tree, writeRequestForTest(id, 0, []byte("bad!"))); status != statusDenied {
					t.Fatalf("denied %s allowed WRITE: %#x", denied, status)
				}
				after := requireIOContents(t, fixture, "file", "good")
				if !reflect.DeepEqual(after.Metadata, current.Metadata) || !after.ModTime.Equal(current.ModTime) {
					t.Fatalf("denied %s changed file metadata/time", denied)
				}
			}
			fixture.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil })
			closeFixtureHandle(t, fixture, id)
		})
	}
}

func newQuotaIOFixture(t *testing.T, remote bool) (realSMBFixture, *sqlite.LockingStore) {
	t.Helper()
	allowance := int64(4096)
	meta, err := sqlite.OpenLocking(t.Context(), sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "quota.db"), Volume: "files", Allowance: allowance,
		SQLite: sqlite.DefaultOptions(), Locks: locking.DefaultOptions(), Initialize: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	volume := objectstore.New(memory.New(), meta)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := volume.Write(t.Context(), "full", bytes.Repeat([]byte{'a'}, 4096)); err != nil {
		t.Fatal(err)
	}
	var backend storage.FileStorage = volume
	if remote {
		options := httprest.DefaultHandlerOptions()
		options.Files = httprest.DefaultFileLimits()
		options.Files.Session = fixtureFileSessionOptions()
		handler, err := httprest.NewHandlerWithOptions(volume, nil, options)
		if err != nil {
			t.Fatal(err)
		}
		endpoint := httptest.NewServer(handler)
		t.Cleanup(func() {
			endpoint.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := handler.Close(ctx); err != nil {
				t.Error(err)
			}
		})
		client, err := httprest.Dial(endpoint.URL, endpoint.Client())
		if err != nil {
			t.Fatal(err)
		}
		backend = client
	}
	raw, err := backend.NewFileSession(t.Context(), fixtureFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := raw.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	state, err := raw.Status(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := raw.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{config: endpointConfig(), nameComparer: testNameCompare}
	export := &Export{server: server, share: Share{Volume: "files", Backend: backend, BackendVolume: identity.Backend.Volume, RootNodeID: identity.Backend.RootNodeID}}
	authority := &authoritySession{raw: raw, identity: identity, export: export, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining)}
	s := &session{id: 91}
	tree := &tree{kind: volumeTree, sessionID: s.id, export: export, authority: authority}
	return realSMBFixture{volume: volume, raw: raw, state: state, server: server, tree: tree, session: s, connection: &connection{server: server}}, meta
}

func TestFileIOQuotaRefusalHasNoPublishedEffects(t *testing.T) {
	for _, test := range []struct {
		name   string
		remote bool
	}{{"native", false}, {"http", true}} {
		t.Run(test.name, func(t *testing.T) {
			fixture, meta := newQuotaIOFixture(t, test.remote)
			before := seedIOWindowsMetadata(t, fixture, "full", dosHidden)
			_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("full", 1, accessWriteData))
			if status != statusOK {
				t.Fatalf("quota OPEN=%#x", status)
			}
			position, err := meta.CommittedPosition(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			ctx, finalize := ioResponseContextForTest(t.Context())
			_, status = fixture.connection.writeFile(ctx, fixture.session, fixture.tree, writeRequestForTest(id, 4096, []byte{'!'}))
			finalize()
			if status != statusQuotaExceeded {
				t.Fatalf("quota WRITE status=%#x want=%#x", status, statusQuotaExceeded)
			}
			after := requireIOContents(t, fixture, "full", string(bytes.Repeat([]byte{'a'}, 4096)))
			if after.ID != before.ID || after.Size != before.Size || !reflect.DeepEqual(after.Metadata, before.Metadata) || !after.ModTime.Equal(before.ModTime) || !reflect.DeepEqual(after.ChangeTime, before.ChangeTime) {
				t.Fatalf("quota refusal published file effect: before=%+v after=%+v", before, after)
			}
			currentPosition, err := meta.CommittedPosition(t.Context())
			if err != nil || currentPosition != position {
				t.Fatalf("quota refusal advanced change log: before=%d after=%d err=%v", position, currentPosition, err)
			}
			observed := fixture.server.Status()
			if observed.UnknownWrites != 0 || observed.RetainedWriteBytes != 0 || len(observed.WriteFailures) != 1 {
				t.Fatalf("quota refusal became unknown: %+v", observed)
			}
			fact := observed.WriteFailures[0]
			if fact.Execution != WriteExecutionNotExecuted || !fact.ChainSettled || !fact.PayloadReleased || !fact.Terminal {
				t.Fatalf("quota refusal lacked nonexecution proof: %+v", fact)
			}
			receipt, err := fixture.raw.(storage.FileActions).QueryFileAction(t.Context(), fact.Action)
			if err != nil || receipt.Operation != storage.OpFileMutate || receipt.Outcome != storage.FileActionNotExecuted {
				t.Fatalf("quota refusal receipt=%+v err=%v", receipt, err)
			}
			if err := fixture.tree.closeFileHandle(t.Context(), fixture.tree.findFileHandle(id)); err == nil {
				t.Fatal("CLOSE hid reported failed WRITE")
			}
			if fixture.tree.findFileHandle(id) != nil {
				t.Fatal("quota failure retained released handle slot")
			}
			if err := fixture.server.AcknowledgeWriteFailures([]WriteOwnerID{fact.Owner}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFileIOSignedRelatedCreateWriteFlushClose(t *testing.T) {
	fixture, client := startRealFileProtocolServer(t)
	sessionID, key := authenticateProtocol(t, client)
	treeID := connectProtocolTree(t, client, key, sessionID, 3, "data")
	data := []byte("signed compound content\x00with binary bytes")
	create := createRequestForTest("compound", 2, accessReadData|accessWriteData|accessReadAttr)
	write := writeRequestForTest(wire.InvalidFileID, 0, data)
	flush := flushRequestForTest(wire.InvalidFileID)
	closeBody := make([]byte, 24)
	binary.LittleEndian.PutUint16(closeBody, 24)
	binary.LittleEndian.PutUint16(closeBody[2:], 1)
	copy(closeBody[8:], wire.InvalidFileID[:])
	headers := []wire.Header{
		{Command: wire.Create, MessageID: 4, SessionID: sessionID, TreeID: treeID, Credits: 1},
		{Command: wire.Write, MessageID: 5, Credits: 1, CreditCharge: 1},
		{Command: wire.Flush, MessageID: 6, Credits: 1},
		{Command: wire.Close, MessageID: 7, Credits: 1},
	}
	sendFrame(t, client, compoundRequest(t, key, headers, [][]byte{create.Body, write.Body, flush.Body, closeBody}, true))
	response := readFrame(t, client)
	var bodies [][]byte
	offset := 0
	for index, command := range []uint16{wire.Create, wire.Write, wire.Flush, wire.Close} {
		header, err := wire.ParseHeader(response[offset:])
		if err != nil || header.Command != command || header.Status != statusOK || header.MessageID != uint64(4+index) || (header.Flags&wire.FlagRelated != 0) != (index > 0) {
			t.Fatalf("compound command %d response=%+v err=%v", command, header, err)
		}
		end := len(response)
		if header.NextCommand != 0 {
			end = offset + int(header.NextCommand)
		}
		if end < offset+wire.HeaderSize || end > len(response) || key.Verify(response[offset:end]) != nil {
			t.Fatalf("compound response %d boundary/signature: offset=%d end=%d", command, offset, end)
		}
		bodies = append(bodies, response[offset+wire.HeaderSize:end])
		if index < 3 && header.NextCommand == 0 || index == 3 && header.NextCommand != 0 {
			t.Fatalf("compound response %d next=%d", command, header.NextCommand)
		}
		offset = end
	}
	if offset != len(response) || binary.LittleEndian.Uint32(bodies[1][4:]) != uint32(len(data)) || binary.LittleEndian.Uint16(bodies[2]) != 4 {
		t.Fatalf("compound WRITE/FLUSH response=%x", bodies)
	}
	closed := bodies[3]
	if binary.LittleEndian.Uint16(closed) != 60 || binary.LittleEndian.Uint16(closed[2:]) != 1 || binary.LittleEndian.Uint64(closed[48:]) != uint64(len(data)) || binary.LittleEndian.Uint32(closed[56:])&dosArchive == 0 {
		t.Fatalf("CLOSE postquery omitted drained WRITE result: %x", closed)
	}
	got, err := fixture.volume.Read(t.Context(), "compound")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("signed compound content=%q err=%v", got, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for {
		status := fixture.server.Status()
		if status.Handles == 0 && status.WriteOwners == 0 && status.PendingFlushes == 0 && status.RetainedWriteBytes == 0 && len(status.WriteFailures) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("completed signed frame retained I/O owners: %+v", status)
		default:
			runtime.Gosched()
		}
	}
}

func TestFileIOEnrolledEffectsDoNotAuthorizeInitialMetadata(t *testing.T) {
	for _, remote := range []bool{false, true} {
		name := "direct"
		if remote {
			name = "http"
		}
		t.Run(name, func(t *testing.T) {
			fixture, meta := newQuotaIOFixture(t, remote)
			before := seedIOWindowsMetadata(t, fixture, "full", dosHidden)
			arbitraryChecks := 0
			fixture.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
				if request.Operation == storage.OpFileSetMetadata && len(request.ContentMetadataEffects) == 0 {
					arbitraryChecks++
					return authz.ErrDenied
				}
				return nil
			})
			for _, test := range []struct {
				name        string
				disposition uint32
				attributes  uint32
			}{{"new", 2, 0}, {"full", 4, dosHidden}} {
				position, err := meta.CommittedPosition(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				request := createRequestForTest(test.name, test.disposition, accessWriteData)
				binary.LittleEndian.PutUint32(request.Body[28:], test.attributes)
				if _, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, request); status != statusDenied {
					t.Fatalf("effect-only policy authorized disposition %d metadata: %#x", test.disposition, status)
				}
				afterPosition, err := meta.CommittedPosition(t.Context())
				if err != nil || afterPosition != position {
					t.Fatalf("initial metadata denial advanced change log: before=%d after=%d err=%v", position, afterPosition, err)
				}
			}
			if arbitraryChecks != 2 {
				t.Fatalf("arbitrary initial metadata policy checks=%d", arbitraryChecks)
			}
			if _, err := fixture.volume.Stat(t.Context(), "new"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("denied initial metadata created new name: %v", err)
			}
			after := requireIOContents(t, fixture, "full", string(bytes.Repeat([]byte{'a'}, 4096)))
			if after.ID != before.ID || !reflect.DeepEqual(after.Metadata, before.Metadata) || !after.ModTime.Equal(before.ModTime) || !reflect.DeepEqual(after.ChangeTime, before.ChangeTime) {
				t.Fatal("enrolled effects authorized reset metadata/content")
			}
			if fixture.server.Status().Handles != 0 {
				t.Fatal("initial metadata denial retained handle capacity")
			}
		})
	}
}
