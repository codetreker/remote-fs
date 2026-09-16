package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

type protocolRoundTripper func(*http.Request) (*http.Response, error)

func (f protocolRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func protocolRawResponse(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{HeaderProtocol: []string{Version}}, Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body))}
}
func protocolTransport(t *testing.T, trip protocolRoundTripper) *Storage {
	t.Helper()
	client, err := Dial("http://example.test/prefix", &http.Client{Transport: trip})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func protocolAttr() storage.Attr {
	return storage.Attr{ID: 7, Kind: storage.NodeRegular, Size: 4, MetadataRevision: 3,
		Metadata: storage.Metadata{{Key: "application", Version: 7, Data: []byte{0, 255, 1}}}, AccessTime: time.Unix(1, 2).UTC(), ModTime: time.Unix(3, 4).UTC()}
}
func protocolStatus() storage.FileSessionStatus {
	return storage.FileSessionStatus{Epoch: "authority", Remaining: time.Minute, Revision: 1, ActionEpoch: 1, HistoryRemaining: time.Minute}
}
func protocolID(t *testing.T) storage.FileActionID {
	t.Helper()
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func protocolObservation(t *testing.T, attr storage.Attr) *fileObservation {
	t.Helper()
	value, err := fileObservationOf(storage.FileObservation{Attr: attr})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func protocolResponse(t *testing.T, status int, value any) *http.Response {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	r := protocolRawResponse(string(body))
	r.StatusCode = status
	r.Header.Set("Content-Type", contentJSON)
	return r
}
func protocolClient(t *testing.T, serve func(fileRequest) (*http.Response, error)) *Storage {
	t.Helper()
	return protocolTransport(t, func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		var req fileRequest
		if err := decodeFileJSON(body, &req); err != nil {
			t.Fatalf("client encoded malformed request: %v", err)
		}
		if err := validateFileRequest(req); err != nil {
			t.Fatalf("client encoded invalid operands: %v", err)
		}
		endpoint := "/prefix/v3/file"
		if fileControl(req.Op) {
			endpoint += "-control"
		}
		if r.Method != http.MethodPost || r.URL.Path != endpoint || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != contentJSON {
			t.Fatalf("file endpoint = %s %s", r.Method, r.URL)
		}
		return serve(req)
	})
}
func protocolSession(client *Storage) *remoteFileSession {
	return &remoteFileSession{storage: client, id: strings.Repeat("a", 64)}
}
func protocolFile(client *Storage) *remoteFile {
	return &remoteFile{session: protocolSession(client), id: 11, node: 7}
}
func protocolReceipt(req fileRequest) *fileReceipt {
	return &fileReceipt{Action: req.Action, Operation: req.Op, State: storage.FileActionCompleted, HistoryRemaining: int64(time.Minute)}
}

func TestFileClientPreservesGenericRequestsAndActionResults(t *testing.T) {
	root := storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 1, NodeID: 1, Ancestors: []storage.EntryCondition{}}
	target := storage.EntryTarget{Parent: 1, ParentID: 1, Name: []byte("file"), DirectoryRevision: 2, Witness: &root}
	existing := target
	existing.ExpectedNodeID = 7
	existing.ExpectedEntryID = 4
	existing.ExpectedMetadataRevision = 3
	entry := storage.EntryCondition{ParentID: 1, DirectoryRevision: 2, EntryID: 4, NodeID: 7, Name: []byte("file")}
	location := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 7, Ancestors: []storage.EntryCondition{entry}}
	metadata := storage.Metadata{{Key: "application", Version: 9, Data: []byte{0, 255}}}
	change := storage.AttrChange{ExpectedRevision: 3, Metadata: &metadata}
	initial := storage.NodeInitial{Kind: storage.NodeRegular, Metadata: metadata, LinkTarget: []byte{}}
	claim := storage.AccessClaim{Uses: storage.ReadContent, Excludes: storage.RemoveEntry}
	expectedSize := int64(4)
	owner := storage.RangeOwnerID(42)
	scope := storage.RangeScope{Domain: 91}
	ranges := storage.RangeReplaceRequest{Owner: owner, Scope: scope, ExpectedRevision: 5, Ranges: []storage.RangeAcquisition{{ID: 1, Start: 2, End: 8}, {ID: 2, Start: 2, End: 8}}}
	prepare := storage.PrepareRemovalRequest{ExpectedMetadataRevision: 3, Entry: entry, Witness: location, Condition: storage.RemovalFile}
	type actionCase struct {
		name  string
		op    storage.Operation
		run   func(storage.FileSession, storage.File, storage.FileActionID) (storage.FileActionReceipt, error)
		check func(fileRequest)
	}
	checkEqual := func(got, want any) {
		t.Helper()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("operands = %#v, want %#v", got, want)
		}
	}
	cases := []actionCase{
		{"retain", storage.OpFileRetain, func(s storage.FileSession, _ storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return s.Retain(t.Context(), storage.RetainRequest{NodeID: 7, ExpectedMetadataRevision: 3, Claim: claim}, id)
		}, func(r fileRequest) { checkEqual(r.Retain.Claim, claim) }},
		{"retain at", storage.OpFileRetainAt, func(s storage.FileSession, _ storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return s.RetainAt(t.Context(), storage.RetainAtRequest{Target: existing, Claim: claim}, id)
		}, func(r fileRequest) { checkEqual(r.RetainAt.Target.storage(), existing) }},
		{"create", storage.OpFileCreateAndRetainAt, func(s storage.FileSession, _ storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return s.CreateAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: target, Initial: initial, Claim: claim}, id)
		}, func(r fileRequest) {
			v, e := r.Create.storage()
			if e != nil {
				t.Fatal(e)
			}
			checkEqual(v.Initial, initial)
		}},
		{"replace", storage.OpFileReplaceAndRetainAt, func(s storage.FileSession, _ storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return s.ReplaceAndRetainAt(t.Context(), storage.CreateAndRetainRequest{Target: existing, Initial: initial, Claim: claim}, id)
		}, func(r fileRequest) { checkEqual(r.Create.Target.storage(), existing) }},
		{"reset", storage.OpFileResetAndRetainAt, func(s storage.FileSession, _ storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return s.ResetAndRetainAt(t.Context(), storage.ResetAndRetainRequest{Target: existing, ExpectedRevision: 3, Change: change, Claim: claim}, id)
		}, func(r fileRequest) {
			v, e := r.Reset.storage()
			if e != nil {
				t.Fatal(e)
			}
			checkEqual(v.Change, change)
		}},
		{"write", storage.OpFileWrite, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.WriteAt(t.Context(), storage.FileWriteRequest{ExpectedSize: &expectedSize, Offset: 2, Data: []byte{0, 255}, Owner: &owner}, id)
		}, func(r fileRequest) {
			checkEqual(r.Write.storage(), storage.FileWriteRequest{ExpectedSize: &expectedSize, Offset: 2, Data: []byte{0, 255}, Owner: &owner})
		}},
		{"truncate", storage.OpFileTruncate, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.Truncate(t.Context(), storage.FileTruncateRequest{Size: 17, Owner: &owner}, id)
		}, func(r fileRequest) {
			checkEqual(r.Truncate.storage(), storage.FileTruncateRequest{Size: 17, Owner: &owner})
		}},
		{"metadata", storage.OpFileSetAttr, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.SetAttr(t.Context(), change, id)
		}, func(r fileRequest) {
			v, e := r.Change.Storage()
			if e != nil {
				t.Fatal(e)
			}
			checkEqual(v, change)
		}},
		{"node metadata", storage.OpFileSetNodeAttr, func(s storage.FileSession, _ storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return s.SetNodeAttr(t.Context(), 7, change, id)
		}, func(r fileRequest) { checkEqual(r.Node, uint64(7)) }},
		{"kind", storage.OpFileSetKind, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.SetKind(t.Context(), storage.SetKindRequest{Owner: &owner, ExpectedRevision: 3, Kind: storage.NodeSymlink, LinkTarget: []byte("../target"), Metadata: metadata, Witness: &location}, id)
		}, func(r fileRequest) {
			value, err := r.Kind.storage()
			if err != nil {
				t.Fatal(err)
			}
			checkEqual(value, storage.SetKindRequest{Owner: &owner, ExpectedRevision: 3, Kind: storage.NodeSymlink, LinkTarget: []byte("../target"), Metadata: metadata, Witness: &location})
		}},
		{"rename", storage.OpFileRename, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			dest := target
			dest.Name = []byte("renamed")
			return f.Rename(t.Context(), storage.RenameRequest{Source: existing, Destination: dest, NewName: []byte{255, 'R'}}, id)
		}, func(r fileRequest) {
			dest := target
			dest.Name = []byte("renamed")
			value, err := r.Rename.storage()
			if err != nil {
				t.Fatal(err)
			}
			checkEqual(value, storage.RenameRequest{Source: existing, Destination: dest, NewName: []byte{255, 'R'}})
		}},
		{"claim", storage.OpFileReplaceClaim, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.ReplaceClaim(t.Context(), claim, id)
		}, func(r fileRequest) { checkEqual(*r.Claim, claim) }},
		{"prepare", storage.OpFilePrepareRemoval, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.PrepareRemoval(t.Context(), prepare, id)
		}, func(r fileRequest) { checkEqual(*r.Prepare, prepare) }},
		{"cancel prepared", storage.OpFileCancelPrepared, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.CancelPrepared(t.Context(), 19, id)
		}, func(r fileRequest) { checkEqual(*r.Intent, storage.RemovalIntentID(19)) }},
		{"drain", storage.OpFileDrainEntry, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.DrainEntry(t.Context(), storage.DrainEntryRequest(prepare), id)
		}, func(r fileRequest) { checkEqual(*r.Drain, storage.DrainEntryRequest(prepare)) }},
		{"cancel drain", storage.OpFileCancelDrain, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.CancelDrain(t.Context(), storage.CancelDrainRequest{EntryID: 4, Generation: 9}, id)
		}, func(r fileRequest) { checkEqual(*r.CancelDrain, storage.CancelDrainRequest{EntryID: 4, Generation: 9}) }},
		{"ranges", storage.OpFileReplaceRanges, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.ReplaceRanges(t.Context(), ranges, id)
		}, func(r fileRequest) { checkEqual(*r.Ranges, ranges) }},
		{"wait", storage.OpFileWaitRanges, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.WaitRanges(t.Context(), storage.RangeWaitRequest{Owner: owner, Scope: scope, ExpectedRevision: 5, Ranges: ranges.Ranges, DetectDeadlock: true}, id)
		}, func(r fileRequest) {
			checkEqual(r.Wait.Ranges, ranges.Ranges)
			if !r.Wait.DetectDeadlock {
				t.Fatal("lost deadlock request")
			}
		}},
		{"retire ranges", storage.OpFileRetireRanges, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.RetireRangeOwner(t.Context(), owner, scope, id)
		}, func(r fileRequest) { checkEqual(*r.Owner, owner); checkEqual(*r.Scope, scope) }},
		{"retire owner", storage.OpFileRetireRangeOwner, func(s storage.FileSession, _ storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return s.RetireRangeOwner(t.Context(), owner, id)
		}, func(r fileRequest) { checkEqual(*r.Owner, owner) }},
		{"close reference", storage.OpFileClose, func(_ storage.FileSession, f storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return f.Close(t.Context(), id)
		}, func(r fileRequest) { checkEqual(r.Reference, storage.FileReferenceID(11)) }},
		{"close session", storage.OpFileSessionClose, func(s storage.FileSession, _ storage.File, id storage.FileActionID) (storage.FileActionReceipt, error) {
			return s.Close(t.Context(), id)
		}, func(r fileRequest) { checkEqual(r.Reference, storage.FileReferenceID(0)) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			id := protocolID(t)
			calls := 0
			client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
				calls++
				if req.Op != test.op || req.Action != id || req.Session != strings.Repeat("a", 64) {
					t.Fatalf("request identity = %+v", req)
				}
				test.check(req)
				receipt := protocolReceipt(req)
				receipt.RangeRevision = 23
				switch req.Op {
				case storage.OpFileRetain, storage.OpFileRetainAt, storage.OpFileCreateAndRetainAt, storage.OpFileResetAndRetainAt, storage.OpFileReplaceAndRetainAt:
					receipt.Effects = storage.EffectRetained
					receipt.Reference = 11
					receipt.Observation = protocolObservation(t, protocolAttr())
				}

				return protocolResponse(t, 200, fileResponse{Receipt: receipt}), nil
			})
			got, err := test.run(protocolSession(client), protocolFile(client), id)
			if err != nil || got.Action != id || got.Operation != test.op || got.State != storage.FileActionCompleted || got.RangeRevision != 23 || calls != 1 {
				t.Fatalf("result = %+v, %v; calls=%d", got, err, calls)
			}
		})
	}
}

func TestFileClientObservationsPreserveIdentityMetadataAndRangeMultiplicity(t *testing.T) {
	attr := protocolAttr()
	observation := protocolObservation(t, attr)
	seen := map[storage.Operation]int{}
	held := storage.RangeSnapshot{Revision: 3, Available: 5, OwnerAvailable: 2, Own: []storage.RangeAcquisition{{ID: 1, Start: 0, End: 8}, {ID: 2, Start: 0, End: 8}}, Other: []storage.HeldRange{{Owner: storage.RangeOwner{Session: "other", ID: 4}, Range: storage.RangeAcquisition{ID: 3, Start: 8, End: 9, Exclusive: true}}}}
	client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
		seen[req.Op]++
		r := fileResponse{}
		switch req.Op {
		case storage.OpFileState:
			r.State = &storage.FileVolumeState{VolumeIdentity: "stable-volume", RootID: 1, MaxEventBytes: 4096}
		case storage.OpFileSessionOpen, storage.OpFileStatus, storage.OpFileRenew:
			status := protocolStatus()
			r.Status = &status
			if req.Op == storage.OpFileSessionOpen {
				r.Session = strings.Repeat("a", 64)
			}
		case storage.OpFileReference:
			r.Reference = req.Reference
			r.Node = 7
		case storage.OpFileStat, storage.OpFileStatNode:
			r.Observation = observation
		case storage.OpFileRead:
			r.Observation = observation
			r.Data = []byte("data")
			if req.Read.Owner == nil || *req.Read.Owner != 0 {
				t.Fatal("explicit zero owner lost")
			}
		case storage.OpFileRangeSnapshot:
			r.Ranges = &held
		case storage.OpFileSync:
		default:
			t.Fatalf("unexpected operation %s", req.Op)
		}
		return protocolResponse(t, 200, r), nil
	})
	state, err := client.FileState(t.Context())
	if err != nil || state.RootID != 1 || state.VolumeIdentity != "stable-volume" {
		t.Fatalf("state=%+v %v", state, err)
	}
	s, status, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil || status.ActionEpoch != 1 || status.Remaining <= 0 || status.Remaining > time.Minute {
		t.Fatalf("enrollment=%+v %v", status, err)
	}
	for _, call := range []func(context.Context) (storage.FileSessionStatus, error){s.Status, s.Renew} {
		got, err := call(t.Context())
		if err != nil || got.Revision != 1 || got.HistoryRemaining <= 0 || got.HistoryRemaining > time.Minute {
			t.Fatalf("status=%+v %v", got, err)
		}
	}
	f, err := s.Reference(t.Context(), 11)
	if err != nil || (f.Reference() != 11 || f.NodeID() != 7) {
		t.Fatalf("reference=%v %v", f, err)
	}
	got, err := f.Stat(t.Context(), storage.ObservationOptions{})
	if err != nil || !reflect.DeepEqual(got.Attr.Clone(), attr.Clone()) {
		t.Fatalf("stat=%+v %v", got, err)
	}
	got, err = s.StatNode(t.Context(), 7, storage.ObservationOptions{})
	if err != nil || !reflect.DeepEqual(got.Attr.Clone(), attr.Clone()) {
		t.Fatalf("node stat=%+v %v", got, err)
	}
	zero := storage.RangeOwnerID(0)
	read, err := f.ReadAt(t.Context(), storage.FileReadRequest{Length: 4, Owner: &zero})
	if err != nil || string(read.Data) != "data" || !reflect.DeepEqual(read.Attr.Clone(), attr.Clone()) {
		t.Fatalf("read=%+v %v", read, err)
	}
	ranges, err := f.RangeSnapshot(t.Context(), 2, storage.RangeScope{Domain: 99})
	if err != nil || !reflect.DeepEqual(ranges, held) {
		t.Fatalf("ranges=%+v %v", ranges, err)
	}
	if err := f.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 10 {
		t.Fatalf("only %d operations observed: %v", len(seen), seen)
	}
}

func TestFileClientPreservesPartialEffectsAndHistoricalFailure(t *testing.T) {
	id := protocolID(t)
	for _, op := range []storage.Operation{storage.OpFileClose, storage.OpFileQueryAction, storage.OpFileCancelAction} {
		t.Run(string(op), func(t *testing.T) {
			client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
				receipt := protocolReceipt(req)
				receipt.Operation = storage.OpFileClose
				receipt.Effects = storage.EffectReferenceRetired
				receipt.Errno = "EIO"
				receipt.Removal = storage.RemovalStatus{EntryID: 3, State: storage.EntryDraining, Generation: 4, DrainCondition: storage.RemovalFile}
				return protocolResponse(t, StatusStorageError, fileErrorResponse{Errno: "EIO", Message: "cleanup failed", Receipt: receipt}), nil
			})
			s := protocolSession(client)
			var got storage.FileActionReceipt
			var err error
			switch op {
			case storage.OpFileClose:
				got, err = protocolFile(client).Close(t.Context(), id)
			case storage.OpFileQueryAction:
				got, err = s.QueryAction(t.Context(), id)
			default:
				got, err = s.CancelAction(t.Context(), id)
			}
			if !errors.Is(err, syscall.EIO) || got.Action != id || got.Effects != storage.EffectReferenceRetired || got.Removal.Generation != 4 || got.Errno != syscall.EIO {
				t.Fatalf("receipt=%+v %v", got, err)
			}
		})
	}
}

func TestFileClientUnknownMutationRequiresExplicitQuery(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(map[bool]string{false: "denied query", true: "known receipt"}[known], func(t *testing.T) {
			id := protocolID(t)
			writes, queries := 0, 0
			client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
				if req.Action != id {
					t.Fatalf("action changed: %q", req.Action)
				}
				switch req.Op {
				case storage.OpFileWrite:
					writes++
					return nil, io.ErrUnexpectedEOF
				case storage.OpFileQueryAction:
					queries++
					if !known {
						return protocolResponse(t, StatusStorageError, fileErrorResponse{Errno: "EACCES", Message: "denied"}), nil
					}
					receipt := protocolReceipt(req)
					receipt.Operation = storage.OpFileWrite
					receipt.Effects = storage.EffectContentChanged
					return protocolResponse(t, 200, fileResponse{Receipt: receipt}), nil
				default:
					t.Fatalf("unexpected operation %s", req.Op)
				}
				return nil, nil
			})
			file := protocolFile(client)
			unknown, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("once")}, id)
			if !errors.Is(err, syscall.EIO) || unknown.State != storage.FileActionUnknown || unknown.Effects != 0 || writes != 1 || queries != 0 {
				t.Fatalf("unknown call performed hidden recovery: %+v %v writes=%d queries=%d", unknown, err, writes, queries)
			}
			observed, err := protocolSession(client).QueryAction(t.Context(), id)
			if known {
				if err != nil || observed.State != storage.FileActionCompleted || observed.Effects != storage.EffectContentChanged {
					t.Fatalf("explicit history=%+v %v", observed, err)
				}
			} else if !errors.Is(err, syscall.EACCES) || observed.Effects != 0 {
				t.Fatalf("denied query invented history: %+v %v", observed, err)
			}
			if unknown.State != storage.FileActionUnknown || writes != 1 || queries != 1 {
				t.Fatalf("query changed prior result or repeated mutation: %+v writes=%d queries=%d", unknown, writes, queries)
			}
		})
	}
}

func TestFileClientRejectsMalformedAndMismatchedReceipts(t *testing.T) {
	for name, change := range map[string]func(*fileReceipt){
		"wrong action":                  func(r *fileReceipt) { r.Action = storage.FileActionID("1:" + strings.Repeat("b", 32)) },
		"wrong operation":               func(r *fileReceipt) { r.Operation = storage.OpFileTruncate },
		"invalid state":                 func(r *fileReceipt) { r.State = 99 },
		"negative history":              func(r *fileReceipt) { r.HistoryRemaining = -1 },
		"unknown effects":               func(r *fileReceipt) { r.Effects = 1 << 31 },
		"unknown claims effects":        func(r *fileReceipt) { r.State = storage.FileActionUnknown; r.Effects = storage.EffectContentChanged },
		"unknown success without errno": func(r *fileReceipt) { r.State = storage.FileActionUnknown },
		"not applied without errno":     func(r *fileReceipt) { r.State = storage.FileActionNotApplied },
		"retained without reference":    func(r *fileReceipt) { r.Effects = storage.EffectRetained },
		"unrecognized errno":            func(r *fileReceipt) { r.Errno = "ENOTREAL" },
		"failed success":                func(r *fileReceipt) { r.Errno = "EACCES" },
		"invalid conflict":              func(r *fileReceipt) { r.Conflict = &fileConflict{Kind: 99} },
	} {
		t.Run(name, func(t *testing.T) {
			id := protocolID(t)
			client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
				r := protocolReceipt(req)
				change(r)
				return protocolResponse(t, 200, fileResponse{Receipt: r}), nil
			})
			got, err := protocolFile(client).WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("x")}, id)
			if !errors.Is(err, syscall.EIO) || got.State == storage.FileActionCompleted {
				t.Fatalf("malformed receipt accepted: %+v %v", got, err)
			}
		})
	}
}

func TestFileClientReadCancellationDiffersFromUnknownWrite(t *testing.T) {
	for _, write := range []bool{false, true} {
		t.Run(map[bool]string{false: "read", true: "write"}[write], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
				calls++
				if req.Op == storage.OpFileQueryAction {
					return protocolResponse(t, StatusStorageError, fileErrorResponse{Errno: "EACCES", Message: "denied"}), nil
				}
				cancel()
				return nil, context.Canceled
			})
			f := protocolFile(client)
			if write {
				got, err := f.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("x")}, protocolID(t))
				if !errors.Is(err, syscall.EIO) || got.State != storage.FileActionUnknown || calls != 1 {
					t.Fatalf("write=%+v %v calls=%d", got, err, calls)
				}
			} else {
				_, err := f.ReadAt(ctx, storage.FileReadRequest{Length: 1})
				if !errors.Is(err, syscall.EINTR) || !errors.Is(err, context.Canceled) || calls != 1 {
					t.Fatalf("read=%v calls=%d", err, calls)
				}
			}
		})
	}
}

func TestFileClientRejectsWrongIdentityProtocolAndIncompleteBodies(t *testing.T) {
	for name, alter := range map[string]func(*http.Response){"protocol": func(r *http.Response) { r.Header.Del(HeaderProtocol) }, "media": func(r *http.Response) { r.Header.Set("Content-Type", "text/plain") }, "short body": func(r *http.Response) { r.ContentLength++ },
		"oversized": func(r *http.Response) { r.ContentLength = DefaultMaxBodyBytes + 1 },
		"status":    func(r *http.Response) { r.StatusCode = http.StatusBadGateway }, "wrong identity": func(r *http.Response) {
			v := protocolObservation(t, protocolAttr())
			v.Attr.ID = 8
			*r = *protocolResponse(t, 200, fileResponse{Observation: v})
		}} {
		t.Run(name, func(t *testing.T) {
			client := protocolClient(t, func(fileRequest) (*http.Response, error) {
				r := protocolResponse(t, 200, fileResponse{Observation: protocolObservation(t, protocolAttr())})
				alter(r)
				return r, nil
			})
			if _, err := protocolSession(client).StatNode(t.Context(), 7, storage.ObservationOptions{}); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid response=%v", err)
			}
		})
	}
}

func TestFileClientValidatesRequestsBeforeDispatch(t *testing.T) {
	calls := 0
	client := protocolClient(t, func(fileRequest) (*http.Response, error) { calls++; return nil, io.ErrUnexpectedEOF })
	s, f := protocolSession(client), protocolFile(client)
	id := protocolID(t)
	cases := []func() error{
		func() error {
			source := storage.EntryTarget{Parent: 1, ParentID: 1, Name: []byte("source"), DirectoryRevision: 2, ExpectedEntryID: 3, ExpectedNodeID: 7}
			dest := storage.EntryTarget{Parent: 1, ParentID: 1, Name: []byte("dest"), DirectoryRevision: 2}
			_, err := f.Rename(t.Context(), storage.RenameRequest{Source: source, Destination: dest, NewName: []byte{}}, id)
			return err
		},
		func() error { _, e := s.Retain(t.Context(), storage.RetainRequest{}, id); return e },
		func() error { _, e := f.ReadAt(t.Context(), storage.FileReadRequest{Offset: -1}); return e },
		func() error { _, e := f.WriteAt(t.Context(), storage.FileWriteRequest{Offset: -1}, id); return e },
		func() error {
			negative := int64(-1)
			_, e := f.WriteAt(t.Context(), storage.FileWriteRequest{ExpectedSize: &negative, Data: []byte("x")}, id)
			return e
		},
		func() error { _, e := f.Truncate(t.Context(), storage.FileTruncateRequest{Size: -1}, id); return e },
		func() error {
			_, e := f.SetKind(t.Context(), storage.SetKindRequest{ExpectedRevision: 3, Kind: storage.NodeSymlink, LinkTarget: []byte("target"), Witness: &storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 7}}, id)
			return e
		},
		func() error { _, e := f.ReplaceClaim(t.Context(), storage.AccessClaim{Uses: 1 << 63}, id); return e },
		func() error {
			_, e := f.ReplaceRanges(t.Context(), storage.RangeReplaceRequest{ExpectedRevision: 1, Ranges: []storage.RangeAcquisition{{ID: 1}, {ID: 1}}}, id)
			return e
		},
		func() error { _, e := f.CancelPrepared(t.Context(), 0, id); return e },
		func() error { _, e := f.CancelDrain(t.Context(), storage.CancelDrainRequest{}, id); return e },
	}
	for i, call := range cases {
		if err := call(); err == nil || !storage.IsFileCallNotAdmitted(err) {
			t.Fatalf("invalid request %d lacks nonadmission proof: %v", i, err)
		}
	}
	if calls != 0 {
		t.Fatalf("invalid requests dispatched %d calls", calls)
	}
}

func TestFileClientRejectsMalformedCapabilityEnvelopes(t *testing.T) {
	for name, body := range map[string]string{"missing state": `{}`, "null state": `{"state":null}`, "duplicate state": `{"state":{},"state":{}}`, "unknown member": `{"state":{},"extra":false}`, "wrong state type": `{"state":true}`} {
		t.Run(name, func(t *testing.T) {
			client := protocolTransport(t, func(*http.Request) (*http.Response, error) {
				r := protocolRawResponse(body)
				r.Header.Set("Content-Type", contentJSON)
				return r, nil
			})
			if _, err := client.FileState(t.Context()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("malformed capability accepted: %v", err)
			}
		})
	}
}

func TestFileClientStrongFailureEnvelopeIsAnExactUnion(t *testing.T) {
	code := locking.Conflict
	recorded := true
	valid := fileErrorResponse{Errno: "EBUSY", Message: "conflict", LockCode: &code, Recorded: &recorded}
	client := protocolClient(t, func(fileRequest) (*http.Response, error) { return protocolResponse(t, StatusStorageError, valid), nil })
	_, err := protocolFile(client).WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("x")}, protocolID(t))
	var classified *locking.Error
	if !errors.As(err, &classified) || classified.Code != code || !classified.Recorded {
		t.Fatalf("strong classification=%v", err)
	}
	for name, alter := range map[string]func(*fileErrorResponse){"missing code": func(r *fileErrorResponse) { r.LockCode = nil }, "missing recorded": func(r *fileErrorResponse) { r.Recorded = nil }, "unknown code": func(r *fileErrorResponse) { v := locking.Code("unknown"); r.LockCode = &v }, "mismatched errno": func(r *fileErrorResponse) { r.Errno = "EACCES" }, "mixed conflict": func(r *fileErrorResponse) { r.Conflict = &fileConflict{Kind: storage.ConflictClaim} }} {
		t.Run(name, func(t *testing.T) {
			failure := valid
			alter(&failure)
			client := protocolClient(t, func(fileRequest) (*http.Response, error) {
				return protocolResponse(t, StatusStorageError, failure), nil
			})
			_, err := protocolFile(client).ReadAt(t.Context(), storage.FileReadRequest{Length: 1})
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid union accepted: %v", err)
			}
			var native *locking.Error
			if errors.As(err, &native) {
				t.Fatalf("malformed union assigned native outcome: %+v", native)
			}
		})
	}
}

func TestFileClientTerminalCleanupFactDoesNotInventHistoricalAction(t *testing.T) {
	id := protocolID(t)
	req := fileRequest{Op: storage.OpFileClose, Reference: 11, Action: id}
	terminal := fileReceipt{Operation: storage.OpFileClose, State: storage.FileActionRetired, Reference: 11}
	if err := validateFileReceipt(req, terminal); err != nil {
		t.Fatalf("terminal reference fact=%v", err)
	}
	sessionFact := fileReceipt{Operation: storage.OpFileSessionClose, State: storage.FileActionRetired}
	if err := validateFileReceipt(fileRequest{Op: storage.OpFileSessionClose, Action: id}, sessionFact); err != nil {
		t.Fatal(err)
	}
	for name, alter := range map[string]func(*fileReceipt){"action": func(r *fileReceipt) { r.Action = id }, "history": func(r *fileReceipt) { r.HistoryRemaining = 1 }, "effects": func(r *fileReceipt) { r.Effects = storage.EffectReferenceRetired }, "observation": func(r *fileReceipt) { r.Observation = protocolObservation(t, protocolAttr()) }, "wrong reference": func(r *fileReceipt) { r.Reference = 12 }, "errno": func(r *fileReceipt) { r.Errno = "EIO" }, "conflict": func(r *fileReceipt) { r.Conflict = &fileConflict{Kind: storage.ConflictRetired} }, "removal": func(r *fileReceipt) { r.Removal.EntryID = 1 }} {
		t.Run(name, func(t *testing.T) {
			value := terminal
			alter(&value)
			if err := validateFileReceipt(req, value); err == nil {
				t.Fatal("terminal fact accepted invented historical fields")
			}
		})
	}
	if err := validateFileReceipt(fileRequest{Op: storage.OpFileQueryAction, Action: id}, terminal); err == nil {
		t.Fatal("query accepted retirement as historical execution")
	}
	client := protocolClient(t, func(fileRequest) (*http.Response, error) {
		return protocolResponse(t, 200, fileResponse{Receipt: &terminal}), nil
	})
	got, err := protocolFile(client).Close(t.Context(), id)
	if err != nil || got.State != storage.FileActionRetired || got.Action != "" || got.Reference != 11 {
		t.Fatalf("terminal cleanup=%+v %v", got, err)
	}
}

func TestFileClientUnknownQueryPreservesAbsentOperation(t *testing.T) {
	id := protocolID(t)
	client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
		r := &fileReceipt{Action: id, State: storage.FileActionUnknown, Errno: "EIO"}
		return protocolResponse(t, StatusStorageError, fileErrorResponse{Errno: "EIO", Message: "unknown action", Receipt: r}), nil
	})
	got, err := protocolSession(client).QueryAction(t.Context(), id)
	if !errors.Is(err, syscall.EIO) || got.State != storage.FileActionUnknown || got.Operation != "" || got.Action != id {
		t.Fatalf("unknown query invented execution: %+v %v", got, err)
	}
}

func TestFileClientCancellationAfterLifecycleDispatchIsUnknown(t *testing.T) {
	for _, op := range []storage.Operation{storage.OpFileSessionOpen, storage.OpFileRenew, storage.OpFileCancelAction, storage.OpFileSync} {
		t.Run(string(op), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls := 0
			client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
				calls++
				if req.Op != op {
					t.Fatalf("unexpected recovery dispatch=%s", req.Op)
				}
				cancel()
				return nil, context.Canceled
			})
			s := protocolSession(client)
			var err error
			switch op {
			case storage.OpFileSessionOpen:
				_, _, err = client.NewFileSession(ctx, storage.DefaultFileSessionOptions())
			case storage.OpFileRenew:
				_, err = s.Renew(ctx)
			case storage.OpFileCancelAction:
				_, err = s.CancelAction(ctx, protocolID(t))
			case storage.OpFileSync:
				err = protocolFile(client).Sync(ctx)
			}
			wantCalls := 1

			if !errors.Is(err, syscall.EIO) || !errors.Is(err, context.Canceled) || calls != wantCalls {
				t.Fatalf("lifecycle cancellation=%v calls=%d", err, calls)
			}
		})
	}
}

func TestFileClientRetainedResponsesCannotSwitchNodes(t *testing.T) {
	for _, op := range []storage.Operation{storage.OpFileStat, storage.OpFileRead, storage.OpFileWrite} {
		t.Run(string(op), func(t *testing.T) {
			client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
				attr := protocolAttr()
				attr.ID = 8
				observation := protocolObservation(t, attr)
				response := fileResponse{Observation: observation}
				if op == storage.OpFileRead {
					response.Data = []byte("data")
				}
				if op == storage.OpFileWrite {
					r := protocolReceipt(req)
					r.Observation = observation
					r.Effects = storage.EffectContentChanged
					response = fileResponse{Receipt: r}
				}
				return protocolResponse(t, 200, response), nil
			})
			f := protocolFile(client)
			var err error
			switch op {
			case storage.OpFileStat:
				_, err = f.Stat(t.Context(), storage.ObservationOptions{})
			case storage.OpFileRead:
				_, err = f.ReadAt(t.Context(), storage.FileReadRequest{Length: 4})
			case storage.OpFileWrite:
				_, err = f.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("data")}, protocolID(t))
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("reference switched nodes: %v", err)
			}
		})
	}
}

func TestFileClientRejectsMalformedConflictFacts(t *testing.T) {
	for name, conflict := range map[string]*fileConflict{
		"kind":  {Kind: 99},
		"claim": {Kind: storage.ConflictClaim, Claim: storage.AccessClaim{Uses: 1 << 63}},
		"range": {Kind: storage.ConflictRange, Range: &storage.HeldRange{Owner: storage.RangeOwner{Session: "owner"}, Range: storage.RangeAcquisition{ID: 1, Start: 2, End: 1}}},
		"owner": {Kind: storage.ConflictRange, Range: &storage.HeldRange{Range: storage.RangeAcquisition{ID: 1, Start: 1, End: 2}}},
	} {
		t.Run(name, func(t *testing.T) {
			client := protocolClient(t, func(fileRequest) (*http.Response, error) {
				return protocolResponse(t, StatusStorageError, fileErrorResponse{Errno: "EBUSY", Message: "conflict", Conflict: conflict}), nil
			})
			_, err := protocolFile(client).ReadAt(t.Context(), storage.FileReadRequest{Length: 1})
			var native *storage.FileError
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EBUSY) || errors.As(err, &native) {
				t.Fatalf("malformed conflict became authoritative: %v", err)
			}
		})
	}
}

func TestFileClientUnknownSessionActionRequiresExplicitHistoryLookup(t *testing.T) {
	for _, op := range []storage.Operation{storage.OpFileRetain, storage.OpFileRetainAt, storage.OpFileResetAndRetainAt, storage.OpFileSetNodeAttr} {
		for _, lost := range []bool{false, true} {
			t.Run(string(op)+map[bool]string{false: "/direct", true: "/lost reply"}[lost], func(t *testing.T) {
				id := protocolID(t)
				writes, queries := 0, 0
				client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
					if req.Action != id {
						t.Fatal("action identity changed")
					}
					if req.Op == op {
						writes++
						if lost {
							return nil, io.ErrUnexpectedEOF
						}
					} else if req.Op == storage.OpFileQueryAction {
						queries++
					} else {
						t.Fatalf("unexpected request=%s", req.Op)
					}
					r := protocolReceipt(req)
					r.Operation = op
					r.Observation = protocolObservation(t, func() storage.Attr { v := protocolAttr(); v.ID = 8; return v }())
					r.Effects = storage.EffectMetadataChanged
					if op != storage.OpFileSetNodeAttr {
						r.Effects = storage.EffectRetained
						r.Reference = 11
					}
					return protocolResponse(t, 200, fileResponse{Receipt: r}), nil
				})
				s := protocolSession(client)
				target := storage.EntryTarget{Parent: 1, ParentID: 1, Name: []byte("file"), DirectoryRevision: 2, ExpectedEntryID: 4, ExpectedNodeID: 7, ExpectedMetadataRevision: 3, Witness: &storage.EntryLocation{State: storage.LocationRoot, RootNodeID: 1, NodeID: 1}}
				var got storage.FileActionReceipt
				var err error
				switch op {
				case storage.OpFileRetain:
					got, err = s.Retain(t.Context(), storage.RetainRequest{NodeID: 7}, id)
				case storage.OpFileRetainAt:
					got, err = s.RetainAt(t.Context(), storage.RetainAtRequest{Target: target}, id)
				case storage.OpFileResetAndRetainAt:
					got, err = s.ResetAndRetainAt(t.Context(), storage.ResetAndRetainRequest{Target: target, ExpectedRevision: 3}, id)
				case storage.OpFileSetNodeAttr:
					got, err = s.SetNodeAttr(t.Context(), 7, storage.AttrChange{}, id)
				}
				if !errors.Is(err, syscall.EIO) || got.State != storage.FileActionUnknown || writes != 1 || queries != 0 {
					t.Fatalf("unknown action became confirmed: %+v %v mutations=%d queries=%d", got, err, writes, queries)
				}
				observed, err := s.QueryAction(t.Context(), id)
				if err != nil || observed.Observation.Attr.ID != 8 || observed.Operation != op || got.State != storage.FileActionUnknown || writes != 1 || queries != 1 {
					t.Fatalf("explicit history changed invocation result: %+v %v original=%+v mutations=%d queries=%d", observed, err, got, writes, queries)
				}
			})
		}
	}
}

func TestFileClientObservationConfirmationRequiresExactWitness(t *testing.T) {
	attr := protocolAttr()
	attr.Kind = storage.NodeDirectory
	attr.Size = 0
	attr.DirectoryRevision = 4
	location := storage.EntryLocation{State: storage.LocationLinked, RootNodeID: 1, NodeID: 7, Ancestors: []storage.EntryCondition{{ParentID: 1, DirectoryRevision: 2, EntryID: 4, NodeID: 7, Name: []byte("directory")}}}
	condition := storage.ObservationCondition{MetadataRevision: 3, DirectoryRevision: 4, Location: location}
	for name, alter := range map[string]func(*storage.FileObservation){"exact": func(*storage.FileObservation) {}, "metadata revision": func(o *storage.FileObservation) { o.Attr.MetadataRevision++ }, "directory revision": func(o *storage.FileObservation) { o.Attr.DirectoryRevision++ }, "missing location": func(o *storage.FileObservation) { o.Location = nil }, "changed name": func(o *storage.FileObservation) { o.Location.Ancestors[0].Name = []byte("other") }, "changed ancestor revision": func(o *storage.FileObservation) { o.Location.Ancestors[0].DirectoryRevision++ }} {
		t.Run(name, func(t *testing.T) {
			client := protocolClient(t, func(fileRequest) (*http.Response, error) {
				loc := location.Clone()
				observation := storage.FileObservation{Attr: attr.Clone(), Location: &loc}
				alter(&observation)
				wire, err := fileObservationOf(observation)
				if err != nil {
					t.Fatal(err)
				}
				return protocolResponse(t, 200, fileResponse{Observation: wire}), nil
			})
			got, err := protocolFile(client).CheckObservation(t.Context(), condition)
			if name == "exact" {
				if err != nil || got.Attr.MetadataRevision != 3 || got.Attr.DirectoryRevision != 4 {
					t.Fatalf("valid confirmation=%+v %v", got, err)
				}
			} else if !errors.Is(err, syscall.EIO) {
				t.Fatalf("stale confirmation accepted: %+v %v", got, err)
			}
		})
	}
}

func TestFileClientExpiredEnrollmentReplyRetainsCleanupIdentity(t *testing.T) {
	id := protocolID(t)
	calls := 0
	client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
		calls++
		switch req.Op {
		case storage.OpFileSessionOpen:
			status := protocolStatus()
			status.Remaining = time.Nanosecond
			status.HistoryRemaining = time.Nanosecond
			return protocolResponse(t, 200, fileResponse{Session: strings.Repeat("a", 64), Status: &status}), nil
		case storage.OpFileSessionClose:
			if req.Session != strings.Repeat("a", 64) || req.Action != id {
				t.Fatalf("cleanup identity changed: %+v", req)
			}
			return protocolResponse(t, 200, fileResponse{Receipt: protocolReceipt(req)}), nil
		default:
			t.Fatalf("unexpected enrollment recovery: %s", req.Op)
		}
		return nil, nil
	})
	session, status, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if !errors.Is(err, syscall.EIO) || session == nil || status.ActionEpoch != 1 || status.Remaining != 0 || status.HistoryRemaining != 0 {
		t.Fatalf("expired reply lost cleanup ownership: %v %+v %v", session, status, err)
	}
	if _, err := session.Close(t.Context(), id); err != nil {
		t.Fatalf("cleanup=%v", err)
	}
	if calls != 2 {
		t.Fatalf("unexpected exchanges=%d", calls)
	}
}

func TestFileClientWrongNodeMutationRemainsUnknownUntilCallerQueries(t *testing.T) {
	for _, op := range []storage.Operation{storage.OpFileWrite, storage.OpFileTruncate} {
		for _, recovered := range []bool{false, true} {
			t.Run(string(op)+map[bool]string{false: "/wrong query node", true: "/correct query node"}[recovered], func(t *testing.T) {
				id := protocolID(t)
				mutations, queries := 0, 0
				client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
					if req.Action != id {
						t.Fatalf("reconciliation changed action: %q", req.Action)
					}
					attr := protocolAttr()
					attr.ID = 8
					switch req.Op {
					case op:
						mutations++
						if req.Reference != 11 {
							t.Fatalf("mutation changed reference: %d", req.Reference)
						}
					case storage.OpFileQueryAction:
						queries++
						if recovered {
							attr.ID = 7
						}
					default:
						t.Fatalf("unexpected recovery operation=%s", req.Op)
					}
					receipt := protocolReceipt(req)
					receipt.Operation = op
					receipt.Reference = 11
					receipt.Effects = storage.EffectContentChanged
					receipt.Observation = protocolObservation(t, attr)
					return protocolResponse(t, http.StatusOK, fileResponse{Receipt: receipt}), nil
				})
				file := protocolFile(client)
				var got storage.FileActionReceipt
				var err error
				switch op {
				case storage.OpFileWrite:
					got, err = file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("data")}, id)
				case storage.OpFileTruncate:
					got, err = file.Truncate(t.Context(), storage.FileTruncateRequest{Size: 4}, id)
				}
				if !errors.Is(err, syscall.EIO) || got.State != storage.FileActionUnknown || got.Effects != 0 || mutations != 1 || queries != 0 {
					t.Fatalf("wrong direct node became confirmed: %+v %v mutations=%d queries=%d", got, err, mutations, queries)
				}
				observed, queryErr := protocolSession(client).QueryAction(t.Context(), id)
				wantNode := uint64(8)
				if recovered {
					wantNode = 7
				}
				if queryErr != nil || observed.State != storage.FileActionCompleted || observed.Observation.Attr.ID != wantNode || observed.Action != id || observed.Operation != op || got.State != storage.FileActionUnknown {
					t.Fatalf("explicit query promoted old invocation: %+v %v original=%+v", observed, queryErr, got)
				}
				if mutations != 1 || queries != 1 {
					t.Fatalf("mutation was retried or query skipped: mutations=%d queries=%d", mutations, queries)
				}
			})
		}
	}
}

func TestFileClientLookupAtPreservesExactNamesAndAbsence(t *testing.T) {
	for _, found := range []bool{false, true} {
		for _, name := range [][]byte{[]byte("file"), {255, 'x'}} {
			t.Run(string(name)+map[bool]string{false: "/absent", true: "/present"}[found], func(t *testing.T) {
				calls := 0
				client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
					calls++
					if req.Op != storage.OpFileLookupAt || req.Reference != 11 || string(req.Name) != string(name) {
						t.Fatalf("exact lookup became another request: %+v", req)
					}
					lookup := &fileEntryLookup{ParentID: 7, DirectoryRevision: 9, Name: name, Found: found}
					if found {
						lookup.EntryID = 4
						lookup.Attr = protocolObservation(t, func() storage.Attr { a := protocolAttr(); a.ID = 8; return a }()).Attr
					}
					return protocolResponse(t, 200, fileResponse{Lookup: lookup}), nil
				})
				got, err := protocolFile(client).LookupAt(t.Context(), name)
				if err != nil || got.ParentID != 7 || got.DirectoryRevision != 9 || string(got.Name) != string(name) || got.Found != found || calls != 1 {
					t.Fatalf("lookup=%+v %v calls=%d", got, err, calls)
				}
				if found {
					if got.EntryID != 4 || got.Attr.ID != 8 {
						t.Fatalf("present entry identity=%+v", got)
					}
				} else if got.EntryID != 0 || !reflect.ValueOf(got.Attr).IsZero() {
					t.Fatalf("absent lookup invented facts: %+v", got)
				}
			})
		}
	}
}

func TestFileClientLookupAtRejectsUnboundOrInventedFacts(t *testing.T) {
	for name, alter := range map[string]func(*fileEntryLookup){"parent": func(l *fileEntryLookup) { l.ParentID = 9 }, "name": func(l *fileEntryLookup) { l.Name = []byte("other") }, "revision": func(l *fileEntryLookup) { l.DirectoryRevision = 0 }, "missing attr": func(l *fileEntryLookup) { l.Attr = nil }, "missing entry": func(l *fileEntryLookup) { l.EntryID = 0 }, "absent with attr": func(l *fileEntryLookup) { l.Found = false; l.EntryID = 0 }, "absent with entry": func(l *fileEntryLookup) { l.Found = false; l.Attr = nil }} {
		t.Run(name, func(t *testing.T) {
			client := protocolClient(t, func(fileRequest) (*http.Response, error) {
				l := &fileEntryLookup{ParentID: 7, DirectoryRevision: 9, Name: []byte("file"), Found: true, EntryID: 4, Attr: protocolObservation(t, func() storage.Attr { a := protocolAttr(); a.ID = 8; return a }()).Attr}
				alter(l)
				return protocolResponse(t, 200, fileResponse{Lookup: l}), nil
			})
			got, err := protocolFile(client).LookupAt(t.Context(), []byte("file"))
			if !errors.Is(err, syscall.EIO) || got.Found || got.ParentID != 0 {
				t.Fatalf("unbound lookup accepted: %+v %v", got, err)
			}
		})
	}
	calls := 0
	client := protocolClient(t, func(fileRequest) (*http.Response, error) { calls++; return nil, io.ErrUnexpectedEOF })
	for _, name := range [][]byte{nil, []byte("."), []byte(".."), []byte("a/b"), {'a', 0}} {
		if _, err := protocolFile(client).LookupAt(t.Context(), name); err == nil {
			t.Fatalf("invalid lookup name accepted: %q", name)
		}
	}
	if calls != 0 {
		t.Fatalf("invalid lookup dispatched %d calls", calls)
	}
}

func TestFileClientLostChangedOperandsRefusalCannotPromoteOldReceipt(t *testing.T) {
	id := protocolID(t)
	mutations, queries := 0, 0
	content := ""
	var original *fileReceipt
	notAdmitted := false
	client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
		if req.Action != id {
			t.Fatalf("action changed: %q", req.Action)
		}
		switch req.Op {
		case storage.OpFileWrite:
			mutations++
			if original == nil {
				content = string(req.Write.Data)
				original = protocolReceipt(req)
				original.Reference = 11
				original.Effects = storage.EffectContentChanged
				attr := protocolAttr()
				attr.Size = int64(len(content))
				original.Observation = protocolObservation(t, attr)
				return protocolResponse(t, 200, fileResponse{Receipt: original}), nil
			}
			if string(req.Write.Data) == content {
				t.Fatal("test did not change operands")
			}
			refusal := &storage.FileError{Code: syscall.EINVAL, NotAdmitted: true}
			notAdmitted = storage.IsFileCallNotAdmitted(refusal)
			return nil, io.ErrUnexpectedEOF
		case storage.OpFileQueryAction:
			queries++
			return protocolResponse(t, 200, fileResponse{Receipt: original}), nil
		default:
			t.Fatalf("unexpected operation %s", req.Op)
		}
		return nil, nil
	})
	file := protocolFile(client)
	first, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("first")}, id)
	if err != nil || first.State != storage.FileActionCompleted {
		t.Fatalf("original operation=%+v %v", first, err)
	}
	changed, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("different")}, id)
	if !notAdmitted || !errors.Is(err, syscall.EIO) || changed.State != storage.FileActionUnknown || changed.Effects != 0 || mutations != 2 || queries != 0 || content != "first" {
		t.Fatalf("lost refusal promoted historical success: %+v %v mutations=%d queries=%d content=%q", changed, err, mutations, queries, content)
	}
	history, err := protocolSession(client).QueryAction(t.Context(), id)
	if err != nil || history.State != storage.FileActionCompleted || history.Observation.Attr.Size != 5 || history.Action != first.Action || changed.State != storage.FileActionUnknown || queries != 1 || mutations != 2 {
		t.Fatalf("explicit old history changed new invocation: %+v %v current=%+v", history, err, changed)
	}
}

func TestFileClientNotAdmittedProofIsLocalToOneInvocation(t *testing.T) {
	id := protocolID(t)
	calls := 0
	client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
		calls++
		if req.Action != id || req.Op != storage.OpFileWrite {
			t.Fatalf("unexpected implicit operation: %+v", req)
		}
		if calls == 1 {
			return nil, io.ErrUnexpectedEOF
		}
		return protocolResponse(t, StatusStorageError, fileErrorResponse{Errno: "EAGAIN", Message: "capacity", NotAdmitted: true}), nil
	})
	file := protocolFile(client)
	original, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("same")}, id)
	if !errors.Is(err, syscall.EIO) || original.State != storage.FileActionUnknown || storage.IsFileCallNotAdmitted(err) {
		t.Fatalf("unknown call acquired nonadmission proof: %+v %v", original, err)
	}
	current, err := file.WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("same")}, id)
	if !errors.Is(err, syscall.EAGAIN) || !storage.IsFileCallNotAdmitted(err) || !reflect.ValueOf(current).IsZero() || original.State != storage.FileActionUnknown || calls != 2 {
		t.Fatalf("current proof invented historical result: %+v %v prior=%+v calls=%d", current, err, original, calls)
	}
}

func TestFileClientRejectsNotAdmittedCombinedWithHistoricalReceipt(t *testing.T) {
	client := protocolClient(t, func(req fileRequest) (*http.Response, error) {
		r := protocolReceipt(req)
		r.State = storage.FileActionNotApplied
		r.Errno = "EAGAIN"
		return protocolResponse(t, StatusStorageError, fileErrorResponse{Errno: "EAGAIN", Message: "capacity", NotAdmitted: true, Receipt: r}), nil
	})
	got, err := protocolFile(client).WriteAt(t.Context(), storage.FileWriteRequest{Data: []byte("x")}, protocolID(t))
	if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EAGAIN) || storage.IsFileCallNotAdmitted(err) || got.State != storage.FileActionUnknown {
		t.Fatalf("ambiguous nonadmission proof accepted: %+v %v", got, err)
	}
}
