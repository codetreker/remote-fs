package smb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

func clientTestAction(t *testing.T) storage.FileActionID {
	t.Helper()
	id, err := storage.NewFileActionID(17)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func clientTestOpenRequest() windowsOpenRequest {
	return windowsOpenRequest{windowsOpenIntent: windowsOpenIntent{Access: windowsReadData, Share: windowsShareAll, Disposition: windowsOpen, Kind: windowsRegularFile}, Lookup: windowsLookup{ParentID: 2, ParentReference: 22, Name: "report.txt"}}
}
func clientTestReceipt(id storage.FileActionID, op storage.Operation, f *clientTestFile) storage.FileActionReceipt {
	return storage.FileActionReceipt{Action: id, Operation: op, State: storage.FileActionCompleted, Effects: storage.EffectRetained | storage.EffectClaimChanged, Reference: f.ref, Observation: f.observation.Clone()}
}

func TestClientOpenSendsCanonicalBytesAndWholeWitness(t *testing.T) {
	session, raw, root, parent, child := newClientTestSession(t)
	var got storage.RetainAtRequest
	calls := 0
	raw.onRetainAt = func(r storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
		got = r
		calls++
		return clientTestReceipt(id, storage.OpFileRetainAt, child), nil
	}
	opened, err := session.Open(t.Context(), clientTestOpenRequest(), clientTestAction(t))
	if err != nil || opened.File == nil || opened.File.Reference() != 33 || opened.Attr.NameInfo.Path != "Parent/Report.TXT" {
		t.Fatalf("canonical open %+v %v", opened, err)
	}
	if calls != 1 || string(got.Target.Name) != "Report.TXT" || got.Target.Parent != 22 || got.Target.ParentID != 2 || got.Target.DirectoryRevision != 41 || got.Target.ExpectedEntryID != 23 || got.Target.ExpectedNodeID != 3 || got.Target.ExpectedMetadataRevision != 7 {
		t.Fatalf("target %+v", got.Target)
	}
	if got.Target.Witness == nil || !reflect.DeepEqual(*got.Target.Witness, *parent.observation.Location) {
		t.Fatalf("whole witness %+v", got.Target.Witness)
	}
	if len(root.lists) != 1 || root.lists[0].Revision != 31 || len(parent.lists) != 1 || parent.lists[0].Revision != 41 || len(parent.checks) != 1 || !reflect.DeepEqual(parent.checks[0].Location, *parent.observation.Location) {
		t.Fatal("directory revisions and final witness were not checked")
	}
	if got.Claim != (storage.AccessClaim{Uses: storage.ReadContent}) {
		t.Fatalf("claim %+v", got.Claim)
	}
}

func TestClientOpenRejectsEntireInvalidDirectoryView(t *testing.T) {
	for _, bad := range []string{"REPORT.txt", "bad:name", "CON.txt"} {
		t.Run(bad, func(t *testing.T) {
			session, raw, _, parent, child := newClientTestSession(t)
			calls := 0
			raw.onRetainAt = func(storage.RetainAtRequest, storage.FileActionID) (storage.FileActionReceipt, error) {
				calls++
				return storage.FileActionReceipt{}, nil
			}
			parent.onList = func(r storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
				if len(r.Cursor.After) == 0 {
					return storage.DirectoryPage{ParentID: 2, Revision: 41, Entries: parent.entries, Next: storage.DirectoryCursor{ParentID: 2, Revision: 41, After: []byte("Report.TXT")}}, nil
				}
				return storage.DirectoryPage{ParentID: 2, Revision: 41, Entries: []storage.DirectoryEntry{{EntryID: 24, Name: []byte(bad), Attr: child.observation.Attr}}, Done: true}, nil
			}
			if _, err := session.Open(t.Context(), clientTestOpenRequest(), clientTestAction(t)); err == nil || calls != 0 || len(parent.lists) != 2 {
				t.Fatalf("invalid full view err=%v retain calls=%d pages=%d", err, calls, len(parent.lists))
			}
		})
	}
}

func TestClientOpenRejectsChangedWitnessBeforeRetention(t *testing.T) {
	session, raw, _, parent, _ := newClientTestSession(t)
	parent.checkErr = &storage.FileError{Code: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision}}
	calls := 0
	raw.onRetainAt = func(storage.RetainAtRequest, storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		return storage.FileActionReceipt{}, nil
	}
	if _, err := session.Open(t.Context(), clientTestOpenRequest(), clientTestAction(t)); !errors.Is(err, syscall.EAGAIN) || calls != 0 {
		t.Fatalf("stale witness err=%v calls=%d", err, calls)
	}
}

func TestClientOpenReadOnlyAndSharingClaims(t *testing.T) {
	for _, access := range []windowsAccess{windowsReadData, windowsWriteData, windowsAppendData, windowsDelete} {
		t.Run(fmt.Sprintf("access_%x", access), func(t *testing.T) {
			session, raw, _, parent, child := newClientTestSession(t)
			payload, err := encodeWindowsMetadata(windowsMetadata{Attributes: dosReadOnly})
			if err != nil {
				t.Fatal(err)
			}
			child.observation.Attr.Metadata = storage.Metadata{{Key: windowsMetadataKey, Version: 1, Data: payload}}
			parent.entries[0].Attr = child.observation.Attr.Clone()
			calls := 0
			raw.onRetainAt = func(r storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				calls++
				uses := storage.ReadContent
				if access == windowsDelete {
					uses = storage.RemoveEntry
				}
				if r.Claim != (storage.AccessClaim{Uses: uses, Excludes: storage.WriteContent | storage.RemoveEntry}) {
					t.Fatalf("read-only share claim %+v", r.Claim)
				}
				return clientTestReceipt(id, storage.OpFileRetainAt, child), nil
			}
			request := clientTestOpenRequest()
			request.Access = access
			request.Share = windowsShareRead
			_, err = session.Open(t.Context(), request, clientTestAction(t))
			if access == windowsReadData || access == windowsDelete {
				if err != nil || calls != 1 {
					t.Fatalf("read err=%v calls=%d", err, calls)
				}
			} else if !errors.Is(err, syscall.EACCES) || calls != 0 {
				t.Fatalf("mutation access %d err=%v calls=%d", access, err, calls)
			}
		})
	}
}

func TestClientBackupMetadataOpenUsesOrdinaryAuthorization(t *testing.T) {
	for _, pattern := range []struct{ options, access, share uint32 }{{0x204042, 0x100080, 7}, {0x204002, 0x80, 7}, {0x224022, 0x100080, 0}} {
		t.Run(fmt.Sprintf("options_%x", pattern.options), func(t *testing.T) {
			session, raw, _, _, child := newClientTestSession(t)
			decoded := wire.CreateRequest{Options: pattern.options, DesiredAccess: pattern.access, ShareAccess: pattern.share, Disposition: 1}
			intent, err := windowsIntent(decoded)
			if err != nil {
				t.Fatal(err)
			}
			decoded.Options &^= 0x4000
			ordinary, err := windowsIntent(decoded)
			if err != nil || intent != ordinary {
				t.Fatalf("backup altered intent %+v ordinary %+v err %v", intent, ordinary, err)
			}
			request := clientTestOpenRequest()
			request.windowsOpenIntent = intent
			var authorized []authz.AccessRequest
			denied := false
			session.backend.authorize = authz.AuthorizerFunc(func(_ context.Context, r authz.AccessRequest) error {
				if r.Operation == storage.OpFileRetainAt {
					authorized = append(authorized, r)
					if denied {
						return authz.ErrDenied
					}
				}
				return nil
			})
			calls := 0
			raw.onRetainAt = func(r storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				calls++
				if r.Claim != windowsClaim(ordinary, storage.NodeRegular) {
					t.Fatalf("retained claim %+v", r.Claim)
				}
				return clientTestReceipt(id, storage.OpFileRetainAt, child), nil
			}
			if _, err := session.Open(t.Context(), request, clientTestAction(t)); err != nil {
				t.Fatal(err)
			}
			if len(authorized) != 1 || authorized[0].Volume != "trusted" || authorized[0].Effects != storage.EffectRetained|storage.EffectClaimChanged || authorized[0].Claim.Uses != 0 || authorized[0].Claim != windowsClaim(ordinary, storage.NodeRegular) || authorized[0].Parent != 22 || authorized[0].Node != 3 {
				t.Fatalf("ordinary authorization %+v", authorized)
			}
			denied = true
			if _, err := session.Open(t.Context(), request, clientTestAction(t)); !errors.Is(err, authz.ErrDenied) || calls != 1 || len(authorized) != 2 {
				t.Fatalf("backup policy bypass err=%v admissions=%d authorizations=%d", err, calls, len(authorized))
			}
		})
	}
}

func TestClientCreateAuthorizationPrecedesMutation(t *testing.T) {
	session, raw, _, parent, _ := newClientTestSession(t)
	parent.entries = nil
	var authorized authz.AccessRequest
	session.backend.authorize = authz.AuthorizerFunc(func(_ context.Context, r authz.AccessRequest) error {
		if r.Operation == storage.OpFileCreateAndRetainAt {
			authorized = r
			return authz.ErrDenied
		}
		return nil
	})
	calls := 0
	raw.onCreate = func(storage.CreateAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		return storage.FileActionReceipt{}, nil
	}
	request := clientTestOpenRequest()
	request.Disposition = windowsCreate
	request.Access = windowsWriteData
	request.Share = windowsShareRead
	if _, err := session.Open(t.Context(), request, clientTestAction(t)); !errors.Is(err, authz.ErrDenied) || calls != 0 {
		t.Fatalf("create policy err=%v calls=%d", err, calls)
	}
	if authorized.Operation != storage.OpFileCreateAndRetainAt || authorized.Effects != storage.EffectRetained|storage.EffectClaimChanged|storage.EffectCreated || authorized.Claim != (storage.AccessClaim{Uses: storage.WriteContent, Excludes: storage.WriteContent | storage.RemoveEntry}) || authorized.Parent != 22 || authorized.Node != 0 {
		t.Fatalf("create authority %+v", authorized)
	}
}

func TestClientUnknownCreateReconcilesOriginalReceipt(t *testing.T) {
	session, raw, _, parent, child := newClientTestSession(t)
	parent.entries = nil
	id := clientTestAction(t)
	calls, queries := 0, 0
	original := clientTestReceipt(id, storage.OpFileCreateAndRetainAt, child)
	original.Effects |= storage.EffectCreated
	original.Observation.Attr.Size = 73
	original.Observation.Location.Ancestors[1].Name = []byte("new.txt")
	raw.onCreate = func(r storage.CreateAndRetainRequest, actual storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		if actual != id || string(r.Target.Name) != "new.txt" {
			t.Fatalf("create identity %q %+v", actual, r)
		}
		child.observation.Attr.Size = 999
		return storage.FileActionReceipt{Action: actual, State: storage.FileActionUnknown}, syscall.EIO
	}
	raw.onQuery = func(actual storage.FileActionID) (storage.FileActionReceipt, error) {
		queries++
		if actual != id {
			t.Fatalf("queried replacement action %q", actual)
		}
		return original.Clone(), nil
	}
	request := clientTestOpenRequest()
	request.Lookup.Name = "new.txt"
	request.Disposition = windowsCreate
	dispatcher := newFileDispatcher(session.backend, session, 17, DefaultLimits())
	opened, err := dispatcher.open(t.Context(), request, id)
	if err != nil || calls != 1 || queries != 1 || opened.File == nil || opened.File.Reference() != 33 || opened.Attr.Size != 73 || opened.Attr.NameInfo.Path != "Parent/new.txt" || opened.CreateAction != windowsCreated {
		t.Fatalf("receipt recovery result=%+v error=%v mutations=%d queries=%d", opened, err, calls, queries)
	}
	if len(child.stats) != 0 {
		t.Fatal("recovery restatted the retained object")
	}
}

func TestClientMaximumAllowedUsesResolvedKindAndCurrentPolicy(t *testing.T) {
	for _, kind := range []storage.NodeKind{storage.NodeRegular, storage.NodeDirectory} {
		t.Run(fmt.Sprintf("kind_%d", kind), func(t *testing.T) {
			session, raw, _, parent, child := newClientTestSession(t)
			child.observation.Attr.Kind = kind
			parent.entries[0].Attr = child.observation.Attr.Clone()
			var candidates []authz.AccessRequest
			session.backend.authorize = authz.AuthorizerFunc(func(_ context.Context, r authz.AccessRequest) error {
				if r.Node != 3 && r.Operation != storage.OpFileRetainAt {
					return nil
				}
				candidates = append(candidates, r)
				switch r.Operation {
				case storage.OpFileRetainAt:
					if r.Claim.Uses&^storage.ReadContent == 0 {
						return nil
					}
				case storage.OpFileRead:
					if kind == storage.NodeRegular {
						return nil
					}
				case storage.OpFileListAt:
					if kind == storage.NodeDirectory {
						return nil
					}
				case storage.OpFileStat:
					return nil
				}
				return authz.ErrDenied
			})
			calls := 0
			raw.onRetainAt = func(r storage.RetainAtRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				calls++
				want := storage.AccessClaim{}
				if kind == storage.NodeRegular {
					want.Uses = storage.ReadContent
				}
				if r.Claim != want {
					t.Fatalf("resolved kind %d claim %+v", kind, r.Claim)
				}
				return clientTestReceipt(id, storage.OpFileRetainAt, child), nil
			}
			request := clientTestOpenRequest()
			request.Kind = windowsAny
			request.Access = 0
			request.MaximumAllowed = true
			opened, err := session.Open(t.Context(), request, clientTestAction(t))
			if err != nil || encodeAccess(opened.GrantedAccess) != 0x20081 || calls != 1 {
				t.Fatalf("maximum grant %+v err=%v calls=%d", opened, err, calls)
			}
			found := false
			for _, r := range candidates {
				if r.Node == 3 && ((kind == storage.NodeRegular && r.Operation == storage.OpFileRead) || (kind == storage.NodeDirectory && r.Operation == storage.OpFileListAt)) {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing actual-kind candidate %+v", candidates)
			}
			cause := errors.New("current policy unavailable")
			session.backend.authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return cause })
			if _, err := session.Open(t.Context(), request, clientTestAction(t)); !errors.Is(err, cause) || calls != 1 {
				t.Fatalf("policy outage err=%v admissions=%d", err, calls)
			}
		})
	}
}

func TestClientCreateRetriesOnlyKnownRevisionConflict(t *testing.T) {
	for _, unknownSecond := range []bool{false, true} {
		t.Run(fmt.Sprintf("second_unknown_%t", unknownSecond), func(t *testing.T) {
			session, raw, _, parent, child := newClientTestSession(t)
			parent.entries = nil
			original := clientTestAction(t)
			var actual []storage.FileActionID
			var targets []storage.EntryTarget
			raw.onCreate = func(r storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				actual = append(actual, id)
				targets = append(targets, r.Target)
				if len(actual) == 1 {
					parent.observation.Attr.DirectoryRevision = 42
					return storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision}}, syscall.EAGAIN
				}
				if unknownSecond {
					return storage.FileActionReceipt{Action: id, State: storage.FileActionUnknown}, syscall.EIO
				}
				return clientTestReceipt(id, storage.OpFileCreateAndRetainAt, child), nil
			}
			queries := 0
			raw.onQuery = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
				queries++
				if len(actual) != 2 || id != actual[1] {
					t.Fatalf("queried stale action %q actual=%v", id, actual)
				}
				return clientTestReceipt(id, storage.OpFileCreateAndRetainAt, child), nil
			}
			request := clientTestOpenRequest()
			request.Disposition = windowsCreate
			d := newFileDispatcher(session.backend, session, 17, DefaultLimits())
			opened, err := d.open(t.Context(), request, original)
			if err != nil || opened.File == nil || len(actual) != 2 || actual[0] != original || actual[1] == original {
				t.Fatalf("retry err=%v actions=%v", err, actual)
			}
			if targets[0].DirectoryRevision != 41 || targets[1].DirectoryRevision != 42 || targets[0].Witness == nil || targets[1].Witness == nil || !reflect.DeepEqual(*targets[0].Witness, *parent.observation.Location) || !reflect.DeepEqual(*targets[1].Witness, *parent.observation.Location) {
				t.Fatalf("retry targets %+v", targets)
			}
			if unknownSecond && queries != 1 || !unknownSecond && queries != 0 {
				t.Fatalf("unexpected queries %d", queries)
			}
			if record := session.action(original); record == nil || record.actual != actual[1] {
				t.Fatalf("action alias %+v", record)
			}
		})
	}
}

func TestClientOpenRetriesReadConflictWithOriginalAction(t *testing.T) {
	session, raw, _, parent, child := newClientTestSession(t)
	parent.onList = func(r storage.DirectoryPageRequest) (storage.DirectoryPage, error) {
		if len(parent.lists) == 1 {
			return storage.DirectoryPage{}, syscall.EAGAIN
		}
		return storage.DirectoryPage{ParentID: 2, Revision: 41, Entries: parent.entries, Done: true}, nil
	}
	id := clientTestAction(t)
	calls := 0
	raw.onRetainAt = func(_ storage.RetainAtRequest, actual storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		if actual != id {
			t.Fatalf("unadmitted action changed %q", actual)
		}
		return clientTestReceipt(actual, storage.OpFileRetainAt, child), nil
	}
	if _, err := session.Open(t.Context(), clientTestOpenRequest(), id); err != nil || calls != 1 || len(parent.lists) != 2 {
		t.Fatalf("read retry err=%v calls=%d pages=%d", err, calls, len(parent.lists))
	}
}

func TestClientCreateRetryBudgetAndUnsafeReceipts(t *testing.T) {
	for _, kind := range []string{"revision", "unknown", "effect", "reference", "wrong action"} {
		t.Run(kind, func(t *testing.T) {
			session, raw, _, parent, _ := newClientTestSession(t)
			parent.entries = nil
			calls := 0
			wrong := clientTestAction(t)
			raw.onCreate = func(_ storage.CreateAndRetainRequest, id storage.FileActionID) (storage.FileActionReceipt, error) {
				calls++
				r := storage.FileActionReceipt{Action: id, State: storage.FileActionNotApplied, Errno: syscall.EAGAIN, Conflict: &storage.FileConflict{Kind: storage.ConflictRevision}}
				switch kind {
				case "unknown":
					r.State = storage.FileActionUnknown
				case "effect":
					r.Effects = storage.EffectCreated
				case "reference":
					r.Reference = 33
				case "wrong action":
					r.Action = wrong
				}
				return r, syscall.EAGAIN
			}
			request := clientTestOpenRequest()
			request.Disposition = windowsCreate
			if _, err := session.Open(t.Context(), request, clientTestAction(t)); err == nil {
				t.Fatal("unsafe or exhausted retry succeeded")
			}
			want := 1
			if kind == "revision" {
				want = 8
			}
			if calls != want {
				t.Fatalf("%s admissions=%d want=%d", kind, calls, want)
			}
		})
	}
}

func TestClientQueryRejectsWrongActualAction(t *testing.T) {
	session, raw, _, _, child := newClientTestSession(t)
	original, actual, wrong := clientTestAction(t), clientTestAction(t), clientTestAction(t)
	if err := session.rememberOpen(original, actual, clientAction{access: windowsReadData}); err != nil {
		t.Fatal(err)
	}
	raw.onQuery = func(id storage.FileActionID) (storage.FileActionReceipt, error) {
		if id != actual {
			t.Fatalf("query=%q want=%q", id, actual)
		}
		return clientTestReceipt(wrong, storage.OpFileRetainAt, child), nil
	}
	result, err := session.QueryAction(t.Context(), original)
	if !errors.Is(err, syscall.EIO) || result.Action == original || result.File != nil {
		t.Fatalf("mismatched receipt accepted: %+v %v", result, err)
	}
	if a := session.action(original); a == nil || a.terminal {
		t.Fatalf("wrong action changed history %+v", a)
	}
}

func TestClientOverwriteMetadataCapacityFailsBeforeActionAdmission(t *testing.T) {
	session, raw, _, parent, child := newClientTestSession(t)
	metadata := make(storage.Metadata, storage.MaxMetadataEntries)
	for i := range metadata {
		metadata[i] = storage.OpaqueMetadata{Key: fmt.Sprintf("foreign.%02d", i), Version: 1, Data: []byte{byte(i)}}
	}
	if err := metadata.Check(); err != nil {
		t.Fatal(err)
	}
	child.observation.Attr.Metadata = metadata.Clone()
	parent.entries[0].Attr = child.observation.Attr.Clone()
	session.options.MaxActions = 1
	calls := 0
	raw.onReset = func(storage.ResetAndRetainRequest, storage.FileActionID) (storage.FileActionReceipt, error) {
		calls++
		return storage.FileActionReceipt{}, nil
	}
	request := clientTestOpenRequest()
	request.Disposition = windowsOverwrite
	request.Access = windowsWriteData
	request.DOSAttributes = dosArchive
	for range 3 {
		if _, err := session.Open(t.Context(), request, clientTestAction(t)); !errors.Is(err, syscall.EFBIG) {
			t.Fatalf("metadata envelope capacity: %v", err)
		}
		if len(session.actions) != 0 || calls != 0 {
			t.Fatalf("local validation consumed action capacity: records=%d resets=%d", len(session.actions), calls)
		}
	}
	if !reflect.DeepEqual(child.observation.Attr.Metadata, metadata) {
		t.Fatal("failed overwrite modified foreign metadata")
	}
}
