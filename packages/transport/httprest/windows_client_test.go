package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func windowsTestAttr() storage.WindowsAttr {
	return storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 7, Mode: 0o640, Size: 4, AccessTime: time.Unix(1, 2), ModTime: time.Unix(3, 4)}, CreationTime: time.Unix(5, 6), ChangeTime: time.Unix(7, 8), DOSAttributes: storage.WindowsDOSArchive}, NameInfo: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "file"}}
}
func windowsTestStatus() storage.FileSessionStatus {
	return storage.FileSessionStatus{Epoch: "authority", Remaining: time.Minute, Revision: 1, ActionEpoch: 1, HistoryRemaining: time.Minute}
}
func windowsTestID(t *testing.T) storage.WindowsActionID {
	t.Helper()
	id, err := storage.NewLockRequestID(1)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func windowsTestHTTPResponse(t *testing.T, status int, value any) *http.Response {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	response := lockTestResponse(string(body))
	response.StatusCode = status
	response.Header.Set("Content-Type", contentJSON)
	return response
}
func windowsTestClient(t *testing.T, serve func(windowsRequest) (windowsResponse, error)) *Storage {
	t.Helper()
	return lockTestClient(t, func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		var request windowsRequest
		if err := decodeWindowsJSON(body, &request); err != nil {
			t.Fatalf("client emitted invalid Windows request: %v", err)
		}
		path := "/prefix/v3/windows"
		if windowsControl(request.Op) {
			path = "/prefix/v3/windows-control"
		}
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != contentJSON || r.URL.Path != path || r.URL.RawQuery != "" {
			t.Fatalf("Windows endpoint admission mismatch: %s %s", r.Method, r.URL.RequestURI())
		}
		response, err := serve(request)
		if err != nil {
			return nil, err
		}
		if response.Data == nil {
			response.Data = []byte{}
		}
		if response.Entries == nil {
			response.Entries = []windowsEntry{}
		}
		return windowsTestHTTPResponse(t, http.StatusOK, response), nil
	})
}

func TestWindowsClientPreservesSemanticRequestsAndNativeResults(t *testing.T) {
	sessionID, fileID := strings.Repeat("a", 64), strings.Repeat("b", 64)
	attr := windowsTestAttr()
	seen := map[storage.Operation]int{}
	client := windowsTestClient(t, func(req windowsRequest) (windowsResponse, error) {
		seen[req.Op]++
		response := windowsResponse{}
		switch req.Op {
		case storage.OpWindowsState:
			response.State = &storage.WindowsState{Enabled: true, ActionEpoch: 1, MaxEventBytes: 4096, VolumeIdentity: "stable-volume", VolumeSerial: 42}
		case storage.OpWindowsEnable, storage.OpWindowsQueryActivation:
			response.Activation = &windowsActivation{Action: req.Action, State: storage.WindowsActionCompleted, Enabled: true, HistoryRemaining: time.Minute}
		case storage.OpWindowsSessionOpen:
			response.Session = sessionID
			status := windowsTestStatus()
			response.Status = &status
		default:
			if req.Session != sessionID {
				t.Fatalf("wrong Windows owner: %q", req.Session)
			}
			switch req.Op {
			case storage.OpWindowsRenew, storage.OpWindowsStatus:
				status := windowsTestStatus()
				response.Status = &status
			case storage.OpWindowsOpen:
				if req.Open.Intent != (storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen, Kind: storage.WindowsRegularFile}) || req.Open.Lookup.Name != "file" || req.Open.DOSAttributes != storage.WindowsDOSArchive {
					t.Fatalf("open intent changed: %+v", req.Open)
				}
				response.File = fileID
				response.Attr = windowsAttrOf(attr)
				response.CreateAction = storage.WindowsOpened
			case storage.OpWindowsQueryAction, storage.OpWindowsCancelAction:
				state := storage.WindowsActionCompleted
				if req.Op == storage.OpWindowsCancelAction {
					state = storage.WindowsActionCancelled
				}
				response.Action = &windowsAction{Action: req.Action, State: state, HistoryRemaining: time.Minute}
			case storage.OpWindowsSessionClose:
			default:
				if req.File != fileID {
					t.Fatalf("wrong Windows reference: %q", req.File)
				}
				switch req.Op {
				case storage.OpWindowsStat:
					response.Attr = windowsAttrOf(attr)
				case storage.OpWindowsRead:
					response.ReadAttr = AttrOf(attr.Attr)
					response.Data = []byte("data")
				case storage.OpWindowsList:
					response.Entries = []windowsEntry{{Name: "child", Attr: windowsBasicAttrOf(attr.WindowsBasicAttr)}}
				case storage.OpWindowsReadLink:
					response.Symlink = &windowsSymlink{Target: "../target", Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "parent/link"}}
				case storage.OpWindowsSync:
				default:
					response.Action = &windowsAction{Action: req.Action, State: storage.WindowsActionCompleted, Attr: windowsAttrOf(attr), Applied: 1, HistoryRemaining: time.Minute}
				}
			}
		}
		return response, nil
	})
	if err := client.CheckWindowsStorage(); err != nil {
		t.Fatal(err)
	}
	state, err := client.WindowsState(t.Context())
	if err != nil || state.VolumeSerial != 42 || state.VolumeIdentity != "stable-volume" || !state.Enabled {
		t.Fatalf("state = %+v, %v", state, err)
	}
	activationID := windowsTestID(t)
	for _, call := range []func(context.Context, storage.WindowsActionID) (storage.WindowsActivation, error){client.EnableWindows, client.QueryWindowsActivation} {
		got, err := call(t.Context(), activationID)
		if err != nil || got.Action != activationID || !got.Enabled || got.HistoryRemaining <= 0 || got.HistoryRemaining > time.Minute {
			t.Fatalf("activation = %+v, %v", got, err)
		}
	}
	session, err := client.NewWindowsSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	opened, err := session.Open(t.Context(), storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen, Kind: storage.WindowsRegularFile}, Lookup: storage.WindowsLookup{ParentID: 1, Name: "file"}, Mode: 0o640, DOSAttributes: storage.WindowsDOSArchive}, windowsTestID(t))
	if err != nil || !reflect.DeepEqual(opened.Attr, attr) || opened.CreateAction != storage.WindowsOpened || opened.File.Reference() != fileID {
		t.Fatalf("open = %+v, %v", opened, err)
	}
	for _, call := range []func(context.Context) (storage.FileSessionStatus, error){session.Status, session.Renew} {
		got, err := call(t.Context())
		if err != nil || got.Epoch != "authority" || got.ActionEpoch != 1 {
			t.Fatalf("status = %+v, %v", got, err)
		}
	}
	file := opened.File
	gotAttr, err := file.Stat(t.Context())
	if err != nil || !reflect.DeepEqual(gotAttr, attr) {
		t.Fatalf("stat = %+v, %v", gotAttr, err)
	}
	read, err := file.ReadAt(t.Context(), 0, 4)
	if err != nil || string(read.Data) != "data" || read.Attr.ID != 7 {
		t.Fatalf("read = %+v, %v", read, err)
	}
	listing, err := storage.NewWindowsListResult(4096, 0, func(_ int, n int64, a storage.WindowsBasicAttr) (int64, error) {
		if !a.CreationTime.Equal(attr.CreationTime) {
			t.Error("listing lost Windows timestamps")
		}
		return 256 + n, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := file.ListBounded(t.Context(), listing); err != nil {
		t.Fatal(err)
	}
	entries, err := listing.Entries()
	if err != nil || len(entries) != 1 || entries[0].Name != "child" || entries[0].Attr.DOSAttributes != storage.WindowsDOSArchive {
		t.Fatalf("listing = %+v, %v", entries, err)
	}
	creation := time.Unix(-10, 12)
	dos := storage.WindowsDOSHidden
	calls := []func(storage.WindowsActionID) (storage.WindowsActionResult, error){
		func(id storage.WindowsActionID) (storage.WindowsActionResult, error) {
			return file.WriteAt(t.Context(), 0, []byte("data"), id)
		},
		func(id storage.WindowsActionID) (storage.WindowsActionResult, error) {
			return file.Truncate(t.Context(), 4, id)
		},
		func(id storage.WindowsActionID) (storage.WindowsActionResult, error) {
			return file.SetAttr(t.Context(), storage.WindowsAttrChange{CreationTime: &creation, DOSAttributes: &dos}, id)
		},
		func(id storage.WindowsActionID) (storage.WindowsActionResult, error) {
			return file.Rename(t.Context(), storage.WindowsRenameRequest{Source: storage.WindowsLookup{ParentID: 1, Name: "file", ExpectedID: 7}, Destination: storage.WindowsLookup{ParentID: 1, Name: "renamed"}}, id)
		},
		func(id storage.WindowsActionID) (storage.WindowsActionResult, error) {
			return file.SetDeletePending(t.Context(), true, id)
		},
		func(id storage.WindowsActionID) (storage.WindowsActionResult, error) {
			return file.LockBatch(t.Context(), storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{{Offset: 0, Length: 4, Type: storage.Exclusive}}}, id)
		},
		func(id storage.WindowsActionID) (storage.WindowsActionResult, error) {
			return file.Close(t.Context(), id)
		},
		func(id storage.WindowsActionID) (storage.WindowsActionResult, error) {
			return session.QueryAction(t.Context(), id)
		},
		func(id storage.WindowsActionID) (storage.WindowsActionResult, error) {
			return session.CancelAction(t.Context(), id)
		},
	}
	for _, call := range calls {
		id := windowsTestID(t)
		got, err := call(id)
		if err != nil || got.Action != id || got.HistoryRemaining <= 0 {
			t.Fatalf("action = %+v, %v", got, err)
		}
	}
	link, err := file.ReadLink(t.Context())
	if err != nil || link.Target != "../target" || link.Location.Path != "parent/link" || link.Unparsed != "" {
		t.Fatalf("read-link = %+v, %v", link, err)
	}
	linkID := windowsTestID(t)
	set, err := file.SetLink(t.Context(), "../target", linkID)
	if err != nil || set.Action != linkID {
		t.Fatalf("set-link = %+v, %v", set, err)
	}
	if err := file.Sync(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("closed session remained usable: %v", err)
	}
	if len(seen) != 23 {
		t.Fatalf("exercised %d Windows operations: %v", len(seen), seen)
	}
}

func TestWindowsClientPreservesPartialFailureAndCancelGrantReceipts(t *testing.T) {
	id := windowsTestID(t)
	cap := strings.Repeat("a", 64)
	attr := windowsTestAttr()
	for _, failure := range []storage.WindowsFailure{storage.WindowsSharingViolation, storage.WindowsLockConflict, storage.WindowsDeletePending, storage.WindowsRangeNotLocked} {
		t.Run(string(failure), func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				return windowsTestHTTPResponse(t, StatusStorageError, windowsErrorResponse{Errno: "EACCES", Message: "Windows request refused", Failure: failure, Action: &windowsAction{Action: id, State: storage.WindowsActionRejected, Errno: "EACCES", Failure: failure, Applied: 1, HistoryRemaining: time.Minute}}), nil
			})
			session := &remoteWindowsSession{storage: client, id: cap}
			result, err := session.QueryAction(t.Context(), id)
			if !errors.Is(err, syscall.EACCES) || storage.WindowsFailureOf(err) != failure || result.Action != id || result.Applied != 1 || result.Errno != syscall.EACCES || result.Failure != failure || result.State != storage.WindowsActionRejected {
				t.Fatalf("partial outcome = %+v, %v", result, err)
			}
		})
	}
	client := windowsTestClient(t, func(req windowsRequest) (windowsResponse, error) {
		return windowsResponse{Action: &windowsAction{Action: req.Action, State: storage.WindowsActionCompleted, File: cap, Attr: windowsAttrOf(attr), CreateAction: storage.WindowsOpened, HistoryRemaining: time.Minute}}, nil
	})
	session := &remoteWindowsSession{storage: client, id: cap}
	granted, err := session.CancelAction(t.Context(), id)
	if err != nil || granted.State != storage.WindowsActionCompleted || granted.File == nil || granted.File.Reference() != cap || granted.Attr.ID != attr.ID {
		t.Fatalf("grant winning cancellation = %+v, %v", granted, err)
	}
}

func TestWindowsClientUnknownMutationIsNotRetriedOrInventedByDeniedQuery(t *testing.T) {
	calls := 0
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return nil, errors.New("response lost after write")
		}
		return windowsTestHTTPResponse(t, StatusStorageError, windowsErrorResponse{Errno: "EACCES", Message: "access denied"}), nil
	})
	session := &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}
	file := &remoteWindowsFile{session: session, id: strings.Repeat("b", 64), node: 7}
	id := windowsTestID(t)
	result, unknown := file.WriteAt(t.Context(), 0, []byte("bytes"), id)
	var transport *operationError
	if !errors.As(unknown, &transport) || !transport.unknown || !errors.Is(unknown, syscall.EIO) || result.Action != "" || calls != 1 {
		t.Fatalf("unknown mutation = %+v, %v; calls=%d", result, unknown, calls)
	}
	result, err := session.QueryAction(t.Context(), id)
	if !errors.Is(err, syscall.EACCES) || result.Action != "" || result.File != nil || calls != 2 {
		t.Fatalf("denied query invented outcome = %+v, %v", result, err)
	}
	if !transport.unknown {
		t.Fatal("denied reconciliation changed unknown mutation")
	}
}

func TestWindowsClientRejectsMalformedAndMismatchedResponses(t *testing.T) {
	id := windowsTestID(t)
	other := windowsTestID(t)
	valid := windowsResponse{Action: &windowsAction{Action: id, State: storage.WindowsActionCompleted, HistoryRemaining: time.Minute}, Data: []byte{}, Entries: []windowsEntry{}}
	for name, damage := range map[string]func(*windowsResponse){
		"missing action":       func(r *windowsResponse) { r.Action = nil },
		"other action":         func(r *windowsResponse) { r.Action.Action = other },
		"unknown action state": func(r *windowsResponse) { r.Action.State = 255 },
		"negative applied":     func(r *windowsResponse) { r.Action.Applied = -1 },
		"unknown failure":      func(r *windowsResponse) { r.Action.Failure = "made-up" },
		"failed success":       func(r *windowsResponse) { r.Action.Errno = "UNRECOGNIZED" },
		"unrelated state": func(r *windowsResponse) {
			r.State = &storage.WindowsState{ActionEpoch: 1, MaxEventBytes: 4096, VolumeIdentity: "volume"}
		},
		"file without attributes": func(r *windowsResponse) {
			r.Action.File = strings.Repeat("a", 64)
			r.Action.CreateAction = storage.WindowsOpened
		},
	} {
		t.Run(name, func(t *testing.T) {
			action := *valid.Action
			response := valid
			response.Action = &action
			damage(&response)
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				return windowsTestHTTPResponse(t, http.StatusOK, response), nil
			})
			session := &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}
			result, err := session.QueryAction(t.Context(), id)
			if !errors.Is(err, syscall.EIO) || result.Action != "" {
				t.Fatalf("malformed receipt = %+v, %v", result, err)
			}
		})
	}
	for name, body := range map[string]string{
		"missing data":     `{"session":"","file":"","createAction":0,"entries":[]}`,
		"null data":        `{"session":"","file":"","createAction":0,"data":null,"entries":[]}`,
		"duplicate":        `{"session":"","session":"","file":"","createAction":0,"data":"","entries":[]}`,
		"unknown":          `{"session":"","file":"","createAction":0,"data":"","entries":[],"extra":false}`,
		"wrong state type": `{"state":true,"session":"","file":"","createAction":0,"data":"","entries":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				r := lockTestResponse(body)
				r.Header.Set("Content-Type", contentJSON)
				return r, nil
			})
			if _, err := client.WindowsState(t.Context()); !errors.Is(err, syscall.EIO) {
				t.Fatalf("malformed Windows capability accepted: %v", err)
			}
		})
	}
}

func TestWindowsClientRejectsWrongObjectIdentityAndProtocol(t *testing.T) {
	for name, damage := range map[string]func(*http.Response){
		"protocol":  func(r *http.Response) { r.Header.Set(HeaderProtocol, "0") },
		"type":      func(r *http.Response) { r.Header.Set("Content-Type", "text/plain") },
		"oversized": func(r *http.Response) { r.ContentLength = windowsControlResponseLimit() + 1 },
		"short":     func(r *http.Response) { r.ContentLength++ },
		"status":    func(r *http.Response) { r.StatusCode = http.StatusBadGateway },
	} {
		t.Run(name, func(t *testing.T) {
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				r := windowsTestHTTPResponse(t, http.StatusOK, windowsResponse{State: &storage.WindowsState{ActionEpoch: 1, MaxEventBytes: 4096, VolumeIdentity: "volume"}, Data: []byte{}, Entries: []windowsEntry{}})
				damage(r)
				return r, nil
			})
			if err := client.CheckWindowsStorage(); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid capability response = %v", err)
			}
		})
	}
	attr := windowsTestAttr()
	attr.ID = 8
	client := windowsTestClient(t, func(windowsRequest) (windowsResponse, error) { return windowsResponse{Attr: windowsAttrOf(attr)}, nil })
	file := &remoteWindowsFile{session: &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}, id: strings.Repeat("b", 64), node: 7}
	if _, err := file.Stat(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("replacement object accepted: %v", err)
	}
}

func TestWindowsClientValidatesIntentAndRangesBeforeDispatch(t *testing.T) {
	calls := 0
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("unexpected dispatch") })
	session := &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}
	file := &remoteWindowsFile{session: session, id: strings.Repeat("b", 64), node: 7}
	id := windowsTestID(t)
	checks := []func() error{
		func() error {
			_, err := client.NewWindowsSession(t.Context(), storage.FileSessionOptions{})
			return err
		},
		func() error { _, err := client.EnableWindows(t.Context(), "invalid"); return err },
		func() error { _, err := session.Open(t.Context(), storage.WindowsOpenRequest{}, id); return err },
		func() error { _, err := file.ReadAt(t.Context(), -1, 1); return err },
		func() error { _, err := file.WriteAt(t.Context(), -1, []byte("x"), id); return err },
		func() error { _, err := file.Truncate(t.Context(), -1, id); return err },
		func() error { _, err := file.LockBatch(t.Context(), storage.WindowsLockBatch{}, id); return err },
		func() error { _, err := file.Rename(t.Context(), storage.WindowsRenameRequest{}, id); return err },
		func() error {
			unknown := uint32(1 << 31)
			_, err := file.SetAttr(t.Context(), storage.WindowsAttrChange{DOSAttributes: &unknown}, id)
			return err
		},
		func() error { return file.ListBounded(t.Context(), nil) },
	}
	for _, check := range checks {
		if err := check(); err == nil {
			t.Fatal("invalid Windows request accepted")
		}
	}
	if calls != 0 {
		t.Fatalf("invalid requests reached transport %d times", calls)
	}
}

func TestWindowsClientListingFailureDiscardsEarlierEntries(t *testing.T) {
	attr := windowsTestAttr()
	client := windowsTestClient(t, func(windowsRequest) (windowsResponse, error) {
		return windowsResponse{Entries: []windowsEntry{{Name: "a", Attr: windowsBasicAttrOf(attr.WindowsBasicAttr)}, {Name: "bb", Attr: windowsBasicAttrOf(attr.WindowsBasicAttr)}}}, nil
	})
	file := &remoteWindowsFile{session: &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}, id: strings.Repeat("b", 64), node: 7}
	result, err := storage.NewWindowsListResult(1, 0, func(_ int, n int64, _ storage.WindowsBasicAttr) (int64, error) { return n, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := file.ListBounded(t.Context(), result); err == nil {
		t.Fatal("oversized listing succeeded")
	}
	if entries, err := result.Entries(); err == nil || entries != nil {
		t.Fatalf("listing failure exposed a prefix: %+v, %v", entries, err)
	}
}

func TestWindowsClientReadCancellationDiffersFromUnknownWrite(t *testing.T) {
	for _, read := range []bool{true, false} {
		t.Run(map[bool]string{true: "read", false: "write"}[read], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) { cancel(); return nil, ctx.Err() })
			file := &remoteWindowsFile{session: &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}, id: strings.Repeat("b", 64), node: 7}
			var err error
			if read {
				_, err = file.ReadAt(ctx, 0, 1)
			} else {
				_, err = file.WriteAt(ctx, 0, []byte("x"), windowsTestID(t))
			}
			want := syscall.EIO
			if read {
				want = syscall.EINTR
			}
			if !errors.Is(err, want) || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled operation = %v; want %v", err, want)
			}
		})
	}
}

func TestWindowsClientSymlinkFailurePreservesBoundedNativeRedirect(t *testing.T) {
	detail := windowsSymlink{Target: "../target", Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "parent/link"}, Unparsed: "/child"}
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		return windowsTestHTTPResponse(t, StatusStorageError, windowsErrorResponse{Errno: "ELOOP", Message: "Windows path encounters a symbolic link", Symlink: &detail}), nil
	})
	session := &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}
	request := storage.WindowsOpenRequest{WindowsOpenIntent: storage.WindowsOpenIntent{Disposition: storage.WindowsOpen, Kind: storage.WindowsAny}, Lookup: storage.WindowsLookup{ParentID: 1, Name: "link"}}
	result, err := session.Open(t.Context(), request, windowsTestID(t))
	var link *storage.WindowsSymlinkError
	if !errors.Is(err, syscall.ELOOP) || !errors.As(err, &link) || link.Target != detail.Target || link.Location != detail.Location || link.Unparsed != detail.Unparsed || result.File != nil {
		t.Fatalf("symlink result = %+v, %v", result, err)
	}
	for name, damage := range map[string]func(*windowsErrorResponse){
		"wrong errno":        func(r *windowsErrorResponse) { r.Errno = "EACCES" },
		"absolute host path": func(r *windowsErrorResponse) { r.Symlink.Target = "C:\\outside" },
		"invalid location":   func(r *windowsErrorResponse) { r.Symlink.Location.State = storage.WindowsNameDetached },
		"oversized suffix": func(r *windowsErrorResponse) {
			r.Symlink.Unparsed = strings.Repeat("x", storage.WindowsMaxLinkTargetBytes+1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			copy := detail
			response := windowsErrorResponse{Errno: "ELOOP", Message: "link", Symlink: &copy}
			damage(&response)
			bad := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				return windowsTestHTTPResponse(t, StatusStorageError, response), nil
			})
			badSession := &remoteWindowsSession{storage: bad, id: strings.Repeat("a", 64)}
			_, err := badSession.Open(t.Context(), request, windowsTestID(t))
			var link *storage.WindowsSymlinkError
			if !errors.Is(err, syscall.EIO) || errors.As(err, &link) {
				t.Fatalf("malformed symlink response accepted: %v", err)
			}
		})
	}
}

func TestWindowsClientAdmissionRefusalDoesNotDispatch(t *testing.T) {
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		t.Fatal("capacity refusal reached transport")
		return nil, nil
	})
	bytes := retainedResponseMultiplier * windowsControlResponseLimit()
	client.windowsControls = newBodyAdmission(1, bytes, 0)
	release, err := client.windowsControls.acquire(t.Context(), bytes)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := client.WindowsState(t.Context()); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("capacity refusal = %v", err)
	}
}

func TestWindowsClientQueryPreservesHistoricalFailure(t *testing.T) {
	id := windowsTestID(t)
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		return windowsTestHTTPResponse(t, StatusStorageError, windowsErrorResponse{Errno: "EINVAL", Message: "range was not held", Failure: storage.WindowsRangeNotLocked, Action: &windowsAction{Action: id, State: storage.WindowsActionRejected, Errno: "EINVAL", Failure: storage.WindowsRangeNotLocked, Applied: 1, HistoryRemaining: time.Minute}}), nil
	})
	session := &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}
	for _, lookup := range []func(context.Context, storage.WindowsActionID) (storage.WindowsActionResult, error){session.QueryAction, session.CancelAction} {
		result, err := lookup(t.Context(), id)
		if !errors.Is(err, syscall.EINVAL) || storage.WindowsFailureOf(err) != storage.WindowsRangeNotLocked || result.Action != id || result.State != storage.WindowsActionRejected || result.Errno != syscall.EINVAL || result.Failure != storage.WindowsRangeNotLocked || result.Applied != 1 {
			t.Fatalf("historical failed action = %+v, %v", result, err)
		}
	}
}

func TestWindowsClientQueriesHistoricalSymlinkFacts(t *testing.T) {
	id := windowsTestID(t)
	link := &windowsSymlink{Target: "../target", Location: storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: "parent/link"}, Unparsed: "/child"}
	client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
		return windowsTestHTTPResponse(t, StatusStorageError, windowsErrorResponse{Errno: "ELOOP", Message: "symbolic link", Symlink: link, Action: &windowsAction{Action: id, State: storage.WindowsActionRejected, Errno: "ELOOP", Symlink: link, HistoryRemaining: time.Minute}}), nil
	})
	session := &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}
	result, err := session.QueryAction(t.Context(), id)
	var failure *storage.WindowsSymlinkError
	if !errors.Is(err, syscall.ELOOP) || !errors.As(err, &failure) || result.Errno != syscall.ELOOP || result.Symlink == nil || result.Symlink.Target != link.Target || result.Symlink.Location != link.Location || result.Symlink.Unparsed != link.Unparsed {
		t.Fatalf("historical link facts = %+v, %v", result, err)
	}
}

func TestWindowsClientLockBatchPreservesAnInvalidLaterElement(t *testing.T) {
	id := windowsTestID(t)
	client := lockTestClient(t, func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		var request windowsRequest
		if err := decodeWindowsJSON(body, &request); err != nil {
			t.Fatal(err)
		}
		if len(request.Ranges) != 2 || request.Ranges[0].Type != storage.Unlock || request.Ranges[1].Type != 255 {
			t.Fatalf("client rewrote batch semantics: %+v", request.Ranges)
		}
		return windowsTestHTTPResponse(t, StatusStorageError, windowsErrorResponse{Errno: "EINVAL", Message: "invalid later lock element", Action: &windowsAction{Action: id, State: storage.WindowsActionRejected, Errno: "EINVAL", Applied: 1, HistoryRemaining: time.Minute}}), nil
	})
	file := &remoteWindowsFile{session: &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}, id: strings.Repeat("b", 64), node: 7}
	result, err := file.LockBatch(t.Context(), storage.WindowsLockBatch{Ranges: []storage.WindowsLockRange{{Offset: 0, Length: 1, Type: storage.Unlock}, {Offset: 1, Length: 1, Type: 255}}}, id)
	if !errors.Is(err, syscall.EINVAL) || result.Action != id || result.Applied != 1 {
		t.Fatalf("partial batch = %+v, %v", result, err)
	}
}

func TestWindowsClientRetainsListingAdmissionUntilCallerTransferEnds(t *testing.T) {
	attr := windowsTestAttr()
	calls := 0
	client := windowsTestClient(t, func(windowsRequest) (windowsResponse, error) {
		calls++
		return windowsResponse{Entries: []windowsEntry{{Name: "child", Attr: windowsBasicAttrOf(attr.WindowsBasicAttr)}}}, nil
	})
	client.fileRequests = newBodyAdmission(1, retainedResponseMultiplier*client.maxBodyBytes, 0)
	file := &remoteWindowsFile{session: &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}, id: strings.Repeat("b", 64), node: 7}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	blocked, err := storage.NewWindowsListResult(1024, 0, func(int, int64, storage.WindowsBasicAttr) (int64, error) { close(entered); <-release; return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- file.ListBounded(t.Context(), blocked) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("listing transfer did not begin")
	}
	next, err := storage.NewWindowsListResult(1024, 0, func(int, int64, storage.WindowsBasicAttr) (int64, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := file.ListBounded(t.Context(), next); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("blocked transfer released admission early: %v", err)
	}
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listing transfer did not complete")
	}
	fresh, err := storage.NewWindowsListResult(1024, 0, func(int, int64, storage.WindowsBasicAttr) (int64, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := file.ListBounded(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("capacity refusal dispatched or release was lost: %d exchanges", calls)
	}
}

func TestWindowsClientValidatesBatchCompletionAgainstRequestedElementCount(t *testing.T) {
	for name, tc := range map[string]struct {
		state   storage.WindowsActionState
		applied int
		errno   string
		count   int
		status  int
		valid   bool
	}{
		"partial success":       {storage.WindowsActionCompleted, 1, "", 2, http.StatusOK, false},
		"extra success":         {storage.WindowsActionCompleted, 3, "", 2, http.StatusOK, false},
		"extra failure":         {storage.WindowsActionRejected, 3, "EINVAL", 2, StatusStorageError, false},
		"complete":              {storage.WindowsActionCompleted, 2, "", 2, http.StatusOK, true},
		"pending single":        {storage.WindowsActionPending, 0, "", 1, http.StatusOK, true},
		"known partial failure": {storage.WindowsActionRejected, 1, "EINVAL", 2, StatusStorageError, true},
	} {
		t.Run(name, func(t *testing.T) {
			id := windowsTestID(t)
			client := lockTestClient(t, func(*http.Request) (*http.Response, error) {
				action := &windowsAction{Action: id, State: tc.state, Applied: tc.applied, Errno: tc.errno, HistoryRemaining: time.Minute}
				if tc.status == StatusStorageError {
					return windowsTestHTTPResponse(t, tc.status, windowsErrorResponse{Errno: tc.errno, Message: "batch refused", Action: action}), nil
				}
				return windowsTestHTTPResponse(t, http.StatusOK, windowsResponse{Action: action, Data: []byte{}, Entries: []windowsEntry{}}), nil
			})
			file := &remoteWindowsFile{session: &remoteWindowsSession{storage: client, id: strings.Repeat("a", 64)}, id: strings.Repeat("b", 64), node: 7}
			ranges := make([]storage.WindowsLockRange, tc.count)
			for i := range ranges {
				ranges[i] = storage.WindowsLockRange{Offset: uint64(i), Length: 1, Type: storage.Exclusive}
			}
			result, err := file.LockBatch(t.Context(), storage.WindowsLockBatch{Ranges: ranges}, id)
			if !tc.valid {
				if !errors.Is(err, syscall.EIO) || result.Action != "" {
					t.Fatalf("incomplete batch accepted: %+v, %v", result, err)
				}
				return
			}
			if result.Action != id || result.Applied != tc.applied || result.State != tc.state {
				t.Fatalf("valid batch result changed: %+v, %v", result, err)
			}
			if tc.errno == "" && err != nil || tc.errno != "" && !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("batch error = %v", err)
			}
		})
	}
}

func TestWindowsClientRequiresFramesThatFitTheDeclaredEventCapability(t *testing.T) {
	state := storage.WindowsState{Enabled: true, ActionEpoch: 1, MaxEventBytes: 512 << 10, VolumeIdentity: "volume", VolumeSerial: 1}
	client := windowsTestClient(t, func(windowsRequest) (windowsResponse, error) { return windowsResponse{State: &state}, nil })
	client.maxFrameBytes = 1024
	if err := client.CheckWindowsStorage(); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("small frame advertised complete notification capability: %v", err)
	}
	required, err := maxChangeFrameBytes(state.MaxEventBytes)
	if err != nil {
		t.Fatal(err)
	}
	client.maxFrameBytes = required
	got, err := client.WindowsState(t.Context())
	if err != nil || got.MaxEventBytes != state.MaxEventBytes {
		t.Fatalf("exact frame capacity = %+v, %v", got, err)
	}
}
