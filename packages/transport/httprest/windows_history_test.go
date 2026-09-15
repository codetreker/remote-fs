package httprest

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestWindowsRegistryPrunesOnlyCompletedExpiredHistory(t *testing.T) {
	now := time.Unix(1000, 0)
	before, after := now.Add(-time.Second), now.Add(time.Hour)
	session := &servedWindowsSession{
		actions: make(map[storage.WindowsActionID]*servedWindowsAction),
		files: map[string]*servedWindowsFile{
			"expired":  {reference: "expired-native", closed: true, forgetAfter: before},
			"retained": {reference: "retained-native", closed: true, forgetAfter: after},
			"active":   {reference: "active-native", forgetAfter: before},
		},
		references: map[string]string{"expired-native": "expired", "retained-native": "retained", "active-native": "active"},
	}
	ids := make(map[string]storage.WindowsActionID)
	for _, test := range []struct {
		name                    string
		cleanup, running, known bool
		expires                 time.Time
	}{
		{name: "expired data", known: true, expires: before},
		{name: "expired cleanup", cleanup: true, known: true, expires: before},
		{name: "retained data", known: true, expires: after},
		{name: "retained cleanup", cleanup: true, known: true, expires: after},
		{name: "pending", known: true},
		{name: "unknown"},
		{name: "running", running: true, known: true, expires: before},
	} {
		id, err := storage.NewLockRequestID(1)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		if !test.running {
			close(done)
		}
		session.actions[id] = &servedWindowsAction{cleanup: test.cleanup, running: test.running, known: test.known, expires: test.expires, done: done}
		ids[test.name] = id
		if test.cleanup {
			session.cleanupActions++
		} else {
			session.dataActions++
		}
	}
	session.mu.Lock()
	session.pruneLocked(now)
	session.pruneLocked(now)
	if session.dataActions != 4 || session.cleanupActions != 1 || len(session.actions) != 5 {
		t.Errorf("expired history accounting = data %d cleanup %d records %d", session.dataActions, session.cleanupActions, len(session.actions))
	}
	for _, name := range []string{"pending", "unknown", "running", "retained data", "retained cleanup"} {
		if session.actions[ids[name]] == nil {
			t.Errorf("pruning removed %s history", name)
		}
	}
	if session.files["expired"] != nil || session.references["expired-native"] != "" || session.files["retained"] == nil || session.references["retained-native"] != "retained" || session.files["active"] == nil {
		t.Error("reference pruning did not separate expired, retained, and active references")
	}
	running := session.actions[ids["running"]]
	session.mu.Unlock()
	session.finishAction(running)
	session.mu.Lock()
	defer session.mu.Unlock()
	session.pruneLocked(now)
	if session.actions[ids["running"]] != nil || session.dataActions != 3 || session.cleanupActions != 1 {
		t.Errorf("completed expired replay retained its charge: data %d cleanup %d", session.dataActions, session.cleanupActions)
	}
	session.pruneLocked(after)
	session.pruneLocked(after)
	if session.dataActions != 2 || session.cleanupActions != 0 || len(session.actions) != 2 || session.actions[ids["pending"]] == nil || session.actions[ids["unknown"]] == nil {
		t.Errorf("pruning fabricated completion of pending outcomes: data %d cleanup %d records %d", session.dataActions, session.cleanupActions, len(session.actions))
	}
	if len(session.files) != 1 || session.files["active"] == nil || len(session.references) != 1 || session.references["active-native"] != "active" {
		t.Error("history expiry removed a live file or retained a retired reference")
	}
}

func TestWindowsRegistryPruningPreservesQueryableOpenAndAdmissionCharge(t *testing.T) {
	limits := DefaultFileLimits()
	limits.MaxActions = 1
	client, handler, backend := windowsServerFixture(t, limits)
	if err := backend.Write(t.Context(), "file", nil); err != nil {
		t.Fatal(err)
	}
	root, err := backend.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.MaxFiles = 1
	session := windowsServerSession(t, client, options)
	request := storage.WindowsOpenRequest{
		WindowsOpenIntent: storage.WindowsOpenIntent{Access: storage.WindowsAllAccess, Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen},
		Lookup:            storage.WindowsLookup{ParentID: root.ID, Name: "file"},
	}
	openID := windowsServerAction(t, session)
	opened, err := session.Open(t.Context(), request, openID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.File.Close(t.Context(), windowsServerAction(t, session)); err != nil {
		t.Fatal(err)
	}
	remote := session.(*remoteWindowsSession)
	handler.windows.mu.Lock()
	served := handler.windows.sessions[remote.id]
	handler.windows.mu.Unlock()
	served.mu.Lock()
	served.pruneLocked(time.Now())
	retained := served.actions[openID] != nil && served.files[opened.File.Reference()] != nil && served.dataActions == 1
	served.mu.Unlock()
	if !retained {
		t.Fatal("pruning dropped a still-queryable open receipt, closed reference, or admission charge")
	}
	result, err := session.QueryAction(t.Context(), openID)
	if err != nil || result.File == nil || result.File.Reference() != opened.File.Reference() || result.Attr.ID != opened.Attr.ID || result.HistoryRemaining <= 0 {
		t.Fatalf("query after pruning = %+v, %v", result, err)
	}
	if _, err := session.Open(t.Context(), request, windowsServerAction(t, session)); !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("pruning released capacity occupied by queryable history: %v", err)
	}
	if _, err := result.File.ReadAt(t.Context(), 0, 1); !errors.Is(err, syscall.EBADF) {
		t.Fatalf("historical open receipt recreated a closed reference: %v", err)
	}
}
