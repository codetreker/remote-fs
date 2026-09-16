package windows

import (
	"context"
	"errors"
	"io"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func nativeTestHTTP(t *testing.T, a *nativeAuthority) *httprest.Storage {
	t.Helper()
	h, err := httprest.NewHandler(a, a)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(func() {
		h.Stop()
		server.Close()
		if err := h.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	client, err := httprest.Dial(server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func TestNativeAuthoritySubscriptionStartsAtRetention(t *testing.T) {
	a := newNativeAuthority()
	client := nativeTestHTTP(t, a)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, populated := range []bool{false, true} {
		if populated {
			s := nativeTestSession(t, client)
			root := nativeTestRoot(t, s)
			nativeTestCreate(t, s, root, "history")
		}
		sub, err := client.Subscribe(ctx)
		if err != nil {
			t.Fatalf("subscribe populated=%v: %v", populated, err)
		}
		if sub.Incarnation() != metastore.Incarnation(a.identity) || sub.Position() != a.position() || sub.Tail() != a.position() || !sub.CaughtUp() {
			t.Errorf("retention populated=%v incarnation=%q position=%d tail=%d caughtUp=%v", populated, sub.Incarnation(), sub.Position(), sub.Tail(), sub.CaughtUp())
		}
		if err := sub.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestNativeAuthorityDeliversHTTPEventsAndBoundedSnapshot(t *testing.T) {
	a := newNativeAuthority()
	client := nativeTestHTTP(t, a)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sub, err := client.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	s := nativeTestSession(t, client)
	root := nativeTestRoot(t, s)
	file := nativeTestCreate(t, s, root, "live.bin")
	created, err := sub.Next()
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}
	if created.Kind != metastore.Created || created.Node == nil || string(created.Name) != "live.bin" || created.Notification == nil || created.Notification.After == nil {
		t.Fatalf("created=%+v", created)
	}
	lookup, err := root.LookupAt(ctx, []byte("live.bin"))
	if err != nil || !lookup.Found || lookup.EntryID == 0 || lookup.Attr.ID != uint64(created.Node.ID) || lookup.Check([]byte("live.bin")) != nil {
		t.Fatalf("HTTP exact lookup lost source identity: %+v %v", lookup, err)
	}
	if _, err := file.WriteAt(ctx, storage.FileWriteRequest{Data: []byte("payload")}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	modified, err := sub.Next()
	if err != nil {
		t.Fatalf("write delivery: %v", err)
	}
	if modified.Kind != metastore.Modified || modified.Position <= created.Position || modified.Node == nil || modified.Node.Size != 7 || modified.Notification.ChangeMask&metastore.ChangeContent == 0 {
		t.Fatalf("modified=%+v", modified)
	}
	if _, err := file.Rename(ctx, storage.RenameRequest{Source: nativeTestTarget(t, root, "live.bin"), Destination: nativeTestTarget(t, root, "renamed.bin")}, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
	renamed, err := sub.Next()
	if err != nil {
		t.Fatalf("rename delivery: %v", err)
	}
	if renamed.Kind != metastore.Renamed || renamed.Position <= modified.Position || renamed.From == nil || string(renamed.From.Name) != "live.bin" || string(renamed.Name) != "renamed.bin" {
		t.Fatalf("renamed=%+v", renamed)
	}
	for _, change := range []metastore.Change{created, modified, renamed} {
		if err := metastore.ValidateNotification(change); err != nil {
			t.Fatal(err)
		}
	}
	encoded, err := storage.EncodeMetadata(nil)
	if err != nil {
		t.Fatal(err)
	}
	entryID := created.Notification.After.Location.Ancestors[len(created.Notification.After.Location.Ancestors)-1].EntryID
	if entryID == 0 || entryID == storage.EntryID(created.Node.ID) {
		t.Fatal("snapshot fixture needs a source entry identity distinct from its node identity")
	}
	const scalarBytes int64 = 9
	exact := 2*scalarBytes + int64(len("renamed.bin")+2*len(encoded))
	small := scalarBytes - 1 + int64(len("renamed.bin")+len(encoded))
	for _, bound := range []int64{exact, small} {
		snap, position, err := a.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if position != renamed.Position {
			t.Fatalf("snapshot position=%d want%d", position, renamed.Position)
		}
		result, err := metastore.NewRowResult(bound, 0, func(_ int, row metastore.Row, l metastore.RowPayloadLengths) (int64, error) {
			if len(row.Name) != 0 || row.Node.Content != "" || len(row.Node.Metadata) != 0 || len(row.Node.LinkTarget) != 0 {
				t.Fatal("snapshot charged retained payload")
			}
			if row.Parent == 0 && row.EntryID != 0 || row.Parent != 0 && row.EntryID != entryID {
				t.Fatalf("snapshot reservation changed source entry identity: %+v", row)
			}
			return scalarBytes + l.Name + l.Content + l.Metadata + l.Target, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		done, readErr := snap.Next(ctx, 8, result)
		rows, resultErr := result.Rows()
		if err := snap.Close(); err != nil {
			t.Fatal(err)
		}
		if bound == small {
			if !errors.Is(readErr, syscall.EFBIG) || !errors.Is(resultErr, syscall.EFBIG) || len(rows) != 0 {
				t.Fatalf("small snapshot rows=%v errors=%v,%v", rows, readErr, resultErr)
			}
			continue
		}
		if readErr != nil || resultErr != nil || !done || len(rows) != 2 || rows[0].EntryID != 0 || rows[1].EntryID != entryID || string(rows[1].Name) != "renamed.bin" || rows[1].Node.ID != created.Node.ID || rows[1].Node.Size != 7 {
			t.Fatalf("snapshot done=%v rows=%+v errors=%v,%v", done, rows, readErr, resultErr)
		}
	}
	picture, err := client.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer picture.Close()
	var received []metastore.Row
	for {
		page, err := picture.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(received)+len(page) > 2 {
			t.Fatal("HTTP snapshot returned extra entries")
		}
		received = append(received, page...)
	}
	if picture.Position() != renamed.Position || len(received) != 2 || received[0].EntryID != 0 || received[1].EntryID != entryID || received[1].Node.ID != created.Node.ID || string(received[1].Name) != "renamed.bin" {
		t.Fatalf("HTTP snapshot lost source entry identity: position=%d rows=%+v", picture.Position(), received)
	}
	if _, err := file.Close(ctx, nativeTestAction(t, s)); err != nil {
		t.Fatal(err)
	}
}
