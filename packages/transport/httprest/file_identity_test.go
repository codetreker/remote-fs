package httprest

import (
	"context"
	"errors"
	"io/fs"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestRetainedHTTPNodeOperationsFollowIdentityThroughNamespaceChanges(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("original")); err != nil {
		t.Fatal(err)
	}
	session, pin := openRetainedFixture(t, client)
	original, err := pin.Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Rename(ctx, "file", "moved"); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, "file", []byte("replacement")); err != nil {
		t.Fatal(err)
	}
	replacement, err := backend.Stat(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	file, err := session.OpenNode(ctx, original.ID, storage.FileOpenOptions{ExpectedID: original.ID, Read: true, Write: true})
	if err != nil {
		t.Fatal(err)
	}
	read, err := file.ReadAt(ctx, 0, 64)
	if err != nil || read.Attr.ID != original.ID || string(read.Data) != "original" {
		t.Fatalf("identity open read=%+v, error=%v", read, err)
	}
	mode := fs.FileMode(0640)
	stamp := time.Unix(1700000000, 123456789)
	attr, err := session.SetNodeAttr(ctx, original.ID, storage.AttrChange{Mode: &mode, ModTime: &stamp})
	if err != nil || attr.ID != original.ID || attr.Mode.Perm() != mode || !attr.ModTime.Equal(stamp) {
		t.Fatalf("identity attributes=%+v, error=%v", attr, err)
	}
	moved, err := backend.Stat(ctx, "moved")
	if err != nil || moved.ID != original.ID || moved.Mode.Perm() != mode || !moved.ModTime.Equal(stamp) {
		t.Fatalf("renamed native attributes=%+v, error=%v", moved, err)
	}
	if err := backend.Remove(ctx, "moved"); err != nil {
		t.Fatal(err)
	}
	mode = 0600
	attr, err = session.SetNodeAttr(ctx, original.ID, storage.AttrChange{Mode: &mode})
	if err != nil || attr.ID != original.ID || attr.Mode.Perm() != mode {
		t.Fatalf("detached identity attributes=%+v, error=%v", attr, err)
	}
	retained, err := file.Stat(ctx)
	if err != nil || retained.ID != original.ID || retained.Mode.Perm() != mode {
		t.Fatalf("retained attributes=%+v, error=%v", retained, err)
	}
	current, err := backend.Stat(ctx, "file")
	if err != nil || current.ID != replacement.ID || current.Mode != replacement.Mode || !current.ModTime.Equal(replacement.ModTime) {
		t.Fatalf("identity mutation changed replacement=%+v, error=%v", current, err)
	}
	if _, err := backend.Stat(ctx, "moved"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("detached attribute mutation recreated a name: %v", err)
	}
	if err := pin.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.OpenNode(ctx, original.ID, storage.FileOpenOptions{Read: true}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reclaimed identity open=%v", err)
	}
	if _, err := session.SetNodeAttr(ctx, original.ID, storage.AttrChange{Mode: &mode}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("reclaimed identity attributes=%v", err)
	}
}

func TestRetainedHTTPNodeOperationsPreserveValidationAndCancellationErrors(t *testing.T) {
	ctx := context.Background()
	client, _, backend := retainedHTTPFixture(t, DefaultFileLimits())
	if err := backend.Write(ctx, "file", []byte("preserve")); err != nil {
		t.Fatal(err)
	}
	session, file := openRetainedFixture(t, client)
	before, err := file.Stat(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		id   uint64
		open storage.FileOpenOptions
		want syscall.Errno
	}{
		{"zero identity", 0, storage.FileOpenOptions{Read: true}, syscall.EINVAL},
		{"inconsistent identity", before.ID, storage.FileOpenOptions{Read: true, ExpectedID: before.ID + 1}, syscall.EINVAL},
		{"creation by identity", before.ID, storage.FileOpenOptions{Write: true, Create: true}, syscall.EINVAL},
		{"unknown identity", math.MaxUint64, storage.FileOpenOptions{Read: true}, syscall.ESTALE},
	} {
		t.Run(test.name, func(t *testing.T) {
			opened, err := session.OpenNode(ctx, test.id, test.open)
			if !errors.Is(err, test.want) || opened != nil {
				t.Fatalf("identity open=%v, error=%v, want %v", opened, err, test.want)
			}
		})
	}
	invalidMode := fs.ModeDir
	if _, err := session.SetNodeAttr(ctx, before.ID, storage.AttrChange{Mode: &invalidMode}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid identity attributes=%v", err)
	}
	request, cancel := context.WithCancel(ctx)
	cancel()
	if opened, err := session.OpenNode(request, before.ID, storage.FileOpenOptions{Write: true, Truncate: true}); storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, context.Canceled) || opened != nil {
		t.Fatalf("cancelled identity truncate open=%v, error=%v", opened, err)
	}
	mode := fs.FileMode(0600)
	if _, err := session.SetNodeAttr(request, before.ID, storage.AttrChange{Mode: &mode}); storage.ErrnoOf(err) != syscall.EINTR || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled identity attributes=%v", err)
	}
	read, err := file.ReadAt(ctx, 0, 64)
	if err != nil || string(read.Data) != "preserve" || read.Attr.Mode != before.Mode || !read.Attr.ModTime.Equal(before.ModTime) {
		t.Fatalf("refused identity operations changed native state=%+v, error=%v", read, err)
	}
}
