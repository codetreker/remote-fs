package fuse

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestDirectoryHTTPReadOpenSeparatesMetadataMutationAuthorization(t *testing.T) {
	for _, writable := range []bool{false, true} {
		name := "readonly"
		if writable {
			name = "writable"
		}
		t.Run(name, func(t *testing.T) {
			_, backend := memoryfixture.New(t, "directory-authorization", 1<<20, locking.DefaultOptions())
			if err := backend.Mkdir(t.Context(), "directory"); err != nil {
				t.Fatal(err)
			}
			if err := backend.Create(t.Context(), "directory/child"); err != nil {
				t.Fatal(err)
			}
			original, err := backend.Stat(t.Context(), "directory")
			if err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			var requests []authz.AccessRequest
			options := httprest.DefaultHandlerOptions()
			options.Volume = "directory-authorization"
			options.Authorizer = authz.AuthorizerFunc(func(_ context.Context, request authz.AccessRequest) error {
				mu.Lock()
				requests = append(requests, request)
				mu.Unlock()
				if writable {
					return nil
				}
				if request.Open.Write || request.Open.Create || request.Open.Truncate || request.Open.Exclusive {
					return authz.ErrDenied
				}
				switch request.Operation {
				case storage.OpFileWrite, storage.OpFileTruncate, storage.OpFileSetAttr, storage.OpFileSetNodeAttr, storage.OpFileSetMetadata, storage.OpFileSetNodeMetadata:
					return authz.ErrDenied
				}
				return nil
			})
			handler, err := httprest.NewHandlerWithOptions(backend, nil, options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := handler.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			client, err := httprest.Dial(server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			session, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := session.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			n := namespaceRoot(&namespaceFixture{})
			n.id = rootIdentity(original.ID)
			n.volume.files = session
			opened, _, errno := n.OpendirHandle(t.Context(), syscall.O_RDONLY)
			if errno != 0 {
				t.Fatalf("readonly opendir: %v", errno)
			}
			d := opened.(*directoryHandle)
			t.Cleanup(func() { d.Releasedir(context.Background(), 0) })
			entry, errno := d.Readdirent(t.Context())
			if errno != 0 || entry == nil || entry.Name != "child" {
				t.Fatalf("readdir=%+v %v", entry, errno)
			}
			mu.Lock()
			var open storage.OpenAccess
			for _, request := range requests {
				if request.Operation == storage.OpFileOpenNodeRef {
					open = request.Open
				}
			}
			mu.Unlock()
			if open != (storage.OpenAccess{Read: true}) {
				t.Fatalf("directory open authorization=%+v", open)
			}
			chmod := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: gofuse.FATTR_MODE, Mode: 0700}}
			times := &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: gofuse.FATTR_MTIME | gofuse.FATTR_ATIME, Mtime: 123, Atime: 456}}
			var out gofuse.AttrOut
			want := syscall.EACCES
			if writable {
				want = 0
			}
			if errno := n.Setattr(t.Context(), d, chmod, &out); errno != want {
				t.Fatalf("fd chmod=%v want=%v", errno, want)
			}
			if writable && out.Mode&0777 != 0700 {
				t.Fatalf("fd mode=%o", out.Mode)
			}
			if errno := n.Setattr(t.Context(), d, times, &out); errno != want {
				t.Fatalf("fd times=%v want=%v", errno, want)
			}
			if writable && (out.Mtime != 123 || out.Atime != 456) {
				t.Fatalf("fd times=%+v", out.Attr)
			}
			if !writable {
				denied, err := session.(storage.NodeReferences).OpenNodeRef(t.Context(), original.ID, storage.NodeRefOptions{Kind: storage.NodeDirectory, Target: storage.ChildCondition{State: storage.SameNode, NodeID: original.ID}, MetadataAccess: storage.WriteMetadata})
				if !errors.Is(err, syscall.EACCES) || denied.Reference != nil {
					t.Fatalf("metadata-only write open escaped policy: %+v %v", denied, err)
				}
				unchanged, err := backend.Stat(t.Context(), "directory")
				if err != nil || len(unchanged.Metadata) != 0 || !unchanged.ModTime.Equal(original.ModTime) || !unchanged.AccessTime.Equal(original.AccessTime) {
					t.Fatalf("denied mutations changed directory: %+v %v", unchanged, err)
				}
				return
			}

			if err := backend.Remove(t.Context(), "directory/child"); err != nil {
				t.Fatal(err)
			}
			if err := backend.Rename(t.Context(), "directory", "moved"); err != nil {
				t.Fatal(err)
			}
			if err := backend.Mkdir(t.Context(), "directory"); err != nil {
				t.Fatal(err)
			}
			replacement, err := backend.Stat(t.Context(), "directory")
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.RemoveDir(t.Context(), "moved"); err != nil {
				t.Fatal(err)
			}
			chmod.Mode, times.Mtime = 0711, 789
			if errno := n.Setattr(t.Context(), d, chmod, &out); errno != 0 || out.Mode&0777 != 0711 {
				t.Fatalf("detached chmod=%v mode=%o", errno, out.Mode)
			}
			if errno := n.Setattr(t.Context(), d, times, &out); errno != 0 || out.Mtime != 789 {
				t.Fatalf("detached times=%v attr=%+v", errno, out.Attr)
			}
			retained, err := d.stat(t.Context())
			if err != nil || retained.ID != original.ID || retained.ModTime.Unix() != 789 {
				t.Fatalf("detached identity=%+v %v", retained, err)
			}
			current, err := backend.Stat(t.Context(), "directory")
			if err != nil || current.ID != replacement.ID || len(current.Metadata) != 0 || !current.ModTime.Equal(replacement.ModTime) {
				t.Fatalf("descriptor changed replacement: %+v %v", current, err)
			}
			if _, err := backend.Stat(t.Context(), "moved"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("detached mutation recreated a name: %v", err)
			}
			d.Releasedir(t.Context(), 0)
			if errno := n.Setattr(t.Context(), d, chmod, &out); errno != syscall.EBADF {
				t.Fatalf("closed chmod=%v", errno)
			}
			if errno := n.Setattr(t.Context(), d, times, &out); errno != syscall.EBADF {
				t.Fatalf("closed times=%v", errno)
			}
			t.Run("expired session", func(t *testing.T) {
				options := storage.DefaultFileSessionOptions()
				options.Lease = time.Second
				expiring, err := client.NewFileSession(t.Context(), options)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := expiring.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				n := namespaceRoot(&namespaceFixture{})
				n.id, n.volume.files = rootIdentity(replacement.ID), expiring
				opened, _, errno := n.OpendirHandle(t.Context(), syscall.O_RDONLY)
				if errno != 0 {
					t.Fatal(errno)
				}
				d := opened.(*directoryHandle)
				t.Cleanup(func() { d.Releasedir(context.Background(), 0) })
				// Waiting a full lease after enrollment also covers server clock/RTT subtraction.
				timer := time.NewTimer(options.Lease)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-t.Context().Done():
					t.Fatal(t.Context().Err())
				}
				if err := n.setPermissions(t.Context(), d, 0777); !errors.Is(err, syscall.ESTALE) {
					t.Fatalf("expired chmod=%v", err)
				}
				stamp := time.Unix(999, 0)
				if _, err := d.setAttr(t.Context(), storage.AttrChange{ModTime: &stamp}); !errors.Is(err, syscall.ESTALE) {
					t.Fatalf("expired times=%v", err)
				}
				current, err := backend.Stat(t.Context(), "directory")
				if err != nil || len(current.Metadata) != 0 || !current.ModTime.Equal(replacement.ModTime) {
					t.Fatalf("expired mutation changed directory: %+v %v", current, err)
				}
			})
		})
	}
}
