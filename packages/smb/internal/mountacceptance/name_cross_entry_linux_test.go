//go:build linux && rfs_mountacceptance

package mountacceptance

import (
	"context"
	"encoding/binary"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/smb"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func nameCrossEntryFixture(t *testing.T, transport string) (*objectstore.Storage, string, *protocolClient, *smb.Server) {
	t.Helper()
	_, backing := memoryfixture.New(t, "name-cross-entry", 1<<20, locking.DefaultOptions())
	var smbBackend, fuseBackend storage.FileStorage = backing, backing
	if transport == "http" {
		handler, err := httprest.NewHandler(backing, nil)
		if err != nil {
			t.Fatal(err)
		}
		endpoint := httptest.NewServer(handler)
		t.Cleanup(func() {
			endpoint.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := handler.Close(ctx); err != nil {
				t.Errorf("close name authority: %v", err)
			}
		})
		smbBackend, fuseBackend = dialBackend(t, endpoint.URL), dialBackend(t, endpoint.URL)
	}
	point := t.TempDir()
	mount, err := fuse.New(point, fuseBackend, fuse.Options{})
	if mount != nil {
		t.Cleanup(func() {
			if err := mount.Unmount(); err != nil {
				t.Errorf("unmount name filesystem: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	var server *smb.Server
	client := startSMBObserved(t, smbBackend, func(value *smb.Server) { server = value })
	return backing, point, client, server
}

func openNameDescriptor(t *testing.T, path string, flags int) (*os.File, func()) {
	t.Helper()
	held, err := os.OpenFile(path, flags, 0)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	close := func() {
		if !closed {
			closed = true
			if err := held.Close(); err != nil {
				t.Errorf("close name descriptor: %v", err)
			}
		}
	}
	t.Cleanup(close)
	return held, close
}

func (c *protocolClient) openName(t *testing.T, name string, access, share uint32, directory bool) (wire.FileID, func()) {
	t.Helper()
	encoded := wire.EncodeUTF16(name)
	body := make([]byte, max(57, 56+len(encoded)))
	binary.LittleEndian.PutUint16(body, 57)
	binary.LittleEndian.PutUint32(body[4:], 2)
	binary.LittleEndian.PutUint32(body[24:], access)
	binary.LittleEndian.PutUint32(body[32:], share)
	binary.LittleEndian.PutUint32(body[36:], 1)
	if directory {
		binary.LittleEndian.PutUint32(body[40:], 1)
	}
	binary.LittleEndian.PutUint16(body[44:], 120)
	binary.LittleEndian.PutUint16(body[46:], uint16(len(encoded)))
	copy(body[56:], encoded)
	result := c.call(t, wire.Create, body, 0)
	if len(result) != 88 {
		t.Fatalf("invalid name CREATE response %x", result)
	}
	var id wire.FileID
	copy(id[:], result[64:80])
	closed := false
	close := func() {
		if !closed {
			closed = true
			body := make([]byte, 24)
			binary.LittleEndian.PutUint16(body, 24)
			copy(body[8:], id[:])
			c.call(t, wire.Close, body, 0)
		}
	}
	t.Cleanup(close)
	return id, close
}

func renameNameInformation(name string, replace bool) []byte {
	encoded := wire.EncodeUTF16(name)
	data := make([]byte, 20+len(encoded))
	if replace {
		data[0] = 1
	}
	binary.LittleEndian.PutUint32(data[16:], uint32(len(encoded)))
	copy(data[20:], encoded)
	return data
}

func (c *protocolClient) normalizedName(t *testing.T, id wire.FileID) string {
	t.Helper()
	body := make([]byte, 41)
	binary.LittleEndian.PutUint16(body, 41)
	body[2], body[3] = wire.InfoFile, wire.FileNormalizedNameInformation
	binary.LittleEndian.PutUint32(body[4:], 4096)
	copy(body[24:40], id[:])
	result := c.call(t, wire.QueryInfo, body, 0)
	if len(result) < 12 || binary.LittleEndian.Uint32(result[4:]) != uint32(len(result)-8) || binary.LittleEndian.Uint32(result[8:]) != uint32(len(result)-12) {
		t.Fatalf("invalid normalized name response %x", result)
	}
	name, err := wire.DecodeUTF16(result[12:])
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func TestSMBNameReplacementPreservesMountedDescriptors(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			backing, point, client, _ := nameCrossEntryFixture(t, transport)
			for name, contents := range map[string]string{"source": "original", "target": "old-data"} {
				if err := os.WriteFile(filepath.Join(point, name), []byte(contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			source, _ := openNameDescriptor(t, filepath.Join(point, "source"), os.O_RDWR)
			target, closeTarget := openNameDescriptor(t, filepath.Join(point, "target"), os.O_RDWR)
			sourceStat, targetStat := descriptorStat(t, source), descriptorStat(t, target)
			if sourceStat.Ino == targetStat.Ino {
				t.Fatal("distinct source and target share an inode")
			}
			id, _ := client.openName(t, "source", 0x10000|0x1|0x2, 7, false)
			client.set(t, id, wire.FileRenameInformation, renameNameInformation("renamed", false), 0)
			if got := client.normalizedName(t, id); got != `\renamed` {
				t.Fatalf("current SMB name=%q", got)
			}
			if _, err := os.Stat(filepath.Join(point, "source")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old source name survived rename: %v", err)
			}
			client.set(t, id, wire.FileRenameInformation, renameNameInformation("target", true), 0)
			checkDescriptor(t, source, sourceStat.Ino, sourceStat.Mode, []byte("original"))
			checkDescriptor(t, target, targetStat.Ino, targetStat.Mode, []byte("old-data"))
			if got := binary.LittleEndian.Uint64(client.query(t, id, wire.FileInternalInformation)); got != sourceStat.Ino {
				t.Fatalf("SMB replacement retargeted original handle: %d", got)
			}
			if err := os.WriteFile(filepath.Join(point, "source"), []byte("new-name"), 0600); err != nil {
				t.Fatal(err)
			}
			writeAt(t, source, []byte("updated!"), 0)
			writeAt(t, target, []byte("detached"), 0)
			checkDescriptor(t, source, sourceStat.Ino, sourceStat.Mode, []byte("updated!"))
			checkDescriptor(t, target, targetStat.Ino, targetStat.Mode, []byte("detached"))
			if data := client.read(t, id, 9); string(data) != "updated!" || client.normalizedName(t, id) != `\target` {
				t.Fatalf("SMB original handle lost mounted mutation: %q", data)
			}
			for name, want := range map[string]string{"target": "updated!", "source": "new-name"} {
				data, err := os.ReadFile(filepath.Join(point, name))
				if err != nil || string(data) != want {
					t.Fatalf("mounted %s=%q, %v; want=%q", name, data, err, want)
				}
			}
			before, err := backing.Space(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			closeTarget()
			deadline := time.Now().Add(time.Second)
			for {
				after, err := backing.Space(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if after.Used == before.Used-4096 {
					break
				}
				if after.Used != before.Used || time.Now().After(deadline) {
					t.Fatalf("detached target release quota before=%+v after=%+v", before, after)
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}

func TestSMBDirectoryRenameCountsMountedDescendantsAndPreservesRawRename(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		for _, heldKind := range []string{"data", "directory"} {
			t.Run(transport+"/"+heldKind, func(t *testing.T) {
				_, point, client, server := nameCrossEntryFixture(t, transport)
				if err := os.MkdirAll(filepath.Join(point, "parent", "child"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(point, "parent", "child", "file"), []byte("body"), 0600); err != nil {
					t.Fatal(err)
				}
				heldPath := filepath.Join(point, "parent", "child", "file")
				if heldKind == "directory" {
					heldPath = filepath.Dir(heldPath)
				}
				held, closeHeld := openNameDescriptor(t, heldPath, os.O_RDONLY)
				if heldKind == "directory" {
					entries, err := held.ReadDir(-1)
					if err != nil || len(entries) != 1 || entries[0].Name() != "file" {
						t.Fatalf("mounted descendant directory=%v, %v", entries, err)
					}
				}
				seed := descriptorStat(t, held)
				id, closeDirectory := client.openName(t, "parent", 0x10000, 7, true)
				directoryID := binary.LittleEndian.Uint64(client.query(t, id, wire.FileInternalInformation))
				client.set(t, id, wire.FileRenameInformation, renameNameInformation("windows", false), 0xc0000022)
				if _, err := os.Stat(filepath.Join(point, "windows")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("refused SMB rename created destination: %v", err)
				}
				if err := os.Rename(filepath.Join(point, "parent"), filepath.Join(point, "linux")); err != nil {
					t.Fatalf("Linux raw rename inherited SMB descendant restriction: %v", err)
				}
				if got := client.normalizedName(t, id); got != `\linux` {
					t.Fatalf("original directory reference did not follow Linux rename: %q", got)
				}
				if got := descriptorStat(t, held); got.Ino != seed.Ino || got.Mode != seed.Mode {
					t.Fatalf("raw rename retargeted descendant: %+v -> %+v", seed, got)
				}
				if heldKind == "data" {
					checkDescriptor(t, held, seed.Ino, seed.Mode, []byte("body"))
				}
				client.set(t, id, wire.FileRenameInformation, renameNameInformation("windows", false), 0xc0000022)
				if _, err := os.Stat(filepath.Join(point, "windows")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("refused current-name rename created destination: %v", err)
				}
				closeHeld()
				client.set(t, id, wire.FileRenameInformation, renameNameInformation("windows", false), 0)
				closeDirectory()
				_, closeRestricted := client.openName(t, "windows", 0x10000, 3, true)
				if err := os.Rename(filepath.Join(point, "windows"), filepath.Join(point, "blocked")); !errors.Is(err, syscall.EAGAIN) {
					t.Fatalf("Linux raw rename bypassed source sharing: %v", err)
				}
				closeRestricted()
				if err := os.Rename(filepath.Join(point, "windows"), filepath.Join(point, "released")); err != nil {
					t.Fatalf("released source share continued blocking raw rename: %v", err)
				}
				if data, err := os.ReadFile(filepath.Join(point, "released", "child", "file")); err != nil || string(data) != "body" {
					t.Fatalf("directory moves changed descendant bytes: %q, %v", data, err)
				}
				status := server.Status()
				if status.Handles != 0 || status.RetainedWriteBytes != 0 || status.RetainedDirectoryBytes != 0 || len(status.WriteFailures) != 2 {
					t.Fatalf("closed name owners did not settle exact refusals: %+v", status)
				}
				owners := make([]smb.WriteOwnerID, 0, 2)
				for _, failure := range status.WriteFailures {
					if failure.Operation != storage.OpFileMutateName || failure.NodeID != directoryID || failure.FileGeneration != binary.LittleEndian.Uint64(id[8:]) || failure.Action == "" || failure.Execution != smb.WriteExecutionNotExecuted || failure.ErrorCategory != smb.WriteErrorDenied || failure.Response != smb.ResponseSent || !failure.ResponseFinal || !failure.Terminal || !failure.ChainSettled || !failure.PayloadReleased {
						t.Fatalf("refused rename has unsettled or unrelated fact: %+v", failure)
					}
					owners = append(owners, failure.Owner)
				}
				if err := server.AcknowledgeWriteFailures(owners); err != nil {
					t.Fatalf("acknowledge exact terminal rename refusals: %v", err)
				}
			})
		}
	}
}
