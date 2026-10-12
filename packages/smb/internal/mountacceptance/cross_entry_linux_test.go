//go:build linux && rfs_mountacceptance

package mountacceptance

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/fuse"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

const (
	dosReadOnly = uint32(0x01)
	dosHidden   = uint32(0x02)
	dosSystem   = uint32(0x04)
	dosArchive  = uint32(0x20)
)

func TestSMBAndFUSEShareFactsAndPreservePlatformMetadata(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			_, backing := memoryfixture.New(t, "cross-entry", 1<<20, locking.DefaultOptions())
			var smbBackend, fuseBackend storage.FileStorage = backing, backing
			if transport == "http" {
				handler, err := httprest.NewHandler(backing, nil)
				if err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(handler)
				t.Cleanup(func() {
					server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					if err := handler.Close(ctx); err != nil {
						t.Errorf("close HTTP authority: %v", err)
					}
				})
				// Separate HTTP clients and native sessions keep the two entry points
				// from sharing any client-side observation or file-reference state.
				smbBackend = dialBackend(t, server.URL)
				fuseBackend = dialBackend(t, server.URL)
			}
			mountpoint := t.TempDir()
			mount, err := fuse.New(mountpoint, fuseBackend, fuse.Options{})
			if mount != nil {
				t.Cleanup(func() {
					if err := mount.Unmount(); err != nil {
						t.Errorf("unmount cross-entry filesystem: %v", err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			client := startSMB(t, smbBackend)
			held, err := os.OpenFile(filepath.Join(mountpoint, "file"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0640)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := held.Close(); err != nil {
					t.Errorf("close held FUSE descriptor: %v", err)
				}
			})
			writeAt(t, held, []byte("abcdef"), 0)
			seed := descriptorStat(t, held)
			id := client.open(t, "file")
			identity := client.query(t, id, wire.FileIdInformation)
			if binary.LittleEndian.Uint64(client.query(t, id, wire.FileInternalInformation)) != seed.Ino || binary.LittleEndian.Uint64(identity[8:]) != seed.Ino {
				t.Fatal("SMB and FUSE report different object identities")
			}

			birth := time.Unix(1700000000, 100)
			access := time.Unix(1700000001, 200)
			modified := time.Unix(1700000002, 300)
			client.set(t, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{
				CreationTime: filetime(birth), LastAccessTime: filetime(access), LastWriteTime: filetime(modified), Attributes: dosHidden | dosSystem,
			}), 0)
			stat := descriptorStat(t, held)
			if stat.Ino != seed.Ino || stat.Mode != seed.Mode || stat.Atim != (syscall.Timespec{Sec: access.Unix(), Nsec: int64(access.Nanosecond())}) || stat.Mtim != (syscall.Timespec{Sec: modified.Unix(), Nsec: int64(modified.Nanosecond())}) {
				t.Fatalf("SMB Basic changed POSIX identity/mode or lost shared times: seed=%+v stat=%+v", seed, stat)
			}
			basic := queryBasic(t, client, id)
			if basic.CreationTime != filetime(birth) || basic.LastAccessTime != filetime(access) || basic.LastWriteTime != filetime(modified) || basic.Attributes != dosHidden|dosSystem {
				t.Fatalf("Basic did not preserve exact shared times and Windows flags: %+v", basic)
			}

			for _, step := range []struct {
				size int64
				want []byte
			}{{3, []byte("abc")}, {8, []byte("abc\x00\x00\x00\x00\x00")}} {
				client.set(t, id, wire.FileEndOfFileInformation, binary.LittleEndian.AppendUint64(nil, uint64(step.size)), 0)
				checkDescriptor(t, held, seed.Ino, seed.Mode, step.want)
				if !bytes.Equal(client.query(t, id, wire.FileIdInformation), identity) || queryBasic(t, client, id).Attributes != dosHidden|dosSystem|dosArchive {
					t.Fatal("SMB EOF changed identity or lost its atomic ARCHIVE effect")
				}
			}

			client.set(t, id, wire.FileBasicInformation, wire.FileBasicInformationBody(wire.FileBasicInfo{Attributes: dosReadOnly | dosHidden | dosSystem}), 0)
			if err := held.Chmod(0600); err != nil {
				t.Fatal(err)
			}
			checkWindowsFlags(t, client, id)
			writeAt(t, held, []byte("LINUX"), 0)
			checkWindowsFlags(t, client, id)
			checkSMBSize(t, client, id, 8)
			if err := held.Truncate(2); err != nil {
				t.Fatal(err)
			}
			checkWindowsFlags(t, client, id)
			checkSMBSize(t, client, id, 2)
			if err := held.Truncate(7); err != nil {
				t.Fatal(err)
			}
			checkWindowsFlags(t, client, id)
			want := []byte("LI\x00\x00\x00\x00\x00")
			checkDescriptor(t, held, seed.Ino, syscall.S_IFREG|0600, want)
			stat = descriptorStat(t, held)
			basic = queryBasic(t, client, id)
			standard := client.query(t, id, wire.FileStandardInformation)
			network := client.query(t, id, wire.FileNetworkOpenInformation)
			if basic.Attributes != dosReadOnly|dosHidden|dosSystem || basic.LastWriteTime != filetime(time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec)) || basic.LastWriteTime == filetime(modified) || binary.LittleEndian.Uint64(standard[8:]) != 7 || binary.LittleEndian.Uint64(network[40:]) != 7 || binary.LittleEndian.Uint32(network[48:]) != basic.Attributes {
				t.Fatalf("FUSE mutations lost shared facts or changed Windows flags: Basic=%+v Standard=%x NetworkOpen=%x stat=%+v", basic, standard, network, stat)
			}
			if got := client.read(t, id, len(want)+1); !bytes.Equal(got, want) || !bytes.Equal(client.query(t, id, wire.FileIdInformation), identity) {
				t.Fatalf("SMB held handle did not see FUSE bytes and stable identity: %x", got)
			}
			client.set(t, id, wire.FileEndOfFileInformation, binary.LittleEndian.AppendUint64(nil, 1), 0xc0000022)
			checkDescriptor(t, held, seed.Ino, syscall.S_IFREG|0600, want)
		})
	}
}

func dialBackend(t *testing.T, url string) storage.FileStorage {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{}, Timeout: 5 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	backend, err := httprest.Dial(url, client)
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func descriptorStat(t *testing.T, held *os.File) syscall.Stat_t {
	t.Helper()
	info, err := held.Stat()
	if err != nil {
		t.Fatal(err)
	}
	return *info.Sys().(*syscall.Stat_t)
}

func checkDescriptor(t *testing.T, held *os.File, inode uint64, mode uint32, want []byte) {
	t.Helper()
	data := make([]byte, len(want)+1)
	n, err := held.ReadAt(data, 0)
	if n != len(want) || !errors.Is(err, io.EOF) || !bytes.Equal(data[:n], want) {
		t.Fatalf("held FUSE descriptor read=%x count=%d error=%v; want=%x and EOF", data[:n], n, err, want)
	}
	stat := descriptorStat(t, held)
	if stat.Ino != inode || stat.Mode != mode || stat.Size != int64(len(want)) || stat.Blocks != 8 {
		t.Fatalf("held FUSE descriptor stat=%+v; want inode=%d mode=%o size=%d blocks=8", stat, inode, mode, len(want))
	}
}

func writeAt(t *testing.T, held *os.File, data []byte, offset int64) {
	t.Helper()
	if n, err := held.WriteAt(data, offset); err != nil || n != len(data) {
		t.Fatalf("FUSE write offset=%d count=%d error=%v; want count=%d", offset, n, err, len(data))
	}
}

func queryBasic(t *testing.T, client *protocolClient, id wire.FileID) wire.FileBasicInfo {
	t.Helper()
	info, err := wire.ParseFileBasicInformation(client.query(t, id, wire.FileBasicInformation))
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func checkWindowsFlags(t *testing.T, client *protocolClient, id wire.FileID) {
	t.Helper()
	if info := queryBasic(t, client, id); info.Attributes != dosReadOnly|dosHidden|dosSystem {
		t.Fatalf("FUSE operation changed Windows-only attributes: %+v", info)
	}
}

func checkSMBSize(t *testing.T, client *protocolClient, id wire.FileID, size uint64) {
	t.Helper()
	if info := client.query(t, id, wire.FileStandardInformation); binary.LittleEndian.Uint64(info[8:]) != size || binary.LittleEndian.Uint64(info) != 4096 {
		t.Fatalf("SMB query after FUSE operation did not see size=%d and allocation=4096: %x", size, info)
	}
}

func filetime(instant time.Time) int64 {
	return (instant.Unix()+11644473600)*10000000 + int64(instant.Nanosecond()/100)
}
