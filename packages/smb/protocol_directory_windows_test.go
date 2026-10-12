package smb_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf16"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb"
	smbwindows "github.com/codetreker/remote-fs/packages/smb/windows"
	win "golang.org/x/sys/windows"
)

func TestNativeWindowsDirectoryEnumeration(t *testing.T) {
	identity, err := smbwindows.CurrentIdentity()
	if err != nil {
		t.Fatal(err)
	}
	policy, err := smbwindows.AllowIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	limits := smb.DefaultLimits()
	limits.RequestTimeout, limits.CleanupTimeout = 10*time.Second, 10*time.Second
	server, err := smb.New(smb.Config{Authenticator: smbwindows.NewAuthenticator(), AuthorizeIdentity: policy,
		Authorize: authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return nil }),
		Limits:    limits, Logger: slog.New(slog.NewTextHandler(nativeProbeLog{t}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	share := fmt.Sprintf("rfsprobe%x", time.Now().UnixNano())
	label := "directory-probe"
	if _, err := server.Publish(smb.Share{Name: share, Volume: share, BackendVolume: "test-volume", RootNodeID: 1,
		Backend: smb.NewNativeDirectoryProbeBackend(), VolumeLabel: &label}); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	receipts := &nativeDirectoryReceipts{}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, nativeDirectoryListener{listener, receipts}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-served:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(15 * time.Second):
			t.Error("native SMB server did not stop")
		}
		stop, release := context.WithTimeout(context.Background(), 15*time.Second)
		defer release()
		if err := server.Shutdown(stop); err != nil {
			t.Error(err)
		}
	})
	drive := nativeUnusedDrive(t)
	remote := `\\127.0.0.1\` + share
	port := listener.Addr().(*net.TCPAddr).Port
	// Alternative TCP ports require Windows Server 2025 and an elevated token.
	// https://learn.microsoft.com/en-us/windows-server/storage/file-server/smb-ports
	t.Cleanup(func() { nativeMappingCommand(t, drive, remote, port, true) })
	nativeMappingCommand(t, drive, remote, port, false)

	rootNames := []string{"alpha.txt", "beta.bin", "folder", "long-😀-name.txt"}
	for _, test := range []struct {
		path  string
		names []string
	}{
		{drive + `\`, rootNames}, {drive + `\folder`, []string{"nested.txt"}},
	} {
		t.Run("find/"+test.path, func(t *testing.T) { nativeFindNames(t, strings.TrimRight(test.path, `\`)+`\*`, test.names) })
	}
	t.Run("find/pattern", func(t *testing.T) { nativeFindNames(t, drive+`\*.txt`, []string{"alpha.txt", "long-😀-name.txt"}) })
	t.Run("find/question-mark", func(t *testing.T) { nativeFindNames(t, drive+`\a????.txt`, []string{"alpha.txt"}) })
	t.Run("find/case", func(t *testing.T) { nativeFindNames(t, drive+`\ALPHA.TXT`, []string{"alpha.txt"}) })

	for _, class := range []uint32{1, 2, 3, 12, 37, 38} {
		for _, directory := range []struct {
			path  string
			names []string
		}{
			{drive + `\`, rootNames}, {drive + `\folder`, []string{"nested.txt"}},
		} {
			t.Run(fmt.Sprintf("nt-class-%d/%s", class, directory.path), func(t *testing.T) {
				h := nativeOpenDirectory(t, directory.path)
				got := nativeReadAll(t, h, class, "*", false)
				nativeWantNames(t, got, directory.names)
				nativeWantNames(t, nativeReadAll(t, h, class, "", true), directory.names)
			})
		}
	}
	t.Run("nt/single-and-class-switch", func(t *testing.T) {
		h := nativeOpenDirectory(t, drive+`\`)
		var names []string
		for i, class := range []uint32{12, 37, 1, 38, 2} {
			status, data := nativeQueryDirectory(t, h, class, "*", 4096, true, i == 0, true)
			if i == 4 {
				if status != nativeNoMoreFiles {
					t.Fatalf("exhaustion status=%#x", status)
				}
				break
			}
			if status != 0 {
				t.Fatalf("class-switch status=%#x", status)
			}
			entries := nativeDecodeDirectory(t, class, data)
			if len(entries) != 1 {
				t.Fatalf("single-entry result=%v", entries)
			}
			names = append(names, entries...)
		}
		nativeWantNames(t, names, rootNames)
	})
	for _, pattern := range []string{".", ".."} {
		t.Run("nt/literal-"+pattern, func(t *testing.T) {
			h := nativeOpenDirectory(t, drive+`\`)
			status, _ := nativeQueryDirectory(t, h, 12, pattern, 4096, false, true, false)
			if status != nativeNoSuchFile && status != nativeNoMoreFiles {
				t.Fatalf("literal %q status=%#x", pattern, status)
			}
		})
	}
	t.Run("nt/application-short-buffer", func(t *testing.T) {
		h := nativeOpenDirectory(t, drive+`\`)
		status, data := nativeQueryDirectory(t, h, 12, "long*", 15, true, true, false)
		if status != nativeBufferOverflow {
			t.Fatalf("short buffer status=%#x bytes=%d", status, len(data))
		}
		if len(data) < 12 || binary.LittleEndian.Uint32(data[8:]) != uint32(2*len(utf16.Encode([]rune("long-😀-name.txt")))) {
			t.Fatalf("partial name length lost: %x", data)
		}
		prefix := []byte{'l', 0, 'o'}
		if len(data) != 15 || !reflect.DeepEqual(data[12:], prefix) {
			t.Fatalf("partial native name prefix changed: %x", data)
		}
		// The redirector owns its own application buffer/cursor adaptation. A
		// restarted full query proves the entry remains observable after warning.
		nativeWantNames(t, nativeReadAll(t, h, 12, "", true), []string{"long-😀-name.txt"})
	})
	facts, err := receipts.snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) == 0 {
		t.Fatal("native APIs issued no QUERY_DIRECTORY requests")
	}
	for _, fact := range facts {
		t.Logf("native QUERY_DIRECTORY: class=%d flags=%#x output=%d pattern=%q", fact.class, fact.flags, fact.output, fact.pattern)
	}
	// Application information classes may be projected by the redirector from
	// another wire class; the receipts state precisely which classes reached SMB.
}

type nativeProbeLog struct{ t *testing.T }

func (w nativeProbeLog) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }

func nativeUnusedDrive(t *testing.T) string {
	t.Helper()
	mask, err := win.GetLogicalDrives()
	if err != nil {
		t.Fatal(err)
	}
	for letter := 'Z'; letter >= 'D'; letter-- {
		if mask&(1<<uint(letter-'A')) == 0 {
			return string(letter) + ":"
		}
	}
	t.Fatal("no unused drive letter for native SMB probe")
	return ""
}

func nativeMappingCommand(t *testing.T, drive, remote string, port int, remove bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	script := `$ErrorActionPreference='Stop'; New-SmbMapping -LocalPath $env:RFS_NATIVE_DRIVE -RemotePath $env:RFS_NATIVE_REMOTE -TcpPort ([int]$env:RFS_NATIVE_PORT) -Persistent $false -RequireIntegrity $true | Out-String | Write-Output`
	if remove {
		script = `$ErrorActionPreference='Stop'; $m=@(Get-SmbMapping -ErrorAction Stop | Where-Object { $_.LocalPath -ceq $env:RFS_NATIVE_DRIVE }); if ($m.Count -eq 0) { exit 0 }; if ($m.Count -ne 1 -or $m[0].RemotePath -cne $env:RFS_NATIVE_REMOTE) { throw 'Native probe mapping ownership changed' }; Remove-SmbMapping -LocalPath $env:RFS_NATIVE_DRIVE -Force -ErrorAction Stop`
	}
	command := exec.CommandContext(ctx, "powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script)
	command.Env = append(os.Environ(), "RFS_NATIVE_DRIVE="+drive, "RFS_NATIVE_REMOTE="+remote, "RFS_NATIVE_PORT="+strconv.Itoa(port))
	command.Stdout, command.Stderr = nativeProbeLog{t}, nativeProbeLog{t}
	if err := command.Run(); err != nil {
		t.Fatalf("native SMB mapping (remove=%v): %v", remove, err)
	}
}

func nativeFindNames(t *testing.T, expression string, want []string) {
	t.Helper()
	name, err := win.UTF16PtrFromString(expression)
	if err != nil {
		t.Fatal(err)
	}
	var entry win.Win32finddata
	handle, err := win.FindFirstFile(name, &entry)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := win.FindClose(handle); err != nil {
			t.Error(err)
		}
	}()
	var got []string
	for {
		got = append(got, win.UTF16ToString(entry.FileName[:]))
		if len(got) > 64 {
			t.Fatal("native enumeration exceeded fixture bound")
		}
		err := win.FindNextFile(handle, &entry)
		if err == win.ERROR_NO_MORE_FILES {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	nativeWantNames(t, got, want)
}

func nativeWantNames(t *testing.T, got, want []string) {
	t.Helper()
	got, want = append([]string(nil), got...), append([]string(nil), want...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("native names=%q want=%q", got, want)
	}
}

func nativeOpenDirectory(t *testing.T, path string) win.Handle {
	t.Helper()
	name, err := win.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := win.CreateFile(name, win.FILE_LIST_DIRECTORY|win.FILE_READ_ATTRIBUTES|win.SYNCHRONIZE,
		win.FILE_SHARE_READ|win.FILE_SHARE_WRITE|win.FILE_SHARE_DELETE, nil, win.OPEN_EXISTING, win.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := win.CloseHandle(handle); err != nil {
			t.Error(err)
		}
	})
	return handle
}

const (
	nativeNoMoreFiles    uint32 = 0x80000006
	nativeNoSuchFile     uint32 = 0xc000000f
	nativeBufferOverflow uint32 = 0x80000005
)

var nativeNTDLL = win.NewLazySystemDLL("ntdll.dll")
var nativeNTQueryDirectory = nativeNTDLL.NewProc("NtQueryDirectoryFile")
var nativeNTQueryDirectoryEx = nativeNTDLL.NewProc("NtQueryDirectoryFileEx")

func nativeQueryDirectory(t *testing.T, handle win.Handle, class uint32, pattern string, length int, single, restart, extended bool) (uint32, []byte) {
	t.Helper()
	buffer := make([]byte, length)
	var status win.IO_STATUS_BLOCK
	var name *win.NTUnicodeString
	if pattern != "" {
		var err error
		name, err = win.NewNTUnicodeString(pattern)
		if err != nil {
			t.Fatal(err)
		}
	}
	boolean := func(value bool) uintptr {
		if value {
			return 1
		}
		return 0
	}
	// CreateFile supplied a synchronous directory handle. Event/APC remain
	// NULL, so the native call owns its buffers until completion.
	// https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/nf-ntifs-ntquerydirectoryfileex
	var result uintptr
	if extended {
		flags := uint32(0)
		if restart {
			flags |= 1
		}
		if single {
			flags |= 2
		}
		result, _, _ = nativeNTQueryDirectoryEx.Call(uintptr(handle), 0, 0, 0,
			uintptr(unsafe.Pointer(&status)), uintptr(unsafe.Pointer(&buffer[0])), uintptr(length), uintptr(class), uintptr(flags), uintptr(unsafe.Pointer(name)))
	} else {
		result, _, _ = nativeNTQueryDirectory.Call(uintptr(handle), 0, 0, 0,
			uintptr(unsafe.Pointer(&status)), uintptr(unsafe.Pointer(&buffer[0])), uintptr(length), uintptr(class),
			boolean(single), uintptr(unsafe.Pointer(name)), boolean(restart))
	}
	runtime.KeepAlive(name)
	runtime.KeepAlive(buffer)
	runtime.KeepAlive(status)
	if uint32(result) == 0x103 {
		t.Fatal("synchronous directory query returned STATUS_PENDING")
	}
	if status.Information > uintptr(length) {
		t.Fatalf("native query exceeded output capacity: %d > %d", status.Information, length)
	}
	return uint32(result), buffer[:int(status.Information)]
}

func nativeReadAll(t *testing.T, handle win.Handle, class uint32, pattern string, restart bool) []string {
	t.Helper()
	var names []string
	for call := 0; call < 64; call++ {
		expression := ""
		if call == 0 {
			expression = pattern
		}
		status, data := nativeQueryDirectory(t, handle, class, expression, 4096, false, call == 0 && restart, false)
		if status == nativeNoMoreFiles {
			return names
		}
		if status != 0 {
			t.Fatalf("native class %d query status=%#x output=%x", class, status, data)
		}
		entries := nativeDecodeDirectory(t, class, data)
		if len(entries) == 0 {
			t.Fatal("successful native enumeration returned no records")
		}
		names = append(names, entries...)
	}
	t.Fatal("native enumeration exceeded bounded call count")
	return nil
}

func nativeDecodeDirectory(t *testing.T, class uint32, data []byte) []string {
	t.Helper()
	header, nameLength := 0, 60
	switch class {
	case 1:
		header = 64
	case 2:
		header = 68
	case 3:
		header = 94
	case 12:
		header, nameLength = 12, 8
	case 37:
		header = 104
	case 38:
		header = 80
	default:
		t.Fatalf("unexpected native class %d", class)
	}
	var names []string
	for offset := 0; ; {
		if offset > len(data)-header {
			t.Fatalf("truncated native class %d record: offset=%d bytes=%d", class, offset, len(data))
		}
		record := data[offset:]
		length := int(binary.LittleEndian.Uint32(record[nameLength:]))
		if length < 2 || length%2 != 0 || length > len(record)-header {
			t.Fatalf("invalid native name extent: class=%d bytes=%d", class, length)
		}
		units := make([]uint16, length/2)
		for i := range units {
			units[i] = binary.LittleEndian.Uint16(record[header+2*i:])
		}
		name := string(utf16.Decode(units))
		if class != 12 {
			attributes := binary.LittleEndian.Uint32(record[56:])
			if attributes == 0 || binary.LittleEndian.Uint64(record[8:]) == 0 {
				t.Fatalf("native rich attributes/times absent for %q", name)
			}
			if (name == "folder") != (attributes&0x10 != 0) {
				t.Fatalf("native directory kind changed for %q: %#x", name, attributes)
			}
			if class == 3 || class == 37 {
				if record[68] != 0 {
					t.Fatalf("native short name unexpectedly present for %q", name)
				}
			}
			if class == 37 && binary.LittleEndian.Uint64(record[96:]) == 0 || class == 38 && binary.LittleEndian.Uint64(record[72:]) == 0 {
				t.Fatalf("native ID missing for %q", name)
			}
		}
		names = append(names, name)
		next := int(binary.LittleEndian.Uint32(record))
		if next == 0 {
			return names
		}
		if next%8 != 0 || next < header+length || next > len(record)-header {
			t.Fatalf("invalid native record chain class=%d next=%d", class, next)
		}
		offset += next
		if len(names) > 64 {
			t.Fatal("native record count exceeded fixture bound")
		}
	}
}

type nativeDirectoryFact struct {
	class, flags byte
	output       uint32
	pattern      string
}
type nativeDirectoryReceipts struct {
	mu    sync.Mutex
	facts []nativeDirectoryFact
	err   error
}

func (r *nativeDirectoryReceipts) record(packet []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for offset, commands := 0, 0; offset <= len(packet)-64; commands++ {
		if commands >= 32 {
			r.err = fmt.Errorf("native compound trace exceeds 32 commands")
			return
		}
		h := packet[offset:]
		if string(h[:4]) != "\xfeSMB" {
			return
		}
		next := int(binary.LittleEndian.Uint32(h[20:]))
		end := len(packet)
		if next != 0 {
			if next < 64 || next > len(packet)-offset {
				r.err = fmt.Errorf("invalid native compound extent")
				return
			}
			end = offset + next
		}
		if binary.LittleEndian.Uint16(h[12:]) == 14 {
			if len(r.facts) >= 512 {
				r.err = fmt.Errorf("native directory trace exceeds 512 requests")
				return
			}
			if binary.LittleEndian.Uint32(h[16:])&8 == 0 {
				r.err = fmt.Errorf("native directory request is unsigned")
				return
			}
			body := packet[offset+64 : end]
			if len(body) < 32 {
				r.err = io.ErrUnexpectedEOF
				return
			}
			start := int(binary.LittleEndian.Uint16(body[24:]))
			length := int(binary.LittleEndian.Uint16(body[26:]))
			fact := nativeDirectoryFact{class: body[2], flags: body[3], output: binary.LittleEndian.Uint32(body[28:])}
			if length != 0 {
				if length%2 != 0 || length > 510 || start < 96 || start+length > end-offset {
					r.err = fmt.Errorf("invalid native directory pattern extent")
					return
				}
				units := make([]uint16, length/2)
				for i := range units {
					units[i] = binary.LittleEndian.Uint16(h[start+2*i:])
				}
				fact.pattern = string(utf16.Decode(units))
			}
			r.facts = append(r.facts, fact)
		}
		if next == 0 {
			return
		}
		offset += next
	}
}

func (r *nativeDirectoryReceipts) snapshot() ([]nativeDirectoryFact, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]nativeDirectoryFact(nil), r.facts...), r.err
}

type nativeDirectoryListener struct {
	net.Listener
	receipts *nativeDirectoryReceipts
}

func (l nativeDirectoryListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &nativeDirectoryConnection{Conn: c, receipts: l.receipts}, nil
}

type nativeDirectoryConnection struct {
	net.Conn
	receipts *nativeDirectoryReceipts
	frame    []byte
	length   int
}

func (c *nativeDirectoryConnection) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	input := p[:n]
	for len(input) > 0 {
		needed := 4 - len(c.frame)
		if len(c.frame) >= 4 {
			needed = c.length + 4 - len(c.frame)
		}
		take := min(needed, len(input))
		c.frame = append(c.frame, input[:take]...)
		input = input[take:]
		if len(c.frame) == 4 && c.length == 0 {
			c.length = int(c.frame[1])<<16 | int(c.frame[2])<<8 | int(c.frame[3])
			if c.frame[0] != 0 || c.length < 4 || c.length > 2<<20 {
				return n, fmt.Errorf("invalid native SMB frame length: %d", c.length)
			}
		}
		if c.length != 0 && len(c.frame) == c.length+4 {
			c.receipts.record(c.frame[4:])
			clear(c.frame)
			c.frame, c.length = c.frame[:0], 0
		}
	}
	return n, err
}
