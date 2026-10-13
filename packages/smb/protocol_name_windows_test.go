package smb_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	smbwindows "github.com/codetreker/remote-fs/packages/smb/windows"
	"github.com/codetreker/remote-fs/packages/storage"
	win "golang.org/x/sys/windows"
)

func TestNativeWindowsNameMutation(t *testing.T) {
	drive, backend, receipts := nativeNameProbe(t)
	const deleteAccess = uint32(0x10000)
	open := func(t *testing.T, leaf string, access uint32, disposition uint32, flags uint32) *nativeDeleteHandle {
		return nativeDeleteOpen(t, drive+`\`+leaf, access, 7, disposition, flags)
	}
	t.Run("rename-case-move-current-binding", func(t *testing.T) {
		h := open(t, "alpha.txt", deleteAccess|win.FILE_READ_ATTRIBUTES, win.OPEN_EXISTING, 0)
		before := nativeNameIdentity(t, h.Handle)
		for _, test := range []struct {
			target, parentName string
			parent             uint64
		}{
			{"renamed.txt", "renamed.txt", 1}, {"RENAMED.txt", "RENAMED.txt", 1}, {`folder\moved.txt`, "moved.txt", 2},
		} {
			defer receipts.begin(t, wire.SetInfo, wire.FileRenameInformation)()
			receipts.expectRename(test.target, false)
			if err := nativeRenameFile(h.Handle, drive+`\`+test.target, false); err != nil {
				t.Fatal(err)
			}
			receipts.finish(t, 0, true)
			nativeAuthorityName(t, backend, test.parent, test.parentName, before, true)
			if after := nativeNameIdentity(t, h.Handle); after != before {
				t.Fatalf("rename changed original native object identity: %d -> %d", before, after)
			}
		}
	})
	t.Run("replacement-keeps-old-target-reference", func(t *testing.T) {
		source := open(t, "source.txt", deleteAccess|win.FILE_READ_ATTRIBUTES, win.CREATE_NEW, 0)
		target := open(t, "target.txt", win.GENERIC_READ|win.FILE_READ_ATTRIBUTES, win.CREATE_NEW, 0)
		sourceID, targetID := nativeNameIdentity(t, source.Handle), nativeNameIdentity(t, target.Handle)
		defer receipts.begin(t, wire.SetInfo, wire.FileRenameInformation)()
		receipts.expectRename("target.txt", true)
		if err := nativeRenameFile(source.Handle, drive+`\target.txt`, true); err != nil {
			t.Fatal(err)
		}
		receipts.finish(t, 0, true)
		nativeAuthorityName(t, backend, 1, "target.txt", sourceID, true)
		nativeAuthorityName(t, backend, 1, "source.txt", sourceID, false)
		if nativeNameIdentity(t, target.Handle) != targetID || sourceID == targetID {
			t.Fatal("replacement reassigned the old target reference identity")
		}
		var data [1]byte
		var count uint32
		if err := win.ReadFile(target.Handle, data[:], &count, nil); err != nil || count != 0 {
			t.Fatalf("old replacement target read=%d error=%v", count, err)
		}
	})
	t.Run("readonly-source-rename-and-target-denial", func(t *testing.T) {
		source := open(t, "readonly-source.txt", deleteAccess|win.FILE_READ_ATTRIBUTES, win.CREATE_NEW, win.FILE_ATTRIBUTE_READONLY)
		sourceID := nativeNameIdentity(t, source.Handle)
		defer receipts.begin(t, wire.SetInfo, wire.FileRenameInformation)()
		receipts.expectRename("readonly-moved.txt", false)
		if err := nativeRenameFile(source.Handle, drive+`\readonly-moved.txt`, false); err != nil {
			t.Fatal(err)
		}
		receipts.finish(t, 0, true)
		nativeAuthorityName(t, backend, 1, "readonly-moved.txt", sourceID, true)
		target := open(t, "readonly-target.txt", win.FILE_READ_ATTRIBUTES, win.CREATE_NEW, win.FILE_ATTRIBUTE_READONLY)
		targetID := nativeNameIdentity(t, target.Handle)
		defer receipts.begin(t, wire.SetInfo, wire.FileRenameInformation)()
		receipts.expectRename("readonly-target.txt", true)
		if err := nativeRenameFile(source.Handle, drive+`\readonly-target.txt`, true); !errors.Is(err, win.ERROR_ACCESS_DENIED) {
			t.Fatalf("readonly replacement error=%v", err)
		}
		receipts.finish(t, 0xc0000022, true)
		nativeAuthorityName(t, backend, 1, "readonly-moved.txt", sourceID, true)
		nativeAuthorityName(t, backend, 1, "readonly-target.txt", targetID, true)
	})
	t.Run("directory-open-descendant-denial", func(t *testing.T) {
		directory := open(t, "folder", deleteAccess|win.FILE_READ_ATTRIBUTES, win.OPEN_EXISTING, win.FILE_FLAG_BACKUP_SEMANTICS)
		child := open(t, `folder\nested.txt`, win.FILE_READ_ATTRIBUTES, win.OPEN_EXISTING, 0)
		defer receipts.begin(t, wire.SetInfo, wire.FileRenameInformation)()
		receipts.expectRename("folder-moved", false)
		if err := nativeRenameFile(directory.Handle, drive+`\folder-moved`, false); !errors.Is(err, win.ERROR_ACCESS_DENIED) {
			t.Fatalf("directory with open descendant rename error=%v", err)
		}
		receipts.finish(t, 0xc0000022, true)
		nativeAuthorityName(t, backend, 1, "folder", 2, true)
		nativeNameClose(t, child, receipts)
		defer receipts.begin(t, wire.SetInfo, wire.FileRenameInformation)()
		receipts.expectRename("folder-moved", false)
		if err := nativeRenameFile(directory.Handle, drive+`\folder-moved`, false); err != nil {
			t.Fatal(err)
		}
		receipts.finish(t, 0, true)
		nativeAuthorityName(t, backend, 1, "folder-moved", 2, true)
	})
	t.Run("ordinary-disposition-delete-only-clear-and-delete", func(t *testing.T) {
		h := open(t, "ordinary.txt", deleteAccess, win.CREATE_NEW, 0)
		id := nativeAuthorityName(t, backend, 1, "ordinary.txt", 0, true)
		for _, deleting := range []bool{true, false, true} {
			defer receipts.begin(t, wire.SetInfo, wire.FileDispositionInformation)()
			if err := nativeDisposition(h.Handle, deleting); err != nil {
				t.Fatal(err)
			}
			receipts.finish(t, 0, true)
			nativeAuthorityPending(t, backend, id, deleting)
		}
		nativeNameClose(t, h, receipts)
		nativeAuthorityName(t, backend, 1, "ordinary.txt", id, false)
	})
	t.Run("supersede-same-object-new-fileid-without-data-rights", func(t *testing.T) {
		old := open(t, "supersede.txt", win.FILE_READ_ATTRIBUTES, win.OPEN_EXISTING, 0)
		id := nativeNameIdentity(t, old.Handle)
		probe := backend.(interface {
			ProbeNodeState(uint64) storage.ReferenceState
		})
		before := probe.ProbeNodeState(id)
		if before.Attr.Size != 9 || before.Attr.AllocationSize != 4096 {
			t.Fatalf("supersede authority initial content facts=%+v", before.Attr)
		}
		defer receipts.begin(t, wire.Create, 0)()
		h := nativeSupersede(t, drive+`\supersede.txt`, deleteAccess)
		facts := receipts.finish(t, 0, true)
		if len(facts) != 1 || facts[0].createAction != 0 || facts[0].fileID == (wire.FileID{}) || facts[0].desiredAccess&(win.GENERIC_READ|win.GENERIC_WRITE|win.FILE_READ_DATA|win.FILE_WRITE_DATA) != 0 {
			t.Fatalf("native supersede signed CREATE facts=%+v", facts)
		}
		nativeAuthorityName(t, backend, 1, "supersede.txt", id, true)
		after := probe.ProbeNodeState(id)
		if after.Attr.Size != 0 || after.Attr.AllocationSize != 0 || !after.Attr.AllocationKnown || after.Attr.BirthTime == nil || !after.Attr.BirthTime.Equal(*before.Attr.BirthTime) {
			t.Fatalf("supersede authority reset facts=%+v", after.Attr)
		}
		if nativeNameIdentity(t, old.Handle) != id {
			t.Fatal("supersede changed old object's identity")
		}
		var count uint32
		var data [1]byte
		if err := win.ReadFile(h.Handle, data[:], &count, nil); !errors.Is(err, win.ERROR_ACCESS_DENIED) {
			t.Fatalf("delete-only supersede read error=%v", err)
		}
		if err := win.WriteFile(h.Handle, data[:], &count, nil); !errors.Is(err, win.ERROR_ACCESS_DENIED) {
			t.Fatalf("delete-only supersede write error=%v", err)
		}
		defer receipts.begin(t, wire.Create, 0)()
		second := nativeSupersede(t, drive+`\supersede.txt`, deleteAccess)
		secondFacts := receipts.finish(t, 0, true)
		if len(secondFacts) != 1 || secondFacts[0].fileID == facts[0].fileID {
			t.Fatal("supersede reused per-open SMB FileId")
		}
		_ = second
	})
	t.Run("normalized-name-full-minimum-overflow", func(t *testing.T) {
		h := open(t, "long-😀-name.txt", deleteAccess, win.OPEN_EXISTING, 0)
		want := wire.EncodeUTF16(`\long-😀-name.txt`)
		for _, length := range []int{512, 7, 8} {
			defer receipts.begin(t, wire.QueryInfo, wire.FileNormalizedNameInformation)()
			status, data := nativeNameQuery(t, h.Handle, uint32(wire.FileNormalizedNameInformation), length)
			facts := receipts.finish(t, 0, false)
			if length == 7 {
				if status != 0xc0000004 || len(data) != 0 {
					t.Fatalf("minimum native normalized query status=%#x bytes=%d", status, len(data))
				}
				continue
			}
			if length == 512 {
				if status != 0 || len(data) != 4+len(want) || !bytes.Equal(data[4:], want) || binary.LittleEndian.Uint32(data) != uint32(len(want)) {
					t.Fatalf("native normalized output status=%#x bytes=%x", status, data)
				}
				if len(facts) == 0 {
					t.Fatal("native normalized full query issued no class48 SMB exchange")
				}
			} else if status != 0x80000005 || len(data) != length || binary.LittleEndian.Uint32(data) != uint32(len(want)) || !bytes.Equal(data[4:], want[:length-4]) {
				t.Fatalf("native normalized overflow status=%#x bytes=%x", status, data)
			}
			for _, fact := range facts {
				expected, overflow, err := wire.FileNormalizedNameInformationBody(`\long-😀-name.txt`, fact.outputCapacity)
				if err != nil || fact.outputBytes != uint32(len(expected)) || fact.outputDigest != sha256.Sum256(expected) || fact.status != nativeNameWireStatus(overflow) {
					t.Fatalf("normalized signed wire projection=%+v error=%v", fact, err)
				}
			}
		}
	})
}

func nativeNameProbe(t *testing.T) (string, storage.FileStorage, *nativeNameReceipts) {
	t.Helper()
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
	backend := smb.NewNativeNameProbeBackend()
	owner, err := storage.NewDeleteIntentOwner()
	if err != nil {
		t.Fatal(err)
	}
	share := fmt.Sprintf("rfsname%x", time.Now().UnixNano())
	export, err := server.Publish(smb.Share{Name: share, Volume: share, BackendVolume: "test-volume", RootNodeID: 1, Backend: backend, DeleteIntentOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	if err := export.RecoverDeleteIntents(smb.WithPrincipal(t.Context(), identity)); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	receipts := &nativeNameReceipts{}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, nativeNameListener{listener, receipts}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-served:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(15 * time.Second):
			t.Error("native name server did not stop")
		}
		stop, release := context.WithTimeout(smb.WithPrincipal(context.Background(), identity), 15*time.Second)
		defer release()
		if err := server.Shutdown(stop); err != nil {
			t.Error(err)
		}
		receipts.mu.Lock()
		defer receipts.mu.Unlock()
		if receipts.err != nil {
			t.Error(receipts.err)
		}
	})
	drive, remote := nativeUnusedDrive(t), `\\127.0.0.1\`+share
	port := listener.Addr().(*net.TCPAddr).Port
	t.Cleanup(func() { nativeMappingCommand(t, drive, remote, port, true) })
	nativeMappingCommand(t, drive, remote, port, false)
	return drive, backend, receipts
}

func nativeNameIdentity(t *testing.T, handle win.Handle) uint64 {
	t.Helper()
	var identity smb.NativeProbeFileIDInfo
	if err := win.GetFileInformationByHandleEx(handle, win.FileIdInfo, (*byte)(unsafe.Pointer(&identity)), uint32(unsafe.Sizeof(identity))); err != nil {
		t.Fatal(err)
	}
	return binary.LittleEndian.Uint64(identity.FileID[:])
}

func nativeAuthorityName(t *testing.T, backend storage.FileStorage, parent uint64, leaf string, id uint64, present bool) uint64 {
	t.Helper()
	session, err := backend.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	result, err := storage.NewListResult(65536, 0, func(_ int, name, metadata int64, _ storage.Attr) (int64, error) {
		return storage.ObservedEntryBytes(name, metadata)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.(storage.DirectoryReader).ReadDirNodeBounded(t.Context(), storage.DirectoryTarget{NodeID: parent}, result); err != nil {
		t.Fatal(err)
	}
	entries, err := result.Entries()
	if err != nil {
		t.Fatal(err)
	}
	actualBinding := "<no binding for expected object>"
	for _, entry := range entries {
		if entry.Attr.ID == id {
			actualBinding = entry.Name
		}
		if entry.Name != leaf {
			continue
		}
		if !present || id != 0 && entry.Attr.ID != id {
			t.Fatalf("authority binding %q=node%d want present=%t node%d", leaf, entry.Attr.ID, present, id)
		}
		return entry.Attr.ID
	}
	if present {
		t.Fatalf("authority is missing %q under node%d; node%d current binding=%q", leaf, parent, id, actualBinding)
	}
	return id
}

// FILE_RENAME_INFO has a HANDLE-aligned RootDirectory and a counted WCHAR tail.
// https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_rename_info
type nativeRenameInformation struct {
	ReplaceIfExists uint8
	RootDirectory   win.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

func nativeRenameFile(handle win.Handle, destination string, replace bool) error {
	buffer := nativeRenameBuffer(destination, replace)
	return win.SetFileInformationByHandle(handle, win.FileRenameInfo, &buffer[0], uint32(len(buffer)))
}

func nativeRenameBuffer(destination string, replace bool) []byte {
	name := wire.EncodeUTF16(destination)
	var header nativeRenameInformation
	offset := int(unsafe.Offsetof(header.FileName))
	// The Win32 wrapper converts FileName as a NUL-terminated DOS path before
	// constructing native counted input. FileNameLength excludes the terminator.
	// https://github.com/wine-mirror/wine/blob/081042ede6a165a36bf9988350dd584b440d7a3d/dlls/kernelbase/file.c#L3846
	buffer := make([]byte, offset+len(name)+2)
	if replace {
		buffer[0] = 1
	}
	binary.LittleEndian.PutUint32(buffer[unsafe.Offsetof(header.FileNameLength):], uint32(len(name)))
	copy(buffer[offset:], name)
	return buffer
}

func TestNativeRenameBufferContract(t *testing.T) {
	var header nativeRenameInformation
	if unsafe.Sizeof(header) != 24 || unsafe.Offsetof(header.RootDirectory) != 8 || unsafe.Offsetof(header.FileNameLength) != 16 || unsafe.Offsetof(header.FileName) != 20 {
		t.Fatal("invalid 64-bit FILE_RENAME_INFO native layout")
	}
	for _, name := range []string{`Z:\RENAMED.txt`, `Z:\folder\moved-😀.txt`} {
		encoded := wire.EncodeUTF16(name)
		buffer := nativeRenameBuffer(name, true)
		if len(buffer) != 20+len(encoded)+2 || buffer[0] != 1 || binary.LittleEndian.Uint64(buffer[8:16]) != 0 || binary.LittleEndian.Uint32(buffer[16:20]) != uint32(len(encoded)) || !bytes.Equal(buffer[20:20+len(encoded)], encoded) || binary.LittleEndian.Uint16(buffer[len(buffer)-2:]) != 0 {
			t.Fatal("native rename input does not own its counted name and trailing NUL WCHAR")
		}
	}
}

func TestNativeNameReceiptsCorrelateSemanticRejection(t *testing.T) {
	receipts := &nativeNameReceipts{}
	for index, test := range []struct {
		name   string
		root   uint64
		status uint32
	}{{"RENAMED.txt", 1, 0xc000000d}, {`\RENAMED.txt`, 0, 0xc000000d}, {"healthy.txt", 0, 0}} {
		defer receipts.begin(t, wire.SetInfo, wire.FileRenameInformation)()
		receipts.expectRename(test.name, false)
		name := wire.EncodeUTF16(test.name)
		input := make([]byte, 20+len(name))
		binary.LittleEndian.PutUint64(input[8:16], test.root)
		binary.LittleEndian.PutUint32(input[16:20], uint32(len(name)))
		copy(input[20:], name)
		body := make([]byte, 32+len(input))
		binary.LittleEndian.PutUint16(body, 33)
		body[2], body[3], body[16] = wire.InfoFile, wire.FileRenameInformation, 1
		binary.LittleEndian.PutUint32(body[4:8], uint32(len(input)))
		binary.LittleEndian.PutUint16(body[8:10], wire.HeaderSize+32)
		copy(body[32:], input)
		header := wire.Header{Command: wire.SetInfo, SessionID: 12, MessageID: uint64(index + 1), Flags: wire.FlagSigned}
		packet := make([]byte, wire.HeaderSize+len(body))
		if err := header.Encode(packet); err != nil {
			t.Fatal(err)
		}
		copy(packet[wire.HeaderSize:], body)
		receipts.request(packet)
		header.Status = test.status
		response := wire.SetInfoResponseBody()
		if test.status != 0 {
			response = wire.ErrorResponseBody()
		}
		receipts.response(wire.EncodeResponse(header, response))
		facts := receipts.finish(t, test.status, true)
		if len(facts) != 1 || !facts[0].renameDecoded || !facts[0].renameAdmissionChecked || facts[0].renameRoot != test.root || facts[0].renameCount != uint32(len(name)) || facts[0].renameName != test.name || facts[0].renameAdmitted != (test.status == 0) || facts[0].renameDigest != sha256.Sum256(input) {
			t.Fatalf("semantic rejection or later healthy window lost its correlated receipt: %+v", facts)
		}
	}
}

func nativeDisposition(handle win.Handle, deleting bool) error {
	var value byte
	if deleting {
		value = 1
	}
	return win.SetFileInformationByHandle(handle, win.FileDispositionInfo, &value, 1)
}

func nativeSupersede(t *testing.T, path string, access uint32) *nativeDeleteHandle {
	t.Helper()
	name, err := win.NewNTUnicodeString(`\??\` + path)
	if err != nil {
		t.Fatal(err)
	}
	attributes := win.OBJECT_ATTRIBUTES{ObjectName: name, Attributes: win.OBJ_CASE_INSENSITIVE}
	attributes.Length = uint32(unsafe.Sizeof(attributes))
	var handle win.Handle
	var result win.IO_STATUS_BLOCK
	if err := win.NtCreateFile(&handle, access|win.SYNCHRONIZE, &attributes, &result, nil, win.FILE_ATTRIBUTE_NORMAL, 7,
		win.FILE_SUPERSEDE, win.FILE_SYNCHRONOUS_IO_NONALERT|win.FILE_NON_DIRECTORY_FILE, 0, 0); err != nil {
		t.Fatal(err)
	}
	runtime.KeepAlive(name)
	owned := &nativeDeleteHandle{Handle: handle}
	t.Cleanup(func() {
		if err := owned.Close(); err != nil {
			t.Error(err)
		}
	})
	return owned
}

func nativeNameQuery(t *testing.T, handle win.Handle, class uint32, length int) (uint32, []byte) {
	t.Helper()
	buffer := make([]byte, length)
	var iosb win.IO_STATUS_BLOCK
	err := win.NtQueryInformationFile(handle, &iosb, &buffer[0], uint32(length), class)
	runtime.KeepAlive(buffer)
	var status uint32
	if err != nil {
		var native win.NTStatus
		if !errors.As(err, &native) {
			t.Fatal(err)
		}
		status = uint32(native)
	}
	if status == 0x103 || iosb.Information > uintptr(length) {
		t.Fatalf("synchronous native name query status=%#x information=%d", status, iosb.Information)
	}
	return status, buffer[:int(iosb.Information)]
}

func nativeNameWireStatus(overflow bool) uint32 {
	if overflow {
		return 0x80000005
	}
	return 0
}

func nativeAuthorityPending(t *testing.T, backend storage.FileStorage, id uint64, pending bool) {
	t.Helper()
	state := backend.(interface {
		ProbeNodeState(uint64) storage.ReferenceState
	}).ProbeNodeState(id)
	if state.PendingUnlink != pending || state.Detached || state.Attr.ID != id || pending != (len(state.PendingGeneration) != 0) {
		t.Fatalf("authority ordinary disposition node%d state=%+v want pending=%t", id, state, pending)
	}
}

func nativeNameClose(t *testing.T, handle *nativeDeleteHandle, receipts *nativeNameReceipts) {
	t.Helper()
	defer receipts.begin(t, wire.Close, 0)()
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	receipts.finish(t, 0, true)
}

type nativeNameFact struct {
	command                uint16
	class                  byte
	status                 uint32
	desiredAccess          uint32
	outputCapacity         uint32
	outputBytes            uint32
	outputDigest           [32]byte
	fileID                 wire.FileID
	createAction           uint32
	renameName             string
	renameExpected         string
	renameReplace          bool
	renameMatches          bool
	renameBytes            uint32
	renameDigest           [32]byte
	renameHeader           bool
	renameRoot             uint64
	renameCount            uint32
	renameFirst            uint16
	renameDecoded          bool
	renameAdmitted         bool
	renameAdmissionChecked bool
}

type nativeNameReceipts struct {
	mu              sync.Mutex
	active          bool
	command         uint16
	class           byte
	pending         map[[2]uint64]nativeNameFact
	facts           []nativeNameFact
	err             error
	window          uint64
	expectedName    string
	expectedReplace bool
}

func (r *nativeNameReceipts) begin(t *testing.T, command uint16, class byte) func() {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active || len(r.pending) != 0 {
		r.err = fmt.Errorf("native name receipt overlapped an unfinished operation")
		return func() {}
	}
	r.window++
	window := r.window
	r.active, r.command, r.class = true, command, class
	r.expectedName, r.expectedReplace = "", false
	r.pending = make(map[[2]uint64]nativeNameFact)
	r.facts = nil
	return func() {
		r.mu.Lock()
		unfinished := r.active && r.window == window
		r.mu.Unlock()
		if unfinished {
			r.finish(t, 0, false)
		}
	}
}

func (r *nativeNameReceipts) expectRename(name string, replace bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expectedName, r.expectedReplace = name, replace
}

func (r *nativeNameReceipts) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *nativeNameReceipts) request(packet []byte) {
	if len(packet) < 4 || string(packet[:4]) != "\xfeSMB" {
		return
	}
	requests, err := wire.ParseFrame(packet, wire.Limits{MaxBytes: 2 << 20, MaxCommands: 32, MaxContexts: 16})
	if err != nil {
		r.fail(err)
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, request := range requests {
		if !r.active || request.Header.Command != r.command {
			continue
		}
		fact := nativeNameFact{command: request.Header.Command}
		switch request.Header.Command {
		case wire.SetInfo:
			value, err := request.SetInfo()
			if err != nil {
				r.err = err
				return
			}
			fact.class, fact.fileID = value.FileInfoClass, value.FileID
			if value.InfoType != wire.InfoFile || fact.class != r.class {
				continue
			}
			if fact.class == wire.FileRenameInformation {
				if len(r.expectedName) > 256 {
					r.err = fmt.Errorf("native rename expectation exceeds fixture name bound")
					return
				}
				fact.renameExpected = r.expectedName
				fact.renameBytes, fact.renameDigest = uint32(len(value.Buffer)), sha256.Sum256(value.Buffer)
				if len(value.Buffer) > 0 {
					fact.renameReplace = value.Buffer[0] != 0
				}
				if len(value.Buffer) >= 20 {
					fact.renameHeader = true
					fact.renameRoot = binary.LittleEndian.Uint64(value.Buffer[8:16])
					fact.renameCount = binary.LittleEndian.Uint32(value.Buffer[16:20])
					if len(value.Buffer) >= 22 {
						fact.renameFirst = binary.LittleEndian.Uint16(value.Buffer[20:22])
					}
					if fact.renameCount <= 256 && fact.renameCount&1 == 0 && uint64(fact.renameCount) <= uint64(len(value.Buffer)-20) {
						name, decodeErr := wire.DecodeUTF16(value.Buffer[20 : 20+int(fact.renameCount)])
						fact.renameName, fact.renameDecoded = name, decodeErr == nil
						_, admissionErr := wire.ParseFileRenameInformation(value.Buffer)
						fact.renameAdmissionChecked, fact.renameAdmitted = true, admissionErr == nil
					}
				}
				fact.renameMatches = fact.renameDecoded && fact.renameName == r.expectedName && fact.renameReplace == r.expectedReplace
			}
		case wire.QueryInfo:
			value, err := request.QueryInfo()
			if err != nil {
				r.err = err
				return
			}
			fact.class, fact.fileID, fact.outputCapacity = value.FileInfoClass, value.FileID, value.OutputBufferLength
			if value.InfoType != wire.InfoFile || fact.class != r.class {
				continue
			}
		case wire.Create:
			value, err := request.Create()
			if err != nil {
				r.err = err
				return
			}
			if value.Disposition != 0 {
				continue
			}
			fact.desiredAccess = value.DesiredAccess
		case wire.Close:
		default:
			r.err = fmt.Errorf("unexpected native name receipt command %d", request.Header.Command)
			return
		}
		if request.Header.Flags&wire.FlagSigned == 0 {
			r.err = fmt.Errorf("native name request is unsigned")
			return
		}
		if len(r.pending)+len(r.facts) >= 8 {
			r.err = fmt.Errorf("native name operation exceeded eight request receipts")
			return
		}
		key := [2]uint64{request.Header.SessionID, request.Header.MessageID}
		if _, duplicate := r.pending[key]; duplicate {
			r.err = fmt.Errorf("native name operation repeated a message identity")
			return
		}
		r.pending[key] = fact
	}
}

func (r *nativeNameReceipts) response(packet []byte) {
	if len(packet) < 4 || string(packet[:4]) != "\xfeSMB" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for commands := 0; commands < 32; commands++ {
		header, err := wire.ParseHeader(packet)
		if err != nil {
			r.err = err
			return
		}
		end := len(packet)
		if header.NextCommand != 0 {
			if header.NextCommand < wire.HeaderSize || uint64(header.NextCommand)+wire.HeaderSize > uint64(len(packet)) {
				r.err = wire.ErrMalformed
				return
			}
			end = int(header.NextCommand)
		}
		key := [2]uint64{header.SessionID, header.MessageID}
		if fact, found := r.pending[key]; found {
			if header.Command != fact.command || header.Flags&(wire.FlagResponse|wire.FlagSigned) != wire.FlagResponse|wire.FlagSigned {
				r.err = fmt.Errorf("native name response does not match its signed request")
				return
			}
			fact.status = header.Status
			body := packet[wire.HeaderSize:end]
			if fact.command == wire.Create && header.Status == 0 {
				if len(body) < 88 || binary.LittleEndian.Uint16(body) != 89 {
					r.err = wire.ErrMalformed
					return
				}
				fact.createAction = binary.LittleEndian.Uint32(body[4:8])
				copy(fact.fileID[:], body[64:80])
			}
			if fact.command == wire.QueryInfo && (header.Status == 0 || header.Status == 0x80000005) {
				if len(body) < 8 || binary.LittleEndian.Uint16(body) != 9 {
					r.err = wire.ErrMalformed
					return
				}
				offset, length := int(binary.LittleEndian.Uint16(body[2:4])), binary.LittleEndian.Uint32(body[4:8])
				if offset < wire.HeaderSize+8 || uint64(offset)+uint64(length) > uint64(end) || length > fact.outputCapacity {
					r.err = wire.ErrMalformed
					return
				}
				fact.outputBytes = length
				fact.outputDigest = sha256.Sum256(packet[offset : offset+int(length)])
			}
			r.facts = append(r.facts, fact)
			delete(r.pending, key)
		}
		if header.NextCommand == 0 {
			return
		}
		packet = packet[header.NextCommand:]
	}
	r.err = fmt.Errorf("native name response exceeded compound bound")
}

func (r *nativeNameReceipts) finish(t *testing.T, expected uint32, require bool) []nativeNameFact {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		r.mu.Lock()
		err := r.err
		pending, count := len(r.pending), len(r.facts)
		if err != nil || pending == 0 && (count != 0 || !require) {
			facts := append([]nativeNameFact(nil), r.facts...)
			r.active = false
			r.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			for _, fact := range facts {
				t.Logf("native name signed exchange: command=%d class=%d status=%#x capacity=%d bytes=%d", fact.command, fact.class, fact.status, fact.outputCapacity, fact.outputBytes)
				if fact.command == wire.SetInfo && fact.class == wire.FileRenameInformation {
					t.Logf("native rename input: requested=%q wire=%q replace=%t exactMatch=%t bytes=%d fixedHeader=%t root=%#x count=%d first=%#x decoded=%t admissionChecked=%t admitted=%t digest=%x", fact.renameExpected, fact.renameName, fact.renameReplace, fact.renameMatches, fact.renameBytes, fact.renameHeader, fact.renameRoot, fact.renameCount, fact.renameFirst, fact.renameDecoded, fact.renameAdmissionChecked, fact.renameAdmitted, fact.renameDigest)
				}
				if require && fact.status != expected {
					t.Fatalf("native name wire status=%#x want %#x", fact.status, expected)
				}
			}
			if len(facts) == 0 {
				t.Log("native name API issued no matching SMB exchange")
			}
			return facts
		}
		r.mu.Unlock()
		select {
		case <-deadline.C:
			t.Fatalf("native name operation has %d responses and %d unmatched requests", count, pending)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

type nativeNameListener struct {
	net.Listener
	receipts *nativeNameReceipts
}

func (l nativeNameListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &nativeNameConnection{Conn: connection,
		input:  nativeFrameStream{record: l.receipts.request, fail: l.receipts.fail},
		output: nativeFrameStream{record: l.receipts.response, fail: l.receipts.fail}}, nil
}

type nativeNameConnection struct {
	net.Conn
	input, output nativeFrameStream
}

func (c *nativeNameConnection) Read(buffer []byte) (int, error) {
	count, err := c.Conn.Read(buffer)
	c.input.capture(buffer[:count])
	return count, err
}

func (c *nativeNameConnection) Write(buffer []byte) (int, error) {
	count, err := c.Conn.Write(buffer)
	c.output.capture(buffer[:count])
	return count, err
}
