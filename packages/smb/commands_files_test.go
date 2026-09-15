package smb

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type commandFile struct {
	storage.WindowsFile
	attr     storage.WindowsAttr
	data     []byte
	readErr  error
	writeErr error
	result   storage.WindowsActionResult
	closed   int
	writes   int
}

func (f *commandFile) Reference() string                                 { return "retained" }
func (f *commandFile) Stat(context.Context) (storage.WindowsAttr, error) { return f.attr, nil }
func (f *commandFile) ReadAt(_ context.Context, offset int64, size int) (storage.FileRead, error) {
	if f.readErr != nil {
		return storage.FileRead{}, f.readErr
	}
	start := min(offset, int64(len(f.data)))
	end := min(start+int64(size), int64(len(f.data)))
	return storage.FileRead{Attr: f.attr.Attr, Data: f.data[start:end]}, nil
}
func (f *commandFile) WriteAt(_ context.Context, _ int64, _ []byte, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	f.writes++
	r := f.result
	r.Action = id
	return r, f.writeErr
}
func (f *commandFile) Close(_ context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	f.closed++
	return storage.WindowsActionResult{Action: id, State: storage.WindowsActionCompleted, Attr: f.attr}, nil
}
func (f *commandFile) Sync(context.Context) error { return f.readErr }

type commandSession struct {
	storage.WindowsSession
	result   storage.WindowsActionResult
	queryErr error
	closed   int
	open     func(storage.WindowsOpenRequest) (storage.WindowsOpenResult, error)
}

func (s *commandSession) Status(context.Context) (storage.FileSessionStatus, error) {
	return storage.FileSessionStatus{ActionEpoch: 1}, nil
}
func (s *commandSession) QueryAction(_ context.Context, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	result := s.result
	result.Action = id
	return result, s.queryErr
}
func (s *commandSession) Close(context.Context) error { s.closed++; return nil }
func (s *commandSession) Open(_ context.Context, r storage.WindowsOpenRequest, _ storage.WindowsActionID) (storage.WindowsOpenResult, error) {
	return s.open(r)
}

func fileRequest(command uint16, b []byte) wire.Request {
	p := make([]byte, 64+len(b))
	copy(p[64:], b)
	return wire.Request{Header: wire.Header{Command: command}, Body: p[64:], Packet: p}
}
func commandDispatcher() (*fileDispatcher, *commandFile, *commandSession, wire.FileID) {
	f := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 7, Size: 3}}}, data: []byte("abc"), result: storage.WindowsActionResult{State: storage.WindowsActionCompleted}}
	s := &commandSession{}
	d := newFileDispatcher(nil, s, 1, DefaultLimits())
	id := wire.FileID{1}
	d.handles[id] = &fileHandle{file: f, identity: notificationIdentity{ID: f.attr.ID, Directory: f.attr.IsDir()}}
	return d, f, s, id
}
func readCommand(id wire.FileID, off uint64, n uint32) wire.Request {
	b := make([]byte, 48)
	smbLE.PutUint16(b, 49)
	smbLE.PutUint32(b[4:], n)
	smbLE.PutUint64(b[8:], off)
	copy(b[16:], id[:])
	return fileRequest(wire.Read, b)
}
func writeCommand(id wire.FileID, data []byte) wire.Request {
	b := make([]byte, 48+len(data))
	smbLE.PutUint16(b, 49)
	smbLE.PutUint16(b[2:], 112)
	smbLE.PutUint32(b[4:], uint32(len(data)))
	copy(b[16:], id[:])
	copy(b[48:], data)
	return fileRequest(wire.Write, b)
}

func TestCommandReadBoundsAndFailure(t *testing.T) {
	d, f, _, id := commandDispatcher()
	b, status := d.handle(context.Background(), readCommand(id, 1, 5))
	if status != 0 || string(b[16:]) != "bc" {
		t.Fatalf("read = %x %q", status, b)
	}
	_, status = d.handle(context.Background(), readCommand(id, 3, 1))
	if status != fileEOF {
		t.Fatalf("EOF = %x", status)
	}
	_, status = d.handle(context.Background(), readCommand(id, 0, uint32(d.limits.MaxIOBytes+1)))
	if status != fileInvalidParameter {
		t.Fatalf("oversize = %x", status)
	}
	f.readErr = syscall.EIO
	_, status = d.handle(context.Background(), readCommand(id, 0, 1))
	if status != fileIOError {
		t.Fatalf("outage = %x", status)
	}
}

func TestCommandWriteReconcilesWithoutRepeating(t *testing.T) {
	d, f, s, id := commandDispatcher()
	f.writeErr = syscall.EIO
	s.result = storage.WindowsActionResult{State: storage.WindowsActionCompleted}
	b, status := d.handle(context.Background(), writeCommand(id, []byte("test")))
	if status != 0 || smbLE.Uint32(b[4:]) != 4 || f.writes != 1 || s.closed != 0 {
		t.Fatalf("known write = %x, writes %d closed %d", status, f.writes, s.closed)
	}
}
func TestCommandUnknownWriteFencesReferences(t *testing.T) {
	d, f, s, id := commandDispatcher()
	f.writeErr = syscall.EIO
	s.queryErr = syscall.EIO
	_, status := d.handle(context.Background(), writeCommand(id, []byte("x")))
	if status != fileIOError || s.closed != 1 {
		t.Fatalf("unknown = %x closed %d", status, s.closed)
	}
	_, status = d.handle(context.Background(), readCommand(id, 0, 1))
	if status != fileIOError {
		t.Fatalf("fenced read = %x", status)
	}
}
func TestCommandCloseRemovesOnlyItsHandle(t *testing.T) {
	d, f, _, id := commandDispatcher()
	other := wire.FileID{2}
	d.handles[other] = &fileHandle{file: f}
	b := make([]byte, 24)
	smbLE.PutUint16(b, 24)
	smbLE.PutUint16(b[2:], 1)
	copy(b[8:], id[:])
	data, status := d.handle(context.Background(), fileRequest(wire.Close, b))
	if status != 0 || len(data) != 60 || f.closed != 1 || d.handleCount() != 1 {
		t.Fatalf("close = %x count %d", status, d.handleCount())
	}
	if _, ok := d.file(other); !ok {
		t.Fatal("unrelated handle removed")
	}
}

type deniedStatFile struct {
	*commandFile
	statCalls int
}

func (f *deniedStatFile) Stat(context.Context) (storage.WindowsAttr, error) {
	f.statCalls++
	return storage.WindowsAttr{}, syscall.EACCES
}

func TestCloseDoesNotRequireMetadataAccess(t *testing.T) {
	for _, postQuery := range []bool{false, true} {
		d, f, _, id := commandDispatcher()
		restricted := &deniedStatFile{commandFile: f}
		d.handles[id].file = restricted
		body := make([]byte, 24)
		smbLE.PutUint16(body, 24)
		if postQuery {
			smbLE.PutUint16(body[2:], 1)
		}
		copy(body[8:], id[:])
		response, status := d.handle(context.Background(), fileRequest(wire.Close, body))
		if status != 0 || f.closed != 1 || d.handleCount() != 0 || smbLE.Uint16(response[2:]) != 0 {
			t.Fatalf("POSTQUERY %v close=%x closed%d response%x", postQuery, status, f.closed, response)
		}
		want := 0
		if postQuery {
			want = 1
		}
		if restricted.statCalls != want {
			t.Fatalf("POSTQUERY %v queried attributes %d times", postQuery, restricted.statCalls)
		}
	}
}

type changeTimeFile struct {
	*commandFile
	sets, stats int
}

func (f *changeTimeFile) Stat(context.Context) (storage.WindowsAttr, error) {
	f.stats++
	return storage.WindowsAttr{}, syscall.EACCES
}
func (f *changeTimeFile) SetAttr(_ context.Context, c storage.WindowsAttrChange, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	f.sets++
	if c.ChangeTime == nil {
		f.attr.ChangeTime = time.Unix(100, 0)
	} else {
		f.attr.ChangeTime = *c.ChangeTime
	}
	if c.DOSAttributes != nil {
		f.attr.DOSAttributes = *c.DOSAttributes
	}
	return storage.WindowsActionResult{Action: id, State: storage.WindowsActionCompleted}, nil
}

func TestUnbufferedCreateIsRejectedBeforeBackendAdmission(t *testing.T) {
	d, f, s, _ := commandDispatcher()
	opens := 0
	s.open = func(r storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		opens++
		if r.Lookup.Name == "" {
			root := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir}}}}
			return storage.WindowsOpenResult{File: root, Attr: root.attr, CreateAction: storage.WindowsOpened}, nil
		}
		return storage.WindowsOpenResult{File: f, Attr: f.attr, CreateAction: storage.WindowsOpened}, nil
	}
	r := createCommand("file", 3, 1)
	smbLE.PutUint32(r.Body[40:], 0x8)
	if _, status := d.create(context.Background(), r); status != fileNotSupported || opens != 0 || f.writes != 0 || string(f.data) != "abc" {
		t.Fatalf("unbuffered status%x opens%d writes%d data%q", status, opens, f.writes, f.data)
	}
	smbLE.PutUint32(r.Body[40:], 0x2)
	if _, status := d.create(context.Background(), r); status != 0 || opens != 2 {
		t.Fatalf("write-through status%x opens%d", status, opens)
	}
}

func TestChangeTimePreserveSentinelCannotSilentlyBecomeAutomaticTime(t *testing.T) {
	d, f, _, id := commandDispatcher()
	f.attr.ChangeTime = time.Unix(10, 0)
	file := &changeTimeFile{commandFile: f}
	d.handles[id].file = file
	data := make([]byte, 40)
	smbLE.PutUint64(data[24:], ^uint64(0))
	smbLE.PutUint32(data[32:], storage.WindowsDOSHidden)
	if _, status := d.setInfo(context.Background(), setCommand(id, 4, data)); status != fileNotSupported || file.sets != 0 || file.stats != 0 || !f.attr.ChangeTime.Equal(time.Unix(10, 0)) || f.attr.DOSAttributes != 0 {
		t.Fatalf("preserve sentinel status%x sets%d stats%d attr%+v", status, file.sets, file.stats, f.attr)
	}
	smbLE.PutUint64(data[24:], 0)
	if _, status := d.setInfo(context.Background(), setCommand(id, 4, data)); status != 0 || file.sets != 1 || file.stats != 0 || !f.attr.ChangeTime.Equal(time.Unix(100, 0)) {
		t.Fatalf("automatic time status%x attr%+v", status, f.attr)
	}
	explicit := time.Unix(200, 123400)
	smbLE.PutUint64(data[24:], windowsTime(explicit))
	if _, status := d.setInfo(context.Background(), setCommand(id, 4, data)); status != 0 || file.sets != 2 || !f.attr.ChangeTime.Equal(explicit) {
		t.Fatalf("explicit time status%x attr%+v", status, f.attr)
	}
}
func TestFileInformationAndTimeEncoding(t *testing.T) {
	instant := time.Date(2026, 9, 15, 12, 34, 56, 123456700, time.UTC)
	if got := decodeWindowsTime(windowsTime(instant)); got == nil || !got.Equal(instant) {
		t.Fatalf("time = %v", got)
	}
	if decodeWindowsTime(0) != nil || decodeWindowsTime(^uint64(0)) != nil {
		t.Fatal("timestamp preserve values changed")
	}
	a := storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 42, Size: 123, Mode: 0644}, CreationTime: instant, DeletePending: true, DOSAttributes: storage.WindowsDOSHidden}}
	b, s := encodeFileInfo(5, a, 0, 0)
	if s != 0 || smbLE.Uint64(b) != 123 || b[20] != 1 {
		t.Fatalf("standard = %x %x", s, b)
	}
	b, s = directoryEntry(37, "name", a, 9)
	if s != 0 || smbLE.Uint64(b[96:]) != 42 || smbLE.Uint32(b[4:]) != 9 {
		t.Fatalf("directory = %x %x", s, b)
	}
	if _, s = directoryEntry(255, "", a, 0); s != fileInvalidInfoClass {
		t.Fatalf("unknown class = %x", s)
	}
	a.Mode = fs.ModeDir
	if fileAttributes(a)&0x10 == 0 {
		t.Fatal("directory bit missing")
	}
}
func TestSMBPatternMatching(t *testing.T) {
	for _, tc := range []struct {
		p, n string
		want bool
	}{{"*", "abc", true}, {"*.txt", "A.TXT", true}, {"a?c", "abc", true}, {"a?c", "ac", false}, {"<.txt", "a.b.txt", true}, {"<.txt", "a.txt.bak", false}, {">>>", "ab", true}, {"a\"", "a", true}, {"a\"", "a.", true}, {"", "a", false}} {
		if got := matchSMBPattern(tc.p, tc.n); got != tc.want {
			t.Errorf("%q %q = %v", tc.p, tc.n, got)
		}
	}
}
func TestWindowsAccessAndStatusMapping(t *testing.T) {
	i, err := windowsIntent(wire.CreateRequest{DesiredAccess: 0x80000000, ShareAccess: 7, Disposition: 1})
	if err != nil || i.Access&storage.WindowsReadData == 0 || i.Disposition != storage.WindowsOpen {
		t.Fatalf("intent = %+v %v", i, err)
	}
	_, err = windowsIntent(wire.CreateRequest{DesiredAccess: 1, Disposition: 1, Options: 0x1000})
	if !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("delete access = %v", err)
	}
	if got := statusError(&storage.WindowsError{Failure: storage.WindowsSharingViolation, Err: syscall.EACCES}); got != 0xc0000043 {
		t.Fatalf("sharing = %x", got)
	}
	if got := statusAction(storage.WindowsActionResult{State: storage.WindowsActionPending}, nil); got != fileIOError {
		t.Fatalf("pending = %x", got)
	}
}

type commandBackend struct {
	storage.WindowsStorage
	space storage.Space
	err   error
}

func (b *commandBackend) Space(context.Context) (storage.Space, error) { return b.space, b.err }
func (f *commandFile) ListBounded(_ context.Context, r *storage.WindowsListResult) error {
	if f.readErr != nil {
		return r.Fail(f.readErr)
	}
	return r.Add(storage.WindowsEntry{Name: "a.txt", Attr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 8, Size: 1}, DOSAttributes: storage.WindowsDOSHidden}})
}
func (f *commandFile) Truncate(_ context.Context, size int64, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	f.attr.Size = size
	return storage.WindowsActionResult{Action: id, State: storage.WindowsActionCompleted}, nil
}
func (f *commandFile) SetAttr(_ context.Context, c storage.WindowsAttrChange, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	if c.DOSAttributes != nil {
		f.attr.DOSAttributes = *c.DOSAttributes
	}
	return storage.WindowsActionResult{Action: id, State: storage.WindowsActionCompleted}, nil
}
func (f *commandFile) SetDeletePending(_ context.Context, value bool, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	f.attr.DeletePending = value
	return storage.WindowsActionResult{Action: id, State: storage.WindowsActionCompleted}, nil
}
func (f *commandFile) Rename(_ context.Context, r storage.WindowsRenameRequest, id storage.WindowsActionID) (storage.WindowsActionResult, error) {
	if err := r.Check(); err != nil {
		return storage.WindowsActionResult{}, err
	}
	return storage.WindowsActionResult{Action: id, State: storage.WindowsActionCompleted}, nil
}

func createCommand(name string, access uint32, disposition uint32) wire.Request {
	n := wire.EncodeUTF16(name)
	b := make([]byte, 56+len(n))
	smbLE.PutUint16(b, 57)
	smbLE.PutUint32(b[24:], access)
	smbLE.PutUint32(b[32:], 7)
	smbLE.PutUint32(b[36:], disposition)
	if len(n) > 0 {
		smbLE.PutUint16(b[44:], 120)
		smbLE.PutUint16(b[46:], uint16(len(n)))
		copy(b[56:], n)
	}
	return fileRequest(wire.Create, b)
}
func setCommand(id wire.FileID, class byte, data []byte) wire.Request {
	b := make([]byte, 32+len(data))
	smbLE.PutUint16(b, 33)
	b[2] = 1
	b[3] = class
	smbLE.PutUint32(b[4:], uint32(len(data)))
	smbLE.PutUint16(b[8:], 96)
	copy(b[16:], id[:])
	copy(b[32:], data)
	return fileRequest(wire.SetInfo, b)
}
func queryCommand(id wire.FileID, typ, class byte, limit uint32) wire.Request {
	b := make([]byte, 40)
	smbLE.PutUint16(b, 41)
	b[2] = typ
	b[3] = class
	smbLE.PutUint32(b[4:], limit)
	copy(b[24:], id[:])
	return fileRequest(wire.QueryInfo, b)
}
func dirCommand(id wire.FileID, flags byte, limit uint32) wire.Request {
	b := make([]byte, 34)
	smbLE.PutUint16(b, 33)
	b[2] = 37
	b[3] = flags
	copy(b[8:], id[:])
	smbLE.PutUint16(b[24:], 96)
	smbLE.PutUint16(b[26:], 2)
	smbLE.PutUint32(b[28:], limit)
	copy(b[32:], wire.EncodeUTF16("*"))
	return fileRequest(wire.QueryDirectory, b)
}

func TestCreateRetainsExactParentAndBoundsHandles(t *testing.T) {
	d, f, s, _ := commandDispatcher()
	var requests []storage.WindowsOpenRequest
	root := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir}}}}
	parent := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 2, Mode: fs.ModeDir}}}}
	s.open = func(r storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		requests = append(requests, r)
		if r.Lookup.Name == "" {
			return storage.WindowsOpenResult{File: root, Attr: root.attr}, nil
		}
		if r.Lookup.Name == "dir" {
			return storage.WindowsOpenResult{File: parent, Attr: parent.attr}, nil
		}
		return storage.WindowsOpenResult{File: f, Attr: f.attr, CreateAction: storage.WindowsCreated}, nil
	}
	body, status := d.handle(context.Background(), createCommand("dir\\name", 3, 2))
	if status != 0 || smbLE.Uint32(body[4:]) != 2 {
		t.Fatalf("create %x", status)
	}
	if len(requests) != 3 || requests[2].Lookup.ParentID != 2 || requests[2].Lookup.ParentReference != "retained" || root.closed != 1 || parent.closed != 1 {
		t.Fatalf("path admission %+v root closed %d parent closed %d", requests, root.closed, parent.closed)
	}
	d.limits.MaxOpens = d.handleCount()
	_, status = d.handle(context.Background(), createCommand("name", 1, 1))
	if status != fileInsufficientResources {
		t.Fatalf("open bound %x", status)
	}
	for _, name := range []string{"../escape", "dir\\..\\x", "a:b", "\\absolute"} {
		if _, status = d.handle(context.Background(), createCommand(name, 1, 1)); status == 0 {
			t.Errorf("accepted %q", name)
		}
	}
}
func TestOpenUnknownReconcilesAndFences(t *testing.T) {
	d, f, s, _ := commandDispatcher()
	s.open = func(storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		return storage.WindowsOpenResult{}, syscall.EIO
	}
	id, _ := d.actionID()
	s.result = storage.WindowsActionResult{State: storage.WindowsActionCompleted, File: f, Attr: f.attr, CreateAction: storage.WindowsOpened}
	got, err := d.open(context.Background(), storage.WindowsOpenRequest{}, id)
	if err != nil || got.File != f {
		t.Fatalf("reconciled %v %v", got, err)
	}
	s.result = storage.WindowsActionResult{State: storage.WindowsActionPending}
	_, err = d.open(context.Background(), storage.WindowsOpenRequest{}, id)
	if !errors.Is(err, syscall.EIO) || s.closed != 1 {
		t.Fatalf("pending %v close %d", err, s.closed)
	}
}
func TestQueryDirectoryCursorSurvivesSmallBuffer(t *testing.T) {
	d, f, s, id := commandDispatcher()
	f.attr.Mode = fs.ModeDir
	s.open = func(r storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		t.Fatal("enumeration reopened an entry")
		return storage.WindowsOpenResult{}, nil
	}
	_, status := d.handle(context.Background(), dirCommand(id, 0, 1))
	if status != fileBufferTooSmall {
		t.Fatalf("small %x", status)
	}
	body, status := d.handle(context.Background(), dirCommand(id, 0, 1024))
	if status != 0 || smbLE.Uint64(body[8+96:]) != 8 || smbLE.Uint32(body[8+56:]) != storage.WindowsDOSHidden {
		t.Fatalf("entry %x %x", status, body)
	}
	_, status = d.handle(context.Background(), dirCommand(id, 0, 1024))
	if status != fileNoMoreFiles {
		t.Fatalf("end %x", status)
	}
	_, status = d.handle(context.Background(), dirCommand(id, 1, 1024))
	if status != 0 {
		t.Fatalf("restart %x", status)
	}
	f.readErr = syscall.EIO
	_, status = d.handle(context.Background(), dirCommand(id, 1, 1024))
	if status != fileIOError {
		t.Fatalf("failed listing %x", status)
	}
}
func TestSetInformationMutatesOnlySelectedFields(t *testing.T) {
	d, f, _, id := commandDispatcher()
	ctx := context.Background()
	for _, tc := range []struct {
		class byte
		data  []byte
	}{{13, []byte{1}}, {20, []byte{9, 0, 0, 0, 0, 0, 0, 0}}, {14, []byte{3, 0, 0, 0, 0, 0, 0, 0}}} {
		if _, status := d.handle(ctx, setCommand(id, tc.class, tc.data)); status != 0 {
			t.Fatalf("class %d = %x", tc.class, status)
		}
	}
	basic := make([]byte, 40)
	smbLE.PutUint32(basic[32:], storage.WindowsDOSHidden)
	if _, status := d.handle(ctx, setCommand(id, 4, basic)); status != 0 {
		t.Fatalf("basic %x", status)
	}
	if !f.attr.DeletePending || f.attr.Size != 9 || f.attr.DOSAttributes != storage.WindowsDOSHidden || d.get(id).position != 3 {
		t.Fatalf("attributes %+v", f.attr)
	}
	if _, status := d.handle(ctx, setCommand(id, 13, []byte{2})); status != fileInvalidParameter {
		t.Fatalf("invalid boolean %x", status)
	}
	if _, status := d.handle(ctx, setCommand(id, 99, nil)); status != fileNotSupported {
		t.Fatalf("unknown class %x", status)
	}
}
func TestFileAndFilesystemQueryShapes(t *testing.T) {
	d, f, _, id := commandDispatcher()
	d.handles[id].access = 1
	d.backend = &commandBackend{space: storage.Space{Total: 100, Used: 60, Avail: 30}}
	for _, class := range []byte{4, 5, 6, 7, 8, 14, 16, 17, 22, 35} {
		b, status := d.handle(context.Background(), queryCommand(id, 1, class, 1024))
		if status != 0 || smbLE.Uint32(b[4:]) != uint32(len(b)-8) {
			t.Errorf("class %d = %x %x", class, status, b)
		}
	}
	for _, class := range []byte{3, 4, 5, 7} {
		if _, status := d.handle(context.Background(), queryCommand(id, 2, class, 1024)); status != 0 {
			t.Errorf("FS class %d = %x", class, status)
		}
	}
	_, status := d.handle(context.Background(), queryCommand(id, 1, 4, 1))
	if status != fileBufferTooSmall {
		t.Fatalf("short buffer %x", status)
	}
	f.attr.Mode = fs.ModeDir
	b, status := d.handle(context.Background(), queryCommand(id, 1, 22, 1024))
	if status != 0 || len(b) != 8 {
		t.Fatalf("directory streams %x %x", status, b)
	}
	d.backend = &commandBackend{err: syscall.EIO}
	_, status = d.handle(context.Background(), queryCommand(id, 2, 3, 1024))
	if status != fileIOError {
		t.Fatalf("unknown space %x", status)
	}
	d.backend = &commandBackend{space: storage.Space{Total: 1, Avail: 2}}
	_, status = d.handle(context.Background(), queryCommand(id, 2, 3, 1024))
	if status != fileIOError {
		t.Fatalf("incoherent space %x", status)
	}
}

func TestIdentityQueriesDoNotRequireReadAttributes(t *testing.T) {
	d, f, _, id := commandDispatcher()
	file := &deniedStatFile{commandFile: f}
	d.handles[id].file = file
	d.handles[id].access = 2
	d.backend = &sessionBackend{}
	for _, class := range []byte{6, 8, 14, 16, 59} {
		body, status := d.queryInfo(context.Background(), queryCommand(id, 1, class, 1024))
		if status != 0 || len(body) < 12 {
			t.Fatalf("class%d status%x body%x", class, status, body)
		}
		if class == 6 && smbLE.Uint64(body[8:]) != f.attr.ID {
			t.Fatal("identity changed")
		}
	}
	if _, status := d.queryInfo(context.Background(), queryCommand(id, 2, 4, 1024)); status != 0 {
		t.Fatalf("filesystem info %x", status)
	}
	if file.statCalls != 0 {
		t.Fatalf("identity queries read attributes %d times", file.statCalls)
	}
	if _, status := d.queryInfo(context.Background(), queryCommand(id, 1, 4, 1024)); status != statusDenied {
		t.Fatalf("basic attributes unexpectedly granted: %x", status)
	}
	d.handles[id].access = 0x80
	if _, status := d.queryInfo(context.Background(), queryCommand(id, 1, 14, 1024)); status != statusDenied {
		t.Fatalf("position without data access %x", status)
	}
}

func TestWriteOpenAndRenameIdentityProbesRequestNoExtraRights(t *testing.T) {
	d, f, s, _ := commandDispatcher()
	root := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir}}}}
	target := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 99}}}}
	parents, probes := 0, 0
	s.open = func(r storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		if r.Access&storage.WindowsReadAttributes != 0 {
			return storage.WindowsOpenResult{}, syscall.EACCES
		}
		switch r.Lookup.Name {
		case "":
			parents++
			if r.Access != 0 {
				t.Fatalf("parent access %+v", r)
			}
			return storage.WindowsOpenResult{File: root, Attr: root.attr, CreateAction: storage.WindowsOpened}, nil
		case "file":
			if r.Access != storage.WindowsWriteData {
				t.Fatalf("final access %+v", r)
			}
			return storage.WindowsOpenResult{File: f, Attr: f.attr, CreateAction: storage.WindowsOpened}, nil
		case "target":
			probes++
			if r.Access != 0 {
				t.Fatalf("destination probe access %+v", r)
			}
			return storage.WindowsOpenResult{File: target, Attr: target.attr, CreateAction: storage.WindowsOpened}, nil
		}
		return storage.WindowsOpenResult{}, syscall.ENOENT
	}
	body, status := d.create(context.Background(), createCommand("file", 2, 1))
	if status != 0 {
		t.Fatalf("write-only open %x", status)
	}
	var id wire.FileID
	copy(id[:], body[64:80])
	d.handles[id].access = 0x10000
	name := wire.EncodeUTF16("target")
	rename := make([]byte, 20+len(name))
	rename[0] = 1
	smbLE.PutUint32(rename[16:], uint32(len(name)))
	copy(rename[20:], name)
	if _, status := d.setInfo(context.Background(), setCommand(id, 10, rename)); status != 0 || parents != 2 || probes != 1 {
		t.Fatalf("rename status%x parents%d probes%d", status, parents, probes)
	}
}

func TestRenameUsesExpectedSourceAndDestination(t *testing.T) {
	d, f, s, id := commandDispatcher()
	h := d.get(id)
	h.lookup = storage.WindowsLookup{ParentID: 1, Name: "old", ExpectedID: f.attr.ID}
	root := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir}}}}
	target := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 99}}}}
	s.open = func(r storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
		if r.Lookup.Name == "" {
			return storage.WindowsOpenResult{File: root, Attr: root.attr}, nil
		}
		return storage.WindowsOpenResult{File: target, Attr: target.attr}, nil
	}
	name := wire.EncodeUTF16("new")
	data := make([]byte, 20+len(name))
	data[0] = 1
	smbLE.PutUint32(data[16:], uint32(len(name)))
	copy(data[20:], name)
	_, status := d.handle(context.Background(), setCommand(id, 10, data))
	if status != 0 {
		t.Fatalf("rename %x", status)
	}
	if h.lookup.Name != "new" || h.lookup.ExpectedID != 7 || h.lookup.ParentReference != "" || root.closed != 1 || target.closed != 1 {
		t.Fatalf("rename identity %+v root/target %d/%d", h.lookup, root.closed, target.closed)
	}
	data[0] = 0
	_, status = d.handle(context.Background(), setCommand(id, 10, data))
	if status != 0xc0000035 {
		t.Fatalf("replace not authorized %x", status)
	}
}
func TestStorageFailuresKeepDistinctProtocolMeaning(t *testing.T) {
	cases := []struct {
		err    error
		status uint32
	}{{syscall.EACCES, 0xc0000022}, {syscall.ENOENT, 0xc0000034}, {syscall.EEXIST, 0xc0000035}, {syscall.ENOTDIR, 0xc0000103}, {syscall.EISDIR, 0xc00000ba}, {syscall.ENOTEMPTY, 0xc0000101}, {syscall.EBADF, fileClosed}, {syscall.EINVAL, fileInvalidParameter}, {syscall.EINTR, 0xc0000120}, {syscall.EOPNOTSUPP, fileNotSupported}, {syscall.ENOSPC, 0xc000007f}, {syscall.EROFS, 0xc00000a2}, {syscall.ENAMETOOLONG, 0xc0000106}, {syscall.EFBIG, 0xc0000904}, {syscall.ENOMEM, fileInsufficientResources}, {syscall.EAGAIN, 0xc000022d}, {errors.New("unknown"), fileIOError}}
	for _, tc := range cases {
		if got := statusError(tc.err); got != tc.status {
			t.Errorf("%v = %x, want %x", tc.err, got, tc.status)
		}
	}
}

func TestProtocolNativeCreateDeclinesOptionalCaching(t *testing.T) {
	c, session, tree, signer, file := compoundProtocol(t, false)
	r := createCommand("file", 0x0012019f, 5)
	r.Body[3] = 9
	smbLE.PutUint32(r.Body[4:], 2)
	smbLE.PutUint32(r.Body[28:], storage.WindowsDOSNormal)
	smbLE.PutUint32(r.Body[32:], 7)
	smbLE.PutUint32(r.Body[40:], 0x00020042)
	r = requestContexts(t, r, []wire.CreateContext{{Name: []byte("DH2Q"), Data: make([]byte, 32)}, {Name: []byte("QFid")}})
	decoded, err := r.Create()
	if err != nil {
		t.Fatal(err)
	}
	withHint, err := windowsIntent(decoded)
	if err != nil {
		t.Fatal(err)
	}
	decoded.Options &^= 0x00020000
	withoutHint, err := windowsIntent(decoded)
	if err != nil || withHint != withoutHint || withHint.Share != storage.WindowsShareAll || withHint.Kind != storage.WindowsRegularFile || withHint.Disposition != storage.WindowsOverwriteIf {
		t.Fatalf("ignored option changed open intent: %+v %+v %v", withHint, withoutHint, err)
	}
	decoded.Options |= 0x00020008
	if _, err := windowsIntent(decoded); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatal("ignored hint masked unsupported unbuffered access")
	}
	packet := requestPacket(wire.Header{Command: wire.Create, MessageID: 4, SessionID: session, TreeID: tree, Credits: 1}, r.Body)
	if err := signer.Sign(packet); err != nil {
		t.Fatal(err)
	}
	sendFrame(t, c, packet)
	response := readFrame(t, c)
	h, err := wire.ParseHeader(response)
	if err != nil || h.Status != 0 || signer.Verify(response) != nil || len(response) < 152 || response[66] != 0 {
		t.Fatalf("native CREATE response=%+v length=%d error=%v", h, len(response), err)
	}
	contextOffset := int(smbLE.Uint32(response[144:]))
	contextLength := int(smbLE.Uint32(response[148:]))
	if contextOffset != 152 || contextLength != 56 || len(response) != contextOffset+contextLength {
		t.Fatalf("native CREATE contexts offset=%d length=%d", contextOffset, contextLength)
	}
	identity := response[contextOffset:]
	if smbLE.Uint32(identity) != 0 || string(identity[16:20]) != "QFid" || smbLE.Uint64(identity[24:]) != file.attr.ID {
		t.Fatal("CREATE granted durability or omitted authoritative file identity")
	}
	var id wire.FileID
	copy(id[:], response[128:144])
	closeBody := make([]byte, 24)
	smbLE.PutUint16(closeBody, 24)
	copy(closeBody[8:], id[:])
	packet = requestPacket(wire.Header{Command: wire.Close, MessageID: 5, SessionID: session, TreeID: tree, Credits: 1}, closeBody)
	_ = signer.Sign(packet)
	sendFrame(t, c, packet)
	response = readFrame(t, c)
	h, err = wire.ParseHeader(response)
	if err != nil || h.Status != 0 || signer.Verify(response) != nil || file.closes.Load() != 1 {
		t.Fatalf("native CREATE handle did not close: %+v %v", h, err)
	}
}

func TestNativeBackupMetadataOpenPreservesAuthorizationAndAbsence(t *testing.T) {
	for _, pattern := range []struct {
		options, access, share uint32
		identity               bool
	}{{0x204042, 0x100080, 7, false}, {0x204002, 0x80, 7, true}, {0x224022, 0x100080, 0, true}} {
		t.Run(fmt.Sprintf("options_%x", pattern.options), func(t *testing.T) {
			c, session, _, _, backend, _ := testConnection(t)
			var opened []storage.WindowsOpenRequest
			backend.open = func(request storage.WindowsOpenRequest) (storage.WindowsOpenResult, error) {
				opened = append(opened, request)
				if request.Lookup.Name == "" {
					root := &commandFile{attr: storage.WindowsAttr{WindowsBasicAttr: storage.WindowsBasicAttr{Attr: storage.Attr{ID: 1, Mode: fs.ModeDir}}}}
					return storage.WindowsOpenResult{File: root, Attr: root.attr}, nil
				}
				return storage.WindowsOpenResult{}, syscall.ENOENT
			}
			request := createCommand("missing", pattern.access, 1)
			smbLE.PutUint32(request.Body[4:], 2)
			smbLE.PutUint32(request.Body[32:], pattern.share)
			smbLE.PutUint32(request.Body[40:], pattern.options)
			if pattern.identity {
				request = requestContexts(t, request, []wire.CreateContext{{Name: []byte("QFid")}})
			}
			request = signedRequest(t, session, request)
			header := request.Header
			var authorized *storage.WindowsOpenIntent
			c.server.config.Authorize = authz.AuthorizerFunc(func(_ context.Context, access authz.AccessRequest) error {
				authorized = &access.WindowsOpen
				return nil
			})
			_, status, _ := c.dispatch(t.Context(), request, request, &header)
			if status != 0xc0000034 || len(opened) != 2 || authorized == nil {
				t.Fatalf("absent metadata open status=%x backendCalls=%d intent=%+v", status, len(opened), authorized)
			}
			got := opened[1].WindowsOpenIntent
			if got != *authorized || got.Share != storage.WindowsShare(pattern.share) || !got.OpenReparsePoint || got.Disposition != storage.WindowsOpen || got.Access&storage.WindowsReadAttributes == 0 || got.Access&^(storage.WindowsReadAttributes|storage.WindowsSynchronize) != 0 {
				t.Fatalf("backup hint changed granted authority: %+v authorized=%+v", got, authorized)
			}
			opened = nil
			c.server.config.Authorize = authz.AuthorizerFunc(func(context.Context, authz.AccessRequest) error { return authz.ErrDenied })
			if _, status, _ := c.dispatch(t.Context(), request, request, &header); status != statusDenied || len(opened) != 0 {
				t.Fatal("backup hint bypassed business authorization")
			}
		})
	}
}
