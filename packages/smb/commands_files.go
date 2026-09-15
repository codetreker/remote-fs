package smb

import (
	"context"
	"crypto/rand"
	"errors"
	"io/fs"
	"math"
	"strings"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/packages/authz"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type fileHandle struct {
	identity   notificationIdentity
	file       storage.WindowsFile
	lookup     storage.WindowsLookup
	name       string
	access     uint32
	mu         sync.Mutex
	entries    []storage.WindowsEntry
	cursor     int
	pattern    string
	class      byte
	enumerated bool
	position   uint64
}

type fileDispatcher struct {
	backend     storage.WindowsStorage
	session     storage.WindowsSession
	epoch       uint64
	limits      Limits
	mu          sync.Mutex
	handles     map[wire.FileID]*fileHandle
	opening     int
	failed      bool
	fenced      bool
	cleanupErr  error
	onCleanup   func(error)
	onOpen      func(int)
	onUncertain func()
	onFence     func(int)
	retired     bool
}

func newFileDispatcher(backend storage.WindowsStorage, session storage.WindowsSession, epoch uint64, limits Limits) *fileDispatcher {
	return &fileDispatcher{backend: backend, session: session, epoch: epoch, limits: limits, handles: make(map[wire.FileID]*fileHandle)}
}
func (d *fileDispatcher) handleCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.handles) + d.opening
}
func (d *fileDispatcher) file(id wire.FileID) (storage.WindowsFile, bool) {
	h := d.get(id)
	if h == nil {
		return nil, false
	}
	return h.file, true
}
func (d *fileDispatcher) get(id wire.FileID) *fileHandle {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failed {
		return nil
	}
	return d.handles[id]
}
func (d *fileDispatcher) actionID() (storage.WindowsActionID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), d.limits.CleanupTimeout)
	defer cancel()
	status, err := d.session.Status(ctx)
	if err != nil {
		return "", err
	}
	if status.Retired || status.Fenced {
		return "", syscall.EIO
	}
	return storage.NewLockRequestID(status.ActionEpoch)
}
func statusError(err error) uint32 {
	if err == nil {
		return fileSuccess
	}
	if errors.Is(err, authz.ErrDenied) {
		return 0xc0000022
	}
	switch storage.WindowsFailureOf(err) {
	case storage.WindowsSharingViolation:
		return 0xc0000043
	case storage.WindowsLockConflict:
		return 0xc0000054
	case storage.WindowsDeletePending:
		return 0xc0000056
	case storage.WindowsRangeNotLocked:
		return 0xc000007e
	case storage.WindowsNotReparsePoint:
		return 0xc0000275
	}
	switch storage.ErrnoOf(err) {
	case syscall.EACCES, syscall.EPERM:
		return 0xc0000022
	case syscall.ENOENT:
		return 0xc0000034
	case syscall.EEXIST:
		return 0xc0000035
	case syscall.ENOTDIR:
		return 0xc0000103
	case syscall.EISDIR:
		return 0xc00000ba
	case syscall.ENOTEMPTY:
		return 0xc0000101
	case syscall.EBADF:
		return fileClosed
	case syscall.EINVAL:
		return fileInvalidParameter
	case syscall.EINTR:
		return 0xc0000120
	case syscall.ENOSYS, syscall.EOPNOTSUPP:
		return fileNotSupported
	case syscall.ENOSPC, syscall.EDQUOT:
		return 0xc000007f
	case syscall.EROFS:
		return 0xc00000a2
	case syscall.ENAMETOOLONG:
		return 0xc0000106
	case syscall.EFBIG:
		return 0xc0000904
	case syscall.ENOMEM, syscall.EMFILE, syscall.ENFILE, syscall.ENOLCK:
		return fileInsufficientResources
	case syscall.EAGAIN:
		return 0xc000022d
	default:
		return fileIOError
	}
}
func statusAction(r storage.WindowsActionResult, err error) uint32 {
	if err != nil {
		if r.Failure != "" && r.Errno != 0 && storage.ErrnoOf(err) == r.Errno {
			return statusError(&storage.WindowsError{Failure: r.Failure, Err: err})
		}
		return statusError(err)
	}
	if r.State == storage.WindowsActionCancelled {
		return statusCancelled
	}
	if r.Errno != 0 {
		return statusError(&storage.WindowsError{Failure: r.Failure, Err: r.Errno})
	}
	if r.State == storage.WindowsActionCancelled {
		return 0xc0000120
	}
	if r.State != storage.WindowsActionCompleted {
		return fileIOError
	}
	return fileSuccess
}

func windowsIntent(r wire.CreateRequest) (storage.WindowsOpenIntent, error) {
	// FILE_DISALLOW_EXCLUSIVE has no server-side meaning in SMB2 CREATE.
	// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/e8fb45c1-a03d-44ca-b7ae-47385cfd7997
	r.Options &^= 0x00020000
	// Backup intent grants no additional access: this adapter supplies no backup
	// or restore privileges, and every open retains ordinary host authorization.
	// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/8ada5fbe-db4e-49fd-aef6-20d54b748e40
	r.Options &^= 0x00004000
	if r.Options&0x8 != 0 {
		return storage.WindowsOpenIntent{}, syscall.EOPNOTSUPP
	}
	access, status := decodeAccess(r.DesiredAccess)
	if status != 0 {
		return storage.WindowsOpenIntent{}, syscall.EOPNOTSUPP
	}
	if r.Disposition > 5 || r.ShareAccess > 7 || r.Options&^uint32(0x0020186f) != 0 {
		return storage.WindowsOpenIntent{}, syscall.EOPNOTSUPP
	}
	kind := storage.WindowsAny
	if r.Options&1 != 0 {
		kind = storage.WindowsDirectory
	}
	if r.Options&0x40 != 0 {
		if kind != storage.WindowsAny {
			return storage.WindowsOpenIntent{}, syscall.EINVAL
		}
		kind = storage.WindowsRegularFile
	}
	i := storage.WindowsOpenIntent{Access: access, Share: storage.WindowsShare(r.ShareAccess), Disposition: storage.WindowsDisposition(r.Disposition + 1), Kind: kind, DeleteOnClose: r.Options&0x1000 != 0, OpenReparsePoint: r.Options&0x200000 != 0}
	return i, i.Check()
}

func (d *fileDispatcher) open(ctx context.Context, request storage.WindowsOpenRequest, id storage.WindowsActionID) (storage.WindowsOpenResult, error) {
	result, err := d.session.Open(ctx, request, id)
	if err == nil {
		if result.File == nil || result.Attr.ID == 0 {
			d.fence()
			return storage.WindowsOpenResult{}, syscall.EIO
		}
		return result, nil
	}
	if storage.ErrnoOf(err) != syscall.EIO && storage.ErrnoOf(err) != syscall.EINTR {
		return result, err
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.limits.CleanupTimeout)
	defer cancel()
	known, queryErr := d.session.QueryAction(cleanupCtx, id)
	if knownAction(known, queryErr, id) && known.State != storage.WindowsActionPending {
		if known.Errno != 0 {
			if known.Errno == syscall.ELOOP && known.Symlink != nil {
				return storage.WindowsOpenResult{}, &storage.WindowsSymlinkError{WindowsSymlinkInfo: *known.Symlink, Err: queryErr}
			}
			return storage.WindowsOpenResult{}, queryErr
		}
		if known.State == storage.WindowsActionCompleted && known.File != nil && known.Attr.ID != 0 {
			return storage.WindowsOpenResult{File: known.File, Attr: known.Attr, CreateAction: known.CreateAction}, nil
		}
		if known.State == storage.WindowsActionCancelled {
			return storage.WindowsOpenResult{}, syscall.EINTR
		}
	}
	if request.Disposition != storage.WindowsOpen && d.onUncertain != nil {
		d.onUncertain()
	}
	d.fence()
	return storage.WindowsOpenResult{}, syscall.EIO
}

// Each intermediate directory stays retained through final admission. Expected
// identities prevent a concurrently replaced name from redirecting an operation.
func (d *fileDispatcher) resolve(ctx context.Context, name string) (storage.WindowsLookup, func() error, error) {
	if strings.Contains(name, ":") {
		return storage.WindowsLookup{}, nil, syscall.EOPNOTSUPP
	}
	if strings.HasPrefix(name, "\\") || strings.ContainsAny(name, "/\x00") {
		return storage.WindowsLookup{}, nil, syscall.EINVAL
	}
	if name == "" {
		return storage.WindowsLookup{}, func() error { return nil }, nil
	}
	parts := strings.Split(name, "\\")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return storage.WindowsLookup{}, nil, syscall.EINVAL
		}
	}
	var parents []storage.WindowsFile
	cleanup := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.limits.CleanupTimeout)
		defer cancel()
		var errs []error
		for i := len(parents) - 1; i >= 0; i-- {
			id, e := d.actionID()
			if e != nil {
				errs = append(errs, e)
				continue
			}
			r, e := parents[i].Close(cleanupCtx, id)
			if s := statusAction(r, e); s != 0 {
				errs = append(errs, syscall.EIO)
			}
		}
		err := errors.Join(errs...)
		if err != nil {
			d.fence()
		}
		return err
	}
	lookup := storage.WindowsLookup{}
	for i := 0; i < len(parts); i++ {
		id, err := d.actionID()
		if err != nil {
			_ = cleanup()
			return storage.WindowsLookup{}, nil, err
		}
		r, err := d.open(ctx, storage.WindowsOpenRequest{Lookup: lookup, WindowsOpenIntent: storage.WindowsOpenIntent{Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen, Kind: storage.WindowsDirectory}}, id)
		if err != nil {
			var link *storage.WindowsSymlinkError
			if errors.As(err, &link) {
				copy := *link
				copy.Unparsed += "/" + strings.Join(parts[i:], "/")
				err = &copy
			}
			if cleanupErr := cleanup(); cleanupErr != nil {
				return storage.WindowsLookup{}, nil, cleanupErr
			}
			return storage.WindowsLookup{}, nil, err
		}
		parents = append(parents, r.File)
		lookup = storage.WindowsLookup{ParentID: r.Attr.ID, ParentReference: r.File.Reference(), Name: parts[i]}
	}
	return lookup, cleanup, nil
}

func (d *fileDispatcher) handle(ctx context.Context, r wire.Request) ([]byte, uint32) {
	d.mu.Lock()
	failed := d.failed
	d.mu.Unlock()
	if failed {
		return nil, fileIOError
	}
	switch r.Header.Command {
	case wire.Create:
		return d.create(ctx, r)
	case wire.Read:
		return d.read(ctx, r)
	case wire.Write:
		return d.write(ctx, r)
	case wire.Close, wire.Flush:
		return d.closeOrFlush(ctx, r)
	case wire.QueryDirectory:
		return d.queryDirectory(ctx, r)
	case wire.QueryInfo:
		return d.queryInfo(ctx, r)
	case wire.SetInfo:
		return d.setInfo(ctx, r)
	case wire.IOCTL:
		if _, err := r.IOCTL(); err != nil {
			return nil, fileInvalidParameter
		}
		return nil, fileNotSupported
	default:
		return nil, fileNotSupported
	}
}

func (d *fileDispatcher) create(ctx context.Context, r wire.Request) ([]byte, uint32) {
	request, err := r.Create()
	if err != nil {
		return nil, fileInvalidParameter
	}
	if request.SecurityFlags != 0 || request.Impersonation > 3 {
		return nil, fileInvalidParameter
	}
	intent, err := windowsIntent(request)
	if err != nil {
		return createFailure(err)
	}
	if request.Attributes & ^uint32(storage.WindowsSettableDOSAttributes|0x10) != 0 {
		return nil, fileNotSupported
	}
	var volumeSerial uint64
	for _, c := range request.Contexts {
		switch string(c.Name) {
		case "MxAc":
			if len(c.Data) != 0 && len(c.Data) != 8 {
				return nil, fileInvalidParameter
			}
		case "QFid":
			if len(c.Data) != 0 {
				return nil, fileInvalidParameter
			}
			state, err := d.backend.WindowsState(ctx)
			if err != nil {
				return nil, statusError(err)
			}
			if !state.Enabled || state.VolumeIdentity == "" {
				return nil, fileIOError
			}
			volumeSerial = state.VolumeSerial
		case "RqLs", "DHnQ", "DH2Q":
		default:
			return nil, fileNotSupported
		}
	}
	d.mu.Lock()
	if len(d.handles)+d.opening >= d.limits.MaxOpens {
		d.mu.Unlock()
		return nil, fileInsufficientResources
	}
	d.opening++
	d.mu.Unlock()
	defer func() { d.mu.Lock(); d.opening--; d.mu.Unlock() }()
	lookup, cleanup, err := d.resolve(ctx, request.Name)
	if err != nil {
		return createFailure(err)
	}
	id, err := d.actionID()
	if err != nil {
		_ = cleanup()
		return createFailure(err)
	}
	opened, err := d.open(ctx, storage.WindowsOpenRequest{Lookup: lookup, WindowsOpenIntent: intent, Mode: 0666, DOSAttributes: request.Attributes &^ uint32(0x10)}, id)
	closeErr := cleanup()
	if err != nil {
		return createFailure(err)
	}
	if closeErr != nil {
		d.fence()
		return nil, fileIOError
	}
	if opened.File == nil || opened.Attr.ID == 0 {
		d.fence()
		return nil, fileIOError
	}
	var fid wire.FileID
	if _, err = rand.Read(fid[:]); err != nil {
		d.fence()
		return nil, fileIOError
	}
	lookup.ParentReference = ""
	lookup.ExpectedID = opened.Attr.ID
	h := &fileHandle{identity: notificationIdentity{ID: opened.Attr.ID, Directory: opened.Attr.IsDir()}, file: opened.File, lookup: lookup, name: request.Name, access: expandAccess(request.DesiredAccess)}
	d.mu.Lock()
	if d.failed {
		d.mu.Unlock()
		d.fence()
		return nil, fileIOError
	}
	d.handles[fid] = h
	d.mu.Unlock()
	if d.onOpen != nil {
		d.onOpen(1)
	}
	b := make([]byte, 88)
	smbLE.PutUint16(b, 89)
	action := uint32(1)
	switch opened.CreateAction {
	case storage.WindowsCreated:
		action = 2
	case storage.WindowsOverwritten:
		action = 3
	case storage.WindowsSuperseded:
		action = 0
	}
	smbLE.PutUint32(b[4:], action)
	copy(b[8:40], encodeBasicInfo(opened.Attr)[:32])
	smbLE.PutUint64(b[40:], allocationSize(opened.Attr.Attr))
	if !opened.Attr.IsDir() && opened.Attr.Mode&fs.ModeSymlink == 0 {
		smbLE.PutUint64(b[48:], uint64(opened.Attr.Size))
	}
	smbLE.PutUint32(b[56:], fileAttributes(opened.Attr))
	copy(b[64:80], fid[:])
	contexts := createContexts(ctx, request, opened.Attr, volumeSerial)
	if len(contexts) != 0 {
		smbLE.PutUint32(b[80:], 152)
		smbLE.PutUint32(b[84:], uint32(len(contexts)))
		b = append(b, contexts...)
	}
	return b, fileSuccess
}

func (d *fileDispatcher) fence() {
	d.mu.Lock()
	if d.fenced || d.retired {
		d.mu.Unlock()
		return
	}
	d.fenced = true
	d.failed = true
	if d.onFence != nil {
		d.onFence(1)
	}
	d.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), d.limits.CleanupTimeout)
	defer cancel()
	err := d.session.Close(ctx)
	d.mu.Lock()
	d.cleanupErr = err
	d.mu.Unlock()
	if err != nil && d.onCleanup != nil {
		d.onCleanup(err)
	}
}

func (d *fileDispatcher) read(ctx context.Context, r wire.Request) ([]byte, uint32) {
	q, err := r.Read()
	if err == nil && (q.Flags&1 != 0 || q.Flags&^byte(3) != 0) {
		return nil, fileNotSupported
	}
	if err != nil || q.Offset > math.MaxInt64 || int64(q.Length) > int64(d.limits.MaxIOBytes) || q.Channel != 0 || len(q.ChannelInfo) > 0 {
		return nil, fileInvalidParameter
	}
	h := d.get(q.FileID)
	if h == nil {
		return nil, fileClosed
	}
	value, err := h.file.ReadAt(ctx, int64(q.Offset), int(q.Length))
	if err != nil {
		return nil, statusError(err)
	}
	if len(value.Data) > int(q.Length) || value.Attr.Size < 0 {
		return nil, fileIOError
	}
	if len(value.Data) == 0 && q.Length != 0 && int64(q.Offset) >= value.Attr.Size {
		return nil, fileEOF
	}
	if uint32(len(value.Data)) < q.MinimumCount {
		return nil, fileEOF
	}
	b := make([]byte, 16+len(value.Data))
	smbLE.PutUint16(b, 17)
	b[2] = 80
	smbLE.PutUint32(b[4:], uint32(len(value.Data)))
	copy(b[16:], value.Data)
	return b, fileSuccess
}

func (d *fileDispatcher) write(ctx context.Context, r wire.Request) ([]byte, uint32) {
	q, err := r.Write()
	if err != nil || q.Offset > math.MaxInt64 || len(q.Data) > d.limits.MaxIOBytes || q.Channel != 0 || len(q.ChannelInfo) > 0 || q.Flags&^uint32(1) != 0 {
		return nil, fileInvalidParameter
	}
	h := d.get(q.FileID)
	if h == nil {
		return nil, fileClosed
	}
	id, err := d.actionID()
	if err != nil {
		return nil, statusError(err)
	}
	result, err := h.file.WriteAt(ctx, int64(q.Offset), q.Data, id)
	if status := d.mutationResult(id, result, err); status != 0 {
		return nil, status
	}
	b := make([]byte, 16)
	smbLE.PutUint16(b, 17)
	smbLE.PutUint32(b[4:], uint32(len(q.Data)))
	return b, fileSuccess
}

func (d *fileDispatcher) mutationResult(id storage.WindowsActionID, result storage.WindowsActionResult, err error) uint32 {
	if err == nil {
		if result.Action != id || result.State == storage.WindowsActionPending {
			if d.onUncertain != nil {
				d.onUncertain()
			}
			d.fence()
			return fileIOError
		}
		return statusAction(result, nil)
	}
	if storage.ErrnoOf(err) != syscall.EIO && storage.ErrnoOf(err) != syscall.EINTR {
		return statusError(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.limits.CleanupTimeout)
	defer cancel()
	known, queryErr := d.session.QueryAction(ctx, id)
	if knownAction(known, queryErr, id) && known.State != storage.WindowsActionPending {
		return statusAction(known, queryErr)
	}
	if d.onUncertain != nil {
		d.onUncertain()
	}
	d.fence()
	return fileIOError
}

func (d *fileDispatcher) closeOrFlush(ctx context.Context, r wire.Request) ([]byte, uint32) {
	id, err := r.FileID()
	if err != nil {
		return nil, fileInvalidParameter
	}
	h := d.get(id)
	if h == nil {
		return nil, fileClosed
	}
	if r.Header.Command == wire.Flush {
		if err := h.file.Sync(ctx); err != nil {
			return nil, statusError(err)
		}
		return []byte{4, 0, 0, 0}, fileSuccess
	}
	postQuery := smbLE.Uint16(r.Body[2:])&1 != 0
	var attr storage.WindowsAttr
	if postQuery {
		// A failed optional attribute query must not retain the open. The response
		// clears POSTQUERY so the zero fields do not claim observed metadata.
		// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/f2b4183a-29c2-4fc0-a5de-fe290408f68c
		attr, err = h.file.Stat(ctx)
		postQuery = err == nil
	}
	action, err := d.actionID()
	if err != nil {
		return nil, statusError(err)
	}
	result, err := h.file.Close(ctx, action)
	if s := d.mutationResult(action, result, err); s != 0 {
		return nil, s
	}
	d.mu.Lock()
	_, existed := d.handles[id]
	delete(d.handles, id)
	d.mu.Unlock()
	if existed && d.onOpen != nil {
		d.onOpen(-1)
	}
	b := make([]byte, 60)
	smbLE.PutUint16(b, 60)
	if postQuery {
		smbLE.PutUint16(b[2:], 1)
		copy(b[8:40], encodeBasicInfo(attr)[:32])
		smbLE.PutUint64(b[40:], allocationSize(attr.Attr))
		if !attr.IsDir() && attr.Mode&fs.ModeSymlink == 0 {
			smbLE.PutUint64(b[48:], uint64(attr.Size))
		}
		smbLE.PutUint32(b[56:], fileAttributes(attr))
	}
	return b, fileSuccess
}

func (d *fileDispatcher) queryDirectory(ctx context.Context, r wire.Request) ([]byte, uint32) {
	q, err := r.QueryDirectory()
	if err != nil || q.OutputLength > uint32(d.limits.MaxIOBytes) || q.Flags&^byte(0x17) != 0 {
		return nil, fileInvalidParameter
	}
	if _, s := directoryEntry(q.Class, "", storage.WindowsAttr{}, 0); s != 0 {
		return nil, s
	}
	h := d.get(q.FileID)
	if h == nil {
		return nil, fileClosed
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if q.Flags&0x10 != 0 || q.Flags&1 != 0 {
		h.enumerated = false
		h.entries = nil
		h.cursor = 0
	}
	if !h.enumerated {
		if len(wire.EncodeUTF16(q.Pattern)) > 510 {
			return nil, fileInvalidParameter
		}
		pattern := q.Pattern
		if pattern == "" {
			pattern = "*"
		}
		if strings.ContainsAny(pattern, "/\\\x00") {
			return nil, fileInvalidParameter
		}
		list, err := storage.NewWindowsListResult(d.limits.MaxDirectoryBytes, 0, func(_ int, n int64, _ storage.WindowsBasicAttr) (int64, error) { return 256 + n*2, nil })
		if err != nil {
			return nil, statusError(err)
		}
		if err = h.file.ListBounded(ctx, list); err != nil {
			return nil, statusError(err)
		}
		entries, err := list.Entries()
		if err != nil {
			return nil, statusError(err)
		}
		h.entries = entries
		h.enumerated = true
		h.pattern = pattern
		h.class = q.Class
		h.cursor = 0
	} else if q.Class != h.class || q.Pattern != "" && q.Pattern != h.pattern {
		return nil, fileInvalidParameter
	}
	if q.Flags&4 != 0 {
		if uint64(q.Index) > uint64(len(h.entries)) {
			return nil, fileNoMoreFiles
		}
		h.cursor = int(q.Index)
	}
	cursor := h.cursor
	var out []byte
	previous := -1
	for cursor < len(h.entries) {
		e := h.entries[cursor]
		if !matchSMBPattern(h.pattern, e.Name) {
			cursor++
			continue
		}
		b, status := directoryEntry(q.Class, e.Name, storage.WindowsAttr{WindowsBasicAttr: e.Attr}, uint32(cursor))
		if status != 0 {
			return nil, status
		}
		if len(b) > int(q.OutputLength)-len(out) {
			if len(out) == 0 {
				return nil, fileBufferTooSmall
			}
			break
		}
		if previous >= 0 {
			smbLE.PutUint32(out[previous:], uint32(len(out)-previous))
		}
		previous = len(out)
		out = append(out, b...)
		cursor++
		if q.Flags&2 != 0 {
			break
		}
	}
	h.cursor = cursor
	if len(out) == 0 {
		return nil, fileNoMoreFiles
	}
	return queryResponse(out), fileSuccess
}

// DOS_STAR, DOS_QM and DOS_DOT use the SMB wildcard vocabulary. The two-row
// dynamic program bounds work and memory independently of wildcard backtracking.
func matchSMBPattern(pattern, name string) bool {
	p := []rune(strings.ToUpper(pattern))
	n := []rune(strings.ToUpper(name))
	prev := make([]bool, len(n)+1)
	prev[0] = true
	lastDot := -1
	for i, r := range n {
		if r == '.' {
			lastDot = i
		}
	}
	for _, r := range p {
		cur := make([]bool, len(n)+1)
		switch r {
		case '*':
			cur[0] = prev[0]
			for j := 1; j <= len(n); j++ {
				cur[j] = prev[j] || cur[j-1]
			}
		case '<':
			cur[0] = prev[0]
			for j := 1; j <= len(n); j++ {
				cur[j] = prev[j] || (cur[j-1] && (lastDot < 0 || j-1 < lastDot))
			}
		case '>':
			for j := 0; j <= len(n); j++ {
				if j == len(n) || n[j] == '.' {
					cur[j] = cur[j] || prev[j]
				} else {
					cur[j+1] = cur[j+1] || prev[j]
				}
			}
		case '"':
			for j := 0; j <= len(n); j++ {
				if j == len(n) {
					cur[j] = cur[j] || prev[j]
				} else if n[j] == '.' {
					cur[j+1] = cur[j+1] || prev[j]
				}
			}
		default:
			for j := 1; j <= len(n); j++ {
				cur[j] = prev[j-1] && (r == '?' || r == n[j-1])
			}
		}
		prev = cur
	}
	return prev[len(n)]
}

func (d *fileDispatcher) queryInfo(ctx context.Context, r wire.Request) ([]byte, uint32) {
	q, err := r.QueryInfo()
	if err != nil || q.OutputLength > uint32(d.limits.MaxIOBytes) || len(q.Input) > 0 || q.Flags != 0 {
		return nil, fileInvalidParameter
	}
	h := d.get(q.FileID)
	if h == nil {
		return nil, fileClosed
	}
	var attr storage.WindowsAttr
	identityOnly := q.Type == 1 && (q.Class == 6 || q.Class == 8 || q.Class == 14 || q.Class == 16 || q.Class == 59)
	if q.Type == 1 && q.Class == 14 && h.access&3 == 0 {
		return nil, statusDenied
	}
	if identityOnly || q.Type == 2 {
		if err := h.file.Sync(ctx); err != nil {
			return nil, statusError(err)
		}
		attr.ID = h.identity.ID
	} else {
		attr, err = h.file.Stat(ctx)
		if err != nil {
			return nil, statusError(err)
		}
	}
	var data []byte
	var status uint32
	switch q.Type {
	case 1:
		h.mu.Lock()
		if q.Class == 59 {
			state, err := d.backend.WindowsState(ctx)
			if err != nil {
				status = statusError(err)
			} else if !state.Enabled || state.VolumeIdentity == "" {
				status = fileIOError
			} else {
				data = make([]byte, 24)
				smbLE.PutUint64(data, state.VolumeSerial)
				smbLE.PutUint64(data[8:], attr.ID)
			}
		} else {
			data, status = encodeFileInfo(q.Class, attr, h.access, h.position)
		}
		h.mu.Unlock()
	case 2:
		data, status = d.filesystemInfo(ctx, q.Class)
	default:
		return nil, fileNotSupported
	}
	if status != 0 {
		return nil, status
	}
	if len(data) > int(q.OutputLength) {
		return nil, fileBufferTooSmall
	}
	return queryResponse(data), fileSuccess
}

func (d *fileDispatcher) filesystemInfo(ctx context.Context, class byte) ([]byte, uint32) {
	switch class {
	case 1:
		state, err := d.backend.WindowsState(ctx)
		if err != nil {
			return nil, statusError(err)
		}
		if !state.Enabled || state.VolumeIdentity == "" {
			return nil, fileIOError
		}
		label := wire.EncodeUTF16("volume")
		b := make([]byte, 18+len(label))
		smbLE.PutUint32(b[8:], uint32(state.VolumeSerial))
		smbLE.PutUint32(b[12:], uint32(len(label)))
		copy(b[18:], label)
		return b, fileSuccess
	case 11:
		b := make([]byte, 28)
		for _, offset := range []int{0, 4, 8, 12} {
			smbLE.PutUint32(b[offset:], 1)
		}
		return b, fileSuccess
	case 3, 7:
		s, err := d.backend.Space(ctx)
		if err != nil {
			return nil, statusError(err)
		}
		if !s.Coherent() {
			return nil, fileIOError
		}
		size := 24
		if class == 7 {
			size = 32
		}
		b := make([]byte, size)
		smbLE.PutUint64(b, uint64(s.Total))
		smbLE.PutUint64(b[8:], uint64(s.Avail))
		if class == 7 {
			smbLE.PutUint64(b[16:], uint64(max(s.Total-s.Used, 0)))
		}
		smbLE.PutUint32(b[size-8:], 1)
		smbLE.PutUint32(b[size-4:], 1)
		return b, fileSuccess
	case 4:
		b := make([]byte, 8)
		smbLE.PutUint32(b, 7)
		smbLE.PutUint32(b[4:], 0x10)
		return b, fileSuccess
	case 5:
		name := wire.EncodeUTF16("remote-fs")
		b := make([]byte, 12+len(name))
		smbLE.PutUint32(b, 0x2|0x4)
		smbLE.PutUint32(b[4:], 255)
		smbLE.PutUint32(b[8:], uint32(len(name)))
		copy(b[12:], name)
		return b, fileSuccess
	default:
		return nil, fileNotSupported
	}
}

func (d *fileDispatcher) setInfo(ctx context.Context, r wire.Request) ([]byte, uint32) {
	q, err := r.SetInfo()
	if err != nil {
		return nil, fileInvalidParameter
	}
	if q.Type != 1 || q.Additional != 0 {
		return nil, fileNotSupported
	}
	h := d.get(q.FileID)
	if h == nil {
		return nil, fileClosed
	}
	id, err := d.actionID()
	if err != nil {
		return nil, statusError(err)
	}
	var result storage.WindowsActionResult
	switch q.Class {
	case 4:
		if len(q.Input) != 40 {
			return nil, fileInvalidParameter
		}
		if smbLE.Uint64(q.Input[24:]) == math.MaxUint64 {
			return nil, fileNotSupported
		}
		change := storage.WindowsAttrChange{CreationTime: decodeWindowsTime(smbLE.Uint64(q.Input)), ChangeTime: decodeWindowsTime(smbLE.Uint64(q.Input[24:])), AttrChange: storage.AttrChange{AccessTime: decodeWindowsTime(smbLE.Uint64(q.Input[8:])), ModTime: decodeWindowsTime(smbLE.Uint64(q.Input[16:]))}}
		mask := smbLE.Uint32(q.Input[32:])
		if mask != 0 {
			change.DOSAttributes = &mask
		}
		if err := change.Check(); err != nil {
			return nil, statusError(err)
		}
		result, err = h.file.SetAttr(ctx, change, id)
	case 13:
		if len(q.Input) != 1 || q.Input[0] > 1 {
			return nil, fileInvalidParameter
		}
		result, err = h.file.SetDeletePending(ctx, q.Input[0] != 0, id)
	case 14:
		if len(q.Input) != 8 || smbLE.Uint64(q.Input) > math.MaxInt64 {
			return nil, fileInvalidParameter
		}
		h.mu.Lock()
		h.position = smbLE.Uint64(q.Input)
		h.mu.Unlock()
		return []byte{2, 0}, fileSuccess
	case 20:
		if len(q.Input) != 8 || smbLE.Uint64(q.Input) > math.MaxInt64 {
			return nil, fileInvalidParameter
		}
		result, err = h.file.Truncate(ctx, int64(smbLE.Uint64(q.Input)), id)
	case 10:
		if len(q.Input) < 20 || q.Input[0] > 1 || smbLE.Uint64(q.Input[8:]) != 0 || uint64(smbLE.Uint32(q.Input[16:])) != uint64(len(q.Input)-20) {
			return nil, fileInvalidParameter
		}
		name, decodeErr := wire.DecodeUTF16(q.Input[20:])
		if decodeErr != nil {
			return nil, fileInvalidParameter
		}
		destination, cleanup, resolveErr := d.resolve(ctx, name)
		if resolveErr != nil {
			return nil, statusError(resolveErr)
		}
		if destination.Name == "" {
			_ = cleanup()
			return nil, fileInvalidParameter
		}
		// Destination identities are resolved explicitly. The final rename rejects a
		// replacement admitted between this observation and its ordered mutation.
		probeID, probeErr := d.actionID()
		if probeErr != nil {
			_ = cleanup()
			return nil, statusError(probeErr)
		}
		existing, probeErr := d.open(ctx, storage.WindowsOpenRequest{Lookup: destination, WindowsOpenIntent: storage.WindowsOpenIntent{Share: storage.WindowsShareAll, Disposition: storage.WindowsOpen, OpenReparsePoint: true}}, probeID)
		if probeErr == nil {
			destination.ExpectedID = existing.Attr.ID
			closeID, e := d.actionID()
			if e != nil {
				_ = cleanup()
				d.fence()
				return nil, fileIOError
			}
			closed, e := existing.File.Close(ctx, closeID)
			if d.mutationResult(closeID, closed, e) != 0 {
				_ = cleanup()
				return nil, fileIOError
			}
		} else if storage.ErrnoOf(probeErr) != syscall.ENOENT {
			_ = cleanup()
			return nil, statusError(probeErr)
		}
		if destination.ExpectedID != 0 && q.Input[0] == 0 {
			if cleanupErr := cleanup(); cleanupErr != nil {
				return nil, fileIOError
			}
			return nil, statusError(syscall.EEXIST)
		}
		h.mu.Lock()
		source := h.lookup
		h.mu.Unlock()
		result, err = h.file.Rename(ctx, storage.WindowsRenameRequest{Source: source, Destination: destination, Replace: q.Input[0] != 0}, id)
		cleanupErr := cleanup()
		if cleanupErr != nil {
			d.fence()
			return nil, fileIOError
		}
		if status := d.mutationResult(id, result, err); status != 0 {
			return nil, status
		}
		destination.ParentReference = ""
		destination.ExpectedID = source.ExpectedID
		h.mu.Lock()
		h.lookup = destination
		h.name = name
		h.mu.Unlock()
		return []byte{2, 0}, fileSuccess
	default:
		return nil, fileNotSupported
	}
	if status := d.mutationResult(id, result, err); status != 0 {
		return nil, status
	}
	return []byte{2, 0}, fileSuccess
}

func knownAction(r storage.WindowsActionResult, err error, id storage.WindowsActionID) bool {
	if r.Action != id || r.State < storage.WindowsActionPending || r.State > storage.WindowsActionCancelled {
		return false
	}
	if r.Errno == 0 {
		return err == nil
	}
	return r.State != storage.WindowsActionPending && err != nil && storage.ErrnoOf(err) == r.Errno
}
