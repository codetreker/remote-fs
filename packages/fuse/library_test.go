// The tests in this file exercise the package without mounting anything, so they run on
// a machine with no /dev/fuse. See docs/testing.md.
package fuse

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"hash/fnv"
	iofs "io/fs"
	"math"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
)

// R-INT-2 forbids this package, linked into somebody else's process, from touching that
// process: no signal handlers, no writing to its output, no exiting it, no work at import
// time. TestNothingIsWrittenToTheProcessOutput catches the output half at run time,
// including whatever the FUSE library does behind us; this catches all of it, including
// in code that no test happens to reach.
func TestNoProcessWideStateIsTouched(t *testing.T) {
	forbidden := map[string]string{
		"os.Exit":           "exits the caller's process",
		"signal.Notify":     "registers a signal handler in the caller's process",
		"signal.Ignore":     "changes signal disposition in the caller's process",
		"signal.Reset":      "changes signal disposition in the caller's process",
		"os.Stdout":         "names the caller's standard output",
		"os.Stderr":         "names the caller's standard error",
		"log.Print":         "writes through the standard logger",
		"log.Printf":        "writes through the standard logger",
		"log.Println":       "writes through the standard logger",
		"log.Fatal":         "writes through the standard logger and exits",
		"log.Fatalf":        "writes through the standard logger and exits",
		"log.Panic":         "writes through the standard logger",
		"log.Panicf":        "writes through the standard logger",
		"log.Default":       "writes through the standard logger",
		"log.SetOutput":     "reconfigures the standard logger",
		"fmt.Print":         "writes to the caller's standard output",
		"fmt.Printf":        "writes to the caller's standard output",
		"fmt.Println":       "writes to the caller's standard output",
		"print":             "writes to the caller's standard error",
		"println":           "writes to the caller's standard error",
		"os.Setenv":         "changes the caller's environment",
		"os.Unsetenv":       "changes the caller's environment",
		"os.Chdir":          "changes the caller's working directory",
		"syscall.Umask":     "changes a process-wide setting",
		"syscall.Setrlimit": "changes a process-wide limit",
	}

	for _, file := range parsePackage(t) {
		ast.Inspect(file.node, func(n ast.Node) bool {
			ident := calleeName(n)
			if why, found := forbidden[ident]; found {
				t.Errorf("%s: %s %s, which R-INT-2 forbids", file.position(n), ident, why)
			}
			return true
		})

		for _, decl := range file.node.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.Name == "init" {
					t.Errorf("%s: an init function runs at import time, which R-INT-2 forbids", file.position(d))
				}
			case *ast.GenDecl:
				if d.Tok != token.VAR {
					continue
				}
				for _, spec := range d.Specs {
					value := spec.(*ast.ValueSpec)
					for i, v := range value.Values {
						if _, isCall := v.(*ast.CallExpr); isCall && !isNilConversion(v) {
							t.Errorf("%s: package-level %s is initialised by a call, which runs at import time",
								file.position(v), value.Names[i].Name)
						}
					}
				}
			}
		}
	}
}

type parsedFile struct {
	fset *token.FileSet
	node *ast.File
}

func (f parsedFile) position(n ast.Node) token.Position { return f.fset.Position(n.Pos()) }

func parsePackage(t *testing.T) map[string]parsedFile {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(info os.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}

	files := map[string]parsedFile{}
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			files[name] = parsedFile{fset: fset, node: file}
		}
	}
	if len(files) == 0 {
		t.Fatal("no source files were parsed; this guard would pass on an empty package")
	}
	return files
}

// calleeName reports what a call or a selector names, as "package.Name", and "" for
// anything else. Both forms matter: os.Exit is a call, os.Stderr is not.
func calleeName(n ast.Node) string {
	expr, ok := n.(ast.Expr)
	if !ok {
		return ""
	}
	if call, isCall := expr.(*ast.CallExpr); isCall {
		expr = call.Fun
	}
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		if pkg, isPkg := e.X.(*ast.Ident); isPkg {
			return pkg.Name + "." + e.Sel.Name
		}
	}
	return ""
}

// The mount hands the kernel the errno the storage produced. This mapping is where "I
// could not find out" would turn into "that file is not there" if it were sloppy.
func TestErrnoOf(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want syscall.Errno
	}{
		{"nothing went wrong", nil, 0},
		{"a bare errno", syscall.ENOENT, syscall.ENOENT},
		{"an errno inside a PathError", &os.PathError{Op: "stat", Err: syscall.EACCES}, syscall.EACCES},
		{"an errno wrapped in text", fmt.Errorf("reaching the namespace: %w", syscall.ENOSPC), syscall.ENOSPC},
		{"a canceled request", context.Canceled, syscall.EINTR},
		{"a wrapped canceled request", fmt.Errorf("stat: %w", context.Canceled), syscall.EINTR},
		{"an expired deadline", context.DeadlineExceeded, syscall.EIO},
		{"an unknown outcome with cancellation", errors.Join(syscall.EIO, context.Canceled), syscall.EIO},
		{"an error carrying no errno", errors.New("the namespace is unreachable"), syscall.EIO},
		{"a wrapped error carrying no errno", fmt.Errorf("dialling: %w", errors.New("no route")), syscall.EIO},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := errnoOf(c.err); got != c.want {
				t.Fatalf("errnoOf(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// isNilConversion reports whether an expression is `(*T)(nil)`, the compile-time
// interface assertion. It is a conversion rather than a call and does nothing when the
// package is loaded, so it is not the import-time work this guard is looking for.
func isNilConversion(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}
	if _, parenthesised := call.Fun.(*ast.ParenExpr); !parenthesised {
		return false
	}
	arg, ok := call.Args[0].(*ast.Ident)
	return ok && arg.Name == "nil"
}

// The kinds a namespace can report have to reach the kernel as themselves. A kind we
// cannot name is refused rather than presented as an ordinary file, because presenting
// it would invite reads and writes that cannot mean what they appear to.
func TestSystemMode(t *testing.T) {
	for _, c := range []struct {
		name string
		mode iofs.FileMode
		want uint32
	}{
		{"a file", 0o644, syscall.S_IFREG | 0o644},
		{"a directory", iofs.ModeDir | 0o755, syscall.S_IFDIR | 0o755},
		{"a symbolic link", iofs.ModeSymlink | 0o777, syscall.S_IFLNK | 0o777},
		{"a named pipe", iofs.ModeNamedPipe | 0o600, syscall.S_IFIFO | 0o600},
		{"a socket", iofs.ModeSocket | 0o600, syscall.S_IFSOCK | 0o600},
		{"a block device", iofs.ModeDevice | 0o600, syscall.S_IFBLK | 0o600},
		{"a character device", iofs.ModeDevice | iofs.ModeCharDevice | 0o600, syscall.S_IFCHR | 0o600},

		// The three beyond the permission bits are settable, so they have to be reported
		// as well as accepted. A mount that dropped them would answer a chmod that
		// succeeded with the mode the file had before it.
		{"a setuid file", iofs.ModeSetuid | 0o755, syscall.S_IFREG | syscall.S_ISUID | 0o755},
		{"a setgid directory", iofs.ModeDir | iofs.ModeSetgid | 0o2775, syscall.S_IFDIR | syscall.S_ISGID | 0o775},
		{"a sticky directory", iofs.ModeDir | iofs.ModeSticky | 0o777, syscall.S_IFDIR | syscall.S_ISVTX | 0o777},
		{"all three at once", iofs.ModeSetuid | iofs.ModeSetgid | iofs.ModeSticky | 0o700,
			syscall.S_IFREG | syscall.S_ISUID | syscall.S_ISGID | syscall.S_ISVTX | 0o700},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, errno := systemMode(c.mode)
			if errno != 0 {
				t.Fatalf("systemMode(%v) failed with %v", c.mode, errno)
			}
			if got != c.want {
				t.Fatalf("systemMode(%v) = %o, want %o", c.mode, got, c.want)
			}
		})
	}

	for _, mode := range []iofs.FileMode{iofs.ModeIrregular, iofs.ModeIrregular | iofs.ModeDir} {
		if _, errno := systemMode(mode); errno != syscall.EIO {
			t.Errorf("systemMode(%v) failed with %v, want EIO", mode, errno)
		}
	}
}

// The way back. A mode arrives from the kernel with the node's kind still in it, and the
// kind is dropped rather than translated: what comes back is a mode to set, and a node's
// kind is not something a caller sets.
func TestNamespaceMode(t *testing.T) {
	for _, c := range []struct {
		name string
		mode uint32
		want iofs.FileMode
	}{
		{"a file's permissions", syscall.S_IFREG | 0o644, 0o644},
		{"a directory's permissions", syscall.S_IFDIR | 0o755, 0o755},
		{"no permissions at all", syscall.S_IFREG, 0},
		{"setuid", syscall.S_IFREG | syscall.S_ISUID | 0o755, iofs.ModeSetuid | 0o755},
		{"setgid", syscall.S_IFDIR | syscall.S_ISGID | 0o2775, iofs.ModeSetgid | 0o775},
		{"sticky", syscall.S_IFDIR | syscall.S_ISVTX | 0o1777, iofs.ModeSticky | 0o777},
		{"all three at once", syscall.S_ISUID | syscall.S_ISGID | syscall.S_ISVTX | 0o700,
			iofs.ModeSetuid | iofs.ModeSetgid | iofs.ModeSticky | 0o700},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := namespaceMode(c.mode); got != c.want {
				t.Fatalf("namespaceMode(%o) = %v, want %v", c.mode, got, c.want)
			}
			if got := namespaceMode(c.mode); got&^storage.SettableMode != 0 {
				t.Fatalf("namespaceMode(%o) = %v, which the contract will refuse", c.mode, got)
			}
		})
	}
}

func TestRetainedHandlePreservesReadWriteAndTruncateBoundaries(t *testing.T) {
	h := aHandle(t, []byte("payload"), 1<<20)
	dest := make([]byte, 16)

	for _, c := range []struct {
		name string
		off  int64
		want string
	}{
		{"from the beginning", 0, "payload"},
		{"from the middle", 4, "oad"},
		{"from exactly the end", 7, ""},
		{"from past the end", 100, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			result, errno := h.Read(t.Context(), dest, c.off)
			if errno != 0 {
				t.Fatalf("reading at %d failed with %v", c.off, errno)
			}
			got, status := result.Bytes(dest)
			if !status.Ok() {
				t.Fatal(status)
			}
			if string(got) != c.want {
				t.Fatalf("reading at %d gave %q, want %q", c.off, got, c.want)
			}
		})
	}

	t.Run("a write past the end leaves zeroes", func(t *testing.T) {
		if _, errno := h.Write(t.Context(), []byte("tail"), 10); errno != 0 {
			t.Fatalf("writing failed with %v", errno)
		}
		result, errno := h.Read(t.Context(), dest, 0)
		if errno != 0 {
			t.Fatalf("reading failed with %v", errno)
		}
		got, status := result.Bytes(dest)
		if !status.Ok() {
			t.Fatal(status)
		}
		if string(got) != "payload\x00\x00\x00tail" {
			t.Fatalf("the file holds %q, want the gap filled with zeroes", got)
		}
	})

	t.Run("shortening and lengthening", func(t *testing.T) {
		if errno := errnoOf(h.resize(t.Context(), 3)); errno != 0 {
			t.Fatalf("shortening failed with %v", errno)
		}
		if errno := errnoOf(h.resize(t.Context(), 5)); errno != 0 {
			t.Fatalf("lengthening failed with %v", errno)
		}
		result, errno := h.Read(t.Context(), dest, 0)
		if errno != 0 {
			t.Fatalf("reading failed with %v", errno)
		}
		got, status := result.Bytes(dest)
		if !status.Ok() {
			t.Fatal(status)
		}
		if string(got) != "pay\x00\x00" {
			t.Fatalf("the file holds %q, want %q", got, "pay\x00\x00")
		}
	})
}

func activeTestNamespace(s storage.Storage, maxFileSize int64) *namespace {
	return &namespace{storage: s, maxFileSize: maxFileSize, flushTimeout: DefaultFlushTimeout,
		deadline: time.Now().Add(time.Hour), stop: make(chan struct{}), done: make(chan struct{})}
}

func aHandle(t *testing.T, contents []byte, maxFileSize int64) *handle {
	t.Helper()
	return aHandleWithAllowance(t, contents, maxFileSize, 0)
}

func aHandleWithAllowance(t *testing.T, contents []byte, maxFileSize, allowance int64) *handle {
	t.Helper()
	_, backing := memoryfixture.New(t, "handle", allowance, locking.DefaultOptions())
	if err := backing.Write(t.Context(), "file", contents); err != nil {
		t.Fatal(err)
	}
	session, err := backing.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Errorf("close handle session: %v", err)
		}
	})
	file, err := session.OpenFile(t.Context(), "file", storage.FileOpenOptions{Read: true, Write: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := file.Close(ctx); err != nil {
			t.Errorf("close retained test file: %v", err)
		}
	})
	attr, err := file.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ns := activeTestNamespace(unmeasured{}, maxFileSize)
	ns.files = session
	n := &node{ns: ns, id: rootIdentity(math.MaxUint64).child("file", syscall.S_IFREG, attr.ID)}
	return newHandle(n, file, true, true)
}

func retainedContents(t *testing.T, h *handle) []byte {
	t.Helper()
	read, err := h.file.ReadAt(t.Context(), 0, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return read.Data
}

// Descriptor data goes through File; this separate namespace supplies only the
// optional capacity estimate used before a growth request.
type unmeasured struct{ storage.Storage }

func (unmeasured) Space(context.Context) (storage.Space, error) {
	return storage.Space{}, syscall.ENOSYS
}

func TestRetainedHandleRefusesGrowthPastTheCeiling(t *testing.T) {
	const ceiling = 64

	for _, c := range []struct {
		name string
		act  func(h *handle) syscall.Errno
	}{
		{"a resize to exactly the ceiling", func(h *handle) syscall.Errno { return errnoOf(h.resize(t.Context(), ceiling)) }},
		{"a write ending exactly at the ceiling", func(h *handle) syscall.Errno {
			_, errno := h.Write(t.Context(), make([]byte, 8), ceiling-8)
			return errno
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if errno := c.act(aHandle(t, []byte("payload"), ceiling)); errno != 0 {
				t.Fatalf("%s failed with %v; the ceiling is the largest size that fits", c.name, errno)
			}
		})
	}

	for _, c := range []struct {
		name string
		act  func(h *handle) syscall.Errno
	}{
		{"a resize one byte past the ceiling", func(h *handle) syscall.Errno { return errnoOf(h.resize(t.Context(), ceiling+1)) }},
		{"a resize well past the ceiling", func(h *handle) syscall.Errno { return errnoOf(h.resize(t.Context(), 1<<20)) }},
		{"a write ending one byte past the ceiling", func(h *handle) syscall.Errno {
			_, errno := h.Write(t.Context(), make([]byte, 8), ceiling-7)
			return errno
		}},
		{"a write starting well past the ceiling", func(h *handle) syscall.Errno {
			_, errno := h.Write(t.Context(), []byte("tail"), 1<<20)
			return errno
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := aHandle(t, []byte("payload"), ceiling)
			if errno := c.act(h); errno != syscall.EFBIG {
				t.Fatalf("%s returned %v, want EFBIG", c.name, errno)
			}
			if got := retainedContents(t, h); string(got) != "payload" {
				t.Fatalf("the file changed after a refused request: %q", got)
			}
		})
	}

	// Every size above is one a mount with no ceiling would simply allocate, so a lost
	// ceiling shows up as a failed case. The two below are not: the first asks for a
	// terabyte, which Go answers with a fatal error that no recover catches, and the
	// second overflows int64 into a negative length. Neither can be allowed to run
	// before the ceiling has been shown to hold at a harmless size.
	t.Run("sizes that cannot safely be attempted without the ceiling", func(t *testing.T) {
		if errno := errnoOf(aHandle(t, nil, ceiling).resize(t.Context(), ceiling+1)); errno != syscall.EFBIG {
			t.Fatalf("the ceiling returned %v at %d bytes; the larger sizes are not attempted",
				errno, ceiling+1)
		}
		if errno := errnoOf(aHandle(t, nil, ceiling).resize(t.Context(), 1<<40)); errno != syscall.EFBIG {
			t.Fatalf("a resize to 1 TiB returned %v, want EFBIG", errno)
		}
		// off is whatever the caller seeked to, so off+len(data) is where int64 runs out.
		// Forming that sum before comparing it wraps to a negative number, and a negative
		// end is below every ceiling.
		if _, errno := aHandle(t, nil, ceiling).Write(t.Context(), []byte("tail"), math.MaxInt64-1); errno != syscall.EFBIG {
			t.Fatalf("a write at an offset that overflows int64 returned %v, want EFBIG", errno)
		}
	})
}

// The bits the kernel sends were measured rather than assumed, on Linux 6.8, by recording
// every request one run of the everyday tools produced: chmod arrives as FATTR_MODE alone
// with the node's kind still in the mode word, chown as FATTR_UID and FATTR_GID together
// or either alone, utimensat as FATTR_ATIME|FATTR_MTIME or either alone, touch with no
// time given as both with the _NOW bits added, truncate(2) as FATTR_SIZE|FATTR_LOCKOWNER,
// and ftruncate(2) as that with FATTR_FH. Nothing observed carried FATTR_CTIME or
// FATTR_KILL_SUIDGID.
//
// Size is handled separately, so the change reported here never names it. A request this
// filesystem cannot carry out has to be refused rather than accepted and dropped.
func TestTheChangeARequestAsksFor(t *testing.T) {
	const (
		uid = 1000
		gid = 2000
	)
	owner := gofuse.Owner{Uid: uid, Gid: gid}
	chosen := time.Unix(1755000000, 123456789)

	for _, c := range []struct {
		name  string
		in    gofuse.SetAttrIn
		want  storage.AttrChange
		errno syscall.Errno
	}{
		{
			name: "truncate(2) asks for no attribute",
			in:   gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: gofuse.FATTR_SIZE | gofuse.FATTR_LOCKOWNER}},
		},
		{
			name: "ftruncate(2) asks for no attribute",
			in: gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid: gofuse.FATTR_SIZE | gofuse.FATTR_LOCKOWNER | gofuse.FATTR_FH}},
		},
		{
			name: "chmod",
			in: gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid: gofuse.FATTR_MODE, Mode: syscall.S_IFREG | 0o640}},
			want: storage.AttrChange{Mode: aMode(0o640)},
		},
		{
			// The kernel keeps these three in the mode word beside the permission bits, and
			// they are settable, so they have to arrive as themselves rather than be lost
			// on the way in.
			name: "chmod with the setuid, setgid and sticky bits",
			in: gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid: gofuse.FATTR_MODE,
				Mode:  syscall.S_IFREG | syscall.S_ISUID | syscall.S_ISGID | syscall.S_ISVTX | 0o755}},
			want: storage.AttrChange{
				Mode: aMode(0o755 | iofs.ModeSetuid | iofs.ModeSetgid | iofs.ModeSticky)},
		},
		{
			name: "utimensat with both times chosen",
			in: gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid:     gofuse.FATTR_ATIME | gofuse.FATTR_MTIME,
				Atime:     uint64(chosen.Unix()),
				Atimensec: uint32(chosen.Nanosecond()),
				Mtime:     uint64(chosen.Unix()),
				Mtimensec: uint32(chosen.Nanosecond())}},
			want: storage.AttrChange{AccessTime: &chosen, ModTime: &chosen},
		},
		{
			name: "utimensat with only the modification time",
			in: gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid:     gofuse.FATTR_MTIME,
				Mtime:     uint64(chosen.Unix()),
				Mtimensec: uint32(chosen.Nanosecond())}},
			want: storage.AttrChange{ModTime: &chosen},
		},
		{
			// The mount reports every node as belonging to whoever made the mount, so this
			// asks for what is already the case. Refusing it would refuse cp -p and tar -x,
			// and answering it claims nothing that is not true.
			name: "chown to the owner the mount already reports",
			in: gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid: gofuse.FATTR_UID | gofuse.FATTR_GID, Owner: owner}},
		},
		{
			name: "chown to another user",
			in: gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid: gofuse.FATTR_UID, Owner: gofuse.Owner{Uid: uid + 1, Gid: gid}}},
			errno: syscall.EPERM,
		},
		{
			name: "chgrp to another group",
			in: gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
				Valid: gofuse.FATTR_GID, Owner: gofuse.Owner{Uid: uid, Gid: gid + 1}}},
			errno: syscall.EPERM,
		},
		{
			// The kernel only asks for this of a filesystem that negotiated
			// CAP_HANDLE_KILLPRIV_V2, which the library beneath this one never does, so it
			// clears the two bits itself with an ordinary mode change. If it ever did
			// arrive it would be a change, and dropping it would leave a setuid bit on a
			// file the kernel had decided should lose it.
			name:  "a request to clear the setuid and setgid bits",
			in:    gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: gofuse.FATTR_KILL_SUIDGID}},
			errno: syscall.EPERM,
		},
		{
			name:  "a bit this filesystem has never heard of",
			in:    gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{Valid: 1 << 30}},
			errno: syscall.EPERM,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ns := &namespace{owner: owner}
			got, errno := ns.requestedChange(&c.in)
			if errno != c.errno {
				t.Fatalf("the request failed with %v, want %v", errno, c.errno)
			}
			if errno != 0 {
				return
			}
			if !sameChange(got, c.want) {
				t.Fatalf("the request asks for %s, want %s", describeChange(got), describeChange(c.want))
			}
		})
	}
}

// A change time accompanies a change rather than being one: no system call sets it, and
// the kernel attaches it to say what follows from what it is already asking for. This is
// the request a kernel that does not carry O_TRUNC into Open sends instead, and refusing
// it would leave that kernel unable to truncate anything.
func TestAChangeTimeIsNotARequestOfItsOwn(t *testing.T) {
	in := gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		Valid: gofuse.FATTR_SIZE | gofuse.FATTR_ATIME | gofuse.FATTR_ATIME_NOW |
			gofuse.FATTR_MTIME | gofuse.FATTR_MTIME_NOW | gofuse.FATTR_CTIME,
	}}
	ns := &namespace{owner: gofuse.Owner{}}
	got, errno := ns.requestedChange(&in)
	if errno != 0 {
		t.Fatalf("the request failed with %v", errno)
	}
	if got.Mode != nil || got.AccessTime == nil || got.ModTime == nil {
		t.Fatalf("the request asks for %s, want both times and no mode", describeChange(got))
	}
	// The kernel asked for "now" rather than for an instant of its own choosing, and what
	// it gets has to be around now rather than the epoch.
	if since := time.Since(*got.ModTime); since < 0 || since > time.Minute {
		t.Fatalf("the modification time asked for is %v, which is not now", got.ModTime)
	}
}

func aMode(m iofs.FileMode) *iofs.FileMode { return &m }

func sameChange(got, want storage.AttrChange) bool {
	sameTime := func(a, b *time.Time) bool {
		return (a == nil) == (b == nil) && (a == nil || a.Equal(*b))
	}
	sameMode := (got.Mode == nil) == (want.Mode == nil) && (got.Mode == nil || *got.Mode == *want.Mode)
	return sameMode && sameTime(got.AccessTime, want.AccessTime) && sameTime(got.ModTime, want.ModTime)
}

func describeChange(c storage.AttrChange) string {
	parts := []string{}
	if c.Mode != nil {
		parts = append(parts, fmt.Sprintf("mode %v", *c.Mode))
	}
	if c.AccessTime != nil {
		parts = append(parts, fmt.Sprintf("accessed %v", c.AccessTime.UTC()))
	}
	if c.ModTime != nil {
		parts = append(parts, fmt.Sprintf("changed %v", c.ModTime.UTC()))
	}
	if len(parts) == 0 {
		return "nothing"
	}
	return strings.Join(parts, ", ")
}

// A ceiling below zero admits no file at all, so it is a mistake rather than a policy, and
// it is reported where the mistake was made instead of turning every later operation into
// EFBIG. Nothing is mounted and the storage is never reached, which is why both can be
// left out here.
func TestACeilingThatAdmitsNothingIsRefused(t *testing.T) {
	m, err := New(t.TempDir(), nil, Options{MaxFileSize: -1})
	cleanupReturnedTestMount(t, m)
	if err == nil {
		t.Fatal("mounting with a negative MaxFileSize succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "MaxFileSize") {
		t.Fatalf("mounting with a negative MaxFileSize failed with %v; the message does not name the option", err)
	}
}

func cleanupReturnedTestMount(t *testing.T, m *Mount) {
	t.Helper()
	if m == nil {
		return
	}
	t.Cleanup(func() {
		if err := m.Unmount(); err != nil {
			t.Errorf("close unexpectedly returned mount: %v", err)
		}
	})
}

// named resolves a name whose node has not changed. Every case below about the record's
// own behaviour wants that: a stand-in identity, stable across calls and distinct between
// names, so that a case about names is not also a case about a node being replaced. The
// cases that are about that pass their own.
func (i *identity) named(name string, mode uint32) *identity {
	return i.child(name, mode, stableNode(name))
}

func stableNode(name string) uint64 {
	sum := fnv.New64a()
	sum.Write([]byte(name))
	return sum.Sum64()
}

// The record of which node each name refers to turns the namespace's identity for a node
// into the number the kernel knows it by. These reach it directly, because several of its
// cases — one
// name resolved twice at the same time, a name whose identity was dropped between a lookup
// and the rename that follows it, a name that appears while a listing is in flight — are
// races through a mountpoint and plain calls here.
func TestOneNameKeepsOneIdentity(t *testing.T) {
	root := rootIdentity(math.MaxUint64)

	first := root.named("f", syscall.S_IFREG)
	if again := root.named("f", syscall.S_IFREG); again != first {
		t.Fatalf("the same name resolved to %d and then to %d", first.ino, again.ino)
	}
	if other := root.named("g", syscall.S_IFREG); other.ino == first.ino {
		t.Fatalf("two names share the number %d", other.ino)
	}
	if root.ino == first.ino {
		t.Fatalf("the root and a file beneath it share the number %d", root.ino)
	}
}

// Two lookups of one name resolve it at the same time whenever two programs reach for it
// at once, and they have to agree: a name that resolved to two numbers would be two nodes.
func TestOneNameResolvedAtOnceKeepsOneIdentity(t *testing.T) {
	root := rootIdentity(math.MaxUint64)

	const resolvers = 32
	resolved := make([]*identity, resolvers)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	ready.Add(resolvers)
	done.Add(resolvers)
	for i := range resolved {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			resolved[i] = root.named("f", syscall.S_IFREG)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	for _, id := range resolved[1:] {
		if id != resolved[0] {
			t.Fatalf("resolving one name at once gave %d and %d", resolved[0].ino, id.ino)
		}
	}
}

// A name whose node has been replaced by one of another kind refers to a different node,
// and the node that left may still be open.
func TestANameThatChangesKindChangesIdentity(t *testing.T) {
	root := rootIdentity(math.MaxUint64)

	asFile := root.child("x", syscall.S_IFREG, 41)
	asDir := root.child("x", syscall.S_IFDIR, 42)
	if asDir.ino == asFile.ino {
		t.Fatalf("a name that became a directory kept the number %d it had as a file", asFile.ino)
	}
	if asDir.children == nil {
		t.Fatal("a directory's identity has nowhere to hold the identities beneath it")
	}
}

func TestAnIdentityIsNeverHandedOutTwice(t *testing.T) {
	root := rootIdentity(math.MaxUint64)
	seen := map[uint64]string{root.ino: "the root"}

	record := func(what string, id *identity) {
		t.Helper()
		if previous, ok := seen[id.ino]; ok {
			t.Fatalf("%s was given %d, which already named %s", what, id.ino, previous)
		}
		seen[id.ino] = what
	}

	record("f", root.named("f", syscall.S_IFREG))
	root.forget("f")
	record("f again", root.child("f", syscall.S_IFREG, stableNode("replacement-f")))

	d := root.named("d", syscall.S_IFDIR)
	record("d", d)
	record("d/x", d.named("x", syscall.S_IFREG))
	root.move("d", root, "moved")
	record("d again", root.child("d", syscall.S_IFDIR, stableNode("replacement-d")))
}

// A directory carries the identities beneath it, and leaves nothing behind at the name it
// came from.
// The identities are written out here rather than derived from the names, because a rename
// is the one thing that separates the two: the node that arrives at "onto" is the node that
// was at "from", and it keeps the identity it had. A stand-in derived from the name would be
// asserting the opposite of what a rename means.
func TestMovingADirectoryCarriesWhatIsBeneathIt(t *testing.T) {
	const (
		mover  = 1 // the directory that is renamed
		sub    = 2 // a directory beneath it
		buried = 3 // a file beneath that
		onto   = 4 // the directory the rename lands on top of
	)
	root := rootIdentity(math.MaxUint64)
	from := root.child("from", syscall.S_IFDIR, mover)
	deep := from.child("sub", syscall.S_IFDIR, sub).child("f", syscall.S_IFREG, buried)
	doomed := root.child("onto", syscall.S_IFDIR, onto)

	root.move("from", root, "onto")

	moved := root.child("onto", syscall.S_IFDIR, mover)
	if moved != from {
		t.Fatal("the destination does not refer to the node that moved there")
	}
	if moved.ino == doomed.ino {
		t.Fatalf("what arrived kept the number %d of what it replaced", doomed.ino)
	}
	if carried := moved.child("sub", syscall.S_IFDIR, sub).child("f", syscall.S_IFREG, buried); carried != deep {
		t.Fatal("a file beneath the directory did not move with it")
	}
	if fresh := root.child("from", syscall.S_IFDIR, 5); fresh == from || fresh.ino == from.ino {
		t.Fatal("the name the directory left still refers to it")
	}
}

// A rename whose source this mount never named still clears the destination: something was
// there, and it is gone.
func TestMovingANameThatWasNeverResolvedStillClearsTheDestination(t *testing.T) {
	root := rootIdentity(math.MaxUint64)
	doomed := root.named("onto", syscall.S_IFREG)

	root.move("never-resolved", root, "onto")

	if fresh := root.child("onto", syscall.S_IFREG, stableNode("replacement")); fresh.ino == doomed.ino {
		t.Fatalf("the destination still refers to %d, the node the rename replaced", doomed.ino)
	}
	if _, named := root.children["never-resolved"]; named {
		t.Fatal("the source name was given an identity by being renamed away from")
	}
}

// A listing is the whole truth about one directory, so it is where a name removed by
// somebody this mount never heard from is noticed. A name that appears while the listing
// is in flight is not: the listing was taken before that name existed, and dropping it
// would give the node that has it a second identity on the next lookup.
func TestAListingDropsTheNamesTheDirectoryNoLongerHas(t *testing.T) {
	root := rootIdentity(math.MaxUint64)
	gone := root.named("gone", syscall.S_IFDIR)
	gone.named("beneath", syscall.S_IFREG)
	kept := root.named("kept", syscall.S_IFREG)

	before := root.given()
	appeared := root.named("appeared", syscall.S_IFREG)
	root.keepOnly(map[string]struct{}{"kept": {}}, before)

	if root.named("kept", syscall.S_IFREG) != kept {
		t.Fatal("a name the listing holds lost its identity")
	}
	if root.named("appeared", syscall.S_IFREG) != appeared {
		t.Fatal("a name that appeared while the listing was in flight lost its identity")
	}
	if again := root.child("gone", syscall.S_IFDIR, stableNode("replacement-gone")); again == gone {
		t.Fatal("a name the listing does not hold kept its identity")
	} else if again.child("beneath", syscall.S_IFREG, stableNode("replacement-beneath")).ino == gone.children["beneath"].ino {
		t.Fatal("a name beneath the one that went kept its identity")
	}
}

func TestIdentityFollowsNativeIDAtAPreviouslyUnseenName(t *testing.T) {
	root := rootIdentity(math.MaxUint64)
	before := root.child("before", syscall.S_IFREG, 42)
	after := root.child("after", syscall.S_IFREG, 42)
	if before.ino != 42 || after.ino != before.ino {
		t.Fatalf("same native node at another name changed inode: %d -> %d", before.ino, after.ino)
	}
}

type forbiddenCapacityProbe struct{ storage.Storage }

func (forbiddenCapacityProbe) Space(context.Context) (storage.Space, error) {
	panic("descriptor mutation must use authoritative File quota enforcement")
}

func TestDescriptorMutationsDoNotProbeNamespaceCapacity(t *testing.T) {
	h := aHandle(t, []byte("contents"), 1<<20)
	h.node.ns.storage = forbiddenCapacityProbe{}
	if n, errno := h.Write(t.Context(), []byte("growth"), 8); errno != 0 || n != 6 {
		t.Fatalf("write returned %d, %v", n, errno)
	}
	if err := h.resize(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	if err := h.resize(t.Context(), 3); err != nil {
		t.Fatal(err)
	}
	if got := retainedContents(t, h); string(got) != "con" {
		t.Fatalf("write and truncation left %q", got)
	}
}

// A name whose node has been replaced refers to a different node, and must not keep the
// number the kernel already holds attributes and cached pages against. This is R-FS-5, and
// it is the case the record could not answer while it was keyed by name alone: the
// operations that update it are this mount's own, so a rename over the name by another
// mount — or by a client that is not a mount at all — reached none of them.
//
// Nothing about the name changes here, and neither does the kind. Only the identity the
// namespace reports does, which is exactly what arrives from a change this mount did not
// make.
func TestANameWhoseNodeWasReplacedGetsANewIdentity(t *testing.T) {
	root := rootIdentity(math.MaxUint64)

	before := root.child("doc.txt", syscall.S_IFREG, 42)
	after := root.child("doc.txt", syscall.S_IFREG, 43)
	if after.ino == before.ino {
		t.Fatalf("a name holding a different node kept the number %d, so the kernel has two nodes under one", before.ino)
	}
	if again := root.child("doc.txt", syscall.S_IFREG, 43); again != after {
		t.Fatalf("the replacement resolved to %d and then to %d", after.ino, again.ino)
	}
	// The number the node that left had is not handed out again, so nothing still holding
	// it finds it naming somebody else's node.
	if third := root.child("other", syscall.S_IFREG, 44); third.ino == before.ino {
		t.Fatalf("the number %d that named the replaced node was given to another name", before.ino)
	}
}

// The same node under the same name keeps its number, which is the other half of R-FS-5 and
// the half a record that minted a fresh number every time would fail. A mount that renumbered
// a file on every lookup would break everything that remembers an inode across two calls.
func TestANameWhoseNodeIsUnchangedKeepsItsIdentity(t *testing.T) {
	root := rootIdentity(math.MaxUint64)

	first := root.child("f", syscall.S_IFREG, 7)
	for range 3 {
		if again := root.child("f", syscall.S_IFREG, 7); again != first {
			t.Fatalf("an unchanged node was renumbered from %d to %d", first.ino, again.ino)
		}
	}
	// And it survives a rename, because a rename changes a name and not a node.
	root.move("f", root, "g")
	if moved := root.child("g", syscall.S_IFREG, 7); moved != first {
		t.Fatalf("a renamed node came back as %d, want the %d it had", moved.ino, first.ino)
	}
}

// A directory replaced by another directory is a different node, and the identities beneath
// the old one go with it. Keeping them would leave the children of a directory nobody can
// reach holding numbers the kernel still has.
func TestAReplacedDirectoryDoesNotKeepWhatWasBeneathIt(t *testing.T) {
	root := rootIdentity(math.MaxUint64)

	before := root.child("d", syscall.S_IFDIR, 10)
	beneath := before.child("f", syscall.S_IFREG, 11)

	after := root.child("d", syscall.S_IFDIR, 20)
	if after.ino == before.ino {
		t.Fatalf("a replaced directory kept the number %d", before.ino)
	}
	if fresh := after.child("f", syscall.S_IFREG, 21); fresh.ino == beneath.ino {
		t.Fatalf("a name under the replacement kept %d, the number it had under the directory that left", beneath.ino)
	}
}
