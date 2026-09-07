// Package metastoretest holds the contract that every metastore.Store implementation must
// satisfy. It is the executable form of the obligations written in the metastore package's
// documentation: an implementation is correct exactly when it passes Run.
//
// It is kept here rather than beside any one implementation for the reason storagetest is:
// a contract with one implementation is a description of that implementation. The
// obligations below are the ones a database has to decide for itself, and they are the ones
// a local directory gets from the kernel's path resolution for free — ENOTDIR for a
// component that is a file, ENOENT for a missing parent, ENOTEMPTY for a directory that
// still holds names. Each of those is a query and a branch that somebody wrote, and a wrong
// one is not a crash but a lie.
package metastoretest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// NewStore produces a fresh, empty namespace held under an allowance of allowance bytes, or
// under no allowance at all when allowance is zero. Run calls it once per case, so cases
// never observe each other's writes.
type NewStore func(t *testing.T, allowance int64) metastore.Store

// Run exercises the whole contract.
func Run(t *testing.T, newStore NewStore) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.run(t, newStore(t, c.allowance))
		})
	}
}

// testCase is one obligation. allowance is zero for every case that is not about the
// allowance, which is also the namespace whose Space refuses to answer.
type testCase struct {
	name      string
	allowance int64
	run       func(t *testing.T, s metastore.Store)
}

// rootNames is every way of naming the root. storage.CleanPath maps all of them to the
// empty string, and an implementation that refuses the root has to refuse it however it is
// spelled rather than by comparing against one spelling.
var rootNames = []string{"", ".", "./", "a/.."}

// cases is every obligation, grouped by the part of the contract it comes from.
var cases = slices.Concat(rootCases, pathCases, nodeCases, attrCases, renameCases, objectCases, spaceCases,
	logCases, renameLogCases, snapshotCases, sinceCases)

var rootCases = []testCase{
	{name: "root is a directory, however it is named", run: func(t *testing.T, s metastore.Store) {
		for _, p := range rootNames {
			node, err := s.Stat(ctx(t), p)
			if err != nil {
				t.Fatalf("stat %q: %v", p, err)
			}
			if !node.IsDir() {
				t.Fatalf("stat %q has mode %v, want a directory", p, node.Mode)
			}
			if node.Content != "" {
				t.Fatalf("stat %q references object %q, want a directory to reference nothing", p, node.Content)
			}
		}
	}},

	{name: "a fresh namespace is empty", run: func(t *testing.T, s metastore.Store) {
		children, err := s.List(ctx(t), "")
		mustSucceed(t, err)
		if len(children) != 0 {
			t.Fatalf("fresh namespace lists %v, want nothing", names(children))
		}
	}},

	// EBUSY rather than ENOTEMPTY or EPERM: the root is refused for what it is rather than
	// for what it still holds. A namespace whose root is gone answers ENOENT to everything
	// afterwards, which reads as "that file is not there" when the truth is that the
	// namespace is not there.
	{name: "the root cannot be removed", run: func(t *testing.T, s metastore.Store) {
		for _, p := range rootNames {
			mustFail(t, s.RemoveDir(ctx(t), p), syscall.EBUSY)
		}
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustFail(t, s.RemoveDir(ctx(t), ""), syscall.EBUSY)
		mustHoldExactly(t, s, "f")
	}},

	// Remove is the file operation, and the root is a directory, so it is refused as one.
	{name: "the root is not a file", run: func(t *testing.T, s metastore.Store) {
		for _, p := range rootNames {
			mustFail(t, s.Remove(ctx(t), p), syscall.EISDIR)
		}
		mustHoldExactly(t, s)
	}},

	{name: "the root cannot be renamed, in either direction", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		for _, p := range rootNames {
			mustFail(t, s.Rename(ctx(t), p, "moved"), syscall.EBUSY)
			mustFail(t, s.Rename(ctx(t), "f", p), syscall.EBUSY)
		}
		mustHoldExactly(t, s, "f")
	}},

	// EEXIST rather than EBUSY: something is already at that name, which is what Create
	// promises to refuse, and the root is that something.
	{name: "nothing can be made where the root is", run: func(t *testing.T, s metastore.Store) {
		for _, p := range rootNames {
			mustFail(t, s.Create(ctx(t), p), syscall.EEXIST)
			mustFail(t, s.Mkdir(ctx(t), p), syscall.EEXIST)
		}
		mustHoldExactly(t, s)
	}},

	{name: "the root takes attributes like any directory", run: func(t *testing.T, s metastore.Store) {
		changed := time.Date(2021, 3, 4, 5, 6, 7, 89, time.UTC)
		mustSucceed(t, s.SetAttr(ctx(t), "", storage.AttrChange{Mode: mode(0o750), ModTime: &changed}))
		node, err := s.Stat(ctx(t), "")
		mustSucceed(t, err)
		if !node.IsDir() || node.Mode.Perm() != 0o750 || !node.ModTime.Equal(changed) {
			t.Fatalf("the root has mode %v and modification time %v, want a directory of 0750 at %v",
				node.Mode, node.ModTime.UTC(), changed)
		}
	}},
}

var pathCases = []testCase{
	// A path that leaves the root is refused before anything looks for it. These are not
	// names the namespace could hold and then not hold.
	{name: "a path outside the root is refused", run: func(t *testing.T, s metastore.Store) {
		for _, p := range []string{"/abs", "/", "..", "../x", "a/../../x"} {
			_, err := s.Stat(ctx(t), p)
			mustFail(t, err, syscall.EINVAL)
			_, err = s.List(ctx(t), p)
			mustFail(t, err, syscall.EINVAL)
			mustFail(t, s.Create(ctx(t), p), syscall.EINVAL)
			mustFail(t, s.Mkdir(ctx(t), p), syscall.EINVAL)
			mustFail(t, s.Remove(ctx(t), p), syscall.EINVAL)
			mustFail(t, s.RemoveDir(ctx(t), p), syscall.EINVAL)
			mustFail(t, s.SetAttr(ctx(t), p, storage.AttrChange{Mode: mode(0o600)}), syscall.EINVAL)
			mustFail(t, s.Rename(ctx(t), p, "x"), syscall.EINVAL)
			mustFail(t, s.Rename(ctx(t), "x", p), syscall.EINVAL)
		}
		mustHoldExactly(t, s)
	}},

	// The two are different facts and an implementation decides each of them itself. A
	// missing directory is ENOENT; a directory that is really a file is ENOTDIR. Collapsing
	// them tells a caller a file is missing when what is missing is the directory it would
	// have been in.
	{name: "a component that is a file is ENOTDIR and a component that is absent is ENOENT",
		run: func(t *testing.T, s metastore.Store) {
			mustSucceed(t, s.Create(ctx(t), "f"))
			mustSucceed(t, s.Mkdir(ctx(t), "d"))

			for _, c := range []struct {
				path string
				want syscall.Errno
			}{
				{"f/under", syscall.ENOTDIR},
				{"f/under/deeper", syscall.ENOTDIR},
				{"missing/under", syscall.ENOENT},
				{"d/missing/under", syscall.ENOENT},
			} {
				_, err := s.Stat(ctx(t), c.path)
				mustFailAt(t, c.path, err, c.want)
				_, err = s.List(ctx(t), c.path)
				mustFailAt(t, c.path, err, c.want)
				mustFailAt(t, c.path, s.Create(ctx(t), c.path), c.want)
				mustFailAt(t, c.path, s.Mkdir(ctx(t), c.path), c.want)
				mustFailAt(t, c.path, s.Remove(ctx(t), c.path), c.want)
				mustFailAt(t, c.path, s.RemoveDir(ctx(t), c.path), c.want)
				mustFailAt(t, c.path, s.SetAttr(ctx(t), c.path, storage.AttrChange{Mode: mode(0o600)}), c.want)
				mustFailAt(t, c.path, s.Rename(ctx(t), "f", c.path), c.want)
			}
			mustHoldExactly(t, s, "d", "f")
		}},

	{name: "a node that is not there is ENOENT", run: func(t *testing.T, s metastore.Store) {
		_, err := s.Stat(ctx(t), "missing")
		mustFail(t, err, syscall.ENOENT)
		_, err = s.List(ctx(t), "missing")
		mustFail(t, err, syscall.ENOENT)
		mustFail(t, s.Remove(ctx(t), "missing"), syscall.ENOENT)
		mustFail(t, s.RemoveDir(ctx(t), "missing"), syscall.ENOENT)
		mustFail(t, s.SetAttr(ctx(t), "missing", storage.AttrChange{Mode: mode(0o600)}), syscall.ENOENT)
		mustFail(t, s.Rename(ctx(t), "missing", "elsewhere"), syscall.ENOENT)
	}},
}

var nodeCases = []testCase{
	// The modes match what localdir's Create and Mkdir produce. A namespace held in a
	// database and one held in a directory should not disagree about what touch and mkdir
	// make.
	{name: "a new file is empty, references nothing, and is 0644", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		node, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if node.IsDir() || node.Mode.Perm() != 0o644 {
			t.Fatalf("the new file has mode %v, want a file of 0644", node.Mode)
		}
		if node.Size != 0 {
			t.Fatalf("the new file holds %d bytes, want none", node.Size)
		}
		// An empty file references no object: zero bytes are worth no round trip to an
		// object store, and a file that has never been written has nothing to point at.
		if node.Content != "" {
			t.Fatalf("the new file references object %q, want nothing", node.Content)
		}
	}},

	{name: "a new directory is 0755 and empty", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		node, err := s.Stat(ctx(t), "d")
		mustSucceed(t, err)
		if !node.IsDir() || node.Mode.Perm() != 0o755 {
			t.Fatalf("the new directory has mode %v, want a directory of 0755", node.Mode)
		}
		children, err := s.List(ctx(t), "d")
		mustSucceed(t, err)
		if len(children) != 0 {
			t.Fatalf("the new directory lists %v, want nothing", names(children))
		}
	}},

	{name: "a name that is taken is EEXIST, whatever holds it", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		for _, p := range []string{"f", "d"} {
			mustFailAt(t, p, s.Create(ctx(t), p), syscall.EEXIST)
			mustFailAt(t, p, s.Mkdir(ctx(t), p), syscall.EEXIST)
		}
		mustHoldExactly(t, s, "d", "f")
	}},

	// Nothing here makes a directory on the way to a name. A store that did would turn a
	// write to a path whose parent is gone into a silent success.
	{name: "nothing creates an intermediate directory", run: func(t *testing.T, s metastore.Store) {
		mustFail(t, s.Create(ctx(t), "absent/f"), syscall.ENOENT)
		mustFail(t, s.Mkdir(ctx(t), "absent/d"), syscall.ENOENT)
		mustHoldExactly(t, s)
	}},

	// Byte order rather than any collation. Two names differing only in case are two
	// different names, and a listing ordered by a locale's rules would put entries in an
	// order no other implementation of this system agrees with.
	{name: "a listing is in byte order", run: func(t *testing.T, s metastore.Store) {
		made := []string{"z", "A", "readme", "README", "a", "Z", "b.txt", "b"}
		for _, n := range made {
			mustSucceed(t, s.Create(ctx(t), n))
		}
		want := slices.Clone(made)
		slices.Sort(want)
		mustHoldExactly(t, s, want...)
	}},

	// A name on Linux is an arbitrary byte sequence. Uniqueness over it is byte-exact, so
	// README and readme are two files, and a name that is not valid UTF-8 is still a name:
	// a store that let one become U+FFFD on the way in and out would have renamed a file by
	// reading it back.
	{name: "names are bytes, not text", run: func(t *testing.T, s metastore.Store) {
		raw := []string{"README", "readme", string([]byte{0xff, 0xfe}), string([]byte{0x80}), "caf\xc3\xa9", "caf\xe9"}
		for _, n := range raw {
			mustSucceed(t, s.Create(ctx(t), n))
		}
		// Each is a name of its own, so making any of them again is EEXIST rather than a
		// second row beside an equal-but-different one.
		for _, n := range raw {
			mustFail(t, s.Create(ctx(t), n), syscall.EEXIST)
		}

		children, err := s.List(ctx(t), "")
		mustSucceed(t, err)
		if len(children) != len(raw) {
			t.Fatalf("the root holds %d names, want %d: %q", len(children), len(raw), names(children))
		}
		for _, n := range raw {
			if !slices.ContainsFunc(children, func(c metastore.Child) bool { return bytes.Equal(c.Name, []byte(n)) }) {
				t.Fatalf("the listing lost the name %q; it holds %q", n, names(children))
			}
			if _, err := s.Stat(ctx(t), n); err != nil {
				t.Fatalf("stat %q: %v", n, err)
			}
		}
		// The listing is sorted by the bytes of the names, invalid sequences included.
		if !slices.IsSortedFunc(children, func(a, b metastore.Child) int { return bytes.Compare(a.Name, b.Name) }) {
			t.Fatalf("the listing %q is not in byte order", names(children))
		}
	}},

	{name: "listing a file is ENOTDIR", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		_, err := s.List(ctx(t), "f")
		mustFail(t, err, syscall.ENOTDIR)
	}},

	{name: "a file is removed and a directory is not", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustFail(t, s.Remove(ctx(t), "d"), syscall.EISDIR)
		mustSucceed(t, s.Remove(ctx(t), "f"))
		mustHoldExactly(t, s, "d")
	}},

	{name: "an empty directory is removed and a file is not", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustFail(t, s.RemoveDir(ctx(t), "f"), syscall.ENOTDIR)
		mustSucceed(t, s.RemoveDir(ctx(t), "d"))
		mustHoldExactly(t, s, "f")
	}},

	{name: "a directory that still holds names is ENOTEMPTY", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.Create(ctx(t), "d/f"))
		mustFail(t, s.RemoveDir(ctx(t), "d"), syscall.ENOTEMPTY)
		mustSucceed(t, s.Remove(ctx(t), "d/f"))
		mustSucceed(t, s.RemoveDir(ctx(t), "d"))
		mustHoldExactly(t, s)
	}},

	{name: "a subtree is reached and listed at depth", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "a"))
		mustSucceed(t, s.Mkdir(ctx(t), "a/b"))
		mustSucceed(t, s.Mkdir(ctx(t), "a/b/c"))
		mustSucceed(t, s.Create(ctx(t), "a/b/c/deep"))
		node, err := s.Stat(ctx(t), "a/b/c/deep")
		mustSucceed(t, err)
		if node.IsDir() {
			t.Fatalf("a/b/c/deep has mode %v, want a file", node.Mode)
		}
		if got := names(mustList(t, s, "a/b/c")); !slices.Equal(got, []string{"deep"}) {
			t.Fatalf("a/b/c holds %q, want [deep]", got)
		}
	}},
}

var attrCases = []testCase{
	{name: "a mode is set and read back", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		for _, want := range []fs.FileMode{0o600, 0o777, 0o000, 0o444} {
			mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(want)}))
			node, err := s.Stat(ctx(t), "f")
			mustSucceed(t, err)
			if node.Mode.Perm() != want {
				t.Fatalf("the file has mode %v, want %v", node.Mode, want)
			}
			// Setting permissions does not turn a file into something else.
			if node.IsDir() || node.Mode.Type() != 0 {
				t.Fatalf("the file has type bits %v, want a plain file", node.Mode.Type())
			}
		}
	}},

	// The three bits that change how the permission bits are applied travel in
	// storage.SettableMode with them, so they are set and cleared like any other bit.
	{name: "setuid, setgid and sticky are set and cleared", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		special := []fs.FileMode{fs.ModeSetuid, fs.ModeSetgid, fs.ModeSticky}
		for _, bit := range append(slices.Clone(special), fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky) {
			mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o755 | bit)}))
			node, err := s.Stat(ctx(t), "f")
			mustSucceed(t, err)
			if node.Mode&storage.SettableMode != 0o755|bit {
				t.Fatalf("the file has mode %v, want %v", node.Mode, 0o755|bit)
			}

			mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o755)}))
			node, err = s.Stat(ctx(t), "f")
			mustSucceed(t, err)
			if node.Mode&storage.SettableMode != 0o755 {
				t.Fatalf("clearing %v left mode %v, want 0755", bit, node.Mode)
			}
		}
	}},

	// The rest of an fs.FileMode says what kind of node this is, and a node's kind is not a
	// property a caller changes. storage.AttrChange.Check owes every implementation this
	// refusal, so a change naming one is EINVAL before anything is written.
	{name: "a mode naming a kind is EINVAL", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		for _, bad := range []fs.FileMode{fs.ModeDir, fs.ModeSymlink, fs.ModeDevice, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeAppend} {
			mustFail(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o644 | bad)}), syscall.EINVAL)
		}
		node, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if node.Mode != 0o644 {
			t.Fatalf("a refused change left mode %v, want 0644 untouched", node.Mode)
		}
	}},

	// A kernel sends a mode change and a time change as separate requests, and neither may
	// clear what the other set.
	{name: "a mode change leaves the times and a time change leaves the mode",
		run: func(t *testing.T, s metastore.Store) {
			mustSucceed(t, s.Create(ctx(t), "f"))
			accessed := time.Date(2001, 2, 3, 4, 5, 6, 7, time.UTC)
			changed := time.Date(2002, 3, 4, 5, 6, 7, 8, time.UTC)
			mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{
				Mode: mode(0o640), AccessTime: &accessed, ModTime: &changed,
			}))

			mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o600)}))
			node, err := s.Stat(ctx(t), "f")
			mustSucceed(t, err)
			if !node.AccessTime.Equal(accessed) || !node.ModTime.Equal(changed) {
				t.Fatalf("a mode change moved the times to %v and %v, want %v and %v",
					node.AccessTime.UTC(), node.ModTime.UTC(), accessed, changed)
			}

			later := time.Date(2003, 4, 5, 6, 7, 8, 9, time.UTC)
			mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{ModTime: &later}))
			node, err = s.Stat(ctx(t), "f")
			mustSucceed(t, err)
			if node.Mode.Perm() != 0o600 {
				t.Fatalf("a time change moved the mode to %v, want 0600", node.Mode)
			}
			if !node.AccessTime.Equal(accessed) {
				t.Fatalf("setting the modification time moved the access time to %v, want %v",
					node.AccessTime.UTC(), accessed)
			}
		}},

	// The range and the precision a namespace keeps are its own, but a store that keeps
	// times in a database has no filesystem underneath to inherit a range from — it chooses
	// one. Two integers, seconds and nanoseconds, span every time.Time there is; a single
	// count of nanoseconds in an int64 spans only 1678 to 2262, so 2400 below is the value
	// that catches it.
	{name: "times outside a 32-bit epoch round-trip to the nanosecond",
		run: func(t *testing.T, s metastore.Store) {
			mustSucceed(t, s.Create(ctx(t), "f"))
			for _, want := range []time.Time{
				time.Date(1902, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC),
				time.Date(2038, 1, 19, 3, 14, 8, 0, time.UTC),
				time.Date(2400, 6, 1, 12, 0, 0, 500000000, time.UTC),
				time.Unix(0, 0).UTC(),
			} {
				mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{AccessTime: &want, ModTime: &want}))
				node, err := s.Stat(ctx(t), "f")
				mustSucceed(t, err)
				if !node.AccessTime.Equal(want) || !node.ModTime.Equal(want) {
					t.Fatalf("the times came back as %v and %v, want %v",
						node.AccessTime.UTC(), node.ModTime.UTC(), want)
				}
			}
		}},

	{name: "attributes are set on a directory too", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		changed := time.Date(1999, 9, 9, 9, 9, 9, 9, time.UTC)
		mustSucceed(t, s.SetAttr(ctx(t), "d", storage.AttrChange{Mode: mode(0o700), ModTime: &changed}))
		node, err := s.Stat(ctx(t), "d")
		mustSucceed(t, err)
		if !node.IsDir() || node.Mode.Perm() != 0o700 || !node.ModTime.Equal(changed) {
			t.Fatalf("the directory has mode %v and modification time %v, want a directory of 0700 at %v",
				node.Mode, node.ModTime.UTC(), changed)
		}
	}},

	// A change that names nothing is still a statement about one node, and applying it still
	// fails when that node is not there. An implementation that returned early on an empty
	// change would report a node it never looked for.
	{name: "an empty change still answers for the node", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{}))
		mustSucceed(t, s.SetAttr(ctx(t), "", storage.AttrChange{}))
		mustFail(t, s.SetAttr(ctx(t), "missing", storage.AttrChange{}), syscall.ENOENT)
		mustFail(t, s.SetAttr(ctx(t), "f/under", storage.AttrChange{}), syscall.ENOTDIR)
	}},

	// Node.Attr is how a node reaches the storage contract, and everything above this
	// interface sees a namespace through it rather than through a Node. A node whose
	// attributes disagreed with the one the storage contract renders would be reported
	// twice, differently.
	{name: "a node renders as the attributes the storage contract describes",
		run: func(t *testing.T, s metastore.Store) {
			accessed := time.Date(1902, 1, 1, 0, 0, 0, 0, time.UTC)
			changed := time.Date(2400, 6, 1, 12, 0, 0, 500000000, time.UTC)
			put(t, s, "f", 4096)
			mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{
				Mode: mode(0o640), AccessTime: &accessed, ModTime: &changed,
			}))

			node, err := s.Stat(ctx(t), "f")
			mustSucceed(t, err)
			attr := node.Attr()
			if attr.Mode != node.Mode || attr.Size != node.Size {
				t.Fatalf("the node renders as mode %v of %d bytes, want %v of %d",
					attr.Mode, attr.Size, node.Mode, node.Size)
			}
			if !attr.AccessTime.Equal(accessed) || !attr.ModTime.Equal(changed) {
				t.Fatalf("the node renders as accessed %v and changed %v, want %v and %v",
					attr.AccessTime.UTC(), attr.ModTime.UTC(), accessed, changed)
			}
			if attr.IsDir() != node.IsDir() {
				t.Fatalf("the node renders as a directory: %v, want %v", attr.IsDir(), node.IsDir())
			}

			mustSucceed(t, s.Mkdir(ctx(t), "d"))
			dir, err := s.Stat(ctx(t), "d")
			mustSucceed(t, err)
			if !dir.Attr().IsDir() {
				t.Fatalf("the directory renders as mode %v, want a directory", dir.Attr().Mode)
			}
		}},
}

var renameCases = []testCase{
	{name: "a file moves within a directory and across directories", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.Rename(ctx(t), "f", "g"))
		mustHoldExactly(t, s, "d", "g")
		mustSucceed(t, s.Rename(ctx(t), "g", "d/h"))
		mustHoldExactly(t, s, "d")
		if got := names(mustList(t, s, "d")); !slices.Equal(got, []string{"h"}) {
			t.Fatalf("d holds %q, want [h]", got)
		}
	}},

	// Renaming a directory moves everything beneath it. The shape that fails this — a row
	// keyed by the whole path — is the one an implementer reaches for first, and it fails
	// silently: the directory moves and its contents are left behind or lost.
	{name: "renaming a directory moves its whole subtree", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "a"))
		mustSucceed(t, s.Mkdir(ctx(t), "a/b"))
		mustSucceed(t, s.Mkdir(ctx(t), "a/b/c"))
		mustSucceed(t, s.Create(ctx(t), "a/b/c/deep"))
		mustSucceed(t, s.Create(ctx(t), "a/shallow"))
		deep, err := s.Stat(ctx(t), "a/b/c/deep")
		mustSucceed(t, err)

		mustSucceed(t, s.Rename(ctx(t), "a", "moved"))
		mustHoldExactly(t, s, "moved")

		if got := names(mustList(t, s, "moved")); !slices.Equal(got, []string{"b", "shallow"}) {
			t.Fatalf("moved holds %q, want [b shallow]", got)
		}
		moved, err := s.Stat(ctx(t), "moved/b/c/deep")
		mustSucceed(t, err)
		// The node itself survived the move rather than being copied: a rename changes the
		// name a node has, and Node.ID identifies the node rather than the name.
		if moved.ID != deep.ID {
			t.Fatalf("the moved file has id %d, want the %d it had before the move", moved.ID, deep.ID)
		}
		if _, err := s.Stat(ctx(t), "a/b/c/deep"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("the old path still answers with %v, want ENOENT", err)
		}
	}},

	// POSIX has rename(2) whose operands resolve to the same existing entry "return
	// successfully and perform no other action". The node has to be looked up to know it:
	// the same two strings name a node that is not there just as readily, and that is
	// ENOENT — so the shortcut may not be taken from the operand strings alone.
	{name: "renaming a node onto itself changes nothing", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.Create(ctx(t), "d/g"))
		before, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)

		for _, p := range []string{"f", "./f", "d/../f"} {
			mustSucceed(t, s.Rename(ctx(t), p, "f"))
		}
		mustSucceed(t, s.Rename(ctx(t), "d/g", "d/g"))

		after, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if after.ID != before.ID || after.Mode != before.Mode {
			t.Fatalf("the file is now id %d mode %v, want id %d mode %v",
				after.ID, after.Mode, before.ID, before.Mode)
		}
		mustHoldExactly(t, s, "d", "f")
	}},

	{name: "renaming a node that is not there onto itself is ENOENT", run: func(t *testing.T, s metastore.Store) {
		mustFail(t, s.Rename(ctx(t), "missing", "missing"), syscall.ENOENT)
		mustFail(t, s.Rename(ctx(t), "./missing", "missing"), syscall.ENOENT)
		mustHoldExactly(t, s)
	}},

	{name: "a rename replaces an existing file", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "from"))
		mustSucceed(t, s.Create(ctx(t), "onto"))
		from, err := s.Stat(ctx(t), "from")
		mustSucceed(t, err)

		mustSucceed(t, s.Rename(ctx(t), "from", "onto"))
		mustHoldExactly(t, s, "onto")
		node, err := s.Stat(ctx(t), "onto")
		mustSucceed(t, err)
		if node.ID != from.ID {
			t.Fatalf("the destination holds id %d, want the moved node's %d", node.ID, from.ID)
		}
	}},

	{name: "a rename onto an empty directory succeeds", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "from"))
		mustSucceed(t, s.Create(ctx(t), "from/held"))
		mustSucceed(t, s.Mkdir(ctx(t), "onto"))
		mustSucceed(t, s.Rename(ctx(t), "from", "onto"))
		mustHoldExactly(t, s, "onto")
		if got := names(mustList(t, s, "onto")); !slices.Equal(got, []string{"held"}) {
			t.Fatalf("onto holds %q, want [held]", got)
		}
	}},

	// Either errno names the obstacle truthfully: the destination still holds entries, and
	// it would have to be emptied first.
	{name: "a rename onto a directory that still holds names is refused", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "from"))
		mustSucceed(t, s.Mkdir(ctx(t), "onto"))
		mustSucceed(t, s.Create(ctx(t), "onto/held"))
		mustFailWithAny(t, s.Rename(ctx(t), "from", "onto"), syscall.ENOTEMPTY, syscall.EEXIST)
		mustHoldExactly(t, s, "from", "onto")
		if got := names(mustList(t, s, "onto")); !slices.Equal(got, []string{"held"}) {
			t.Fatalf("the refused rename left onto holding %q, want [held]", got)
		}
	}},

	{name: "a file and a directory do not replace one another", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustFail(t, s.Rename(ctx(t), "f", "d"), syscall.EISDIR)
		mustFail(t, s.Rename(ctx(t), "d", "f"), syscall.ENOTDIR)
		mustHoldExactly(t, s, "d", "f")
	}},

	// A directory moved inside itself would hang off a node no root reaches, taking its
	// whole subtree out of the namespace. EINVAL is what rename(2) reports for it.
	{name: "a directory cannot be moved inside itself", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "a"))
		mustSucceed(t, s.Mkdir(ctx(t), "a/b"))
		mustSucceed(t, s.Create(ctx(t), "a/b/held"))
		mustFail(t, s.Rename(ctx(t), "a", "a/b/a"), syscall.EINVAL)
		mustFail(t, s.Rename(ctx(t), "a", "a/under"), syscall.EINVAL)

		mustHoldExactly(t, s, "a")
		if got := names(mustList(t, s, "a/b")); !slices.Equal(got, []string{"held"}) {
			t.Fatalf("the refused rename left a/b holding %q, want [held]", got)
		}
	}},

	{name: "a rename needs a source and a destination directory", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustFail(t, s.Rename(ctx(t), "missing", "g"), syscall.ENOENT)
		mustFail(t, s.Rename(ctx(t), "f", "absent/g"), syscall.ENOENT)
		mustFail(t, s.Rename(ctx(t), "f", "f/g"), syscall.ENOTDIR)
		mustHoldExactly(t, s, "f")
	}},
}

var objectCases = []testCase{
	// A key is opaque: nothing may derive it from the path, and nothing may reuse it after
	// the object it names is gone. Distinctness is the part a test can put a question to.
	{name: "every reservation is a key of its own", run: func(t *testing.T, s metastore.Store) {
		seen := map[metastore.Key]bool{}
		for i := range 32 {
			key, err := s.Reserve(ctx(t), fmt.Sprintf("f%d", i), 16)
			mustSucceed(t, err)
			if key == "" {
				t.Fatal("a reservation returned the empty key, which is what a file with no contents holds")
			}
			if seen[key] {
				t.Fatalf("the key %q was handed out twice", key)
			}
			seen[key] = true
		}
	}},

	{name: "a commit makes a file and points it at the object", run: func(t *testing.T, s metastore.Store) {
		changed := time.Date(2019, 5, 6, 7, 8, 9, 10, time.UTC)
		key, err := s.Reserve(ctx(t), "f", 1234)
		mustSucceed(t, err)
		mustSucceed(t, s.Commit(ctx(t), "f", metastore.Object{
			Key: key, Size: 1234, Digest: []byte{1, 2, 3}, ModTime: changed,
		}))

		node, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if node.Content != key {
			t.Fatalf("the file references object %q, want %q", node.Content, key)
		}
		if node.Size != 1234 {
			t.Fatalf("the file holds %d bytes, want 1234", node.Size)
		}
		if !node.ModTime.Equal(changed) {
			t.Fatalf("the file changed at %v, want %v", node.ModTime.UTC(), changed)
		}
		// A file a commit created gets the mode a new file is made with.
		if node.IsDir() || node.Mode.Perm() != 0o644 {
			t.Fatalf("the committed file has mode %v, want a file of 0644", node.Mode)
		}
		mustHoldExactly(t, s, "f")
	}},

	// Replacing the contents is not a request to change the mode. Every settable bit is
	// carried over, not the nine permission bits alone: dropping a setuid bit here would be
	// a change nobody asked for and nothing reported.
	{name: "a commit leaves the mode a file already had", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o600 | fs.ModeSetuid)}))
		put(t, s, "f", 10)
		node, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if node.Mode&storage.SettableMode != 0o600|fs.ModeSetuid {
			t.Fatalf("the commit left mode %v, want 0600 with setuid", node.Mode)
		}
	}},

	// The directory holding the file has to exist already, and a commit never makes it. A
	// store that treated the path as a flat key would take this write silently: after an
	// unlink, go-fuse commits a still-open file to a placeholder path under a directory that
	// was never made. That fails loudly on a real filesystem, and it has to fail here — the
	// alternative is bytes landing on a key nobody reads and nobody cleans up.
	{name: "a commit whose parent directory is absent is ENOENT and creates nothing",
		run: func(t *testing.T, s metastore.Store) {
			for _, p := range []string{".go-fuse.1234/deleted", "absent/f", "a/b/c"} {
				// The reservation is taken against a name that could be written, so that what
				// is under test is the commit's own refusal rather than the reservation's.
				key, err := s.Reserve(ctx(t), "reservable", 7)
				mustSucceed(t, err)
				mustFailAt(t, p, s.Commit(ctx(t), p, metastore.Object{Key: key, Size: 7, ModTime: time.Now()}), syscall.ENOENT)
				// Neither the leaf nor any directory on the way to it was recorded.
				if _, err := s.Stat(ctx(t), p); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("stat %q after the refused commit: %v, want ENOENT", p, err)
				}
			}
			mustHoldExactly(t, s)
		}},

	{name: "a commit through a file is ENOTDIR", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		key, err := s.Reserve(ctx(t), "reservable", 1)
		mustSucceed(t, err)
		mustFail(t, s.Commit(ctx(t), "f/under", metastore.Object{Key: key, Size: 1, ModTime: time.Now()}), syscall.ENOTDIR)
		mustHoldExactly(t, s, "f")
	}},

	{name: "a commit onto a directory is EISDIR", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		key, err := s.Reserve(ctx(t), "reservable", 1)
		mustSucceed(t, err)
		mustFail(t, s.Commit(ctx(t), "d", metastore.Object{Key: key, Size: 1, ModTime: time.Now()}), syscall.EISDIR)
		mustFail(t, s.Commit(ctx(t), "", metastore.Object{Key: key, Size: 1, ModTime: time.Now()}), syscall.EISDIR)
	}},

	// The object being committed must have been reserved. Without that rule a sweeper cannot
	// tell an object a writer is still working on from one nobody will ever reference.
	{name: "committing a key that was never reserved is EINVAL", run: func(t *testing.T, s metastore.Store) {
		mustFail(t, s.Commit(ctx(t), "f", metastore.Object{Key: "invented", Size: 1, ModTime: time.Now()}), syscall.EINVAL)
		mustHoldExactly(t, s)
	}},

	{name: "a reservation is committed once", run: func(t *testing.T, s metastore.Store) {
		key, err := s.Reserve(ctx(t), "f", 4)
		mustSucceed(t, err)
		mustSucceed(t, s.Commit(ctx(t), "f", metastore.Object{Key: key, Size: 4, ModTime: time.Now()}))
		mustFail(t, s.Commit(ctx(t), "g", metastore.Object{Key: key, Size: 4, ModTime: time.Now()}), syscall.EINVAL)
		mustHoldExactly(t, s, "f")
	}},

	{name: "an unresolved reservation is never collectable or committable", run: func(t *testing.T, s metastore.Store) {
		key, err := s.Reserve(ctx(t), "f", 10)
		mustSucceed(t, err)
		mustSucceed(t, s.Quarantine(ctx(t), key))
		if got := mustGarbage(t, s); len(got) != 0 {
			t.Fatalf("an unresolved reservation is collectable: %v", got)
		}
		mustFail(t, s.Commit(ctx(t), "f", metastore.Object{Key: key, Size: 10, ModTime: time.Now()}), syscall.EINVAL)
		mustHoldExactly(t, s)
	}},

	{name: "quarantining a missing or already-unresolved key converges", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Quarantine(ctx(t), "missing"))
		mustSucceed(t, s.Quarantine(ctx(t), "missing"))
		key, err := s.Reserve(ctx(t), "f", 10)
		mustSucceed(t, err)
		mustSucceed(t, s.Quarantine(ctx(t), key))
		mustSucceed(t, s.Quarantine(ctx(t), key))
	}},

	{name: "quarantining referenced or garbage objects is EINVAL", run: func(t *testing.T, s metastore.Store) {
		referenced := put(t, s, "f", 10)
		mustFail(t, s.Quarantine(ctx(t), referenced), syscall.EINVAL)
		garbage, err := s.Reserve(ctx(t), "g", 10)
		mustSucceed(t, err)
		mustSucceed(t, s.Abandon(ctx(t), garbage))
		mustFail(t, s.Quarantine(ctx(t), garbage), syscall.EINVAL)
	}},

	{name: "an abandoned reservation is immediately collectable and cannot commit", run: func(t *testing.T, s metastore.Store) {
		key, err := s.Reserve(ctx(t), "f", 10)
		mustSucceed(t, err)
		mustSucceed(t, s.Abandon(ctx(t), key))
		if got := mustGarbage(t, s); !slices.Equal(got, []metastore.Key{key}) {
			t.Fatalf("collectable objects are %v, want the abandoned %q", got, key)
		}
		mustFail(t, s.Commit(ctx(t), "f", metastore.Object{Key: key, Size: 10, ModTime: time.Now()}), syscall.EINVAL)
		mustHoldExactly(t, s)
	}},

	{name: "abandoning a missing or already-garbage key converges", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Abandon(ctx(t), "missing"))
		mustSucceed(t, s.Abandon(ctx(t), "missing"))

		key, err := s.Reserve(ctx(t), "f", 10)
		mustSucceed(t, err)
		mustSucceed(t, s.Abandon(ctx(t), key))
		mustSucceed(t, s.Abandon(ctx(t), key))
		if got := mustGarbage(t, s); !slices.Equal(got, []metastore.Key{key}) {
			t.Fatalf("collectable objects are %v, want the abandoned %q", got, key)
		}
	}},

	{name: "abandoning a referenced key is EINVAL", run: func(t *testing.T, s metastore.Store) {
		key := put(t, s, "f", 10)
		mustFail(t, s.Abandon(ctx(t), key), syscall.EINVAL)
		node, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if node.Content != key {
			t.Fatalf("the file references %q after the refused abandon, want %q", node.Content, key)
		}
		if got := mustGarbage(t, s); len(got) != 0 {
			t.Fatalf("the refused abandon made referenced objects collectable: %v", got)
		}
	}},

	// Zero bytes are worth no round trip to an object store, so a file with no contents
	// references no object and there is nothing to reserve for it. The empty key is how a
	// commit says so, and it is the same absence Create leaves behind — a truncation to
	// nothing has to reach the same state as a file that was never written, or the two
	// would be distinguishable for no reason a caller could act on.
	{name: "an empty key commits a file with no contents", run: func(t *testing.T, s metastore.Store) {
		changed := time.Date(2011, 2, 3, 4, 5, 6, 7, time.UTC)
		mustSucceed(t, s.Commit(ctx(t), "fresh", metastore.Object{ModTime: changed}))
		node, err := s.Stat(ctx(t), "fresh")
		mustSucceed(t, err)
		if node.Content != "" || node.Size != 0 {
			t.Fatalf("the file references %q and holds %d bytes, want nothing and 0", node.Content, node.Size)
		}
		if node.IsDir() || node.Mode.Perm() != 0o644 {
			t.Fatalf("the file has mode %v, want a file of 0644", node.Mode)
		}
		if !node.ModTime.Equal(changed) {
			t.Fatalf("the file changed at %v, want %v", node.ModTime.UTC(), changed)
		}

		// Truncating a file that held bytes releases the object it held.
		held := put(t, s, "written", 500)
		mustSucceed(t, s.Commit(ctx(t), "written", metastore.Object{ModTime: changed}))
		node, err = s.Stat(ctx(t), "written")
		mustSucceed(t, err)
		if node.Content != "" || node.Size != 0 {
			t.Fatalf("the truncated file references %q and holds %d bytes, want nothing and 0",
				node.Content, node.Size)
		}
		if got := mustGarbage(t, s); !slices.Equal(got, []metastore.Key{held}) {
			t.Fatalf("collectable objects are %v, want the released %q", got, held)
		}
		mustHoldExactly(t, s, "fresh", "written")
	}},

	// A length with no object to hold it describes bytes that are nowhere. Taking it would
	// charge the namespace for contents no key names and no read could ever return.
	{name: "an empty key with a length is EINVAL", run: func(t *testing.T, s metastore.Store) {
		mustFail(t, s.Commit(ctx(t), "f", metastore.Object{Size: 1, ModTime: time.Now()}), syscall.EINVAL)
		mustHoldExactly(t, s)
	}},

	// An object a name stops pointing at is recorded as garbage rather than deleted, because
	// this interface does not reach the object store.
	{name: "an object a commit displaces becomes garbage", run: func(t *testing.T, s metastore.Store) {
		first := put(t, s, "f", 10)
		if got := mustGarbage(t, s); len(got) != 0 {
			t.Fatalf("a live object is already collectable: %v", got)
		}
		second := put(t, s, "f", 20)
		if got := mustGarbage(t, s); !slices.Equal(got, []metastore.Key{first}) {
			t.Fatalf("collectable objects are %v, want just the displaced %q", got, first)
		}
		node, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if node.Content != second {
			t.Fatalf("the file references %q, want the object just committed, %q", node.Content, second)
		}
	}},

	{name: "removing a file makes its object garbage", run: func(t *testing.T, s metastore.Store) {
		key := put(t, s, "f", 10)
		mustSucceed(t, s.Remove(ctx(t), "f"))
		if got := mustGarbage(t, s); !slices.Equal(got, []metastore.Key{key}) {
			t.Fatalf("collectable objects are %v, want just %q", got, key)
		}
	}},

	{name: "a rename that replaces a file makes its object garbage", run: func(t *testing.T, s metastore.Store) {
		put(t, s, "from", 10)
		replaced := put(t, s, "onto", 20)
		mustSucceed(t, s.Rename(ctx(t), "from", "onto"))
		if got := mustGarbage(t, s); !slices.Equal(got, []metastore.Key{replaced}) {
			t.Fatalf("collectable objects are %v, want just the replaced %q", got, replaced)
		}
	}},

	// Time cannot distinguish a dead writer from a slow writer in another process. A
	// reservation remains committable until the caller explicitly resolves its outcome.
	{name: "a reservation never becomes collectable merely because time passes",
		run: func(t *testing.T, s metastore.Store) {
			key, err := s.Reserve(ctx(t), "f", 1)
			mustSucceed(t, err)
			if got := mustGarbage(t, s); len(got) != 0 {
				t.Fatalf("a fresh reservation is collectable: %v", got)
			}
			mustSucceed(t, s.Commit(ctx(t), "f", metastore.Object{Key: key, Size: 1, ModTime: time.Now()}))
			if got := mustGarbage(t, s); len(got) != 0 {
				t.Fatalf("a committed object is collectable: %v", got)
			}

			reserved, err := s.Reserve(ctx(t), "g", 1)
			mustSucceed(t, err)
			if got := mustGarbage(t, s); len(got) != 0 {
				t.Fatalf("an uncommitted reservation became collectable: %v", got)
			}
			mustSucceed(t, s.Commit(ctx(t), "g", metastore.Object{Key: reserved, Size: 1, ModTime: time.Now()}))
		}},

	{name: "a collection is capped at the limit it was given", run: func(t *testing.T, s metastore.Store) {
		for i := range 5 {
			key, err := s.Reserve(ctx(t), fmt.Sprintf("f%d", i), 1)
			mustSucceed(t, err)
			mustSucceed(t, s.Abandon(ctx(t), key))
		}
		for _, limit := range []int{0, 1, 3, 5, 50} {
			got, err := s.Garbage(ctx(t), limit)
			mustSucceed(t, err)
			if len(got) > limit {
				t.Fatalf("a limit of %d returned %d objects", limit, len(got))
			}
			if want := min(limit, 5); len(got) != want {
				t.Fatalf("a limit of %d returned %d objects, want %d", limit, len(got), want)
			}
		}
	}},

	{name: "a collection refuses a limit that is not a count",
		run: func(t *testing.T, s metastore.Store) {
			_, err := s.Garbage(ctx(t), -1)
			mustFail(t, err, syscall.EINVAL)
		}},

	{name: "forgetting drops the record of an object whose bytes are gone", run: func(t *testing.T, s metastore.Store) {
		key := put(t, s, "f", 10)
		mustSucceed(t, s.Remove(ctx(t), "f"))
		mustSucceed(t, s.Forget(ctx(t), []metastore.Key{key}))
		if got := mustGarbage(t, s); len(got) != 0 {
			t.Fatalf("a forgotten object is still collectable: %v", got)
		}
	}},

	// Only garbage represents an object whose bytes a sweeper has authority to remove.
	{name: "forgetting any non-garbage object is EINVAL", run: func(t *testing.T, s metastore.Store) {
		referenced := put(t, s, "f", 10)
		reserved, err := s.Reserve(ctx(t), "reserved", 1)
		mustSucceed(t, err)
		unresolved, err := s.Reserve(ctx(t), "unresolved", 1)
		mustSucceed(t, err)
		mustSucceed(t, s.Quarantine(ctx(t), unresolved))
		for _, key := range []metastore.Key{referenced, reserved, unresolved} {
			mustFail(t, s.Forget(ctx(t), []metastore.Key{key}), syscall.EINVAL)
		}
		node, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if node.Content != referenced {
			t.Fatalf("the file references %q after the refused forget, want %q", node.Content, referenced)
		}

		// The refusal covers the whole call: a batch holding one unresolved key drops none
		// of the others, so a caller is never left unable to say what happened.
		garbage, err := s.Reserve(ctx(t), "garbage", 1)
		mustSucceed(t, err)
		mustSucceed(t, s.Abandon(ctx(t), garbage))
		mustFail(t, s.Forget(ctx(t), []metastore.Key{garbage, unresolved}), syscall.EINVAL)
		if got := mustGarbage(t, s); !slices.Equal(got, []metastore.Key{garbage}) {
			t.Fatalf("collectable objects are %v, want the untouched %q", got, garbage)
		}
	}},

	{name: "an abandoned key a collection handed out can no longer be committed", run: func(t *testing.T, s metastore.Store) {
		key, err := s.Reserve(ctx(t), "f", 10)
		mustSucceed(t, err)
		mustSucceed(t, s.Abandon(ctx(t), key))
		if got := mustGarbage(t, s); !slices.Equal(got, []metastore.Key{key}) {
			t.Fatalf("collectable objects are %v, want the swept reservation %q", got, key)
		}
		mustFail(t, s.Commit(ctx(t), "f", metastore.Object{Key: key, Size: 10, ModTime: time.Now()}), syscall.EINVAL)
		// The refused commit left no file behind, so nothing points at bytes the sweeper is
		// about to delete.
		mustHoldExactly(t, s)
	}},

	// A second collection may return the same key — it is garbage until Forget drops the
	// record — but it must not have gone back to being committable in between.
	{name: "collecting twice does not make a swept key committable again", run: func(t *testing.T, s metastore.Store) {
		key, err := s.Reserve(ctx(t), "f", 10)
		mustSucceed(t, err)
		mustSucceed(t, s.Abandon(ctx(t), key))
		first := mustGarbage(t, s)
		second := mustGarbage(t, s)
		if !slices.Equal(first, second) {
			t.Fatalf("two collections returned %v then %v, want the same keys until they are forgotten", first, second)
		}
		if got := mustGarbage(t, s); !slices.Equal(got, []metastore.Key{key}) {
			t.Fatalf("a swept key is %v, want %q", got, key)
		}
		mustFail(t, s.Commit(ctx(t), "f", metastore.Object{Key: key, Size: 10, ModTime: time.Now()}), syscall.EINVAL)
	}},

	// A reservation knows the write it is for, so it refuses what the commit would refuse
	// anyway. That is not the authority — the commit asks again, because the namespace may
	// change in between — but it is what stops a write that cannot land from paying to
	// upload its bytes first.
	{name: "a reservation refuses a write that cannot land", run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.Create(ctx(t), "f"))

		for _, c := range []struct {
			path string
			size int64
			want syscall.Errno
		}{
			{"absent/g", 1, syscall.ENOENT},
			{".go-fuse.1234/deleted", 1, syscall.ENOENT},
			{"f/under", 1, syscall.ENOTDIR},
			{"d", 1, syscall.EISDIR},
			{"", 1, syscall.EISDIR},
			{"g", -1, syscall.EINVAL},
		} {
			_, err := s.Reserve(ctx(t), c.path, c.size)
			mustFailAt(t, c.path, err, c.want)
		}

		// A refused reservation leaves no record. One that recorded a key before deciding it
		// could not be used would leak an object for the sweeper to find, and the sweeper
		// cannot tell it from a write that is still in flight.
		if got := mustGarbage(t, s); len(got) != 0 {
			t.Fatalf("refused reservations left %v behind, want nothing", got)
		}
		mustHoldExactly(t, s, "d", "f")
	}},

	// The ordinary path is unaffected: a reservation the namespace has room for is taken,
	// and the commit that follows it works.
	{name: "a reservation the namespace has room for is committed", allowance: allowance, run: func(t *testing.T, s metastore.Store) {
		key, err := s.Reserve(ctx(t), "f", 500)
		mustSucceed(t, err)
		// The reservation itself takes no room; the bytes are the namespace's only once they
		// are committed.
		mustUsed(t, s, 0)
		mustSucceed(t, s.Commit(ctx(t), "f", metastore.Object{Key: key, Size: 500, ModTime: time.Now()}))
		mustUsed(t, s, 500)
		mustHoldExactly(t, s, "f")
	}},

	{name: "a reservation past the allowance is EDQUOT and records nothing", allowance: allowance, run: func(t *testing.T, s metastore.Store) {
		put(t, s, "f", allowance-100)

		_, err := s.Reserve(ctx(t), "g", 101)
		mustFail(t, err, syscall.EDQUOT)
		if got := mustGarbage(t, s); len(got) != 0 {
			t.Fatalf("a refused reservation left %v behind, want nothing", got)
		}
		mustUsed(t, s, allowance-100)

		// Replacing a file is charged the difference, so a reservation that would overwrite
		// the large file is taken even though the whole size would not fit twice.
		_, err = s.Reserve(ctx(t), "f", allowance-100)
		mustSucceed(t, err)
		_, err = s.Reserve(ctx(t), "g", 100)
		mustSucceed(t, err)
	}},

	// Forgetting is driven by a sweeper that may have been interrupted between deleting the
	// object and recording that it did, so running it again has to converge rather than fail.
	{name: "forgetting converges", run: func(t *testing.T, s metastore.Store) {
		key := put(t, s, "f", 10)
		mustSucceed(t, s.Remove(ctx(t), "f"))
		mustSucceed(t, s.Forget(ctx(t), []metastore.Key{key}))
		mustSucceed(t, s.Forget(ctx(t), []metastore.Key{key}))
		mustSucceed(t, s.Forget(ctx(t), []metastore.Key{"never-existed"}))
		mustSucceed(t, s.Forget(ctx(t), nil))
	}},
}

// allowance is what the cases below hold their namespace to. It is large enough that the
// sizes they commit are readable as themselves and small enough to be filled in one step.
const allowance = 1 << 20

var spaceCases = []testCase{
	// A namespace with no allowance has no room of its own to report: an object store has no
	// capacity to report, so a namespace held in one has no figures at all beyond an
	// allowance it was never given. The refusal is a standing property rather than a
	// condition of the call — one that refuses never starts answering.
	{name: "a namespace with no allowance reports none", run: func(t *testing.T, s metastore.Store) {
		for range 3 {
			_, err := s.Space(ctx(t))
			mustFail(t, err, syscall.ENOSYS)
		}
		put(t, s, "f", 100)
		_, err := s.Space(ctx(t))
		mustFail(t, err, syscall.ENOSYS)
	}},

	{name: "a namespace with an allowance reports it", allowance: allowance, run: func(t *testing.T, s metastore.Store) {
		space, err := s.Space(ctx(t))
		mustSucceed(t, err)
		if space.Total != allowance {
			t.Fatalf("the namespace reports a total of %d bytes, want %d", space.Total, allowance)
		}
		if space.Used != 0 {
			t.Fatalf("a fresh namespace reports %d bytes used, want none", space.Used)
		}
		if !space.Coherent() {
			t.Fatalf("the namespace reports %+v, which cannot be true of anything", space)
		}
	}},

	// Used is exact and is maintained by the same changes that move bytes in and out, so it
	// is a census of what the namespace's files hold rather than a sample of anything.
	{name: "used bytes follow what the namespace holds", allowance: allowance, run: func(t *testing.T, s metastore.Store) {
		mustSucceed(t, s.Create(ctx(t), "empty"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustUsed(t, s, 0)

		put(t, s, "f", 1000)
		mustUsed(t, s, 1000)

		put(t, s, "d/g", 500)
		mustUsed(t, s, 1500)

		// Replacing contents charges the difference in each direction.
		put(t, s, "f", 3000)
		mustUsed(t, s, 3500)
		put(t, s, "f", 100)
		mustUsed(t, s, 600)

		mustSucceed(t, s.Remove(ctx(t), "f"))
		mustUsed(t, s, 500)

		// A rename moves bytes that are charged already and credits back what it destroys.
		put(t, s, "h", 250)
		mustUsed(t, s, 750)
		mustSucceed(t, s.Rename(ctx(t), "h", "d/g"))
		mustUsed(t, s, 250)

		mustSucceed(t, s.Remove(ctx(t), "d/g"))
		mustSucceed(t, s.RemoveDir(ctx(t), "d"))
		mustUsed(t, s, 0)

		space, err := s.Space(ctx(t))
		mustSucceed(t, err)
		if space.Avail != allowance {
			t.Fatalf("an empty namespace reports %d bytes available, want the whole %d", space.Avail, allowance)
		}
	}},

	// The refusal happens in the same change that records the size, so no window exists
	// between deciding there is room and taking it.
	{name: "a commit past the allowance is EDQUOT and changes nothing", allowance: allowance, run: func(t *testing.T, s metastore.Store) {
		put(t, s, "f", allowance-100)
		mustUsed(t, s, allowance-100)

		// The reservation is for what fits; the commit then asks for more than the namespace
		// has left. That is the arrangement a namespace which filled up between the two calls
		// produces, and it is what makes the commit rather than the reservation the authority.
		key, err := s.Reserve(ctx(t), "g", 100)
		mustSucceed(t, err)
		mustFail(t, s.Commit(ctx(t), "g", metastore.Object{Key: key, Size: 101, ModTime: time.Now()}), syscall.EDQUOT)

		// Neither the file nor the bytes were recorded.
		if _, err := s.Stat(ctx(t), "g"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("stat g after the refused commit: %v, want ENOENT", err)
		}
		mustUsed(t, s, allowance-100)
		mustHoldExactly(t, s, "f")

		// What fits is taken.
		fits, err := s.Reserve(ctx(t), "g", 100)
		mustSucceed(t, err)
		mustSucceed(t, s.Commit(ctx(t), "g", metastore.Object{Key: fits, Size: 100, ModTime: time.Now()}))
		mustUsed(t, s, allowance)

		space, err := s.Space(ctx(t))
		mustSucceed(t, err)
		if space.Avail != 0 {
			t.Fatalf("a full namespace reports %d bytes available, want none", space.Avail)
		}
	}},

	// Replacing a file with a larger one is charged the difference rather than the whole,
	// and a replacement that shrinks it is never refused.
	{name: "the allowance is charged the difference a replacement makes", allowance: allowance, run: func(t *testing.T, s metastore.Store) {
		put(t, s, "f", allowance)
		mustUsed(t, s, allowance)

		// Reserving the size the file already holds costs the namespace nothing, so the
		// reservation is taken; the commit then asks for one byte more than there is room for.
		key, err := s.Reserve(ctx(t), "f", allowance)
		mustSucceed(t, err)
		mustFail(t, s.Commit(ctx(t), "f", metastore.Object{Key: key, Size: allowance + 1, ModTime: time.Now()}), syscall.EDQUOT)

		node, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if node.Size != allowance {
			t.Fatalf("the refused commit left the file holding %d bytes, want %d", node.Size, allowance)
		}
		mustUsed(t, s, allowance)

		// A full namespace still takes a write that shrinks it, which is the only way back
		// under the allowance.
		put(t, s, "f", 10)
		mustUsed(t, s, 10)
	}},

	{name: "a commit of a negative length is EINVAL", allowance: allowance, run: func(t *testing.T, s metastore.Store) {
		key, err := s.Reserve(ctx(t), "f", 0)
		mustSucceed(t, err)
		mustFail(t, s.Commit(ctx(t), "f", metastore.Object{Key: key, Size: -1, ModTime: time.Now()}), syscall.EINVAL)
		mustUsed(t, s, 0)
		mustHoldExactly(t, s)
	}},
}

// --- helpers ------------------------------------------------------------------------

func ctx(t *testing.T) context.Context { return t.Context() }

// mode is the address of a mode, which is what an AttrChange takes: a nil there means "not
// changing this", so every mode being set has to be somewhere addressable.
func mode(m fs.FileMode) *fs.FileMode { return &m }

// put reserves a key and commits an object of the given length at path, which is the two
// steps every write through this contract takes.
func put(t *testing.T, s metastore.Store, path string, size int64) metastore.Key {
	t.Helper()
	key, err := s.Reserve(ctx(t), path, size)
	mustSucceed(t, err)
	mustSucceed(t, s.Commit(ctx(t), path, metastore.Object{Key: key, Size: size, ModTime: time.Now()}))
	return key
}

// names renders a listing as strings, for a failure message. The names themselves are bytes
// and are compared as bytes wherever that distinction is the point.
func names(children []metastore.Child) []string {
	got := make([]string, 0, len(children))
	for _, c := range children {
		got = append(got, string(c.Name))
	}
	return got
}

func mustList(t *testing.T, s metastore.Store, path string) []metastore.Child {
	t.Helper()
	children, err := s.List(ctx(t), path)
	if err != nil {
		t.Fatalf("list %q: %v", path, err)
	}
	return children
}

// mustGarbage collects with a limit high enough to return everything, and sorts the result:
// nothing in the contract fixes the order objects come back in.
func mustGarbage(t *testing.T, s metastore.Store) []metastore.Key {
	t.Helper()
	keys, err := s.Garbage(ctx(t), 1000)
	if err != nil {
		t.Fatalf("collecting garbage: %v", err)
	}
	slices.Sort(keys)
	return keys
}

func mustUsed(t *testing.T, s metastore.Store, want int64) {
	t.Helper()
	space, err := s.Space(ctx(t))
	if err != nil {
		t.Fatalf("asking for the room the namespace has: %v", err)
	}
	if space.Used != want {
		t.Fatalf("the namespace reports %d bytes used, want %d", space.Used, want)
	}
	if !space.Coherent() {
		t.Fatalf("the namespace reports %+v, which cannot be true of anything", space)
	}
}

func mustSucceed(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func mustFail(t *testing.T, err error, want syscall.Errno) {
	t.Helper()
	mustFailWithAny(t, err, want)
}

// mustFailAt names the path involved, for the cases that put one question to many paths in
// a loop and would otherwise report which of them failed only by line number.
func mustFailAt(t *testing.T, path string, err error, want syscall.Errno) {
	t.Helper()
	if err == nil {
		t.Fatalf("%q: call succeeded, want %v", path, want)
	}
	if !errors.Is(err, want) {
		t.Fatalf("%q: call failed with %v, want %v", path, err, want)
	}
}

func mustFailWithAny(t *testing.T, err error, want ...syscall.Errno) {
	t.Helper()
	if err == nil {
		t.Fatalf("call succeeded, want one of %v", want)
	}
	for _, w := range want {
		if errors.Is(err, w) {
			return
		}
	}
	t.Fatalf("call failed with %v, want one of %v", err, want)
}

// mustHoldExactly checks that the root is still a directory and still holds exactly these
// names. An operation aimed at the root that damaged it would otherwise surface much later,
// as ENOENT from something unrelated.
func mustHoldExactly(t *testing.T, s metastore.Store, want ...string) {
	t.Helper()
	node, err := s.Stat(ctx(t), "")
	if err != nil {
		t.Fatalf("the root no longer stats: %v", err)
	}
	if !node.IsDir() {
		t.Fatalf("the root has mode %v, want a directory", node.Mode)
	}
	children, err := s.List(ctx(t), "")
	if err != nil {
		t.Fatalf("the root no longer lists: %v", err)
	}
	if got := names(children); !slices.Equal(got, want) {
		t.Fatalf("the root holds %q, want %q", got, want)
	}
}
