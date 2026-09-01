// Package storagetest holds the contract that every storage.Storage implementation must
// satisfy. It is the executable form of the obligations written in the storage package's
// documentation: an implementation is correct exactly when it passes Run.
//
// The point of keeping the suite here, rather than beside any one implementation, is
// that the local-directory storage and the network-backed one run the identical cases.
// Two implementations passing the same suite is the only evidence that the interface is
// an abstraction rather than a description of whichever one was written first.
package storagetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// NewStorage produces a fresh, empty namespace. Run calls it once per case, so cases
// never observe each other's writes.
type NewStorage func(t *testing.T) storage.Storage

// Run exercises the whole contract.
func Run(t *testing.T, newStorage NewStorage) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.run(t, newStorage(t))
		})
	}
}

type testCase struct {
	name string
	run  func(t *testing.T, s storage.Storage)
}

var cases = []testCase{
	// --- identity (R-FS-5) --------------------------------------------------------

	{"a node has an identity, and it is not zero", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		for _, p := range []string{"", "f", "d"} {
			attr, err := s.Stat(ctx(t), p)
			if err != nil {
				t.Fatalf("stat %q: %v", p, err)
			}
			// Zero is what a namespace that never filled this in reports, and it would
			// compare equal for every node — a mountpoint above it would hand one node's
			// bytes to a descriptor open on another and never say so.
			if attr.ID == 0 {
				t.Fatalf("%q reports no identity, so nothing above can tell it from any other node", p)
			}
		}
	}},

	{"two nodes have two identities", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Create(ctx(t), "g"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))

		seen := map[uint64]string{}
		for _, p := range []string{"", "f", "g", "d"} {
			attr, err := s.Stat(ctx(t), p)
			if err != nil {
				t.Fatalf("stat %q: %v", p, err)
			}
			if previous, taken := seen[attr.ID]; taken {
				t.Fatalf("%q and %q share the identity %d", previous, p, attr.ID)
			}
			seen[attr.ID] = p
		}
	}},

	// A rename is the case R-FS-5 turns on, and it is the one every namespace can answer:
	// a name changes and a node does not.
	//
	// Whether a *write* keeps a node's identity is deliberately not asked here. A namespace
	// that keeps its tree separately from its bytes repoints the node and keeps it; one over
	// a host filesystem stages the new contents beside the old and renames them into place,
	// which is the only way to make the replacement atomic there and gives the host's own
	// numbering no choice but to change. Both are honest, so the contract asks for neither.
	{"an identity survives a rename", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Write(ctx(t), "before", []byte("one")))
		was := statID(t, s, "before")

		mustSucceed(t, s.Rename(ctx(t), "before", "after"))
		if now := statID(t, s, "after"); now != was {
			t.Fatalf("a renamed node reports identity %d, want the %d it had: a rename changes a name, not a node", now, was)
		}
	}},

	// A name removed and then created again is deliberately not asked about either. A
	// namespace with its own numbering never reuses one, and a host filesystem hands the
	// number back as soon as the node holding it is gone, so a name recreated at once can
	// come back with the number that just left. What that costs is bounded by where the
	// number the kernel sees is minted, which is above this contract and never reuses one:
	// the identity here is only ever compared to decide whether a name still holds the node
	// it held before, and a recycled one makes that comparison miss rather than lie.
	{"a name that holds a new node holds a new identity", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Write(ctx(t), "doc", []byte("one")))
		was := statID(t, s, "doc")

		// The ordinary atomic save. What is at the name afterwards is a different node,
		// and reporting the identity the old one had is what lets a mountpoint above hand
		// an open descriptor another file's bytes.
		mustSucceed(t, s.Write(ctx(t), "doc.tmp", []byte("two")))
		mustSucceed(t, s.Rename(ctx(t), "doc.tmp", "doc"))
		if now := statID(t, s, "doc"); now == was {
			t.Fatalf("a name renamed over kept the identity %d of the node it held before", was)
		}
	}},

	{"a listing reports the same identities a stat does", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))

		entries, err := s.List(ctx(t), "")
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, e := range entries {
			// A listing and a stat of one name must agree, or a program that pairs the two
			// sees two files where there is one.
			if want := statID(t, s, e.Name); e.Attr.ID != want {
				t.Fatalf("the listing gives %q identity %d and a stat gives it %d", e.Name, e.Attr.ID, want)
			}
		}
	}},

	// --- the root -----------------------------------------------------------------

	{"root is a directory, however it is named", func(t *testing.T, s storage.Storage) {
		for _, p := range []string{"", ".", "./", "a/.."} {
			attr, err := s.Stat(ctx(t), p)
			if err != nil {
				t.Fatalf("stat %q: %v", p, err)
			}
			if !attr.IsDir() {
				t.Fatalf("stat %q has mode %v, want a directory", p, attr.Mode)
			}
		}
	}},

	{"a fresh namespace is empty", func(t *testing.T, s storage.Storage) {
		entries, err := s.List(ctx(t), "")
		mustSucceed(t, err)
		if len(entries) != 0 {
			t.Fatalf("fresh namespace lists %v, want nothing", entries)
		}
	}},

	// The root is not a node the caller put there, so removing it, moving it, or putting
	// something else in its place are not operations the namespace offers. A namespace
	// whose root is gone answers ENOENT to everything afterwards — "that file is not
	// there" when the truth is that the namespace is not there, which is the one lie this
	// contract exists to prevent.
	//
	// EBUSY is what the kernel answers for the equivalent. rmdir("/") and rename with "/"
	// as either operand both fail with EBUSY on Linux, refusing the directory for what it
	// is rather than for how it was named or what it still holds; rmdir(2) gives the
	// reason as "pathname is currently in use by the system", which is precisely the case
	// here. See https://man7.org/linux/man-pages/man2/rmdir.2.html and
	// https://man7.org/linux/man-pages/man2/rename.2.html.
	{"the root cannot be removed", func(t *testing.T, s storage.Storage) {
		for _, p := range []string{"", ".", "./", "a/.."} {
			mustFail(t, s.RemoveDir(ctx(t), p), syscall.EBUSY)
		}
		// Still EBUSY once the root has entries. ENOTEMPTY would say the root would go if
		// it were emptied first.
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustFail(t, s.RemoveDir(ctx(t), ""), syscall.EBUSY)
		mustHoldExactly(t, s, "f")
	}},

	{"the root cannot be moved or replaced", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustFail(t, s.Rename(ctx(t), "", "elsewhere"), syscall.EBUSY)
		mustFail(t, s.Rename(ctx(t), "f", ""), syscall.EBUSY)
		mustFail(t, s.Rename(ctx(t), "", ""), syscall.EBUSY)
		mustHoldExactly(t, s, "f")
	}},

	// These four need no rule of their own — the root is a directory that is already
	// there, and each operation's rule for such a node settles it. They are here because
	// the empty path arrives at them by a route no other case takes, and because on a
	// namespace backed by a directory the answers come from the operating system, which
	// answers these the same way: unlink("/") is EISDIR, mkdir("/") is EEXIST.
	{"making a node at the root fails as it would for any directory that is already there", func(t *testing.T, s storage.Storage) {
		mustFail(t, s.Create(ctx(t), ""), syscall.EEXIST)
		mustFail(t, s.Mkdir(ctx(t), ""), syscall.EEXIST)
		mustFail(t, s.Write(ctx(t), "", []byte("x")), syscall.EISDIR)
		mustFail(t, s.Remove(ctx(t), ""), syscall.EISDIR)
		mustHoldExactly(t, s)
	}},

	// --- create -------------------------------------------------------------------

	{"create makes an empty file", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if attr.IsDir() {
			t.Fatal("created node is a directory, want a file")
		}
		if attr.Size != 0 {
			t.Fatalf("created file has size %d, want 0", attr.Size)
		}
	}},

	{"create over an existing file is EEXIST", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustFail(t, s.Create(ctx(t), "f"), syscall.EEXIST)
	}},

	{"create over an existing directory is EEXIST", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustFail(t, s.Create(ctx(t), "d"), syscall.EEXIST)
	}},

	{"create under a missing directory is ENOENT", func(t *testing.T, s storage.Storage) {
		mustFail(t, s.Create(ctx(t), "missing/f"), syscall.ENOENT)
	}},

	{"create under a file is ENOTDIR", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustFail(t, s.Create(ctx(t), "f/g"), syscall.ENOTDIR)
	}},

	// --- write and read -----------------------------------------------------------

	{"write then read returns the same bytes", func(t *testing.T, s storage.Storage) {
		content := []byte("hello\x00\xff world")
		mustSucceed(t, s.Write(ctx(t), "f", content))
		got, err := s.Read(ctx(t), "f")
		mustSucceed(t, err)
		if !bytes.Equal(got, content) {
			t.Fatalf("read %q, want %q", got, content)
		}
	}},

	{"write creates a file that was not there", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Write(ctx(t), "f", []byte("x")))
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if attr.Size != 1 {
			t.Fatalf("size %d, want 1", attr.Size)
		}
	}},

	{"write replaces the whole of the previous contents", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Write(ctx(t), "f", []byte("a long previous value")))
		mustSucceed(t, s.Write(ctx(t), "f", []byte("short")))
		got, err := s.Read(ctx(t), "f")
		mustSucceed(t, err)
		if string(got) != "short" {
			t.Fatalf("read %q, want %q — the tail of the old value survived", got, "short")
		}
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if attr.Size != int64(len("short")) {
			t.Fatalf("size %d, want %d", attr.Size, len("short"))
		}
	}},

	{"an empty write leaves an empty file", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Write(ctx(t), "f", []byte("something")))
		mustSucceed(t, s.Write(ctx(t), "f", nil))
		got, err := s.Read(ctx(t), "f")
		mustSucceed(t, err)
		if len(got) != 0 {
			t.Fatalf("read %q, want nothing", got)
		}
	}},

	{"write to a directory is EISDIR", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustFail(t, s.Write(ctx(t), "d", []byte("x")), syscall.EISDIR)
	}},

	{"write under a missing directory is ENOENT", func(t *testing.T, s storage.Storage) {
		mustFail(t, s.Write(ctx(t), "missing/f", []byte("x")), syscall.ENOENT)
	}},

	{"read a missing file is ENOENT", func(t *testing.T, s storage.Storage) {
		_, err := s.Read(ctx(t), "f")
		mustFail(t, err, syscall.ENOENT)
	}},

	{"read a directory is EISDIR", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		_, err := s.Read(ctx(t), "d")
		mustFail(t, err, syscall.EISDIR)
	}},

	// --- write is atomic ------------------------------------------------------------

	// Every other case in this suite reads only after a write has returned, and an
	// implementation that opens the target and truncates it satisfies all of them. The
	// difference between that and a replacement is visible from one place only: a reader
	// running while a write is in flight.
	//
	// The asymmetry is what makes this reliable rather than a race that usually loses. An
	// implementation that replaces the contents in one step cannot fail this however the
	// goroutines interleave, because at every instant the file holds one of the two
	// values. One that truncates first fails within the first few observations, because
	// the gap between emptying the file and filling it is most of the write.
	{"a reader never sees a write in progress", func(t *testing.T, s storage.Storage) {
		// Different lengths and different bytes, so that a torn observation is caught
		// whether it is a prefix of one value or one value laid over the tail of the
		// other. Large enough that filling the file is not instantaneous.
		contents := [2][]byte{
			bytes.Repeat([]byte{'a'}, 128<<10),
			bytes.Repeat([]byte{'b'}, 96<<10),
		}
		// Written before the readers start, so that a reader observing ENOENT is a
		// finding rather than a file that does not exist yet.
		mustSucceed(t, s.Write(ctx(t), "f", contents[0]))

		const writes = 200
		const readers = 2

		done := make(chan struct{})
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			for i := range writes {
				if err := s.Write(ctx(t), "f", contents[i%2]); err != nil {
					t.Errorf("write %d: %v", i, err)
					return
				}
			}
		}()

		seen := make([]map[int]int, readers)
		for r := range readers {
			seen[r] = map[int]int{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-done:
						return
					default:
					}
					got, err := s.Read(ctx(t), "f")
					if err != nil {
						t.Errorf("read while a write was in flight: %v", err)
						return
					}
					which := slices.IndexFunc(contents[:], func(content []byte) bool {
						return bytes.Equal(got, content)
					})
					if which < 0 {
						t.Errorf("a read observed %s, which is neither of the two values being written (%s and %s) — the file was read part way through a write",
							summarize(got), summarize(contents[0]), summarize(contents[1]))
						return
					}
					seen[r][which]++
				}
			}()
		}
		wg.Wait()

		// Without both values on record the readers may have run entirely between two
		// writes, in which case nothing above was ever exercised. A reader that stopped on
		// a finding overlapped a write by definition, so the question only arises here
		// when everything observed was well formed.
		if t.Failed() {
			return
		}
		total := map[int]int{}
		for _, counts := range seen {
			for which, n := range counts {
				total[which] += n
			}
		}
		if len(total) < len(contents) {
			t.Fatalf("the readers observed %v across %d writes, so no read overlapped a write and this case proved nothing", total, writes)
		}
	}},

	// --- stat ---------------------------------------------------------------------

	{"stat a missing node is ENOENT", func(t *testing.T, s storage.Storage) {
		_, err := s.Stat(ctx(t), "f")
		mustFail(t, err, syscall.ENOENT)
	}},

	{"stat under a file is ENOTDIR", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		_, err := s.Stat(ctx(t), "f/g")
		mustFail(t, err, syscall.ENOTDIR)
	}},

	{"a written file carries a recent modification time", func(t *testing.T, s storage.Storage) {
		before := time.Now().Add(-time.Minute)
		mustSucceed(t, s.Write(ctx(t), "f", []byte("x")))
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if attr.ModTime.Before(before) || attr.ModTime.After(time.Now().Add(time.Minute)) {
			t.Fatalf("modification time %v is not close to now", attr.ModTime)
		}
	}},

	// --- setattr ------------------------------------------------------------------

	{"setattr sets the permission bits of a file", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o600)}))
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if attr.Mode != 0o600 {
			t.Fatalf("mode %v (%#o), want %v — the type bits must survive a mode change",
				attr.Mode, uint32(attr.Mode), fs.FileMode(0o600))
		}
	}},

	{"setattr sets the permission bits of a directory", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.SetAttr(ctx(t), "d", storage.AttrChange{Mode: mode(0o700)}))
		attr, err := s.Stat(ctx(t), "d")
		mustSucceed(t, err)
		if !attr.IsDir() {
			t.Fatalf("mode %v, want a directory — a mode change may not change a node's kind", attr.Mode)
		}
		if attr.Mode.Perm() != 0o700 {
			t.Fatalf("permission bits %v, want %v", attr.Mode.Perm(), fs.FileMode(0o700))
		}
	}},

	// The three bits beyond the permission bits are settable, so they have to survive
	// being set. An implementation that keeps only Perm() drops them silently, which
	// reads as chmod having done something other than what it was asked.
	{"setattr sets the setuid, setgid and sticky bits", func(t *testing.T, s storage.Storage) {
		want := fs.FileMode(0o754) | fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: &want}))
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if attr.Mode != want {
			t.Fatalf("mode %v, want %v", attr.Mode, want)
		}
		// And they come off again, which is the half a chmod that only ever adds bits
		// would pass without.
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o644)}))
		attr, err = s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if attr.Mode != 0o644 {
			t.Fatalf("mode %v after clearing them, want %v", attr.Mode, fs.FileMode(0o644))
		}
	}},

	// A mode carrying a node's kind is refused rather than masked down to the bits that
	// are settable. Masking would report success for a request to turn a file into a
	// directory, and the caller would go on believing it had happened.
	{"setattr refuses a mode that names a kind", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o600)}))
		for _, kind := range []fs.FileMode{
			fs.ModeDir, fs.ModeSymlink, fs.ModeNamedPipe, fs.ModeSocket, fs.ModeDevice,
			fs.ModeCharDevice, fs.ModeIrregular, fs.ModeAppend, fs.ModeExclusive,
			fs.ModeTemporary,
		} {
			requested := kind | 0o644
			mustFail(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: &requested}), syscall.EINVAL)
		}
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if attr.Mode != 0o600 {
			t.Fatalf("mode %v after the refusals, want the untouched %v", attr.Mode, fs.FileMode(0o600))
		}
	}},

	{"setattr sets the modification time", func(t *testing.T, s storage.Storage) {
		want := time.Date(2001, time.February, 3, 4, 5, 6, 789_012_345, time.UTC)
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{ModTime: &want}))
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if !attr.ModTime.Equal(want) {
			t.Fatalf("modification time %v, want %v", attr.ModTime.UTC(), want)
		}
	}},

	{"setattr sets the access time", func(t *testing.T, s storage.Storage) {
		want := time.Date(2001, time.February, 3, 4, 5, 6, 789_012_345, time.UTC)
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{AccessTime: &want}))
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if !attr.AccessTime.Equal(want) {
			t.Fatalf("access time %v, want %v", attr.AccessTime.UTC(), want)
		}
	}},

	// The two are separate instants. An implementation keeping one field for both passes
	// each of the cases above and fails this one, and so does a mount that reports the
	// modification time as the access time — which is what utimensat(2) callers such as
	// cp -p and tar are asking about.
	{"setattr sets the access time and the modification time apart", func(t *testing.T, s storage.Storage) {
		accessed := time.Date(1999, time.December, 31, 23, 59, 58, 1, time.UTC)
		changed := time.Date(2004, time.July, 6, 1, 2, 3, 999_999_999, time.UTC)
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{AccessTime: &accessed, ModTime: &changed}))
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if !attr.AccessTime.Equal(accessed) || !attr.ModTime.Equal(changed) {
			t.Fatalf("the node is accessed %v and changed %v, want %v and %v",
				attr.AccessTime.UTC(), attr.ModTime.UTC(), accessed, changed)
		}
	}},

	// The range every Linux filesystem holds, in both directions from the epoch. A time
	// carried as nanoseconds alone would lose the first of these: time.Time.UnixNano is
	// undefined before 1678, and the value that comes back is a plausible date with
	// nothing marking it wrong.
	{"setattr carries times either side of the epoch", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		for _, want := range []time.Time{
			time.Date(1902, time.January, 1, 0, 0, 0, 0, time.UTC),
			time.Date(1969, time.December, 31, 23, 59, 59, 999_999_999, time.UTC),
			time.Unix(0, 0).UTC(),
			time.Date(2038, time.January, 19, 3, 14, 8, 0, time.UTC),
			time.Date(2400, time.June, 1, 12, 0, 0, 500_000_000, time.UTC),
		} {
			mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{ModTime: &want}))
			attr, err := s.Stat(ctx(t), "f")
			mustSucceed(t, err)
			if !attr.ModTime.Equal(want) {
				t.Fatalf("modification time %v, want %v", attr.ModTime.UTC(), want)
			}
		}
	}},

	// chmod and utimensat arrive as separate calls, so setting one must not disturb the
	// other. This is the case a change carrying whole attributes rather than named ones
	// would fail.
	{"setattr leaves the attributes a change does not name alone", func(t *testing.T, s storage.Storage) {
		accessed := time.Date(1999, time.December, 31, 23, 59, 58, 1, time.UTC)
		changed := time.Date(2004, time.July, 6, 1, 2, 3, 999_999_999, time.UTC)
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{
			Mode: mode(0o640), AccessTime: &accessed, ModTime: &changed,
		}))

		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: mode(0o604)}))
		attr, err := s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if !attr.AccessTime.Equal(accessed) || !attr.ModTime.Equal(changed) {
			t.Fatalf("setting the mode moved the times to %v and %v, want %v and %v",
				attr.AccessTime.UTC(), attr.ModTime.UTC(), accessed, changed)
		}

		later := changed.Add(time.Hour)
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{ModTime: &later}))
		attr, err = s.Stat(ctx(t), "f")
		mustSucceed(t, err)
		if attr.Mode != 0o604 {
			t.Fatalf("setting the modification time moved the mode to %v, want %v", attr.Mode, fs.FileMode(0o604))
		}
		if !attr.AccessTime.Equal(accessed) {
			t.Fatalf("setting the modification time moved the access time to %v, want %v",
				attr.AccessTime.UTC(), accessed)
		}
	}},

	// Replacing a file's contents is not a request to change its permissions. Somebody
	// who sets a file to 0600 would otherwise find it back at whatever a new file gets
	// after the next write. Every settable bit, not only the nine permission bits: a
	// namespace that keeps Perm() alone drops a setuid bit with nothing said.
	{"a write keeps the mode the file already had", func(t *testing.T, s storage.Storage) {
		for _, want := range []fs.FileMode{
			0o600,
			0o755 | fs.ModeSetuid,
			0o750 | fs.ModeSetgid,
			0o777 | fs.ModeSticky,
		} {
			mustSucceed(t, s.Write(ctx(t), "f", []byte("first")))
			mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{Mode: &want}))
			mustSucceed(t, s.Write(ctx(t), "f", []byte("second")))
			attr, err := s.Stat(ctx(t), "f")
			mustSucceed(t, err)
			if attr.Mode != want {
				t.Fatalf("mode %v after a write, want the %v it was set to", attr.Mode, want)
			}
			mustSucceed(t, s.Remove(ctx(t), "f"))
		}
	}},

	// A listing carries attributes so that listing n entries costs one call. That is only
	// worth anything if they are the same attributes a stat reports.
	{"list carries the attributes setattr set", func(t *testing.T, s storage.Storage) {
		accessed := time.Date(1999, time.December, 31, 23, 59, 58, 1, time.UTC)
		changed := time.Date(2004, time.July, 6, 1, 2, 3, 999_999_999, time.UTC)
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{
			Mode: mode(0o640), AccessTime: &accessed, ModTime: &changed,
		}))

		entries, err := s.List(ctx(t), "")
		mustSucceed(t, err)
		if len(entries) != 1 || entries[0].Name != "f" {
			t.Fatalf("listed %+v, want exactly the name %q", entries, "f")
		}
		got := entries[0].Attr
		if got.Mode != 0o640 || !got.AccessTime.Equal(accessed) || !got.ModTime.Equal(changed) {
			t.Fatalf("the listing reports mode %v, accessed %v, changed %v; a stat reports what was set: %v, %v, %v",
				got.Mode, got.AccessTime.UTC(), got.ModTime.UTC(), fs.FileMode(0o640), accessed, changed)
		}
	}},

	// The root is a directory that is already there, and the rule for such a node settles
	// this: rsync -a and tar -x both set the mode and the times of the directory they are
	// filling, and that directory is the root when the destination is the whole namespace.
	{"setattr changes the root like any other directory", func(t *testing.T, s storage.Storage) {
		changed := time.Date(2004, time.July, 6, 1, 2, 3, 0, time.UTC)
		mustSucceed(t, s.Create(ctx(t), "f"))
		for _, p := range []string{"", ".", "./", "a/.."} {
			mustSucceed(t, s.SetAttr(ctx(t), p, storage.AttrChange{Mode: mode(0o750), ModTime: &changed}))
		}
		attr, err := s.Stat(ctx(t), "")
		mustSucceed(t, err)
		if !attr.IsDir() || attr.Mode.Perm() != 0o750 || !attr.ModTime.Equal(changed) {
			t.Fatalf("the root is %v, changed %v; want a directory with %v at %v",
				attr.Mode, attr.ModTime.UTC(), fs.FileMode(0o750), changed)
		}
		mustHoldExactly(t, s, "f")
		// Left usable for whatever runs next, which a namespace whose root cannot be
		// entered would not be.
		mustSucceed(t, s.SetAttr(ctx(t), "", storage.AttrChange{Mode: mode(0o755)}))
	}},

	// Each attribute is reached by a route of its own, so each one is a separate chance to
	// answer for a node that is not there.
	{"setattr on a missing node is ENOENT", func(t *testing.T, s storage.Storage) {
		moment := time.Date(2004, time.July, 6, 1, 2, 3, 0, time.UTC)
		for _, change := range []storage.AttrChange{
			{Mode: mode(0o600)},
			{AccessTime: &moment},
			{ModTime: &moment},
			{Mode: mode(0o600), AccessTime: &moment, ModTime: &moment},
			{},
		} {
			mustFail(t, s.SetAttr(ctx(t), "f", change), syscall.ENOENT)
		}
	}},

	{"setattr under a file is ENOTDIR", func(t *testing.T, s storage.Storage) {
		moment := time.Date(2004, time.July, 6, 1, 2, 3, 0, time.UTC)
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustFail(t, s.SetAttr(ctx(t), "f/g", storage.AttrChange{Mode: mode(0o600)}), syscall.ENOTDIR)
		mustFail(t, s.SetAttr(ctx(t), "f/g", storage.AttrChange{ModTime: &moment}), syscall.ENOTDIR)
	}},

	// A change that names nothing is still a statement about one node, so it answers for
	// that node rather than succeeding on the strength of having nothing to do.
	{"setattr with nothing to change still answers for the node", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.SetAttr(ctx(t), "f", storage.AttrChange{}))
		mustFail(t, s.SetAttr(ctx(t), "missing", storage.AttrChange{}), syscall.ENOENT)
	}},

	// --- list ---------------------------------------------------------------------

	{"list returns entries sorted by name", func(t *testing.T, s storage.Storage) {
		for _, name := range []string{"c", "a", "b"} {
			mustSucceed(t, s.Create(ctx(t), name))
		}
		entries, err := s.List(ctx(t), "")
		mustSucceed(t, err)
		var got []string
		for _, e := range entries {
			got = append(got, e.Name)
		}
		want := []string{"a", "b", "c"}
		if len(got) != len(want) {
			t.Fatalf("listed %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("listed %v, want %v", got, want)
			}
		}
	}},

	{"list carries each entry's attributes", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Write(ctx(t), "f", []byte("12345")))
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		entries, err := s.List(ctx(t), "")
		mustSucceed(t, err)
		byName := map[string]storage.Attr{}
		for _, e := range entries {
			byName[e.Name] = e.Attr
		}
		if got, ok := byName["f"]; !ok || got.IsDir() || got.Size != 5 {
			t.Fatalf("entry for the file is %+v (present=%v), want a 5-byte file", got, ok)
		}
		if got, ok := byName["d"]; !ok || !got.IsDir() {
			t.Fatalf("entry for the directory is %+v (present=%v), want a directory", got, ok)
		}
	}},

	{"list names only the immediate children", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.Create(ctx(t), "d/inner"))
		entries, err := s.List(ctx(t), "d")
		mustSucceed(t, err)
		if len(entries) != 1 || entries[0].Name != "inner" {
			t.Fatalf("listed %+v, want exactly the name %q", entries, "inner")
		}
	}},

	{"list a file is ENOTDIR", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		_, err := s.List(ctx(t), "f")
		mustFail(t, err, syscall.ENOTDIR)
	}},

	{"list a missing directory is ENOENT", func(t *testing.T, s storage.Storage) {
		_, err := s.List(ctx(t), "d")
		mustFail(t, err, syscall.ENOENT)
	}},

	// --- mkdir --------------------------------------------------------------------

	{"mkdir makes a directory", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		attr, err := s.Stat(ctx(t), "d")
		mustSucceed(t, err)
		if !attr.IsDir() {
			t.Fatalf("mode %v, want a directory", attr.Mode)
		}
	}},

	{"mkdir over anything that exists is EEXIST", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustFail(t, s.Mkdir(ctx(t), "d"), syscall.EEXIST)
		mustFail(t, s.Mkdir(ctx(t), "f"), syscall.EEXIST)
	}},

	{"mkdir under a missing directory is ENOENT", func(t *testing.T, s storage.Storage) {
		mustFail(t, s.Mkdir(ctx(t), "missing/d"), syscall.ENOENT)
	}},

	{"directories nest", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "a"))
		mustSucceed(t, s.Mkdir(ctx(t), "a/b"))
		mustSucceed(t, s.Write(ctx(t), "a/b/f", []byte("deep")))
		got, err := s.Read(ctx(t), "a/b/f")
		mustSucceed(t, err)
		if string(got) != "deep" {
			t.Fatalf("read %q, want %q", got, "deep")
		}
	}},

	// --- remove -------------------------------------------------------------------

	{"remove deletes a file", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustSucceed(t, s.Remove(ctx(t), "f"))
		_, err := s.Stat(ctx(t), "f")
		mustFail(t, err, syscall.ENOENT)
	}},

	{"remove a missing file is ENOENT", func(t *testing.T, s storage.Storage) {
		mustFail(t, s.Remove(ctx(t), "f"), syscall.ENOENT)
	}},

	{"remove a directory is EISDIR", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustFail(t, s.Remove(ctx(t), "d"), syscall.EISDIR)
	}},

	{"removedir deletes an empty directory", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.RemoveDir(ctx(t), "d"))
		_, err := s.Stat(ctx(t), "d")
		mustFail(t, err, syscall.ENOENT)
	}},

	{"removedir a missing directory is ENOENT", func(t *testing.T, s storage.Storage) {
		mustFail(t, s.RemoveDir(ctx(t), "d"), syscall.ENOENT)
	}},

	{"removedir a file is ENOTDIR", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "f"))
		mustFail(t, s.RemoveDir(ctx(t), "f"), syscall.ENOTDIR)
	}},

	{"removedir a directory that still has entries is ENOTEMPTY", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.Create(ctx(t), "d/f"))
		mustFail(t, s.RemoveDir(ctx(t), "d"), syscall.ENOTEMPTY)
	}},

	// --- rename -------------------------------------------------------------------

	{"rename moves a file and keeps its contents", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Write(ctx(t), "from", []byte("payload")))
		mustSucceed(t, s.Rename(ctx(t), "from", "to"))
		if _, err := s.Stat(ctx(t), "from"); !errors.Is(err, syscall.ENOENT) {
			t.Fatalf("the source still stats with error %v, want ENOENT", err)
		}
		got, err := s.Read(ctx(t), "to")
		mustSucceed(t, err)
		if string(got) != "payload" {
			t.Fatalf("read %q, want %q", got, "payload")
		}
	}},

	{"rename moves a directory with everything under it", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "from"))
		mustSucceed(t, s.Write(ctx(t), "from/f", []byte("payload")))
		mustSucceed(t, s.Rename(ctx(t), "from", "to"))
		got, err := s.Read(ctx(t), "to/f")
		mustSucceed(t, err)
		if string(got) != "payload" {
			t.Fatalf("read %q, want %q", got, "payload")
		}
	}},

	{"rename across directories", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "a"))
		mustSucceed(t, s.Mkdir(ctx(t), "b"))
		mustSucceed(t, s.Write(ctx(t), "a/f", []byte("payload")))
		mustSucceed(t, s.Rename(ctx(t), "a/f", "b/f"))
		got, err := s.Read(ctx(t), "b/f")
		mustSucceed(t, err)
		if string(got) != "payload" {
			t.Fatalf("read %q, want %q", got, "payload")
		}
	}},

	{"rename over an existing file replaces it", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Write(ctx(t), "from", []byte("new")))
		mustSucceed(t, s.Write(ctx(t), "to", []byte("old")))
		mustSucceed(t, s.Rename(ctx(t), "from", "to"))
		got, err := s.Read(ctx(t), "to")
		mustSucceed(t, err)
		if string(got) != "new" {
			t.Fatalf("read %q, want %q", got, "new")
		}
	}},

	{"rename a missing source is ENOENT", func(t *testing.T, s storage.Storage) {
		mustFail(t, s.Rename(ctx(t), "from", "to"), syscall.ENOENT)
	}},

	{"rename into a missing directory is ENOENT", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Create(ctx(t), "from"))
		mustFail(t, s.Rename(ctx(t), "from", "missing/to"), syscall.ENOENT)
	}},

	// rename(2) permits either ENOTEMPTY or EEXIST here, and which one arrives depends
	// on the host filesystem. Pinning one of them would make the contract stricter than
	// the kernel it has to sit on top of, and nothing above needs to tell them apart.
	{"rename over a directory that still has entries fails", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "from"))
		mustSucceed(t, s.Mkdir(ctx(t), "to"))
		mustSucceed(t, s.Create(ctx(t), "to/f"))
		mustFailWithAny(t, s.Rename(ctx(t), "from", "to"), syscall.ENOTEMPTY, syscall.EEXIST)
	}},

	// POSIX settles this and settles it as a no-op: when the two names "resolve to either
	// the same existing directory entry or different directory entries for the same
	// existing file, rename() shall return successfully and perform no other action". The
	// node keeps its place and its contents however either name is spelled. Nothing is
	// destroyed, which is what an implementation counting what the namespace holds has to
	// see — there is no removal here for it to credit anyone for.
	// https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html
	{"rename onto itself succeeds and changes nothing", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.Write(ctx(t), "f", []byte("payload")))
		mustSucceed(t, s.Write(ctx(t), "d/inner", []byte("deeper")))

		for _, move := range [][2]string{
			{"f", "f"},
			{"f", "./f"},
			{"d/../f", "f"},
			{"d", "d"},
			{"d/inner", "d/./inner"},
		} {
			if err := s.Rename(ctx(t), move[0], move[1]); err != nil {
				t.Fatalf("rename %q to %q: %v", move[0], move[1], err)
			}
		}

		for _, held := range [][2]string{{"f", "payload"}, {"d/inner", "deeper"}} {
			got, err := s.Read(ctx(t), held[0])
			if err != nil {
				t.Fatalf("read %q: %v", held[0], err)
			}
			if string(got) != held[1] {
				t.Fatalf("read %q gave %q, want %q", held[0], got, held[1])
			}
		}
		mustHoldExactly(t, s, "d", "f")
	}},

	// The node still has to be there, and this is the input that says whether an
	// implementation established that. Two names that resolve to one node are cheap to spot
	// from the operands alone, and answering from them is a report that a move happened
	// where the truth is that there was nothing to move — indistinguishable, to everything
	// above, from the successful no-op above it.
	{"rename a missing node onto itself is ENOENT", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		for _, move := range [][2]string{
			{"missing", "missing"},
			{"d/../missing", "missing"},
			{"d/missing", "d/./missing"},
		} {
			if err := s.Rename(ctx(t), move[0], move[1]); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("rename %q to %q failed with %v, want ENOENT", move[0], move[1], err)
			}
		}
		mustHoldExactly(t, s, "d")
	}},

	// --- space --------------------------------------------------------------------

	// Space has two permitted outcomes and no third. Any other error would leave whatever
	// asked unable to tell "this namespace has no room of its own to report" from "the
	// room could not be measured this time", and those call for opposite reactions: the
	// first is settled for good, the second is worth asking again.
	//
	// Figures that could not be true of anything are the same failure wearing a plausible
	// face. They arrive as room that exists, and a program that checks for space before
	// writing acts on them.
	{"space either answers with figures that can be true, or reports ENOSYS", func(t *testing.T, s storage.Storage) {
		spaceOf(t, s)
	}},

	// Whether a namespace has room of its own to report is a property of what it is, not
	// of what it currently holds. An implementation that answered only once it had
	// something to count would have a caller reading its refusal as a transient failure —
	// and an empty namespace is the state anything asks about first.
	{"whether space answers does not change over a namespace's life", func(t *testing.T, s storage.Storage) {
		_, answered := spaceOf(t, s)

		mustSucceed(t, s.Mkdir(ctx(t), "d"))
		mustSucceed(t, s.Write(ctx(t), "d/f", bytes.Repeat([]byte{'x'}, 64<<10)))
		if _, again := spaceOf(t, s); again != answered {
			t.Fatalf("space %s for an empty namespace and %s once it held a file",
				answerKind(answered), answerKind(again))
		}

		mustSucceed(t, s.Remove(ctx(t), "d/f"))
		mustSucceed(t, s.RemoveDir(ctx(t), "d"))
		if _, again := spaceOf(t, s); again != answered {
			t.Fatalf("space %s for an empty namespace and %s once it was empty again",
				answerKind(answered), answerKind(again))
		}
	}},

	// --- paths --------------------------------------------------------------------

	{"an absolute path is EINVAL", func(t *testing.T, s storage.Storage) {
		_, err := s.Stat(ctx(t), "/f")
		mustFail(t, err, syscall.EINVAL)
	}},

	{"a path that climbs out of the root is EINVAL", func(t *testing.T, s storage.Storage) {
		for _, p := range []string{"..", "../f", "a/../../f"} {
			if _, err := s.Stat(ctx(t), p); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("stat %q failed with %v, want EINVAL", p, err)
			}
		}
	}},

	// Checking this on one operation is not enough. Each operation resolves the path
	// itself, so each one is a separate chance to follow it out of the namespace.
	{"every operation refuses a path outside the root", func(t *testing.T, s storage.Storage) {
		const outside = "../outside"

		_, err := s.Stat(ctx(t), outside)
		mustFail(t, err, syscall.EINVAL)
		_, err = s.List(ctx(t), outside)
		mustFail(t, err, syscall.EINVAL)
		_, err = s.Read(ctx(t), outside)
		mustFail(t, err, syscall.EINVAL)

		mustFail(t, s.Write(ctx(t), outside, []byte("x")), syscall.EINVAL)
		mustFail(t, s.SetAttr(ctx(t), outside, storage.AttrChange{Mode: mode(0o600)}), syscall.EINVAL)
		mustFail(t, s.Create(ctx(t), outside), syscall.EINVAL)
		mustFail(t, s.Mkdir(ctx(t), outside), syscall.EINVAL)
		mustFail(t, s.Remove(ctx(t), outside), syscall.EINVAL)
		mustFail(t, s.RemoveDir(ctx(t), outside), syscall.EINVAL)
		mustFail(t, s.Rename(ctx(t), outside, "to"), syscall.EINVAL)
		mustFail(t, s.Rename(ctx(t), "from", outside), syscall.EINVAL)
	}},

	{"redundant path forms address the same node", func(t *testing.T, s storage.Storage) {
		mustSucceed(t, s.Mkdir(ctx(t), "a"))
		mustSucceed(t, s.Write(ctx(t), "a/f", []byte("payload")))
		for _, p := range []string{"a//f", "./a/f", "a/./f", "a/b/../f"} {
			got, err := s.Read(ctx(t), p)
			if err != nil {
				t.Fatalf("read %q: %v", p, err)
			}
			if string(got) != "payload" {
				t.Fatalf("read %q gave %q, want %q", p, got, "payload")
			}
		}
	}},
}

// statID is the identity the namespace reports for one name.
func statID(t *testing.T, s storage.Storage, path string) uint64 {
	t.Helper()
	attr, err := s.Stat(ctx(t), path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	return attr.ID
}

func ctx(t *testing.T) context.Context { return t.Context() }

// mode is the address of a mode, which is what an AttrChange takes: a nil there means
// "not changing this", so every mode being set has to be somewhere addressable.
func mode(m fs.FileMode) *fs.FileMode { return &m }

// summarize renders an observation in one line. The contents involved run to six figures
// of identical bytes, so printing them says nothing; the length and the run structure say
// exactly how an observation was torn — 98304 bytes of 'b' where 131072 were expected is
// a write caught part way, and 'b's followed by 'a's is one value laid over another.
func summarize(b []byte) string {
	if len(b) == 0 {
		return "0 bytes"
	}
	var runs []string
	for i := 0; i < len(b); {
		j := i
		for j < len(b) && b[j] == b[i] {
			j++
		}
		runs = append(runs, fmt.Sprintf("%d×%q", j-i, b[i]))
		i = j
		if len(runs) == 4 && i < len(b) {
			runs = append(runs, "…")
			break
		}
	}
	return fmt.Sprintf("%d bytes, %s", len(b), strings.Join(runs, " then "))
}

// spaceOf asks for the room the namespace has and refuses everything the contract does
// not allow: a refusal other than ENOSYS, and an answer that could not be true of
// anything. The second result says whether figures came back, so that a case which goes
// on to read them says so rather than passing quietly on a namespace that refused —
// silence there would let an implementation satisfy the whole section by answering ENOSYS
// to all of it.
func spaceOf(t *testing.T, s storage.Storage) (storage.Space, bool) {
	t.Helper()
	space, err := s.Space(ctx(t))
	if errors.Is(err, syscall.ENOSYS) {
		return storage.Space{}, false
	}
	if err != nil {
		t.Fatalf("space failed with %v, want either figures or ENOSYS", err)
	}
	if !space.Coherent() {
		t.Fatalf("space reports a total of %d bytes with %d used and %d available, which cannot be true of anything",
			space.Total, space.Used, space.Avail)
	}
	return space, true
}

// answerKind names what Space did, for the message of a case that compares two calls.
func answerKind(answered bool) string {
	if answered {
		return "answered"
	}
	return "reported ENOSYS"
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
// names. An operation aimed at the root that damaged it would otherwise surface much
// later, as ENOENT from something unrelated.
func mustHoldExactly(t *testing.T, s storage.Storage, names ...string) {
	t.Helper()
	attr, err := s.Stat(ctx(t), "")
	if err != nil {
		t.Fatalf("the root no longer stats: %v", err)
	}
	if !attr.IsDir() {
		t.Fatalf("the root has mode %v, want a directory", attr.Mode)
	}
	entries, err := s.List(ctx(t), "")
	if err != nil {
		t.Fatalf("the root no longer lists: %v", err)
	}
	got := make([]string, 0, len(entries))
	for _, e := range entries {
		got = append(got, e.Name)
	}
	if !slices.Equal(got, names) {
		t.Fatalf("the root holds %v, want %v", got, names)
	}
}
