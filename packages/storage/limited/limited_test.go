package limited_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/storage/storagetest"
)

// The whole contract is put to a limited namespace, under an allowance no case here comes
// near. A decorator that answered anything itself — an early refusal, a rewritten errno, a
// path it cleaned differently from the storage beneath it — shows up as one of these
// sixty-odd cases failing, and nothing else looks for it.
func TestContract(t *testing.T) {
	for _, native := range []bool{true, false} {
		name := "native publication"
		if !native {
			name = "unmanaged bounded storage"
		}
		t.Run(name, func(t *testing.T) {
			storagetest.Run(t, func(t *testing.T) storage.Storage {
				backing := openDir(t, t.TempDir())
				if !native {
					backing = &faulty{BoundedStorage: backing}
				}
				return newStorageOver(t, backing, 1<<30)
			})
		})
	}
}

func TestBoundedContract(t *testing.T) {
	storagetest.RunBounded(t, func(t *testing.T) storage.BoundedStorage {
		return newStorage(t, t.TempDir(), 1<<30)
	})
}

func TestWriteChargesWhatTheFileGains(t *testing.T) {
	s := newStorage(t, t.TempDir(), 1<<20)

	mustWrite(t, s, "f", 100)
	mustUse(t, s, 100)

	mustWrite(t, s, "f", 30)
	mustUse(t, s, 30)

	mustWrite(t, s, "f", 250)
	mustUse(t, s, 250)

	mustWrite(t, s, "g", 40)
	mustUse(t, s, 290)
}

func TestRemoveCreditsWhatTheFileHeld(t *testing.T) {
	s := newStorage(t, t.TempDir(), 1<<20)

	mustWrite(t, s, "f", 100)
	mustWrite(t, s, "g", 40)
	mustUse(t, s, 140)

	if err := s.Remove(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 40)
}

// A move charges nothing for the bytes it carries — they are charged already and stay
// charged wherever they land — and credits back only what it destroys on arrival.
func TestRenameCreditsOnlyWhatItReplaces(t *testing.T) {
	s := newStorage(t, t.TempDir(), 1<<20)

	mustWrite(t, s, "a", 100)
	mustWrite(t, s, "b", 40)
	mustUse(t, s, 140)

	if err := s.Rename(t.Context(), "a", "c"); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 140)

	if err := s.Rename(t.Context(), "c", "b"); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 100)
}

// A node moved onto itself destroys nothing: POSIX has such a move return successfully and
// perform no other action, so the bytes at the name are the same bytes that are there
// afterwards. Crediting them would hand out room the namespace never released, for as many
// times as anyone cares to repeat the move, and every spelling of the name is a way to ask
// for it.
func TestRenameOntoItselfCreditsNothing(t *testing.T) {
	s := newStorage(t, t.TempDir(), 1<<20)

	mustWrite(t, s, "f", 100)
	mustWrite(t, s, "g", 40)
	mustUse(t, s, 140)

	for _, to := range []string{"f", "f", "./f", "d/../f"} {
		if err := s.Rename(t.Context(), "f", to); err != nil {
			t.Fatalf("renaming %q onto %q: %v", "f", to, err)
		}
		mustUse(t, s, 140)
	}

	// The count is not merely unchanged but still the measured figure, which a count moved
	// twice in opposite directions would also satisfy.
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 140)
}

// Create makes an empty file, which costs nothing; the bytes arrive later through Write.
// A directory and an attribute change hold no bytes either.
func TestTheOperationsThatCarryNoBytesChargeNothing(t *testing.T) {
	s := newStorage(t, t.TempDir(), 1<<20)

	if err := s.Create(t.Context(), "f"); err != nil {
		t.Fatal(err)
	}
	if err := s.Mkdir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	mode := os.FileMode(0o600)
	if err := s.SetAttr(t.Context(), "f", storage.AttrChange{Mode: &mode}); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveDir(t.Context(), "d"); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 0)
}

func TestAWriteBeyondTheAllowanceIsRefusedAndCostsNothing(t *testing.T) {
	s := newStorage(t, t.TempDir(), 8192)

	mustWrite(t, s, "f", 8000)
	mustUse(t, s, 8000)

	err := s.Write(t.Context(), "g", content(500))
	if !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("writing 500 bytes with 192 of an 8192-byte allowance left failed with %v, want EDQUOT", err)
	}
	mustUse(t, s, 8000)
	if _, err := s.Stat(t.Context(), "g"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("the refused write left %v behind, want nothing", err)
	}
}

// An allowance lowered underneath content already written leaves a namespace over its
// limit, and the way back under it is to write less. Refusing the write that shrinks a
// file would close that route off.
func TestAWriteThatShrinksIsTakenFromOverTheAllowance(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f"), content(10000), 0o600); err != nil {
		t.Fatal(err)
	}
	s := newStorage(t, root, 8192)

	space := spaceOf(t, s)
	if space.Used != 10000 || space.Avail != 0 {
		t.Fatalf("a namespace holding 10000 bytes under an 8192-byte allowance reports %+v, want 10000 used and nothing available", space)
	}

	mustWrite(t, s, "f", 100)
	mustUse(t, s, 100)
}

// The charge is made before the write and has to come back when the write does not happen.
// A charge left standing would take the namespace's room away a failure at a time.
func TestAWriteTheStoreBeneathRefusesGivesTheChargeBack(t *testing.T) {
	s := newStorageOver(t, &faulty{BoundedStorage: openDir(t, t.TempDir()), write: syscall.EIO}, 8192)

	if err := s.Write(t.Context(), "f", content(500)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("the write failed with %v, want the EIO the store beneath gave", err)
	}
	mustUse(t, s, 0)
}

// Failed shrinking writes retain the original count even when an out-of-band change
// has left it below the file's current size.
func TestFailedShrinkPreservesTheCountAfterOutOfBandGrowth(t *testing.T) {
	root := t.TempDir()
	beneath := &faulty{BoundedStorage: openDir(t, root)}
	s := newStorageOver(t, beneath, 8192)

	mustWrite(t, s, "f", 5)
	if err := os.WriteFile(filepath.Join(root, "f"), content(15), 0o600); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 5)

	beneath.write = syscall.EIO
	if err := s.Write(t.Context(), "f", content(5)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("the write failed with %v, want the EIO the store beneath gave", err)
	}
	mustUse(t, s, 5)
}

// What a write charges is the difference from what the file holds now, so a size that
// could not be read leaves nothing to charge against. The write is refused with what the
// stat reported rather than charged as if the file were not there: a write that landed
// uncharged is the one direction the count may not err in.
func TestAWriteIsRefusedWhenWhatTheFileHoldsCannotBeRead(t *testing.T) {
	s := newStorageOver(t, &faulty{BoundedStorage: openDir(t, t.TempDir()), stat: syscall.EIO}, 8192)

	if err := s.Write(t.Context(), "f", content(500)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("the write failed with %v, want the EIO the stat gave", err)
	}
	mustUse(t, s, 0)
}

// Bytes are credited back only once they are gone. A removal that failed released nothing,
// and crediting it would hand out room the namespace still holds.
func TestARemovalThatFailedCreditsNothing(t *testing.T) {
	s := newStorageOver(t, &faulty{BoundedStorage: openDir(t, t.TempDir()), remove: syscall.EACCES}, 8192)

	mustWrite(t, s, "f", 500)
	if err := s.Remove(t.Context(), "f"); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("the removal failed with %v, want the EACCES the store beneath gave", err)
	}
	mustUse(t, s, 500)
}

// A namespace that cannot be walked cannot be held under an allowance: every figure
// afterwards is that walk moved by what passes through, so an unmeasured start is an
// invented one.
func TestNewRefusesANamespaceItCouldNotWalk(t *testing.T) {
	_, err := limited.New(t.Context(), &faulty{BoundedStorage: openDir(t, t.TempDir()), list: syscall.EIO}, 8192)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening the namespace failed with %v, want EIO", err)
	}
}

func TestMeasurementLimitsHaveBoundedDefaultsAndRejectImpossibleValues(t *testing.T) {
	defaults := limited.DefaultMeasurementLimits()
	if defaults.MaxDirectoryBytes <= 0 || defaults.MaxFrontierBytes <= 0 {
		t.Fatalf("the default measurement limits are not bounded positive values: %+v", defaults)
	}
	settled, err := (limited.MeasurementLimits{}).Effective()
	if err != nil {
		t.Fatal(err)
	}
	if settled != defaults {
		t.Fatalf("zero measurement limits settle to %+v, want %+v", settled, defaults)
	}
	for _, limits := range []limited.MeasurementLimits{
		{MaxDirectoryBytes: -1},
		{MaxDirectoryBytes: math.MaxInt64},
		{MaxFrontierBytes: -1},
		{MaxFrontierBytes: 1},
		{MaxFrontierBytes: math.MaxInt64},
	} {
		if err := limits.Validate(); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("measurement limits %+v were refused with %v, want EINVAL", limits, err)
		}
	}
}

func TestNewRequiresBoundedListingsBeforeItEnumeratesTheNamespace(t *testing.T) {
	backing := &unboundedListing{Storage: openDir(t, t.TempDir())}
	_, err := limited.New(t.Context(), backing, 8192)
	if !errors.Is(err, syscall.ENOSYS) {
		t.Fatalf("opening over storage without bounded listings failed with %v, want ENOSYS", err)
	}
	if backing.listed {
		t.Fatal("opening enumerated the namespace through ordinary List")
	}
}

func TestNewChecksBoundedCapabilityBeforeItEnumeratesTheNamespace(t *testing.T) {
	backing := &boundedProbe{BoundedStorage: openDir(t, t.TempDir()), check: syscall.EIO}
	_, err := limited.New(t.Context(), backing, 8192)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening over an unusable bounded storage failed with %v, want EIO", err)
	}
	if backing.boundedLists != 0 || backing.ordinaryLists != 0 {
		t.Fatalf("capability failure was followed by %d bounded and %d ordinary listings",
			backing.boundedLists, backing.ordinaryLists)
	}
}

func TestMeasurementUsesBoundedListingAndStopsBeforeRetainingAHugeDirectory(t *testing.T) {
	const entries = 100_000
	backing := &generatedListing{BoundedStorage: openDir(t, t.TempDir()), entries: entries}
	limits := limited.MeasurementLimits{
		MaxDirectoryBytes: 1024,
		MaxFrontierBytes:  limited.DefaultMaxFrontierBytes,
	}
	_, err := limited.NewWithLimits(t.Context(), backing, 8192, limits)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("opening over a huge directory failed with %v, want EIO", err)
	}
	if backing.ordinaryLists != 0 {
		t.Fatalf("measurement called ordinary List %d times", backing.ordinaryLists)
	}
	if backing.produced == 0 {
		t.Fatal("the bounded listing did not produce an entry before reaching the bound")
	}
	if backing.produced >= entries {
		t.Fatalf("measurement produced all %d entries before enforcing its directory bound", entries)
	}
}

func TestDirectoryMeasurementBoundIncludesEntryAndNameRetention(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "f"), content(7), 0o600); err != nil {
		t.Fatal(err)
	}
	entryBytes := int64(unsafe.Sizeof(storage.Entry{})) + 1
	for _, c := range []struct {
		name  string
		bound int64
		want  syscall.Errno
	}{
		{"the exact charge", entryBytes, 0},
		{"one byte below the charge", entryBytes - 1, syscall.EIO},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, err := limited.NewWithLimits(t.Context(), openDir(t, root), 8192, limited.MeasurementLimits{
				MaxDirectoryBytes: c.bound,
				MaxFrontierBytes:  limited.DefaultMaxFrontierBytes,
			})
			if c.want != 0 {
				if !errors.Is(err, c.want) {
					t.Fatalf("opening failed with %v, want %v", err, c.want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			mustUse(t, s, 7)
		})
	}
}

func TestFrontierMeasurementBoundIncludesActiveAndPendingPaths(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "d"), 0o700); err != nil {
		t.Fatal(err)
	}
	frontierRecordBytes := int64(unsafe.Sizeof(struct {
		path string
		next unsafe.Pointer
	}{}))
	exact := 2*frontierRecordBytes + int64(len("d"))
	for _, c := range []struct {
		name  string
		bound int64
		want  syscall.Errno
	}{
		{"the active root and pending child fit exactly", exact, 0},
		{"one byte below the active and pending paths", exact - 1, syscall.EIO},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := limited.NewWithLimits(t.Context(), openDir(t, root), 8192, limited.MeasurementLimits{
				MaxDirectoryBytes: limited.DefaultMaxDirectoryBytes,
				MaxFrontierBytes:  c.bound,
			})
			if c.want == 0 && err != nil {
				t.Fatal(err)
			}
			if c.want != 0 && !errors.Is(err, c.want) {
				t.Fatalf("opening failed with %v, want %v", err, c.want)
			}
		})
	}
}

// A walk that failed replaces nothing. The count that was there is the last measured one,
// and a figure that could not be measured is not one to put in its place.
func TestARecountThatCouldNotWalkKeepsTheCountItHad(t *testing.T) {
	beneath := &faulty{BoundedStorage: openDir(t, t.TempDir())}
	s := newStorageOver(t, beneath, 8192)
	mustWrite(t, s, "f", 500)

	beneath.list = syscall.EIO
	if err := s.Recount(t.Context()); !errors.Is(err, syscall.EIO) {
		t.Fatalf("the recount failed with %v, want EIO", err)
	}
	mustUse(t, s, 500)
}

func TestAFrontierThatExceedsItsAggregateBoundInvalidatesTheRecount(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "held"), content(7), 0o600); err != nil {
		t.Fatal(err)
	}
	limits := limited.MeasurementLimits{
		MaxDirectoryBytes: 1 << 20,
		MaxFrontierBytes:  256,
	}
	s, err := limited.NewWithLimits(t.Context(), openDir(t, root), 8192, limits)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 100 {
		if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("directory-%03d", i)), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "uncharged"), content(100), 0o600); err != nil {
		t.Fatal(err)
	}

	err = s.Recount(t.Context())
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("recounting beyond the traversal-frontier bound failed with %v, want EIO", err)
	}
	if !strings.Contains(err.Error(), "traversal frontier") {
		t.Fatalf("recounting beyond the traversal-frontier bound failed with %q", err)
	}
	mustUse(t, s, 7)
	if err := s.Write(t.Context(), "after", content(3)); err != nil {
		t.Fatalf("the failed recount kept the operation gate: %v", err)
	}
	mustUse(t, s, 10)
}

func TestCancelingARecountInvalidatesItAndReleasesTheOperationGate(t *testing.T) {
	backing := &blockingListing{BoundedStorage: openDir(t, t.TempDir()), entered: make(chan struct{})}
	s := newStorageOver(t, backing, 8192)
	mustWrite(t, s, "held", 7)
	backing.block = true

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- s.Recount(ctx) }()
	<-backing.entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("the canceled recount failed with %v, want context cancellation", err)
	}
	mustUse(t, s, 7)
	if err := s.Write(t.Context(), "after", content(3)); err != nil {
		t.Fatalf("the canceled recount kept the operation gate: %v", err)
	}
	mustUse(t, s, 10)
}

func TestCanceledRecountDoesNotDescendAfterTheCurrentListingReturns(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "child"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "child", "held"), content(7), 0o600); err != nil {
		t.Fatal(err)
	}
	backing := &delayedReturnListing{
		BoundedStorage: openDir(t, root),
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}
	s := newStorageOver(t, backing, 8192)
	backing.childLists = 0
	backing.delayRoot = true

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- s.Recount(ctx) }()
	<-backing.entered
	cancel()
	close(backing.release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("the canceled recount failed with %v, want context cancellation", err)
	}
	if backing.childLists != 0 {
		t.Fatalf("the canceled recount listed %d child directories after the root listing returned", backing.childLists)
	}
	mustUse(t, s, 7)
}

// Below one block the mount would report a filesystem of zero blocks, which reads as a
// disk with nothing left rather than as a workspace with a little room.
func TestNewRefusesAnAllowanceBelowOneBlock(t *testing.T) {
	backing := openDir(t, t.TempDir())
	for _, limit := range []int64{math.MinInt64, -1, 0, 1, limited.MinLimit - 1} {
		if _, err := limited.New(t.Context(), backing, limit); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("an allowance of %d bytes was refused with %v, want EINVAL", limit, err)
		}
	}
	if _, err := limited.New(t.Context(), backing, limited.MinLimit); err != nil {
		t.Errorf("an allowance of one block: %v", err)
	}
}

// The walk that opens a namespace is what makes every figure afterwards a measured one. A
// symbolic link counts for the length of the target it holds; a directory counts for
// nothing, its size being unspecified.
func TestNewCountsWhatTheNamespaceAlreadyHolds(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "d", "deeper"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, size := range map[string]int{"f": 100, "d/g": 40, "d/deeper/h": 7} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), content(size), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	const target = "d/deeper/h"
	if err := os.Symlink(target, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}

	s := newStorage(t, root, 1<<20)
	mustUse(t, s, 100+40+7+int64(len(target)))
}

// Neither figure can come out of a directory, and both would be taken for measured fact if
// they were summed: a negative size shrinks the count towards room that is not there, and a
// sum past what a byte count holds wraps into one.
func TestNewRefusesSizesNoNamespaceCanHold(t *testing.T) {
	backing := openDir(t, t.TempDir())
	for _, c := range []struct {
		name    string
		entries []storage.Entry
		want    syscall.Errno
	}{
		{"a negative size", []storage.Entry{{Name: "f", Attr: storage.Attr{Size: -1}}}, syscall.EIO},
		{"more bytes than a count holds", []storage.Entry{
			{Name: "f", Attr: storage.Attr{Size: math.MaxInt64}},
			{Name: "g", Attr: storage.Attr{Size: 1}},
		}, syscall.EOVERFLOW},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := limited.New(t.Context(), listing{BoundedStorage: backing, entries: c.entries}, 1<<20)
			if !errors.Is(err, c.want) {
				t.Fatalf("opening the namespace failed with %v, want %v", err, c.want)
			}
		})
	}
}

// Modifying the served namespace behind the server's back is a non-goal, not a case that is
// handled. This pins what the guard on the count is worth when it is done anyway, and what
// it is not worth.
//
// A file added out of band was never charged, so removing it in band credits bytes nobody
// spent and would drive the count below zero. Holding the count at zero bounds what is
// reported: the figures stay ones that can be true, with no negative Used and no more room
// offered than the allowance holds — which is all that keeps a kernel reply's unsigned
// fields from advertising room no disk anywhere has.
//
// It bounds nothing about what is true. The count sitting at what the namespace holds is
// not guaranteed, and here it is plainly false: 0 reported against the 500 bytes still
// there. The room that appears free is then written, and the namespace ends up over the
// allowance it is held under. Only a fresh measurement puts the figure back.
func TestTheGuardBoundsWhatIsReportedAndNotWhatIsHeld(t *testing.T) {
	root := t.TempDir()
	s := newStorage(t, root, 8192)

	mustWrite(t, s, "f", 500)
	if err := os.WriteFile(filepath.Join(root, "g"), content(1000), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), "g"); err != nil {
		t.Fatal(err)
	}

	space := spaceOf(t, s)
	if space.Used != 0 {
		t.Fatalf("the namespace reports %d bytes used, want the 0 the guard holds it at", space.Used)
	}
	if space.Avail > space.Total {
		t.Fatalf("the namespace reports %d bytes available out of a total of %d", space.Avail, space.Total)
	}

	// The consequence, pinned rather than papered over: the whole allowance is spent again
	// on top of the 500 bytes the namespace has never stopped holding.
	mustWrite(t, s, "h", 8192)
	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 8692)
}

func TestRecountReplacesAFigureThatDriftedOutOfBand(t *testing.T) {
	root := t.TempDir()
	s := newStorage(t, root, 1<<20)

	mustWrite(t, s, "f", 500)
	if err := os.WriteFile(filepath.Join(root, "g"), content(700), 0o600); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 500)

	if err := s.Recount(t.Context()); err != nil {
		t.Fatal(err)
	}
	mustUse(t, s, 1200)
}

// Writers to different paths do not exclude one another, so the check and the charge have
// to be one step: writers that all checked before any of them charged would all be told
// there was room for them. Only one of these can be paid for.
func TestTheLastBytesAreSpentOnce(t *testing.T) {
	s := newStorage(t, t.TempDir(), limited.MinLimit)

	const writers, size = 8, 3000
	failures := make([]error, writers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			failures[i] = s.Write(t.Context(), fmt.Sprintf("f%d", i), content(size))
		}()
	}
	close(start)
	wg.Wait()

	taken := 0
	for i, err := range failures {
		switch {
		case err == nil:
			taken++
		case errors.Is(err, syscall.EDQUOT):
		default:
			t.Fatalf("writer %d failed with %v, want either success or EDQUOT", i, err)
		}
	}
	if taken != 1 {
		t.Fatalf("%d of %d writes of %d bytes were taken under an allowance of %d bytes, want 1",
			taken, writers, size, limited.MinLimit)
	}
	mustUse(t, s, size)
}

// Avail is the smaller of what the allowance leaves and what the disk beneath has room
// for, which is why the contract carries it apart from Total-Used. An allowance is a
// ceiling on what may be written rather than evidence the bytes will fit, and a promise of
// room the machine cannot supply is one that file managers and package managers act on.
func TestSpaceReportsTheTighterOfTheAllowanceAndTheDiskBeneath(t *testing.T) {
	for _, c := range []struct {
		name    string
		beneath int64
		want    int64
	}{
		{"the disk beneath is tighter", 300, 300},
		{"the allowance is tighter", 1 << 40, 8192 - 500},
	} {
		t.Run(c.name, func(t *testing.T) {
			beneath := storage.Space{Total: 1 << 40, Used: 0, Avail: c.beneath}
			s := newStorageOver(t, spaceBeneath{BoundedStorage: openDir(t, t.TempDir()), space: beneath}, 8192)

			mustWrite(t, s, "f", 500)
			space := spaceOf(t, s)
			if space.Total != 8192 || space.Used != 500 || space.Avail != c.want {
				t.Fatalf("the namespace reports %+v, want 8192 total, 500 used and %d available", space, c.want)
			}
		})
	}
}

// A store with no room of its own to report leaves the allowance as the only measured fact
// there is, and the allowance is then reported rather than the question being refused.
func TestSpaceLeansOnTheAllowanceAloneWhenTheStoreBeneathHasNoRoom(t *testing.T) {
	s := newStorageOver(t, spaceBeneath{BoundedStorage: openDir(t, t.TempDir()), err: syscall.ENOSYS}, 8192)

	mustWrite(t, s, "f", 500)
	if space := spaceOf(t, s); space != (storage.Space{Total: 8192, Used: 500, Avail: 7692}) {
		t.Fatalf("the namespace reports %+v, want 8192 total, 500 used and 7692 available", space)
	}
}

// Whatever the store beneath says travels into our own Avail through the minimum, so an
// answer of its that cannot be true of anything is a failure to report here too. A failure
// of any other kind is its own answer and is carried out unchanged.
func TestSpaceCarriesOutWhatTheStoreBeneathCouldNotAnswer(t *testing.T) {
	for _, c := range []struct {
		name       string
		beneath    storage.Space
		beneathErr error
		want       syscall.Errno
	}{
		{"figures that cannot be true of anything", storage.Space{Total: 100, Avail: 101}, nil, syscall.EIO},
		{"a store that could not measure itself", storage.Space{}, syscall.EACCES, syscall.EACCES},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := newStorageOver(t, spaceBeneath{BoundedStorage: openDir(t, t.TempDir()), space: c.beneath, err: c.beneathErr}, 8192)
			if _, err := s.Space(t.Context()); !errors.Is(err, c.want) {
				t.Fatalf("space failed with %v, want %v", err, c.want)
			}
		})
	}
}

func openDir(t *testing.T, root string) storage.BoundedStorage {
	t.Helper()
	backing, err := localdir.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if closer, ok := any(backing).(interface{ Close() error }); ok {
		t.Cleanup(func() {
			if err := closer.Close(); err != nil {
				t.Errorf("closing backing directory: %v", err)
			}
		})
	}
	return backing
}

func newStorage(t *testing.T, root string, limit int64) *limited.Storage {
	t.Helper()
	return newStorageOver(t, openDir(t, root), limit)
}

func newStorageOver(t *testing.T, backing storage.Storage, limit int64) *limited.Storage {
	t.Helper()
	s, err := limited.New(t.Context(), backing, limit)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func content(size int) []byte { return bytes.Repeat([]byte{'x'}, size) }

func mustWrite(t *testing.T, s *limited.Storage, name string, size int) {
	t.Helper()
	if err := s.Write(t.Context(), name, content(size)); err != nil {
		t.Fatalf("writing %d bytes to %q: %v", size, name, err)
	}
}

func spaceOf(t *testing.T, s *limited.Storage) storage.Space {
	t.Helper()
	space, err := s.Space(t.Context())
	if err != nil {
		t.Fatalf("space: %v", err)
	}
	if !space.Coherent() {
		t.Fatalf("space reports a total of %d bytes with %d used and %d available, which cannot be true of anything",
			space.Total, space.Used, space.Avail)
	}
	return space
}

func mustUse(t *testing.T, s *limited.Storage, want int64) {
	t.Helper()
	if space := spaceOf(t, s); space.Used != want {
		t.Fatalf("the namespace reports %d bytes used, want %d", space.Used, want)
	}
}

// The three below each fabricate answers that a namespace held in a directory cannot be
// made to give on demand. Each embeds a storage.Storage, so everything except the answers
// under examination is a real namespace's.

// faulty fails the operations it has been given an error for, which is how each of the
// failures a charge has to survive is put to the storage. The errors are settable after it
// is built, for the cases where the namespace has to be filled before the failure starts.
type faulty struct {
	storage.BoundedStorage
	stat   error
	list   error
	write  error
	remove error
}

func (f *faulty) Stat(ctx context.Context, name string) (storage.Attr, error) {
	if f.stat != nil {
		return storage.Attr{}, f.stat
	}
	return f.BoundedStorage.Stat(ctx, name)
}

func (f *faulty) List(ctx context.Context, name string) ([]storage.Entry, error) {
	if f.list != nil {
		return nil, f.list
	}
	return f.BoundedStorage.List(ctx, name)
}

func (f *faulty) ListBounded(ctx context.Context, name string, result *storage.ListResult) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	if f.list != nil {
		return f.list
	}
	return f.BoundedStorage.ListBounded(ctx, name, result)
}

func (f *faulty) Write(ctx context.Context, name string, content []byte) error {
	if f.write != nil {
		return f.write
	}
	return f.BoundedStorage.Write(ctx, name, content)
}

func (f *faulty) Remove(ctx context.Context, name string) error {
	if f.remove != nil {
		return f.remove
	}
	return f.BoundedStorage.Remove(ctx, name)
}

type listing struct {
	storage.BoundedStorage
	entries []storage.Entry
}

func (l listing) List(ctx context.Context, name string) ([]storage.Entry, error) {
	if name == "" {
		return l.entries, nil
	}
	return l.BoundedStorage.List(ctx, name)
}

func (l listing) ListBounded(ctx context.Context, name string, result *storage.ListResult) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	if name != "" {
		return l.BoundedStorage.ListBounded(ctx, name, result)
	}
	for _, entry := range l.entries {
		if err := result.Add(entry); err != nil {
			return err
		}
	}
	return nil
}

type unboundedListing struct {
	storage.Storage
	listed bool
}

func (s *unboundedListing) List(ctx context.Context, name string) ([]storage.Entry, error) {
	s.listed = true
	return s.Storage.List(ctx, name)
}

type boundedProbe struct {
	storage.BoundedStorage
	check         error
	ordinaryLists int
	boundedLists  int
}

func (s *boundedProbe) CheckBounded() error { return s.check }

func (s *boundedProbe) List(ctx context.Context, name string) ([]storage.Entry, error) {
	s.ordinaryLists++
	return s.BoundedStorage.List(ctx, name)
}

func (s *boundedProbe) ListBounded(ctx context.Context, name string, result *storage.ListResult) error {
	s.boundedLists++
	return s.BoundedStorage.ListBounded(ctx, name, result)
}

type generatedListing struct {
	storage.BoundedStorage
	entries       int
	produced      int
	ordinaryLists int
}

func (s *generatedListing) List(context.Context, string) ([]storage.Entry, error) {
	s.ordinaryLists++
	return nil, syscall.EIO
}

func (s *generatedListing) ListBounded(
	ctx context.Context,
	name string,
	result *storage.ListResult,
) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	if name != "" {
		return s.BoundedStorage.ListBounded(ctx, name, result)
	}
	for i := range s.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := storage.Entry{Name: fmt.Sprintf("entry-%06d", i), Attr: storage.Attr{Size: 1}}
		if err := result.Add(entry); err != nil {
			return err
		}
		s.produced++
	}
	return nil
}

type blockingListing struct {
	storage.BoundedStorage
	block   bool
	entered chan struct{}
}

func (s *blockingListing) ListBounded(
	ctx context.Context,
	name string,
	result *storage.ListResult,
) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	if !s.block {
		return s.BoundedStorage.ListBounded(ctx, name, result)
	}
	close(s.entered)
	<-ctx.Done()
	return ctx.Err()
}

type delayedReturnListing struct {
	storage.BoundedStorage
	delayRoot  bool
	entered    chan struct{}
	release    chan struct{}
	childLists int
}

func (s *delayedReturnListing) ListBounded(
	ctx context.Context,
	name string,
	result *storage.ListResult,
) (returned error) {
	if result != nil {
		defer func() {
			if returned != nil {
				result.Fail(returned)
			}
		}()
	}
	if name != "" {
		s.childLists++
	}
	if err := s.BoundedStorage.ListBounded(ctx, name, result); err != nil {
		return err
	}
	if s.delayRoot && name == "" {
		close(s.entered)
		<-s.release
	}
	return nil
}

type spaceBeneath struct {
	storage.BoundedStorage
	space storage.Space
	err   error
}

func (s spaceBeneath) Space(context.Context) (storage.Space, error) { return s.space, s.err }
