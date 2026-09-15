package storage

import (
	"errors"
	"io/fs"
	"math"
	"strings"
	"syscall"
	"testing"
	"unicode/utf16"
)

func TestWindowsOpenValidation(t *testing.T) {
	base := WindowsOpenRequest{WindowsOpenIntent: WindowsOpenIntent{Access: WindowsReadAttributes, Share: WindowsShareAll, Disposition: WindowsOpen}, Lookup: WindowsLookup{ParentID: 1, Name: "file"}}
	cases := []struct {
		name   string
		change func(*WindowsOpenRequest)
		want   syscall.Errno
	}{
		{"metadata only", func(*WindowsOpenRequest) {}, 0},
		{"zero access", func(r *WindowsOpenRequest) { r.Access = 0 }, 0},
		{"unknown access", func(r *WindowsOpenRequest) { r.Access = 1 << 31 }, syscall.EINVAL},
		{"unknown share", func(r *WindowsOpenRequest) { r.Share = 8 }, syscall.EINVAL},
		{"unknown kind", func(r *WindowsOpenRequest) { r.Kind = 3 }, syscall.EINVAL},
		{"zero disposition", func(r *WindowsOpenRequest) { r.Disposition = 0 }, syscall.EINVAL},
		{"unknown disposition", func(r *WindowsOpenRequest) { r.Disposition = 7 }, syscall.EINVAL},
		{"delete access", func(r *WindowsOpenRequest) { r.DeleteOnClose = true }, syscall.EINVAL},
		{"valid delete", func(r *WindowsOpenRequest) { r.DeleteOnClose = true; r.Access |= WindowsDelete }, 0},
		{"overwrite access", func(r *WindowsOpenRequest) { r.Disposition = WindowsOverwrite }, syscall.EINVAL},
		{"valid overwrite", func(r *WindowsOpenRequest) { r.Disposition = WindowsOverwriteIf; r.Access |= WindowsWriteData }, 0},
		{"supersede access", func(r *WindowsOpenRequest) { r.Disposition = WindowsSupersede }, syscall.EINVAL},
		{"valid supersede", func(r *WindowsOpenRequest) { r.Disposition = WindowsSupersede; r.Access |= WindowsDelete }, 0},
		{"overwrite directory", func(r *WindowsOpenRequest) {
			r.Kind = WindowsDirectory
			r.Disposition = WindowsOverwrite
			r.Access |= WindowsWriteData
		}, syscall.EINVAL},
		{"create directory", func(r *WindowsOpenRequest) { r.Kind = WindowsDirectory; r.Disposition = WindowsCreate }, 0},
		{"mode type", func(r *WindowsOpenRequest) { r.Mode = fs.ModeDir }, syscall.EINVAL},
		{"create hidden readonly", func(r *WindowsOpenRequest) {
			r.Disposition = WindowsCreate
			r.DOSAttributes = WindowsDOSHidden | WindowsDOSReadOnly
		}, 0},
		{"create normal", func(r *WindowsOpenRequest) { r.Disposition = WindowsCreate; r.DOSAttributes = WindowsDOSNormal }, 0},
		{"invalid creation attributes", func(r *WindowsOpenRequest) { r.Disposition = WindowsCreate; r.DOSAttributes = 1 << 31 }, syscall.EINVAL},
		{"normal combined with hidden", func(r *WindowsOpenRequest) {
			r.Disposition = WindowsCreate
			r.DOSAttributes = WindowsDOSNormal | WindowsDOSHidden
		}, syscall.EINVAL},
		{"invalid lookup", func(r *WindowsOpenRequest) { r.Lookup.ParentID = 0 }, syscall.EINVAL},
		{"root", func(r *WindowsOpenRequest) { r.Lookup = WindowsLookup{}; r.Kind = WindowsDirectory }, 0},
		{"create root", func(r *WindowsOpenRequest) { r.Lookup = WindowsLookup{}; r.Disposition = WindowsCreate }, syscall.EINVAL},
		{"file root", func(r *WindowsOpenRequest) { r.Lookup = WindowsLookup{}; r.Kind = WindowsRegularFile }, syscall.EINVAL},
		{"delete root", func(r *WindowsOpenRequest) {
			r.Lookup = WindowsLookup{}
			r.Access |= WindowsDelete
			r.DeleteOnClose = true
		}, syscall.EINVAL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { r := base; tc.change(&r); checkWindowsErr(t, r.Check(), tc.want) })
	}
}

func TestWindowsLookupValidation(t *testing.T) {
	cases := []struct {
		lookup WindowsLookup
		want   syscall.Errno
	}{
		{WindowsLookup{}, 0},
		{WindowsLookup{ParentID: 1}, syscall.EINVAL},
		{WindowsLookup{ExpectedID: 1}, syscall.EINVAL},
		{WindowsLookup{ParentReference: "ref"}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: "a", ParentReference: strings.Repeat("r", 129)}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: "a", ParentReference: "a\x00b"}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: "a", ParentReference: "ref", ExpectedID: 2}, 0},
		{WindowsLookup{Name: "a"}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: "."}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: ".."}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: "a/b"}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: "a\\b"}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: "a\x00b"}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: "\xff"}, syscall.EINVAL},
		{WindowsLookup{ParentID: 1, Name: strings.Repeat("a", 255)}, 0},
		{WindowsLookup{ParentID: 1, Name: strings.Repeat("a", 256)}, syscall.ENAMETOOLONG},
		{WindowsLookup{ParentID: 1, Name: strings.Repeat("😀", 127) + "a"}, 0},
		{WindowsLookup{ParentID: 1, Name: strings.Repeat("😀", 128)}, syscall.ENAMETOOLONG},
	}
	for _, tc := range cases {
		checkWindowsErr(t, tc.lookup.Check(), tc.want)
	}
}

func TestWindowsRenameValidation(t *testing.T) {
	base := WindowsRenameRequest{Source: WindowsLookup{ParentID: 1, Name: "a", ExpectedID: 2}, Destination: WindowsLookup{ParentID: 3, Name: "b"}}
	checkWindowsErr(t, base.Check(), 0)
	r := base
	r.Replace = true
	r.Destination.ExpectedID = 4
	checkWindowsErr(t, r.Check(), 0)
	r.Replace = false
	checkWindowsErr(t, r.Check(), syscall.EINVAL)
	r = base
	r.Source.ExpectedID = 0
	checkWindowsErr(t, r.Check(), syscall.EINVAL)
	r = base
	r.Source = WindowsLookup{}
	checkWindowsErr(t, r.Check(), syscall.EINVAL)
	r = base
	r.Destination = WindowsLookup{}
	checkWindowsErr(t, r.Check(), syscall.EINVAL)
	r = base
	r.Source.Name = "/"
	checkWindowsErr(t, r.Check(), syscall.EINVAL)
	r = base
	r.Destination.Name = "/"
	checkWindowsErr(t, r.Check(), syscall.EINVAL)
}

func TestWindowsAttributeValidation(t *testing.T) {
	checkWindowsErr(t, (WindowsAttrChange{}).Check(), 0)
	for _, mask := range []uint32{0, WindowsDOSNormal, WindowsDOSReadOnly | WindowsDOSArchive} {
		checkWindowsErr(t, (WindowsAttrChange{DOSAttributes: &mask}).Check(), 0)
	}
	for _, mask := range []uint32{1 << 31, WindowsDOSNormal | WindowsDOSReadOnly, WindowsDOSDirectory} {
		checkWindowsErr(t, (WindowsAttrChange{DOSAttributes: &mask}).Check(), syscall.EINVAL)
	}
	mode := fs.ModeDir
	checkWindowsErr(t, (WindowsAttrChange{AttrChange: AttrChange{Mode: &mode}}).Check(), syscall.EINVAL)
}

func TestWindowsNameInformationSeparatesRootLinkedAndDetachedObjects(t *testing.T) {
	for _, name := range []WindowsNameInfo{
		{State: WindowsNameRoot},
		{State: WindowsNameDetached},
		{State: WindowsNameLinked, Path: "file"},
		{State: WindowsNameLinked, Path: "dir/CasePreserved.txt"},
		{State: WindowsNameLinked, Path: "目录/😀"},
	} {
		checkWindowsErr(t, name.Check(), 0)
	}
	for _, name := range []WindowsNameInfo{
		{},
		{State: WindowsNameState(4)},
		{State: WindowsNameRoot, Path: "file"},
		{State: WindowsNameDetached, Path: "former/name"},
		{State: WindowsNameLinked},
		{State: WindowsNameLinked, Path: "/file"},
		{State: WindowsNameLinked, Path: "dir/"},
		{State: WindowsNameLinked, Path: "dir//file"},
		{State: WindowsNameLinked, Path: "dir/../file"},
		{State: WindowsNameLinked, Path: "dir/./file"},
		{State: WindowsNameLinked, Path: "dir\\file"},
		{State: WindowsNameLinked, Path: "dir/\x00"},
		{State: WindowsNameLinked, Path: "dir/\xff"},
		{State: WindowsNameLinked, Path: "dir/" + strings.Repeat("a", 256)},
	} {
		if err := name.Check(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("invalid authoritative name %+v returned %v", name, err)
		}
	}
}

func TestWindowsNameInformationEnforcesItsCompletePathByteBound(t *testing.T) {
	component := strings.Repeat("界", 255) + "/"
	prefix := strings.Repeat(component, 85)
	remaining := WindowsMaxNameInfoBytes - len(prefix)
	path := prefix + strings.Repeat("界", remaining/3) + strings.Repeat("x", remaining%3)
	if len(path) != WindowsMaxNameInfoBytes {
		t.Fatalf("boundary fixture has %d bytes, want %d", len(path), WindowsMaxNameInfoBytes)
	}
	checkWindowsErr(t, (WindowsNameInfo{State: WindowsNameLinked, Path: path}).Check(), 0)
	checkWindowsErr(t, (WindowsNameInfo{State: WindowsNameLinked, Path: path + "x"}).Check(), syscall.EIO)
}

func TestWindowsNameInformationEnforcesItsCompleteUTF16PathBound(t *testing.T) {
	prefix := strings.Repeat(strings.Repeat("a", 255)+"/", 127) + strings.Repeat("b", 100) + "/"
	for _, path := range []string{
		prefix + strings.Repeat("c", 154),
		prefix + "😀" + strings.Repeat("c", 152),
	} {
		if units := len(utf16.Encode([]rune(path))); units != WindowsMaxPathUTF16Units {
			t.Fatalf("boundary fixture has %d UTF-16 units, want %d", units, WindowsMaxPathUTF16Units)
		}
		checkWindowsErr(t, (WindowsNameInfo{State: WindowsNameLinked, Path: path}).Check(), 0)
		checkWindowsErr(t, (WindowsNameInfo{State: WindowsNameLinked, Path: path + "x"}).Check(), syscall.EIO)
	}
}

func TestWindowsLinkTargetValidationPreservesVolumePathSyntax(t *testing.T) {
	for _, target := range []string{"target", "../target", "/dir/target", "/", ".", "目录/😀", strings.Repeat("x", WindowsMaxLinkTargetBytes)} {
		checkWindowsErr(t, CheckWindowsLinkTarget(target), 0)
	}
	for _, target := range []string{"", "//host/share", "\\host\\share", "C:/host/file", "a:b", "a\x00b", "\xff"} {
		if err := CheckWindowsLinkTarget(target); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid link target %q returned %v", target, err)
		}
	}
	checkWindowsErr(t, CheckWindowsLinkTarget(strings.Repeat("x", WindowsMaxLinkTargetBytes+1)), syscall.ENAMETOOLONG)
}

func TestWindowsLockValidationPreservesBatchOrdering(t *testing.T) {
	for _, r := range []WindowsLockRange{{Type: Unlock}, {Type: Shared, Length: 1}, {Type: Exclusive, Offset: math.MaxInt64, FailImmediately: true}} {
		checkWindowsErr(t, r.Check(), 0)
	}
	for _, r := range []WindowsLockRange{{Type: 0}, {Type: LockType(4)}, {Type: Shared, Offset: math.MaxUint64}, {Type: Exclusive, Offset: math.MaxInt64, Length: 1}, {Type: Unlock, FailImmediately: true}} {
		checkWindowsErr(t, r.Check(), syscall.EINVAL)
	}
	checkWindowsErr(t, (WindowsLockBatch{}).Check(), syscall.EINVAL)
	checkWindowsErr(t, (WindowsLockBatch{Ranges: make([]WindowsLockRange, WindowsMaxLockBatch+1)}).Check(), syscall.EINVAL)
	// A malformed later element is left to the ordered executor; earlier unlocks
	// must remain applied when that later element fails.
	batch := WindowsLockBatch{Ranges: []WindowsLockRange{{Type: Unlock, Length: 1}, {Type: 0}}}
	checkWindowsErr(t, batch.Check(), 0)
	checkWindowsErr(t, batch.Ranges[1].Check(), syscall.EINVAL)
}

func TestWindowsIORangeValidation(t *testing.T) {
	for _, r := range [][2]int64{{0, 0}, {0, math.MaxInt64}, {math.MaxInt64, 0}, {3, 4}} {
		checkWindowsErr(t, CheckWindowsRange(r[0], r[1]), 0)
	}
	for _, r := range [][2]int64{{-1, 0}, {0, -1}, {math.MaxInt64, 1}, {1, math.MaxInt64}} {
		checkWindowsErr(t, CheckWindowsRange(r[0], r[1]), syscall.EINVAL)
	}
}

func checkWindowsErr(t *testing.T, err error, want syscall.Errno) {
	t.Helper()
	if want == 0 {
		if err != nil {
			t.Fatal(err)
		}
	} else if !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}
