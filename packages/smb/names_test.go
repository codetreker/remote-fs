package smb

import (
	"errors"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

// This fixture defines ASCII ordering only; Windows casing is tested natively.
func testNameCompare(left, right []uint16) (int, error) {
	for i := 0; i < min(len(left), len(right)); i++ {
		a, b := left[i], right[i]
		if a >= 'a' && a <= 'z' {
			a -= 'a' - 'A'
		}
		if b >= 'a' && b <= 'z' {
			b -= 'a' - 'A'
		}
		if a < b {
			return -1, nil
		}
		if a > b {
			return 1, nil
		}
	}
	if len(left) < len(right) {
		return -1, nil
	}
	if len(left) > len(right) {
		return 1, nil
	}
	return 0, nil
}

func TestSMBNamesPreserveLiteralPathAndDirectoryIntent(t *testing.T) {
	for _, test := range []struct {
		input     string
		parts     []string
		directory bool
	}{
		{"", nil, false}, {`\`, nil, true}, {`file`, []string{"file"}, false}, {`\file`, []string{"file"}, false},
		{`Folder\file`, []string{"Folder", "file"}, false}, {`\Folder\`, []string{"Folder"}, true},
		{`%2e%2e\é`, []string{"%2e%2e", "é"}, false},
	} {
		got, err := parseSMBPath(test.input)
		if err != nil || !reflect.DeepEqual(got.components, test.parts) || got.directoryRequired != test.directory {
			t.Fatalf("parse %q: %+v %v", test.input, got, err)
		}
	}
	for _, input := range []string{`\\host\share`, `a\\b`, `a\\`, `a/b`, `.`, `a\..\b`, `a\.\b`, "x\x00y", string([]byte{0xff})} {
		if _, err := parseSMBPath(input); !errors.Is(err, errNameInvalid) {
			t.Fatalf("invalid path %q accepted: %v", input, err)
		}
	}
	for _, input := range []string{`file::$DATA`, `file:stream`, `C:\file`} {
		if _, err := parseSMBPath(input); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("stream/drive syntax %q: %v", input, err)
		}
	}
	if _, err := parseSMBPath(strings.Repeat("x", maxPathUnits+1)); !errors.Is(err, errNameInvalid) {
		t.Fatalf("path bound: %v", err)
	}
	if _, err := parseSMBPath(strings.Repeat(`x\`, storage.MaxNamespaceGuards) + "x"); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("component bound: %v", err)
	}
}

func TestSMBNamesEnforceUTF16AndLiteralLeafEligibility(t *testing.T) {
	for _, name := range []string{"file", "COM0", "LPT10", "CONSOLE", "résumé", "é", strings.Repeat("x", 255), strings.Repeat("😀", 127) + "x"} {
		if err := checkWindowsLeaf(name); err != nil {
			t.Fatalf("eligible leaf %q: %v", name, err)
		}
		decoded, err := wire.DecodeUTF16(wire.EncodeUTF16(name))
		if err != nil || decoded != name {
			t.Fatalf("literal UTF16 changed: %q %v", decoded, err)
		}
	}
	for _, name := range []string{"", ".", "..", "con", "NuL.txt", "CON .txt", "com1", "COM¹", "lpt².txt", "LPT³", "trail.", "trail ", "a?b", "a*b", "a:b", "a\\b", "a/b", "a|b", "a<b", "a>b", "a\"b", "a\t", string([]byte{0xff}), strings.Repeat("x", 256), strings.Repeat("😀", 128)} {
		if err := checkWindowsLeaf(name); !errors.Is(err, errNameInvalid) {
			t.Fatalf("ineligible leaf %q accepted: %v", name, err)
		}
	}
	for _, invalid := range [][]byte{{1}, {0, 0}, {0, 0xd8}, {0, 0xdc}, {0, 0xd8, 'a', 0}} {
		if _, err := wire.DecodeUTF16(invalid); err == nil {
			t.Fatalf("invalid UTF16 accepted: %v", invalid)
		}
	}
	if got := nameUnits("a😀"); !reflect.DeepEqual(got, []uint16{'a', 0xd83d, 0xde00}) {
		t.Fatalf("UTF16 projection: %v", got)
	}
}

func TestSMBNamesValidateWholeDirectoryBeforeSelection(t *testing.T) {
	entry := func(name string, id uint64) storage.Entry {
		return storage.Entry{Name: name, Attr: storage.Attr{ID: id, Kind: storage.NodeRegular}}
	}
	for _, test := range []struct {
		name    string
		entries []storage.Entry
		want    error
	}{
		{"unrelated invalid", []storage.Entry{entry("wanted", 2), entry("bad.", 3)}, errNameInvalid},
		{"unrelated collision", []storage.Entry{entry("wanted", 2), entry("Other", 3), entry("OTHER", 4)}, errNameAmbiguous},
		{"duplicate identity", []storage.Entry{entry("a", 2), entry("b", 2)}, syscall.EIO},
		{"zero identity", []storage.Entry{entry("a", 0)}, syscall.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := projectDirectory(test.entries, testNameCompare); !errors.Is(err, test.want) {
				t.Fatalf("partial directory accepted: %v", err)
			}
		})
	}
	entries := []storage.Entry{entry("z", 2), entry("Actual", 3), entry("B", 4)}
	projection, err := projectDirectory(entries, testNameCompare)
	if err != nil {
		t.Fatal(err)
	}
	index, present, err := selectName(projection, nameUnits("ACTUAL"), testNameCompare)
	if err != nil || !present || entries[index].Name != "Actual" {
		t.Fatalf("selection replaced raw spelling: %d %v %v", index, present, err)
	}
	if _, present, err := selectName(projection, nameUnits("missing"), testNameCompare); err != nil || present {
		t.Fatalf("missing selection: %v %v", present, err)
	}
	failure := errors.New("native comparison unavailable")
	broken := func([]uint16, []uint16) (int, error) { return 0, failure }
	if _, err := projectDirectory(entries, broken); !errors.Is(err, failure) {
		t.Fatalf("sort error hidden: %v", err)
	}
	if _, _, err := selectName(projection, nameUnits("a"), broken); !errors.Is(err, failure) {
		t.Fatalf("selection error hidden: %v", err)
	}
}
