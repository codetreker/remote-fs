package smb

import (
	"bytes"
	"errors"
	"strings"
	"syscall"
	"testing"
	"unicode/utf16"
)

func TestNameKey(t *testing.T) {
	for _, test := range []struct{ name, key string }{
		{"file.txt", "FILE.TXT"}, {"README", "README"}, {"école", "ÉCOLE"},
		{"中文", "中文"}, {"😀", "😀"}, {"ςσΣ", "ΣΣΣ"}, {"𐐨", "𐐀"},
		{"ß", "ß"}, {"ﬀ", "ﬀ"}, {"e\u0301", "E\u0301"},
		{"COM0", "COM0"}, {"COM10", "COM10"}, {"LPT0", "LPT0"},
		{"COM⁴", "COM⁴"}, {"console", "CONSOLE"}, {"a..b", "A..B"},
		{strings.Repeat("界", 255), strings.Repeat("界", 255)},
		{strings.Repeat("😀", 127) + "x", strings.Repeat("😀", 127) + "X"},
	} {
		t.Run(test.name, func(t *testing.T) {
			key, err := nameKey([]byte(test.name))
			if err != nil || key != test.key {
				t.Fatalf("nameKey(%q) = %q, %v; want %q", test.name, key, err, test.key)
			}
		})
	}
}

func TestNameKeyRejectsInvalidNames(t *testing.T) {
	for _, name := range []string{
		"", ".", "..", "a.", "a ", "a:b", "a/b", `a\b`, "a?b", "a*b",
		"a<b", "a>b", "a|b", `a"b`, "a\x00b", "a\x1fb", "\xff", "\xc0\xaf", "\xed\xa0\x80",
		"con", "PRN", "AUX.txt", "NUL.txt", "COM1", "com9.txt", "LPT9",
		"COM¹", "LPT².log", "com³.txt", "CON .txt", "NUL  .bin", "CONIN$", "CONOUT$.log",
	} {
		t.Run(name, func(t *testing.T) {
			key, err := nameKey([]byte(name))
			if !errors.Is(err, syscall.EINVAL) || key != "" {
				t.Fatalf("nameKey(%q) = %q, %v; want empty key and EINVAL", name, key, err)
			}
		})
	}
	for _, name := range []string{strings.Repeat("x", 256), strings.Repeat("界", 256), strings.Repeat("😀", 128), strings.Repeat("x", 1021)} {
		if key, err := nameKey([]byte(name)); !errors.Is(err, syscall.ENAMETOOLONG) || key != "" {
			t.Fatalf("oversized name = %q, %v; want empty key and ENAMETOOLONG", key, err)
		}
	}
}

func TestValidateWindowsPath(t *testing.T) {
	for _, path := range []string{"", "file", "目录/😀", "CasePreserved/file.txt", strings.Repeat("a/", 255) + "a"} {
		if err := validateWindowsPath(path); err != nil {
			t.Errorf("valid path %q: %v", path, err)
		}
	}
	for _, path := range []string{"/file", "dir/", "dir//file", ".", "..", "dir/../file", "dir/./file", `dir\file`, `C:\file`, `\\host\share`, "dir/\xff", "dir/CON", "dir/file. "} {
		if err := validateWindowsPath(path); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("invalid path %q: %v; want EINVAL", path, err)
		}
	}
	for _, path := range []string{strings.Repeat("a/", 256) + "a", "dir/" + strings.Repeat("a", 256)} {
		if err := validateWindowsPath(path); !errors.Is(err, syscall.ENAMETOOLONG) {
			t.Errorf("oversized path: %v; want ENAMETOOLONG", err)
		}
	}
}

func TestValidateWindowsPathUTF16Boundary(t *testing.T) {
	prefix := strings.Repeat(strings.Repeat("a", 255)+"/", 127) + strings.Repeat("b", 100) + "/"
	for _, path := range []string{prefix + strings.Repeat("c", 154), prefix + "😀" + strings.Repeat("c", 152)} {
		if units := len(utf16.Encode([]rune(path))); units != 32767 {
			t.Fatalf("fixture has %d UTF-16 units", units)
		}
		if err := validateWindowsPath(path); err != nil {
			t.Fatalf("boundary path: %v", err)
		}
		if err := validateWindowsPath(path + "x"); !errors.Is(err, syscall.ENAMETOOLONG) {
			t.Fatalf("oversized UTF-16 path: %v; want ENAMETOOLONG", err)
		}
	}
}

func TestValidateWindowsPathByteBoundary(t *testing.T) {
	prefix := strings.Repeat(strings.Repeat("界", 255)+"/", 85)
	remaining := (65536 + 256) - len(prefix)
	path := prefix + strings.Repeat("界", remaining/3) + strings.Repeat("x", remaining%3)
	if len(path) != 65536+256 {
		t.Fatalf("fixture has %d bytes", len(path))
	}
	if err := validateWindowsPath(path); err != nil {
		t.Fatalf("boundary path: %v", err)
	}
	if err := validateWindowsPath(path + "x"); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("oversized UTF-8 path: %v; want ENAMETOOLONG", err)
	}
}

func TestDirectoryNameIndexPreservesExactEntries(t *testing.T) {
	entries := []localNameEntry{
		{Name: []byte("ReadMe"), ID: 10},
		{Name: []byte("é"), ID: 20},
		{Name: []byte("e\u0301"), ID: 30},
		{Name: []byte("😀"), ID: 40},
		{Name: []byte("ß"), ID: 50},
		{Name: []byte("SS"), ID: 60},
	}
	index, err := directoryNameIndex(entries, 4096)
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{"README", "É", "E\u0301", "😀", "ß", "SS"} {
		if found, ok := index[key]; !ok || found != i {
			t.Errorf("key %q = %d, %v; want %d", key, found, ok, i)
		}
	}
	if string(entries[0].Name) != "ReadMe" || entries[0].ID != 10 {
		t.Fatalf("validation changed the authoritative entry: %+v", entries[0])
	}
	copy(entries[0].Name, "mutate")
	if _, ok := index["README"]; !ok {
		t.Fatal("index retained mutable name bytes")
	}
	if index, err := directoryNameIndex(nil, 1); err != nil || index == nil || len(index) != 0 {
		t.Fatalf("empty complete directory: %v, %v", index, err)
	}
}

func TestDirectoryNameIndexRejectsWholeObservation(t *testing.T) {
	for _, test := range []struct {
		name    string
		entries []localNameEntry
		want    error
	}{
		{"case collision", []localNameEntry{{Name: []byte("file"), ID: 1}, {Name: []byte("FILE"), ID: 2}}, syscall.EEXIST},
		{"same identity collision", []localNameEntry{{Name: []byte("file"), ID: 1}, {Name: []byte("FILE"), ID: 1}}, syscall.EEXIST},
		{"exact duplicate", []localNameEntry{{Name: []byte("file")}, {Name: []byte("file")}}, syscall.EEXIST},
		{"Unicode collision", []localNameEntry{{Name: []byte("Σ")}, {Name: []byte("ς")}}, syscall.EEXIST},
		{"supplementary collision", []localNameEntry{{Name: []byte("𐐨")}, {Name: []byte("𐐀")}}, syscall.EEXIST},
		{"unrelated invalid sibling", []localNameEntry{{Name: []byte("target")}, {Name: []byte("CON")}}, syscall.EINVAL},
		{"unrelated malformed sibling", []localNameEntry{{Name: []byte("target")}, {Name: []byte{0xff}}}, syscall.EINVAL},
		{"unrelated oversized sibling", []localNameEntry{{Name: []byte("target")}, {Name: bytes.Repeat([]byte("x"), 256)}}, syscall.ENAMETOOLONG},
		{"oversized source bytes", []localNameEntry{{Name: bytes.Repeat([]byte("x"), 1021)}}, syscall.ENAMETOOLONG},
	} {
		t.Run(test.name, func(t *testing.T) {
			index, err := directoryNameIndex(test.entries, 8192)
			if index != nil || !errors.Is(err, test.want) {
				t.Fatalf("index = %v, %v; want nil and %v", index, err, test.want)
			}
		})
	}
}

func TestDirectoryNameIndexBudget(t *testing.T) {
	entries := []localNameEntry{{Name: []byte("one")}, {Name: []byte("two")}}
	for _, budget := range []int64{-1, 0} {
		if index, err := directoryNameIndex(entries, budget); index != nil || !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("budget %d: %v, %v; want nil and EINVAL", budget, index, err)
		}
	}
	const exact = 2 * (256 + 4*3)
	if index, err := directoryNameIndex(entries, exact-1); index != nil || !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("insufficient budget: %v, %v; want nil and EFBIG", index, err)
	}
	if index, err := directoryNameIndex(entries, exact); err != nil || len(index) != 2 {
		t.Fatalf("exact budget: %v, %v", index, err)
	}
}
