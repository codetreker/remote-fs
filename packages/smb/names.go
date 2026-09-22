package smb

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"syscall"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/storage"
)

const maxNameUnits = 255
const maxPathUnits = 32767

var errNameInvalid = errors.New("invalid Windows name")
var errNameAmbiguous = errors.New("directory contains ambiguous Windows names")
var errPathMissing = errors.New("observed intermediate path component is absent")

type smbPath struct {
	components        []string
	directoryRequired bool
}

type nameComparer func([]uint16, []uint16) (int, error)

type projectedName struct {
	units []uint16
	entry int
}

// Tree-relative names retain directory intent; stream selectors and path
// normalization are outside this literal namespace.
func parseSMBPath(name string) (smbPath, error) {
	if !utf8.ValidString(name) || utf16Length(name) > maxPathUnits || strings.HasPrefix(name, `\\`) {
		return smbPath{}, errNameInvalid
	}
	if strings.ContainsRune(name, ':') {
		return smbPath{}, fmt.Errorf("SMB data streams are unsupported: %w", syscall.EOPNOTSUPP)
	}
	if name == "" {
		return smbPath{}, nil
	}
	name = strings.TrimPrefix(name, `\`)
	if name == "" {
		return smbPath{directoryRequired: true}, nil
	}
	directory := strings.HasSuffix(name, `\`)
	if directory {
		name = strings.TrimSuffix(name, `\`)
	}
	parts := strings.Split(name, `\`)
	if len(parts) > storage.MaxNamespaceGuards {
		return smbPath{}, syscall.EFBIG
	}
	for _, part := range parts {
		if err := checkWindowsLeaf(part); err != nil {
			return smbPath{}, err
		}
	}
	return smbPath{components: parts, directoryRequired: directory}, nil
}

func utf16Length(value string) int {
	length := 0
	for _, r := range value {
		length++
		if r > 0xffff {
			length++
		}
	}
	return length
}

// Reserved device stems and trailing-dot/space rules follow Win32 name
// representation. Raw names are never repaired or filtered from an observation.
// https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file
func checkWindowsLeaf(name string) error {
	if name == "" || !utf8.ValidString(name) || len(name) > storage.MaxLeafBytes || utf16Length(name) > maxNameUnits ||
		name == "." || name == ".." || strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
		return errNameInvalid
	}
	for _, r := range name {
		if r < 32 || strings.ContainsRune(`<>:"/\|?*`, r) {
			return errNameInvalid
		}
	}
	stem, _, _ := strings.Cut(name, ".")
	stem = strings.TrimRight(stem, " ")
	for _, reserved := range []string{"CON", "PRN", "AUX", "NUL"} {
		if asciiNameEqual(stem, reserved) {
			return errNameInvalid
		}
	}
	if len(stem) >= 3 && (asciiNameEqual(stem[:3], "COM") || asciiNameEqual(stem[:3], "LPT")) {
		suffix := stem[3:]
		if len(suffix) == 1 && suffix[0] >= '1' && suffix[0] <= '9' || suffix == "¹" || suffix == "²" || suffix == "³" {
			return errNameInvalid
		}
	}
	return nil
}

func asciiNameEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := 0; i < len(left); i++ {
		value := left[i]
		if value >= 'a' && value <= 'z' {
			value -= 'a' - 'A'
		}
		if value != right[i] {
			return false
		}
	}
	return true
}

func nameUnits(name string) []uint16 {
	units := make([]uint16, 0, utf16Length(name))
	for _, r := range name {
		if r <= 0xffff {
			units = append(units, uint16(r))
		} else {
			high, low := utf16.EncodeRune(r)
			units = append(units, uint16(high), uint16(low))
		}
	}
	return units
}

// Every name is checked before selecting a child; unrelated invalid names or
// ordinal aliases invalidate the complete observation.
func projectDirectory(entries []storage.Entry, compare nameComparer) ([]projectedName, error) {
	if len(entries) > storage.MaxDirectoryEntries {
		return nil, syscall.EFBIG
	}
	projected := make([]projectedName, 0, len(entries))
	identities := make(map[uint64]struct{}, len(entries))
	for index, entry := range entries {
		if err := checkWindowsLeaf(entry.Name); err != nil {
			return nil, err
		}
		if entry.Attr.ID == 0 || entry.Attr.Kind.Check() != nil || entry.Attr.Kind != storage.NodeDirectory && entry.Attr.Size < 0 {
			return nil, syscall.EIO
		}
		if _, duplicate := identities[entry.Attr.ID]; duplicate {
			return nil, syscall.EIO
		}
		identities[entry.Attr.ID] = struct{}{}
		projected = append(projected, projectedName{units: nameUnits(entry.Name), entry: index})
	}
	var failure error
	sort.Slice(projected, func(i, j int) bool {
		if failure != nil {
			return false
		}
		order, err := compare(projected[i].units, projected[j].units)
		if err != nil {
			failure = err
			return false
		}
		return order < 0
	})
	if failure != nil {
		return nil, failure
	}
	for index := 1; index < len(projected); index++ {
		order, err := compare(projected[index-1].units, projected[index].units)
		if err != nil {
			return nil, err
		}
		if order == 0 {
			return nil, errNameAmbiguous
		}
	}
	return projected, nil
}

func selectName(projected []projectedName, wanted []uint16, compare nameComparer) (int, bool, error) {
	low, high := 0, len(projected)
	for low < high {
		middle := low + (high-low)/2
		order, err := compare(projected[middle].units, wanted)
		if err != nil {
			return 0, false, err
		}
		if order == 0 {
			return projected[middle].entry, true, nil
		}
		if order < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return 0, false, nil
}
