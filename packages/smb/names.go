package smb

import (
	"fmt"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

const (
	windowsNameUnicodeVersion = "15.0.0"
	windowsMaxNameUTF16Units  = 255
	windowsMaxPathUTF16Units  = 32767
	windowsMaxPathBytes       = (64 << 10) + 256
	windowsMaxPathComponents  = 256
)

// Comparison uses Unicode 15.0 simple uppercase without normalization or
// multi-rune case expansion. A different Unicode table requires an explicit
// adapter policy change so two client versions cannot silently disagree.
func nameKey(name []byte) (string, error) {
	if unicode.Version != windowsNameUnicodeVersion {
		return "", syscall.ENOSYS
	}
	if len(name) > 4*windowsMaxNameUTF16Units {
		return "", syscall.ENAMETOOLONG
	}
	if len(name) == 0 || !utf8.Valid(name) {
		return "", syscall.EINVAL
	}
	text := string(name)
	if strings.HasSuffix(text, ".") || strings.HasSuffix(text, " ") {
		return "", syscall.EINVAL
	}
	units := 0
	for _, r := range text {
		if r < 32 || strings.ContainsRune(`<>:"/\|?*`, r) {
			return "", syscall.EINVAL
		}
		units++
		if r > 0xffff {
			units++
		}
		if units > windowsMaxNameUTF16Units {
			return "", syscall.ENAMETOOLONG
		}
	}
	key := strings.ToUpper(text)
	base, _, _ := strings.Cut(key, ".")
	// Win32 recognizes device names even with spaces before the extension.
	base = strings.TrimRight(base, " ")
	switch base {
	case "CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$":
		return "", syscall.EINVAL
	}
	if len(base) > 3 && (strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT")) {
		suffix := base[3:]
		if len(suffix) == 1 && suffix[0] >= '1' && suffix[0] <= '9' || suffix == "¹" || suffix == "²" || suffix == "³" {
			return "", syscall.EINVAL
		}
	}
	return key, nil
}

// Empty denotes the volume root. Nonempty paths must already be canonical and
// volume-relative; cleaning them would hide a traversal or malformed name.
func validateWindowsPath(path string) error {
	if len(path) > windowsMaxPathBytes {
		return syscall.ENAMETOOLONG
	}
	if path == "" {
		return nil
	}
	units, components := 0, 0
	for component := range strings.SplitSeq(path, "/") {
		components++
		if components > windowsMaxPathComponents {
			return syscall.ENAMETOOLONG
		}
		if _, err := nameKey([]byte(component)); err != nil {
			return fmt.Errorf("Windows path component %d: %w", components, err)
		}
		if components > 1 {
			units++
		}
		for _, r := range component {
			units++
			if r > 0xffff {
				units++
			}
		}
		if units > windowsMaxPathUTF16Units {
			return syscall.ENAMETOOLONG
		}
	}
	return nil
}

type localNameEntry struct {
	Name []byte
	ID   uint64
}

// The caller supplies a complete directory observation and owns its revision
// validation. Every sibling is checked before an index is returned; invalid
// names and collisions invalidate the whole observation. Keys own their bytes.
func directoryNameIndex(entries []localNameEntry, maxBytes int64) (map[string]int, error) {
	if maxBytes <= 0 {
		return nil, syscall.EINVAL
	}
	remaining := maxBytes
	for _, entry := range entries {
		if len(entry.Name) > 4*windowsMaxNameUTF16Units {
			return nil, syscall.ENAMETOOLONG
		}
		// Bound retained entries, original names, and uppercase key expansion
		// before allocating the index.
		cost := int64(256) + 4*int64(len(entry.Name))
		if cost > remaining {
			return nil, syscall.EFBIG
		}
		remaining -= cost
	}
	index := make(map[string]int, len(entries))
	for i, entry := range entries {
		key, err := nameKey(entry.Name)
		if err != nil {
			return nil, fmt.Errorf("Windows directory entry %d: %w", i, err)
		}
		if _, exists := index[key]; exists {
			return nil, fmt.Errorf("Windows directory entries have colliding names: %w", syscall.EEXIST)
		}
		index[key] = i
	}
	return index, nil
}
