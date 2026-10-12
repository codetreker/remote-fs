package smb

import (
	"slices"
	"syscall"
	"unicode/utf16"
)

type directoryPattern struct {
	units []uint16
	qmEnd [maxNameUnits + 1]uint16
	all   bool
}

func compileDirectoryPattern(pattern []uint16) (directoryPattern, error) {
	if len(pattern) == 0 {
		pattern = []uint16{'*'}
	}
	if len(pattern) > maxNameUnits {
		return directoryPattern{}, errNameInvalid
	}
	wild := false
	for i := 0; i < len(pattern); i++ {
		unit := pattern[i]
		if unit < 32 || unit == ':' || unit == '/' || unit == '\\' || unit == '|' {
			return directoryPattern{}, errNameInvalid
		}
		if unit >= 0xd800 && unit <= 0xdbff {
			if i+1 == len(pattern) || pattern[i+1] < 0xdc00 || pattern[i+1] > 0xdfff {
				return directoryPattern{}, errNameInvalid
			}
			i++
		} else if unit >= 0xdc00 && unit <= 0xdfff {
			return directoryPattern{}, errNameInvalid
		}
		if unit == '*' || unit == '?' || unit == '<' || unit == '>' || unit == '"' {
			wild = true
		}
	}
	if !wild {
		literal := string(utf16.Decode(pattern))
		if literal != "." && literal != ".." {
			if err := checkWindowsLeaf(literal); err != nil {
				return directoryPattern{}, err
			}
		}
	}
	units := slices.Clone(pattern)
	for i := range units {
		units[i] = uppercaseNameUnit(units[i])
	}
	compiled := directoryPattern{units: units, all: slices.Equal(units, []uint16{'*'}) || slices.Equal(units, []uint16{'*', '.', '*'})}
	end := len(units)
	for i := len(units) - 1; i >= 0; i-- {
		if units[i] != '>' {
			end = i
		} else {
			compiled.qmEnd[i] = uint16(end)
		}
	}
	return compiled, nil
}

// The Windows matching automaton consumes UTF-16 units. DOS_STAR takes only
// an epsilon transition at the last dot; DOS_QM skips its run at a dot or EOF.
// https://github.com/dotnet/runtime/blob/e6482f46d52e4ef6686071129d4cf80aa56f4ef2/src/libraries/System.Private.CoreLib/src/System/IO/Enumeration/FileSystemName.cs#L217
func (pattern directoryPattern) match(name []uint16, remaining *uint64) (bool, error) {
	if remaining == nil {
		return false, syscall.EINVAL
	}
	if len(pattern.units) == 0 || len(pattern.units) > maxNameUnits || len(name) > maxNameUnits {
		return false, errNameInvalid
	}
	if pattern.all {
		if *remaining == 0 {
			return false, syscall.ENOMEM
		}
		*remaining -= 1
		return len(name) != 0, nil
	}
	lastDot := -1
	for i, unit := range name {
		if unit == '.' {
			lastDot = i
		}
	}
	var current, next [maxNameUnits + 1]bool
	current[0] = true
	for nameIndex := 0; nameIndex <= len(name); nameIndex++ {
		clear(next[:])
		active := false
		for position := 0; position <= len(pattern.units); position++ {
			if !current[position] {
				continue
			}
			if *remaining == 0 {
				return false, syscall.ENOMEM
			}
			*remaining -= 1
			if position == len(pattern.units) {
				if nameIndex == len(name) {
					return true, nil
				}
				continue
			}
			unit := pattern.units[position]
			atEnd := nameIndex == len(name)
			switch unit {
			case '*', '<':
				current[position+1] = true
				if !atEnd && (unit == '*' || nameIndex != lastDot) {
					next[position] = true
					active = true
				}
			case '>':
				if atEnd || name[nameIndex] == '.' {
					current[pattern.qmEnd[position]] = true
				} else {
					next[position+1] = true
					active = true
				}
			case '"':
				if atEnd {
					current[position+1] = true
				} else if name[nameIndex] == '.' {
					next[position+1] = true
					active = true
				}
			default:
				if !atEnd && (unit == '?' || unit == uppercaseNameUnit(name[nameIndex])) {
					next[position+1] = true
					active = true
				}
			}
		}
		if !active {
			return false, nil
		}
		current = next
	}
	return false, nil
}
