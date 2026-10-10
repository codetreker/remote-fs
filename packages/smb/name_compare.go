package smb

import "syscall"

func uppercaseNameUnit(unit uint16) uint16 {
	if upper := windowsUppercase[unit]; upper != 0 {
		return upper
	}
	return unit
}

// MS-UCODEREF 3.1.5.5.1 compares mapped UTF-16 units, including surrogate units.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-ucoderef/ba172a49-1971-4448-855e-448f8804127e
func portableNameCompare(left, right []uint16) (int, error) {
	if len(left) == 0 || len(right) == 0 || len(left) > maxNameUnits || len(right) > maxNameUnits {
		return 0, syscall.EINVAL
	}
	for i := 0; i < min(len(left), len(right)); i++ {
		a, b := uppercaseNameUnit(left[i]), uppercaseNameUnit(right[i])
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
