//go:build windows

package smb

import (
	"fmt"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

var ordinalCompare = windows.NewLazySystemDLL("kernel32.dll").NewProc("CompareStringOrdinal")

func nameComparisonAvailable() error { return ordinalCompare.Find() }

// Pointer arguments escape before DLL lookup or stack growth can move them.
//
//go:uintptrescapes
func callOrdinal(arguments ...uintptr) (uintptr, error) {
	if err := ordinalCompare.Find(); err != nil {
		return 0, err
	}
	result, _, err := ordinalCompare.Call(arguments...)
	if result == 0 {
		if err == syscall.Errno(0) {
			err = syscall.EIO
		}
		return 0, fmt.Errorf("Windows ordinal name comparison: %w", err)
	}
	return result, nil
}

// Explicit lengths preserve literal UTF-16. The OS uppercase table owns casing;
// normalization and Unicode case folding do not define this comparison.
// https://learn.microsoft.com/en-us/windows/win32/api/stringapiset/nf-stringapiset-comparestringordinal
func nativeNameCompare(left, right []uint16) (int, error) {
	if len(left) == 0 || len(right) == 0 || len(left) > maxNameUnits || len(right) > maxNameUnits {
		return 0, syscall.EINVAL
	}
	result, err := callOrdinal(uintptr(unsafe.Pointer(&left[0])), uintptr(len(left)), uintptr(unsafe.Pointer(&right[0])), uintptr(len(right)), 1)
	runtime.KeepAlive(left)
	runtime.KeepAlive(right)
	if err != nil {
		return 0, err
	}
	if result < 1 || result > 3 {
		return 0, syscall.EIO
	}
	return int(result) - 2, nil
}
