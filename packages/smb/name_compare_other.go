//go:build !windows

package smb

import "syscall"

func nameComparisonAvailable() error                    { return syscall.EOPNOTSUPP }
func nativeNameCompare([]uint16, []uint16) (int, error) { return 0, syscall.EOPNOTSUPP }
