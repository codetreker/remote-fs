//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package password

import "errors"

func readPrivateFile(string, int) (string, error) {
	return "", errors.New("secure password file ownership validation is unsupported on this platform")
}
