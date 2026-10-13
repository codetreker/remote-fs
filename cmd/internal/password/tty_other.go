//go:build !linux

package password

import (
	"context"
	"errors"
	"io"
	"os"
)

func readTTY(context.Context, *os.File, io.Writer, string, int) (string, error) {
	return "", errors.New("secure cancellable terminal password input is unsupported on this platform")
}
