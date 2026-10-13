// Package password reads bounded secrets for interactive commands without
// echoing terminal input. Callers own the terminal and signal context.
package password

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
)

func ReadTTY(ctx context.Context, terminal *os.File, output io.Writer, prompt string, maxBytes int) (string, error) {
	if err := checkMaximum(maxBytes); err != nil {
		return "", err
	}
	if terminal == nil {
		return "", errors.New("password input requires a terminal")
	}
	return readTTY(ctx, terminal, output, prompt, maxBytes)
}

// ReadPrivateFile requires a single-link regular file with owner-only access.
// Permission or ownership validation unavailable on a platform is an error.
func ReadPrivateFile(ctx context.Context, path string, maxBytes int) (string, error) {
	if err := checkMaximum(maxBytes); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return readPrivateFile(path, maxBytes)
}

func ReadLine(reader io.Reader, maxBytes int) (string, error) {
	if err := checkMaximum(maxBytes); err != nil {
		return "", err
	}
	value, err := io.ReadAll(io.LimitReader(reader, int64(maxBytes)+3))
	if err != nil {
		return "", err
	}
	if len(value) > maxBytes+2 {
		return "", errors.New("password input exceeds its byte limit")
	}
	secret := strings.TrimSuffix(strings.TrimSuffix(string(value), "\n"), "\r")
	if len(secret) == 0 || len(secret) > maxBytes || strings.ContainsAny(secret, "\r\n") {
		return "", errors.New("password requires one nonempty line within its byte limit")
	}
	return secret, nil
}

func checkMaximum(maxBytes int) error {
	if maxBytes < 1 || maxBytes > 1024 {
		return errors.New("password byte limit must be between 1 and 1024")
	}
	return nil
}
