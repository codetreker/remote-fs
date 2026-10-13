package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/codetreker/remote-fs/cmd/internal/password"
)

func administratorPassword(ctx context.Context, input io.Reader, output io.Writer, path string) (string, error) {
	var secret string
	var err error
	if path != "" {
		secret, err = password.ReadPrivateFile(ctx, path, 1024)
	} else {
		terminal, ok := input.(*os.File)
		if !ok {
			return "", errors.New("password input requires a terminal or --password-file")
		}
		secret, err = password.ReadTTY(ctx, terminal, output, "Administrator password: ", 1024)
	}
	if err != nil {
		return "", err
	}
	if len(secret) < 12 {
		return "", errors.New("administrator password requires 12 to 1024 bytes")
	}
	return secret, nil
}

func readAdministratorPassword(reader io.Reader) (string, error) {
	secret, err := password.ReadLine(reader, 1024)
	if err != nil {
		return "", err
	}
	if len(secret) < 12 {
		return "", errors.New("administrator password requires 12 to 1024 bytes")
	}
	return secret, nil
}
