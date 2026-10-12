package password

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadLinePreservesSecretAndEnforcesBound(t *testing.T) {
	for _, input := range []string{" surrounding spaces ", "nonempty password\r\n", strings.Repeat("x", 1024)} {
		secret, err := ReadLine(strings.NewReader(input), 1024)
		if err != nil || secret != strings.TrimSuffix(strings.TrimSuffix(input, "\n"), "\r") {
			t.Fatalf("password contents changed: %v", err)
		}
	}
	for _, input := range []string{"", "\n", "two\nlines", strings.Repeat("x", 1025)} {
		if _, err := ReadLine(strings.NewReader(input), 1024); err == nil {
			t.Fatal("invalid password input was accepted")
		}
	}
	for _, limit := range []int{0, -1, 1025} {
		if _, err := ReadLine(strings.NewReader("secret"), limit); err == nil {
			t.Fatal("invalid password bound was accepted")
		}
	}
	if _, err := ReadTTY(context.Background(), nil, io.Discard, "Password: ", 1024); err == nil {
		t.Fatal("absent terminal was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadPrivateFile(ctx, "not-opened", 1024); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled private file input = %v", err)
	}
	readErr := errors.New("secret input unavailable")
	if _, err := ReadLine(failureReader{readErr}, 1024); !errors.Is(err, readErr) {
		t.Fatalf("read failure chain changed: %v", err)
	}
}

type failureReader struct{ err error }

func (r failureReader) Read([]byte) (int, error) { return 0, r.err }
