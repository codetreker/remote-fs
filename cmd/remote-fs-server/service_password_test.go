package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAdministratorPasswordFileRequiresPrivateUnaliasedInput(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "password")
	const password = "a safe password for testing"
	if err := os.WriteFile(path, []byte(password+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	got, err := administratorPassword(context.Background(), strings.NewReader(""), &output, path)
	if err != nil || got != password || output.Len() != 0 {
		t.Fatalf("password read = %q, %v; output %q", got, err, output.String())
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := administratorPassword(context.Background(), nil, io.Discard, link); err == nil {
		t.Fatal("symlink password file was accepted")
	}
	hardlink := filepath.Join(root, "hardlink")
	if err := os.Link(path, hardlink); err != nil {
		t.Fatal(err)
	}
	if _, err := administratorPassword(context.Background(), nil, io.Discard, path); err == nil {
		t.Fatal("aliased password file was accepted")
	}
	if err := os.Remove(hardlink); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := administratorPassword(context.Background(), nil, io.Discard, path); err == nil {
		t.Fatal("public password file was accepted")
	}
	if _, err := administratorPassword(context.Background(), strings.NewReader(password), io.Discard, ""); err == nil {
		t.Fatal("piped password input was accepted without an explicit password file")
	}
}

func TestAdministratorPasswordPreservesContentsAndRejectsExtraInput(t *testing.T) {
	for _, line := range []string{" twelve bytes of password ", "twelve bytes of password\r\n"} {
		got, err := readAdministratorPassword(strings.NewReader(line))
		if err != nil || got != strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r") {
			t.Fatalf("password contents changed: %q, %v", got, err)
		}
	}
	for _, input := range []string{"", "short", "long enough password\nanother line\n", strings.Repeat("x", 1025)} {
		if _, err := readAdministratorPassword(strings.NewReader(input)); err == nil {
			t.Fatal("invalid password input was accepted")
		}
	}
	failure := errors.New("password input unavailable")
	if _, err := readAdministratorPassword(passwordFailureReader{failure}); !errors.Is(err, failure) {
		t.Fatalf("password input error lost: %v", err)
	}
}

type passwordFailureReader struct{ err error }

func (r passwordFailureReader) Read([]byte) (int, error) { return 0, r.err }
