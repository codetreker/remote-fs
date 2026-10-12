//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package password

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPrivatePasswordFileRequiresSingleOwnerOnlyRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte(" secret password \n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := ReadPrivateFile(t.Context(), path, 1024)
	if err != nil || got != " secret password " {
		t.Fatalf("private password changed: %q %v", got, err)
	}
	if _, err := ReadPrivateFile(t.Context(), path, 0); err == nil {
		t.Fatal("invalid maximum accepted")
	}
	if _, err := ReadPrivateFile(t.Context(), path+".missing", 1024); err == nil {
		t.Fatal("missing password reported success")
	}
	link := path + ".symlink"
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPrivateFile(t.Context(), link, 1024); err == nil {
		t.Fatal("password symlink accepted")
	}
	hardlink := path + ".hardlink"
	if err := os.Link(path, hardlink); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPrivateFile(t.Context(), path, 1024); err == nil {
		t.Fatal("multiply linked password accepted")
	}
	if err := os.Remove(hardlink); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadPrivateFile(t.Context(), path, 1024); err == nil {
		t.Fatal("group-readable password accepted")
	}
	if _, err := ReadPrivateFile(t.Context(), filepath.Dir(path), 1024); err == nil {
		t.Fatal("directory accepted as password")
	}
}
