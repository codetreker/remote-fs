package storage_test

import (
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

// The six errnos the contract names by hand must be in the vocabulary, or an
// implementation cannot report what the contract obliges it to report.
func TestContractErrnosAreInTheVocabulary(t *testing.T) {
	for _, e := range []syscall.Errno{
		syscall.EINVAL, syscall.ENOENT, syscall.EEXIST,
		syscall.ENOTDIR, syscall.EISDIR, syscall.ENOTEMPTY,
		syscall.EIO,
	} {
		name, ok := storage.ErrnoName(e)
		if !ok {
			t.Fatalf("errno %v has no name in the vocabulary", e)
		}
		back, ok := storage.ErrnoByName(name)
		if !ok || back != e {
			t.Fatalf("name %q maps back to %v (found=%v), want %v", name, back, ok, e)
		}
	}
}

func TestErrnoNamesAreUnique(t *testing.T) {
	seen := map[string]syscall.Errno{}
	for _, e := range storage.Errnos() {
		name, ok := storage.ErrnoName(e)
		if !ok {
			t.Fatalf("Errnos lists %v but ErrnoName does not know it", e)
		}
		if first, clash := seen[name]; clash {
			t.Fatalf("name %q is shared by %v and %v", name, first, e)
		}
		seen[name] = e
	}
}

// A name outside the vocabulary means two parties disagree about what an error is. That
// is the "cannot determine the outcome" case, not a lookup to guess at.
func TestErrnoByNameRejectsWhatItDoesNotKnow(t *testing.T) {
	for _, name := range []string{"", "ENOTANERRNO", "enoent", "2", "EIO "} {
		if e, ok := storage.ErrnoByName(name); ok {
			t.Fatalf("name %q resolved to %v, want no match", name, e)
		}
	}
}

func TestErrnoNameRejectsWhatItDoesNotKnow(t *testing.T) {
	for _, e := range []syscall.Errno{0, syscall.Errno(4095), syscall.ECONNREFUSED} {
		if name, ok := storage.ErrnoName(e); ok {
			t.Fatalf("errno %v resolved to name %q, want no match", e, name)
		}
	}
}
