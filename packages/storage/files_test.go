package storage_test

import (
	"encoding/json"
	"errors"
	"io/fs"
	"math"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileOpenOptionsJSONPreservesSharedAccessFields(t *testing.T) {
	options := storage.FileOpenOptions{
		OpenAccess: storage.OpenAccess{Read: true, Write: true, Create: true, Truncate: true, Exclusive: true},
		ExpectedID: 42, Mode: 0o600,
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"Read": true, "Write": true, "Create": true, "Truncate": true, "Exclusive": true,
		"ExpectedID": float64(42), "Mode": float64(0o600)}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("file open JSON changed field names or shape: %s", encoded)
	}
	var decoded storage.FileOpenOptions
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != options {
		t.Fatalf("shared open access changed on round trip: %+v", decoded)
	}
}

func TestFileOpenOptionsRequireExplicitAccessAndValidCreation(t *testing.T) {
	for _, options := range []storage.FileOpenOptions{
		{},
		{OpenAccess: storage.OpenAccess{Read: true, Exclusive: true}},
		{OpenAccess: storage.OpenAccess{Read: true, Truncate: true}},
		{OpenAccess: storage.OpenAccess{Write: true}, Mode: fs.ModeDir},
		{OpenAccess: storage.OpenAccess{Read: true, Create: true}, Mode: fs.ModeSymlink},
	} {
		if err := options.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("options %+v returned %v, want EINVAL", options, err)
		}
	}
	for _, options := range []storage.FileOpenOptions{
		{OpenAccess: storage.OpenAccess{Read: true}},
		{OpenAccess: storage.OpenAccess{Write: true}},
		{OpenAccess: storage.OpenAccess{Read: true, Write: true}},
		{OpenAccess: storage.OpenAccess{Read: true, Create: true, Exclusive: true}, Mode: 0600},
		{OpenAccess: storage.OpenAccess{Write: true, Create: true, Truncate: true}, Mode: storage.SettableMode},
		{OpenAccess: storage.OpenAccess{Read: true}, ExpectedID: 42},
	} {
		if err := options.Check(); err != nil {
			t.Errorf("options %+v returned %v", options, err)
		}
	}
}

func TestFileNodeOpenCannotCreateOrChangeItsRequestedIdentity(t *testing.T) {
	for _, test := range []struct {
		id      uint64
		options storage.FileOpenOptions
	}{
		{42, storage.FileOpenOptions{}},
		{0, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}}},
		{42, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}, ExpectedID: 43}},
		{42, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Create: true}}},
		{42, storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true, Create: true, Exclusive: true}}},
	} {
		if err := test.options.CheckNode(test.id); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("node %d options %+v returned %v, want EINVAL", test.id, test.options, err)
		}
	}
	for _, options := range []storage.FileOpenOptions{
		{OpenAccess: storage.OpenAccess{Read: true}},
		{OpenAccess: storage.OpenAccess{Write: true, Truncate: true}, ExpectedID: 42},
	} {
		if err := options.CheckNode(42); err != nil {
			t.Errorf("node 42 options %+v returned %v", options, err)
		}
	}
}

func TestFileSessionLimitsAreExplicitAndValidatedBeforeAdmission(t *testing.T) {
	if err := (storage.FileSessionOptions{}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("zero session limits returned %v, want EINVAL", err)
	}
	defaults := storage.DefaultFileSessionOptions()
	if err := defaults.Check(); err != nil {
		t.Fatalf("default session limits: %v", err)
	}
	changes := map[string]func(*storage.FileSessionOptions){
		"lease zero":         func(o *storage.FileSessionOptions) { o.Lease = 0 },
		"lease negative":     func(o *storage.FileSessionOptions) { o.Lease = -time.Second },
		"history zero":       func(o *storage.FileSessionOptions) { o.History = 0 },
		"history negative":   func(o *storage.FileSessionOptions) { o.History = -time.Second },
		"file size":          func(o *storage.FileSessionOptions) { o.MaxFileSize = 0 },
		"negative file size": func(o *storage.FileSessionOptions) { o.MaxFileSize = -1 },
		"files":              func(o *storage.FileSessionOptions) { o.MaxFiles = 0 },
		"operations":         func(o *storage.FileSessionOptions) { o.MaxOperations = 0 },
		"waiters":            func(o *storage.FileSessionOptions) { o.MaxWaiters = -1 },
		"lock owners":        func(o *storage.FileSessionOptions) { o.MaxLockOwners = 0 },
		"lock ranges":        func(o *storage.FileSessionOptions) { o.MaxLockRanges = 0 },
		"pending locks":      func(o *storage.FileSessionOptions) { o.MaxPendingLocks = 0 },
		"lock action count":  func(o *storage.FileSessionOptions) { o.MaxLockActions = 0 },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			options := defaults
			change(&options)
			if err := options.Check(); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("limits %+v returned %v, want EINVAL", options, err)
			}
		})
	}
	defaults.MaxWaiters = 0
	if err := defaults.Check(); err != nil {
		t.Fatalf("zero waiter queue must disable waiting: %v", err)
	}
}

func TestAdvisoryLockValidationPreservesInclusiveRangeBoundaries(t *testing.T) {
	for _, lock := range []storage.FileLock{
		{Family: storage.POSIX, Type: storage.Shared, Start: 0, End: 0},
		{Family: storage.POSIX, Type: storage.Exclusive, Start: math.MaxInt64, End: math.MaxInt64, Wait: true},
		{Family: storage.POSIX, Type: storage.Unlock, Start: 17, End: 21},
		{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64},
		{Family: storage.Flock, Type: storage.Shared, End: math.MaxInt64, Wait: true},
		{Family: storage.Flock, Type: storage.Unlock, End: math.MaxInt64},
	} {
		if err := lock.Check(); err != nil {
			t.Errorf("lock %+v returned %v", lock, err)
		}
	}
	for _, lock := range []storage.FileLock{
		{},
		{Family: storage.LockFamily(255), Type: storage.Shared},
		{Family: storage.POSIX, Type: storage.LockType(255)},
		{Family: storage.POSIX, Type: storage.Shared, Start: 2, End: 1},
		{Family: storage.POSIX, Type: storage.Shared, End: uint64(math.MaxInt64) + 1},
		{Family: storage.Flock, Type: storage.Exclusive, Start: 1, End: math.MaxInt64},
		{Family: storage.Flock, Type: storage.Exclusive, End: math.MaxInt64 - 1},
		{Family: storage.POSIX, Type: storage.Unlock, Wait: true},
	} {
		if err := lock.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("lock %+v returned %v, want EINVAL", lock, err)
		}
	}
}

func TestLockRequestIdentityIncludesItsServerEpochAndRandomNonce(t *testing.T) {
	if id, err := storage.NewLockRequestID(0); id != "" || !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("zero action epoch produced %q, %v", id, err)
	}
	seen := make(map[storage.LockRequestID]bool)
	for _, want := range []uint64{1, 42, math.MaxUint64, 42} {
		id, err := storage.NewLockRequestID(want)
		if err != nil {
			t.Fatal(err)
		}
		epoch, err := id.Epoch()
		if err != nil || epoch != want {
			t.Fatalf("identity %q has epoch %d, %v; want %d", id, epoch, err, want)
		}
		if seen[id] {
			t.Fatalf("distinct actions reused request identity %q", id)
		}
		seen[id] = true
	}
}

func TestLockRequestIdentityRejectsUnboundedAndNoncanonicalInputs(t *testing.T) {
	nonce := strings.Repeat("a", 32)
	for _, id := range []string{
		"", "1", ":" + nonce, "0:" + nonce, "01:" + nonce,
		"+1:" + nonce, "-1:" + nonce, "x:" + nonce,
		"18446744073709551616:" + nonce,
		"123456789012345678901:" + nonce,
		"1:" + nonce[:31], "1:" + nonce + "a",
		"1:" + strings.Repeat("A", 32),
		"1:" + strings.Repeat("g", 32),
		strings.Repeat("a", 1000),
	} {
		if epoch, err := storage.LockRequestID(id).Epoch(); epoch != 0 || !errors.Is(err, syscall.EINVAL) {
			t.Errorf("invalid identity %q returned %d, %v", id, epoch, err)
		}
	}
}
