package storage_test

import (
	"errors"
	"math"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

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
		"lock owners":        func(o *storage.FileSessionOptions) { o.MaxRangeOwners = 0 },
		"lock ranges":        func(o *storage.FileSessionOptions) { o.MaxRanges = 0 },
		"pending locks":      func(o *storage.FileSessionOptions) { o.MaxPendingActions = 0 },
		"lock action count":  func(o *storage.FileSessionOptions) { o.MaxActions = 0 },
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

func TestFileActionIdentityIncludesItsServerEpochAndRandomNonce(t *testing.T) {
	if id, err := storage.NewFileActionID(0); id != "" || !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("zero action epoch produced %q, %v", id, err)
	}
	seen := make(map[storage.FileActionID]bool)
	for _, want := range []uint64{1, 42, math.MaxUint64, 42} {
		id, err := storage.NewFileActionID(want)
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

func TestFileActionIdentityRejectsUnboundedAndNoncanonicalInputs(t *testing.T) {
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
		if epoch, err := storage.FileActionID(id).Epoch(); epoch != 0 || !errors.Is(err, syscall.EINVAL) {
			t.Errorf("invalid identity %q returned %d, %v", id, epoch, err)
		}
	}
}

func TestFileIOValidationPreservesEmptyOperationsAndOverflowBoundaries(t *testing.T) {
	for _, request := range []storage.FileReadRequest{{}, {Offset: math.MaxInt64}, {Offset: math.MaxInt64 - 1, Length: 1}} {
		if err := request.Check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, request := range []storage.FileReadRequest{{Offset: -1}, {Length: -1}, {Offset: math.MaxInt64, Length: 1}} {
		if err := request.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatal(err)
		}
	}
	for _, request := range []storage.FileWriteRequest{{}, {Offset: math.MaxInt64}, {Offset: math.MaxInt64 - 1, Data: []byte{1}}} {
		if err := request.Check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, request := range []storage.FileWriteRequest{{Offset: -1}, {Offset: math.MaxInt64, Data: []byte{1}}} {
		if err := request.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatal(err)
		}
	}
	if err := (storage.FileTruncateRequest{}).Check(); err != nil {
		t.Fatal(err)
	}
	if err := (storage.FileTruncateRequest{Size: -1}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	for _, io := range []storage.FileIO{{}, {Offset: math.MaxInt64}, {Truncate: true, Write: true, Size: math.MaxInt64}} {
		if err := io.Check(); err != nil {
			t.Fatal(err)
		}
	}
	for _, io := range []storage.FileIO{{Offset: -1}, {Length: -1}, {Offset: math.MaxInt64, Length: 1}, {Size: 1}, {Truncate: true}, {Truncate: true, Write: true, Size: -1}, {Truncate: true, Write: true, Offset: 1}, {Truncate: true, Write: true, Length: 1}} {
		if err := io.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid IO %+v: %v", io, err)
		}
	}
}

func TestFileObservationSeparatesIdentityAndRequestedNameFacts(t *testing.T) {
	attr := storage.Attr{ID: 3, Kind: storage.NodeSymlink, Size: 1, MetadataRevision: 1}
	location := actionLocation()
	plain := storage.FileObservation{Attr: attr}
	if err := plain.Check(storage.ObservationOptions{}); err != nil {
		t.Fatal(err)
	}
	full := storage.FileObservation{Attr: attr, Location: &location, LinkTarget: []byte("a")}
	options := storage.ObservationOptions{IncludeLocation: true, IncludeLinkTarget: true}
	if err := full.Check(options); err != nil {
		t.Fatal(err)
	}
	for _, observation := range []storage.FileObservation{plain, {Attr: attr, Location: &location}, {Attr: attr, Location: &storage.EntryLocation{}, LinkTarget: []byte("a")}, {Attr: storage.Attr{}, Location: &location, LinkTarget: []byte("a")}, {Attr: attr, Location: &location, LinkTarget: []byte("wrong length")}} {
		if err := observation.Check(options); storage.ErrnoOf(err) != syscall.EIO {
			t.Fatalf("invalid observation: %v", err)
		}
	}
	if err := full.Check(storage.ObservationOptions{}); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatal("unrequested location accepted")
	}
	if err := (storage.FileObservation{Attr: attr, LinkTarget: []byte("a")}).Check(storage.ObservationOptions{}); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatal("unrequested link accepted")
	}
	wrong := full
	other := location.Clone()
	other.NodeID = 4
	other.Ancestors[0].NodeID = 4
	wrong.Location = &other
	if err := wrong.Check(options); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatal("wrong identity accepted")
	}
	missing := full
	missing.LinkTarget = nil
	if err := missing.Check(options); storage.ErrnoOf(err) != syscall.EIO {
		t.Fatal("missing target accepted")
	}
}

func TestOptionalWriteSizePreconditionRetainsZeroAndRejectsNegativeValues(t *testing.T) {
	for _, size := range []int64{0, 1, math.MaxInt64} {
		if err := (storage.FileWriteRequest{ExpectedSize: &size}).Check(); err != nil {
			t.Fatal(err)
		}
		if err := (storage.FileIO{Write: true, ExpectedSize: &size}).Check(); err != nil {
			t.Fatal(err)
		}
	}
	negative := int64(-1)
	if err := (storage.FileWriteRequest{ExpectedSize: &negative}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative expected write size: %v", err)
	}
	if err := (storage.FileIO{Write: true, ExpectedSize: &negative}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("negative expected native size: %v", err)
	}
}
