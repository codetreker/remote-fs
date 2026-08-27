package httprest_test

import (
	"context"
	"fmt"
	"io/fs"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localdir"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

// The environment this test binary re-executes itself under, and the outcome the second
// process leaves behind. The outcome travels through a file because the two channels the
// test is about — this process's stdout and stderr — have to stay untouched.
const (
	exerciseEnv = "REMOTE_FS_EXERCISE_THE_LIBRARY"
	outcomeEnv  = "REMOTE_FS_EXERCISE_OUTCOME"
	finished    = "finished"
)

func TestMain(m *testing.M) {
	if os.Getenv(exerciseEnv) == "" {
		os.Exit(m.Run())
	}
	// From here on this process is not a test binary but the program a third party would
	// have written: it imports these packages, uses them, and does nothing else.
	outcome := finished
	if err := exerciseTheLibrary(); err != nil {
		outcome = err.Error()
	}
	os.WriteFile(os.Getenv(outcomeEnv), []byte(outcome), 0o600)
	os.Exit(0)
}

// This package is linked into someone else's process. Anything it prints lands in that
// process's output, and anything it exits ends that process, neither of which its
// operator agreed to (R-INT-2).
//
// The assertion is made on a real second process rather than by swapping os.Stdout and
// os.Stderr, because swapping those variables does not reach the writer that matters: a
// package-level logger built at init holds the *os.File it was handed, and goes on
// writing to the process's real stderr whatever the variable is set to afterwards. That
// is the most idiomatic form of the mistake, so a check it can walk past is not a check.
// What a third party sees is the process's own output, so that is what is captured.
func TestNothingIsPrinted(t *testing.T) {
	outcome := filepath.Join(t.TempDir(), "outcome")
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(),
		exerciseEnv+"=1",
		outcomeEnv+"="+outcome,
		// Where an instrumented binary leaves its counters. Unset, it says so on stderr,
		// and that is the harness talking rather than the library.
		"GOCOVERDIR="+t.TempDir(),
	)
	printed, err := cmd.CombinedOutput()

	recorded, readErr := os.ReadFile(outcome)
	if readErr != nil {
		t.Fatalf("the process ended before it was finished with the library (%v): %v", err, readErr)
	}
	if string(recorded) != finished {
		t.Fatalf("the library could not be exercised: %s", recorded)
	}
	if err != nil {
		t.Fatalf("the process exercising the library failed: %v", err)
	}
	if len(printed) != 0 {
		t.Fatalf("the process wrote %q, want nothing", printed)
	}
}

// exerciseTheLibrary runs every operation across both ends of the transport. The failing
// paths are driven alongside the successful ones because a diagnostic printed on the way
// out is the likely form of the mistake.
func exerciseTheLibrary() error {
	dir, err := os.MkdirTemp("", "remote-fs-library")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	backing, err := localdir.New(dir)
	if err != nil {
		return err
	}
	handler, err := httprest.NewHandler(backing, nil)
	if err != nil {
		return err
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()

	s, err := httprest.Dial(srv.URL, srv.Client())
	if err != nil {
		return err
	}

	ctx := context.Background()
	for _, step := range []struct {
		what string
		run  func() error
	}{
		{"mkdir", func() error { return s.Mkdir(ctx, "d") }},
		{"write", func() error { return s.Write(ctx, "d/f", []byte("payload")) }},
		{"read", func() error { _, err := s.Read(ctx, "d/f"); return err }},
		{"list", func() error { _, err := s.List(ctx, "d"); return err }},
		{"stat", func() error { _, err := s.Stat(ctx, "d/f"); return err }},
		{"setattr", func() error {
			mode := fs.FileMode(0o600)
			return s.SetAttr(ctx, "d/f", storage.AttrChange{Mode: &mode})
		}},
		{"create", func() error { return s.Create(ctx, "d/g") }},
		{"rename", func() error { return s.Rename(ctx, "d/f", "d/g") }},
		{"remove", func() error { return s.Remove(ctx, "d/g") }},
		{"removedir", func() error { return s.RemoveDir(ctx, "d") }},
		{"space", func() error { _, err := s.Space(ctx); return err }},
	} {
		if err := step.run(); err != nil {
			return fmt.Errorf("%s: %w", step.what, err)
		}
	}

	// These are expected to fail, and their errors are the answer rather than a problem.
	s.Stat(ctx, "missing")
	s.Read(ctx, "missing")
	s.List(ctx, "missing")
	s.Remove(ctx, "missing")
	s.RemoveDir(ctx, "missing")
	s.Rename(ctx, "missing", "elsewhere")
	s.Create(ctx, "missing/f")
	s.Mkdir(ctx, "../outside")
	s.Write(ctx, "missing/f", []byte("x"))
	s.SetAttr(ctx, "missing", storage.AttrChange{})

	// A local directory keeps no change log, so these three refuse. The refusing path is
	// the one worth running here: a diagnostic printed on the way out is what this test
	// exists to catch, and that is where one gets written.
	s.Subscribe(ctx)
	s.Resubscribe(ctx, "a-log", 1)
	s.Snapshot(ctx)
	return nil
}
