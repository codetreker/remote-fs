package service

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/codetreker/remote-fs/internal/management"
)

func rootTestConfig(t *testing.T) Config {
	t.Helper()
	return DefaultConfig(filepath.Join(t.TempDir(), "state"), "https://files.example.test")
}

func rootTestIdentity(t *testing.T) management.ID {
	t.Helper()
	id, err := management.NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestStateRootStableLockAndReopen(t *testing.T) {
	c := rootTestConfig(t)
	r, err := openRoot(t.Context(), c, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	id := rootTestIdentity(t)
	if err := r.BindDatabase(id); err != nil {
		t.Fatal(err)
	}
	if _, err := openRoot(t.Context(), c, false); !errors.Is(err, ErrRootBusy) {
		t.Fatalf("second independent opener: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openRoot(t.Context(), c, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.DatabaseID != id {
		t.Fatalf("identity changed: %s != %s", reopened.DatabaseID, id)
	}
	if err := reopened.BindDatabase(rootTestIdentity(t)); !errors.Is(err, management.ErrCorrupt) {
		t.Fatalf("replace durable identity: %v", err)
	}
}

func TestStateRootRejectsMissingOrUnknownEvidence(t *testing.T) {
	for _, mutation := range []string{"missing-proof", "wrong-path", "duplicate-proof", "unknown-data", "weak-proof", "symlink-data", "weak-anchor", "hardlink-anchor", "empty-anchor"} {
		t.Run(mutation, func(t *testing.T) {
			c := rootTestConfig(t)
			r, err := openRoot(t.Context(), c, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.BindDatabase(rootTestIdentity(t)); err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			proof := filepath.Join(c.StateRoot, "data", "root.json")
			anchor := filepath.Join(c.StateRoot, "service.lock")
			switch mutation {
			case "missing-proof":
				err = os.Remove(proof)
			case "wrong-path":
				err = os.WriteFile(proof, []byte(`{"version":1,"state_root":"/elsewhere","database_id":"00000000000000000000000000000000"}`+"\n"), 0600)
			case "duplicate-proof":
				err = os.WriteFile(proof, []byte(`{"version":1,"version":1,"state_root":"/elsewhere","database_id":"00000000000000000000000000000000"}`+"\n"), 0600)
			case "unknown-data":
				err = os.WriteFile(filepath.Join(c.StateRoot, "data", "unknown.sqlite"), []byte("unknown"), 0600)
				if err == nil {
					_, err = openRoot(t.Context(), c, true)
					if err == nil {
						t.Fatal("initialization adopted existing state")
					}
					return
				}
			case "weak-proof":
				err = os.Chmod(proof, 0644)
			case "symlink-data":
				original := filepath.Join(c.StateRoot, "data")
				err = os.Rename(original, filepath.Join(c.StateRoot, "elsewhere"))
				if err == nil {
					err = os.Symlink("elsewhere", original)
				}
			case "weak-anchor":
				err = os.Chmod(anchor, 0644)
			case "hardlink-anchor":
				err = os.Link(anchor, filepath.Join(c.StateRoot, "other-lock"))
			case "empty-anchor":
				err = os.WriteFile(anchor, nil, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			if opened, err := openRoot(t.Context(), c, false); err == nil {
				_ = opened.Close()
				t.Fatal("accepted unknown or unsafe state")
			}
		})
	}
}

func TestStateRootRejectsUnsafeAncestorsAndCancellation(t *testing.T) {
	c := rootTestConfig(t)
	parent := filepath.Dir(c.StateRoot)
	if err := os.Chmod(parent, 0777); err != nil {
		t.Fatal(err)
	}
	if root, err := openRoot(t.Context(), c, true); err == nil {
		_ = root.Close()
		t.Fatal("accepted writable ancestor")
	}
	if err := os.Chmod(parent, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := openRoot(ctx, c, true); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled initialization: %v", err)
	}
	if _, err := os.Stat(c.StateRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled initialization touched state: %v", err)
	}
	alias := filepath.Join(parent, "alias")
	if err := os.Symlink(parent, alias); err != nil {
		t.Fatal(err)
	}
	c.StateRoot = filepath.Join(alias, "state")
	if root, err := openRoot(t.Context(), c, true); err == nil {
		_ = root.Close()
		t.Fatal("accepted symlink ancestor")
	}
}

func TestStateRootDetectsAnchorReplacement(t *testing.T) {
	c := rootTestConfig(t)
	r, err := openRoot(t.Context(), c, true)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.validateAnchor(); err != nil {
		t.Fatalf("pristine uninitialized anchor: %v", err)
	}
	if err := r.BindDatabase(rootTestIdentity(t)); err != nil {
		t.Fatal(err)
	}
	anchor := filepath.Join(c.StateRoot, "service.lock")
	if err := r.validateAnchor(); err != nil {
		t.Fatalf("unchanged bound anchor: %v", err)
	}
	content, err := os.ReadFile(anchor)
	if err != nil {
		t.Fatal(err)
	}
	var proof rootProof
	if err := json.Unmarshal(content, &proof); err != nil {
		t.Fatal(err)
	}
	proof.DatabaseID = rootTestIdentity(t)
	changed, err := json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(anchor, append(changed, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.validateAnchor(); !errors.Is(err, management.ErrCorrupt) {
		t.Fatalf("same-inode anchor identity mutation: %v", err)
	}
	if err := os.WriteFile(anchor, content, 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.validateAnchor(); err != nil {
		t.Fatalf("restored canonical anchor: %v", err)
	}
	if err := os.Rename(anchor, anchor+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(anchor, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := r.validateAnchor(); !errors.Is(err, management.ErrCorrupt) {
		t.Fatalf("anchor replacement validation: %v", err)
	}
}

func TestStateRootDoesNotAdoptInterruptedUnknownData(t *testing.T) {
	for _, leaf := range []string{"management.sqlite", "volumes/unknown-root"} {
		t.Run(leaf, func(t *testing.T) {
			c := rootTestConfig(t)
			r, err := openRoot(t.Context(), c, true)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(c.StateRoot, "data", leaf)
			if err := os.WriteFile(path, []byte("unowned data"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := InitState(t.Context(), c); err == nil {
				t.Fatal("initialization adopted unknown residual data")
			}
			content, err := os.ReadFile(path)
			if err != nil || string(content) != "unowned data" {
				t.Fatalf("unknown data was altered: %q %v", content, err)
			}
		})
	}
}
