package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func fileConfigArgs(root string) []string {
	return []string{"-listen", "127.0.0.1:0", "-local-store", root, "-volume", "workspace", "-quota", "1M"}
}

func TestFileDefaultsPreserveTheOneGiBFileCeiling(t *testing.T) {
	config, _, err := parseConfig(fileConfigArgs("/store"), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if config.files.service.MaxFileBytes != 1<<30 || config.files.service.MaxMaterializedBytes < 2<<30 {
		t.Fatalf("file ceiling=%d materialization=%d", config.files.service.MaxFileBytes, config.files.service.MaxMaterializedBytes)
	}
	if config.files.service.MaxSessions != config.http.Files.MaxSessions {
		t.Fatalf("HTTP and native session bounds disagree: %d/%d", config.http.Files.MaxSessions, config.files.service.MaxSessions)
	}
}

func TestFileControlsReachTheirOwnedConfiguration(t *testing.T) {
	config, _, err := parseConfig(append(fileConfigArgs("/store"),
		"-max-retained-files", "19", "-max-file-size", "4M", "-max-file-staging-bytes", "8M",
		"-file-operation-timeout", "2s", "-http-max-file-sessions", "3",
		"-file-session-max-actions", "7", "-file-session-max-pending-actions", "5",
		"-file-session-lease", "3s", "-file-session-history", "4s",
	), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if config.files.retained != 19 || config.files.service.MaxFileBytes != 4<<20 || config.files.service.MaxMaterializedBytes != 8<<20 ||
		config.files.service.FileOperationTimeout != 2*time.Second || config.files.service.MaxSessions != 3 {
		t.Fatalf("native file configuration is %+v", config.files)
	}
	files := config.http.Files
	if files.MaxSessions != 3 || files.Session.MaxActions != 7 || files.Session.MaxPendingActions != 5 ||
		files.Session.Lease != 3*time.Second || files.Session.History != 4*time.Second {
		t.Fatalf("HTTP file configuration is %+v", files)
	}
}

func TestImplicitFileCeilingRespectsConfiguredObjectCapacity(t *testing.T) {
	args := append(fileConfigArgs("/store"), "-local-max-object-bytes", "4M", "-http-max-write-bytes", "4M")
	config, _, err := parseConfig(args, io.Discard)
	if err != nil || config.files.service.MaxFileBytes != 4<<20 {
		t.Fatalf("inherited file ceiling=%d err=%v", config.files.service.MaxFileBytes, err)
	}
	_, _, err = parseConfig(append(args, "-max-file-size", "8M"), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "-local-max-object-bytes") {
		t.Fatalf("explicit file ceiling above object capacity returned %v", err)
	}
}

func TestInvalidFileControlsFailBeforeListenerAndStorageInitialization(t *testing.T) {
	for _, test := range []struct{ name, value string }{
		{"max-retained-files", "0"}, {"max-retained-files", strconv.Itoa(math.MaxInt)},
		{"max-file-size", "0"}, {"max-file-staging-bytes", "1K"},
		{"file-operation-timeout", "0s"}, {"http-max-file-sessions", "0"},
		{"file-session-max-actions", "0"}, {"file-session-max-pending-actions", "0"},
		{"file-session-lease", "0s"}, {"file-session-history", "0s"},
	} {
		t.Run(test.name+"="+test.value, func(t *testing.T) {
			root := emptyPrivateDirectory(t)
			var output bytes.Buffer
			err := run(append(fileConfigArgs(root), "-listen", "invalid-address", "-initialize-lock-state", "-"+test.name, test.value), &output)
			if err == nil || !strings.Contains(err.Error()+output.String(), test.name) {
				t.Fatalf("invalid %s reached another startup stage: %v %s", test.name, err, output.String())
			}
			entries, readErr := os.ReadDir(root)
			if readErr != nil || len(entries) != 0 {
				t.Fatalf("invalid file controls changed storage: entries=%d err=%v", len(entries), readErr)
			}
		})
	}
}

func TestNativeFileLimitsReachTheLocalStore(t *testing.T) {
	config, _, err := parseConfig(append(fileConfigArgs(emptyPrivateDirectory(t)),
		"-initialize-lock-state", "-max-retained-files", "2", "-http-max-file-sessions", "1",
		"-max-file-size", "4K", "-max-file-staging-bytes", "8K",
	), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	v, err := open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.close(); err != nil {
			t.Errorf("close native file volume: %v", err)
		}
	})
	files := v.volume.(storage.FileStorage)
	session, _, err := files.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := session.Close(context.Background(), newTestFileAction(t, session)); err != nil {
			t.Errorf("close native file session: %v", err)
		}
	})
	if extra, _, err := files.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.EAGAIN) {
		if extra != nil {
			_, _ = extra.Close(context.Background(), newTestFileAction(t, extra))
		}
		t.Fatalf("second native file session returned %v, want EAGAIN", err)
	}
	root, err := retainTestRoot(t.Context(), files, session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := root.Close(context.Background(), newTestFileAction(t, session)); err != nil {
			t.Errorf("close parent reference: %v", err)
		}
	})
	first, err := createTestFileAt(t.Context(), session, root, "first")
	if err != nil {
		t.Fatal(err)
	}
	if extra, err := createTestFileAt(t.Context(), session, root, "second"); !errors.Is(err, syscall.EMFILE) {
		if extra != nil {
			_, _ = extra.Close(context.Background(), newTestFileAction(t, session))
		}
		t.Fatalf("second native reference returned %v, want EMFILE", err)
	}
	if _, err := v.volume.Stat(t.Context(), "second"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused open created a volume entry: %v", err)
	}
	if _, err := first.WriteAt(t.Context(), storage.FileWriteRequest{Offset: 4096, Data: []byte("x")}, newTestFileAction(t, session)); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("write beyond configured file size returned %v, want EFBIG", err)
	}
	if _, err := first.Close(t.Context(), newTestFileAction(t, session)); err != nil {
		t.Fatal(err)
	}
	second, err := createTestFileAt(t.Context(), session, root, "second")
	if err != nil {
		t.Fatalf("close did not release native reference capacity: %v", err)
	}
	if _, err := second.Close(t.Context(), newTestFileAction(t, session)); err != nil {
		t.Fatal(err)
	}
}

func nextTestFileAction(ctx context.Context, session storage.FileSession) (storage.FileActionID, error) {
	status, err := session.Status(ctx)
	if err != nil {
		return "", err
	}
	return storage.NewFileActionID(status.ActionEpoch)
}

func newTestFileAction(t *testing.T, session storage.FileSession) storage.FileActionID {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	action, err := nextTestFileAction(ctx, session)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

func retainTestRoot(ctx context.Context, files storage.FileStorage, session storage.FileSession) (storage.File, error) {
	state, err := files.FileState(ctx)
	if err != nil {
		return nil, err
	}
	action, err := nextTestFileAction(ctx, session)
	if err != nil {
		return nil, err
	}
	receipt, err := session.Retain(ctx, storage.RetainRequest{NodeID: state.RootID}, action)
	if err != nil {
		return nil, err
	}
	if receipt.State != storage.FileActionCompleted || receipt.Reference == 0 {
		return nil, fmt.Errorf("root retention did not complete: %w", syscall.EIO)
	}
	return session.Reference(ctx, receipt.Reference)
}

func createTestFileAt(ctx context.Context, session storage.FileSession, parent storage.File, name string) (storage.File, error) {
	observation, err := parent.Stat(ctx, storage.ObservationOptions{IncludeLocation: true})
	if err != nil {
		return nil, err
	}
	if observation.Location == nil {
		return nil, fmt.Errorf("parent observation omitted its requested location witness: %w", syscall.EIO)
	}
	action, err := nextTestFileAction(ctx, session)
	if err != nil {
		return nil, err
	}
	receipt, err := session.CreateAndRetainAt(ctx, storage.CreateAndRetainRequest{
		Target: storage.EntryTarget{Parent: parent.Reference(), ParentID: observation.Attr.ID, Name: []byte(name),
			DirectoryRevision: observation.Attr.DirectoryRevision, Witness: observation.Location},
		Initial: storage.NodeInitial{Kind: storage.NodeRegular},
		Claim:   storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent},
	}, action)
	if err != nil {
		return nil, err
	}
	if receipt.State != storage.FileActionCompleted || receipt.Reference == 0 {
		return nil, fmt.Errorf("file creation did not complete: %w", syscall.EIO)
	}
	return session.Reference(ctx, receipt.Reference)
}
