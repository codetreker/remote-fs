package main

import (
	"bytes"
	"context"
	"errors"
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
	if config.files.advisory.MaxFileBytes != 1<<30 || config.files.advisory.MaxMaterializedBytes < 2<<30 {
		t.Fatalf("file ceiling=%d materialization=%d", config.files.advisory.MaxFileBytes, config.files.advisory.MaxMaterializedBytes)
	}
	if config.files.advisory.MaxSessions != config.http.Files.MaxSessions {
		t.Fatalf("HTTP and native session bounds disagree: %d/%d", config.http.Files.MaxSessions, config.files.advisory.MaxSessions)
	}
}

func TestFileControlsReachTheirOwnedConfiguration(t *testing.T) {
	config, _, err := parseConfig(append(fileConfigArgs("/store"),
		"-max-retained-files", "19", "-max-file-size", "4M", "-max-file-staging-bytes", "8M",
		"-file-operation-timeout", "2s", "-http-max-file-sessions", "3",
		"-http-max-file-actions", "5", "-http-max-file-cleanup-actions", "7",
		"-file-session-lease", "3s", "-file-session-history", "4s", "-http-file-open-ack-timeout", "1s",
	), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if config.files.retained != 19 || config.files.advisory.MaxFileBytes != 4<<20 || config.files.advisory.MaxMaterializedBytes != 8<<20 ||
		config.files.advisory.FileOperationTimeout != 2*time.Second || config.files.advisory.MaxSessions != 3 {
		t.Fatalf("native file configuration is %+v", config.files)
	}
	files := config.http.Files
	if files.MaxSessions != 3 || files.MaxActions != 5 || files.MaxCleanupActions != 7 || files.PendingAck != time.Second ||
		files.Session.Lease != 3*time.Second || files.Session.History != 4*time.Second {
		t.Fatalf("HTTP file configuration is %+v", files)
	}
}

func TestImplicitFileCeilingRespectsConfiguredObjectCapacity(t *testing.T) {
	args := append(fileConfigArgs("/store"), "-local-max-object-bytes", "4M", "-http-max-write-bytes", "4M")
	config, _, err := parseConfig(args, io.Discard)
	if err != nil || config.files.advisory.MaxFileBytes != 4<<20 {
		t.Fatalf("inherited file ceiling=%d err=%v", config.files.advisory.MaxFileBytes, err)
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
		{"http-max-file-actions", "0"}, {"http-max-file-cleanup-actions", "0"},
		{"file-session-lease", "0s"}, {"file-session-history", "0s"},
		{"http-file-open-ack-timeout", "0s"}, {"http-file-open-ack-timeout", "31s"},
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
		"-initialize-lock-state", "-max-retained-files", "1", "-http-max-file-sessions", "1",
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
	session, err := files.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.Background()); err != nil {
			t.Errorf("close native file session: %v", err)
		}
	})
	if extra, err := files.NewFileSession(t.Context(), storage.DefaultFileSessionOptions()); !errors.Is(err, syscall.ENOLCK) {
		if extra != nil {
			_ = extra.Close(context.Background())
		}
		t.Fatalf("second native file session returned %v, want ENOLCK", err)
	}
	options := storage.FileOpenOptions{Read: true, Write: true, Create: true, Mode: 0o600}
	first, err := session.OpenFile(t.Context(), "first", options)
	if err != nil {
		t.Fatal(err)
	}
	if extra, err := session.OpenFile(t.Context(), "second", options); !errors.Is(err, syscall.EAGAIN) {
		if extra != nil {
			_ = extra.Close(context.Background())
		}
		t.Fatalf("second native reference returned %v, want EAGAIN", err)
	}
	if _, err := v.volume.Stat(t.Context(), "second"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("refused open created a volume entry: %v", err)
	}
	if _, err := first.WriteAt(t.Context(), 4096, []byte("x")); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("write beyond configured file size returned %v, want EFBIG", err)
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, err := session.OpenFile(t.Context(), "second", options)
	if err != nil {
		t.Fatalf("close did not release native reference capacity: %v", err)
	}
	if err := second.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
