package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestLocalStorePersistsThroughABinaryRestartAndRefusesASecondOwner(t *testing.T) {
	testRoot := t.TempDir()
	binary := filepath.Join(testRoot, "remote-fs-server")
	buildServerBinary(t, binary)
	storeRoot := filepath.Join(testRoot, "store")
	if err := os.Mkdir(storeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(storeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	args := []string{
		"-listen", "127.0.0.1:0",
		"-local-store", storeRoot,
		"-volume", "workspace",
		"-quota", "8M",
		"-max-pending-objects", "32",
		"-max-pending-bytes", "2G",
	}

	first := startServerBinary(t, binary, append(append([]string(nil), args...), "-initialize-lock-state"))
	remote := dialBinaryServer(t, first.url)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := remote.Create(ctx, "artifact"); err != nil {
		t.Fatalf("create through first process: %v", err)
	}
	want := []byte("content retained across a real process restart")
	if err := remote.Write(ctx, "artifact", want); err != nil {
		t.Fatalf("write through first process: %v", err)
	}
	cancel()

	contenderContext, stopContender := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopContender()
	contender := exec.CommandContext(contenderContext, binary, args...)
	contenderOutput, contenderErr := contender.CombinedOutput()
	if contenderErr == nil {
		t.Fatal("a second process served the local store while its owner was running")
	}
	if contenderContext.Err() != nil {
		t.Fatalf("the second process did not promptly refuse the owned root: %v", contenderContext.Err())
	}
	if !strings.Contains(strings.ToLower(string(contenderOutput)), "busy") {
		t.Fatalf("the second process failed without reporting owner contention: %s", contenderOutput)
	}

	if err := first.stop(); err != nil {
		t.Fatalf("stop first process: %v\n%s", err, first.output.String())
	}
	second := startServerBinary(t, binary, args)
	reopened := dialBinaryServer(t, second.url)
	readContext, cancelRead := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelRead()
	got, err := reopened.Read(readContext, "artifact")
	if err != nil {
		t.Fatalf("read through restarted process: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("restarted process read %q, want %q", got, want)
	}
	if err := second.stop(); err != nil {
		t.Fatalf("stop restarted process: %v\n%s", err, second.output.String())
	}
}

func buildServerBinary(t *testing.T, destination string) {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate binary test source")
	}
	command := exec.Command("go", "build", "-o", destination, ".")
	command.Dir = filepath.Dir(source)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("build remote-fs-server: %v\n%s", err, output)
	}
}

func dialBinaryServer(t *testing.T, address string) *httprest.Storage {
	t.Helper()
	remote, err := httprest.Dial(address, &http.Client{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("dial %s: %v", address, err)
	}
	return remote
}

type serverBinary struct {
	command *exec.Cmd
	url     string
	done    chan error
	output  lockedText

	stopOnce sync.Once
	stopErr  error
}

func startServerBinary(t *testing.T, binary string, args []string) *serverBinary {
	t.Helper()
	command := exec.Command(binary, args...)
	stderr, err := command.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	server := &serverBinary{
		command: command,
		done:    make(chan error, 1),
	}
	lines := make(chan string, 8)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			server.output.append(line)
			lines <- line
		}
		if err := scanner.Err(); err != nil {
			server.output.append(fmt.Sprintf("reading stderr failed: %v", err))
		}
	}()
	go func() { server.done <- command.Wait() }()
	t.Cleanup(func() {
		if err := server.stop(); err != nil {
			t.Errorf("clean up remote-fs-server: %v\n%s", err, server.output.String())
		}
	})

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case line := <-lines:
			if !strings.HasPrefix(line, "remote-fs-server: serving ") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) == 0 || !strings.HasPrefix(fields[len(fields)-1], "http://") {
				t.Fatalf("readiness line has no final address: %q", line)
			}
			server.url = fields[len(fields)-1]
			return server
		case err := <-server.done:
			server.stopOnce.Do(func() { server.stopErr = err })
			t.Fatalf("remote-fs-server exited before readiness: %v\n%s", err, server.output.String())
		case <-deadline.C:
			_ = server.stop()
			t.Fatalf("remote-fs-server produced no readiness line\n%s", server.output.String())
		}
	}
}

func (s *serverBinary) stop() error {
	s.stopOnce.Do(func() {
		select {
		case s.stopErr = <-s.done:
			return
		default:
		}
		if err := s.command.Process.Signal(syscall.SIGTERM); err != nil {
			if errors.Is(err, os.ErrProcessDone) {
				s.stopErr = <-s.done
				return
			}
			select {
			case s.stopErr = <-s.done:
			default:
				s.stopErr = fmt.Errorf("send SIGTERM: %w", err)
			}
			return
		}
		select {
		case s.stopErr = <-s.done:
		case <-time.After(10 * time.Second):
			killErr := s.command.Process.Kill()
			waitErr := <-s.done
			s.stopErr = fmt.Errorf("process did not stop within 10s: %w", errors.Join(killErr, waitErr))
		}
	})
	return s.stopErr
}

type lockedText struct {
	mu   sync.Mutex
	text strings.Builder
}

func (b *lockedText) append(line string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.text.WriteString(line)
	b.text.WriteByte('\n')
}

func (b *lockedText) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.text.String()
}
