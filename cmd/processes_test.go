package cmd_test

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// The two commands, built once per run into a directory TestMain removes.
var (
	buildOnce sync.Once
	buildDir  string
	buildErr  error
)

func serverBinary(t *testing.T) string { return binary(t, "remote-fs-server") }
func mountBinary(t *testing.T) string  { return binary(t, "remote-fs") }

func binary(t *testing.T, name string) string {
	t.Helper()
	buildOnce.Do(func() {
		buildDir, buildErr = os.MkdirTemp("", "remote-fs-binaries-")
		if buildErr != nil {
			return
		}
		// The working directory of a test is its own package directory, so each command
		// is one path element away.
		for _, command := range []string{"remote-fs-server", "remote-fs"} {
			build := exec.Command("go", "build", "-o", filepath.Join(buildDir, command), "./"+command)
			if out, err := build.CombinedOutput(); err != nil {
				buildErr = fmt.Errorf("building %s: %w\n%s", command, err, out)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return filepath.Join(buildDir, name)
}

func removeBuiltBinaries() {
	if buildDir != "" {
		os.RemoveAll(buildDir)
	}
}

// process is one running binary. Everything it writes is kept, so that a failure can show
// what it said rather than only that it happened.
//
// Standard output and standard error are the same pipe. These programs write to standard
// error alone, and a line that turned up on standard output would be a change nobody
// asked for rather than something to be quietly tolerated.
type process struct {
	name string
	cmd  *exec.Cmd

	mu      sync.Mutex
	written []string

	// exited closes once the process has ended and everything it wrote has been
	// collected, so anything read after it is closed is the whole of the output.
	exited chan struct{}
	err    error
}

func start(t *testing.T, name, binary string, args ...string) *process {
	t.Helper()

	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, args...)
	cmd.Stdout, cmd.Stderr = write, write
	if err := cmd.Start(); err != nil {
		write.Close()
		read.Close()
		t.Fatalf("starting %s: %v", name, err)
	}
	// The child holds the only copy that matters now; keeping ours open would mean the
	// read end never reaches the end of the output.
	write.Close()

	p := &process{name: name, cmd: cmd, exited: make(chan struct{})}
	collected := make(chan struct{})
	go func() {
		defer close(collected)
		defer read.Close()
		lines := bufio.NewScanner(read)
		for lines.Scan() {
			p.mu.Lock()
			p.written = append(p.written, lines.Text())
			p.mu.Unlock()
		}
	}()
	go func() {
		p.err = p.cmd.Wait()
		<-collected
		close(p.exited)
	}()
	t.Cleanup(p.stop)
	return p
}

// awaitLine waits for the process to say something, and fails the test if it stops or
// takes too long without saying it.
func (p *process) awaitLine(t *testing.T, substring string, within time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		// Read before the output is searched, never after: once this is closed everything
		// the process wrote is already collected, and a search that came first could miss
		// a line that arrived in between and call the process silent.
		var ended bool
		select {
		case <-p.exited:
			ended = true
		default:
		}

		p.mu.Lock()
		var found string
		for _, line := range p.written {
			if strings.Contains(line, substring) {
				found = line
				break
			}
		}
		p.mu.Unlock()

		switch {
		case found != "":
			return found
		case ended:
			t.Fatalf("%s exited (%v) without saying %q\n%s", p.name, p.err, substring, p.output())
		case time.Now().After(deadline):
			t.Fatalf("%s did not say %q within %v\n%s", p.name, substring, within, p.output())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *process) interrupt(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatalf("interrupting %s: %v", p.name, err)
	}
}

// hangup asks a process for whatever it does about SIGHUP and leaves it running.
func (p *process) hangup(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGHUP); err != nil {
		t.Fatalf("hanging up %s: %v", p.name, err)
	}
}

func (p *process) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-p.exited:
		return p.err
	case <-time.After(startup):
		t.Fatalf("%s did not exit within %v\n%s", p.name, startup, p.output())
		return nil
	}
}

func (p *process) output() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return strings.Join(p.written, "\n")
}

// stop ends a process the test did not end itself. It asks before insisting: a mount
// killed outright leaves the mountpoint attached, which is the state these tests exist to
// keep this machine out of.
func (p *process) stop() {
	select {
	case <-p.exited:
		return
	default:
	}
	p.cmd.Process.Signal(syscall.SIGINT)
	select {
	case <-p.exited:
		return
	case <-time.After(startup):
	}
	p.cmd.Process.Signal(syscall.SIGKILL)
	<-p.exited
}

// runningServer is a remote-fs-server process and the address it reported.
type runningServer struct {
	*process
	url string
}

func startDirectoryServerBinary(t *testing.T, directory string, args ...string) *runningServer {
	t.Helper()
	configured := []string{
		"-listen", "127.0.0.1:0", "-dir", directory,
		"-lock-state-root", privateDirectory(t), "-initialize-lock-state",
	}
	return startServerBinary(t, append(configured, args...)...)
}

func startServerBinary(t *testing.T, args ...string) *runningServer {
	t.Helper()
	p := start(t, "remote-fs-server", serverBinary(t), args...)
	line := p.awaitLine(t, "serving", startup)
	_, address, found := strings.Cut(line, " at ")
	if !found {
		t.Fatalf("remote-fs-server did not report an address: %q", line)
	}
	return &runningServer{process: p, url: strings.TrimSpace(address)}
}

func startMountBinary(t *testing.T, args ...string) *process {
	t.Helper()
	p := start(t, "remote-fs", mountBinary(t), args...)
	p.awaitLine(t, "mounted at", startup)
	return p
}
