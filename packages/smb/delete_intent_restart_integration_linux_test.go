//go:build linux

package smb

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

const smbDeleteCrashChildEnv = "REMOTE_FS_SMB_DELETE_CRASH_CHILD"
const smbDeleteCrashConfigEnv = "REMOTE_FS_SMB_DELETE_CRASH_CONFIG"

type smbDeleteCrashConfig struct {
	Root       string
	Owner      storage.DeleteIntentOwner
	Address    string
	Initialize bool
	Phase      string
}

type smbDeleteCrashReceipt struct {
	URL     string
	ACKLost bool
	Node    uint64
	Intent  storage.DeleteIntentID
	Outcome storage.DeleteIntentOutcome
	Status  uint32
}

type smbDeleteCrashProcess struct {
	command *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	decoder *json.Decoder
	waited  bool
}

func startSMBDeleteCrashProcess(t *testing.T, role string, config smbDeleteCrashConfig) *smbDeleteCrashProcess {
	t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	childCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(childCtx, os.Args[0], "-test.run=^TestSMBDeleteOnCloseSurvivesProcessKill$", "-test.timeout=45s")
	command.Env = append(os.Environ(), smbDeleteCrashChildEnv+"="+role, smbDeleteCrashConfigEnv+"="+string(encoded))
	command.Stderr, command.WaitDelay = os.Stderr, time.Second
	command.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	child := &smbDeleteCrashProcess{command: command, stdin: stdin, stdout: stdout, decoder: json.NewDecoder(bufio.NewReader(stdout))}
	t.Cleanup(func() {
		if !child.waited {
			if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				t.Error(err)
			}
			_ = command.Wait()
			child.waited = true
		}
		_ = stdin.Close()
		_ = stdout.Close()
	})
	return child
}

func (p *smbDeleteCrashProcess) receive(t *testing.T) smbDeleteCrashReceipt {
	t.Helper()
	var receipt smbDeleteCrashReceipt
	decoded := make(chan error, 1)
	go func() { decoded <- p.decoder.Decode(&receipt) }()
	select {
	case err := <-decoded:
		if err != nil {
			diagnostic, _ := io.ReadAll(io.LimitReader(p.decoder.Buffered(), 4096))
			t.Fatalf("child receipt: %v; buffered output: %s", err, diagnostic)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("child failed to reach requested durable boundary")
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	return receipt
}

func (p *smbDeleteCrashProcess) send(t *testing.T, instruction string) {
	t.Helper()
	if _, err := fmt.Fprintln(p.stdin, instruction); err != nil {
		t.Fatal(err)
	}
}

func (p *smbDeleteCrashProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err := p.command.Wait()
	p.waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("child did not exit after SIGKILL: %v", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child was not killed at boundary: %v", exit.Sys())
	}
}

func newSMBDeleteCrashEndpoint(t *testing.T, backend storage.FileStorage, owner storage.DeleteIntentOwner) (*Server, *Export) {
	t.Helper()
	server, err := New(endpointConfig())
	if err != nil {
		t.Fatal(err)
	}
	server.nameComparer = testNameCompare
	identity, err := backend.(storage.BackendIdentity).BackendIdentity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	export, err := server.Publish(Share{Name: "renamed-share", Volume: "files", BackendVolume: identity.Volume, RootNodeID: identity.RootNodeID, Backend: backend, DeleteIntentOwner: owner})
	if err != nil {
		t.Fatal(err)
	}
	return server, export
}

func emitSMBDeleteCrashReceipt(t *testing.T, receipt smbDeleteCrashReceipt) {
	t.Helper()
	if err := json.NewEncoder(os.Stdout).Encode(receipt); err != nil {
		t.Fatal(err)
	}
}

func runSMBDeleteCrashAuthority(t *testing.T, config smbDeleteCrashConfig) {
	locks := locking.DefaultOptions()
	locks.MaxLease = 250 * time.Millisecond
	store, err := localstore.Open(t.Context(), localstore.Config{Root: config.Root, Volume: "files", Quota: 1 << 20,
		Window: sqlite.DefaultWindow(), Maintenance: objectstore.Options{SweepInterval: 10 * time.Millisecond, SweepBatch: 8},
		Locks: &locks, InitializeLocks: config.Initialize})
	if err != nil {
		t.Fatal(err)
	}
	options := httprest.DefaultHandlerOptions()
	options.Files = httprest.DefaultFileLimits()
	options.Files.Session = fixtureFileSessionOptions()
	handler, err := httprest.NewHandlerWithOptions(store, nil, options)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", config.Address)
	if err != nil {
		t.Fatal(err)
	}
	var lostAck atomic.Bool
	faultHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if config.Phase != "ack-lost" || r.URL.Path != httprest.Prefix+string(httprest.OpFileControl) || r.Body == nil {
			handler.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, httprest.MaxFileControlBytes+1))
		if err != nil || len(body) > int(httprest.MaxFileControlBytes) {
			http.Error(w, "fault fixture request exceeds control bound", http.StatusBadRequest)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if len(body) == 0 {
			handler.ServeHTTP(w, r)
			return
		}
		var request struct {
			Op storage.Operation `json:"op"`
		}
		if err := json.Unmarshal(body, &request); err != nil {
			http.Error(w, "fault fixture request is invalid", http.StatusBadRequest)
			return
		}
		if request.Op == storage.OpFileAcknowledgeDeleteIntent && lostAck.CompareAndSwap(false, true) {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			if response.Code != http.StatusOK {
				t.Errorf("ACK fault fired without committed result: %d %s", response.Code, response.Body.String())
			}
			emitSMBDeleteCrashReceipt(t, smbDeleteCrashReceipt{ACKLost: true})
			http.Error(w, "committed ACK reply lost", http.StatusBadGateway)
			return
		}
		if request.Op == storage.OpFileQueryAction && lostAck.Load() {
			http.Error(w, "exact ACK receipt temporarily unavailable", http.StatusBadGateway)
			return
		}
		handler.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: faultHandler, ReadHeaderTimeout: time.Second}
	emitSMBDeleteCrashReceipt(t, smbDeleteCrashReceipt{URL: "http://" + listener.Addr().String()})
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

func runSMBDeleteCrashHost(t *testing.T, config smbDeleteCrashConfig) {
	client, err := httprest.Dial(config.Address, &http.Client{Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	server, export := newSMBDeleteCrashEndpoint(t, client, config.Owner)
	principal := testPrincipal("0123456789abcdef")
	ctx := WithPrincipal(t.Context(), principal)
	if err := export.RecoverDeleteIntents(ctx); err != nil {
		t.Fatal(err)
	}
	options := storage.DefaultFileSessionOptions()
	options.Lease = time.Second
	raw, err := client.NewFileSession(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	state, err := raw.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := raw.(storage.FileSessionIdentity).FileSessionIdentity(ctx)
	if err != nil {
		t.Fatal(err)
	}
	authority := &authoritySession{raw: raw, identity: identity, export: export, principal: principal, actionEpoch: state.ActionEpoch, deadline: time.Now().Add(state.Remaining)}
	s := &session{id: 21, principal: principal}
	tree := &tree{kind: volumeTree, export: export, authority: authority, sessionID: s.id}
	connection := &connection{server: server}
	_, status, id := connection.createFile(ctx, s, tree, deleteCreateRequestForTest("victim", 3, accessReadData|accessDelete, 0))
	if status != statusOK {
		t.Fatalf("child CREATE status=%#x", status)
	}
	node := tree.findFileHandle(id).nodeID
	page, err := raw.(storage.FileActions).ListDeleteIntents(ctx, config.Owner, 0, 1)
	if err != nil || len(page.Intents) != 1 || page.Intents[0].NodeID != node || page.Intents[0].Outcome != storage.DeleteIntentArmed {
		t.Fatalf("accepted child intent=%+v %v", page, err)
	}
	intent := page.Intents[0].ID
	if config.Phase == "pending" {
		if _, status, _ := connection.createFile(ctx, s, tree, createRequestForTest("victim", 1, accessReadData)); status != statusOK {
			t.Fatalf("child compatible old open=%#x", status)
		}
	}
	emitSMBDeleteCrashReceipt(t, smbDeleteCrashReceipt{Node: node, Intent: intent, Outcome: storage.DeleteIntentArmed, Status: statusOK})
	instructions := bufio.NewScanner(os.Stdin)
	for instructions.Scan() {
		switch instructions.Text() {
		case "close":
			body := make([]byte, 24)
			binary.LittleEndian.PutUint16(body, 24)
			copy(body[8:], id[:])
			_, status := connection.closeFile(ctx, s, tree, wire.Request{Header: wire.Header{Command: wire.Close}, Body: body}, wire.FileID{})
			observed, err := raw.(storage.FileActions).QueryDeleteIntent(ctx, config.Owner, intent)
			if err != nil {
				t.Fatal(err)
			}
			emitSMBDeleteCrashReceipt(t, smbDeleteCrashReceipt{Node: node, Intent: intent, Outcome: observed.Outcome, Status: status})
		case "stale-read":
			_, err := tree.findFileHandle(id).file.ReadAt(ctx, 0, 1)
			if !errors.Is(err, syscall.ESTALE) && !errors.Is(err, syscall.EIO) {
				t.Fatalf("retired child reference read=%v", err)
			}
			emitSMBDeleteCrashReceipt(t, smbDeleteCrashReceipt{Node: node, Intent: intent, Status: statusIO})
		default:
			t.Fatalf("unknown crash instruction %q", instructions.Text())
		}
	}
	if err := instructions.Err(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("host pipe closed without requested SIGKILL")
}

func TestSMBDeleteOnCloseSurvivesProcessKill(t *testing.T) {
	if role := os.Getenv(smbDeleteCrashChildEnv); role != "" {
		var config smbDeleteCrashConfig
		if err := json.Unmarshal([]byte(os.Getenv(smbDeleteCrashConfigEnv)), &config); err != nil {
			t.Fatal(err)
		}
		if role == "authority" {
			runSMBDeleteCrashAuthority(t, config)
			return
		}
		if role == "host" {
			runSMBDeleteCrashHost(t, config)
			return
		}
		t.Fatalf("unknown crash child role %s", role)
	}
	for _, target := range []string{"host", "authority", "both"} {
		for _, phase := range []string{"armed", "pending", "ack-lost"} {
			t.Run(target+"/"+phase, func(t *testing.T) {
				root := filepath.Join(t.TempDir(), "store")
				if err := os.Mkdir(root, 0o700); err != nil {
					t.Fatal(err)
				}
				owner, err := storage.NewDeleteIntentOwner()
				if err != nil {
					t.Fatal(err)
				}
				config := smbDeleteCrashConfig{Root: root, Owner: owner, Address: "127.0.0.1:0", Initialize: true, Phase: phase}
				configPath := filepath.Join(filepath.Dir(root), "host-config.json")
				encoded, err := json.Marshal(config)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
					t.Fatal(err)
				}
				authority := startSMBDeleteCrashProcess(t, "authority", config)
				url := authority.receive(t).URL
				if url == "" {
					t.Fatal("authority published no HTTP endpoint")
				}
				config.Address = url
				host := startSMBDeleteCrashProcess(t, "host", config)
				accepted := host.receive(t)
				if accepted.Node == 0 || accepted.Intent.Check() != nil || accepted.Outcome != storage.DeleteIntentArmed {
					t.Fatalf("host reached no accepted CREATE: %+v", accepted)
				}
				if phase != "armed" {
					host.send(t, "close")
					closed := host.receive(t)
					if closed.Node != accepted.Node || closed.Intent != accepted.Intent {
						t.Fatalf("close moved responsibility: %+v -> %+v", accepted, closed)
					}
					if phase == "pending" && (closed.Status != statusOK || closed.Outcome != storage.DeleteIntentPending) {
						t.Fatalf("pending boundary=%+v", closed)
					}
					if phase == "ack-lost" {
						if closed.Outcome != storage.DeleteIntentUnknown {
							t.Fatalf("committed ACK-lost boundary=%+v", closed)
						}
						if fault := authority.receive(t); !fault.ACKLost {
							t.Fatalf("ACK loss was not exercised=%+v", fault)
						}
					}
				}
				if target == "host" || target == "both" {
					host.kill(t)
				}
				if target != "host" {
					authority.kill(t)
					config.Address, config.Initialize = url[len("http://"):], false
					authority = startSMBDeleteCrashProcess(t, "authority", config)
					if restarted := authority.receive(t).URL; restarted != url {
						t.Fatalf("authority moved endpoint=%q want=%q", restarted, url)
					}
					if target == "authority" {
						if phase == "armed" {
							host.send(t, "stale-read")
							if got := host.receive(t); got.Status != statusIO {
								t.Fatalf("live host hid retired reference=%+v", got)
							}
						}
						if err := host.command.Process.Signal(syscall.Signal(0)); err != nil {
							t.Fatalf("host did not survive authority kill: %v", err)
						}
						host.kill(t)
					}
				}
				client, err := httprest.Dial(url, &http.Client{Timeout: 3 * time.Second})
				if err != nil {
					t.Fatal(err)
				}
				observer, err := client.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := observer.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				actions := observer.(storage.FileActions)
				deadline := time.Now().Add(5 * time.Second)
				for {
					status, err := actions.QueryDeleteIntent(t.Context(), owner, accepted.Intent)
					if err != nil {
						t.Fatal(err)
					}
					if status.Outcome == storage.DeleteIntentCompleted || phase == "ack-lost" && status.Outcome == storage.DeleteIntentUnknown {
						if phase != "ack-lost" && status.NodeID != accepted.Node {
							t.Fatalf("restart changed original target=%+v", status)
						}
						break
					}
					if status.Outcome != storage.DeleteIntentPending && status.Outcome != storage.DeleteIntentArmed {
						t.Fatalf("restart unexpected intent=%+v", status)
					}
					if !time.Now().Before(deadline) {
						t.Fatalf("accepted intent did not converge=%+v", status)
					}
					if target != "host" && status.Outcome == storage.DeleteIntentArmed {
						t.Fatal("public authority reopen admitted old Armed without classification")
					}
					time.Sleep(10 * time.Millisecond)
				}
				if _, err := client.Stat(t.Context(), "victim"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("durable completion left original name: %v", err)
				}
				if err := client.Write(t.Context(), "victim", []byte("replacement")); err != nil {
					t.Fatal(err)
				}
				replacement, err := client.Stat(t.Context(), "victim")
				if err != nil || replacement.ID == accepted.Node {
					t.Fatalf("replacement lost object identity=%+v %v", replacement, err)
				}
				encoded, err = os.ReadFile(configPath)
				if err != nil {
					t.Fatal(err)
				}
				var saved smbDeleteCrashConfig
				if err := json.Unmarshal(encoded, &saved); err != nil || saved.Owner != owner {
					t.Fatalf("host lost stable configured owner=%+v %v", saved, err)
				}
				server, export := newSMBDeleteCrashEndpoint(t, client, saved.Owner)
				ctx := WithPrincipal(t.Context(), testPrincipal("fedcba9876543210"))
				if err := export.RecoverDeleteIntents(ctx); err != nil {
					t.Fatal(err)
				}
				acknowledged, err := actions.QueryDeleteIntent(t.Context(), saved.Owner, accepted.Intent)
				if err != nil || acknowledged.Outcome != storage.DeleteIntentUnknown {
					t.Fatalf("restarted host did not ACK old owner=%+v %v", acknowledged, err)
				}
				body, err := client.Read(t.Context(), "victim")
				if err != nil || string(body) != "replacement" {
					t.Fatalf("old recovery touched replacement=%q %v", body, err)
				}
				if status := server.Status(); status.DeleteRecoveryReadyExports != 1 || status.LocalDeleteOwners != 0 || status.DeleteRecoveryCleanupOwners != 0 {
					t.Fatalf("restart retained settlement=%+v", status)
				}
				if err := export.Unpublish(ctx); err != nil {
					t.Fatal(err)
				}
				if err := observer.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
				authority.kill(t)
			})
		}
	}
}
