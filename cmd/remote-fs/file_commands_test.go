package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/internal/serviceapi"
	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func TestFileCommandsUseNativeIdentityAndVerifiedTransfers(t *testing.T) {
	ctx := t.Context()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	locks := locking.DefaultOptions()
	store, err := localstore.Open(ctx, localstore.Config{Root: root, Volume: "cli-test", Quota: 10 << 20, QuotaPolicy: sqlite.DurableQuotaPolicy, InitializeQuota: true, Window: sqlite.DefaultWindow(), Maintenance: objectstore.DefaultOptions(), Locks: &locks, InitializeLocks: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	spool := filepath.Join(t.TempDir(), "spool")
	if err = os.Mkdir(spool, 0700); err != nil {
		t.Fatal(err)
	}
	subject := strings.Repeat("b", 32)
	volumeID := strings.Repeat("c", 32)
	options := httprest.DefaultHandlerOptions()
	options.AccessBinding = func(context.Context) (storage.AccessBinding, error) {
		return storage.AccessBinding{SubjectID: subject, AccessID: strings.Repeat("a", 32)}, nil
	}
	options.Transfers = httprest.TransferOptions{Enabled: true, SpoolDir: spool, MaxBytes: 2 << 20, MaxConcurrent: 4, Lifetime: time.Minute,
		ActionNamespace: func(ctx context.Context, key storage.ActionKey) error {
			if key.SubjectID != subject {
				return syscall.EACCES
			}
			return store.RegisterActionNamespace(ctx, storage.ActionNamespace{SubjectID: subject, Sequence: key.Namespace, AcceptUntil: time.Now().Add(time.Hour).Truncate(time.Hour)})
		}}
	handler, err := httprest.NewHandlerWithOptions(store, store.Log(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := handler.Close(cleanup); err != nil {
			t.Error(err)
		}
	}()
	var namespace atomic.Uint64
	mux := http.NewServeMux()
	mux.Handle("/api/volumes/"+volumeID+"/", http.StripPrefix("/api/volumes/"+volumeID, handler))
	mux.HandleFunc("GET /api/service/v1/capabilities", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(serviceapi.Capabilities{FileSession: serviceapi.FileSessionLimitsFromOptions(storage.DefaultFileSessionOptions()), MaxTransferBytes: "2097152"})
	})
	mux.HandleFunc("POST /api/service/v1/action-namespaces", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
		json.NewEncoder(w).Encode(serviceapi.ActionNamespace{SubjectID: subject, Namespace: serviceapi.DecimalFromUint64(namespace.Add(1)), AcceptUntil: time.Now().Add(time.Hour)})
	})
	var managementRevision int64 = 7
	displayName := "before-rename"
	volumeDTO := func() serviceapi.Volume {
		native, err := store.Summary(ctx)
		if err != nil {
			t.Error(err)
		}
		return serviceapi.Volume{ID: volumeID, OwnerUserID: subject, Name: displayName, State: "active", Revision: serviceapi.DecimalFromInt64(managementRevision), CreatedAt: time.Now(), UpdatedAt: time.Now(), Summary: &serviceapi.VolumeSummary{FileCount: serviceapi.DecimalFromUint64(native.FileCount), DirectoryCount: serviceapi.DecimalFromUint64(native.DirectoryCount), UsedBytes: serviceapi.DecimalFromInt64(native.AllocationUsed), QuotaBytes: serviceapi.DecimalFromInt64(native.QuotaBytes), QuotaRevision: serviceapi.DecimalFromUint64(native.QuotaRevision)}}
	}
	completed := func(w http.ResponseWriter, kind string) {
		json.NewEncoder(w).Encode(serviceapi.Operation{ID: strings.Repeat("d", 32), VolumeID: volumeID, Kind: kind, State: "succeeded", Phase: "completed", Revision: "1", Settled: true, CreatedAt: time.Now(), UpdatedAt: time.Now()})
	}
	mux.HandleFunc("GET /api/service/v1/volumes/"+volumeID, func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(volumeDTO()) })
	mux.HandleFunc("PATCH /api/service/v1/volumes/"+volumeID, func(w http.ResponseWriter, r *http.Request) {
		var input serviceapi.RenameVolumeRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		if input.ExpectedRevision != serviceapi.DecimalFromInt64(managementRevision) {
			t.Error("rename used wrong management revision")
		}
		displayName = input.Name
		managementRevision++
		completed(w, "rename")
	})
	mux.HandleFunc("PUT /api/service/v1/volumes/"+volumeID+"/quota", func(w http.ResponseWriter, r *http.Request) {
		var input serviceapi.SetQuotaRequest
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			t.Error(err)
		}
		sequence, err := input.Action.Namespace.Uint64()
		if err != nil {
			t.Error(err)
		}
		revision, err := input.ExpectedRevision.Uint64()
		if err != nil {
			t.Error(err)
		}
		quota, err := input.QuotaBytes.Int64()
		if err != nil {
			t.Error(err)
		}
		if err = options.Transfers.ActionNamespace(r.Context(), storage.ActionKey{SubjectID: subject, Namespace: sequence, Nonce: input.Action.Nonce}); err != nil {
			t.Error(err)
		}
		command := storage.QuotaCommand{Action: storage.ActionKey{SubjectID: subject, Namespace: sequence, Nonce: input.Action.Nonce}, ExpectedRevision: revision, QuotaBytes: quota}
		command.Fingerprint = command.EffectFingerprint()
		receipt, err := store.SetQuota(r.Context(), command)
		if err != nil || receipt.Outcome != storage.DurableActionCompleted {
			t.Errorf("native quota receipt %+v: %v", receipt, err)
			w.WriteHeader(409)
			json.NewEncoder(w).Encode(serviceapi.ErrorResponse{Error: serviceapi.ErrorDetail{Code: "condition_conflict"}})
			return
		}
		managementRevision++
		completed(w, "quota")
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	credentials, record := credentialFixture(t, server.URL)
	client, err := serviceapi.NewClient(server.URL, server.Client(), serviceapi.Credential{ID: record.ID, Secret: record.Secret})
	if err != nil {
		t.Fatal(err)
	}
	c := cliOptions{credentials: credentials, volume: volumeID, json: true, timeout: 5 * time.Second}
	if err = volumeMutation(ctx, client, record, c, "volume rename", "after-rename", "", nil, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if err = volumeMutation(ctx, client, record, c, "volume quota", "", "8MiB", nil, io.Discard, io.Discard); err != nil {
		t.Fatalf("quota after rename: %v", err)
	}
	quotaSummary, err := store.Summary(ctx)
	if err != nil || quotaSummary.QuotaBytes != 8<<20 || quotaSummary.QuotaRevision != 2 || managementRevision != 9 {
		t.Fatalf("independent quota/management revisions %+v %d %v", quotaSummary, managementRevision, err)
	}
	invoke := func(command string, args ...string) string {
		t.Helper()
		var out bytes.Buffer
		if err := fileCommand(ctx, client, record, c, command, args, false, &out, io.Discard); err != nil {
			t.Fatalf("%s: %v", command, err)
		}
		return out.String()
	}
	invoke("files mkdir", "dir")
	payload := bytes.Repeat([]byte("exact-content\x00"), 10000)
	local := filepath.Join(t.TempDir(), "source")
	if err = os.WriteFile(local, payload, 0600); err != nil {
		t.Fatal(err)
	}
	invoke("files upload", local, "dir/file")
	listing := invoke("files ls", "dir")
	if !strings.Contains(listing, `"rawLeafBase64":"ZmlsZQ=="`) {
		t.Fatalf("exact listing %s", listing)
	}
	destination := filepath.Join(t.TempDir(), "download")
	invoke("files download", "dir/file", destination)
	downloaded, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(downloaded, payload) {
		t.Fatalf("download bytes differ: %v", err)
	}
	if err := fileCommand(ctx, client, record, c, "files download", []string{"dir/file", destination}, false, io.Discard, io.Discard); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("existing destination error %v", err)
	}
	session, err := store.NewFileSession(ctx, storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	status, err := session.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	action, err := storage.NewFileActionID(status.ActionEpoch)
	if err != nil {
		t.Fatal(err)
	}
	rootAttr, err := store.Stat(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = session.(storage.NamespaceAccess).MutateName(ctx, storage.NameCommand{Kind: storage.NameSymlink, Action: action, Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: rootAttr.ID}, RawLeaf: []byte("link")}, Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{LinkTarget: []byte("dir/file")}})
	if err != nil {
		t.Fatal(err)
	}
	if err = session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	oldLink, err := store.Stat(ctx, "link")
	if err != nil {
		t.Fatal(err)
	}
	if err = fileCommand(ctx, client, record, c, "files upload", []string{local, "link"}, true, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	replaced, err := store.Stat(ctx, "link")
	if err != nil || replaced.Kind != storage.NodeRegular || replaced.ID == oldLink.ID {
		t.Fatalf("symlink replacement %+v %v", replaced, err)
	}
	targetBytes, err := store.Read(ctx, "dir/file")
	if err != nil || !bytes.Equal(targetBytes, payload) {
		t.Fatalf("symlink target changed: %v", err)
	}
	invoke("files rm", "link")
	invoke("files rename", "dir/file", "dir/renamed")
	if _, err = store.Stat(ctx, "dir/file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("old name persists: %v", err)
	}
	invoke("files rm", "dir/renamed")
	invoke("files rm", "dir")
	summary, err := store.Summary(ctx)
	if err != nil || summary.FileCount != 0 || summary.DirectoryCount != 0 {
		t.Fatalf("native summary %+v %v", summary, err)
	}
	if entries, err := os.ReadDir(spool); err != nil || len(entries) != 0 {
		t.Fatalf("spool residue %v %v", entries, err)
	}
}

type cleanupImport struct {
	detach        storage.ReferenceCloseResult
	err           error
	cancellations int
}

func (*cleanupImport) Receive(context.Context, io.Reader) (storage.ImportSeal, error) {
	return storage.ImportSeal{}, syscall.ENOSYS
}
func (*cleanupImport) Publish(context.Context) (storage.ImportReceipt, error) {
	return storage.ImportReceipt{}, syscall.ENOSYS
}
func (i *cleanupImport) Detach(context.Context) (storage.ReferenceCloseResult, error) {
	return i.detach, i.err
}
func (i *cleanupImport) Cancel(context.Context) (storage.ReferenceCloseResult, error) {
	i.cancellations++
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}
func TestUnknownReceiveCleanupPreservesSealedDurableImport(t *testing.T) {
	for _, test := range []struct {
		name     string
		detached storage.ReferenceCloseResult
		err      error
		cancel   bool
	}{{"sealed", storage.ReferenceCloseResult{Released: true, Determined: true}, nil, false}, {"unknown", storage.ReferenceCloseResult{}, syscall.EIO, false}, {"unsealed", storage.ReferenceCloseResult{Determined: true}, syscall.EBUSY, true}} {
		t.Run(test.name, func(t *testing.T) {
			file := &cleanupImport{detach: test.detached, err: test.err}
			err := settleImport(t.Context(), file)
			if (file.cancellations == 1) != test.cancel {
				t.Fatal("cleanup abandoned unproven sealed work")
			}
			if test.name == "unknown" && !errors.Is(err, syscall.EIO) {
				t.Fatal("unknown cleanup swallowed")
			}
		})
	}
}
