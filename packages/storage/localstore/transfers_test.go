package localstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/localstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

const transferTestSubject = "transfer-owner"

func transferConfig(root string) localstore.Config {
	config := testConfig(root)
	config.QuotaPolicy = sqlite.DurableQuotaPolicy
	config.InitializeQuota = true
	config.Maintenance = objectstore.DefaultOptions()
	return config
}

func registerTransferNamespace(t *testing.T, store *localstore.Store) {
	t.Helper()
	if err := store.RegisterActionNamespace(t.Context(), storage.ActionNamespace{
		SubjectID: transferTestSubject, Sequence: 1, AcceptUntil: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
}

func newTransferStore(t *testing.T) *localstore.Store {
	t.Helper()
	store := open(t, transferConfig(privateRoot(t)))
	t.Cleanup(func() { closeStore(t, store) })
	if err := store.CheckFileTransfer(); err != nil {
		t.Fatal(err)
	}
	registerTransferNamespace(t, store)
	return store
}

func transferCommand(t *testing.T, store *localstore.Store, leaf, nonce string, payload []byte, expected uint64) storage.ImportCommand {
	t.Helper()
	root, err := store.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	command := storage.ImportCommand{
		Action: storage.ActionKey{SubjectID: transferTestSubject, Namespace: 1, Nonce: nonce},
		Target: storage.ChildSelection{Name: storage.ChildName{
			Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(leaf),
		}},
		Expected: storage.ChildCondition{State: storage.Absent}, Mode: storage.ImportCreate,
		DeclaredBytes: int64(len(payload)), Digest: sha256.Sum256(payload),
	}
	if expected != 0 {
		command.Mode = storage.ImportReplaceName
		command.Expected = storage.ChildCondition{State: storage.SameNode, NodeID: expected}
	}
	command.Fingerprint = command.EffectFingerprint()
	return command
}

func beginTestImport(t *testing.T, ctx context.Context, store *localstore.Store, command storage.ImportCommand) storage.FileImport {
	t.Helper()
	file, err := store.BeginImport(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		result, err := file.Cancel(context.WithoutCancel(ctx))
		if err != nil || !result.Determined || !result.Released {
			t.Errorf("cancel transfer during cleanup = %+v, %v", result, err)
		}
	})
	return file
}

func assertTransferSummary(t *testing.T, store *localstore.Store, files, directories uint64, used int64) {
	t.Helper()
	summary, err := store.Summary(t.Context())
	if err != nil || summary.FileCount != files || summary.DirectoryCount != directories || summary.AllocationUsed != used {
		t.Fatalf("summary = %+v, %v; want files=%d directories=%d used=%d", summary, err, files, directories, used)
	}
}

func TestLocalstoreImportPublishesOneNamedFileAndReplaysOriginalReceipt(t *testing.T) {
	store := newTransferStore(t)
	payload := []byte("published only after the native commit")
	command := transferCommand(t, store, "artifact", "create", payload, 0)
	file := beginTestImport(t, t.Context(), store, command)
	assertTransferSummary(t, store, 0, 0, 0)
	if entries, err := store.List(t.Context(), ""); err != nil || len(entries) != 0 {
		t.Fatalf("prepared import leaked a named node: %+v, %v", entries, err)
	}
	seal, err := file.Receive(t.Context(), bytes.NewReader(payload))
	if err != nil || seal.Bytes != int64(len(payload)) || seal.Digest != command.Digest {
		t.Fatalf("Receive = %+v, %v", seal, err)
	}
	assertTransferSummary(t, store, 0, 0, 0)
	if _, err := store.Stat(t.Context(), "artifact"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("sealed import became visible before publication: %v", err)
	}
	receipt, err := file.Publish(t.Context())
	if err != nil || receipt.Check() != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("Publish = %+v, %v", receipt, err)
	}
	assertTransferSummary(t, store, 1, 0, receipt.Attr.AllocationSize)
	if data, err := store.Read(t.Context(), "artifact"); err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("published data = %q, %v", data, err)
	}
	if err := store.Write(t.Context(), "artifact", []byte("later content")); err != nil {
		t.Fatal(err)
	}
	queried, err := store.QueryImport(t.Context(), command.Action)
	if err != nil || queried.Attr.ID != receipt.Attr.ID || queried.Attr.Size != receipt.Attr.Size || queried.Outcome != storage.DurableActionCompleted {
		t.Fatalf("original receipt changed after a later write: %+v, %v", queried, err)
	}
	replay := beginTestImport(t, t.Context(), store, command)
	replayed, err := replay.Publish(t.Context())
	if err != nil || replayed.Attr.ID != receipt.Attr.ID || replayed.Attr.Size != receipt.Attr.Size {
		t.Fatalf("replayed publication = %+v, %v", replayed, err)
	}
	if data, err := store.Read(t.Context(), "artifact"); err != nil || string(data) != "later content" {
		t.Fatalf("replay overwrote later contents: %q, %v", data, err)
	}
	if store.LiveTransferCount() != 0 {
		t.Fatal("completed action replay retained transfer ownership")
	}
	changed := command
	changed.DeclaredBytes++
	changed.Fingerprint = changed.EffectFingerprint()
	if _, err := store.BeginImport(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("same action with changed intent = %v, want EINVAL", err)
	}
}

func TestLocalstoreImportReplacementRetainsOldOpenIdentityAndQuota(t *testing.T) {
	store := newTransferStore(t)
	old := []byte("old data")
	if err := store.Write(t.Context(), "artifact", old); err != nil {
		t.Fatal(err)
	}
	session, err := store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("Close session: %v", err)
		}
	})
	held, err := session.OpenFile(t.Context(), "artifact", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	oldAttr, err := held.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	replacement := []byte("replacement data")
	command := transferCommand(t, store, "artifact", "replace", replacement, oldAttr.ID)
	file := beginTestImport(t, t.Context(), store, command)
	if _, err := file.Receive(t.Context(), bytes.NewReader(replacement)); err != nil {
		t.Fatal(err)
	}
	receipt, err := file.Publish(t.Context())
	if err != nil || receipt.Outcome != storage.DurableActionCompleted || receipt.Attr.ID == oldAttr.ID {
		t.Fatalf("replace-name publication = %+v, %v", receipt, err)
	}
	assertTransferSummary(t, store, 1, 0, oldAttr.AllocationSize+receipt.Attr.AllocationSize)
	read, err := held.ReadAt(t.Context(), 0, len(old))
	if err != nil || !bytes.Equal(read.Data, old) {
		t.Fatalf("old reference read replacement bytes: %q, %v", read.Data, err)
	}
	if current, err := held.Stat(t.Context()); err != nil || current.ID != oldAttr.ID {
		t.Fatalf("old reference identity changed: %+v, %v", current, err)
	}
	if data, err := store.Read(t.Context(), "artifact"); err != nil || !bytes.Equal(data, replacement) {
		t.Fatalf("current name did not receive replacement: %q, %v", data, err)
	}
	if err := held.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertTransferSummary(t, store, 1, 0, receipt.Attr.AllocationSize)
}

func TestLocalstoreImportReplacementCannotDiscountRetainedOldAllocation(t *testing.T) {
	config := transferConfig(privateRoot(t))
	config.Quota = 4096
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	registerTransferNamespace(t, store)
	payload := bytes.Repeat([]byte{'a'}, 3000)
	if err := store.Write(t.Context(), "artifact", payload); err != nil {
		t.Fatal(err)
	}
	session, err := store.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(context.WithoutCancel(t.Context())); err != nil {
			t.Errorf("Close session: %v", err)
		}
	})
	held, err := session.OpenFile(t.Context(), "artifact", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	if err != nil {
		t.Fatal(err)
	}
	attr, err := held.Stat(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	replacement := bytes.Repeat([]byte{'b'}, 3000)
	command := transferCommand(t, store, "artifact", "quota-denied", replacement, attr.ID)
	file := beginTestImport(t, t.Context(), store, command)
	if _, err := file.Receive(t.Context(), bytes.NewReader(replacement)); err != nil {
		t.Fatal(err)
	}
	receipt, err := file.Publish(t.Context())
	if !errors.Is(err, syscall.EDQUOT) || receipt.Outcome != storage.DurableActionNotExecuted {
		t.Fatalf("retained replacement exceeded quota: %+v, %v", receipt, err)
	}
	assertTransferSummary(t, store, 1, 0, attr.AllocationSize)
	if data, err := store.Read(t.Context(), "artifact"); err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("quota refusal changed existing content: %q, %v", data, err)
	}
	if err := held.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	allowed := beginTestImport(t, t.Context(), store, transferCommand(t, store, "artifact", "quota-allowed", replacement, attr.ID))
	if _, err := allowed.Receive(t.Context(), bytes.NewReader(replacement)); err != nil {
		t.Fatal(err)
	}
	receipt, err = allowed.Publish(t.Context())
	if err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("replacement after old reference settled: %+v, %v", receipt, err)
	}
	assertTransferSummary(t, store, 1, 0, receipt.Attr.AllocationSize)
}

func TestLocalstoreExportPinsCapturedKeyAcrossOverwriteUnlinkAndSweep(t *testing.T) {
	store := newTransferStore(t)
	original := []byte("captured old content")
	if err := store.Write(t.Context(), "artifact", original); err != nil {
		t.Fatal(err)
	}
	attr, err := store.Stat(t.Context(), "artifact")
	if err != nil {
		t.Fatal(err)
	}
	export, err := store.ExportFile(t.Context(), storage.ExportCommand{NodeID: attr.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		result, err := export.CloseWithResult(context.WithoutCancel(t.Context()))
		if err != nil || !result.Released || !result.Determined {
			t.Errorf("Close export: %+v, %v", result, err)
		}
	})
	if info := export.Info(); info.Attr.ID != attr.ID || info.Attr.Size != int64(len(original)) {
		t.Fatalf("captured export info = %+v", info)
	}
	current := []byte("new")
	if err := store.Write(t.Context(), "artifact", current); err != nil {
		t.Fatal(err)
	}
	currentAttr, err := store.Stat(t.Context(), "artifact")
	if err != nil {
		t.Fatal(err)
	}
	assertTransferSummary(t, store, 1, 0, currentAttr.AllocationSize)
	if _, err := store.Sweep(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := export.WriteTo(t.Context(), &output); err != nil || !bytes.Equal(output.Bytes(), original) {
		t.Fatalf("export after overwrite and GC = %q, %v", output.Bytes(), err)
	}
	if info := export.Info(); info.Digest != sha256.Sum256(original) {
		t.Fatalf("verified captured digest = %x, want %x", info.Digest, sha256.Sum256(original))
	}
	if err := store.Remove(t.Context(), "artifact"); err != nil {
		t.Fatal(err)
	}
	assertTransferSummary(t, store, 0, 0, currentAttr.AllocationSize)
	if _, err := store.Sweep(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	if err := export.WriteTo(t.Context(), &output); err != nil || !bytes.Equal(output.Bytes(), original) {
		t.Fatalf("export after unlink and GC = %q, %v", output.Bytes(), err)
	}
	result, err := export.CloseWithResult(t.Context())
	if err != nil || !result.Released || !result.Determined {
		t.Fatalf("Close export = %+v, %v", result, err)
	}
	assertTransferSummary(t, store, 0, 0, 0)
	if _, err := store.Sweep(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	if err := export.WriteTo(t.Context(), &output); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("closed export remained usable: %v", err)
	}
	status, err := store.Status(t.Context())
	if err != nil || status.Objects.GarbageCount != 0 || status.Objects.ReservedCount != 0 || status.Objects.UnresolvedCount != 0 {
		t.Fatalf("pins or garbage remained after export settlement: %+v, %v", status.Objects, err)
	}
}

func TestLocalstoreFailedReceiveLeavesNoNamedFileAndCanBeCanceled(t *testing.T) {
	for _, name := range []string{"short", "long", "digest"} {
		t.Run(name, func(t *testing.T) {
			store := newTransferStore(t)
			payload := []byte("expected content")
			command := transferCommand(t, store, "artifact", name, payload, 0)
			incoming := payload
			switch name {
			case "short":
				incoming = incoming[:len(incoming)-1]
			case "long":
				incoming = append(append([]byte(nil), incoming...), 0)
			case "digest":
				incoming = bytes.Repeat([]byte{'x'}, len(incoming))
			}
			file := beginTestImport(t, t.Context(), store, command)
			if seal, err := file.Receive(t.Context(), bytes.NewReader(incoming)); err == nil || seal != (storage.ImportSeal{}) {
				t.Fatalf("invalid Receive = %+v, %v", seal, err)
			}
			if _, err := file.Publish(t.Context()); err == nil {
				t.Fatal("failed receive was published")
			}
			assertTransferSummary(t, store, 0, 0, 0)
			if entries, err := store.List(t.Context(), ""); err != nil || len(entries) != 0 {
				t.Fatalf("failed receive leaked names: %+v, %v", entries, err)
			}
			result, err := file.Cancel(t.Context())
			if err != nil || !result.Released || !result.Determined {
				t.Fatalf("Cancel = %+v, %v", result, err)
			}
			if store.LiveTransferCount() != 0 {
				t.Fatal("canceled receive retained transfer ownership")
			}
		})
	}
}

func TestLocalstoreImportChecksNameAgainAtFinalPublication(t *testing.T) {
	store := newTransferStore(t)
	command := transferCommand(t, store, "artifact", "conflict", []byte("upload"), 0)
	file := beginTestImport(t, t.Context(), store, command)
	if _, err := file.Receive(t.Context(), bytes.NewReader([]byte("upload"))); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(t.Context(), "artifact", []byte("concurrent owner")); err != nil {
		t.Fatal(err)
	}
	receipt, err := file.Publish(t.Context())
	if !errors.Is(err, syscall.EAGAIN) || receipt.Outcome != storage.DurableActionNotExecuted {
		t.Fatalf("conflicting Publish = %+v, %v", receipt, err)
	}
	if data, err := store.Read(t.Context(), "artifact"); err != nil || string(data) != "concurrent owner" {
		t.Fatalf("conflicting publication overwrote selected name: %q, %v", data, err)
	}
	queried, err := store.QueryImport(t.Context(), command.Action)
	if !errors.Is(err, syscall.EAGAIN) || queried.Outcome != storage.DurableActionNotExecuted || queried.Failure != syscall.EAGAIN {
		t.Fatalf("conflict receipt = %+v, %v", queried, err)
	}
}

func TestLocalstoreClosePreservesAdmissionUntilTransfersSettle(t *testing.T) {
	config := transferConfig(privateRoot(t))
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	registerTransferNamespace(t, store)
	if err := store.Write(t.Context(), "existing", []byte("preserved")); err != nil {
		t.Fatal(err)
	}
	attr, err := store.Stat(t.Context(), "existing")
	if err != nil {
		t.Fatal(err)
	}
	file := beginTestImport(t, t.Context(), store, transferCommand(t, store, "pending", "pending", []byte("pending"), 0))
	export, err := store.ExportFile(t.Context(), storage.ExportCommand{NodeID: attr.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		result, err := export.CloseWithResult(context.WithoutCancel(t.Context()))
		if err != nil || !result.Released || !result.Determined {
			t.Errorf("close export: %+v, %v", result, err)
		}
	})
	if err := store.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("Close with transfers = %v, want EBUSY", err)
	}
	if data, err := store.Read(t.Context(), "existing"); err != nil || string(data) != "preserved" {
		t.Fatalf("busy Close closed ordinary admission: %q, %v", data, err)
	}
	contender, err := localstore.Open(t.Context(), config)
	if contender != nil {
		closeStore(t, contender)
	}
	if !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("busy Close released root ownership: %v", err)
	}
	if result, err := file.Cancel(t.Context()); err != nil || !result.Released {
		t.Fatalf("settle import = %+v, %v", result, err)
	}
	if err := store.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("Close with remaining export = %v, want EBUSY", err)
	}
	if result, err := export.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("settle export = %+v, %v", result, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	if data, err := reopened.Read(t.Context(), "existing"); err != nil || string(data) != "preserved" {
		t.Fatalf("reopen after settled Close = %q, %v", data, err)
	}
}

func transferBindingContext(t *testing.T, subject, access string) context.Context {
	t.Helper()
	ctx, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: subject, AccessID: access})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestLocalstoreTransferHandlesRejectCrossAccessUse(t *testing.T) {
	store := newTransferStore(t)
	owner := transferBindingContext(t, transferTestSubject, "owner-access")
	otherAccess := transferBindingContext(t, transferTestSubject, "other-access")
	otherSubject := transferBindingContext(t, "another-owner", "owner-access")
	payload := []byte("private transfer")
	command := transferCommand(t, store, "artifact", "bound", payload, 0)
	if _, err := store.BeginImport(otherSubject, command); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("foreign subject accepted owner's action: %v", err)
	}
	file := beginTestImport(t, owner, store, command)
	for index, ctx := range []context.Context{t.Context(), otherAccess, otherSubject} {
		t.Run(fmt.Sprintf("access=%d", index), func(t *testing.T) {
			if _, err := file.Receive(ctx, bytes.NewReader(payload)); !errors.Is(err, syscall.EACCES) {
				t.Fatalf("cross-access Receive = %v", err)
			}
			if _, err := file.Publish(ctx); !errors.Is(err, syscall.EACCES) {
				t.Fatalf("cross-access Publish = %v", err)
			}
			if result, err := file.Cancel(ctx); !errors.Is(err, syscall.EACCES) || result.Released {
				t.Fatalf("cross-access Cancel = %+v, %v", result, err)
			}
		})
	}
	if _, err := file.Receive(owner, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	receipt, err := file.Publish(owner)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("owner Publish = %+v, %v", receipt, err)
	}
	if _, err := store.QueryImport(otherSubject, command.Action); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("foreign subject queried owner's action: %v", err)
	}
	if queried, err := store.QueryImport(otherAccess, command.Action); err != nil || queried.Outcome != storage.DurableActionCompleted {
		t.Fatalf("same subject with another credential lost durable receipt: %+v, %v", queried, err)
	}
	export, err := store.ExportFile(owner, storage.ExportCommand{NodeID: receipt.Attr.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		result, err := export.CloseWithResult(context.WithoutCancel(owner))
		if err != nil || !result.Released || !result.Determined {
			t.Errorf("owner close export: %+v, %v", result, err)
		}
	})
	for _, ctx := range []context.Context{t.Context(), otherAccess, otherSubject} {
		var rejected bytes.Buffer
		if err := export.WriteTo(ctx, &rejected); !errors.Is(err, syscall.EACCES) || rejected.Len() != 0 {
			t.Fatalf("cross-access export wrote %d bytes: %v", rejected.Len(), err)
		}
		if result, err := export.CloseWithResult(ctx); !errors.Is(err, syscall.EACCES) || result.Released {
			t.Fatalf("cross-access export Close = %+v, %v", result, err)
		}
	}
	var output bytes.Buffer
	if err := export.WriteTo(owner, &output); err != nil || !bytes.Equal(output.Bytes(), payload) {
		t.Fatalf("owner export = %q, %v", output.Bytes(), err)
	}
}

const sealedImportRootEnvironment = "REMOTE_FS_LOCALSTORE_SEALED_IMPORT_ROOT"

func TestLocalstoreSealedImportResumesAfterProcessExit(t *testing.T) {
	const nonce = "restart-sealed"
	payload := []byte("sealed bytes survive process exit before publication")
	if root := os.Getenv(sealedImportRootEnvironment); root != "" {
		store := open(t, transferConfig(root))
		registerTransferNamespace(t, store)
		owner := transferBindingContext(t, transferTestSubject, "before-restart")
		command := transferCommand(t, store, "artifact", nonce, payload, 0)
		file, err := store.BeginImport(owner, command)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Receive(owner, bytes.NewReader(payload)); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	config := transferConfig(privateRoot(t))
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLocalstoreSealedImportResumesAfterProcessExit$", "-test.timeout=25s")
	child.Env = append(os.Environ(), sealedImportRootEnvironment+"="+config.Root)
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	child.WaitDelay = time.Second
	if err := child.Run(); err != nil {
		t.Fatalf("seal import in child: %v", err)
	}
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	assertTransferSummary(t, store, 0, 0, 0)
	owner := transferBindingContext(t, transferTestSubject, "after-restart")
	action := storage.ActionKey{SubjectID: transferTestSubject, Namespace: 1, Nonce: nonce}
	file, err := store.ResumeImport(owner, action)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		result, err := file.Cancel(context.WithoutCancel(owner))
		if err != nil || !result.Released || !result.Determined {
			t.Errorf("cancel resumed import: %+v, %v", result, err)
		}
	})
	receipt, err := file.Publish(owner)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("publish resumed sealed import = %+v, %v", receipt, err)
	}
	if data, err := store.Read(t.Context(), "artifact"); err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("resumed sealed content = %q, %v", data, err)
	}
	assertTransferSummary(t, store, 1, 0, receipt.Attr.AllocationSize)
}

func TestLocalstoreSealedDetachAllowsGracefulCloseAndResume(t *testing.T) {
	config := transferConfig(privateRoot(t))
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	registerTransferNamespace(t, store)
	before := transferBindingContext(t, transferTestSubject, "before-graceful-close")
	payload := []byte("sealed bytes retained across a graceful volume close")
	command := transferCommand(t, store, "artifact", "graceful-detach", payload, 0)
	file, err := store.BeginImport(before, command)
	if err != nil {
		t.Fatal(err)
	}
	detached := false
	t.Cleanup(func() {
		if !detached {
			result, err := file.Cancel(context.WithoutCancel(before))
			if err != nil || !result.Released || !result.Determined {
				t.Errorf("cancel unfinished import: %+v, %v", result, err)
			}
		}
	})
	if _, err := file.Receive(before, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if result, err := file.Detach(before); err != nil || !result.Released || !result.Determined {
		t.Fatalf("detach sealed import = %+v, %v", result, err)
	}
	detached = true
	if store.LiveTransferCount() != 0 {
		t.Fatal("sealed detach retained volatile transfer ownership")
	}
	if err := store.CheckVolumeRetirement(); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckDeletionReady(before); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("sealed durable work did not block permanent deletion: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("graceful close with sealed detached import: %v", err)
	}
	config.InitializeQuota = false
	reopened := open(t, config)
	t.Cleanup(func() { closeStore(t, reopened) })
	assertTransferSummary(t, reopened, 0, 0, 0)
	after := transferBindingContext(t, transferTestSubject, "after-graceful-close")
	resumed, err := reopened.ResumeImport(after, command.Action)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		result, err := resumed.Cancel(context.WithoutCancel(after))
		if err != nil || !result.Released || !result.Determined {
			t.Errorf("cancel resumed import: %+v, %v", result, err)
		}
	})
	if seal, err := resumed.Receive(after, nil); err != nil || seal.Bytes != command.DeclaredBytes || seal.Digest != command.Digest {
		t.Fatalf("resumed sealed bytes = %+v, %v", seal, err)
	}
	receipt, err := resumed.Publish(after)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("resumed native publication = %+v, %v", receipt, err)
	}
	if data, err := reopened.Read(t.Context(), "artifact"); err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("content after graceful resume = %q, %v", data, err)
	}
	assertTransferSummary(t, reopened, 1, 0, receipt.Attr.AllocationSize)
	if err := reopened.CheckDeletionReady(after); err != nil {
		t.Fatalf("terminal publication receipt pins permanent deletion: %v", err)
	}
}

const (
	unsealedImportRootEnvironment   = "REMOTE_FS_LOCALSTORE_UNSEALED_IMPORT_ROOT"
	unsealedImportExpiryEnvironment = "REMOTE_FS_LOCALSTORE_UNSEALED_IMPORT_EXPIRY"
)

func TestLocalstoreCancelOrphanedUnsealedImportAfterExpiryAndProcessExit(t *testing.T) {
	const nonce = "restart-unsealed-cancel"
	if root := os.Getenv(unsealedImportRootEnvironment); root != "" {
		expiry, err := time.Parse(time.RFC3339Nano, os.Getenv(unsealedImportExpiryEnvironment))
		if err != nil {
			t.Fatal(err)
		}
		store := open(t, transferConfig(root))
		if err := store.RegisterActionNamespace(t.Context(), storage.ActionNamespace{
			SubjectID: transferTestSubject, Sequence: 1, AcceptUntil: expiry,
		}); err != nil {
			t.Fatal(err)
		}
		original := transferBindingContext(t, transferTestSubject, "process-before-expiry")
		command := transferCommand(t, store, "artifact", nonce, []byte("never received"), 0)
		if _, err := store.BeginImport(original, command); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	config := transferConfig(privateRoot(t))
	expiry := time.Now().Add(5 * time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLocalstoreCancelOrphanedUnsealedImportAfterExpiryAndProcessExit$", "-test.timeout=25s")
	child.Env = append(os.Environ(), unsealedImportRootEnvironment+"="+config.Root, unsealedImportExpiryEnvironment+"="+expiry.Format(time.RFC3339Nano))
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	child.WaitDelay = time.Second
	if err := child.Run(); err != nil {
		t.Fatalf("accept unsealed import in child: %v", err)
	}
	timer := time.NewTimer(max(time.Until(expiry)+time.Millisecond, 0))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	config.InitializeQuota = false
	store := open(t, config)
	t.Cleanup(func() { closeStore(t, store) })
	current := transferBindingContext(t, transferTestSubject, "process-after-expiry")
	action := storage.ActionKey{SubjectID: transferTestSubject, Namespace: 1, Nonce: nonce}
	if receipt, err := store.QueryImport(current, action); err != nil || receipt.Outcome != storage.DurableActionPending {
		t.Fatalf("orphaned accepted action = %+v, %v", receipt, err)
	}
	if _, err := store.ResumeImport(current, action); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("new access inherited former unsealed receive ownership: %v", err)
	}
	receipt, err := store.CancelImport(current, action)
	if err != nil || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.ECANCELED || receipt.Action != action || store.LiveTransferCount() != 0 {
		t.Fatalf("cancel expired orphan = %+v, %v; live %d", receipt, err, store.LiveTransferCount())
	}
	if repeated, err := store.CancelImport(current, action); err != nil || repeated.Outcome != receipt.Outcome || repeated.Failure != receipt.Failure {
		t.Fatalf("orphan cancellation replay = %+v, %v", repeated, err)
	}
	if _, err := store.Stat(t.Context(), "artifact"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("unreceived import created a named file: %v", err)
	}
	assertTransferSummary(t, store, 0, 0, 0)
	if err := store.Close(); err != nil {
		t.Fatalf("settled orphan blocked volume close: %v", err)
	}
}
