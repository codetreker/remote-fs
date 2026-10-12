package objectstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/objectstore"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
	"github.com/codetreker/remote-fs/packages/storage/objectstore/memory"
)

type serviceCapabilityParts struct {
	volume  *objectstore.Storage
	meta    *sqlite.LockingStore
	objects *localdisk.Objects
	root    string
	config  sqlite.LockingConfig
}

func serviceCapabilityVolume(t *testing.T, wrap func(*sqlite.LockingStore) metastore.Store) serviceCapabilityParts {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	objects, err := localdisk.Open(t.Context(), root, localdisk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	options := sqlite.DefaultOptions()
	options.QuotaPolicy, options.InitializeQuota = sqlite.DurableQuotaPolicy, true
	config := sqlite.LockingConfig{
		Database: filepath.Join(t.TempDir(), "service.sqlite"), Volume: "service", Allowance: 1 << 20,
		SQLite: options, Locks: locking.DefaultOptions(), Initialize: true,
	}
	meta, err := sqlite.OpenLocking(t.Context(), config)
	if err != nil {
		if closeErr := objects.Close(); closeErr != nil {
			t.Errorf("close objects after metadata opening failed: %v", closeErr)
		}
		t.Fatal(err)
	}
	var backend metastore.Store = meta
	if wrap != nil {
		backend = wrap(meta)
	}
	volume := objectstore.New(objects, backend)
	t.Cleanup(func() {
		if err := volume.Close(); err != nil {
			t.Errorf("close service capability volume: %v", err)
		}
	})
	return serviceCapabilityParts{volume: volume, meta: meta, objects: objects, root: root, config: config}
}

func serviceCapabilityNamespace(t *testing.T, volume *objectstore.Storage) storage.ActionNamespace {
	t.Helper()
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: time.Now().Add(time.Hour)}
	if err := volume.RegisterActionNamespace(t.Context(), namespace); err != nil {
		t.Fatal(err)
	}
	return namespace
}

func serviceCapabilityImport(t *testing.T, volume *objectstore.Storage, name, nonce string, payload []byte, replaced uint64) storage.ImportCommand {
	t.Helper()
	root, err := volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	command := storage.ImportCommand{
		Action:   storage.ActionKey{SubjectID: "owner", Namespace: 1, Nonce: nonce},
		Target:   storage.ChildSelection{Name: storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte(name)}},
		Expected: storage.ChildCondition{State: storage.Absent}, Mode: storage.ImportCreate,
		DeclaredBytes: int64(len(payload)), Digest: sha256.Sum256(payload),
	}
	if replaced != 0 {
		command.Mode = storage.ImportReplaceName
		command.Expected = storage.ChildCondition{State: storage.SameNode, NodeID: replaced}
	}
	command.Fingerprint = command.EffectFingerprint()
	return command
}

func serviceCapabilityBegin(t *testing.T, ctx context.Context, volume *objectstore.Storage, command storage.ImportCommand) storage.FileImport {
	t.Helper()
	file, err := volume.BeginImport(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		result, err := file.Cancel(context.WithoutCancel(ctx))
		if err != nil || !result.Released || !result.Determined {
			t.Errorf("cancel service import: %+v, %v", result, err)
		}
	})
	return file
}

func TestServiceCapabilitiesSummaryAndDurableQuota(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	for name, check := range map[string]func() error{
		"summary": p.volume.CheckVolumeSummary, "quota": p.volume.CheckQuotaControl,
		"namespaces": p.volume.CheckActionNamespaces, "transfer": p.volume.CheckFileTransfer,
	} {
		if err := check(); err != nil {
			t.Fatalf("check %s: %v", name, err)
		}
	}
	initial, err := p.volume.Summary(t.Context())
	if err != nil || initial.FileCount != 0 || initial.DirectoryCount != 0 || initial.AllocationUsed != 0 {
		t.Fatalf("root-only summary = %+v, %v", initial, err)
	}
	if err := p.volume.Mkdir(t.Context(), "directory"); err != nil {
		t.Fatal(err)
	}
	if err := p.volume.Write(t.Context(), "directory/file", []byte("body")); err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, p.volume, storage.DefaultFileSessionOptions())
	root, err := p.volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.(storage.NamespaceAccess).MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameSymlink, Action: fileActionFor(t, session),
		Name:   storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("link")},
		Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{LinkTarget: []byte("directory/file")},
	}); err != nil {
		t.Fatal(err)
	}
	summary, err := p.volume.Summary(t.Context())
	if err != nil || summary.FileCount != 2 || summary.DirectoryCount != 1 || summary.AllocationUsed <= 0 {
		t.Fatalf("named summary = %+v, %v", summary, err)
	}
	namespace := serviceCapabilityNamespace(t, p.volume)
	command := storage.QuotaCommand{
		Action:           storage.ActionKey{SubjectID: namespace.SubjectID, Namespace: namespace.Sequence, Nonce: "lower"},
		ExpectedRevision: summary.QuotaRevision, QuotaBytes: 0,
	}
	command.Fingerprint = command.EffectFingerprint()
	receipt, err := p.volume.SetQuota(t.Context(), command)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted || receipt.QuotaBytes != 0 {
		t.Fatalf("lower quota = %+v, %v", receipt, err)
	}
	replayed, err := p.volume.SetQuota(t.Context(), command)
	if err != nil || replayed != receipt {
		t.Fatalf("quota replay = %+v, %v", replayed, err)
	}
	queried, err := p.volume.QueryQuotaAction(t.Context(), command.Action)
	if err != nil || queried != receipt {
		t.Fatalf("quota query = %+v, %v", queried, err)
	}
	space, err := p.volume.Space(t.Context())
	if err != nil || space.Total != 0 || space.Avail != 0 || space.Used != summary.AllocationUsed {
		t.Fatalf("lowered quota space = %+v, %v", space, err)
	}
	if err := p.volume.Write(t.Context(), "new", []byte("growth")); !errors.Is(err, syscall.EDQUOT) {
		t.Fatalf("growth under zero quota = %v", err)
	}
	if data, err := p.volume.Read(t.Context(), "directory/file"); err != nil || string(data) != "body" {
		t.Fatalf("quota change damaged existing reads: %q, %v", data, err)
	}
}

func TestServiceCapabilitiesQuotaAcceptancePreservesPreviouslyAcceptedWork(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	if err := p.volume.CheckQuotaAcceptance(); err != nil {
		t.Fatal(err)
	}
	initial, err := p.volume.Summary(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	expired := time.Now().Add(-time.Minute)
	command := storage.QuotaCommand{
		Action:           storage.ActionKey{SubjectID: "accepted-owner", Namespace: 1, Nonce: "accepted-before-expiry"},
		ExpectedRevision: initial.QuotaRevision, QuotaBytes: 8192,
	}
	command.Fingerprint = command.EffectFingerprint()
	accepted := storage.AcceptedQuotaAction{
		Command:    command,
		Namespace:  storage.ActionNamespace{SubjectID: command.Action.SubjectID, Sequence: command.Action.Namespace, AcceptUntil: expired},
		AcceptedAt: expired.Add(-time.Minute),
	}
	for _, at := range []time.Time{{}, expired, expired.Add(time.Nanosecond)} {
		invalid := accepted
		invalid.AcceptedAt = at
		if _, err := p.volume.AcceptQuotaAction(t.Context(), invalid); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid acceptance timestamp %v = %v", at, err)
		}
	}
	pending, err := p.volume.AcceptQuotaAction(t.Context(), accepted)
	if err != nil || pending.Outcome != storage.DurableActionPending || pending.Fingerprint != command.Fingerprint {
		t.Fatalf("accept original proof after expiry = %+v, %v", pending, err)
	}
	if repeated, err := p.volume.AcceptQuotaAction(t.Context(), accepted); err != nil || repeated != pending {
		t.Fatalf("acceptance replay = %+v, %v", repeated, err)
	}
	if queried, err := p.volume.QueryQuotaAction(t.Context(), command.Action); err != nil || queried != pending {
		t.Fatalf("pending accepted quota query = %+v, %v", queried, err)
	}
	if current, err := p.volume.Summary(t.Context()); err != nil || current != initial {
		t.Fatalf("acceptance executed policy before native SetQuota: %+v, %v", current, err)
	}
	retirement := storage.ActionNamespaceRetirement{SubjectID: command.Action.SubjectID, Before: 2, RetainAfter: time.Now().Add(time.Second)}
	if err := p.volume.RetireActionNamespaces(t.Context(), retirement); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("pending accepted responsibility was retired: %v", err)
	}
	late := command
	late.Action.Nonce = "unrelated-late-key"
	if _, err := p.volume.SetQuota(t.Context(), late); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("proof admitted unrelated late key: %v", err)
	}
	changedProof := accepted
	changedProof.AcceptedAt = changedProof.AcceptedAt.Add(time.Nanosecond)
	changedEffect := accepted
	changedEffect.Command.QuotaBytes++
	changedEffect.Command.Fingerprint = changedEffect.Command.EffectFingerprint()
	badFingerprint := accepted
	badFingerprint.Command.Fingerprint[0] ^= 1
	for _, changed := range []storage.AcceptedQuotaAction{changedProof, changedEffect, badFingerprint} {
		if _, err := p.volume.AcceptQuotaAction(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("changed acceptance proof or effect was accepted: %v", err)
		}
	}
	completed, err := p.volume.SetQuota(t.Context(), command)
	if err != nil || completed.Outcome != storage.DurableActionCompleted || completed.QuotaBytes != command.QuotaBytes || completed.QuotaRevision != initial.QuotaRevision+1 {
		t.Fatalf("execute accepted quota after expiry = %+v, %v", completed, err)
	}
	if replayed, err := p.volume.SetQuota(t.Context(), command); err != nil || replayed != completed {
		t.Fatalf("accepted execution replay = %+v, %v", replayed, err)
	}
	if replayed, err := p.volume.AcceptQuotaAction(t.Context(), accepted); err != nil || replayed != completed {
		t.Fatalf("completed acceptance replay = %+v, %v", replayed, err)
	}
	if _, err := p.volume.AcceptQuotaAction(t.Context(), changedProof); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("completed receipt permitted proof alteration: %v", err)
	}
	if current, err := p.volume.Summary(t.Context()); err != nil || current.QuotaRevision != completed.QuotaRevision || current.QuotaBytes != command.QuotaBytes {
		t.Fatalf("accepted quota executed more than once: %+v, %v", current, err)
	}
	retirement.RetainAfter = time.Now().Add(time.Second)
	if err := p.volume.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatalf("retire settled accepted quota: %v", err)
	}
	if _, err := p.volume.AcceptQuotaAction(t.Context(), accepted); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("retired acceptance proof revived action: %v", err)
	}
}

func TestServiceCapabilitiesImportReplacementAndPinnedExport(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	serviceCapabilityNamespace(t, p.volume)
	initial := []byte("imported content")
	command := serviceCapabilityImport(t, p.volume, "file", "initial", initial, 0)
	file := serviceCapabilityBegin(t, t.Context(), p.volume, command)
	duplicate, err := p.volume.BeginImport(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	duplicateDetached := false
	t.Cleanup(func() {
		if !duplicateDetached {
			if result, err := duplicate.Cancel(context.WithoutCancel(t.Context())); err != nil || !result.Released {
				t.Errorf("clean duplicate import = %+v, %v", result, err)
			}
		}
	})
	if receipt, err := p.volume.QueryImport(t.Context(), command.Action); err != nil || receipt.Outcome != storage.DurableActionPending {
		t.Fatalf("pending receipt = %+v, %v", receipt, err)
	}
	seal, err := file.Receive(t.Context(), bytes.NewReader(initial))
	if err != nil || seal.Digest != command.Digest || seal.Bytes != command.DeclaredBytes {
		t.Fatalf("receive = %+v, %v", seal, err)
	}
	if sealed, err := file.Receive(t.Context(), nil); err != nil || sealed != seal {
		t.Fatalf("sealed receive replay = %+v, %v", sealed, err)
	}
	receipt, err := file.Publish(t.Context())
	if err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("publish = %+v, %v", receipt, err)
	}
	if result, err := duplicate.Detach(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("duplicate cached pending handle did not observe completed native action: %+v, %v", result, err)
	}
	duplicateDetached = true
	if _, err := file.Receive(t.Context(), bytes.NewReader(initial)); !errors.Is(err, syscall.EALREADY) {
		t.Fatalf("completed import accepted new bytes: %v", err)
	}
	resume, err := p.volume.ResumeImport(t.Context(), command.Action)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := resume.Publish(t.Context())
	if err != nil || replayed.Attr.ID != receipt.Attr.ID || p.volume.LiveTransferCount() != 0 {
		t.Fatalf("terminal resume = %+v, %v; live %d", replayed, err, p.volume.LiveTransferCount())
	}
	repeat := serviceCapabilityBegin(t, t.Context(), p.volume, command)
	if replayed, err := repeat.Publish(t.Context()); err != nil || replayed.Attr.ID != receipt.Attr.ID || p.volume.LiveTransferCount() != 0 {
		t.Fatalf("terminal begin replay = %+v, %v; live %d", replayed, err, p.volume.LiveTransferCount())
	}
	changed := command
	changed.DeclaredBytes++
	changed.Fingerprint = changed.EffectFingerprint()
	if _, err := p.volume.BeginImport(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed action intent = %v", err)
	}
	session := fileSessionFor(t, p.volume, storage.DefaultFileSessionOptions())
	held := openFileFor(t, session, "file", storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Read: true}})
	replacement := []byte("replacement")
	second := serviceCapabilityBegin(t, t.Context(), p.volume, serviceCapabilityImport(t, p.volume, "file", "replace", replacement, receipt.Attr.ID))
	if _, err := second.Receive(t.Context(), bytes.NewReader(replacement)); err != nil {
		t.Fatal(err)
	}
	replaced, err := second.Publish(t.Context())
	if err != nil || replaced.Attr.ID == receipt.Attr.ID || replaced.Outcome != storage.DurableActionCompleted {
		t.Fatalf("replace-name = %+v, %v", replaced, err)
	}
	if old, err := held.ReadAt(t.Context(), 0, len(initial)); err != nil || !bytes.Equal(old.Data, initial) {
		t.Fatalf("old reference after replace-name = %q, %v", old.Data, err)
	}
	summary, err := p.volume.Summary(t.Context())
	if err != nil || summary.FileCount != 1 || summary.AllocationUsed != receipt.Attr.AllocationSize+replaced.Attr.AllocationSize {
		t.Fatalf("retained replacement summary = %+v, %v", summary, err)
	}
	if err := held.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	node, err := p.meta.Stat(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	export, err := p.volume.ExportFile(t.Context(), storage.ExportCommand{NodeID: replaced.Attr.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		result, err := export.CloseWithResult(context.WithoutCancel(t.Context()))
		if err != nil || !result.Released || !result.Determined {
			t.Errorf("close export: %+v, %v", result, err)
		}
	})
	if info := export.Info(); info.Attr.ID != replaced.Attr.ID || info.DigestAvailable {
		t.Fatalf("export info = %+v", info)
	}
	if err := p.volume.Write(t.Context(), "file", []byte("later")); err != nil {
		t.Fatal(err)
	}
	if _, err := p.volume.Sweep(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := export.WriteTo(t.Context(), &output); err != nil || !bytes.Equal(output.Bytes(), replacement) {
		t.Fatalf("captured export after rewrite/GC = %q, %v", output.Bytes(), err)
	}
	if info := export.Info(); !info.DigestAvailable || info.Digest != sha256.Sum256(replacement) {
		t.Fatalf("verified export digest = %+v", info)
	}
	if result, err := export.CloseWithResult(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("close export = %+v, %v", result, err)
	}
	if _, err := p.volume.Sweep(t.Context(), 100); err != nil {
		t.Fatal(err)
	}
	if _, err := p.objects.GetStream(t.Context(), string(node.Content), io.Discard); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("captured key survived confirmed pin release and GC: %v", err)
	}
	if err := export.WriteTo(t.Context(), &output); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("closed export remained usable: %v", err)
	}
}

func TestServiceCapabilitiesFailedPublicationReplayPreservesRefusal(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	serviceCapabilityNamespace(t, p.volume)
	payload := []byte("upload")
	command := serviceCapabilityImport(t, p.volume, "file", "refused", payload, 0)
	file := serviceCapabilityBegin(t, t.Context(), p.volume, command)
	if _, err := file.Receive(t.Context(), bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if err := p.volume.Write(t.Context(), "file", []byte("concurrent content")); err != nil {
		t.Fatal(err)
	}
	receipt, err := file.Publish(t.Context())
	if !errors.Is(err, syscall.EAGAIN) || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.EAGAIN {
		t.Fatalf("condition refusal = %+v, %v", receipt, err)
	}
	for _, replay := range []storage.FileImport{file, serviceCapabilityBegin(t, t.Context(), p.volume, command)} {
		if _, err := replay.Receive(t.Context(), bytes.NewReader(payload)); !errors.Is(err, syscall.EALREADY) {
			t.Fatalf("terminal refusal accepted another stream: %v", err)
		}
		replayed, err := replay.Publish(t.Context())
		if !errors.Is(err, syscall.EAGAIN) || replayed.Outcome != receipt.Outcome || replayed.Failure != receipt.Failure {
			t.Fatalf("failed publication replay = %+v, %v", replayed, err)
		}
	}
	queried, err := p.volume.QueryImport(t.Context(), command.Action)
	if !errors.Is(err, syscall.EAGAIN) || queried.Outcome != receipt.Outcome || queried.Failure != receipt.Failure {
		t.Fatalf("failed publication query = %+v, %v", queried, err)
	}
	if p.volume.LiveTransferCount() != 0 {
		t.Fatalf("terminal refusal retained %d handles", p.volume.LiveTransferCount())
	}
	if data, err := p.volume.Read(t.Context(), "file"); err != nil || string(data) != "concurrent content" {
		t.Fatalf("replay altered concurrent content: %q, %v", data, err)
	}
}

func TestServiceCapabilitiesImportReplacesSymlinkWithoutFollowingTarget(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	serviceCapabilityNamespace(t, p.volume)
	targetBytes := []byte("target must remain untouched")
	if err := p.volume.Write(t.Context(), "target", targetBytes); err != nil {
		t.Fatal(err)
	}
	target, err := p.volume.Stat(t.Context(), "target")
	if err != nil {
		t.Fatal(err)
	}
	root, err := p.volume.Stat(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	session := fileSessionFor(t, p.volume, storage.DefaultFileSessionOptions())
	link, err := session.(storage.NamespaceAccess).MutateName(t.Context(), storage.NameCommand{
		Kind: storage.NameSymlink, Action: fileActionFor(t, session),
		Name:   storage.ChildName{Parent: storage.DirectoryTarget{NodeID: root.ID}, RawLeaf: []byte("link")},
		Target: storage.ChildCondition{State: storage.Absent}, Initial: storage.InitialFields{LinkTarget: []byte("target")},
	})
	if err != nil || link.Attr == nil || link.Attr.Kind != storage.NodeSymlink {
		t.Fatalf("create selected link = %+v, %v", link, err)
	}
	replacement := []byte("new ordinary file")
	command := serviceCapabilityImport(t, p.volume, "link", "replace-link", replacement, link.Attr.ID)
	file := serviceCapabilityBegin(t, t.Context(), p.volume, command)
	if _, err := file.Receive(t.Context(), bytes.NewReader(replacement)); err != nil {
		t.Fatal(err)
	}
	receipt, err := file.Publish(t.Context())
	if err != nil || receipt.Outcome != storage.DurableActionCompleted || receipt.Attr.Kind != storage.NodeRegular || receipt.Attr.ID == link.Attr.ID {
		t.Fatalf("replace selected symlink = %+v, %v", receipt, err)
	}
	if data, err := p.volume.Read(t.Context(), "target"); err != nil || !bytes.Equal(data, targetBytes) {
		t.Fatalf("replacement followed symlink target: %q, %v", data, err)
	}
	if data, err := p.volume.Read(t.Context(), "link"); err != nil || !bytes.Equal(data, replacement) {
		t.Fatalf("replacement did not publish at selected name: %q, %v", data, err)
	}
	summary, err := p.volume.Summary(t.Context())
	if err != nil || summary.FileCount != 2 || summary.DirectoryCount != 0 || summary.AllocationUsed != target.AllocationSize+receipt.Attr.AllocationSize {
		t.Fatalf("symlink replacement summary = %+v, %v", summary, err)
	}
}

func TestServiceCapabilitiesNamespaceRetirementAndMalformedCommands(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	namespace := serviceCapabilityNamespace(t, p.volume)
	if err := p.volume.RegisterActionNamespace(t.Context(), namespace); err != nil {
		t.Fatalf("namespace replay: %v", err)
	}
	changed := namespace
	changed.AcceptUntil = changed.AcceptUntil.Add(time.Second)
	if err := p.volume.RegisterActionNamespace(t.Context(), changed); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("changed namespace registration = %v", err)
	}
	if err := p.volume.RetireActionNamespaces(t.Context(), storage.ActionNamespaceRetirement{SubjectID: "owner", Before: 2, RetainAfter: time.Now()}); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("active namespace retirement = %v", err)
	}
	for name, invoke := range map[string]func() error{
		"register":     func() error { return p.volume.RegisterActionNamespace(t.Context(), storage.ActionNamespace{}) },
		"retire":       func() error { return p.volume.RetireActionNamespaces(t.Context(), storage.ActionNamespaceRetirement{}) },
		"quota":        func() error { _, err := p.volume.SetQuota(t.Context(), storage.QuotaCommand{}); return err },
		"query-quota":  func() error { _, err := p.volume.QueryQuotaAction(t.Context(), storage.ActionKey{}); return err },
		"import":       func() error { _, err := p.volume.BeginImport(t.Context(), storage.ImportCommand{}); return err },
		"resume":       func() error { _, err := p.volume.ResumeImport(t.Context(), storage.ActionKey{}); return err },
		"query-import": func() error { _, err := p.volume.QueryImport(t.Context(), storage.ActionKey{}); return err },
		"export":       func() error { _, err := p.volume.ExportFile(t.Context(), storage.ExportCommand{}); return err },
	} {
		if err := invoke(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("malformed %s = %v", name, err)
		}
	}
	short := storage.ActionNamespace{SubjectID: "retiring", Sequence: 1, AcceptUntil: time.Now().Add(500 * time.Millisecond)}
	if err := p.volume.RegisterActionNamespace(t.Context(), short); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(time.Until(short.AcceptUntil) + time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	retirement := storage.ActionNamespaceRetirement{SubjectID: short.SubjectID, Before: 2, RetainAfter: time.Now()}
	if err := p.volume.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatal(err)
	}
	if err := p.volume.RetireActionNamespaces(t.Context(), retirement); err != nil {
		t.Fatalf("retirement replay: %v", err)
	}
	key := storage.ActionKey{SubjectID: short.SubjectID, Namespace: 1, Nonce: "retired"}
	if receipt, err := p.volume.QueryQuotaAction(t.Context(), key); err == nil || receipt.Outcome != storage.DurableActionHistoryExpired {
		t.Fatalf("retired history = %+v, %v", receipt, err)
	}
}

type serviceBasicMetastore struct{ metastore.Store }

type serviceQuotaWithoutAcceptance struct {
	metastore.Store
	storage.QuotaControl
	accounting interface{ CheckPublicationAccounting() error }
}

func (s serviceQuotaWithoutAcceptance) CheckPublicationAccounting() error {
	return s.accounting.CheckPublicationAccounting()
}

func TestServiceCapabilitiesRequireCompleteBackingChain(t *testing.T) {
	nonNative := serviceCapabilityVolume(t, func(native *sqlite.LockingStore) metastore.Store {
		return serviceBasicMetastore{Store: native}
	})
	for name, invoke := range map[string]func() error{
		"summary-check":          nonNative.volume.CheckVolumeSummary,
		"summary":                func() error { _, err := nonNative.volume.Summary(t.Context()); return err },
		"quota-check":            nonNative.volume.CheckQuotaControl,
		"quota-acceptance-check": nonNative.volume.CheckQuotaAcceptance,
		"quota-acceptance": func() error {
			_, err := nonNative.volume.AcceptQuotaAction(t.Context(), storage.AcceptedQuotaAction{})
			return err
		},
		"quota": func() error { _, err := nonNative.volume.SetQuota(t.Context(), storage.QuotaCommand{}); return err },
		"quota-query": func() error {
			_, err := nonNative.volume.QueryQuotaAction(t.Context(), storage.ActionKey{})
			return err
		},
		"namespace-check":    nonNative.volume.CheckActionNamespaces,
		"namespace-register": func() error { return nonNative.volume.RegisterActionNamespace(t.Context(), storage.ActionNamespace{}) },
		"namespace-retire": func() error {
			return nonNative.volume.RetireActionNamespaces(t.Context(), storage.ActionNamespaceRetirement{})
		},
		"transfer-check": nonNative.volume.CheckFileTransfer,
		"import":         func() error { _, err := nonNative.volume.BeginImport(t.Context(), storage.ImportCommand{}); return err },
		"resume":         func() error { _, err := nonNative.volume.ResumeImport(t.Context(), storage.ActionKey{}); return err },
		"query-import":   func() error { _, err := nonNative.volume.QueryImport(t.Context(), storage.ActionKey{}); return err },
		"export":         func() error { _, err := nonNative.volume.ExportFile(t.Context(), storage.ExportCommand{}); return err },
	} {
		if err := invoke(); !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("missing native chain for %s = %v", name, err)
		}
	}
	nonStreaming, _ := fileVolume(t, memory.New(), 1<<20, func(options *sqlite.Options) {
		options.QuotaPolicy, options.InitializeQuota = sqlite.DurableQuotaPolicy, true
	})
	if err := nonStreaming.CheckFileTransfer(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("nonstreaming object chain = %v", err)
	}
	noAcceptance := serviceCapabilityVolume(t, func(native *sqlite.LockingStore) metastore.Store {
		return serviceQuotaWithoutAcceptance{Store: native, QuotaControl: native, accounting: native}
	})
	if err := noAcceptance.volume.CheckQuotaControl(); err != nil {
		t.Fatalf("quota-only backend lost supported capability: %v", err)
	}
	if err := noAcceptance.volume.CheckQuotaAcceptance(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing native host acceptance capability = %v", err)
	}
	if _, err := noAcceptance.volume.AcceptQuotaAction(t.Context(), storage.AcceptedQuotaAction{}); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing native host acceptance was dispatched: %v", err)
	}
}

func serviceCapabilityAccess(t *testing.T, subject, access string) context.Context {
	t.Helper()
	ctx, err := storage.WithAccessBinding(t.Context(), storage.AccessBinding{SubjectID: subject, AccessID: access})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestServiceCapabilitiesFailedReceiveBindingAndBusyClose(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	serviceCapabilityNamespace(t, p.volume)
	owner := serviceCapabilityAccess(t, "owner", "first")
	other := serviceCapabilityAccess(t, "owner", "second")
	foreign := serviceCapabilityAccess(t, "foreign", "first")
	payload := []byte("expected bytes")
	command := serviceCapabilityImport(t, p.volume, "file", "bound", payload, 0)
	if _, err := p.volume.BeginImport(foreign, command); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("foreign action begin = %v", err)
	}
	file := serviceCapabilityBegin(t, owner, p.volume, command)
	if _, err := p.volume.BeginImport(other, command); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("unsealed begin changed access binding = %v", err)
	}
	if _, err := p.volume.ResumeImport(other, command.Action); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("unsealed resume changed access binding = %v", err)
	}
	if _, err := p.volume.ResumeImport(owner, command.Action); !errors.Is(err, syscall.EBUSY) || p.volume.LiveTransferCount() != 1 {
		t.Fatalf("unsealed resume = %v; live %d, want EBUSY and original handle only", err, p.volume.LiveTransferCount())
	}
	if result, err := file.Detach(owner); !errors.Is(err, syscall.EBUSY) || result.Released {
		t.Fatalf("unsealed detach = %+v, %v", result, err)
	}
	for _, ctx := range []context.Context{t.Context(), other, foreign} {
		if _, err := file.Receive(ctx, bytes.NewReader(payload)); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("cross-access receive = %v", err)
		}
		if _, err := file.Publish(ctx); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("cross-access publish = %v", err)
		}
		if result, err := file.Detach(ctx); !errors.Is(err, syscall.EACCES) || result.Released {
			t.Fatalf("cross-access detach = %+v, %v", result, err)
		}
		if result, err := file.Cancel(ctx); !errors.Is(err, syscall.EACCES) || result.Released {
			t.Fatalf("cross-access cancel = %+v, %v", result, err)
		}
	}
	for _, incoming := range [][]byte{payload[:len(payload)-1], append(append([]byte(nil), payload...), 0), bytes.Repeat([]byte{'x'}, len(payload))} {
		if seal, err := file.Receive(owner, bytes.NewReader(incoming)); err == nil || seal != (storage.ImportSeal{}) {
			t.Fatalf("invalid receive = %+v, %v", seal, err)
		}
		if _, err := file.Publish(owner); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("unsealed publish = %v", err)
		}
	}
	if _, err := p.volume.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
		t.Fatalf("invalid receive leaked a name: %v", err)
	}
	if err := p.volume.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("Close with live import = %v", err)
	}
	if _, err := p.volume.Summary(t.Context()); err != nil {
		t.Fatalf("busy Close poisoned ordinary admission: %v", err)
	}
	if _, err := file.Receive(owner, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	receipt, err := file.Publish(owner)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("valid retry publish = %+v, %v", receipt, err)
	}
	if _, err := p.volume.QueryImport(foreign, command.Action); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("foreign receipt query = %v", err)
	}
	if _, err := p.volume.ResumeImport(foreign, command.Action); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("foreign resume = %v", err)
	}
	missing := storage.ActionKey{SubjectID: "owner", Namespace: 1, Nonce: "unknown"}
	if _, err := p.volume.ResumeImport(owner, missing); !errors.Is(err, syscall.EIO) {
		t.Fatalf("unknown resume = %v", err)
	}
	if queried, err := p.volume.QueryImport(owner, missing); !errors.Is(err, syscall.EIO) || queried.Outcome != storage.DurableActionUnknown {
		t.Fatalf("unknown query = %+v, %v", queried, err)
	}
	export, err := p.volume.ExportFile(owner, storage.ExportCommand{NodeID: receipt.Attr.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if result, err := export.CloseWithResult(context.WithoutCancel(owner)); err != nil || !result.Released {
			t.Errorf("owner export cleanup = %+v, %v", result, err)
		}
	})
	for _, ctx := range []context.Context{t.Context(), other, foreign} {
		var destination bytes.Buffer
		if err := export.WriteTo(ctx, &destination); !errors.Is(err, syscall.EACCES) || destination.Len() != 0 {
			t.Fatalf("cross-access export wrote %d bytes: %v", destination.Len(), err)
		}
		if result, err := export.CloseWithResult(ctx); !errors.Is(err, syscall.EACCES) || result.Released {
			t.Fatalf("cross-access export close = %+v, %v", result, err)
		}
	}
	if err := export.WriteTo(owner, nil); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("nil export destination = %v", err)
	}
	if err := p.volume.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("Close with live export = %v", err)
	}
	if result, err := export.CloseWithResult(owner); err != nil || !result.Released {
		t.Fatalf("settle export = %+v, %v", result, err)
	}
	if p.volume.LiveTransferCount() != 0 {
		t.Fatalf("failed operations retained %d handles", p.volume.LiveTransferCount())
	}
	aborted := serviceCapabilityBegin(t, owner, p.volume, serviceCapabilityImport(t, p.volume, "aborted", "aborted", payload, 0))
	if result, err := aborted.Cancel(owner); err != nil || !result.Released || !result.Determined {
		t.Fatalf("cancel unsealed import = %+v, %v", result, err)
	}
	if _, err := aborted.Receive(owner, bytes.NewReader(payload)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("canceled import accepted bytes: %v", err)
	}
}

func TestServiceCapabilitiesSealedDetachSurvivesGracefulReopen(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	serviceCapabilityNamespace(t, p.volume)
	owner := serviceCapabilityAccess(t, "owner", "before-reopen")
	payload := []byte("sealed but not yet published")
	command := serviceCapabilityImport(t, p.volume, "file", "detached", payload, 0)
	file, err := p.volume.BeginImport(owner, command)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := p.volume.BeginImport(owner, command)
	if err != nil {
		t.Fatal(err)
	}
	detached := false
	duplicateDetached := false
	t.Cleanup(func() {
		if !duplicateDetached {
			if result, err := duplicate.Cancel(context.WithoutCancel(owner)); err != nil || !result.Released {
				t.Errorf("clean unfinished duplicate import = %+v, %v", result, err)
			}
		}
		if !detached {
			if result, err := file.Cancel(context.WithoutCancel(owner)); err != nil || !result.Released {
				t.Errorf("clean unfinished import = %+v, %v", result, err)
			}
		}
	})
	if _, err := file.Receive(owner, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if result, err := file.Detach(owner); err != nil || !result.Released || !result.Determined {
		t.Fatalf("sealed detach = %+v, %v", result, err)
	}
	detached = true
	if p.volume.LiveTransferCount() != 1 {
		t.Fatalf("first detach dropped another handle: live %d", p.volume.LiveTransferCount())
	}
	if result, err := duplicate.Detach(owner); err != nil || !result.Released || !result.Determined {
		t.Fatalf("duplicate cached unsealed handle did not observe durable seal: %+v, %v", result, err)
	}
	duplicateDetached = true
	if result, err := file.Detach(owner); err != nil || !result.Released {
		t.Fatalf("detach replay = %+v, %v", result, err)
	}
	if _, err := file.Receive(owner, bytes.NewReader(payload)); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("detached receive = %v", err)
	}
	if _, err := file.Publish(owner); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("detached publish = %v", err)
	}
	if result, err := file.Cancel(owner); !errors.Is(err, syscall.ESTALE) || !result.Released {
		t.Fatalf("detached cancel = %+v, %v", result, err)
	}
	if p.volume.LiveTransferCount() != 0 {
		t.Fatal("detached import retained volatile ownership")
	}
	if err := p.volume.Close(); err != nil {
		t.Fatal(err)
	}
	objects, err := localdisk.Open(t.Context(), p.root, localdisk.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := objects.Close(); err != nil {
			t.Errorf("close reopened objects: %v", err)
		}
	})
	config := p.config
	config.Initialize, config.SQLite.InitializeQuota = false, false
	meta, err := sqlite.OpenLocking(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	reopened := objectstore.New(objects, meta)
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened volume: %v", err)
		}
	})
	nextAccess := serviceCapabilityAccess(t, "owner", "after-reopen")
	resumed, err := reopened.ResumeImport(nextAccess, command.Action)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if result, err := resumed.Cancel(context.WithoutCancel(nextAccess)); err != nil || !result.Released {
			t.Errorf("clean resumed import = %+v, %v", result, err)
		}
	})
	if seal, err := resumed.Receive(nextAccess, nil); err != nil || seal.Bytes != command.DeclaredBytes || seal.Digest != command.Digest {
		t.Fatalf("restored seal = %+v, %v", seal, err)
	}
	receipt, err := resumed.Publish(nextAccess)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted {
		t.Fatalf("resumed publish = %+v, %v", receipt, err)
	}
	if data, err := reopened.Read(t.Context(), "file"); err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("resumed content = %q, %v", data, err)
	}
}

func TestServiceCapabilitiesTransferFenceKeepsCleanupAndRefusesNewHandles(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	serviceCapabilityNamespace(t, p.volume)
	if err := p.volume.Create(t.Context(), "empty"); err != nil {
		t.Fatal(err)
	}
	empty, err := p.volume.Stat(t.Context(), "empty")
	if err != nil {
		t.Fatal(err)
	}
	export, err := p.volume.ExportFile(t.Context(), storage.ExportCommand{NodeID: empty.ID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if result, err := export.CloseWithResult(context.WithoutCancel(t.Context())); err != nil || !result.Released {
			t.Errorf("close empty export = %+v, %v", result, err)
		}
	})
	var output bytes.Buffer
	if err := export.WriteTo(t.Context(), &output); err != nil || output.Len() != 0 {
		t.Fatalf("empty export = %q, %v", output.Bytes(), err)
	}
	if info := export.Info(); !info.DigestAvailable || info.Digest != sha256.Sum256(nil) || info.Attr.Size != 0 {
		t.Fatalf("empty export seal = %+v", info)
	}
	if result, err := export.CloseWithResult(t.Context()); err != nil || !result.Released {
		t.Fatalf("settle empty export = %+v, %v", result, err)
	}
	command := serviceCapabilityImport(t, p.volume, "pending", "fence-pending", []byte("pending"), 0)
	file := serviceCapabilityBegin(t, t.Context(), p.volume, command)
	if err := p.volume.FenceTransferAdmission(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("fence with retained import = %v", err)
	}
	if _, err := file.Receive(t.Context(), bytes.NewReader([]byte("pending"))); err != nil {
		t.Fatalf("busy fence closed existing transfer admission: %v", err)
	}
	if result, err := file.Cancel(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("cleanup after busy fence = %+v, %v", result, err)
	}
	if err := p.volume.FenceTransferAdmission(); err != nil {
		t.Fatal(err)
	}
	if err := p.volume.FenceTransferAdmission(); err != nil {
		t.Fatalf("repeated fence: %v", err)
	}
	fresh := serviceCapabilityImport(t, p.volume, "new", "after-fence", []byte("new"), 0)
	if _, err := p.volume.BeginImport(t.Context(), fresh); !errors.Is(err, syscall.EIO) {
		t.Fatalf("fenced begin admitted new handle: %v", err)
	}
	if _, err := p.volume.ResumeImport(t.Context(), command.Action); !errors.Is(err, syscall.EIO) {
		t.Fatalf("fenced resume admitted new handle: %v", err)
	}
	if _, err := p.volume.ExportFile(t.Context(), storage.ExportCommand{NodeID: empty.ID}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("fenced export admitted new handle: %v", err)
	}
	if summary, err := p.volume.Summary(t.Context()); err != nil || summary.FileCount != 1 {
		t.Fatalf("fence lost authoritative status access: %+v, %v", summary, err)
	}
}

type serviceExportCloseFault struct {
	*sqlite.LockingStore
	err error
}

func (s *serviceExportCloseFault) CaptureExport(ctx context.Context, command storage.ExportCommand) (metastore.ExportCapture, error) {
	capture, err := s.LockingStore.CaptureExport(ctx, command)
	if err != nil {
		return capture, err
	}
	capture.Reference = &serviceExportCloseReference{ExportReference: capture.Reference, injected: s.err}
	return capture, nil
}

type serviceExportCloseReference struct {
	metastore.ExportReference
	injected error
}

func (r *serviceExportCloseReference) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	result, err := r.ExportReference.CloseWithResult(ctx)
	if err == nil && result.Released && result.Determined && r.injected != nil {
		injected := r.injected
		r.injected = nil
		return result, injected
	}
	return result, err
}

func TestServiceCapabilitiesExportCloseDistinguishesSemanticAndUnsettledErrors(t *testing.T) {
	semantic := errors.New("native semantic close outcome")
	for _, test := range []struct {
		name string
		err  error
		live int
	}{
		{name: "semantic", err: semantic, live: 0},
		{name: "unsettled", err: &storage.CloseSettlementError{State: storage.CloseSettlementPending, Cause: errors.New("adapter settlement pending")}, live: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := serviceCapabilityVolume(t, func(native *sqlite.LockingStore) metastore.Store {
				return &serviceExportCloseFault{LockingStore: native, err: test.err}
			})
			if err := p.volume.Write(t.Context(), "file", []byte("bytes")); err != nil {
				t.Fatal(err)
			}
			attr, err := p.volume.Stat(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			export, err := p.volume.ExportFile(t.Context(), storage.ExportCommand{NodeID: attr.ID})
			if err != nil {
				t.Fatal(err)
			}
			result, err := export.CloseWithResult(t.Context())
			if !errors.Is(err, test.err) || !result.Released || !result.Determined || p.volume.LiveTransferCount() != test.live {
				t.Fatalf("close = %+v, %v; live=%d, want %d", result, err, p.volume.LiveTransferCount(), test.live)
			}
			if test.live != 0 {
				if err := p.volume.Close(); !errors.Is(err, syscall.EBUSY) {
					t.Fatalf("unsettled export permitted storage close: %v", err)
				}
			}
			repeated, err := export.CloseWithResult(t.Context())
			if !repeated.Released || !repeated.Determined || p.volume.LiveTransferCount() != 0 {
				t.Fatalf("retry close = %+v, %v; live %d", repeated, err, p.volume.LiveTransferCount())
			}
			if test.live == 0 && !errors.Is(err, semantic) || test.live != 0 && err != nil {
				t.Fatalf("close retry lost original semantic outcome or retained settled marker: %v", err)
			}
		})
	}
}

type serviceImportCancelFault struct {
	*sqlite.LockingStore
	abandonCalls  atomic.Int32
	queryCalls    atomic.Int32
	prepareCalls  atomic.Int32
	resumeCalls   atomic.Int32
	retireCalls   atomic.Int32
	beforeAbandon func(context.Context, storage.ActionKey) error
	afterAbandon  func() error
	afterQuery    func(storage.ImportReceipt, error) (storage.ImportReceipt, error)
	beforeRetire  func(context.Context, storage.ActionNamespaceRetirement) error
}

func (s *serviceImportCancelFault) PrepareImport(ctx context.Context, command storage.ImportCommand) (metastore.ImportReservation, error) {
	s.prepareCalls.Add(1)
	return s.LockingStore.PrepareImport(ctx, command)
}

func (s *serviceImportCancelFault) ResumeImportReservation(ctx context.Context, key storage.ActionKey) (metastore.ImportReservation, error) {
	s.resumeCalls.Add(1)
	return s.LockingStore.ResumeImportReservation(ctx, key)
}

func (s *serviceImportCancelFault) QueryImportAction(ctx context.Context, key storage.ActionKey) (storage.ImportReceipt, error) {
	s.queryCalls.Add(1)
	receipt, err := s.LockingStore.QueryImportAction(ctx, key)
	if s.afterQuery != nil {
		return s.afterQuery(receipt, err)
	}
	return receipt, err
}

func (s *serviceImportCancelFault) AbandonImport(ctx context.Context, key storage.ActionKey) error {
	s.abandonCalls.Add(1)
	if s.beforeAbandon != nil {
		if err := s.beforeAbandon(ctx, key); err != nil {
			return err
		}
	}
	if err := s.LockingStore.AbandonImport(ctx, key); err != nil {
		return err
	}
	if s.afterAbandon != nil {
		return s.afterAbandon()
	}
	return nil
}

func (s *serviceImportCancelFault) RetireActionNamespaces(ctx context.Context, retirement storage.ActionNamespaceRetirement) error {
	s.retireCalls.Add(1)
	if s.beforeRetire != nil {
		if err := s.beforeRetire(ctx, retirement); err != nil {
			return err
		}
	}
	return s.LockingStore.RetireActionNamespaces(ctx, retirement)
}

func serviceImportCancelVolume(t *testing.T, configure func(*serviceImportCancelFault)) (serviceCapabilityParts, *serviceImportCancelFault) {
	t.Helper()
	var fault *serviceImportCancelFault
	p := serviceCapabilityVolume(t, func(native *sqlite.LockingStore) metastore.Store {
		fault = &serviceImportCancelFault{LockingStore: native}
		if configure != nil {
			configure(fault)
		}
		return fault
	})
	serviceCapabilityNamespace(t, p.volume)
	return p, fault
}

type serviceImportBlockingReader struct {
	reader  io.Reader
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *serviceImportBlockingReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	<-r.release
	return r.reader.Read(p)
}

func TestServiceCapabilitiesCancelImportRejectsLiveReceiveAndPreservesCompletedResult(t *testing.T) {
	p, fault := serviceImportCancelVolume(t, nil)
	owner := serviceCapabilityAccess(t, "owner", "receiving-access")
	current := serviceCapabilityAccess(t, "owner", "current-access")
	foreign := serviceCapabilityAccess(t, "foreign-owner", "current-access")
	payload := []byte("receive owns this action")
	command := serviceCapabilityImport(t, p.volume, "file", "live-cancel", payload, 0)
	file := serviceCapabilityBegin(t, owner, p.volume, command)
	reader := &serviceImportBlockingReader{reader: bytes.NewReader(payload), entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(reader.release) }) }
	defer unblock()
	received := make(chan error, 1)
	go func() { _, err := file.Receive(owner, reader); received <- err }()
	select {
	case <-reader.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Receive did not enter its source reader")
	}
	canceled := make(chan error, 1)
	go func() { _, err := p.volume.CancelImport(current, command.Action); canceled <- err }()
	select {
	case err := <-canceled:
		if !errors.Is(err, syscall.EBUSY) {
			t.Fatalf("cancel live receiving handle = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CancelImport waited behind a live Receive")
	}
	if fault.abandonCalls.Load() != 0 || fault.queryCalls.Load() != 0 {
		t.Fatalf("live cancellation reached native I/O: abandon=%d query=%d", fault.abandonCalls.Load(), fault.queryCalls.Load())
	}
	if _, err := p.volume.CancelImport(foreign, command.Action); !errors.Is(err, syscall.EACCES) || fault.queryCalls.Load() != 0 {
		t.Fatalf("foreign cancellation reached native ownership: %v", err)
	}
	unblock()
	if err := <-received; err != nil {
		t.Fatal(err)
	}
	published, err := file.Publish(owner)
	if err != nil || published.Outcome != storage.DurableActionCompleted {
		t.Fatalf("publish after busy cancellation = %+v, %v", published, err)
	}
	receipt, err := p.volume.CancelImport(current, command.Action)
	if err != nil || receipt.Outcome != storage.DurableActionCompleted || receipt.Attr.ID != published.Attr.ID || receipt.Attr.Size != published.Attr.Size || fault.abandonCalls.Load() != 0 {
		t.Fatalf("completed action cancellation = %+v, %v; native abandons=%d", receipt, err, fault.abandonCalls.Load())
	}
	if data, err := p.volume.Read(t.Context(), "file"); err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("completed publication was undone: %q, %v", data, err)
	}
}

func serviceCapabilitySealAndDetach(t *testing.T, ctx context.Context, volume *objectstore.Storage, command storage.ImportCommand, payload []byte) {
	t.Helper()
	file, err := volume.BeginImport(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	detached := false
	t.Cleanup(func() {
		if !detached {
			if result, err := file.Cancel(context.WithoutCancel(ctx)); err != nil || !result.Released {
				t.Errorf("cancel before orphan preparation completed = %+v, %v", result, err)
			}
		}
	})
	if _, err := file.Receive(ctx, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if result, err := file.Detach(ctx); err != nil || !result.Released || !result.Determined {
		t.Fatalf("retire volatile owner = %+v, %v", result, err)
	}
	detached = true
}

func TestServiceCapabilitiesCancelImportGatesAbandonAgainstNewHandles(t *testing.T) {
	entered, released := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(released) }) }
	defer unblock()
	var target storage.ActionKey
	p, fault := serviceImportCancelVolume(t, func(fault *serviceImportCancelFault) {
		fault.beforeAbandon = func(ctx context.Context, key storage.ActionKey) error {
			if key != target {
				return nil
			}
			enterOnce.Do(func() { close(entered) })
			select {
			case <-released:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	owner := serviceCapabilityAccess(t, "owner", "old-access")
	current := serviceCapabilityAccess(t, "owner", "new-access")
	payload := []byte("orphaned sealed payload")
	command := serviceCapabilityImport(t, p.volume, "file", "cancel-gate", payload, 0)
	target = command.Action
	serviceCapabilitySealAndDetach(t, owner, p.volume, command, payload)
	t.Cleanup(func() {
		unblock()
		if receipt, err := p.volume.CancelImport(context.WithoutCancel(current), command.Action); err != nil || !receipt.Outcome.Terminal() {
			t.Errorf("settle cancellation gate during cleanup = %+v, %v", receipt, err)
		}
	})
	type result struct {
		receipt storage.ImportReceipt
		err     error
	}
	finished := make(chan result, 1)
	go func() {
		receipt, err := p.volume.CancelImport(current, command.Action)
		finished <- result{receipt, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("CancelImport did not reach native abandonment")
	}
	prepared, resumed := fault.prepareCalls.Load(), fault.resumeCalls.Load()
	if _, err := p.volume.BeginImport(owner, command); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("BeginImport crossed cancellation gate: %v", err)
	}
	if _, err := p.volume.ResumeImport(current, command.Action); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("ResumeImport crossed cancellation gate: %v", err)
	}
	if fault.prepareCalls.Load() != prepared || fault.resumeCalls.Load() != resumed {
		t.Fatal("gated handle creation reached native reservation I/O")
	}
	if _, err := p.volume.CancelImport(current, command.Action); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("concurrent same-key cancellation = %v", err)
	}
	otherCommand := serviceCapabilityImport(t, p.volume, "other", "independent-action", []byte("other"), 0)
	other := serviceCapabilityBegin(t, current, p.volume, otherCommand)
	if result, err := other.Cancel(current); err != nil || !result.Released {
		t.Fatalf("other action was blocked by cancellation gate: %+v, %v", result, err)
	}
	if p.volume.LiveTransferCount() != 1 {
		t.Fatalf("cancellation gate lost ownership: live=%d", p.volume.LiveTransferCount())
	}
	if err := p.volume.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("close crossed native abandonment: %v", err)
	}
	unblock()
	final := <-finished
	if final.err != nil || final.receipt.Outcome != storage.DurableActionNotExecuted || final.receipt.Failure != syscall.ECANCELED || p.volume.LiveTransferCount() != 0 {
		t.Fatalf("cancellation result = %+v, %v; live %d", final.receipt, final.err, p.volume.LiveTransferCount())
	}
}

func TestServiceCapabilitiesCancelImportReconcilesLostResponsesWithoutRepeatingAbandon(t *testing.T) {
	for _, phase := range []string{"abandon-response", "terminal-query", "terminal-classified-query"} {
		t.Run(phase, func(t *testing.T) {
			injected := errors.New("injected native cancellation " + phase + " failure")
			var fail atomic.Bool
			fail.Store(true)
			p, fault := serviceImportCancelVolume(t, func(fault *serviceImportCancelFault) {
				if phase == "abandon-response" {
					fault.afterAbandon = func() error {
						if fail.Swap(false) {
							return injected
						}
						return nil
					}
				} else {
					fault.afterQuery = func(receipt storage.ImportReceipt, err error) (storage.ImportReceipt, error) {
						if receipt.Outcome.Terminal() && fail.Swap(false) {
							if phase == "terminal-classified-query" {
								return receipt, serviceCancellationClassifiedIO{cause: injected}
							}
							return receipt, errors.Join(err, injected)
						}
						return receipt, err
					}
				}
			})
			owner := serviceCapabilityAccess(t, "owner", "original-access")
			current := serviceCapabilityAccess(t, "owner", "replacement-access")
			payload := []byte("accepted cleanup responsibility")
			command := serviceCapabilityImport(t, p.volume, "file", phase, payload, 0)
			serviceCapabilitySealAndDetach(t, owner, p.volume, command, payload)
			t.Cleanup(func() {
				if receipt, err := p.volume.CancelImport(context.WithoutCancel(current), command.Action); err != nil || !receipt.Outcome.Terminal() {
					t.Errorf("recover failed cancellation during cleanup = %+v, %v", receipt, err)
				}
			})
			first, err := p.volume.CancelImport(current, command.Action)
			if !errors.Is(err, injected) || p.volume.LiveTransferCount() != 1 || fault.abandonCalls.Load() != 1 {
				t.Fatalf("failed cancellation = %+v, %v; live=%d abandon=%d", first, err, p.volume.LiveTransferCount(), fault.abandonCalls.Load())
			}
			if err := p.volume.Close(); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("unknown cancellation released volume ownership: %v", err)
			}
			prepared, resumed := fault.prepareCalls.Load(), fault.resumeCalls.Load()
			if _, err := p.volume.BeginImport(owner, command); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("unknown cancellation admitted BeginImport: %v", err)
			}
			if _, err := p.volume.ResumeImport(current, command.Action); !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("unknown cancellation admitted ResumeImport: %v", err)
			}
			if fault.prepareCalls.Load() != prepared || fault.resumeCalls.Load() != resumed {
				t.Fatal("unknown gate allowed native reservation I/O")
			}
			receipt, err := p.volume.CancelImport(current, command.Action)
			if err != nil || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.ECANCELED || p.volume.LiveTransferCount() != 0 || fault.abandonCalls.Load() != 1 {
				t.Fatalf("reconciled cancellation = %+v, %v; live=%d abandon=%d", receipt, err, p.volume.LiveTransferCount(), fault.abandonCalls.Load())
			}
			if _, err := p.volume.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
				t.Fatalf("orphan cancellation published a file: %v", err)
			}
		})
	}
}

type serviceCancellationClassifiedIO struct{ cause error }

func (e serviceCancellationClassifiedIO) Error() string {
	return "cancellation classification with unresolved native query I/O"
}
func (e serviceCancellationClassifiedIO) Unwrap() error         { return e.cause }
func (e serviceCancellationClassifiedIO) Classification() error { return syscall.ECANCELED }

func TestServiceCapabilitiesCancelImportSettlesWrappedSemanticCancellation(t *testing.T) {
	for _, phase := range []string{"wrapped", "repeated-join"} {
		t.Run(phase, func(t *testing.T) {
			p, fault := serviceImportCancelVolume(t, func(fault *serviceImportCancelFault) {
				fault.afterQuery = func(receipt storage.ImportReceipt, err error) (storage.ImportReceipt, error) {
					if receipt.Outcome.Terminal() && receipt.Failure == syscall.ECANCELED && err != nil {
						wrapped := fmt.Errorf("native cancellation query: %w", err)
						if phase == "repeated-join" {
							return receipt, errors.Join(err, wrapped, syscall.ECANCELED)
						}
						return receipt, wrapped
					}
					return receipt, err
				}
			})
			owner := serviceCapabilityAccess(t, "owner", "old-access")
			current := serviceCapabilityAccess(t, "owner", "current-access")
			payload := []byte("wrapped semantic cancellation")
			command := serviceCapabilityImport(t, p.volume, "file", phase, payload, 0)
			serviceCapabilitySealAndDetach(t, owner, p.volume, command, payload)
			receipt, err := p.volume.CancelImport(current, command.Action)
			if err != nil || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.ECANCELED || p.volume.LiveTransferCount() != 0 || fault.abandonCalls.Load() != 1 {
				t.Fatalf("wrapped semantic cancellation = %+v, %v; live=%d abandon=%d", receipt, err, p.volume.LiveTransferCount(), fault.abandonCalls.Load())
			}
			if repeated, err := p.volume.CancelImport(current, command.Action); err != nil || repeated.Outcome != receipt.Outcome || repeated.Failure != receipt.Failure || fault.abandonCalls.Load() != 1 {
				t.Fatalf("wrapped terminal query replay = %+v, %v; abandon=%d", repeated, err, fault.abandonCalls.Load())
			}
		})
	}
}

func TestServiceCapabilitiesUnknownCancellationBlocksNamespaceRetirement(t *testing.T) {
	injected := errors.New("lost native abandonment response before namespace retirement")
	var fail atomic.Bool
	fail.Store(true)
	var fault *serviceImportCancelFault
	p := serviceCapabilityVolume(t, func(native *sqlite.LockingStore) metastore.Store {
		fault = &serviceImportCancelFault{LockingStore: native}
		fault.afterAbandon = func() error {
			if fail.Swap(false) {
				return injected
			}
			return nil
		}
		return fault
	})
	expiry := time.Now().Add(2 * time.Second)
	if err := p.volume.RegisterActionNamespace(t.Context(), storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: expiry}); err != nil {
		t.Fatal(err)
	}
	owner := serviceCapabilityAccess(t, "owner", "original-access")
	current := serviceCapabilityAccess(t, "owner", "cleanup-access")
	command := serviceCapabilityImport(t, p.volume, "file", "retirement-protected-cancel", nil, 0)
	serviceCapabilitySealAndDetach(t, owner, p.volume, command, nil)
	settled := false
	t.Cleanup(func() {
		if !settled {
			if receipt, err := p.volume.CancelImport(context.WithoutCancel(current), command.Action); err != nil {
				t.Errorf("settle cancellation before cleanup = %+v, %v", receipt, err)
			}
		}
	})
	if receipt, err := p.volume.CancelImport(current, command.Action); !errors.Is(err, injected) || receipt.Outcome != storage.DurableActionUnknown || p.volume.LiveTransferCount() != 1 {
		t.Fatalf("lost abandonment response = %+v, %v; live %d", receipt, err, p.volume.LiveTransferCount())
	}
	timer := time.NewTimer(max(time.Until(expiry)+time.Millisecond, 0))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	retirement := storage.ActionNamespaceRetirement{SubjectID: "owner", Before: 2, RetainAfter: time.Now().Add(time.Second)}
	if err := p.volume.RetireActionNamespaces(t.Context(), retirement); !errors.Is(err, syscall.EBUSY) || fault.retireCalls.Load() != 0 {
		t.Fatalf("unknown cancellation receipt was exposed to retirement = %v; native retire calls %d", err, fault.retireCalls.Load())
	}
	if err := p.volume.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("unknown cancellation released volume ownership: %v", err)
	}
	receipt, err := p.volume.CancelImport(current, command.Action)
	if err != nil || receipt.Outcome != storage.DurableActionNotExecuted || receipt.Failure != syscall.ECANCELED || p.volume.LiveTransferCount() != 0 || fault.abandonCalls.Load() != 1 {
		t.Fatalf("exact cancellation recovery after expiry = %+v, %v; live=%d abandon=%d", receipt, err, p.volume.LiveTransferCount(), fault.abandonCalls.Load())
	}
	settled = true
	retirement.RetainAfter = time.Now().Add(time.Second)
	if err := p.volume.RetireActionNamespaces(t.Context(), retirement); err != nil || fault.retireCalls.Load() != 1 {
		t.Fatalf("retire settled cancellation = %v; native retire calls %d", err, fault.retireCalls.Load())
	}
	if receipt, err := p.volume.QueryImport(current, command.Action); !errors.Is(err, syscall.ESTALE) || receipt.Outcome != storage.DurableActionHistoryExpired {
		t.Fatalf("retired settled receipt = %+v, %v", receipt, err)
	}
	if err := p.volume.Close(); err != nil {
		t.Fatalf("settled and retired cancellation blocked Close: %v", err)
	}
}

func TestServiceCapabilitiesNamespaceRetirementGatesOnlyMatchingEnrollments(t *testing.T) {
	entered, released := make(chan struct{}), make(chan struct{})
	var enterOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(released) }) }
	defer unblock()
	p, fault := serviceImportCancelVolume(t, func(fault *serviceImportCancelFault) {
		fault.beforeRetire = func(ctx context.Context, retirement storage.ActionNamespaceRetirement) error {
			if retirement.SubjectID != "retiring-owner" {
				return nil
			}
			enterOnce.Do(func() { close(entered) })
			select {
			case <-released:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	if err := p.volume.RegisterActionNamespace(t.Context(), storage.ActionNamespace{SubjectID: "retiring-owner", Sequence: 2, AcceptUntil: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	retirement := storage.ActionNamespaceRetirement{SubjectID: "retiring-owner", Before: 2, RetainAfter: time.Now().Add(time.Second)}
	finished := make(chan error, 1)
	go func() { finished <- p.volume.RetireActionNamespaces(t.Context(), retirement) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("namespace retirement did not enter native callback")
	}
	count := make(chan int, 1)
	go func() { count <- p.volume.LiveTransferCount() }()
	select {
	case live := <-count:
		if live != 1 {
			t.Fatalf("retirement gate ownership = %d, want 1", live)
		}
	case <-time.After(time.Second):
		t.Fatal("native retirement callback held the transfer mutex")
	}
	current := serviceCapabilityAccess(t, "retiring-owner", "current-access")
	matching := serviceCapabilityImport(t, p.volume, "matching", "matching-retirement", nil, 0)
	matching.Action.SubjectID = "retiring-owner"
	matching.Fingerprint = matching.EffectFingerprint()
	prepared, resumed, queried := fault.prepareCalls.Load(), fault.resumeCalls.Load(), fault.queryCalls.Load()
	for name, invoke := range map[string]func() error{
		"begin":  func() error { _, err := p.volume.BeginImport(current, matching); return err },
		"resume": func() error { _, err := p.volume.ResumeImport(current, matching.Action); return err },
		"cancel": func() error { _, err := p.volume.CancelImport(current, matching.Action); return err },
	} {
		result := make(chan error, 1)
		go func() { result <- invoke() }()
		select {
		case err := <-result:
			if !errors.Is(err, syscall.EBUSY) {
				t.Fatalf("matching %s crossed retiring prefix: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("matching %s waited for native retirement", name)
		}
	}
	if fault.prepareCalls.Load() != prepared || fault.resumeCalls.Load() != resumed || fault.queryCalls.Load() != queried {
		t.Fatal("retiring prefix enrollment reached native action I/O")
	}
	if err := p.volume.RetireActionNamespaces(t.Context(), retirement); !errors.Is(err, syscall.EBUSY) || fault.retireCalls.Load() != 1 {
		t.Fatalf("same-subject concurrent retirement = %v; native calls %d", err, fault.retireCalls.Load())
	}
	above := serviceCapabilityImport(t, p.volume, "above-floor", "above-floor", nil, 0)
	above.Action.SubjectID, above.Action.Namespace = "retiring-owner", 2
	above.Fingerprint = above.EffectFingerprint()
	aboveFile := serviceCapabilityBegin(t, current, p.volume, above)
	if result, err := aboveFile.Cancel(current); err != nil || !result.Released {
		t.Fatalf("unrelated namespace was blocked = %+v, %v", result, err)
	}
	otherContext := serviceCapabilityAccess(t, "owner", "other-subject-access")
	other := serviceCapabilityBegin(t, otherContext, p.volume, serviceCapabilityImport(t, p.volume, "other-subject", "other-subject", nil, 0))
	if result, err := other.Cancel(otherContext); err != nil || !result.Released {
		t.Fatalf("unrelated subject was blocked = %+v, %v", result, err)
	}
	if err := p.volume.Close(); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("active retirement allowed Close: %v", err)
	}
	unblock()
	if err := <-finished; err != nil {
		t.Fatalf("native retirement response: %v", err)
	}
	if p.volume.LiveTransferCount() != 0 {
		t.Fatal("completed retirement retained its gate")
	}
	if _, err := p.volume.BeginImport(current, matching); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("completed retirement admitted an older namespace: %v", err)
	}
	if err := p.volume.Close(); err != nil {
		t.Fatalf("completed retirement blocked Close: %v", err)
	}
}

func TestServiceCapabilitiesDeletionReadinessKeepsDurableResponsibility(t *testing.T) {
	p := serviceCapabilityVolume(t, nil)
	serviceCapabilityNamespace(t, p.volume)
	if err := p.volume.CheckVolumeRetirement(); err != nil {
		t.Fatal(err)
	}
	if err := p.volume.CheckDeletionReady(t.Context()); err != nil {
		t.Fatal(err)
	}
	payload := []byte("pending deletion guard")
	command := serviceCapabilityImport(t, p.volume, "pending", "deletion-ready", payload, 0)
	file, err := p.volume.BeginImport(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	detached := false
	t.Cleanup(func() {
		if !detached {
			if _, err := file.Cancel(t.Context()); err != nil {
				t.Error(err)
			}
		}
	})
	if _, err := file.Receive(t.Context(), bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	if result, err := file.Detach(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("detach=%+v,error=%v", result, err)
	}
	detached = true
	if p.volume.LiveTransferCount() != 0 {
		t.Fatal("detach retained volatile transfer")
	}
	if err := p.volume.CheckDeletionReady(t.Context()); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("durable import deletion readiness=%v", err)
	}
	resumed, err := p.volume.ResumeImport(t.Context(), command.Action)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := resumed.Cancel(t.Context()); err != nil {
			t.Error(err)
		}
	})
	if result, err := resumed.Cancel(t.Context()); err != nil || !result.Released || !result.Determined {
		t.Fatalf("cancel=%+v,error=%v", result, err)
	}
	if err := p.volume.CheckDeletionReady(t.Context()); err != nil {
		t.Fatalf("terminal cancellation receipt pins deletion: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := p.volume.CheckDeletionReady(ctx); err == nil {
		t.Fatal("canceled readiness returned success")
	}
	if err := p.volume.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.volume.CheckDeletionReady(t.Context()); err == nil {
		t.Fatal("closed authority returned deletion readiness")
	}
}

type serviceRetirementMissingNative struct {
	metastore.Store
	metastore.FileStore
	metastore.BoundedLister
}

func TestServiceCapabilitiesDeletionReadinessRequiresNativeContract(t *testing.T) {
	p := serviceCapabilityVolume(t, func(native *sqlite.LockingStore) metastore.Store {
		return &serviceRetirementMissingNative{Store: native, FileStore: native, BoundedLister: native}
	})
	if err := p.volume.CheckFileStorage(); err != nil {
		t.Fatalf("valid file backing chain=%v", err)
	}
	if err := p.volume.CheckVolumeRetirement(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing native retirement contract accepted: %v", err)
	}
	if err := p.volume.CheckDeletionReady(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("missing native retirement readiness accepted: %v", err)
	}
}
