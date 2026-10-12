package storage_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestAccessBindingRejectsSameSubjectDifferentAccess(t *testing.T) {
	owner := storage.AccessBinding{SubjectID: "owner", AccessID: "credential-one"}
	ctx, err := storage.WithAccessBinding(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if observed, present := storage.AccessBindingFrom(ctx); !present || observed != owner {
		t.Fatalf("binding = %+v, %v", observed, present)
	}
	if err := storage.RequireAccessBinding(ctx, owner); err != nil {
		t.Fatal(err)
	}
	for _, other := range []storage.AccessBinding{
		{SubjectID: "owner", AccessID: "credential-two"},
		{SubjectID: "another-owner", AccessID: "credential-one"},
		{},
	} {
		if err := storage.RequireAccessBinding(ctx, other); !errors.Is(err, syscall.EACCES) {
			t.Fatalf("expected binding %+v accepted: %v", other, err)
		}
	}
	if err := storage.RequireAccessBinding(t.Context(), owner); !errors.Is(err, syscall.EACCES) {
		t.Fatalf("missing binding accepted: %v", err)
	}
	if err := storage.RequireAccessBinding(t.Context(), storage.AccessBinding{}); err != nil {
		t.Fatal(err)
	}
	if err := storage.RequireAccessBinding(ctx, storage.AccessBinding{SubjectID: "invalid"}); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	for _, invalid := range []storage.AccessBinding{
		{}, {SubjectID: "owner"}, {AccessID: "credential"},
		{SubjectID: strings.Repeat("x", storage.MaxAccessBindingBytes+1), AccessID: "credential"},
		{SubjectID: "owner", AccessID: strings.Repeat("x", storage.MaxAccessBindingBytes+1)},
	} {
		if _, err := storage.WithAccessBinding(t.Context(), invalid); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid binding accepted: %+v, %v", invalid, err)
		}
	}
}

func TestOpaqueIdentitiesRejectLossyJSONSerialization(t *testing.T) {
	for _, invalid := range []string{string([]byte{0xff}), "owner\xc3", string([]byte{0xc0, 0xaf})} {
		for _, key := range []storage.ActionKey{
			{SubjectID: invalid, Namespace: 1, Nonce: "action"},
			{SubjectID: "owner", Namespace: 1, Nonce: invalid},
		} {
			if err := key.Check(); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("non-roundtrippable action key accepted: %+v, %v", key, err)
			}
		}
		for _, binding := range []storage.AccessBinding{
			{SubjectID: invalid, AccessID: "access"},
			{SubjectID: "owner", AccessID: invalid},
		} {
			if _, err := storage.WithAccessBinding(t.Context(), binding); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("non-roundtrippable binding accepted: %+v, %v", binding, err)
			}
		}
	}
	for _, identity := range []string{"owner", "用户", "\ufffd", "e\u0301", "é"} {
		key := storage.ActionKey{SubjectID: identity, Namespace: 1, Nonce: identity}
		if err := key.Check(); err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(key)
		if err != nil {
			t.Fatal(err)
		}
		var decoded storage.ActionKey
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != key {
			t.Fatalf("accepted key lost identity: %+v → %+v, %v", key, decoded, err)
		}
	}
}

func TestDurableQuotaFingerprintFixesEffectAcrossCredentials(t *testing.T) {
	command := storage.QuotaCommand{
		Action:           storage.ActionKey{SubjectID: "owner", Namespace: 1, Nonce: "first"},
		ExpectedRevision: 2, QuotaBytes: 4096,
	}
	command.Fingerprint = command.EffectFingerprint()
	if err := command.Check(); err != nil {
		t.Fatal(err)
	}
	otherKey := command
	otherKey.Action.Nonce = "second"
	if otherKey.EffectFingerprint() != command.Fingerprint {
		t.Fatal("action nonce changed the effect fingerprint")
	}
	for _, mutate := range []func(*storage.QuotaCommand){
		func(c *storage.QuotaCommand) { c.ExpectedRevision++ },
		func(c *storage.QuotaCommand) { c.QuotaBytes++ },
		func(c *storage.QuotaCommand) { c.QuotaBytes = -1 },
		func(c *storage.QuotaCommand) { c.ExpectedRevision = 0 },
		func(c *storage.QuotaCommand) { c.Action.Namespace = 0 },
	} {
		changed := command
		mutate(&changed)
		if err := changed.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("changed quota input accepted: %+v, %v", changed, err)
		}
	}
	command.QuotaBytes = 0
	command.Fingerprint = command.EffectFingerprint()
	if err := command.Check(); err != nil {
		t.Fatalf("hard zero quota rejected: %v", err)
	}
}

func TestAcceptedQuotaProofCannotExtendOrChangeOriginalAdmission(t *testing.T) {
	deadline := time.Unix(1000000, 0)
	proof := storage.AcceptedQuotaAction{
		Command: storage.QuotaCommand{
			Action:           storage.ActionKey{SubjectID: "owner", Namespace: 3, Nonce: "accepted"},
			ExpectedRevision: 2,
			QuotaBytes:       4096,
		},
		Namespace:  storage.ActionNamespace{SubjectID: "owner", Sequence: 3, AcceptUntil: deadline},
		AcceptedAt: deadline.Add(-time.Second),
	}
	proof.Command.Fingerprint = proof.Command.EffectFingerprint()
	if err := proof.Check(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*storage.AcceptedQuotaAction)
	}{
		{"late", func(a *storage.AcceptedQuotaAction) { a.AcceptedAt = deadline.Add(time.Second) }},
		{"at-deadline", func(a *storage.AcceptedQuotaAction) { a.AcceptedAt = deadline }},
		{"missing-time", func(a *storage.AcceptedQuotaAction) { a.AcceptedAt = time.Time{} }},
		{"changed-owner", func(a *storage.AcceptedQuotaAction) { a.Namespace.SubjectID = "other-owner" }},
		{"changed-sequence", func(a *storage.AcceptedQuotaAction) { a.Namespace.Sequence++ }},
		{"missing-namespace", func(a *storage.AcceptedQuotaAction) { a.Namespace.Sequence = 0 }},
		{"changed-command", func(a *storage.AcceptedQuotaAction) { a.Command.QuotaBytes++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := proof
			test.mutate(&changed)
			if err := changed.Check(); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("invalid acceptance proof admitted: %+v, %v", changed, err)
			}
		})
	}
}

func TestDurableActionReceiptsDoNotTurnExpiredHistoryIntoAbsentEffects(t *testing.T) {
	action := storage.ActionKey{SubjectID: "owner", Namespace: 1, Nonce: "action"}
	if !storage.DurableActionCompleted.Terminal() || !storage.DurableActionNotExecuted.Terminal() {
		t.Fatal("determined action outcomes are not terminal")
	}
	for _, outcome := range []storage.DurableActionOutcome{
		storage.DurableActionPending, storage.DurableActionUnknown, storage.DurableActionHistoryExpired,
	} {
		if outcome.Terminal() {
			t.Fatalf("outcome %d proves settlement", outcome)
		}
		if err := (storage.QuotaReceipt{Action: action, Outcome: outcome}).Check(); err != nil {
			t.Fatal(err)
		}
	}
	complete := storage.QuotaReceipt{Action: action, Outcome: storage.DurableActionCompleted, QuotaRevision: 3, QuotaBytes: 0}
	if err := complete.Check(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []storage.QuotaReceipt{
		{}, {Action: action},
		{Action: action, Outcome: storage.DurableActionCompleted},
		{Action: action, Outcome: storage.DurableActionUnknown, QuotaRevision: 3},
		{Action: action, Outcome: storage.DurableActionHistoryExpired, Failure: syscall.ENOENT},
	} {
		if err := invalid.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("contradictory quota receipt accepted: %+v, %v", invalid, err)
		}
	}
	refused := storage.QuotaReceipt{Action: action, Outcome: storage.DurableActionNotExecuted, Failure: syscall.EAGAIN}
	if err := refused.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestNamespaceRegistrationIsBounded(t *testing.T) {
	expiry := time.Unix(123456, 0)
	namespace := storage.ActionNamespace{SubjectID: "owner", Sequence: 1, AcceptUntil: expiry}
	retirement := storage.ActionNamespaceRetirement{SubjectID: "owner", Before: 2, RetainAfter: expiry}
	if err := namespace.Check(); err != nil {
		t.Fatal(err)
	}
	if err := retirement.Check(); err != nil {
		t.Fatal(err)
	}
	if err := (storage.ActionNamespace{}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	if err := (storage.ActionNamespaceRetirement{}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	if err := (storage.VolumeSummaryResult{FileCount: 2, DirectoryCount: 1, AllocationUsed: 4096, QuotaBytes: 0, QuotaRevision: 2}).Check(); err != nil {
		t.Fatalf("lowered quota summary rejected: %v", err)
	}
	if err := (storage.VolumeSummaryResult{}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
}

func TestImportFingerprintRequiresExactObservedNameAndContent(t *testing.T) {
	command := storage.ImportCommand{
		Action: storage.ActionKey{SubjectID: "owner", Namespace: 1, Nonce: "import"},
		Target: storage.ChildSelection{Name: storage.ChildName{
			Parent: storage.DirectoryTarget{NodeID: 1}, RawLeaf: []byte{0xff, 'f'},
		}},
		Expected: storage.ChildCondition{State: storage.Absent}, Mode: storage.ImportCreate,
		DeclaredBytes: 3, Digest: sha256.Sum256([]byte("abc")),
	}
	command.Fingerprint = command.EffectFingerprint()
	if err := command.Check(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*storage.ImportCommand){
		func(c *storage.ImportCommand) { c.Target.Name.RawLeaf = []byte("other") },
		func(c *storage.ImportCommand) { c.Target.Name.Parent.NodeID++ },
		func(c *storage.ImportCommand) { c.DeclaredBytes++ },
		func(c *storage.ImportCommand) { c.Digest[0]++ },
		func(c *storage.ImportCommand) { c.Mode = storage.ImportReplaceName },
		func(c *storage.ImportCommand) { c.Expected = storage.ChildCondition{State: storage.Any} },
		func(c *storage.ImportCommand) { c.Target.Name.RawLeaf = []byte("../file") },
		func(c *storage.ImportCommand) {
			c.Expected = storage.ChildCondition{State: storage.SameNode, NodeID: 2}
		},
		func(c *storage.ImportCommand) { c.Action.Nonce = "" },
	} {
		changed := command
		mutate(&changed)
		if err := changed.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("changed import accepted: %+v, %v", changed, err)
		}
	}
	command.Mode = storage.ImportReplaceName
	command.Expected = storage.ChildCondition{State: storage.SameNode, NodeID: 2}
	command.Fingerprint = command.EffectFingerprint()
	if err := command.Check(); err != nil {
		t.Fatal(err)
	}
	complete := storage.ImportReceipt{Action: command.Action, Outcome: storage.DurableActionCompleted,
		Attr: storage.Attr{ID: 3, Kind: storage.NodeRegular, Size: 3}}
	if err := complete.Check(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []storage.ImportReceipt{
		{}, {Action: command.Action},
		{Action: command.Action, Outcome: storage.DurableActionCompleted},
		{Action: command.Action, Outcome: storage.DurableActionCompleted, Attr: storage.Attr{ID: 3, Kind: storage.NodeSymlink}},
		{Action: command.Action, Outcome: storage.DurableActionUnknown, Attr: complete.Attr},
		{Action: command.Action, Outcome: storage.DurableActionHistoryExpired, Failure: syscall.ENOENT},
	} {
		if err := invalid.Check(); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("invalid import receipt accepted: %+v, %v", invalid, err)
		}
	}
	if err := (storage.ExportCommand{NodeID: 3}).Check(); err != nil {
		t.Fatal(err)
	}
	if err := (storage.ExportCommand{}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	if err := (storage.ExportCommand{NodeID: 3, Scope: &storage.UseScope{}}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
}

func TestCopyTransferContentSealsExactBytesWithBoundedReads(t *testing.T) {
	content := bytes.Repeat([]byte("abcdefgh"), 1<<18)
	reader := &boundedTransferReader{Reader: bytes.NewReader(content)}
	var output bytes.Buffer
	digest := sha256.Sum256(content)
	seal, err := storage.CopyTransferContent(t.Context(), &output, reader, int64(len(content)), digest)
	if err != nil {
		t.Fatal(err)
	}
	if seal.Bytes != int64(len(content)) || seal.Digest != digest || !bytes.Equal(output.Bytes(), content) {
		t.Fatal("successful seal does not describe the exact copied content")
	}
	if reader.maxRead > 64<<10 {
		t.Fatalf("transfer read requested %d bytes", reader.maxRead)
	}
	if _, err := storage.CopyTransferContent(t.Context(), io.Discard, bytes.NewReader(nil), 0, sha256.Sum256(nil)); err != nil {
		t.Fatal(err)
	}
	for _, failure := range []struct {
		name     string
		content  string
		declared int64
		digest   [sha256.Size]byte
		want     error
	}{
		{"short", "ab", 3, sha256.Sum256([]byte("abc")), syscall.EINVAL},
		{"long", "abcd", 3, sha256.Sum256([]byte("abc")), syscall.EFBIG},
		{"digest", "abc", 3, sha256.Sum256([]byte("abd")), syscall.EIO},
		{"negative", "abc", -1, sha256.Sum256([]byte("abc")), syscall.EINVAL},
	} {
		t.Run(failure.name, func(t *testing.T) {
			seal, err := storage.CopyTransferContent(t.Context(), io.Discard, strings.NewReader(failure.content), failure.declared, failure.digest)
			if !errors.Is(err, failure.want) || seal != (storage.ImportSeal{}) {
				t.Fatalf("failed content produced seal %+v, %v", seal, err)
			}
		})
	}
}

func TestCopyTransferContentPropagatesIOAndCancellation(t *testing.T) {
	digest := sha256.Sum256([]byte("abc"))
	marker := errors.New("source unavailable")
	for _, test := range []struct {
		reader io.Reader
		writer io.Writer
		want   error
	}{
		{nil, io.Discard, syscall.EINVAL},
		{strings.NewReader("abc"), nil, syscall.EINVAL},
		{transferReadError{marker}, io.Discard, marker},
		{strings.NewReader("abc"), transferWriteError{marker}, marker},
		{strings.NewReader("abc"), transferShortWrite{}, io.ErrShortWrite},
		{transferNoProgress{}, io.Discard, io.ErrNoProgress},
	} {
		seal, err := storage.CopyTransferContent(t.Context(), test.writer, test.reader, 3, digest)
		if !errors.Is(err, test.want) || seal != (storage.ImportSeal{}) {
			t.Fatalf("I/O failure produced seal %+v, %v", seal, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := storage.CopyTransferContent(ctx, io.Discard, strings.NewReader("abc"), 3, digest); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(t.Context())
	reader := transferCancelReader{cancel: cancel, Reader: strings.NewReader("abc")}
	var output bytes.Buffer
	if _, err := storage.CopyTransferContent(ctx, &output, reader, 3, digest); !errors.Is(err, context.Canceled) || output.Len() != 0 {
		t.Fatalf("canceled receive wrote %d bytes: %v", output.Len(), err)
	}
}

type boundedTransferReader struct {
	io.Reader
	maxRead int
}

func (r *boundedTransferReader) Read(p []byte) (int, error) {
	if len(p) > r.maxRead {
		r.maxRead = len(p)
	}
	return r.Reader.Read(p)
}

type transferReadError struct{ err error }

func (r transferReadError) Read([]byte) (int, error) { return 0, r.err }

type transferWriteError struct{ err error }

func (w transferWriteError) Write([]byte) (int, error) { return 0, w.err }

type transferShortWrite struct{}

func (transferShortWrite) Write([]byte) (int, error) { return 0, nil }

type transferNoProgress struct{}

func (transferNoProgress) Read([]byte) (int, error) { return 0, nil }

type transferCancelReader struct {
	io.Reader
	cancel context.CancelFunc
}

func (r transferCancelReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.cancel()
	return n, err
}
