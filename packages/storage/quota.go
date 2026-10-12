package storage

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"syscall"
	"time"
	"unicode/utf8"
)

const MaxActionIdentityBytes = 128

// ActionKey identifies durable work across credentials and authority restart.
// SubjectID and Namespace are supplied by the embedding authority, not inferred
// from a bearer token. A known key always returns its original effect; a changed
// fingerprint is EINVAL and an expired history never permits reexecution.
// Identity strings are bounded UTF-8 so serialization preserves exact equality.
type ActionKey struct {
	SubjectID string
	Namespace uint64
	Nonce     string
}

func (k ActionKey) Check() error {
	if !boundedOpaque(k.SubjectID, MaxActionIdentityBytes) || k.Namespace == 0 ||
		!boundedOpaque(k.Nonce, MaxActionIdentityBytes) {
		return syscall.EINVAL
	}
	return nil
}

// ActionFingerprint is SHA-256 over all effect-bearing command input. An action
// key names work; its fingerprint prevents that identity from changing meaning.
type ActionFingerprint [sha256.Size]byte

type DurableActionOutcome uint8

const (
	DurableActionPending DurableActionOutcome = iota + 1
	DurableActionCompleted
	DurableActionNotExecuted
	DurableActionUnknown
	DurableActionHistoryExpired
)

// Terminal distinguishes a retained definitive result from pending or unknown
// work. HistoryExpired is not proof that the action was never executed.
func (o DurableActionOutcome) Terminal() bool {
	return o == DurableActionCompleted || o == DurableActionNotExecuted
}

// ActionNamespaces registers trusted admission windows in the same authority
// that owns action receipts. Registration must precede accepting a new key.
// Implementations persist nondecreasing clock and retirement watermarks, reject
// changed registrations, and replay known keys after admission expires.
type ActionNamespaces interface {
	CheckActionNamespaces() error
	RegisterActionNamespace(context.Context, ActionNamespace) error
	RetireActionNamespaces(context.Context, ActionNamespaceRetirement) error
}

type ActionNamespace struct {
	SubjectID   string
	Sequence    uint64
	AcceptUntil time.Time
}

func (n ActionNamespace) Check() error {
	if !boundedOpaque(n.SubjectID, MaxActionIdentityBytes) || n.Sequence == 0 || n.AcceptUntil.IsZero() {
		return syscall.EINVAL
	}
	return nil
}

// ActionNamespaceRetirement retires sequences strictly below Before. Retirement
// and receipt removal are atomic and cannot pass pending/unknown work or results
// younger than RetainAfter. A retired sequence is permanently rejected.
type ActionNamespaceRetirement struct {
	SubjectID   string
	Before      uint64
	RetainAfter time.Time
}

func (r ActionNamespaceRetirement) Check() error {
	if !boundedOpaque(r.SubjectID, MaxActionIdentityBytes) || r.Before == 0 || r.RetainAfter.IsZero() {
		return syscall.EINVAL
	}
	return nil
}

// QuotaControl changes durable policy under the same ordering used by file
// publication. Policy and receipt commit together. A canceled wait cannot prove
// absence of effects; callers query the original Action before new work.
type QuotaControl interface {
	CheckQuotaControl() error
	SetQuota(context.Context, QuotaCommand) (QuotaReceipt, error)
	QueryQuotaAction(context.Context, ActionKey) (QuotaReceipt, error)
}

// QuotaAcceptance bridges a trusted host's previously committed management
// intent into the native authority. Hosts must prove durable acceptance at
// AcceptedAt and must never expose this capability to untrusted clients as a new
// admission path. The authority retains the exact proof and original fingerprint
// and preserves any still-live original admission window. When that window has
// expired, recovery admits only this accepted key; unrelated late keys remain
// rejected and the window never reopens. Changed proofs and retired namespaces
// are rejected. Pending accepted work prevents retirement until it settles.
type QuotaAcceptance interface {
	CheckQuotaAcceptance() error
	AcceptQuotaAction(context.Context, AcceptedQuotaAction) (QuotaReceipt, error)
}

type AcceptedQuotaAction struct {
	Command    QuotaCommand
	Namespace  ActionNamespace
	AcceptedAt time.Time
}

func (a AcceptedQuotaAction) Check() error {
	if err := a.Command.Check(); err != nil {
		return err
	}
	if err := a.Namespace.Check(); err != nil {
		return err
	}
	if a.Command.Action.SubjectID != a.Namespace.SubjectID ||
		a.Command.Action.Namespace != a.Namespace.Sequence ||
		a.AcceptedAt.IsZero() || !a.AcceptedAt.Before(a.Namespace.AcceptUntil) {
		return syscall.EINVAL
	}
	return nil
}

type QuotaCommand struct {
	Action           ActionKey
	ExpectedRevision uint64
	QuotaBytes       int64
	Fingerprint      ActionFingerprint
}

// EffectFingerprint excludes credential identity and the action key. Only the
// expected policy revision and target quota determine this command's effect.
func (c QuotaCommand) EffectFingerprint() ActionFingerprint {
	var fields [16]byte
	binary.BigEndian.PutUint64(fields[:8], c.ExpectedRevision)
	binary.BigEndian.PutUint64(fields[8:], uint64(c.QuotaBytes))
	h := sha256.New()
	_, _ = h.Write([]byte("remote-fs/quota/v1\x00"))
	_, _ = h.Write(fields[:])
	var result ActionFingerprint
	copy(result[:], h.Sum(nil))
	return result
}

func (c QuotaCommand) Check() error {
	if err := c.Action.Check(); err != nil {
		return err
	}
	if c.ExpectedRevision == 0 || c.QuotaBytes < 0 || c.Fingerprint != c.EffectFingerprint() {
		return syscall.EINVAL
	}
	return nil
}

// QuotaReceipt preserves the committed policy, which may differ from the
// current policy after later actions. Failure is present only for a determined
// refusal. Unknown and HistoryExpired always require an operation error.
type QuotaReceipt struct {
	Action        ActionKey
	Fingerprint   ActionFingerprint
	Outcome       DurableActionOutcome
	QuotaBytes    int64
	QuotaRevision uint64
	Failure       syscall.Errno
}

func (r QuotaReceipt) Check() error {
	if err := r.Action.Check(); err != nil {
		return err
	}
	if r.Outcome < DurableActionPending || r.Outcome > DurableActionHistoryExpired {
		return syscall.EINVAL
	}
	if r.Outcome == DurableActionCompleted {
		if r.QuotaBytes < 0 || r.QuotaRevision == 0 || r.Failure != 0 {
			return syscall.EINVAL
		}
	} else if r.QuotaBytes != 0 || r.QuotaRevision != 0 || (r.Failure != 0 && r.Outcome != DurableActionNotExecuted) {
		return syscall.EINVAL
	}
	return nil
}

func boundedOpaque(value string, maxBytes int) bool {
	return len(value) > 0 && len(value) <= maxBytes && utf8.ValidString(value)
}
