package storage

import "context"

// VolumeRetirementCheck distinguishes permanent volume deletion from a close
// that can hand durable responsibility to the next authority incarnation.
// CheckVolumeRetirement verifies support throughout the backing chain.
type VolumeRetirementCheck interface {
	CheckVolumeRetirement() error

	// CheckDeletionReady observes healthy authoritative state without mutation.
	// Callers must first fence runtime, transport and native admissions and settle
	// all volatile pins; a successful check does not establish that fence itself.
	// Nonterminal durable imports and accepted quota actions return EBUSY. Unknown
	// authority state preserves its actual error and never proves readiness.
	// Terminal receipts do not pin the volume. Ordinary Close may still succeed
	// with pending durable work preserved for restart and is not deletion proof.
	CheckDeletionReady(context.Context) error
}
