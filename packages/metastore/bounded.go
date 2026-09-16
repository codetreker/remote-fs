package metastore

import (
	"bytes"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// MaxChangePayloadBytes bounds the sum of all variable-length fields in one change.
const MaxChangePayloadBytes = 512 << 10

// ChangePayloadLengths declares the variable-length fields of one change before those
// fields are retained in a result. Notification counts its canonical encoded bytes.
type ChangePayloadLengths struct {
	Name         int64
	FromName     int64
	Content      int64
	Metadata     int64
	Target       int64
	Notification int64
}

// Check enforces the producer bound before any payload is loaded or committed.
func (l ChangePayloadLengths) Check() error {
	remaining := int64(MaxChangePayloadBytes)
	for _, length := range []int64{l.Name, l.FromName, l.Content, l.Metadata, l.Target, l.Notification} {
		if length < 0 {
			return fmt.Errorf("negative change payload length: %w", syscall.EIO)
		}
		if length > remaining {
			return fmt.Errorf("change payload exceeds the %d-byte bound: %w", MaxChangePayloadBytes, syscall.EFBIG)
		}
		remaining -= length
	}
	if l.Metadata > storage.MaxMetadataBytes || l.Target > storage.MaxLinkTargetBytes {
		return fmt.Errorf("change node payload exceeds byte bound: %w", syscall.EFBIG)
	}
	if l.Notification > MaxNotificationBytes {
		return fmt.Errorf("notification exceeds encoded byte bound: %w", syscall.EFBIG)
	}
	return nil
}

// ChangeResult retains one page of changes under a caller-defined byte charge. The charge
// describes the representation the caller will build, so the metastore does not depend on
// a transport format.
//
// A producer reserves each change from its scalar fields and payload lengths before loading
// or copying Name, From.Name, Node.Content, Node.Metadata, Node.LinkTarget, or Notification. A result that cannot fit the next change may
// end a non-empty page before it; a change that cannot fit an otherwise empty result fails
// the whole result with EFBIG. Any producer error must be passed to Fail, because a partial
// sequence of changes cannot be presented as the complete answer to Since.
type ChangeResult struct {
	maxBytes    int64
	fixedBytes  int64
	usedBytes   int64
	changeBytes func(index int, change Change, lengths ChangePayloadLengths) (int64, error)
	changes     []Change
	pending     bool
	failure     error
}

// NewChangeResult constructs an empty bounded change page.
func NewChangeResult(
	maxBytes, fixedBytes int64,
	changeBytes func(index int, change Change, lengths ChangePayloadLengths) (int64, error),
) (*ChangeResult, error) {
	if maxBytes < 0 {
		return nil, fmt.Errorf("a change-page byte bound cannot be negative: %w", syscall.EINVAL)
	}
	if fixedBytes < 0 || fixedBytes > maxBytes {
		return nil, fmt.Errorf("an empty change page requires %d bytes under a %d-byte bound: %w", fixedBytes, maxBytes, syscall.EFBIG)
	}
	if changeBytes == nil {
		return nil, fmt.Errorf("a change result needs a change byte charge: %w", syscall.EINVAL)
	}
	return &ChangeResult{
		maxBytes: maxBytes, fixedBytes: fixedBytes, usedBytes: fixedBytes, changeBytes: changeBytes,
	}, nil
}

// Reserve charges one change before its variable-length fields are loaded. The returned
// boolean is false when the change fits an empty result but not the space left in this
// non-empty one; the producer leaves that change for the next call.
func (r *ChangeResult) Reserve(meta Change, lengths ChangePayloadLengths) (*ChangeReservation, bool, error) {
	if r == nil {
		return nil, false, fmt.Errorf("a nil change result cannot retain a change: %w", syscall.EINVAL)
	}
	if r.failure != nil {
		return nil, false, r.failure
	}
	if r.pending {
		return nil, false, r.fail(fmt.Errorf("a change was reserved before the previous reservation was committed: %w", syscall.EIO))
	}
	if err := validateChangeMeta(meta, lengths); err != nil {
		return nil, false, r.fail(err)
	}
	if meta.Name != nil {
		meta.Name = []byte{}
	}
	if meta.From != nil {
		from := *meta.From
		if from.Name != nil {
			from.Name = []byte{}
		}
		meta.From = &from
	}
	if meta.Node != nil {
		node := *meta.Node
		node.Content = ""
		node.Metadata = nil
		node.LinkTarget = nil
		node.CreationTime = boundedTime(node.CreationTime)
		node.ChangeTime = boundedTime(node.ChangeTime)
		node.AccessTime = node.AccessTime.UTC()
		node.ModTime = node.ModTime.UTC()
		meta.Node = &node
	}
	charge, err := r.changeBytes(len(r.changes), meta, lengths)
	if err != nil {
		return nil, false, r.fail(err)
	}
	if charge < 0 {
		return nil, false, r.fail(fmt.Errorf("the change charge at index %d is negative: %w", len(r.changes), syscall.EINVAL))
	}
	if charge > r.maxBytes-r.fixedBytes {
		return nil, false, r.fail(fmt.Errorf("one change requires %d bytes under a %d-byte result bound: %w", charge+r.fixedBytes, r.maxBytes, syscall.EFBIG))
	}
	if charge > r.maxBytes-r.usedBytes {
		return nil, false, nil
	}
	r.usedBytes += charge
	r.pending = true
	return &ChangeReservation{result: r, meta: meta, lengths: lengths}, true, nil
}

func validateChangeMeta(meta Change, lengths ChangePayloadLengths) error {
	if err := lengths.Check(); err != nil {
		return err
	}
	if meta.Notification != nil || len(meta.Name) != 0 || (meta.From != nil && len(meta.From.Name) != 0) || (meta.Node != nil && (len(meta.Node.Content) != 0 || len(meta.Node.Metadata) != 0 || len(meta.Node.LinkTarget) != 0)) {
		return fmt.Errorf("a change reservation already retains variable-length payload: %w", syscall.EINVAL)
	}
	if meta.From == nil && lengths.FromName != 0 {
		return fmt.Errorf("a change without a source declares a %d-byte source name: %w", lengths.FromName, syscall.EIO)
	}
	if meta.Node == nil && (lengths.Content != 0 || lengths.Metadata != 0 || lengths.Target != 0) {
		return fmt.Errorf("a change without a node declares a %d-byte content key: %w", lengths.Content, syscall.EIO)
	}
	return nil
}

// Changes returns the completed page. The returned slice is owned by the result.
func (r *ChangeResult) Changes() ([]Change, error) {
	if r == nil {
		return nil, fmt.Errorf("a nil change result has no completed page: %w", syscall.EINVAL)
	}
	if r.failure != nil {
		return nil, r.failure
	}
	if r.pending {
		return nil, fmt.Errorf("a change payload was reserved but not loaded: %w", syscall.EIO)
	}
	if r.changes == nil {
		return []Change{}, nil
	}
	return r.changes, nil
}

// Fail invalidates every change accumulated by an operation that did not complete.
func (r *ChangeResult) Fail(err error) error {
	if r == nil || err == nil {
		return err
	}
	return r.fail(err)
}

func (r *ChangeResult) fail(err error) error {
	if r.failure == nil {
		r.failure = err
	}
	r.changes = nil
	r.pending = false
	return r.failure
}

// ChangeReservation is one charged change whose variable-length fields have not been
// loaded.
type ChangeReservation struct {
	result    *ChangeResult
	meta      Change
	lengths   ChangePayloadLengths
	committed bool
}

// Commit supplies the fields whose lengths were charged by Reserve.
func (r *ChangeReservation) Commit(name, fromName []byte, content Key, metadata, target, notification []byte) error {
	if r == nil || r.result == nil || r.committed {
		return fmt.Errorf("a change reservation can be committed exactly once: %w", syscall.EINVAL)
	}
	if int64(len(name)) != r.lengths.Name || int64(len(fromName)) != r.lengths.FromName || int64(len(content)) != r.lengths.Content || int64(len(notification)) != r.lengths.Notification || int64(len(metadata)) != r.lengths.Metadata || int64(len(target)) != r.lengths.Target {
		return r.result.fail(fmt.Errorf(
			"a change payload has lengths (%d, %d, %d, %d, %d, %d) after (%d, %d, %d, %d, %d, %d) were reserved: %w",
			len(name), len(fromName), len(content), len(metadata), len(target), len(notification), r.lengths.Name, r.lengths.FromName, r.lengths.Content, r.lengths.Metadata, r.lengths.Target, r.lengths.Notification, syscall.EIO,
		))
	}
	if r.result.failure != nil {
		return r.result.failure
	}
	change := r.meta
	change.Name = bytes.Clone(name)
	if change.From != nil {
		change.From.Name = bytes.Clone(fromName)
	}
	if change.Node != nil {
		change.Node.Content = Key(strings.Clone(string(content)))
		decoded, err := storage.DecodeMetadata(metadata)
		if err != nil {
			return r.result.fail(err)
		}
		change.Node.Metadata = decoded
		change.Node.LinkTarget = bytes.Clone(target)
	}
	if len(notification) != 0 {
		n, err := DecodeNotification(change, notification)
		if err != nil {
			return r.result.fail(err)
		}
		change.Notification = n
	}
	r.committed = true
	r.result.pending = false
	r.result.changes = append(r.result.changes, change)
	return nil
}

// RowPayloadLengths declares the variable-length fields of one snapshot row before they
// are retained in a result.
type RowPayloadLengths struct {
	Name     int64
	Content  int64
	Metadata int64
	Target   int64
}

// RowResult retains one snapshot page under a caller-defined byte charge. Reserve must run
// before names, content keys, opaque metadata, or link targets are loaded. A producer leaves the next row for the next call
// when it fits an empty page but not the remaining space; a single row too large for an
// empty page fails the whole result.
type RowResult struct {
	maxBytes   int64
	fixedBytes int64
	usedBytes  int64
	rowBytes   func(index int, row Row, lengths RowPayloadLengths) (int64, error)
	rows       []Row
	pending    bool
	failure    error
}

// NewRowResult constructs an empty bounded snapshot page.
func NewRowResult(
	maxBytes, fixedBytes int64,
	rowBytes func(index int, row Row, lengths RowPayloadLengths) (int64, error),
) (*RowResult, error) {
	if maxBytes < 0 {
		return nil, fmt.Errorf("a snapshot-page byte bound cannot be negative: %w", syscall.EINVAL)
	}
	if fixedBytes < 0 || fixedBytes > maxBytes {
		return nil, fmt.Errorf("an empty snapshot page requires %d bytes under a %d-byte bound: %w", fixedBytes, maxBytes, syscall.EFBIG)
	}
	if rowBytes == nil {
		return nil, fmt.Errorf("a row result needs a row byte charge: %w", syscall.EINVAL)
	}
	return &RowResult{maxBytes: maxBytes, fixedBytes: fixedBytes, usedBytes: fixedBytes, rowBytes: rowBytes}, nil
}

// Reserve charges one row before its variable-length fields are loaded. The returned
// boolean is false when the row belongs in the next non-empty page.
func (r *RowResult) Reserve(meta Row, lengths RowPayloadLengths) (*RowReservation, bool, error) {
	if r == nil {
		return nil, false, fmt.Errorf("a nil row result cannot retain a row: %w", syscall.EINVAL)
	}
	if r.failure != nil {
		return nil, false, r.failure
	}
	if r.pending {
		return nil, false, r.fail(fmt.Errorf("a row was reserved before the previous reservation was committed: %w", syscall.EIO))
	}
	if err := (ChangePayloadLengths{Name: lengths.Name, Content: lengths.Content, Metadata: lengths.Metadata, Target: lengths.Target}).Check(); err != nil {
		return nil, false, r.fail(err)
	}
	if len(meta.Name) != 0 || len(meta.Node.Content) != 0 || len(meta.Node.Metadata) != 0 || len(meta.Node.LinkTarget) != 0 {
		return nil, false, r.fail(fmt.Errorf("a row reservation already retains variable-length payload: %w", syscall.EINVAL))
	}
	if meta.Name != nil {
		meta.Name = []byte{}
	}
	meta.Node.AccessTime = meta.Node.AccessTime.UTC()
	meta.Node.ModTime = meta.Node.ModTime.UTC()
	meta.Node.Content = ""
	meta.Node.Metadata = nil
	meta.Node.LinkTarget = nil
	meta.Node.CreationTime = boundedTime(meta.Node.CreationTime)
	meta.Node.ChangeTime = boundedTime(meta.Node.ChangeTime)
	charge, err := r.rowBytes(len(r.rows), meta, lengths)
	if err != nil {
		return nil, false, r.fail(err)
	}
	if charge < 0 {
		return nil, false, r.fail(fmt.Errorf("the row charge at index %d is negative: %w", len(r.rows), syscall.EINVAL))
	}
	if charge > r.maxBytes-r.fixedBytes {
		return nil, false, r.fail(fmt.Errorf("one snapshot row requires %d bytes under a %d-byte result bound: %w", charge+r.fixedBytes, r.maxBytes, syscall.EFBIG))
	}
	if charge > r.maxBytes-r.usedBytes {
		return nil, false, nil
	}
	r.usedBytes += charge
	r.pending = true
	return &RowReservation{result: r, meta: meta, lengths: lengths}, true, nil
}

// Rows returns the completed page. The returned slice is owned by the result.
func (r *RowResult) Rows() ([]Row, error) {
	if r == nil {
		return nil, fmt.Errorf("a nil row result has no completed page: %w", syscall.EINVAL)
	}
	if r.failure != nil {
		return nil, r.failure
	}
	if r.pending {
		return nil, fmt.Errorf("a row payload was reserved but not loaded: %w", syscall.EIO)
	}
	if r.rows == nil {
		return []Row{}, nil
	}
	return r.rows, nil
}

// Fail invalidates every row accumulated by an operation that did not complete.
func (r *RowResult) Fail(err error) error {
	if r == nil || err == nil {
		return err
	}
	return r.fail(err)
}

func (r *RowResult) fail(err error) error {
	if r.failure == nil {
		r.failure = err
	}
	r.rows = nil
	r.pending = false
	return r.failure
}

// RowReservation is one charged row whose variable-length fields have not been loaded.
type RowReservation struct {
	result    *RowResult
	meta      Row
	lengths   RowPayloadLengths
	committed bool
}

// Commit supplies the fields whose lengths were charged by Reserve.
func (r *RowReservation) Commit(name []byte, content Key, metadata, target []byte) error {
	if r == nil || r.result == nil || r.committed {
		return fmt.Errorf("a row reservation can be committed exactly once: %w", syscall.EINVAL)
	}
	if int64(len(name)) != r.lengths.Name || int64(len(content)) != r.lengths.Content || int64(len(metadata)) != r.lengths.Metadata || int64(len(target)) != r.lengths.Target {
		return r.result.fail(fmt.Errorf(
			"a snapshot row payload has lengths (%d, %d, %d, %d) after (%d, %d, %d, %d) were reserved: %w",
			len(name), len(content), len(metadata), len(target), r.lengths.Name, r.lengths.Content, r.lengths.Metadata, r.lengths.Target, syscall.EIO,
		))
	}
	if r.result.failure != nil {
		return r.result.failure
	}
	row := r.meta
	row.Name = bytes.Clone(name)
	row.Node.Content = Key(strings.Clone(string(content)))
	decoded, err := storage.DecodeMetadata(metadata)
	if err != nil {
		return r.result.fail(err)
	}
	row.Node.Metadata = decoded
	row.Node.LinkTarget = bytes.Clone(target)
	r.committed = true
	r.result.pending = false
	r.result.rows = append(r.result.rows, row)
	return nil
}

func boundedTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	instant := value.UTC()
	return &instant
}
