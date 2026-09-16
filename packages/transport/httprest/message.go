package httprest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Time is one instant on the wire: whole seconds since the Unix epoch plus a nanosecond
// remainder, which is the pair a filesystem itself keeps.
//
// The pair spans every time.Time there is. Nanoseconds alone do not: time.Time.UnixNano
// is undefined outside 1678-09-21 to 2262-04-11, and a time outside that range, the zero
// time.Time included, comes back as a different and entirely plausible date with nothing
// marking it wrong.
//
// It is one object rather than two fields beside each other so that an instant which may
// be absent — every one in an AttrChange — is absent or present as a whole. Two nullable
// halves could disagree, and a seconds field with no nanoseconds beside it would read as
// an instant on the second.
type Time struct {
	UnixSec int64 `json:"unix_sec"`
	Nanos   int32 `json:"nanos"`
}

// TimeOf renders t for the wire.
func TimeOf(t time.Time) Time {
	return Time{UnixSec: t.Unix(), Nanos: int32(t.Nanosecond())}
}

// Time returns the instant t carries.
func (t Time) Time() time.Time { return time.Unix(t.UnixSec, int64(t.Nanos)) }

// Attr carries generic node facts and canonical opaque metadata.
type Attr struct {
	ID                uint64                       `json:"id"`
	Kind              storage.NodeKind             `json:"kind"`
	Size              int64                        `json:"size"`
	AccessTime        Time                         `json:"access_time"`
	ModTime           Time                         `json:"mod_time"`
	CreationTime      *Time                        `json:"creation_time,omitempty"`
	ChangeTime        *Time                        `json:"change_time,omitempty"`
	MetadataRevision  storage.NodeMetadataRevision `json:"metadata_revision"`
	DirectoryRevision storage.DirectoryRevision    `json:"directory_revision"`
	Metadata          []byte                       `json:"metadata"`
}

func AttrOf(a storage.Attr) (*Attr, error) {
	if err := a.Check(); err != nil {
		return nil, errors.Join(syscall.EIO, err)
	}
	metadata, err := storage.EncodeMetadata(a.Metadata)
	if err != nil {
		return nil, err
	}
	return &Attr{ID: a.ID, Kind: a.Kind, Size: a.Size, AccessTime: TimeOf(a.AccessTime), ModTime: TimeOf(a.ModTime), CreationTime: optionalTimeOf(a.CreationTime), ChangeTime: optionalTimeOf(a.ChangeTime), MetadataRevision: a.MetadataRevision, DirectoryRevision: a.DirectoryRevision, Metadata: metadata}, nil
}
func (a Attr) Storage() (storage.Attr, error) {
	metadata, err := storage.DecodeMetadata(a.Metadata)
	if err != nil {
		return storage.Attr{}, err
	}
	if a.ID == 0 || a.Size < 0 || a.MetadataRevision == 0 || a.Kind.Check() != nil || (a.Kind == storage.NodeDirectory) != (a.DirectoryRevision != 0) || a.Kind == storage.NodeDirectory && a.Size != 0 {
		return storage.Attr{}, errors.New("attributes carry invalid generic node facts")
	}
	for _, instant := range []*Time{&a.AccessTime, &a.ModTime, a.CreationTime, a.ChangeTime} {
		if instant != nil && (instant.Nanos < 0 || instant.Nanos >= 1e9) {
			return storage.Attr{}, errors.New("attribute instant has invalid nanoseconds")
		}
	}
	return storage.Attr{ID: a.ID, Kind: a.Kind, Size: a.Size, AccessTime: a.AccessTime.Time(), ModTime: a.ModTime.Time(), CreationTime: optionalStorageTime(a.CreationTime), ChangeTime: optionalStorageTime(a.ChangeTime), MetadataRevision: a.MetadataRevision, DirectoryRevision: a.DirectoryRevision, Metadata: metadata}, nil
}
func (a *Attr) UnmarshalJSON(data []byte) error {
	type value Attr
	var decoded value
	if err := decodeMessageJSON(data, &decoded, len(data)); err != nil {
		return err
	}
	if _, err := Attr(decoded).Storage(); err != nil {
		return err
	}
	*a = Attr(decoded)
	return nil
}

type AttrChange struct {
	ExpectedRevision storage.NodeMetadataRevision `json:"expected_revision"`
	Metadata         *[]byte                      `json:"metadata,omitempty"`
	AccessTime       *Time                        `json:"access_time,omitempty"`
	ModTime          *Time                        `json:"mod_time,omitempty"`
	CreationTime     *Time                        `json:"creation_time,omitempty"`
	ChangeTime       *Time                        `json:"change_time,omitempty"`
}

func AttrChangeOf(c storage.AttrChange) (*AttrChange, error) {
	if err := c.Check(); err != nil {
		return nil, err
	}
	var metadata *[]byte
	if c.Metadata != nil {
		encoded, err := storage.EncodeMetadata(*c.Metadata)
		if err != nil {
			return nil, err
		}
		metadata = &encoded
	}
	return &AttrChange{ExpectedRevision: c.ExpectedRevision, Metadata: metadata, AccessTime: optionalTimeOf(c.AccessTime), ModTime: optionalTimeOf(c.ModTime), CreationTime: optionalTimeOf(c.CreationTime), ChangeTime: optionalTimeOf(c.ChangeTime)}, nil
}
func (c AttrChange) Storage() (storage.AttrChange, error) {
	var metadata *storage.Metadata
	if c.Metadata != nil {
		decoded, err := storage.DecodeMetadata(*c.Metadata)
		if err != nil {
			return storage.AttrChange{}, err
		}
		metadata = &decoded
	}
	for _, instant := range []*Time{c.AccessTime, c.ModTime, c.CreationTime, c.ChangeTime} {
		if instant != nil && (instant.Nanos < 0 || instant.Nanos >= 1e9) {
			return storage.AttrChange{}, errors.New("attribute change has invalid nanoseconds")
		}
	}
	value := storage.AttrChange{ExpectedRevision: c.ExpectedRevision, Metadata: metadata, AccessTime: optionalStorageTime(c.AccessTime), ModTime: optionalStorageTime(c.ModTime), CreationTime: optionalStorageTime(c.CreationTime), ChangeTime: optionalStorageTime(c.ChangeTime)}
	return value, value.Check()
}

// SetAttrRequest is the body of an OpSetAttr.
//
// The change travels in a body rather than as query operands because its fields are
// optional and the operands are not: a request is refused unless it carries exactly the
// operands its operation takes, which is what keeps a damaged query from reading as a
// request against the whole volume.
type SetAttrRequest struct {
	Change *AttrChange `json:"change"`
}

// UnmarshalJSON decodes the request, and refuses a body that carries no change.
//
// A change naming nothing is a legitimate request — it asks whether the node is there —
// so it cannot be told from a body that lost its contents by looking at the fields. The
// enclosing object is what makes the two distinguishable.
func (r *SetAttrRequest) UnmarshalJSON(data []byte) error {
	type request SetAttrRequest
	var decoded request
	if err := decodeMessageJSON(data, &decoded, len(data)); err != nil {
		return err
	}
	if decoded.Change == nil {
		return errors.New("the request carried no change")
	}
	if _, err := decoded.Change.Storage(); err != nil {
		return err
	}
	*r = SetAttrRequest(decoded)
	return nil
}

// Entry is storage.Entry on the wire.
//
// The name travels as raw bytes, which encoding/json writes as base64. A name on Linux
// is an arbitrary byte sequence rather than text, and encoding/json substitutes U+FFFD
// for any byte that is not valid UTF-8 when it writes a string — so a string field would
// hand back a name that no longer addresses the file it was read from.
type Entry struct {
	Name []byte `json:"name"`
	Attr *Attr  `json:"attr"`
}

// UnmarshalJSON decodes an entry, and refuses one that carries no attributes.
func (e *Entry) UnmarshalJSON(data []byte) error {
	// The alias sheds this method, so what follows is the ordinary decoding.
	type entry Entry
	var decoded entry
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Attr == nil {
		return fmt.Errorf("the listing entry %q carried no attributes", decoded.Name)
	}
	*e = Entry(decoded)
	return nil
}

// EntriesOf renders a listing for the wire. The result is never nil, so that an empty
// directory encodes as an empty JSON list rather than as null.
func EntriesOf(entries []storage.Entry) ([]Entry, error) {
	wire := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		attr, err := AttrOf(entry.Attr)
		if err != nil {
			return nil, err
		}
		wire = append(wire, Entry{Name: []byte(entry.Name), Attr: attr})
	}
	return wire, nil
}

// StatResponse is the body of a successful OpStat.
type StatResponse struct {
	Attr *Attr `json:"attr"`
}

// UnmarshalJSON decodes the response, and refuses a body that carries no attributes.
func (r *StatResponse) UnmarshalJSON(data []byte) error {
	type response StatResponse
	var decoded response
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Attr == nil {
		return errors.New("the response carried no attributes")
	}
	*r = StatResponse(decoded)
	return nil
}

// ListResponse is the body of a successful OpList.
type ListResponse struct {
	Entries []Entry `json:"entries"`
}

// UnmarshalJSON decodes the response, and refuses a body that carries no listing. JSON
// null and an empty list are two characters apart and mean opposite things.
func (r *ListResponse) UnmarshalJSON(data []byte) error {
	type response ListResponse
	var decoded response
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Entries == nil {
		return errors.New("the response carried no listing")
	}
	*r = ListResponse(decoded)
	return nil
}

// Storage returns the listing the response carries.
func (r ListResponse) Storage() ([]storage.Entry, error) {
	entries := make([]storage.Entry, 0, len(r.Entries))
	for _, entry := range r.Entries {
		attr, err := entry.Attr.Storage()
		if err != nil {
			return nil, err
		}
		entries = append(entries, storage.Entry{Name: string(entry.Name), Attr: attr})
	}
	return entries, nil
}

// Space is storage.Space on the wire.
//
// Each count travels by pointer and the decoding below refuses an absent one. They are
// byte counts whose zero is a legitimate figure — a volume holding nothing has used
// none, and a full one has none available — so once a field has been read there is no
// telling absence from zero. As plain fields, a report that lost one would describe a
// volume with no room left, which reads as an ordinary answer and stops every write.
type Space struct {
	Total *int64 `json:"total"`
	Used  *int64 `json:"used"`
	Avail *int64 `json:"avail"`
}

// SpaceOf renders s for the wire.
func SpaceOf(s storage.Space) *Space {
	return &Space{Total: &s.Total, Used: &s.Used, Avail: &s.Avail}
}

// UnmarshalJSON decodes a space report, refusing one that is missing a count and one whose
// counts could not all be true of anything.
//
// Coherence is settled here so that no reader of these messages can omit it. These figures
// end up in a kernel reply whose fields are unsigned, where a negative becomes an enormous
// positive and a program asking whether its write will fit is told it has room no disk
// holds — a fabricated fact rather than a set of figures to repair (R-ERR-2).
func (s *Space) UnmarshalJSON(data []byte) error {
	type space Space
	var decoded space
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	for _, count := range []struct {
		name  string
		value *int64
	}{{"total", decoded.Total}, {"used", decoded.Used}, {"avail", decoded.Avail}} {
		if count.value == nil {
			return fmt.Errorf("the space report carried no %s", count.name)
		}
	}
	report := Space(decoded)
	if reported := report.Storage(); !reported.Coherent() {
		return fmt.Errorf("the space report holds %d bytes in all, of which %d are used and %d available, which cannot all be true",
			reported.Total, reported.Used, reported.Avail)
	}
	*s = report
	return nil
}

// Storage returns the counts s carries. Every Space this package produces has all three:
// SpaceOf sets them, and the decoding refuses a report missing one.
func (s Space) Storage() storage.Space {
	return storage.Space{Total: *s.Total, Used: *s.Used, Avail: *s.Avail}
}

// SpaceResponse is the body of a successful OpSpace.
type SpaceResponse struct {
	Space *Space `json:"space"`
}

// UnmarshalJSON decodes the response, and refuses a body that carries no space report.
func (r *SpaceResponse) UnmarshalJSON(data []byte) error {
	type response SpaceResponse
	var decoded response
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Space == nil {
		return errors.New("the response carried no space report")
	}
	*r = SpaceResponse(decoded)
	return nil
}

// MutationBarrier is an atomic log incarnation and committed position. Mutation replies
// capture it at or after the successful change; Checkpoint captures it without a change.
// Waiting until a replica of the same incarnation has applied through Position establishes
// visibility through that point without identifying any particular change record.
type MutationBarrier struct {
	Incarnation string `json:"incarnation"`
	Position    int64  `json:"position"`
}

// MaxIncarnationBytes is the protocol-wide ceiling for a change-log identity in both stream
// starts and mutation barriers. A Handler may impose a smaller value so the worst-case JSON
// escaping fits its configured body and frame limits.
const MaxIncarnationBytes = 256

const (
	maxMutationBarrierJSONBytes  = 6*MaxIncarnationBytes + 128
	maxMutationResponseJSONBytes = maxMutationBarrierJSONBytes + 32
)

func (b *MutationBarrier) UnmarshalJSON(data []byte) error {
	if len(data) > maxMutationBarrierJSONBytes {
		return errors.New("the mutation barrier exceeds its protocol bound")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return errors.New("the mutation barrier is not an object")
	}
	for name := range fields {
		if name != "incarnation" && name != "position" {
			return fmt.Errorf("the mutation barrier carries unknown field %q", name)
		}
	}
	var decoded struct {
		Incarnation json.RawMessage `json:"incarnation"`
		Position    *int64          `json:"position"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if len(decoded.Incarnation) == 0 || len(decoded.Incarnation) > 6*MaxIncarnationBytes+2 {
		return errors.New("the mutation barrier carries no bounded log incarnation")
	}
	var incarnation string
	if err := json.Unmarshal(decoded.Incarnation, &incarnation); err != nil {
		return fmt.Errorf("the mutation barrier incarnation is not a string: %w", err)
	}
	if incarnation == "" || len(incarnation) > MaxIncarnationBytes {
		return errors.New("the mutation barrier names no log incarnation")
	}
	if decoded.Position == nil || *decoded.Position < 0 {
		return errors.New("the mutation barrier carries no valid log position")
	}
	*b = MutationBarrier{Incarnation: incarnation, Position: *decoded.Position}
	return nil
}

// MutationResponse is the successful response to an operation that may change the
// volume. Barrier is absent only when the served volume has no change log.
type MutationResponse struct {
	Barrier *MutationBarrier `json:"barrier,omitempty"`
}

func (r *MutationResponse) UnmarshalJSON(data []byte) error {
	if len(data) > maxMutationResponseJSONBytes {
		return errors.New("the mutation response exceeds its protocol bound")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return errors.New("the mutation response is not an object")
	}
	for name := range fields {
		if name != "barrier" {
			return fmt.Errorf("the mutation response carries unknown field %q", name)
		}
	}
	raw, present := fields["barrier"]
	if !present {
		*r = MutationResponse{}
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("the mutation response carries a null barrier")
	}
	var barrier MutationBarrier
	if err := json.Unmarshal(raw, &barrier); err != nil {
		return err
	}
	*r = MutationResponse{Barrier: &barrier}
	return nil
}

// ErrorResponse is the body of every response that is not a success.
//
// Errno is set exactly when the status is StatusStorageError, and it holds the symbolic
// name — "ENOENT", not 2 — so that the two sides agree on a vocabulary rather than on
// one platform's numbering, and so that a name neither side has heard of is visibly a
// name neither side has heard of. Message is for whoever reads the logs and carries no
// meaning for the client.
type ErrorResponse struct {
	Errno    string       `json:"errno,omitempty"`
	Message  string       `json:"message,omitempty"`
	LockCode locking.Code `json:"lockCode,omitempty"`
	Recorded *bool        `json:"recorded,omitempty"`
}
