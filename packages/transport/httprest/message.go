package httprest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
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

// Attr is storage.Attr on the wire.
//
// Mode carries io/fs.FileMode's own bit layout rather than a POSIX st_mode, because both
// ends of this protocol are Go and the translation to what a kernel wants belongs to
// whatever presents the volume as a filesystem.
type Attr struct {
	ID         uint64 `json:"id"`
	Mode       uint32 `json:"mode"`
	Size       int64  `json:"size"`
	AccessTime Time   `json:"access_time"`
	ModTime    Time   `json:"mod_time"`
}

// AttrOf renders a for the wire.
//
// Attributes travel by pointer wherever they appear, so that a body carrying none has a
// shape that can be refused. A value could not be refused: a zero Attr reads as a regular
// file, of length zero, dated the epoch, so a mount above it would present a directory as
// an empty file and date every node 1970 — and no check on the values could tell that
// apart from a chmod-000 file, which is a legitimate answer. The refusals themselves are
// in the UnmarshalJSON methods below, where no decoder of these messages can omit them.
func AttrOf(a storage.Attr) *Attr {
	return &Attr{
		ID:         a.ID,
		Mode:       uint32(a.Mode),
		Size:       a.Size,
		AccessTime: TimeOf(a.AccessTime),
		ModTime:    TimeOf(a.ModTime),
	}
}

// Storage returns the attributes a carries.
func (a Attr) Storage() storage.Attr {
	return storage.Attr{
		ID:         a.ID,
		Mode:       fs.FileMode(a.Mode),
		Size:       a.Size,
		AccessTime: a.AccessTime.Time(),
		ModTime:    a.ModTime.Time(),
	}
}

// UnmarshalJSON decodes attributes, and refuses ones that carry no identity.
//
// This is the one field here whose zero is silent rather than loud. A mode of zero is a
// legitimate answer and an instant at the epoch is a value somebody could have set, so the
// refusals elsewhere in this file are about the whole Attr being absent. An identity of zero
// is different: storage.Attr calls it illegal, and every comparison of it in a mount above
// returns equal — so a peer that does not send the field is not read as sending nothing, it
// is read as saying every node is the same node. Measured against a server built before the
// field existed: after an ordinary rename over a name, a held descriptor and the node that
// arrived came back under one inode number, with no diagnostic anywhere.
//
// Refusing here rather than moving the protocol version is deliberate. The version says which
// vocabulary is spoken; this is a peer speaking it and leaving out a word, which the decoders
// in this file are where we catch.
func (a *Attr) UnmarshalJSON(data []byte) error {
	type attr Attr
	var decoded attr
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.ID == 0 {
		return errors.New("the attributes carry no identity for the node they describe")
	}
	*a = Attr(decoded)
	return nil
}

// AttrChange is storage.AttrChange on the wire.
//
// Every field is optional and stays optional here, because an attribute the change does
// not name is exactly what must not be sent: a mode field that defaulted to zero on the
// way across would turn a request to set the modification time into a chmod 000.
type AttrChange struct {
	Mode       *uint32 `json:"mode,omitempty"`
	AccessTime *Time   `json:"access_time,omitempty"`
	ModTime    *Time   `json:"mod_time,omitempty"`
}

// AttrChangeOf renders c for the wire.
func AttrChangeOf(c storage.AttrChange) *AttrChange {
	wire := &AttrChange{}
	if c.Mode != nil {
		mode := uint32(*c.Mode)
		wire.Mode = &mode
	}
	if c.AccessTime != nil {
		accessed := TimeOf(*c.AccessTime)
		wire.AccessTime = &accessed
	}
	if c.ModTime != nil {
		changed := TimeOf(*c.ModTime)
		wire.ModTime = &changed
	}
	return wire
}

// Storage returns the change c carries.
func (c AttrChange) Storage() storage.AttrChange {
	var change storage.AttrChange
	if c.Mode != nil {
		mode := fs.FileMode(*c.Mode)
		change.Mode = &mode
	}
	if c.AccessTime != nil {
		accessed := c.AccessTime.Time()
		change.AccessTime = &accessed
	}
	if c.ModTime != nil {
		changed := c.ModTime.Time()
		change.ModTime = &changed
	}
	return change
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
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Change == nil {
		return errors.New("the request carried no change")
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
func EntriesOf(entries []storage.Entry) []Entry {
	wire := make([]Entry, 0, len(entries))
	for _, e := range entries {
		wire = append(wire, Entry{Name: []byte(e.Name), Attr: AttrOf(e.Attr)})
	}
	return wire
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
func (r ListResponse) Storage() []storage.Entry {
	entries := make([]storage.Entry, 0, len(r.Entries))
	for _, e := range r.Entries {
		entries = append(entries, storage.Entry{Name: string(e.Name), Attr: e.Attr.Storage()})
	}
	return entries
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

// MutationBarrier identifies a log position at or after a successful mutation. Waiting
// until a replica of the same incarnation has applied through Position establishes
// read-after-write visibility without guessing which change record the mutation produced.
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
