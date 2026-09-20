package httprest

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

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

// UnmarshalJSON requires the complete instant and its canonical nanosecond remainder.
func (t *Time) UnmarshalJSON(data []byte) error {
	type instant Time
	var decoded instant
	if err := decodeFileJSON(data, &decoded); err != nil {
		return err
	}
	if err := checkWireTime("metadata", Time(decoded)); err != nil {
		return err
	}
	*t = Time(decoded)
	return nil
}

// TimeOf renders t for the wire.
func TimeOf(t time.Time) Time {
	return Time{UnixSec: t.Unix(), Nanos: int32(t.Nanosecond())}
}

// Time returns the instant t carries.
func (t Time) Time() time.Time { return time.Unix(t.UnixSec, int64(t.Nanos)) }

// OpaquePayload preserves client-owned bytes and their authority-issued version.
type OpaquePayload struct {
	Version []byte `json:"version"`
	Data    []byte `json:"data"`
}

func (p *OpaquePayload) UnmarshalJSON(data []byte) error {
	type payload OpaquePayload
	var decoded payload
	if err := decodeFileJSON(data, &decoded); err != nil {
		return err
	}
	var fields map[string]string
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"version", "data"} {
		value, err := base64.StdEncoding.Strict().DecodeString(fields[name])
		if err != nil || base64.StdEncoding.EncodeToString(value) != fields[name] {
			return fmt.Errorf("metadata %s is not canonical base64", name)
		}
	}
	if len(decoded.Version) == 0 || len(decoded.Version) > storage.MaxObservationTokenBytes || len(decoded.Data) > storage.MaxMetadataValueBytes {
		return errors.New("metadata payload exceeds its field bounds")
	}
	*p = OpaquePayload(decoded)
	return nil
}

func metadataOf(values map[string]storage.OpaquePayload) map[string]OpaquePayload {
	if values == nil {
		return nil
	}
	wire := make(map[string]OpaquePayload, len(values))
	for name, value := range values {
		wire[name] = OpaquePayload{Version: append([]byte{}, value.Version...), Data: append([]byte{}, value.Data...)}
	}
	return wire
}

func metadataStorage(values map[string]OpaquePayload) map[string]storage.OpaquePayload {
	if values == nil {
		return nil
	}
	result := make(map[string]storage.OpaquePayload, len(values))
	for name, value := range values {
		result[name] = storage.OpaquePayload{Version: bytes.Clone(value.Version), Data: bytes.Clone(value.Data)}
	}
	return result
}

func optionalTimeOf(value *time.Time) *Time {
	if value == nil {
		return nil
	}
	instant := TimeOf(*value)
	return &instant
}

func optionalTimeStorage(value *Time) *time.Time {
	if value == nil {
		return nil
	}
	instant := value.Time()
	return &instant
}

// Attr is storage.Attr on the wire. Optional times distinguish unknown facts from
// every representable instant; opaque metadata is never interpreted by transport.
type Attr struct {
	ID         uint64                   `json:"id"`
	Kind       storage.NodeKind         `json:"kind"`
	Size       int64                    `json:"size"`
	AccessTime Time                     `json:"access_time"`
	ModTime    Time                     `json:"mod_time"`
	BirthTime  *Time                    `json:"birth_time,omitempty"`
	ChangeTime *Time                    `json:"change_time,omitempty"`
	Metadata   map[string]OpaquePayload `json:"metadata,omitempty"`
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
	return &Attr{ID: a.ID, Kind: a.Kind, Size: a.Size,
		AccessTime: TimeOf(a.AccessTime), ModTime: TimeOf(a.ModTime),
		BirthTime: optionalTimeOf(a.BirthTime), ChangeTime: optionalTimeOf(a.ChangeTime),
		Metadata: metadataOf(a.Metadata)}
}

// Storage returns the attributes a carries.
func (a Attr) Storage() storage.Attr {
	return storage.Attr{ID: a.ID, Kind: a.Kind, Size: a.Size,
		AccessTime: a.AccessTime.Time(), ModTime: a.ModTime.Time(),
		BirthTime: optionalTimeStorage(a.BirthTime), ChangeTime: optionalTimeStorage(a.ChangeTime),
		Metadata: metadataStorage(a.Metadata)}
}

func (a Attr) check() error {
	if a.ID == 0 {
		return errors.New("the attributes carry no node identity")
	}
	if a.Size < 0 {
		return errors.New("the attributes carry a negative size")
	}
	if err := a.Kind.Check(); err != nil {
		return fmt.Errorf("the attributes carry an invalid node kind: %w", err)
	}
	for _, value := range []struct {
		name    string
		instant *Time
	}{
		{"access", &a.AccessTime}, {"modification", &a.ModTime}, {"birth", a.BirthTime}, {"change", a.ChangeTime},
	} {
		if value.instant != nil {
			if err := checkWireTime(value.name, *value.instant); err != nil {
				return err
			}
		}
	}
	return storage.CheckMetadata(metadataStorage(a.Metadata))
}

// UnmarshalJSON rejects attributes that omit identity, kind, valid times, or
// well-formed metadata. A zero identity would collapse distinct nodes into one.
func (a *Attr) UnmarshalJSON(data []byte) error {
	type attr Attr
	var decoded attr
	if err := decodeFileJSON(data, &decoded); err != nil {
		return err
	}
	got := Attr(decoded)
	if err := got.check(); err != nil {
		return err
	}
	*a = got
	return nil
}

// AttrChange keeps each caller-settable time optional. Omitted fields remain unchanged.
type AttrChange struct {
	AccessTime *Time `json:"access_time,omitempty"`
	ModTime    *Time `json:"mod_time,omitempty"`
	BirthTime  *Time `json:"birth_time,omitempty"`
}

// AttrChangeOf renders c for the wire.
func AttrChangeOf(c storage.AttrChange) *AttrChange {
	return &AttrChange{AccessTime: optionalTimeOf(c.AccessTime), ModTime: optionalTimeOf(c.ModTime), BirthTime: optionalTimeOf(c.BirthTime)}
}

// Storage returns the change c carries.
func (c AttrChange) Storage() storage.AttrChange {
	return storage.AttrChange{AccessTime: optionalTimeStorage(c.AccessTime), ModTime: optionalTimeStorage(c.ModTime), BirthTime: optionalTimeStorage(c.BirthTime)}
}

func (c *AttrChange) UnmarshalJSON(data []byte) error {
	type change AttrChange
	var decoded change
	if err := decodeFileJSON(data, &decoded); err != nil {
		return err
	}
	*c = AttrChange(decoded)
	return nil
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
	if err := decodeFileJSON(data, &decoded); err != nil {
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
	if !utf8.Valid(data) {
		return errors.New("listing entry JSON must be UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoded, err := decodeEntry(decoder)
	if err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("listing entry JSON contains trailing content")
	}
	*e = decoded
	return nil
}

func decodeEntry(decoder *json.Decoder) (Entry, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return Entry{}, errors.New("the listing entry is not an object")
	}
	var result Entry
	var hasName, hasAttr bool
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil {
			return Entry{}, err
		}
		switch field {
		case "name":
			if hasName {
				return Entry{}, errors.New("the listing entry repeats its name")
			}
			hasName = true
			encoded, err := decodeListingString(decoder)
			if err != nil {
				return Entry{}, errors.New("the listing entry name is not base64 text")
			}
			result.Name, err = base64.StdEncoding.Strict().DecodeString(encoded)
			if err != nil || base64.StdEncoding.EncodeToString(result.Name) != encoded {
				return Entry{}, errors.New("the listing entry name is not canonical base64")
			}
		case "attr":
			if hasAttr {
				return Entry{}, errors.New("the listing entry repeats its attributes")
			}
			hasAttr = true
			attr, err := decodeListingAttr(decoder)
			if err != nil {
				return Entry{}, err
			}
			result.Attr = &attr
		default:
			return Entry{}, fmt.Errorf("the listing entry carries unknown field %q", field)
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return Entry{}, errors.New("the listing entry object did not end")
	}
	if !hasName {
		return Entry{}, errors.New("the listing entry carried no name")
	}
	if !hasAttr {
		return Entry{}, fmt.Errorf("the listing entry %q carried no attributes", result.Name)
	}
	return result, nil
}

func decodeListingAttr(decoder *json.Decoder) (Attr, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return Attr{}, errors.New("listing attributes are not an object")
	}
	var result Attr
	seen := make(map[string]bool, 8)
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil {
			return Attr{}, err
		}
		name, ok := field.(string)
		if !ok || seen[name] {
			return Attr{}, errors.New("listing attributes contain a duplicate member")
		}
		seen[name] = true
		switch name {
		case "id":
			result.ID, err = decodeListingUint64(decoder)
		case "kind":
			var value uint64
			value, err = decodeListingUint64(decoder)
			if value > 255 {
				err = errors.New("listing node kind exceeds its wire range")
			} else {
				result.Kind = storage.NodeKind(value)
			}
		case "size":
			result.Size, err = decodeListingInt64(decoder)
		case "access_time":
			result.AccessTime, err = decodeListingTime(decoder)
		case "mod_time":
			result.ModTime, err = decodeListingTime(decoder)
		case "birth_time":
			value, decodeErr := decodeListingTime(decoder)
			result.BirthTime, err = &value, decodeErr
		case "change_time":
			value, decodeErr := decodeListingTime(decoder)
			result.ChangeTime, err = &value, decodeErr
		case "metadata":
			result.Metadata, err = decodeListingMetadata(decoder)
		default:
			return Attr{}, fmt.Errorf("listing attributes carry unknown field %q", name)
		}
		if err != nil {
			return Attr{}, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return Attr{}, errors.New("listing attributes object did not end")
	}
	for _, required := range []string{"id", "kind", "size", "access_time", "mod_time"} {
		if !seen[required] {
			return Attr{}, fmt.Errorf("listing attributes carry no %s", required)
		}
	}
	if err := result.check(); err != nil {
		return Attr{}, err
	}
	return result, nil
}

func decodeListingTime(decoder *json.Decoder) (Time, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return Time{}, errors.New("listing time is not an object")
	}
	var result Time
	var hasSeconds, hasNanos bool
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil {
			return Time{}, err
		}
		switch field {
		case "unix_sec":
			if hasSeconds {
				return Time{}, errors.New("listing time repeats its seconds")
			}
			hasSeconds = true
			result.UnixSec, err = decodeListingInt64(decoder)
		case "nanos":
			if hasNanos {
				return Time{}, errors.New("listing time repeats its nanoseconds")
			}
			hasNanos = true
			var value int64
			value, err = decodeListingInt64(decoder)
			if value < math.MinInt32 || value > math.MaxInt32 {
				err = errors.New("listing time nanoseconds exceed their wire range")
			} else {
				result.Nanos = int32(value)
			}
		default:
			return Time{}, fmt.Errorf("listing time carries unknown field %q", field)
		}
		if err != nil {
			return Time{}, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !hasSeconds || !hasNanos {
		return Time{}, errors.New("listing time is incomplete")
	}
	if err := checkWireTime("listing", result); err != nil {
		return Time{}, err
	}
	return result, nil
}

func decodeListingMetadata(decoder *json.Decoder) (map[string]OpaquePayload, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, errors.New("listing metadata is not an object")
	}
	result := make(map[string]OpaquePayload)
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := field.(string)
		if !ok {
			return nil, errors.New("listing metadata namespace is not text")
		}
		if _, exists := result[name]; exists {
			return nil, errors.New("listing metadata repeats a namespace")
		}
		if len(result) >= storage.MaxMetadataNamespaces {
			return nil, errors.New("listing metadata carries too many namespaces")
		}
		if err := storage.CheckMetadataNamespace(name); err != nil {
			return nil, err
		}
		payload, err := decodeListingPayload(decoder)
		if err != nil {
			return nil, err
		}
		result[name] = payload
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return nil, errors.New("listing metadata object did not end")
	}
	return result, nil
}

func decodeListingPayload(decoder *json.Decoder) (OpaquePayload, error) {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return OpaquePayload{}, errors.New("listing metadata payload is not an object")
	}
	var result OpaquePayload
	var hasVersion, hasData bool
	for decoder.More() {
		field, err := decoder.Token()
		if err != nil {
			return OpaquePayload{}, err
		}
		switch field {
		case "version":
			if hasVersion {
				return OpaquePayload{}, errors.New("listing metadata repeats its version")
			}
			hasVersion = true
			result.Version, err = decodeListingBytes(decoder, "version", storage.MaxObservationTokenBytes, true)
		case "data":
			if hasData {
				return OpaquePayload{}, errors.New("listing metadata repeats its data")
			}
			hasData = true
			result.Data, err = decodeListingBytes(decoder, "data", storage.MaxMetadataValueBytes, false)
		default:
			return OpaquePayload{}, fmt.Errorf("listing metadata carries unknown field %q", field)
		}
		if err != nil {
			return OpaquePayload{}, err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') || !hasVersion || !hasData {
		return OpaquePayload{}, errors.New("listing metadata payload is incomplete")
	}
	return result, nil
}

func decodeListingBytes(decoder *json.Decoder, field string, maximum int, nonempty bool) ([]byte, error) {
	encoded, err := decodeListingString(decoder)
	if err != nil {
		return nil, errors.New("listing bytes are not base64 text")
	}
	if len(encoded) > base64.StdEncoding.EncodedLen(maximum) {
		return nil, fmt.Errorf("listing metadata %s exceeds its field bound", field)
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
		return nil, errors.New("listing bytes are not canonical base64")
	}
	if nonempty && len(decoded) == 0 {
		return nil, fmt.Errorf("listing metadata %s is empty", field)
	}
	return decoded, nil
}

func decodeListingString(decoder *json.Decoder) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	value, ok := token.(string)
	if !ok {
		return "", errors.New("required listing text is not a string")
	}
	return value, nil
}

func decodeListingInt64(decoder *json.Decoder) (int64, error) {
	token, err := decoder.Token()
	if err != nil {
		return 0, err
	}
	number, ok := token.(json.Number)
	if !ok {
		return 0, errors.New("required listing number is not an integer")
	}
	value, err := strconv.ParseInt(string(number), 10, 64)
	if err != nil {
		return 0, errors.New("required listing number is outside its integer range")
	}
	return value, nil
}

func decodeListingUint64(decoder *json.Decoder) (uint64, error) {
	token, err := decoder.Token()
	if err != nil {
		return 0, err
	}
	number, ok := token.(json.Number)
	if !ok {
		return 0, errors.New("required listing number is not an integer")
	}
	value, err := strconv.ParseUint(string(number), 10, 64)
	if err != nil {
		return 0, errors.New("required listing number is outside its integer range")
	}
	return value, nil
}

// EntriesOf renders a listing for the wire. The result is never nil, so that an empty
// directory encodes as an empty JSON list rather than as null.
func EntriesOf(entries []storage.Entry) []Entry {
	wire := make([]Entry, 0, len(entries))
	for _, e := range entries {
		wire = append(wire, Entry{Name: append([]byte{}, e.Name...), Attr: AttrOf(e.Attr)})
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
	if err := decodeFileJSON(data, &decoded); err != nil {
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
	entries := make([]Entry, 0)
	if err := decodeListResponse(data, func(entry Entry) error {
		entries = append(entries, entry)
		return nil
	}); err != nil {
		return err
	}
	*r = ListResponse{Entries: entries}
	return nil
}

func decodeListResponse(data []byte, add func(Entry) error) error {
	if !utf8.Valid(data) {
		return errors.New("listing response JSON must be UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return errors.New("the listing response is not an object")
	}
	if !decoder.More() {
		return errors.New("the response carried no listing")
	}
	field, err := decoder.Token()
	if err != nil || field != "entries" {
		return errors.New("the listing response has an unknown field")
	}
	token, err = decoder.Token()
	if err != nil || token != json.Delim('[') {
		return errors.New("the response carried no listing array")
	}
	for decoder.More() {
		entry, err := decodeEntry(decoder)
		if err != nil {
			return err
		}
		if err := add(entry); err != nil {
			return err
		}
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim(']') {
		return errors.New("the listing array did not end")
	}
	if decoder.More() {
		return errors.New("the listing response carried duplicate or unknown fields")
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return errors.New("the listing response object did not end")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("listing response JSON contains trailing content")
	}
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
	if err := decodeFileJSON(data, &decoded); err != nil {
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
	if err := decodeFileJSON(data, &decoded); err != nil {
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
	type barrier MutationBarrier
	var decoded barrier
	if err := decodeFileJSON(data, &decoded); err != nil {
		return err
	}
	if decoded.Incarnation == "" || len(decoded.Incarnation) > MaxIncarnationBytes {
		return errors.New("the mutation barrier names no log incarnation")
	}
	if decoded.Position < 0 {
		return errors.New("the mutation barrier carries no valid log position")
	}
	*b = MutationBarrier(decoded)
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
	type response MutationResponse
	var decoded response
	if err := decodeFileJSON(data, &decoded); err != nil {
		return err
	}
	*r = MutationResponse(decoded)
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
	CapabilityCode string                `json:"capabilityCode,omitempty"`
	FileRecorded   *bool                 `json:"fileRecorded,omitempty"`
	Attempt        *storage.RangeAttempt `json:"attempt,omitempty"`
	Errno          string                `json:"errno,omitempty"`
	Message        string                `json:"message,omitempty"`
	LockCode       locking.Code          `json:"lockCode,omitempty"`
	Recorded       *bool                 `json:"recorded,omitempty"`
}
