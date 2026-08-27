package httprest

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// The messages of the replication half, and the frames they travel in.
//
// Everything here follows the rules message.go established, for the same reasons. An
// instant is a seconds-and-nanoseconds pair, because a nanosecond count spans only 1678 to
// 2262 and a namespace is asked to hold instants outside that. A name is a byte sequence,
// because encoding/json substitutes U+FFFD for any byte that is not valid UTF-8 when it
// writes a string, and a replica that recorded such a name would address a node that is
// not there. Anything whose absence is indistinguishable from its zero value travels by
// pointer and is refused when it is missing, because a replica applies what arrives here
// without asking anything back: there is no revalidation behind these messages and no
// timeout that repairs one that was wrong.

// Node is metastore.Node on the wire.
type Node struct {
	ID         int64  `json:"id"`
	Mode       uint32 `json:"mode"`
	Size       int64  `json:"size"`
	AccessTime Time   `json:"access_time"`
	ModTime    Time   `json:"mod_time"`

	// Content is the key of the object holding a file's bytes, and empty for a directory
	// and for a file that has never been written. It travels as bytes rather than as a
	// string for the same reason a name does: a key is opaque to everything above the
	// store that allocated it, so this side may not assume it is text, and a key that came
	// back altered names bytes that are not there.
	Content []byte `json:"content"`
}

// NodeOf renders n for the wire.
func NodeOf(n metastore.Node) *Node {
	return &Node{
		ID:         n.ID,
		Mode:       uint32(n.Mode),
		Size:       n.Size,
		AccessTime: TimeOf(n.AccessTime),
		ModTime:    TimeOf(n.ModTime),
		Content:    []byte(n.Content),
	}
}

// Metastore returns the node n carries.
func (n Node) Metastore() metastore.Node {
	return metastore.Node{
		ID:         n.ID,
		Mode:       fs.FileMode(n.Mode),
		Size:       n.Size,
		AccessTime: n.AccessTime.Time(),
		ModTime:    n.ModTime.Time(),
		Content:    metastore.Key(n.Content),
	}
}

// The names the four metastore.ChangeKind values travel under.
//
// Names rather than the numbers behind them, exactly as an errno travels by name: the two
// ends then agree on a vocabulary instead of on the order of a Go constant block, and a
// kind neither end has heard of is visibly one neither end has heard of rather than a
// plausible neighbour. Reordering the constants in metastore would otherwise turn every
// removal in flight into a creation.
const (
	kindCreated  = "created"
	kindRemoved  = "removed"
	kindModified = "modified"
	kindRenamed  = "renamed"
)

var kindNames = map[metastore.ChangeKind]string{
	metastore.Created:  kindCreated,
	metastore.Removed:  kindRemoved,
	metastore.Modified: kindModified,
	metastore.Renamed:  kindRenamed,
}

var kindsByName = func() map[string]metastore.ChangeKind {
	byName := make(map[string]metastore.ChangeKind, len(kindNames))
	for kind, name := range kindNames {
		byName[name] = kind
	}
	return byName
}()

// Location is metastore.Location on the wire.
type Location struct {
	Parent int64  `json:"parent"`
	Name   []byte `json:"name"`
}

// Change is metastore.Change on the wire.
type Change struct {
	Position int64  `json:"position"`
	Kind     string `json:"kind"`
	Parent   int64  `json:"parent"`
	Name     []byte `json:"name"`

	// From is where a renamed node came from, and absent for every other kind.
	From *Location `json:"from,omitempty"`

	// Node is what the name holds afterwards, and absent for a removal.
	Node *Node `json:"node,omitempty"`
}

// ChangeOf renders c for the wire. A kind outside the vocabulary is refused rather than
// carried, because the receiving side would have to guess what happened to the name.
func ChangeOf(c metastore.Change) (*Change, error) {
	name, known := kindNames[c.Kind]
	if !known {
		return nil, fmt.Errorf("the change at position %d is of kind %d, which this protocol cannot name", c.Position, c.Kind)
	}
	wire := &Change{Position: int64(c.Position), Kind: name, Parent: c.Parent, Name: c.Name}
	if c.From != nil {
		wire.From = &Location{Parent: c.From.Parent, Name: c.From.Name}
	}
	if c.Node != nil {
		wire.Node = NodeOf(*c.Node)
	}
	if err := wire.check(); err != nil {
		return nil, fmt.Errorf("the change at position %d cannot be sent: %w", c.Position, err)
	}
	return wire, nil
}

// UnmarshalJSON decodes a change and refuses one that does not describe an outcome.
//
// The three refusals are what stands between a damaged message and a replica that records
// it as fact. A kind this side does not know cannot be applied at all. A change that lost
// its node decodes into a zero Node — mode 0 has no type bits, so it reads as a regular
// file of length zero dated the epoch — and a replica would then report a directory as an
// empty file and never learn otherwise. A rename that lost its source leaves the node
// behind at its old name for good, because the change that would have emptied it has
// already been consumed.
func (c *Change) UnmarshalJSON(data []byte) error {
	type change Change
	var decoded change
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	got := Change(decoded)
	if err := got.check(); err != nil {
		return err
	}
	*c = got
	return nil
}

func (c Change) check() error {
	kind, known := kindsByName[c.Kind]
	if !known {
		return fmt.Errorf("the change carries kind %q, which this protocol does not know", c.Kind)
	}
	if wantNode := kind != metastore.Removed; wantNode != (c.Node != nil) {
		if wantNode {
			return fmt.Errorf("the %s change at position %d carried no node", c.Kind, c.Position)
		}
		return fmt.Errorf("the %s change at position %d carried a node, and a removal leaves none", c.Kind, c.Position)
	}
	if wantFrom := kind == metastore.Renamed; wantFrom != (c.From != nil) {
		if wantFrom {
			return fmt.Errorf("the rename at position %d did not say where the node came from", c.Position)
		}
		return fmt.Errorf("the %s change at position %d says where a node came from, and only a rename does", c.Kind, c.Position)
	}
	return nil
}

// Metastore returns the change c carries. Every Change this package produces has passed
// check, so the kind is in the vocabulary and the optional members are the ones its kind
// takes.
func (c Change) Metastore() metastore.Change {
	change := metastore.Change{
		Position: metastore.Position(c.Position),
		Kind:     kindsByName[c.Kind],
		Parent:   c.Parent,
		Name:     c.Name,
	}
	if c.From != nil {
		change.From = &metastore.Location{Parent: c.From.Parent, Name: c.From.Name}
	}
	if c.Node != nil {
		node := c.Node.Metastore()
		change.Node = &node
	}
	return change
}

// Row is metastore.Row on the wire. The root carries parent 0 and no name.
type Row struct {
	Parent int64  `json:"parent"`
	Name   []byte `json:"name"`
	Node   *Node  `json:"node"`
}

// RowOf renders r for the wire.
func RowOf(r metastore.Row) Row {
	return Row{Parent: r.Parent, Name: r.Name, Node: NodeOf(r.Node)}
}

// UnmarshalJSON decodes a row and refuses one that carries no node, which would otherwise
// arrive as a regular file of length zero dated the epoch.
func (r *Row) UnmarshalJSON(data []byte) error {
	type row Row
	var decoded row
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Node == nil {
		return fmt.Errorf("the snapshot row for %q under %d carried no node", decoded.Name, decoded.Parent)
	}
	*r = Row(decoded)
	return nil
}

// Metastore returns the row r carries.
func (r Row) Metastore() metastore.Row {
	return metastore.Row{Parent: r.Parent, Name: r.Name, Node: r.Node.Metastore()}
}

// The frames a replication stream is made of. Each is one server-sent event: a name
// saying what the frame is, and one line of JSON saying what it carries.
const (
	// A change stream: one start, then changes for as long as the replica watches.
	eventStart  = "start"
	eventChange = "change"

	// A snapshot stream: one open, then pages of rows, then done.
	eventOpen = "open"
	eventRows = "rows"
	eventDone = "done"

	// A change stream that this server is ending on purpose, because it is stopping. It is
	// its own frame rather than an absent one, because a stream that simply ends is what a
	// server that vanished also looks like — and those call for different things: a replica
	// told this keeps what it has and comes back to the position it holds.
	eventGone = "gone"

	// Either stream, in place of everything that would have followed.
	eventFault = "fault"
)

// RebuildReason says why a log cannot continue from where a replica asked, and the three
// values are three different things to do about it.
type RebuildReason string

const (
	// RebuildIncarnation: this is not the log the replica last saw. Its history did not
	// survive, so no position in it means anything here.
	RebuildIncarnation RebuildReason = "incarnation"

	// RebuildAge: the changes the replica is missing were discarded for being old. The
	// replica was away longer than the log keeps history for.
	RebuildAge RebuildReason = "age"

	// RebuildVolume: the changes the replica is missing were discarded for being too
	// many. The namespace changes faster than the log was configured to hold, which is a
	// setting to revisit rather than something the replica did.
	RebuildVolume RebuildReason = "volume"
)

var rebuildReasons = map[RebuildReason]bool{
	RebuildIncarnation: true,
	RebuildAge:         true,
	RebuildVolume:      true,
}

// StreamStart is the first frame of a change stream, and says which of three things is
// true before a single change is delivered.
//
// A rebuild is one shape and the two continuing answers are the other, rather than a
// rebuild being a flag beside a position: a replica that read the position out of a frame
// telling it to rebuild would carry on from a point the log cannot supply, and the
// evidence that it is doing so is exactly the changes it will never receive.
type StreamStart struct {
	// Rebuild is set when the log cannot continue from where the replica asked, and is
	// the only member set then.
	Rebuild RebuildReason `json:"rebuild,omitempty"`

	// Incarnation names the run of history this stream belongs to. The replica records it
	// and offers it back when it reconnects.
	Incarnation string `json:"incarnation,omitempty"`

	// Position is where the stream begins: the replica holds everything up to and
	// including it, and every change that follows is later than it.
	Position *int64 `json:"position,omitempty"`

	// Tail is the newest position the log had reached when the stream began.
	//
	// It is what tells a replica that has missed changes when it has stopped missing them.
	// Everything between Position and this is on its way, and applying the last of it is the
	// moment the copy is current again — which is the moment, and not before, that it may be
	// answered from. Without it a replica knows it is behind and has no way to learn that it
	// no longer is, since a stream with nothing left to replay and a stream that has not
	// begun replaying look the same from the reading end.
	// Whether anything was missed is this and Position compared, and is not sent beside
	// them: it is one fact, and stating it twice buys nothing but a frame that can
	// contradict itself.
	Tail *int64 `json:"tail,omitempty"`
}

// UnmarshalJSON decodes the frame and refuses one that is neither shape whole.
func (s *StreamStart) UnmarshalJSON(data []byte) error {
	type start StreamStart
	var decoded start
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	got := StreamStart(decoded)
	if got.Rebuild != "" {
		if !rebuildReasons[got.Rebuild] {
			return fmt.Errorf("the stream must be rebuilt for the reason %q, which this side does not know", got.Rebuild)
		}
		if got.Position != nil || got.Tail != nil || got.Incarnation != "" {
			return errors.New("the stream must be rebuilt, and the frame carries a place to continue from anyway")
		}
		*s = got
		return nil
	}
	// An empty incarnation is refused rather than carried: it is the value every log with
	// no identity would report, so a replica holding one would match a log that has lost
	// its history and be told it is caught up.
	if got.Incarnation == "" {
		return errors.New("the stream names no incarnation, so nothing could be resumed from it")
	}
	if got.Position == nil {
		return errors.New("the stream names no position to begin at")
	}
	if got.Tail == nil {
		return errors.New("the stream does not say how far the log had reached, so nothing could tell when it has been caught up with")
	}
	// A tail behind the position the stream begins at describes a log that has not reached
	// what it is about to deliver, which is nothing a replica can act on.
	if *got.Tail < *got.Position {
		return fmt.Errorf("the stream begins at position %d, past the log's tail at %d", *got.Position, *got.Tail)
	}
	*s = got
	return nil
}

// SnapshotOpen is the first frame of a snapshot stream.
type SnapshotOpen struct {
	// Position is the position the picture was taken at: no change later than it is in
	// the rows that follow, and every row reflects the same instant.
	//
	// By pointer, and refused when absent, because zero is a legitimate position — a
	// namespace nothing has yet changed is at zero — so a lost figure would arrive as an
	// ordinary answer, and a replica that seeded itself at zero would replay changes it
	// already holds.
	Position *int64 `json:"position"`
}

// UnmarshalJSON decodes the frame and refuses one carrying no position.
func (o *SnapshotOpen) UnmarshalJSON(data []byte) error {
	type open SnapshotOpen
	var decoded open
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Position == nil {
		return errors.New("the snapshot does not say what position it was taken at")
	}
	*o = SnapshotOpen(decoded)
	return nil
}

// SnapshotPage is one page of a snapshot's rows.
type SnapshotPage struct {
	Rows []Row `json:"rows"`
}

// UnmarshalJSON decodes a page and refuses one carrying no rows at all. JSON null and an
// empty list are two characters apart, and a page that lost its rows would otherwise
// remove from the replica every node it was carrying.
func (p *SnapshotPage) UnmarshalJSON(data []byte) error {
	type page SnapshotPage
	var decoded page
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Rows == nil {
		return errors.New("the snapshot page carried no rows")
	}
	*p = SnapshotPage(decoded)
	return nil
}

// StreamFault ends a stream in place of everything that would have followed.
//
// A stream's status and headers are sent before its first frame, so a failure after that
// point has no status left to travel in. It travels as a frame instead — and the reason
// it must travel at all is that the alternative is the stream simply stopping, which is
// what a dropped connection looks like too. The receiving side treats both as a failure,
// so this frame decides only whether anyone can say afterwards what went wrong.
type StreamFault struct {
	Message string `json:"message"`
}
