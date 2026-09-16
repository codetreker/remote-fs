package httprest

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// The messages of the replication half, and the frames they travel in.
//
// Everything here follows the rules message.go established, for the same reasons. An
// instant is a seconds-and-nanoseconds pair, because a nanosecond count spans only 1678 to
// 2262 and a volume is asked to hold instants outside that. A name is a byte sequence,
// because encoding/json substitutes U+FFFD for any byte that is not valid UTF-8 when it
// writes a string, and a replica that recorded such a name would address a node that is
// not there. Anything whose absence is indistinguishable from its zero value travels by
// pointer and is refused when it is missing, because a replica applies what arrives here
// without asking anything back: there is no revalidation behind these messages and no
// timeout that repairs one that was wrong.

// Node is metastore.Node on the wire.
type Node struct {
	ID                int64  `json:"id"`
	Kind              uint8  `json:"kind"`
	Size              int64  `json:"size"`
	AccessTime        Time   `json:"access_time"`
	ModTime           Time   `json:"mod_time"`
	CreationTime      *Time  `json:"creation_time,omitempty"`
	ChangeTime        *Time  `json:"change_time,omitempty"`
	MetadataRevision  uint64 `json:"metadata_revision"`
	DirectoryRevision uint64 `json:"directory_revision"`
	Metadata          []byte `json:"metadata"`
	Content           []byte `json:"content"`
	LinkTarget        []byte `json:"link_target"`
}

func (n *Node) UnmarshalJSON(data []byte) error {
	type node Node
	var decoded node
	if err := decodeMessageJSON(data, &decoded, maximumPageRows); err != nil {
		return err
	}
	got := Node(decoded)
	if err := got.check(); err != nil {
		return err
	}
	*n = got
	return nil
}

func (n Node) check() error {
	if n.ID <= 0 {
		return fmt.Errorf("the node carries invalid identity %d", n.ID)
	}
	if n.Size < 0 {
		return fmt.Errorf("node %d carries negative size %d", n.ID, n.Size)
	}
	if err := storage.NodeKind(n.Kind).Check(); err != nil {
		return fmt.Errorf("node %d carries invalid kind: %w", n.ID, err)
	}
	for _, instant := range []struct {
		name  string
		value *Time
	}{
		{"access", &n.AccessTime}, {"modification", &n.ModTime}, {"creation", n.CreationTime}, {"change", n.ChangeTime},
	} {
		if instant.value != nil {
			if err := checkWireTime(instant.name, *instant.value); err != nil {
				return fmt.Errorf("node %d: %w", n.ID, err)
			}
		}
	}
	if n.MetadataRevision == 0 || (storage.NodeKind(n.Kind) == storage.NodeDirectory) != (n.DirectoryRevision != 0) {
		return fmt.Errorf("node %d carries invalid revision state: %w", n.ID, syscall.EIO)
	}
	if storage.NodeKind(n.Kind) == storage.NodeDirectory && n.Size != 0 {
		return fmt.Errorf("directory %d carries a nonzero size: %w", n.ID, syscall.EIO)
	}
	if storage.NodeKind(n.Kind) == storage.NodeSymlink && n.Size != int64(len(n.LinkTarget)) {
		return fmt.Errorf("link %d size disagrees with target: %w", n.ID, syscall.EIO)
	}
	if err := (metastore.ChangePayloadLengths{Content: int64(len(n.Content)), Metadata: int64(len(n.Metadata)), Target: int64(len(n.LinkTarget))}).Check(); err != nil {
		return err
	}
	if _, err := storage.DecodeMetadata(n.Metadata); err != nil {
		return fmt.Errorf("node %d carries invalid metadata: %w", n.ID, err)
	}
	if len(n.LinkTarget) > storage.MaxLinkTargetBytes {
		return fmt.Errorf("node %d link target exceeds its byte bound: %w", n.ID, syscall.EFBIG)
	}
	if storage.NodeKind(n.Kind) != storage.NodeSymlink && len(n.LinkTarget) != 0 {
		return fmt.Errorf("node %d carries a link target on a non-link: %w", n.ID, syscall.EIO)
	}
	return nil
}

func checkWireTime(name string, instant Time) error {
	if instant.Nanos < 0 || instant.Nanos >= int32(time.Second) {
		return fmt.Errorf("the %s time carries nanoseconds %d outside [0, 1000000000)", name, instant.Nanos)
	}
	return nil
}

// NodeOf validates opaque metadata before allocating its wire representation.
func NodeOf(n metastore.Node) (*Node, error) {
	metadata, err := storage.EncodeMetadata(n.Metadata)
	if err != nil {
		return nil, err
	}
	if err := (metastore.ChangePayloadLengths{Content: int64(len(n.Content)), Metadata: int64(len(metadata)), Target: int64(len(n.LinkTarget))}).Check(); err != nil {
		return nil, err
	}
	wire := nodeShapeOf(n)
	wire.Metadata = metadata
	wire.Content = []byte(n.Content)
	wire.LinkTarget = append([]byte{}, n.LinkTarget...)
	if err := wire.check(); err != nil {
		return nil, err
	}
	return wire, nil
}

// nodeShapeOf contains only fixed-size facts. Frame accounting fills and charges
// every variable byte field separately before asking a source to load it.
func nodeShapeOf(n metastore.Node) *Node {
	wire := &Node{ID: n.ID, Kind: uint8(n.Kind), Size: n.Size,
		AccessTime: TimeOf(n.AccessTime), ModTime: TimeOf(n.ModTime),
		MetadataRevision: uint64(n.MetadataRevision), DirectoryRevision: uint64(n.DirectoryRevision),
		Metadata: []byte{}, Content: []byte{}, LinkTarget: []byte{},
	}
	if n.CreationTime != nil {
		instant := TimeOf(*n.CreationTime)
		wire.CreationTime = &instant
	}
	if n.ChangeTime != nil {
		instant := TimeOf(*n.ChangeTime)
		wire.ChangeTime = &instant
	}
	return wire
}

func (n Node) Metastore() (metastore.Node, error) {
	if err := n.check(); err != nil {
		return metastore.Node{}, err
	}
	metadata, err := storage.DecodeMetadata(n.Metadata)
	if err != nil {
		return metastore.Node{}, err
	}
	node := metastore.Node{ID: n.ID, Kind: storage.NodeKind(n.Kind), Size: n.Size,
		AccessTime: n.AccessTime.Time().UTC(), ModTime: n.ModTime.Time().UTC(),
		MetadataRevision: storage.NodeMetadataRevision(n.MetadataRevision), DirectoryRevision: storage.DirectoryRevision(n.DirectoryRevision),
		Metadata: metadata, Content: metastore.Key(n.Content), LinkTarget: bytes.Clone(n.LinkTarget),
	}
	if n.CreationTime != nil {
		instant := n.CreationTime.Time().UTC()
		node.CreationTime = &instant
	}
	if n.ChangeTime != nil {
		instant := n.ChangeTime.Time().UTC()
		node.ChangeTime = &instant
	}
	return node, nil
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

	// Notification carries canonical historical facts, including the subject and
	// former ancestry of a removal whose Node is absent.
	Notification []byte `json:"notification"`
}

// ChangeOf renders c for the wire. A kind outside the vocabulary is refused rather than
// carried, because the receiving side would have to guess what happened to the name.
func ChangeOf(c metastore.Change) (*Change, error) {
	notification, err := metastore.EncodeNotification(c)
	if err != nil {
		return nil, fmt.Errorf("the change at position %d has invalid notification facts: %w", c.Position, err)
	}
	lengths := metastore.ChangePayloadLengths{Name: int64(len(c.Name)), Notification: int64(len(notification))}
	if c.From != nil {
		lengths.FromName = int64(len(c.From.Name))
	}
	if c.Node != nil {
		metadata, err := storage.EncodeMetadata(c.Node.Metadata)
		if err != nil {
			return nil, err
		}
		lengths.Content = int64(len(c.Node.Content))
		lengths.Metadata = int64(len(metadata))
		lengths.Target = int64(len(c.Node.LinkTarget))
	}
	if err := lengths.Check(); err != nil {
		return nil, err
	}
	wire, err := changeShapeOf(c)
	if err != nil {
		return nil, err
	}
	if c.Node != nil {
		wire.Node, err = NodeOf(*c.Node)
		if err != nil {
			return nil, err
		}
	}
	wire.Notification = notification
	if err := wire.check(); err != nil {
		return nil, fmt.Errorf("the change at position %d cannot be sent: %w", c.Position, err)
	}
	return wire, nil
}

func changeShapeOf(c metastore.Change) (*Change, error) {
	name, known := kindNames[c.Kind]
	if !known {
		return nil, fmt.Errorf("the change at position %d is of kind %d, which this protocol cannot name", c.Position, c.Kind)
	}
	wire := &Change{Position: int64(c.Position), Kind: name, Parent: c.Parent, Name: append([]byte{}, c.Name...)}
	if c.From != nil {
		wire.From = &Location{Parent: c.From.Parent, Name: append([]byte{}, c.From.Name...)}
	}
	if c.Node != nil {
		wire.Node = nodeShapeOf(*c.Node)
	}
	return wire, nil
}

// UnmarshalJSON rejects invalid row shapes and bounds the notification payload.
// Metastore validates its canonical facts before a caller can advance the replica cursor.
func (c *Change) UnmarshalJSON(data []byte) error {
	type change Change
	var decoded change
	if err := decodeMessageJSON(data, &decoded, maximumPageRows); err != nil {
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
	if c.Position <= 0 {
		return fmt.Errorf("the change carries invalid position %d", c.Position)
	}
	if c.Parent < 0 {
		return fmt.Errorf("the change carries negative parent %d", c.Parent)
	}
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
	if c.From != nil && c.From.Parent < 0 {
		return fmt.Errorf("the rename source carries negative parent %d", c.From.Parent)
	}
	if c.Node != nil {
		if err := c.Node.check(); err != nil {
			return err
		}
	}
	if len(c.Notification) == 0 {
		return fmt.Errorf("the change at position %d has no notification facts: %w", c.Position, syscall.EIO)
	}
	if len(c.Notification) > metastore.MaxNotificationBytes {
		return fmt.Errorf("the change at position %d exceeds the notification byte bound: %w", c.Position, syscall.EFBIG)
	}
	return nil
}

// Metastore validates the historical facts against the replication row before returning
// either as committed metadata.
func (c Change) Metastore() (metastore.Change, error) {
	if err := c.check(); err != nil {
		return metastore.Change{}, err
	}
	change := metastore.Change{
		Position: metastore.Position(c.Position),
		Kind:     kindsByName[c.Kind],
		Parent:   c.Parent,
		Name:     c.Name,
	}
	if c.From != nil {
		change.From = &metastore.Location{Parent: c.From.Parent, Name: append([]byte{}, c.From.Name...)}
	}
	if c.Node != nil {
		node, err := c.Node.Metastore()
		if err != nil {
			return metastore.Change{}, err
		}
		change.Node = &node
	}
	notification, err := metastore.DecodeNotification(change, c.Notification)
	if err != nil {
		return metastore.Change{}, fmt.Errorf("the change at position %d has invalid notification facts: %w", c.Position, err)
	}
	change.Notification = notification
	return change, nil
}

// Row is metastore.Row on the wire. The root carries parent 0 and no name.
type Row struct {
	EntryID storage.EntryID `json:"entry_id"`
	Parent  int64           `json:"parent"`
	Name    []byte          `json:"name"`
	Node    *Node           `json:"node"`
}

// RowOf renders r for the wire.
func RowOf(r metastore.Row) (Row, error) {
	if err := (metastore.ChangePayloadLengths{Name: int64(len(r.Name)), Content: int64(len(r.Node.Content)), Target: int64(len(r.Node.LinkTarget))}).Check(); err != nil {
		return Row{}, err
	}
	node, err := NodeOf(r.Node)
	if err != nil {
		return Row{}, err
	}
	if err := (metastore.ChangePayloadLengths{Name: int64(len(r.Name)), Content: int64(len(node.Content)), Metadata: int64(len(node.Metadata)), Target: int64(len(node.LinkTarget))}).Check(); err != nil {
		return Row{}, err
	}
	wire := Row{EntryID: r.EntryID, Parent: r.Parent, Name: append([]byte{}, r.Name...), Node: node}
	if err := wire.check(); err != nil {
		return Row{}, err
	}
	return wire, nil
}

// UnmarshalJSON decodes a row and refuses one that carries no node, which would otherwise
// arrive as a regular file of length zero dated the epoch.
func (r *Row) UnmarshalJSON(data []byte) error {
	type row Row
	var decoded row
	if err := decodeMessageJSON(data, &decoded, maximumPageRows); err != nil {
		return err
	}
	if err := Row(decoded).check(); err != nil {
		return err
	}
	*r = Row(decoded)
	return nil
}

// Metastore returns the row r carries.
func (r Row) Metastore() (metastore.Row, error) {
	if err := r.check(); err != nil {
		return metastore.Row{}, err
	}
	node, err := r.Node.Metastore()
	if err != nil {
		return metastore.Row{}, err
	}
	name := r.Name
	if r.Parent == 0 {
		name = nil
	}
	return metastore.Row{EntryID: r.EntryID, Parent: r.Parent, Name: name, Node: node}, nil
}

func (r Row) check() error {
	if r.Node == nil {
		return fmt.Errorf("snapshot row carries no node: %w", syscall.EIO)
	}
	if err := r.Node.check(); err != nil {
		return err
	}
	if r.Parent < 0 {
		return fmt.Errorf("snapshot row carries negative parent: %w", syscall.EIO)
	}
	if r.Parent == 0 {
		if r.EntryID != 0 || len(r.Name) != 0 || storage.NodeKind(r.Node.Kind) != storage.NodeDirectory {
			return fmt.Errorf("snapshot root is not an unnamed directory: %w", syscall.EIO)
		}
	} else if r.EntryID == 0 || len(r.Name) == 0 || bytes.Equal(r.Name, []byte(".")) || bytes.Equal(r.Name, []byte("..")) || bytes.ContainsAny(r.Name, "/\x00") || r.Parent == r.Node.ID {
		return fmt.Errorf("snapshot row carries an invalid entry: %w", syscall.EIO)
	}
	return nil
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
	// many. The volume changes faster than the log was configured to hold, which is a
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
	if err := decodeMessageJSON(data, &decoded, maximumPageRows); err != nil {
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
	if *got.Position < 0 || *got.Tail < 0 {
		return fmt.Errorf("the stream carries negative position %d or tail %d", *got.Position, *got.Tail)
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
	// volume nothing has yet changed is at zero — so a lost figure would arrive as an
	// ordinary answer, and a replica that seeded itself at zero would replay changes it
	// already holds.
	Position *int64 `json:"position"`
}

// UnmarshalJSON decodes the frame and refuses one carrying no position.
func (o *SnapshotOpen) UnmarshalJSON(data []byte) error {
	type open SnapshotOpen
	var decoded open
	if err := decodeMessageJSON(data, &decoded, maximumPageRows); err != nil {
		return err
	}
	if decoded.Position == nil {
		return errors.New("the snapshot does not say what position it was taken at")
	}
	if *decoded.Position < 0 {
		return fmt.Errorf("the snapshot carries negative position %d", *decoded.Position)
	}
	*o = SnapshotOpen(decoded)
	return nil
}

// SnapshotPage is one page of a snapshot's rows.
type SnapshotPage struct {
	Rows []Row `json:"rows"`
}

// UnmarshalJSON decodes a page and refuses one carrying no rows. Such a page makes no
// snapshot progress, and an endless sequence could keep the stream's silence timer alive
// while preventing the replica from ever completing its seed.
func (p *SnapshotPage) UnmarshalJSON(data []byte) error {
	type page SnapshotPage
	var decoded page
	if err := decodeMessageJSON(data, &decoded, maximumPageRows); err != nil {
		return err
	}
	if len(decoded.Rows) == 0 {
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
	// Errno is absent for generic faults. Authorization faults carry only EACCES
	// or EIO; a present null, empty, or unknown value is a protocol failure.
	Errno *string `json:"errno,omitempty"`
}

func (f *StreamFault) UnmarshalJSON(data []byte) error {
	type fault StreamFault
	var decoded fault
	if err := decodeMessageJSON(data, &decoded, maximumPageRows); err != nil {
		return err
	}
	if decoded.Errno != nil && *decoded.Errno != "EACCES" && *decoded.Errno != "EIO" {
		return errors.New("the stream fault carries an unsupported errno")
	}
	*f = StreamFault(decoded)
	return nil
}
