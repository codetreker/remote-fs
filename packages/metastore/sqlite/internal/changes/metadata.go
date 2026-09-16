package changes

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

var changeScalarNames = []string{"position", "previous_position", "identity_high_water", "volume", "kind", "parent", "from_parent", "node", "node_kind", "size", "atime_sec", "atime_nsec", "mtime_sec", "mtime_nsec", "creation_sec", "creation_nsec", "change_sec", "change_nsec", "metadata_revision", "directory_revision", "recorded_sec", "recorded_nsec"}
var changePayloadNames = []string{"name", "from_name", "content", "metadata", "link_target", "notification"}

// Scalar classes and payload lengths are admitted before SQLite copies any
// variable-length value into the result.
var changeMetadataColumns = func() string {
	var columns []string
	for _, name := range changeScalarNames {
		columns = append(columns, "CASE WHEN typeof("+name+")='integer' THEN "+name+" END", "typeof("+name+")")
	}
	for _, name := range changePayloadNames {
		columns = append(columns, "COALESCE(length(CAST("+name+" AS BLOB)),0)", "typeof("+name+")")
	}
	return strings.Join(columns, ",")
}()

type rowScanner interface{ Scan(...any) error }

type storedChangeScalar struct {
	raw   any
	class string
}

func scanChangeMetadata(row rowScanner, expectedVolume int64) (metastore.Change, metastore.ChangePayloadLengths, int64, int64, error) {
	scalars := make([]storedChangeScalar, len(changeScalarNames))
	var payloadLengths [6]int64
	var payloadTypes [6]string
	var destinations []any
	for i := range scalars {
		destinations = append(destinations, &scalars[i].raw, &scalars[i].class)
	}
	for i := range payloadLengths {
		destinations = append(destinations, &payloadLengths[i], &payloadTypes[i])
	}
	empty := metastore.Change{}
	lengths := metastore.ChangePayloadLengths{}
	if err := row.Scan(destinations...); err != nil {
		return empty, lengths, 0, 0, err
	}
	lengths = metastore.ChangePayloadLengths{Name: payloadLengths[0], FromName: payloadLengths[1], Content: payloadLengths[2], Metadata: payloadLengths[3], Target: payloadLengths[4], Notification: payloadLengths[5]}
	if err := lengths.Check(); err != nil {
		return empty, lengths, 0, 0, err
	}
	if payloadTypes[5] != "blob" || lengths.Notification <= 0 {
		return empty, lengths, 0, 0, fmt.Errorf("invalid notification storage: %w", syscall.EIO)
	}
	values := make(map[string]sql.NullInt64, len(scalars))
	for i := range scalars {
		value, ok := nullableStoredInteger(scalars[i].raw, scalars[i].class)
		if !ok {
			return empty, lengths, 0, 0, invalidStoredChangeScalar(values["position"].Int64, changeScalarNames[i], scalars[i].class)
		}
		values[changeScalarNames[i]] = value
	}
	for _, name := range []string{"position", "previous_position", "identity_high_water", "volume", "kind", "parent", "recorded_sec", "recorded_nsec"} {
		if !values[name].Valid {
			return empty, lengths, 0, 0, invalidStoredChangeScalar(0, name, "null")
		}
	}
	position, previous, volume := values["position"].Int64, values["previous_position"].Int64, values["volume"].Int64
	parent := values["parent"].Int64
	if values["identity_high_water"].Int64 <= 0 || position <= 0 || previous < 0 || previous >= position || volume <= 0 || expectedVolume != 0 && volume != expectedVolume || parent < 0 || !validNanosecond(values["recorded_nsec"].Int64) {
		return empty, lengths, 0, 0, fmt.Errorf("change %d has invalid identity, predecessor, parent or recorded time: %w", position, syscall.EIO)
	}
	kind, err := loadedKind(values["kind"].Int64)
	if err != nil {
		return empty, lengths, 0, 0, err
	}
	change := metastore.Change{Position: metastore.Position(position), Kind: kind, Parent: parent}
	if payloadTypes[0] == "blob" {
		change.Name = []byte{}
	}
	if values["from_parent"].Valid {
		change.From = &metastore.Location{Parent: values["from_parent"].Int64}
		if payloadTypes[1] == "blob" {
			change.From.Name = []byte{}
		}
	}
	if err := validateLocationMetadata(change, lengths, payloadTypes[0], payloadTypes[1]); err != nil {
		return empty, lengths, 0, 0, err
	}
	node, err := scanNodeMetadata(values, kind, payloadTypes, lengths)
	if err != nil {
		return empty, lengths, 0, 0, err
	}
	change.Node = node
	return change, lengths, previous, values["identity_high_water"].Int64, nil
}

func scanNodeMetadata(values map[string]sql.NullInt64, kind metastore.ChangeKind, types [6]string, lengths metastore.ChangePayloadLengths) (*metastore.Node, error) {
	required := []string{"node", "node_kind", "size", "atime_sec", "atime_nsec", "mtime_sec", "mtime_nsec", "metadata_revision", "directory_revision"}
	want := kind != metastore.Removed
	for _, name := range required {
		if values[name].Valid != want {
			return nil, fmt.Errorf("change has incomplete node field %s: %w", name, syscall.EIO)
		}
	}
	if !want {
		for _, name := range []string{"creation_sec", "creation_nsec", "change_sec", "change_nsec"} {
			if values[name].Valid {
				return nil, fmt.Errorf("removed change carries %s: %w", name, syscall.EIO)
			}
		}
		if types[2] != "null" || types[3] != "null" || types[4] != "null" {
			return nil, fmt.Errorf("removed change carries node payload: %w", syscall.EIO)
		}
		return nil, nil
	}
	nodeKind := values["node_kind"].Int64
	if values["node"].Int64 <= 0 || nodeKind < int64(storage.NodeRegular) || nodeKind > int64(storage.NodeSymlink) || values["size"].Int64 < 0 || values["metadata_revision"].Int64 <= 0 || values["directory_revision"].Int64 < 0 || !validNanosecond(values["atime_nsec"].Int64) || !validNanosecond(values["mtime_nsec"].Int64) {
		return nil, fmt.Errorf("change carries invalid node metadata: %w", syscall.EIO)
	}
	creation, err := optionalStoredTime(values, "creation")
	if err != nil {
		return nil, err
	}
	changed, err := optionalStoredTime(values, "change")
	if err != nil {
		return nil, err
	}
	node := &metastore.Node{ID: values["node"].Int64, Kind: storage.NodeKind(nodeKind), Size: values["size"].Int64,
		AccessTime: sqlvalue.LoadedTime(values["atime_sec"].Int64, int32(values["atime_nsec"].Int64)), ModTime: sqlvalue.LoadedTime(values["mtime_sec"].Int64, int32(values["mtime_nsec"].Int64)),
		CreationTime: creation, ChangeTime: changed, MetadataRevision: storage.NodeMetadataRevision(values["metadata_revision"].Int64), DirectoryRevision: storage.DirectoryRevision(values["directory_revision"].Int64)}
	if (node.Kind == storage.NodeDirectory) != (node.DirectoryRevision != 0) || types[3] != "blob" || types[4] != "blob" || lengths.Metadata < 6 || types[2] != "null" && types[2] != "text" || types[2] == "text" && lengths.Content == 0 {
		return nil, fmt.Errorf("change carries invalid node payload classes or revisions: %w", syscall.EIO)
	}
	if node.Kind == storage.NodeSymlink {
		if types[4] != "blob" || types[2] != "null" || node.Size != lengths.Target {
			return nil, fmt.Errorf("change carries invalid symlink data: %w", syscall.EIO)
		}
	} else if lengths.Target != 0 {
		return nil, fmt.Errorf("change carries target for a non-link node: %w", syscall.EIO)
	}
	if node.Kind == storage.NodeDirectory && (node.Size != 0 || types[2] != "null") || node.Kind == storage.NodeRegular && types[2] == "null" && node.Size != 0 {
		return nil, fmt.Errorf("change carries invalid content for its node kind: %w", syscall.EIO)
	}
	return node, nil
}

func optionalStoredTime(values map[string]sql.NullInt64, prefix string) (*time.Time, error) {
	sec, nsec := values[prefix+"_sec"], values[prefix+"_nsec"]
	if sec.Valid != nsec.Valid || nsec.Valid && !validNanosecond(nsec.Int64) {
		return nil, fmt.Errorf("change carries invalid %s time: %w", prefix, syscall.EIO)
	}
	if !sec.Valid {
		return nil, nil
	}
	instant := sqlvalue.LoadedTime(sec.Int64, int32(nsec.Int64))
	return &instant, nil
}

func validNanosecond(value int64) bool { return value >= 0 && value < int64(time.Second) }

func validateLocationMetadata(change metastore.Change, lengths metastore.ChangePayloadLengths, nameType, fromType string) error {
	if nameType != "null" && nameType != "blob" || fromType != "null" && fromType != "blob" {
		return fmt.Errorf("change stores name with invalid storage class: %w", syscall.EIO)
	}
	if (change.Kind == metastore.Renamed) != (change.From != nil) || change.From == nil && (fromType != "null" || lengths.FromName != 0) {
		return fmt.Errorf("change has incomplete source location: %w", syscall.EIO)
	}
	named := change.Parent > 0 && nameType == "blob" && lengths.Name > 0
	switch change.Kind {
	case metastore.Created, metastore.Removed:
		if !named {
			return fmt.Errorf("change has no addressable destination: %w", syscall.EIO)
		}
	case metastore.Modified:
		if !named && !(change.Parent == 0 && nameType == "null" && lengths.Name == 0) {
			return fmt.Errorf("modified change has no valid location: %w", syscall.EIO)
		}
	case metastore.Renamed:
		if !named || change.From.Parent <= 0 || fromType != "blob" || lengths.FromName <= 0 {
			return fmt.Errorf("renamed change has an incomplete location: %w", syscall.EIO)
		}
	}
	return nil
}

func nullableStoredInteger(value any, storageClass string) (sql.NullInt64, bool) {
	if storageClass == "null" && value == nil {
		return sql.NullInt64{}, true
	}
	integer, ok := sqlvalue.StoredInteger(value, storageClass)
	return sql.NullInt64{Int64: integer, Valid: ok}, ok
}
func invalidStoredChangeScalar(position int64, column, storageClass string) error {
	if position > 0 {
		return fmt.Errorf("change %d stores %s as %s: %w", position, column, storageClass, syscall.EIO)
	}
	return fmt.Errorf("a change stores %s as %s: %w", column, storageClass, syscall.EIO)
}

func validateChangePayload(change metastore.Change, name, fromName []byte, content sql.NullString) error {
	if change.Name != nil && !validStoredComponent(name) {
		return fmt.Errorf("change %d carries an invalid destination name: %w", change.Position, syscall.EIO)
	}
	if change.From != nil && !validStoredComponent(fromName) {
		return fmt.Errorf("change %d carries an invalid source name: %w", change.Position, syscall.EIO)
	}
	if content.Valid && content.String == "" {
		return fmt.Errorf("change %d carries an empty content key: %w", change.Position, syscall.EIO)
	}
	return nil
}
func validStoredComponent(name []byte) bool {
	return len(name) > 0 && len(name) <= storage.MaxEntryNameBytes && !bytes.Equal(name, []byte(".")) && !bytes.Equal(name, []byte("..")) && bytes.IndexByte(name, '/') < 0 && bytes.IndexByte(name, 0) < 0
}
