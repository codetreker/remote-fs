package changes

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/fs"
	"math"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// changeMetadataColumns carries every scalar beside its SQLite storage class and only the
// lengths of variable payloads. We validate those facts before asking SQLite to copy a
// BLOB or TEXT value into Go memory.
const changeMetadataColumns = `
	CASE WHEN typeof(position) = 'integer' THEN position END, typeof(position),
	CASE WHEN typeof(previous_position) = 'integer' THEN previous_position END, typeof(previous_position),
	CASE WHEN typeof(namespace) = 'integer' THEN namespace END, typeof(namespace),
	CASE WHEN typeof(kind) = 'integer' THEN kind END, typeof(kind),
	CASE WHEN typeof(parent) = 'integer' THEN parent END, typeof(parent),
	COALESCE(length(CAST(name AS BLOB)), 0), typeof(name),
	CASE WHEN typeof(from_parent) IN ('integer', 'null') THEN from_parent END, typeof(from_parent),
	COALESCE(length(CAST(from_name AS BLOB)), 0), typeof(from_name),
	CASE WHEN typeof(node) IN ('integer', 'null') THEN node END, typeof(node),
	CASE WHEN typeof(mode) IN ('integer', 'null') THEN mode END, typeof(mode),
	CASE WHEN typeof(size) IN ('integer', 'null') THEN size END, typeof(size),
	CASE WHEN typeof(atime_sec) IN ('integer', 'null') THEN atime_sec END, typeof(atime_sec),
	CASE WHEN typeof(atime_nsec) IN ('integer', 'null') THEN atime_nsec END, typeof(atime_nsec),
	CASE WHEN typeof(mtime_sec) IN ('integer', 'null') THEN mtime_sec END, typeof(mtime_sec),
	CASE WHEN typeof(mtime_nsec) IN ('integer', 'null') THEN mtime_nsec END, typeof(mtime_nsec),
	COALESCE(length(CAST(content AS BLOB)), 0), typeof(content),
	CASE WHEN typeof(recorded_sec) = 'integer' THEN recorded_sec END, typeof(recorded_sec),
	CASE WHEN typeof(recorded_nsec) = 'integer' THEN recorded_nsec END, typeof(recorded_nsec)`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanChangeMetadata(
	row rowScanner,
	expectedNamespace int64,
) (metastore.Change, metastore.ChangePayloadLengths, int64, error) {
	var (
		positionRaw, previousRaw, namespaceRaw, kindRaw, parentRaw      any
		fromParentRaw, idRaw, modeRaw, sizeRaw                          any
		atimeSecRaw, atimeNsecRaw, mtimeSecRaw, mtimeNsecRaw            any
		recordedSecRaw, recordedNsecRaw                                 any
		positionType, previousType, namespaceType, kindType, parentType string
		nameType, fromParentType, fromNameType                          string
		idType, modeType, sizeType                                      string
		atimeSecType, atimeNsecType, mtimeSecType, mtimeNsecType        string
		contentType, recordedSecType, recordedNsecType                  string
		lengths                                                         metastore.ChangePayloadLengths
	)
	if err := row.Scan(
		&positionRaw, &positionType, &previousRaw, &previousType,
		&namespaceRaw, &namespaceType, &kindRaw, &kindType,
		&parentRaw, &parentType, &lengths.Name, &nameType,
		&fromParentRaw, &fromParentType, &lengths.FromName, &fromNameType,
		&idRaw, &idType, &modeRaw, &modeType, &sizeRaw, &sizeType,
		&atimeSecRaw, &atimeSecType, &atimeNsecRaw, &atimeNsecType,
		&mtimeSecRaw, &mtimeSecType, &mtimeNsecRaw, &mtimeNsecType,
		&lengths.Content, &contentType,
		&recordedSecRaw, &recordedSecType, &recordedNsecRaw, &recordedNsecType,
	); err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	position, err := requiredStoredInteger("position", positionRaw, positionType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	previous, err := requiredStoredInteger("previous_position", previousRaw, previousType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	namespace, err := requiredStoredInteger("namespace", namespaceRaw, namespaceType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	kindValue, err := requiredStoredInteger("kind", kindRaw, kindType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	parent, err := requiredStoredInteger("parent", parentRaw, parentType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	fromParent, ok := nullableStoredInteger(fromParentRaw, fromParentType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, invalidStoredChangeScalar(position, "from_parent", fromParentType)
	}
	id, ok := nullableStoredInteger(idRaw, idType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, invalidStoredChangeScalar(position, "node", idType)
	}
	mode, ok := nullableStoredInteger(modeRaw, modeType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, invalidStoredChangeScalar(position, "mode", modeType)
	}
	size, ok := nullableStoredInteger(sizeRaw, sizeType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, invalidStoredChangeScalar(position, "size", sizeType)
	}
	atimeSec, ok := nullableStoredInteger(atimeSecRaw, atimeSecType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, invalidStoredChangeScalar(position, "atime_sec", atimeSecType)
	}
	atimeNsec, ok := nullableStoredInteger(atimeNsecRaw, atimeNsecType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, invalidStoredChangeScalar(position, "atime_nsec", atimeNsecType)
	}
	mtimeSec, ok := nullableStoredInteger(mtimeSecRaw, mtimeSecType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, invalidStoredChangeScalar(position, "mtime_sec", mtimeSecType)
	}
	mtimeNsec, ok := nullableStoredInteger(mtimeNsecRaw, mtimeNsecType)
	if !ok {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, invalidStoredChangeScalar(position, "mtime_nsec", mtimeNsecType)
	}
	recordedSec, err := requiredStoredInteger("recorded_sec", recordedSecRaw, recordedSecType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	recordedNsec, err := requiredStoredInteger("recorded_nsec", recordedNsecRaw, recordedNsecType)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	loaded, err := loadedKind(kindValue)
	if err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	change := metastore.Change{Position: metastore.Position(position), Kind: loaded, Parent: parent}
	if nameType == "blob" {
		change.Name = []byte{}
	}
	if fromParent.Valid {
		change.From = &metastore.Location{Parent: fromParent.Int64}
		if fromNameType == "blob" {
			change.From.Name = []byte{}
		}
	}
	if id.Valid {
		change.Node = &metastore.Node{
			ID:         id.Int64,
			Mode:       fs.FileMode(mode.Int64),
			Size:       size.Int64,
			AccessTime: sqlvalue.LoadedTime(atimeSec.Int64, int32(atimeNsec.Int64)),
			ModTime:    sqlvalue.LoadedTime(mtimeSec.Int64, int32(mtimeNsec.Int64)),
		}
	}
	if err := validateChangeMetadata(change, lengths, namespace, expectedNamespace,
		nameType, fromNameType, contentType, id, mode, size, atimeSec, atimeNsec,
		mtimeSec, mtimeNsec, recordedSec, recordedNsec); err != nil {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0, err
	}
	if previous < 0 || previous >= position {
		return metastore.Change{}, metastore.ChangePayloadLengths{}, 0,
			fmt.Errorf("%w: change %d has invalid predecessor %d", syscall.EIO, position, previous)
	}
	return change, lengths, previous, nil
}

func requiredStoredInteger(column string, value any, storageClass string) (int64, error) {
	integer, ok := sqlvalue.StoredInteger(value, storageClass)
	if !ok {
		return 0, invalidStoredChangeScalar(0, column, storageClass)
	}
	return integer, nil
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
		return fmt.Errorf("%w: change %d stores %s as %s", syscall.EIO, position, column, storageClass)
	}
	return fmt.Errorf("%w: a change stores %s as %s", syscall.EIO, column, storageClass)
}

func validateChangeMetadata(
	change metastore.Change,
	lengths metastore.ChangePayloadLengths,
	namespace, expectedNamespace int64,
	nameType, fromNameType, contentType string,
	id, mode, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec sql.NullInt64,
	recordedSec, recordedNsec int64,
) error {
	if change.Position <= 0 || namespace <= 0 || namespace != expectedNamespace || change.Parent < 0 ||
		recordedNsec < 0 || recordedNsec >= int64(time.Second) {
		return fmt.Errorf("%w: change position %d has invalid identity, parent, or recorded time", syscall.EIO, change.Position)
	}
	if nameType != "null" && nameType != "blob" {
		return fmt.Errorf("%w: change %d stores its name as %s", syscall.EIO, change.Position, nameType)
	}
	if fromNameType != "null" && fromNameType != "blob" {
		return fmt.Errorf("%w: change %d stores its source name as %s", syscall.EIO, change.Position, fromNameType)
	}
	if contentType != "null" && contentType != "text" {
		return fmt.Errorf("%w: change %d stores its content key as %s", syscall.EIO, change.Position, contentType)
	}
	if lengths.Name < 0 || lengths.FromName < 0 || lengths.Content < 0 {
		return fmt.Errorf("%w: change %d has a negative payload length", syscall.EIO, change.Position)
	}
	wantFrom := change.Kind == metastore.Renamed
	if wantFrom != (change.From != nil) {
		return fmt.Errorf("%w: change %d has an incomplete source location", syscall.EIO, change.Position)
	}
	wantNode := change.Kind != metastore.Removed
	nodeFields := []sql.NullInt64{id, mode, size, atimeSec, atimeNsec, mtimeSec, mtimeNsec}
	for _, field := range nodeFields {
		if field.Valid != wantNode {
			return fmt.Errorf("%w: change %d has an incomplete node", syscall.EIO, change.Position)
		}
	}
	if !wantNode {
		if contentType != "null" {
			return fmt.Errorf("%w: removed change %d carries content", syscall.EIO, change.Position)
		}
	} else {
		if contentType == "text" && lengths.Content == 0 {
			return fmt.Errorf("%w: change %d carries an empty content key", syscall.EIO, change.Position)
		}
		if id.Int64 <= 0 || size.Int64 < 0 || mode.Int64 < 0 || mode.Int64 > math.MaxUint32 ||
			atimeNsec.Int64 < 0 || atimeNsec.Int64 >= int64(time.Second) ||
			mtimeNsec.Int64 < 0 || mtimeNsec.Int64 >= int64(time.Second) {
			return fmt.Errorf("%w: change %d carries invalid node metadata", syscall.EIO, change.Position)
		}
		if nodeType := fs.FileMode(mode.Int64).Type(); nodeType != 0 && nodeType != fs.ModeDir {
			return fmt.Errorf("%w: change %d carries unsupported node type %v", syscall.EIO, change.Position, fs.FileMode(mode.Int64).Type())
		}
		if fs.FileMode(mode.Int64).IsDir() && (size.Int64 != 0 || contentType != "null") {
			return fmt.Errorf("%w: change %d carries bytes for a directory", syscall.EIO, change.Position)
		}
		if fs.FileMode(mode.Int64).Type() == 0 && contentType == "null" && size.Int64 != 0 {
			return fmt.Errorf("%w: change %d carries file bytes without a content key", syscall.EIO, change.Position)
		}
	}
	switch change.Kind {
	case metastore.Created, metastore.Removed:
		if change.Parent <= 0 || nameType != "blob" || lengths.Name <= 0 {
			return fmt.Errorf("%w: change %d has no addressable destination", syscall.EIO, change.Position)
		}
	case metastore.Modified:
		root := change.Parent == 0 && nameType == "null" && lengths.Name == 0
		named := change.Parent > 0 && nameType == "blob" && lengths.Name > 0
		if !root && !named {
			return fmt.Errorf("%w: modified change %d has no valid location", syscall.EIO, change.Position)
		}
	case metastore.Renamed:
		if change.Parent <= 0 || nameType != "blob" || lengths.Name <= 0 ||
			change.From == nil || change.From.Parent <= 0 || fromNameType != "blob" || lengths.FromName <= 0 {
			return fmt.Errorf("%w: renamed change %d has an incomplete location", syscall.EIO, change.Position)
		}
	}
	return nil
}

func validateChangePayload(change metastore.Change, name, fromName []byte, content sql.NullString) error {
	if change.Name != nil && !validStoredComponent(name) {
		return fmt.Errorf("%w: change %d carries an invalid destination name", syscall.EIO, change.Position)
	}
	if change.From != nil && !validStoredComponent(fromName) {
		return fmt.Errorf("%w: change %d carries an invalid source name", syscall.EIO, change.Position)
	}
	if content.Valid && content.String == "" {
		return fmt.Errorf("%w: change %d carries an empty content key", syscall.EIO, change.Position)
	}
	return nil
}

func validStoredComponent(name []byte) bool {
	return len(name) != 0 && !bytes.Equal(name, []byte(".")) && !bytes.Equal(name, []byte("..")) &&
		bytes.IndexByte(name, '/') < 0 && bytes.IndexByte(name, 0) < 0
}
