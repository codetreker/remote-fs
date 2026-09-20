package sqlite

import (
	"database/sql"
	"fmt"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

type scanner interface{ Scan(...any) error }

var nodeHeaderColumns = fmt.Sprintf(`
	CASE WHEN typeof(n.id)='integer' THEN n.id END,
	CASE WHEN typeof(n.kind)='integer' THEN n.kind END,
	CASE WHEN typeof(n.size)='integer' THEN n.size END,
	CASE WHEN typeof(n.atime_sec)='integer' THEN n.atime_sec END,
	CASE WHEN typeof(n.atime_nsec)='integer' THEN n.atime_nsec END,
	CASE WHEN typeof(n.mtime_sec)='integer' THEN n.mtime_sec END,
	CASE WHEN typeof(n.mtime_nsec)='integer' THEN n.mtime_nsec END,
	CASE WHEN typeof(n.birth_sec)='integer' THEN n.birth_sec END,
	CASE WHEN typeof(n.birth_nsec)='integer' THEN n.birth_nsec END,
	CASE WHEN typeof(n.change_sec)='integer' THEN n.change_sec END,
	CASE WHEN typeof(n.change_nsec)='integer' THEN n.change_nsec END,
	CASE WHEN typeof(n.content) IN ('text','null') THEN coalesce(length(CAST(n.content AS BLOB)),0) ELSE -1 END,
	CASE WHEN typeof(n.metadata)='blob' THEN length(n.metadata) ELSE -1 END,
	CASE WHEN typeof(n.birth_sec) IN ('integer','null') AND typeof(n.birth_nsec) IN ('integer','null')
		AND typeof(n.change_sec) IN ('integer','null') AND typeof(n.change_nsec) IN ('integer','null')
		AND (n.content IS NULL OR (typeof(n.content)='text' AND length(CAST(n.content AS BLOB))>0))
		AND typeof(n.metadata)='blob' AND length(n.metadata)<=%d THEN 1 ELSE 0 END`, storage.MaxMetadataBytes)

var nodeColumns = nodeHeaderColumns + fmt.Sprintf(`,
	CASE WHEN typeof(n.content)='text' THEN n.content END,
	CASE WHEN typeof(n.metadata)='blob' AND length(n.metadata)<=%d THEN n.metadata END`, storage.MaxMetadataBytes)

var nodeAttrColumns = nodeHeaderColumns

type nodeHeader struct {
	id, kind, size        int64
	atimeSec, atimeNsec   int64
	mtimeSec, mtimeNsec   int64
	birthSec, birthNsec   sql.NullInt64
	changeSec, changeNsec sql.NullInt64
	contentBytes          int64
	metadataBytes         int64
	valid                 int64
}

type nodeAttrScan = nodeHeader

func (s *nodeHeader) fields() []any {
	return []any{&s.id, &s.kind, &s.size, &s.atimeSec, &s.atimeNsec, &s.mtimeSec, &s.mtimeNsec,
		&s.birthSec, &s.birthNsec, &s.changeSec, &s.changeNsec, &s.contentBytes, &s.metadataBytes, &s.valid}
}

func (s *nodeHeader) node() (metastore.Node, error) {
	if s.valid != 1 || s.id < 1 || s.kind < int64(storage.NodeRegular) || s.kind > int64(storage.NodeSymlink) || s.size < 0 ||
		s.atimeNsec < 0 || s.atimeNsec >= int64(time.Second) || s.mtimeNsec < 0 || s.mtimeNsec >= int64(time.Second) ||
		s.contentBytes < 0 || s.metadataBytes < 6 {
		return metastore.Node{}, fmt.Errorf("invalid stored node metadata: %w", syscall.EIO)
	}
	if s.metadataBytes > storage.MaxMetadataBytes {
		return metastore.Node{}, fmt.Errorf("stored node metadata exceeds its bound: %w", syscall.EFBIG)
	}
	kind := storage.NodeKind(s.kind)
	if kind == storage.NodeDirectory && (s.size != 0 || s.contentBytes != 0) ||
		kind == storage.NodeSymlink && s.contentBytes != 0 {
		return metastore.Node{}, fmt.Errorf("stored node kind and content disagree: %w", syscall.EIO)
	}
	birth, err := optionalStoredTime(s.birthSec, s.birthNsec)
	if err != nil {
		return metastore.Node{}, err
	}
	changed, err := optionalStoredTime(s.changeSec, s.changeNsec)
	if err != nil {
		return metastore.Node{}, err
	}
	return metastore.Node{ID: s.id, Kind: kind, Size: s.size,
		AccessTime: sqlvalue.LoadedTime(s.atimeSec, int32(s.atimeNsec)), ModTime: sqlvalue.LoadedTime(s.mtimeSec, int32(s.mtimeNsec)),
		BirthTime: birth, ChangeTime: changed}, nil
}

func optionalStoredTime(sec, nsec sql.NullInt64) (*time.Time, error) {
	if sec.Valid != nsec.Valid || nsec.Valid && (nsec.Int64 < 0 || nsec.Int64 >= int64(time.Second)) {
		return nil, fmt.Errorf("incomplete or invalid stored time: %w", syscall.EIO)
	}
	if !sec.Valid {
		return nil, nil
	}
	value := sqlvalue.LoadedTime(sec.Int64, int32(nsec.Int64)).UTC()
	return &value, nil
}

func (s *nodeHeader) attr() (storage.Attr, error) {
	node, err := s.node()
	return node.Attr(), err
}

type nodeScan struct {
	nodeHeader
	content  sql.NullString
	metadata []byte
}

func (s *nodeScan) fields() []any {
	return append(s.nodeHeader.fields(), &s.content, &s.metadata)
}

func (s *nodeScan) node() (metastore.Node, error) {
	node, err := s.nodeHeader.node()
	if err != nil {
		return metastore.Node{}, err
	}
	if int64(len(s.content.String)) != s.contentBytes || int64(len(s.metadata)) != s.metadataBytes {
		return metastore.Node{}, fmt.Errorf("stored node payload changed after admission: %w", syscall.EIO)
	}
	metadata, err := storage.DecodeMetadata(s.metadata)
	if err != nil {
		return metastore.Node{}, err
	}
	node.Content, node.Metadata = metastore.Key(s.content.String), metadata
	return node, nil
}

func scanNode(row scanner) (metastore.Node, error) {
	var node nodeScan
	if err := row.Scan(node.fields()...); err != nil {
		return metastore.Node{}, err
	}
	return node.node()
}
