package sqlite

import (
	"bytes"
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
	CASE WHEN typeof(n.directory_revision)='blob' AND length(n.directory_revision)<=%d THEN n.directory_revision END,
	CASE WHEN typeof(n.content) IN ('text','null') THEN coalesce(length(CAST(n.content AS BLOB)),0) ELSE -1 END,
	CASE WHEN typeof(n.metadata)='blob' THEN length(n.metadata) ELSE -1 END,
	CASE WHEN typeof(n.link_target)='blob' THEN length(n.link_target) ELSE -1 END,
	CASE WHEN typeof(n.birth_sec) IN ('integer','null') AND typeof(n.birth_nsec) IN ('integer','null')
		AND typeof(n.change_sec) IN ('integer','null') AND typeof(n.change_nsec) IN ('integer','null')
		AND (n.content IS NULL OR (typeof(n.content)='text' AND length(CAST(n.content AS BLOB))>0))
		AND typeof(n.metadata)='blob' AND length(n.metadata)<=%d
		AND typeof(n.link_target)='blob' AND length(n.link_target)<=%d
		AND typeof(n.directory_revision)='blob' AND length(n.directory_revision)<=%d THEN 1 ELSE 0 END`,
	storage.MaxObservationTokenBytes, storage.MaxMetadataBytes, storage.MaxLinkTargetBytes, storage.MaxObservationTokenBytes)

var nodeColumns = nodeHeaderColumns + fmt.Sprintf(`,
	CASE WHEN typeof(n.content)='text' THEN n.content END,
	CASE WHEN typeof(n.metadata)='blob' AND length(n.metadata)<=%d THEN n.metadata END,
	CASE WHEN typeof(n.link_target)='blob' AND length(n.link_target)<=%d THEN n.link_target END`, storage.MaxMetadataBytes, storage.MaxLinkTargetBytes)

var nodeAttrColumns = nodeHeaderColumns

type nodeHeader struct {
	id, kind, size        int64
	atimeSec, atimeNsec   int64
	mtimeSec, mtimeNsec   int64
	birthSec, birthNsec   sql.NullInt64
	changeSec, changeNsec sql.NullInt64
	directoryRevision     []byte
	contentBytes          int64
	metadataBytes         int64
	targetBytes           int64
	valid                 int64
}

type nodeAttrScan = nodeHeader

func (s *nodeHeader) fields() []any {
	return []any{&s.id, &s.kind, &s.size, &s.atimeSec, &s.atimeNsec, &s.mtimeSec, &s.mtimeNsec,
		&s.birthSec, &s.birthNsec, &s.changeSec, &s.changeNsec, &s.directoryRevision,
		&s.contentBytes, &s.metadataBytes, &s.targetBytes, &s.valid}
}

func (s *nodeHeader) node() (metastore.Node, error) {
	if s.valid != 1 || s.id < 1 || s.kind < int64(storage.NodeRegular) || s.kind > int64(storage.NodeSymlink) || s.size < 0 ||
		s.atimeNsec < 0 || s.atimeNsec >= int64(time.Second) || s.mtimeNsec < 0 || s.mtimeNsec >= int64(time.Second) ||
		s.contentBytes < 0 || s.metadataBytes < 6 || s.targetBytes < 0 {
		return metastore.Node{}, fmt.Errorf("invalid stored node metadata: %w", syscall.EIO)
	}
	if s.metadataBytes > storage.MaxMetadataBytes || s.targetBytes > storage.MaxLinkTargetBytes {
		return metastore.Node{}, fmt.Errorf("stored node metadata exceeds its bound: %w", syscall.EFBIG)
	}
	kind := storage.NodeKind(s.kind)
	if kind == storage.NodeDirectory && (s.size != 0 || s.contentBytes != 0 || len(s.directoryRevision) == 0) ||
		kind != storage.NodeDirectory && len(s.directoryRevision) != 0 ||
		kind == storage.NodeSymlink && (s.targetBytes == 0 || s.targetBytes != s.size || s.contentBytes != 0) ||
		kind != storage.NodeSymlink && s.targetBytes != 0 {
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
		BirthTime: birth, ChangeTime: changed, DirectoryRevision: bytes.Clone(s.directoryRevision)}, nil
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

type nodeScan struct {
	nodeHeader
	content  sql.NullString
	metadata []byte
	target   []byte
}

func (s *nodeScan) fields() []any {
	return append(s.nodeHeader.fields(), &s.content, &s.metadata, &s.target)
}

func (s *nodeScan) node() (metastore.Node, error) {
	node, err := s.nodeHeader.node()
	if err != nil {
		return metastore.Node{}, err
	}
	if int64(len(s.content.String)) != s.contentBytes || int64(len(s.metadata)) != s.metadataBytes || int64(len(s.target)) != s.targetBytes {
		return metastore.Node{}, fmt.Errorf("stored node payload changed after admission: %w", syscall.EIO)
	}
	metadata, err := storage.DecodeMetadata(s.metadata)
	if err != nil {
		return metastore.Node{}, err
	}
	node.Content, node.Metadata, node.LinkTarget = metastore.Key(s.content.String), metadata, bytes.Clone(s.target)
	return node, nil
}

func scanNode(row scanner) (metastore.Node, error) {
	var node nodeScan
	if err := row.Scan(node.fields()...); err != nil {
		return metastore.Node{}, err
	}
	return node.node()
}

func (s *Store) scanNode(row scanner) (metastore.Node, error) {
	node, err := scanNode(row)
	if err != nil {
		return metastore.Node{}, err
	}
	if err := s.validateLoadedNode(node); err != nil {
		return metastore.Node{}, err
	}
	return node, nil
}

func (s *Store) validateLoadedNode(node metastore.Node) error {
	if !s.replicaMetadata && node.Kind == storage.NodeDirectory && !validDirectoryRevision(node.DirectoryRevision) {
		return fmt.Errorf("directory %d has an invalid name-set revision: %w", node.ID, syscall.EIO)
	}
	return nil
}
