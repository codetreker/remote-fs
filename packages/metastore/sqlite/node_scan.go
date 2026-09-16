package sqlite

import (
	"bytes"
	"database/sql"
	"errors"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
	"github.com/codetreker/remote-fs/packages/storage"
)

// Invalid payload lengths become NULL before the driver materializes the value.
const nodeAttrColumns = `n.id,n.kind,n.size,n.atime_sec,n.atime_nsec,n.mtime_sec,n.mtime_nsec,n.creation_sec,n.creation_nsec,n.change_sec,n.change_nsec,n.metadata_revision,n.directory_revision,CASE WHEN typeof(n.metadata)='blob' AND length(n.metadata)<=32768 THEN n.metadata END`
const nodeColumns = nodeAttrColumns + `,n.content,CASE WHEN typeof(n.link_target)='blob' AND length(n.link_target)<=4096 THEN n.link_target END`

type scanner interface{ Scan(...any) error }

type nodeAttrScan struct {
	id, kind, size            int64
	atimeSec, mtimeSec        int64
	atimeNsec, mtimeNsec      int32
	creationSec, creationNsec sql.NullInt64
	changeSec, changeNsec     sql.NullInt64
	metadataRevision          int64
	directoryRevision         int64
	metadata                  []byte
}

func (s *nodeAttrScan) fields() []any {
	return []any{&s.id, &s.kind, &s.size, &s.atimeSec, &s.atimeNsec, &s.mtimeSec, &s.mtimeNsec,
		&s.creationSec, &s.creationNsec, &s.changeSec, &s.changeNsec,
		&s.metadataRevision, &s.directoryRevision, &s.metadata}
}

func loadedOptionalTime(sec, nsec sql.NullInt64) (*time.Time, error) {
	if sec.Valid != nsec.Valid || nsec.Valid && (nsec.Int64 < 0 || nsec.Int64 >= 1e9) {
		return nil, syscall.EIO
	}
	if !sec.Valid {
		return nil, nil
	}
	value := time.Unix(sec.Int64, nsec.Int64).UTC()
	return &value, nil
}

func (s *nodeAttrScan) attr() (storage.Attr, error) {
	if s.id <= 0 || s.kind < int64(storage.NodeRegular) || s.kind > int64(storage.NodeSymlink) || s.size < 0 || s.metadataRevision < 1 ||
		s.atimeNsec < 0 || s.atimeNsec >= 1e9 || s.mtimeNsec < 0 || s.mtimeNsec >= 1e9 ||
		s.kind == int64(storage.NodeDirectory) && (s.directoryRevision < 1 || s.size != 0) ||
		s.kind != int64(storage.NodeDirectory) && s.directoryRevision != 0 {
		return storage.Attr{}, syscall.EIO
	}
	metadata, err := storage.DecodeMetadata(s.metadata)
	if err != nil {
		return storage.Attr{}, errors.Join(syscall.EIO, err)
	}
	created, err := loadedOptionalTime(s.creationSec, s.creationNsec)
	if err != nil {
		return storage.Attr{}, err
	}
	changed, err := loadedOptionalTime(s.changeSec, s.changeNsec)
	if err != nil {
		return storage.Attr{}, err
	}
	return storage.Attr{ID: uint64(s.id), Kind: storage.NodeKind(s.kind), Size: s.size,
		AccessTime: sqlvalue.LoadedTime(s.atimeSec, s.atimeNsec), ModTime: sqlvalue.LoadedTime(s.mtimeSec, s.mtimeNsec),
		CreationTime: created, ChangeTime: changed, MetadataRevision: storage.NodeMetadataRevision(s.metadataRevision),
		DirectoryRevision: storage.DirectoryRevision(s.directoryRevision), Metadata: metadata}, nil
}

type nodeScan struct {
	nodeAttrScan
	content sql.NullString
	target  nodeTarget
}

func (s *nodeScan) fields() []any {
	return append(s.nodeAttrScan.fields(), &s.content, &s.target)
}

type nodeTarget struct{ data []byte }

func (t *nodeTarget) Scan(value any) error {
	data, ok := value.([]byte)
	if !ok || len(data) > storage.MaxLinkTargetBytes {
		return syscall.EIO
	}
	t.data = bytes.Clone(data)
	return nil
}

func nodeFromAttr(attr storage.Attr, content metastore.Key, target []byte) metastore.Node {
	return metastore.Node{ID: int64(attr.ID), Kind: attr.Kind, Size: attr.Size,
		AccessTime: attr.AccessTime, ModTime: attr.ModTime,
		CreationTime: attr.CreationTime, ChangeTime: attr.ChangeTime,
		MetadataRevision: attr.MetadataRevision, DirectoryRevision: attr.DirectoryRevision,
		Metadata: attr.Metadata, Content: content, LinkTarget: target}
}

func (s *nodeScan) node() (metastore.Node, error) {
	attr, err := s.attr()
	if err != nil {
		return metastore.Node{}, err
	}
	if attr.Kind == storage.NodeSymlink && (int64(len(s.target.data)) != attr.Size || s.content.Valid) ||
		attr.Kind != storage.NodeSymlink && len(s.target.data) != 0 || attr.Kind == storage.NodeDirectory && s.content.Valid {
		return metastore.Node{}, syscall.EIO
	}
	return nodeFromAttr(attr, metastore.Key(s.content.String), s.target.data), nil
}

func scanNode(row scanner) (metastore.Node, error) {
	var node nodeScan
	if err := row.Scan(node.fields()...); err != nil {
		return metastore.Node{}, err
	}
	return node.node()
}
