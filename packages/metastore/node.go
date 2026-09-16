package metastore

import (
	"bytes"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

// Node records one stable object and its current immutable content reference.
// Platform metadata remains opaque to the metadata authority.
type Node struct {
	ID                int64
	Kind              storage.NodeKind
	Size              int64
	AccessTime        time.Time
	ModTime           time.Time
	CreationTime      *time.Time
	ChangeTime        *time.Time
	MetadataRevision  storage.NodeMetadataRevision
	DirectoryRevision storage.DirectoryRevision
	Metadata          storage.Metadata
	LinkTarget        []byte
	Content           Key
}

func (n Node) IsDir() bool { return n.Kind == storage.NodeDirectory }

func (n Node) Attr() storage.Attr {
	return storage.Attr{
		ID: uint64(n.ID), Kind: n.Kind, Size: n.Size,
		AccessTime: n.AccessTime, ModTime: n.ModTime,
		CreationTime: cloneNodeTime(n.CreationTime), ChangeTime: cloneNodeTime(n.ChangeTime),
		MetadataRevision: n.MetadataRevision, DirectoryRevision: n.DirectoryRevision,
		Metadata: n.Metadata.Clone(),
	}
}

func (n Node) Clone() Node {
	n.CreationTime = cloneNodeTime(n.CreationTime)
	n.ChangeTime = cloneNodeTime(n.ChangeTime)
	n.Metadata = n.Metadata.Clone()
	n.LinkTarget = bytes.Clone(n.LinkTarget)
	return n
}

func cloneNodeTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
