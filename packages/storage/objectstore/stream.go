package objectstore

import (
	"context"
	"io"
)

// ObjectSeal describes a fully verified immutable payload. The digest is SHA-256.
type ObjectSeal struct {
	Size   int64
	Digest [32]byte
}

// StreamingObjects materializes immutable objects with bounded buffer memory. GetStream
// writes only to a caller-owned private destination: the caller must discard that
// destination on any error and publish it only after verification has completed.
type StreamingObjects interface {
	Objects
	CheckStreamingObjects() error
	PutStream(context.Context, string, int64, [32]byte, io.Reader) error
	GetStream(context.Context, string, io.Writer) (ObjectSeal, error)
}
