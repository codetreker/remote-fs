package localdisk

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage/objectstore"
)

var _ objectstore.StreamingObjects = (*Objects)(nil)

func (o *Objects) CheckStreamingObjects() error { return o.health.failure() }

// PutStream seals exactly size bytes before the shared durable publication path links
// the immutable object into its final name. Neither a short stream nor a longer one can
// publish a prefix.
func (o *Objects) PutStream(ctx context.Context, key string, size int64, digest [32]byte, source io.Reader) error {
	if size < 0 || source == nil {
		return fmt.Errorf("put %q: invalid stream: %w", key, syscall.EINVAL)
	}
	chunkBytes := o.streamChunkBytes()
	return o.putImmutable(ctx, key, size, func() ([32]byte, error) { return digest, nil }, chunkBytes, func(fd int) error { return writeStream(ctx, fd, source, size, digest, key, chunkBytes) })
}

func writeStream(ctx context.Context, fd int, source io.Reader, size int64, expected [32]byte, key string, chunkBytes int64) error {
	hash := sha256.New()
	buffer := make([]byte, int(chunkBytes))
	remaining := size
	for remaining > 0 {
		if err := checkContext(ctx, "put", key); err != nil {
			return err
		}
		chunk := buffer
		if int64(len(chunk)) > remaining {
			chunk = chunk[:int(remaining)]
		}
		n, err := io.ReadFull(source, chunk)
		if n > 0 {
			if writeErr := writeFull(fd, chunk[:n]); writeErr != nil {
				return writeErr
			}
			_, _ = hash.Write(chunk[:n])
			remaining -= int64(n)
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("stream is shorter than its declared %d bytes: %w", size, syscall.EINVAL)
			}
			return err
		}
	}
	if err := checkContext(ctx, "put", key); err != nil {
		return err
	}
	var extra [1]byte
	n, err := io.ReadFull(source, extra[:])
	if n != 0 {
		return fmt.Errorf("stream exceeds its declared %d bytes: %w", size, syscall.EINVAL)
	}
	if err != io.EOF {
		return err
	}
	var actual [32]byte
	copy(actual[:], hash.Sum(nil))
	if actual != expected {
		return fmt.Errorf("stream checksum does not match declared SHA-256: %w", syscall.EINVAL)
	}
	return nil
}

// GetStream verifies one immutable object's envelope and payload while copying to a
// private destination. It retains operation admission until all verification finishes.
func (o *Objects) GetStream(ctx context.Context, key string, destination io.Writer) (seal objectstore.ObjectSeal, returned error) {
	if destination == nil {
		return seal, syscall.EINVAL
	}
	if err := checkContext(ctx, "get", key); err != nil {
		return seal, err
	}
	location, err := locate(key)
	if err != nil {
		return seal, err
	}
	waiting, err := o.gate.acquireWaiting(ctx)
	if err != nil {
		return seal, err
	}
	defer waiting.release()
	ticket, err := waiting.promote(ctx, o.streamChunkBytes())
	if err != nil {
		return seal, err
	}
	defer ticket.release()
	if err := o.health.failure(); err != nil {
		return seal, err
	}
	defer func() {
		returned = o.health.finish(ctx, returned)
		if returned != nil {
			seal = objectstore.ObjectSeal{}
		}
	}()
	shardFD, absent, err := o.openShard(ctx, location, false)
	if err != nil {
		return seal, err
	}
	if absent {
		return seal, syscall.ENOENT
	}
	defer unix.Close(shardFD)
	fd, st, err := openPrivateObject(shardFD, location.final, o.filesystem, o.ops)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return seal, err
		}
		return seal, fmt.Errorf("get %q: open object: %v: %w", key, err, syscall.EIO)
	}
	defer func() { returned = errors.Join(returned, unix.Close(fd)) }()
	header, err := readHeader(fd, st, o.id, key, o.limits.maxObjectBytes)
	if err != nil {
		return seal, err
	}
	hash := sha256.New()
	buffer := make([]byte, int(o.streamChunkBytes()))
	remaining := header.payloadLength
	offset := int64(fixedEnvelopeBytes + header.keyLength)
	for remaining > 0 {
		if err := checkContext(ctx, "get", key); err != nil {
			return seal, err
		}
		chunk := buffer
		if int64(len(chunk)) > remaining {
			chunk = chunk[:int(remaining)]
		}
		if err := preadFull(fd, chunk, offset); err != nil {
			return seal, fmt.Errorf("read object payload: %w", err)
		}
		_, _ = hash.Write(chunk)
		n, err := destination.Write(chunk)
		if err != nil {
			return seal, err
		}
		if n != len(chunk) {
			return seal, io.ErrShortWrite
		}
		remaining -= int64(n)
		offset += int64(n)
	}
	var actual [32]byte
	copy(actual[:], hash.Sum(nil))
	if actual != header.digest {
		return seal, fmt.Errorf("payload checksum does not match its envelope: %w", syscall.EIO)
	}
	if err := checkContext(ctx, "get", key); err != nil {
		return seal, err
	}
	return objectstore.ObjectSeal{Size: header.payloadLength, Digest: actual}, nil
}

func (o *Objects) streamChunkBytes() int64 {
	if o.limits.maxObjectBytes < writeChunkBytes {
		return o.limits.maxObjectBytes
	}
	return writeChunkBytes
}
