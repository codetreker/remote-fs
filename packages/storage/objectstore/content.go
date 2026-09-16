package objectstore

import (
	"context"
	"errors"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func (s *Storage) fileBody(ctx context.Context, node metastore.FileState, missing metastore.Key) ([]byte, error) {
	if node.Content == "" {
		if node.Size != 0 {
			return nil, fmt.Errorf("retained file has bytes without an object: %w", syscall.EIO)
		}
		return nil, nil
	}
	if node.Content == missing {
		return nil, fmt.Errorf("retained file names a missing object: %w", syscall.EIO)
	}
	body, err := s.objects.(BoundedObjects).GetBounded(ctx, string(node.Content), max(node.Size, 1))
	if err != nil {
		if isOnly(err, syscall.ENOENT) {
			return nil, err
		}
		if errors.Is(err, syscall.EFBIG) {
			return nil, sanitizeFailure("retained file object exceeds its recorded size", err, func(e error) bool { return errors.Is(e, syscall.EFBIG) })
		}
		return nil, objectFailure("reading", fmt.Sprintf("node %d", node.ID), err)
	}
	if int64(len(body)) != node.Size {
		return nil, fmt.Errorf("retained file object size differs from metadata: %w", syscall.EIO)
	}
	return body, nil
}
