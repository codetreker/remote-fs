package sqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

func newReferenceScope() (storage.UseScope, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return storage.UseScope{}, err
	}
	return storage.UseScope{Token: hex.EncodeToString(token[:])}, nil
}

func (f *retainedFile) CheckScopedReference() error { return f.store.CheckFileStore() }

func (f *retainedFile) Scope(ctx context.Context) (storage.UseScope, error) {
	var scope storage.UseScope
	err := f.Order(ctx, func() error { scope = f.scope; return nil })
	return scope, err
}

// Scope lookup indexes the existing retained set. A token never substitutes
// for its session binding or permits a closed reference to become anonymous.
func (s *Store) resolveUseScope(ctx context.Context, scope storage.UseScope, id uint64, uses storage.Uses) (*retainedFile, error) {
	if err := scope.Check(); err != nil {
		return nil, err
	}
	for file := range s.files {
		if file.scope != scope {
			continue
		}
		if !file.active || file.closed || uint64(file.id) != id || file.session != metastore.ReferenceSession(ctx) {
			return nil, storage.ErrInvalidScope
		}
		if uses&^file.use.Uses != 0 {
			return nil, syscall.EBADF
		}
		return file, nil
	}
	return nil, storage.ErrInvalidScope
}

func (s *Store) targetScope(ctx context.Context, id uint64, uses storage.Uses, provided []storage.TargetUse) (storage.UseScope, error) {
	for _, target := range provided {
		if target.NodeID != id {
			continue
		}
		file, err := s.resolveUseScope(ctx, target.Scope, id, uses)
		if err != nil {
			return storage.UseScope{}, err
		}
		return file.scope, nil
	}
	return storage.UseScope{}, nil
}
