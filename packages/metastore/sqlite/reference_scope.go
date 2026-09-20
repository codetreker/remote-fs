package sqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"

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
	err := f.Order(ctx, func() error {
		scope = f.scope
		return nil
	})
	return scope, err
}
