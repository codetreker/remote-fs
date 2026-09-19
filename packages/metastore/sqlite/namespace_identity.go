package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

type namespaceIdentity struct {
	ID                int64
	Kind              storage.NodeKind
	DirectoryRevision []byte
	Detached          bool
}

var namespaceIdentityColumns = nodeHeaderColumns + `,
	CASE WHEN typeof(n.content_revision)='integer' THEN n.content_revision ELSE 0 END,
	CASE WHEN typeof(n.detached)='integer' THEN n.detached ELSE -1 END`

func scanNamespaceIdentity(row scanner) (namespaceIdentity, error) {
	var header nodeHeader
	var revision, detached int64
	if err := row.Scan(append(header.fields(), &revision, &detached)...); err != nil {
		return namespaceIdentity{}, err
	}
	if _, err := header.node(); err != nil {
		return namespaceIdentity{}, err
	}
	if revision < 1 || detached < 0 || detached > 1 {
		return namespaceIdentity{}, syscall.EIO
	}
	return namespaceIdentity{ID: header.id, Kind: storage.NodeKind(header.kind), DirectoryRevision: header.directoryRevision, Detached: detached == 1}, nil
}

func (s *Store) namespaceIdentity(ctx context.Context, tx *sql.Tx, id int64) (namespaceIdentity, error) {
	identity, err := scanNamespaceIdentity(tx.QueryRowContext(ctx,
		`SELECT `+namespaceIdentityColumns+` FROM nodes n WHERE n.volume=? AND n.id=?`, s.volume, id))
	if errors.Is(err, sql.ErrNoRows) {
		return namespaceIdentity{}, syscall.ESTALE
	}
	return identity, err
}

func (s *Store) lookupNamespaceIdentity(ctx context.Context, tx *sql.Tx, parent int64, name []byte) (namespaceIdentity, bool, error) {
	identity, err := scanNamespaceIdentity(tx.QueryRowContext(ctx,
		`SELECT `+namespaceIdentityColumns+` FROM entries e JOIN nodes n ON n.id=e.node
		WHERE e.volume=? AND e.parent=? AND e.name=?`, s.volume, parent, name))
	if errors.Is(err, sql.ErrNoRows) {
		return namespaceIdentity{}, false, nil
	}
	return identity, err == nil, err
}

func (s *Store) directoryMetadataTarget(ctx context.Context, tx *sql.Tx, target storage.DirectoryTarget) (namespaceIdentity, error) {
	if target.NodeID > math.MaxInt64 {
		return namespaceIdentity{}, syscall.ESTALE
	}
	var scope storage.UseScope
	if target.Scope != nil {
		file, err := s.resolveUseScope(ctx, *target.Scope, target.NodeID, 0)
		if err != nil {
			return namespaceIdentity{}, err
		}
		scope = file.scope
	}
	identity, err := s.namespaceIdentity(ctx, tx, int64(target.NodeID))
	if err != nil {
		return namespaceIdentity{}, err
	}
	if identity.Kind != storage.NodeDirectory {
		return namespaceIdentity{}, syscall.ENOTDIR
	}
	if identity.Detached {
		return namespaceIdentity{}, syscall.ESTALE
	}
	if err := s.fileDomain.coordinator.CheckUse(ctx, target.NodeID, scope, 0); err != nil {
		return namespaceIdentity{}, err
	}
	return identity, nil
}
