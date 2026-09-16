package sqlite

import (
	"context"
	"database/sql"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/storage"
)

// captureEventImage shares the mutation transaction and owns the historical
// metadata, target and complete identity-bearing ancestry.
func (s *Store) captureEventImage(ctx context.Context, tx *sql.Tx, node metastore.Node) (*metastore.EventImage, error) {
	location, err := s.captureLocation(ctx, tx, s.root, node.ID)
	if err != nil {
		return nil, err
	}
	return &metastore.EventImage{Attr: node.Attr(), Location: location, LinkTarget: append([]byte(nil), node.LinkTarget...)}, nil
}

func eventLocation(image *metastore.EventImage) metastore.Location {
	if image.Location.State != storage.LocationLinked {
		return metastore.Location{}
	}
	leaf := image.Location.Ancestors[len(image.Location.Ancestors)-1]
	return metastore.Location{Parent: int64(leaf.ParentID), Name: append([]byte(nil), leaf.Name...)}
}
