package integration_test

import (
	"context"

	"github.com/codetreker/remote-fs/packages/metastore"
)

func readChanges(ctx context.Context, log metastore.Log, after metastore.Position, limit int) ([]metastore.Change, metastore.Retention, error) {
	result, err := metastore.NewChangeResult(64<<20, 0, func(_ int, _ metastore.Change, lengths metastore.ChangePayloadLengths) (int64, error) {
		return 256 + lengths.Name + lengths.FromName + lengths.Content, nil
	})
	if err != nil {
		return nil, metastore.Retention{}, err
	}
	retention, err := log.Since(ctx, after, limit, result)
	if err != nil {
		return nil, metastore.Retention{}, err
	}
	changes, err := result.Changes()
	return changes, retention, err
}

func readRows(ctx context.Context, snap metastore.Snap, limit int) ([]metastore.Row, bool, error) {
	result, err := metastore.NewRowResult(64<<20, 0, func(_ int, _ metastore.Row, lengths metastore.RowPayloadLengths) (int64, error) {
		return 192 + lengths.Name + lengths.Content, nil
	})
	if err != nil {
		return nil, false, err
	}
	done, err := snap.Next(ctx, limit, result)
	if err != nil {
		return nil, false, err
	}
	rows, err := result.Rows()
	return rows, done, err
}
