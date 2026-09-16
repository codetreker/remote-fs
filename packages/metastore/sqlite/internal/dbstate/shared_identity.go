package dbstate

import (
	"context"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

const entryIdentityBoundsQuery = `SELECT
	coalesce((SELECT 1 FROM entries INDEXED BY entries_by_identity_bounds
		WHERE CASE WHEN typeof(id) = 'integer' THEN 0 ELSE 1 END = 1 LIMIT 1), 0),
	coalesce((SELECT id FROM entries INDEXED BY entries_by_identity_bounds
		WHERE CASE WHEN typeof(id) = 'integer' THEN 0 ELSE 1 END = 0
		ORDER BY id DESC LIMIT 1), 0)`

const removalIdentityBoundsQuery = `SELECT
	coalesce((SELECT 1 FROM removal_intents INDEXED BY removal_intents_by_identity
		WHERE CASE WHEN typeof(entry) = 'integer' THEN 0 ELSE 1 END = 1 LIMIT 1), 0),
	coalesce((SELECT entry FROM removal_intents INDEXED BY removal_intents_by_identity
		WHERE CASE WHEN typeof(entry) = 'integer' THEN 0 ELSE 1 END = 0
		ORDER BY entry DESC LIMIT 1), 0)`

const factIdentityBoundsQuery = `SELECT
	coalesce((SELECT 1 FROM changes INDEXED BY changes_by_fact_identity
		WHERE CASE WHEN typeof(identity_high_water) = 'integer' THEN 0 ELSE 1 END = 1 LIMIT 1), 0),
	coalesce((SELECT identity_high_water FROM changes INDEXED BY changes_by_fact_identity
		WHERE CASE WHEN typeof(identity_high_water) = 'integer' THEN 0 ELSE 1 END = 0
		ORDER BY identity_high_water DESC LIMIT 1), 0)`

func validateSharedIdentityBounds(ctx context.Context, db sqlvalue.Queryer, highWater int64) error {
	for _, source := range []struct{ name, query string }{
		{"entries", entryIdentityBoundsQuery},
		{"removal intentions", removalIdentityBoundsQuery},
		{"immutable change facts", factIdentityBoundsQuery},
	} {
		var invalid, maximum int64
		if err := db.QueryRowContext(ctx, source.query).Scan(&invalid, &maximum); err != nil {
			return err
		}
		if invalid != 0 || maximum < 0 || maximum > highWater {
			return fmt.Errorf("%s contain invalid identities or maximum %d above witnessed high-water %d: %w",
				source.name, maximum, highWater, syscall.EIO)
		}
	}
	return nil
}
