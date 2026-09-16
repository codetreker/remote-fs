package schema

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

func validateFileLeaseInitialization(ctx context.Context, db sqlvalue.Queryer) error {
	var rows, valid int64
	var initialized sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT count(*),
		coalesce(sum(CASE WHEN typeof(singleton) = 'integer' AND singleton = 1 AND
			typeof(state) = 'integer' AND state IN (0,1) THEN 1 ELSE 0 END),0),
		max(CASE WHEN typeof(state) = 'integer' THEN state END)
		FROM (SELECT singleton, state FROM file_lease_initialization LIMIT 2)`).Scan(&rows, &valid, &initialized); err != nil {
		return err
	}
	if rows != 1 || valid != 1 || !initialized.Valid {
		return fmt.Errorf("file lease initialization is not a valid singleton: %w", syscall.EIO)
	}
	state, err := dbstate.Read(ctx, db)
	if err != nil {
		return err
	}
	// Both tables are bounded singletons. ID content is inspected only after its
	// byte length is known, so a corrupt scalar cannot trigger an unbounded read.
	var quiescent int64
	if err := db.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(CASE WHEN
		typeof(singleton) = 'integer' AND singleton = 1 AND
		CASE WHEN typeof(database_id) = 'text' AND length(CAST(database_id AS BLOB)) = 32
			THEN database_id = ? ELSE 0 END AND
		CASE WHEN typeof(state_id) = 'text' AND length(CAST(state_id AS BLOB)) = 32
			THEN state_id NOT GLOB '*[^0-9a-f]*' ELSE 0 END AND
		typeof(accepted_generation) = 'integer' AND accepted_generation >= 0 AND
		typeof(accepted_nanos) = 'integer' AND accepted_nanos >= 0 AND
		typeof(accepted_quiescent) = 'integer' AND accepted_quiescent IN (0,1) AND
		(accepted_generation != 0 OR (accepted_nanos = 0 AND accepted_quiescent = 1)) AND
		(accepted_quiescent = 1 OR accepted_nanos > 0) AND
		((prepared_generation IS NULL AND prepared_nanos IS NULL AND prepared_quiescent IS NULL) OR
			(typeof(prepared_generation) = 'integer' AND typeof(prepared_nanos) = 'integer' AND
			typeof(prepared_quiescent) = 'integer' AND prepared_quiescent IN (0,1) AND
			accepted_generation < ? AND prepared_generation = accepted_generation + 1 AND
			prepared_nanos >= accepted_nanos AND
			(prepared_quiescent = 1 OR prepared_nanos > 0) AND
			(prepared_quiescent = 0 OR prepared_nanos = accepted_nanos))) AND
		(? = 1 OR (accepted_generation = 0 AND accepted_nanos = 0 AND
			accepted_quiescent = 1 AND prepared_generation IS NULL AND
			prepared_nanos IS NULL AND prepared_quiescent IS NULL))
		THEN 1 ELSE 0 END),0),
		coalesce(sum(CASE WHEN accepted_quiescent = 1 OR prepared_quiescent = 1 THEN 1 ELSE 0 END),0)
		FROM (SELECT * FROM file_lease_recovery LIMIT 2)`,
		state.DatabaseID, int64(math.MaxInt64), initialized.Int64).Scan(&rows, &valid, &quiescent); err != nil {
		return err
	}
	if rows > 1 || rows != valid || initialized.Int64 == 1 && rows != 1 {
		return fmt.Errorf("file lease recovery does not match its initialization state or database: %w", syscall.EIO)
	}
	if quiescent != 0 {
		var obligations bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM removal_intents) OR
			EXISTS(SELECT 1 FROM entries WHERE draining=1)`).Scan(&obligations); err != nil {
			return err
		}
		if obligations {
			return fmt.Errorf("quiescent file lease evidence retains durable removal obligations: %w", syscall.EIO)
		}
	}
	return nil
}
