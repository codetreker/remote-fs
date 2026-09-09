package changes

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// Trim applies the configured retention window inside the caller's transaction.
// Writes and namespace preparation invoke it; an idle log has no background trimmer.
func Trim(ctx context.Context, tx *sql.Tx, namespace int64, window Window) error {
	oldest, oldestAt, err := oldestEntry(ctx, tx, namespace)
	if err != nil || oldest == 0 {
		return err
	}
	newest, err := newestEntry(ctx, tx, namespace)
	if err != nil {
		return err
	}

	// Two index lookups decide the common case, which is that there is nothing to do. The span
	// between the oldest and the newest position overstates the count, because the sequence is
	// shared with the other namespaces in this database — an overstatement only ever costs the
	// count below, never a wrong answer.
	span := int64(newest - oldest + 1)
	overCap := span > int64(window.Cap)
	tooOld := oldestAt.Before(time.Now().Add(-window.Age))
	if !overCap && !tooOld {
		return nil
	}

	var held int64
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM changes WHERE namespace = ?`, namespace).Scan(&held); err != nil {
		return err
	}

	// byVolume is the cut that leaves exactly Cap entries, byFloor the cut that leaves exactly
	// Floor. Age may cut no deeper than byFloor, which is how the floor wins over the age rule.
	byVolume, err := nthOldest(ctx, tx, namespace, held-int64(window.Cap))
	if err != nil {
		return err
	}
	byFloor, err := nthOldest(ctx, tx, namespace, held-int64(window.Floor))
	if err != nil {
		return err
	}
	byAge, err := lastBefore(ctx, tx, namespace, time.Now().Add(-window.Age))
	if err != nil {
		return err
	}
	byAge = min(byAge, byFloor)

	cut := max(byVolume, byAge)
	if cut == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM changes WHERE namespace = ? AND position <= ?`, namespace, cut); err != nil {
		return err
	}
	// The cut itself is recorded, because it is the only thing that can decide whether a
	// returning replica has missed anything: it has missed nothing exactly when it has already
	// seen everything at or below this. Working that out from the oldest surviving entry
	// instead would be working it out from a distance that the other namespaces in this
	// database set, which is a fact about them rather than about this replica.
	//
	// What makes it usable is that it is a position this namespace actually held and no longer
	// does, rather than a bound the DELETE was phrased with. Both candidates above are read out
	// of the table — nthOldest returns the nth entry's own position, and lastBefore returns the
	// newest position among the entries older than an instant rather than the instant — so the
	// larger of them is a row this statement is about to remove. That is what lets a replica
	// below it be told, truthfully, that something it needed is gone.
	//
	// Alongside it, which dimension pushed the oldest entries out, for whoever is told they
	// fell out of the window: age says that caller was away too long, volume says the
	// namespace changes faster than the log was configured to hold. When both would have cut
	// to the same place, volume did it — the entries were over the cap whether or not anybody
	// had been away.
	_, err = tx.ExecContext(ctx,
		`UPDATE logs SET trimmed_through = ?, trimmed_by_age = ? WHERE namespace = ?`,
		int64(cut), byAge > byVolume, namespace)
	return err
}

// oldestEntry reports the oldest position still held and the instant it was recorded, or zero
// for a log holding nothing.
func oldestEntry(ctx context.Context, tx *sql.Tx, namespace int64) (metastore.Position, time.Time, error) {
	var position, sec, nsec int64
	switch err := tx.QueryRowContext(ctx,
		`SELECT position, recorded_sec, recorded_nsec FROM changes
		 WHERE namespace = ? ORDER BY position LIMIT 1`, namespace).Scan(&position, &sec, &nsec); {
	case errors.Is(err, sql.ErrNoRows):
		return 0, time.Time{}, nil
	case err != nil:
		return 0, time.Time{}, err
	}
	return metastore.Position(position), sqlvalue.LoadedTime(sec, int32(nsec)), nil
}

// newestEntry reports the newest position still held, or zero for a log holding nothing. It
// is not the log's tail: the tail is the newest position ever recorded, which outlives the
// entry at it.
func newestEntry(ctx context.Context, tx *sql.Tx, namespace int64) (metastore.Position, error) {
	var newest int64
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(position), 0) FROM changes WHERE namespace = ?`, namespace).Scan(&newest)
	return metastore.Position(newest), err
}

// nthOldest returns the position of the nth oldest entry, which is the cut that discards
// exactly n of them. A count of zero or below discards nothing.
func nthOldest(ctx context.Context, tx *sql.Tx, namespace, n int64) (metastore.Position, error) {
	if n <= 0 {
		return 0, nil
	}
	var position int64
	err := tx.QueryRowContext(ctx,
		`SELECT position FROM changes WHERE namespace = ? ORDER BY position LIMIT 1 OFFSET ?`,
		namespace, n-1).Scan(&position)
	if err != nil {
		return 0, err
	}
	return metastore.Position(position), nil
}

// lastBefore returns the newest position recorded before an instant, which is the cut that
// discards everything older than it.
//
// It is the newest match rather than a scan for a boundary, which is the same answer as long
// as a namespace's entries are recorded in the order their positions were allocated. A wall
// clock that stepped backwards would break that ordering and make this cut slightly deeper
// than the age asked for; the floor is what bounds how much that can cost.
func lastBefore(ctx context.Context, tx *sql.Tx, namespace int64, cutoff time.Time) (metastore.Position, error) {
	sec, nsec := sqlvalue.StoredTime(cutoff)
	var position int64
	err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(position), 0) FROM changes
		WHERE namespace = ?
		  AND (recorded_sec < ? OR (recorded_sec = ? AND recorded_nsec < ?))`,
		namespace, sec, sec, nsec).Scan(&position)
	return metastore.Position(position), err
}
