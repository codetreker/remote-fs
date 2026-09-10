package changes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlvalue"
)

// ReadPage reads retention and bounded changes from the caller's snapshot.
// The caller validates arguments and fails result if this read or its snapshot fails.
func ReadPage(
	ctx context.Context,
	tx *sql.Tx,
	volume, maxIntegrityRecords int64,
	after metastore.Position,
	limit int,
	result *metastore.ChangeResult,
) (retention metastore.Retention, returnErr error) {
	var expected, changeHighWater, fixedWork int64
	var err error
	if retention, expected, changeHighWater, fixedWork, err = logPageState(ctx, tx, volume, after); err != nil {
		return metastore.Retention{}, err
	}
	if fixedWork > maxIntegrityRecords {
		return metastore.Retention{}, fmt.Errorf("the log page anchors require %d records under a %d-record integrity limit: %w",
			fixedWork, maxIntegrityRecords, syscall.EFBIG)
	}
	pageLimit := min(int64(limit), maxIntegrityRecords-fixedWork)
	cursor := after
	if pageLimit == 0 && limit > 0 && cursor < retention.Tail {
		return metastore.Retention{}, fmt.Errorf("the log page has no record budget remaining before tail %d: %w",
			retention.Tail, syscall.EFBIG)
	}
	var examined int64
	stoppedAtCapacity := false
	if pageLimit > 0 {
		rows, err := tx.QueryContext(ctx, `
				SELECT `+changeMetadataColumns+` FROM changes
				WHERE volume = ? AND position > ?
				ORDER BY position
				LIMIT ?`, volume, int64(cursor), pageLimit)
		if err != nil {
			return metastore.Retention{}, err
		}
		for rows.Next() {
			examined++
			change, lengths, previous, err := scanChangeMetadata(rows, volume)
			if err != nil {
				rows.Close()
				return metastore.Retention{}, err
			}
			if previous != expected {
				rows.Close()
				return metastore.Retention{}, fmt.Errorf("change %d follows position %d rather than %d: %w",
					change.Position, previous, expected, syscall.EIO)
			}
			if int64(change.Position) > changeHighWater || previous > changeHighWater {
				rows.Close()
				return metastore.Retention{}, fmt.Errorf("change %d or its predecessor %d exceeds durable high-water %d: %w",
					change.Position, previous, changeHighWater, syscall.EIO)
			}
			if err := ctx.Err(); err != nil {
				rows.Close()
				return metastore.Retention{}, err
			}
			reservation, fits, err := result.Reserve(change, lengths)
			if err != nil {
				rows.Close()
				return metastore.Retention{}, err
			}
			if !fits {
				stoppedAtCapacity = true
				break
			}
			var name, fromName []byte
			var content sql.NullString
			if err := tx.QueryRowContext(ctx, `
					SELECT name, from_name, content FROM changes
					WHERE volume = ? AND position = ?`, volume, int64(change.Position)).Scan(
				&name, &fromName, &content,
			); err != nil {
				rows.Close()
				return metastore.Retention{}, err
			}
			if err := validateChangePayload(change, name, fromName, content); err != nil {
				rows.Close()
				return metastore.Retention{}, err
			}
			if err := reservation.Commit(name, fromName, metastore.Key(content.String)); err != nil {
				rows.Close()
				return metastore.Retention{}, err
			}
			cursor = change.Position
			expected = int64(change.Position)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return metastore.Retention{}, err
		}
	}
	if !stoppedAtCapacity && examined < pageLimit && limit > 0 &&
		cursor >= retention.TrimmedThrough && cursor < retention.Tail {
		return metastore.Retention{}, fmt.Errorf("the log ends after position %d before its committed tail %d: %w",
			cursor, retention.Tail, syscall.EIO)
	}
	return retention, ctx.Err()
}

// logPageState validates the indexed anchors needed to continue one page without scanning
// history the page will not return. The predecessor carried by each returned row proves the
// interior of the page; a later page continues from the last position this one exposed.
func logPageState(
	ctx context.Context,
	tx *sql.Tx,
	volume int64,
	after metastore.Position,
) (metastore.Retention, int64, int64, int64, error) {
	state, err := dbstate.Validate(ctx, tx)
	if err != nil {
		return metastore.Retention{}, 0, 0, 0, err
	}
	var (
		tailRaw, trimmedRaw, ageRaw    any
		incarnationRaw                 any
		tailType, trimmedType, ageType string
		incarnationType                string
		incarnationBytes               int64
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT CASE WHEN typeof(committed_position) = 'integer' THEN committed_position END,
		       typeof(committed_position),
		       CASE WHEN typeof(trimmed_through) = 'integer' THEN trimmed_through END,
		       typeof(trimmed_through),
		       CASE WHEN typeof(trimmed_by_age) = 'integer' THEN trimmed_by_age END,
		       typeof(trimmed_by_age),
		       typeof(incarnation), length(CAST(incarnation AS BLOB)),
		       CASE
		           WHEN typeof(incarnation) = 'text' AND length(CAST(incarnation AS BLOB)) = 32
		           THEN incarnation
		       END
		FROM logs WHERE volume = ?`, volume).Scan(
		&tailRaw, &tailType, &trimmedRaw, &trimmedType, &ageRaw, &ageType,
		&incarnationType, &incarnationBytes, &incarnationRaw,
	); err != nil {
		return metastore.Retention{}, 0, 0, 0, err
	}
	tail, tailOK := sqlvalue.StoredInteger(tailRaw, tailType)
	trimmed, trimmedOK := sqlvalue.StoredInteger(trimmedRaw, trimmedType)
	age, ageOK := sqlvalue.StoredInteger(ageRaw, ageType)
	if !tailOK || !trimmedOK || !ageOK || tail < 0 || trimmed < 0 || trimmed > tail ||
		(age != 0 && age != 1) || incarnationType != "text" || incarnationBytes != 32 {
		return metastore.Retention{}, 0, 0, 0, fmt.Errorf("the volume log header is invalid: %w", syscall.EIO)
	}
	incarnation, ok := incarnationRaw.(string)
	if !ok || len(incarnation) != 32 || strings.Trim(incarnation, "0123456789abcdef") != "" {
		return metastore.Retention{}, 0, 0, 0, fmt.Errorf("the volume log incarnation is invalid: %w", syscall.EIO)
	}
	if tail > state.ChangeHighWater || trimmed > state.ChangeHighWater {
		return metastore.Retention{}, 0, 0, 0, fmt.Errorf(
			"the volume log header exceeds durable change high-water %d: %w",
			state.ChangeHighWater, syscall.EIO)
	}

	oldest, predecessor, found, err := storedPositionPair(tx.QueryRowContext(ctx, `
		SELECT CASE WHEN typeof(position) = 'integer' THEN position END, typeof(position),
		       CASE WHEN typeof(previous_position) = 'integer' THEN previous_position END,
		       typeof(previous_position)
		FROM changes WHERE volume = ? ORDER BY position LIMIT 1`, volume))
	if err != nil {
		return metastore.Retention{}, 0, 0, 0, err
	}
	retention := metastore.Retention{
		Tail:           metastore.Position(tail),
		TrimmedThrough: metastore.Position(trimmed),
		TrimmedByAge:   age == 1,
	}
	if !found {
		if tail != trimmed {
			return metastore.Retention{}, 0, 0, 0, fmt.Errorf(
				"the empty retained log ends at %d but is trimmed only through %d: %w",
				tail, trimmed, syscall.EIO)
		}
		return retention, trimmed, state.ChangeHighWater, 1, nil
	}
	if oldest <= trimmed || predecessor != trimmed {
		return metastore.Retention{}, 0, 0, 0, fmt.Errorf(
			"the oldest retained change %d follows %d with trim anchor %d: %w",
			oldest, predecessor, trimmed, syscall.EIO)
	}
	newest, err := storedPosition(tx.QueryRowContext(ctx, `
		SELECT CASE WHEN typeof(position) = 'integer' THEN position END, typeof(position)
		FROM changes WHERE volume = ? ORDER BY position DESC LIMIT 1`, volume))
	if err != nil {
		return metastore.Retention{}, 0, 0, 0, err
	}
	if newest != tail {
		return metastore.Retention{}, 0, 0, 0, fmt.Errorf(
			"the newest retained change is %d but the committed tail is %d: %w",
			newest, tail, syscall.EIO)
	}
	retention.Oldest = metastore.Position(oldest)

	expected := trimmed
	position, err := storedPosition(tx.QueryRowContext(ctx, `
		SELECT CASE WHEN typeof(position) = 'integer' THEN position END, typeof(position)
		FROM changes
		WHERE volume = ? AND position <= ?
		ORDER BY position DESC LIMIT 1`, volume, int64(after)))
	switch {
	case err == nil:
		expected = position
	case !errors.Is(err, sql.ErrNoRows):
		return metastore.Retention{}, 0, 0, 0, err
	}
	if oldest > state.ChangeHighWater || predecessor > state.ChangeHighWater ||
		newest > state.ChangeHighWater || expected > state.ChangeHighWater {
		return metastore.Retention{}, 0, 0, 0, fmt.Errorf(
			"the volume log anchors exceed durable change high-water %d: %w",
			state.ChangeHighWater, syscall.EIO)
	}
	fixedWork := int64(3) // log header, oldest row, and newest row.
	if err == nil {
		fixedWork++
	}
	return retention, expected, state.ChangeHighWater, fixedWork, nil
}

func storedPosition(row rowScanner) (int64, error) {
	var raw any
	var storageClass string
	if err := row.Scan(&raw, &storageClass); err != nil {
		return 0, err
	}
	position, ok := sqlvalue.StoredInteger(raw, storageClass)
	if !ok || position <= 0 {
		return 0, fmt.Errorf("the log stores an invalid position as %s: %w", storageClass, syscall.EIO)
	}
	return position, nil
}

func storedPositionPair(row rowScanner) (position, predecessor int64, found bool, err error) {
	var positionRaw, predecessorRaw any
	var positionType, predecessorType string
	if err := row.Scan(&positionRaw, &positionType, &predecessorRaw, &predecessorType); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	position, positionOK := sqlvalue.StoredInteger(positionRaw, positionType)
	predecessor, predecessorOK := sqlvalue.StoredInteger(predecessorRaw, predecessorType)
	if !positionOK || !predecessorOK || position <= 0 || predecessor < 0 || predecessor >= position {
		return 0, 0, false, fmt.Errorf("the log stores an invalid position/predecessor pair: %w", syscall.EIO)
	}
	return position, predecessor, true, nil
}

// ReadLogBarrier validates the durable log header in the caller's snapshot.
// Incarnation bytes are copied only when requested and within maxBytes.
func ReadLogBarrier(
	ctx context.Context,
	tx *sql.Tx,
	volume, maxBytes int64,
	loadIncarnation bool,
) (metastore.LogBarrier, error) {
	state, err := dbstate.Validate(ctx, tx)
	if err != nil {
		return metastore.LogBarrier{}, err
	}
	var (
		incarnationType                string
		incarnationBytes               int64
		tailRaw, trimmedRaw, ageRaw    any
		tailType, trimmedType, ageType string
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT typeof(incarnation), length(CAST(incarnation AS BLOB)),
		       CASE WHEN typeof(committed_position) = 'integer' THEN committed_position END,
		       typeof(committed_position),
		       CASE WHEN typeof(trimmed_through) = 'integer' THEN trimmed_through END,
		       typeof(trimmed_through),
		       CASE WHEN typeof(trimmed_by_age) = 'integer' THEN trimmed_by_age END,
		       typeof(trimmed_by_age)
		FROM logs WHERE volume = ?`, volume).Scan(
		&incarnationType, &incarnationBytes,
		&tailRaw, &tailType, &trimmedRaw, &trimmedType, &ageRaw, &ageType,
	); err != nil {
		return metastore.LogBarrier{}, err
	}
	tail, tailOK := sqlvalue.StoredInteger(tailRaw, tailType)
	trimmed, trimmedOK := sqlvalue.StoredInteger(trimmedRaw, trimmedType)
	age, ageOK := sqlvalue.StoredInteger(ageRaw, ageType)
	if incarnationType != "text" || incarnationBytes != 32 || !tailOK || !trimmedOK || !ageOK ||
		tail < 0 || trimmed < 0 || trimmed > tail || (age != 0 && age != 1) ||
		tail > state.ChangeHighWater || trimmed > state.ChangeHighWater {
		return metastore.LogBarrier{}, fmt.Errorf("the volume log header is invalid: %w", syscall.EIO)
	}
	barrier := metastore.LogBarrier{Position: metastore.Position(tail)}
	if !loadIncarnation {
		return barrier, nil
	}
	if incarnationBytes > maxBytes {
		return metastore.LogBarrier{}, fmt.Errorf("the log incarnation has %d bytes under a %d-byte bound: %w",
			incarnationBytes, maxBytes, syscall.EFBIG)
	}
	var raw any
	if err := tx.QueryRowContext(ctx,
		`SELECT incarnation FROM logs WHERE volume = ?`, volume,
	).Scan(&raw); err != nil {
		return metastore.LogBarrier{}, err
	}
	incarnation, ok := raw.(string)
	if !ok || len(incarnation) != 32 || strings.Trim(incarnation, "0123456789abcdef") != "" {
		return metastore.LogBarrier{}, fmt.Errorf("the volume log incarnation is invalid: %w", syscall.EIO)
	}
	barrier.Incarnation = metastore.Incarnation(incarnation)
	return barrier, nil
}
