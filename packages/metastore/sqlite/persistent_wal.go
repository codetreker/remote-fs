package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"syscall"

	moderncsqlite "modernc.org/sqlite"
)

// persistentWALConnector keeps the WAL across abnormal pool closure so recovery retains the
// fact needed to reconcile a failed witness publication. A successful witnessed Close clears
// the flag only after every frame and the checkpoint witness are durable.
type persistentWALConnector struct{ driver.Connector }

func (c persistentWALConnector) Connect(ctx context.Context) (driver.Conn, error) {
	connection, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	control, ok := connection.(moderncsqlite.FileControl)
	if !ok {
		return nil, errors.Join(
			openCloseFailure("SQLite persistent WAL connection", connection.Close()),
			fmt.Errorf("the SQLite driver does not expose persistent WAL control: %w", syscall.EIO),
		)
	}
	mode, err := control.FileControlPersistWAL("main", 1)
	if err != nil || mode != 1 {
		return nil, errors.Join(
			openCloseFailure("SQLite persistent WAL connection", connection.Close()),
			fmt.Errorf("enabling persistent SQLite WAL returned mode %d: %v: %w", mode, err, syscall.EIO),
		)
	}
	return connection, nil
}

func openPersistentWriterPool(
	ctx context.Context,
	database string,
	maxConnections int,
) (*sql.DB, error) {
	base, err := moderncsqlite.NewConnector(poolDataSource(database, true))
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(persistentWALConnector{Connector: base})
	db.SetMaxOpenConns(maxConnections)
	if err := requireFullSynchronous(ctx, db); err != nil {
		return nil, errors.Join(err, poolCloseFailure("writer pool", db.Close()))
	}
	return db, nil
}

func disablePersistentWAL(ctx context.Context, db *sql.DB) error {
	connection, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquiring the SQLite writer connection: %w", failure(err))
	}
	rawErr := connection.Raw(func(driverConnection any) error {
		control, ok := driverConnection.(moderncsqlite.FileControl)
		if !ok {
			return fmt.Errorf("the SQLite driver does not expose persistent WAL control: %w", syscall.EIO)
		}
		mode, err := control.FileControlPersistWAL("main", 0)
		if err != nil || mode != 0 {
			return fmt.Errorf("disabling persistent SQLite WAL returned mode %d: %v: %w",
				mode, err, syscall.EIO)
		}
		return nil
	})
	return errors.Join(rawErr, connection.Close())
}
