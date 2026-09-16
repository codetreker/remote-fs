package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	modernc "modernc.org/sqlite"
)

var preparationDriverSequence atomic.Uint64

type preparationOutcomeDriver struct {
	terminal string
	cause    error
}

func (d preparationOutcomeDriver) Open(name string) (driver.Conn, error) {
	conn, err := (&modernc.Driver{}).Open(name)
	if err != nil {
		return nil, err
	}
	return &preparationOutcomeConn{Conn: conn, fault: d}, nil
}

type preparationOutcomeConn struct {
	driver.Conn
	fault preparationOutcomeDriver
}

func (c *preparationOutcomeConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return &preparationOutcomeTx{Tx: tx, fault: c.fault}, nil
}

func (c *preparationOutcomeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *preparationOutcomeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}

type preparationOutcomeTx struct {
	driver.Tx
	fault preparationOutcomeDriver
}

func (tx *preparationOutcomeTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.fault.terminal == "commit" {
		return tx.fault.cause
	}
	return nil
}

func (tx *preparationOutcomeTx) Rollback() error {
	if err := tx.Tx.Rollback(); err != nil {
		return err
	}
	if tx.fault.terminal == "rollback" {
		return tx.fault.cause
	}
	return nil
}

func TestPreparationPreservesUnknownCommitAndRollbackOutcomes(t *testing.T) {
	for _, terminal := range []string{"commit", "rollback"} {
		t.Run(terminal, func(t *testing.T) {
			cause := errors.New("SQLite terminal result unavailable")
			name := fmt.Sprintf("schema-preparation-outcome-%d", preparationDriverSequence.Add(1))
			sql.Register(name, preparationOutcomeDriver{terminal: terminal, cause: cause})
			db, err := sql.Open(name, filepath.Join(t.TempDir(), "metadata.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			var durable *DurableOpen
			if terminal == "rollback" {
				durable = &DurableOpen{Mode: RequireExistingVolume}
			}
			id, root, state, err := PrepareConfigured(t.Context(), db, "volume", "", changes.DefaultWindow(), 1000, 1<<20, durable)
			if !errors.Is(err, cause) || !sqlerr.IsUncertainCommit(err) || id != 0 || root != 0 || state.DatabaseID != "" {
				t.Fatalf("unknown terminal result became a confirmed outcome: id=%d root=%d state=%+v error=%v", id, root, state, err)
			}
			var exists int
			if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='table' AND name='schema_version'`).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if terminal == "commit" {
				var version int
				if exists != 1 {
					t.Fatal("commit injection did not commit the real SQLite transaction")
				}
				if err := db.QueryRow(`SELECT version FROM schema_version`).Scan(&version); err != nil || version != 6 {
					t.Fatalf("committed schema = %d, %v", version, err)
				}
			} else if exists != 0 {
				t.Fatal("rollback injection did not roll back the real SQLite transaction")
			}
		})
	}
}
