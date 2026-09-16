package schema

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/changes"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/dbstate"
	"github.com/codetreker/remote-fs/packages/metastore/sqlite/internal/sqlerr"
	modernsqlite "modernc.org/sqlite"
)

type migrationCommitConnector struct {
	path  string
	fault error
}

func (c *migrationCommitConnector) Driver() driver.Driver { return &modernsqlite.Driver{} }
func (c *migrationCommitConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.Driver().Open(c.path)
	if err != nil {
		return nil, err
	}
	return &migrationCommitConnection{Conn: conn, connector: c}, nil
}

type migrationCommitConnection struct {
	driver.Conn
	connector *migrationCommitConnector
}

func (c *migrationCommitConnection) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return &migrationCommitTransaction{Tx: tx, connector: c.connector}, nil
}

type migrationCommitTransaction struct {
	driver.Tx
	connector *migrationCommitConnector
}

func (t *migrationCommitTransaction) Commit() error {
	if err := t.Tx.Commit(); err != nil {
		return err
	}
	return t.connector.fault
}

func TestClientMigrationUnknownCommitDoesNotPretendRollback(t *testing.T) {
	source := historicalMetadataDatabase(t, 5)
	var sequence int
	var name, path string
	if err := source.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("commit completed but its result was lost")
	connector := &migrationCommitConnector{path: path, fault: failure}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	id, root, state, err := PrepareConfigured(t.Context(), db, "legacy", "", changes.DefaultWindow(), 1000, 4, 138, nil)
	if !errors.Is(err, failure) || !sqlerr.IsUncertainCommit(err) || id != 0 || root != 0 || state != (dbstate.State{}) {
		t.Fatalf("unknown migration commit exposed success: id=%d root=%d state=%+v error=%v", id, root, state, err)
	}
	var version, generation, nodeHigh, changeHigh, rows, position int64
	var incarnation string
	if err := db.QueryRow(`SELECT version,generation,node_high_water,change_high_water,
		(SELECT count(*) FROM changes),(SELECT position FROM changes),(SELECT incarnation FROM logs)
		FROM schema_version CROSS JOIN database_state`).Scan(&version, &generation, &nodeHigh, &changeHigh, &rows, &position, &incarnation); err != nil {
		t.Fatal(err)
	}
	if version != 6 || generation != 6 || nodeHigh != 2 || changeHigh != 7 || rows != 1 || position != 7 || incarnation != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("unknown commit lost committed migration facts: version=%d generation=%d ids=%d/%d history=%d/%d/%q", version, generation, nodeHigh, changeHigh, rows, position, incarnation)
	}
	connector.fault = nil
	id, root, state, err = PrepareConfigured(t.Context(), db, "legacy", "", changes.DefaultWindow(), 1000, 4, 138, nil)
	if err != nil || id != 1 || root != 1 || state.Generation != 7 || state.NodeHighWater != 2 || state.ChangeHighWater != 7 {
		t.Fatalf("reopen after unknown commit: volume=%d root=%d state=%+v error=%v", id, root, state, err)
	}
}
