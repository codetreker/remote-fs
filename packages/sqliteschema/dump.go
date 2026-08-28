package sqliteschema

import (
	"context"
	"database/sql"
	"strings"
)

// Queryer is what Dump reads a database's layout through, satisfied by both *sql.DB and
// *sql.Tx. Which one a caller has depends on whether it is looking at a database it has open
// or at one it is part way through building.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Dump reads back everything SQLite holds about a database's layout.
//
// This is the current schema written down: the migrations no longer state it anywhere, and
// after a few of them "what does this table look like" is a question spread across files.
// Recording the result beside them answers it, and makes an edit to a landed migration show up
// as a diff in the review whether or not anyone thought to mention it.
//
// It is what SQLite reports rather than a copy of the statements, so it holds what was really
// built: implicit indexes, sqlite_sequence, and the text SQLite chose to store for each object.
// Objects with no statement of their own — the index SQLite builds for a PRIMARY KEY, and the
// like — are named rather than skipped, because whether one exists is part of the layout even
// though nothing wrote it.
//
// Statements are re-indented rather than taken as stored. SQLite keeps the text of a CREATE
// statement exactly as it was given, so the same table written by a Go string literal and by a
// .sql file is stored with different leading whitespace. For comparing two databases, pass the
// result through Structure; this rendering is for reading.
func Dump(ctx context.Context, db Queryer) (string, error) {
	rows, err := db.QueryContext(ctx, `SELECT type, name, sql FROM sqlite_schema ORDER BY type, name`)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var out strings.Builder
	for rows.Next() {
		var kind, name string
		var statement sql.NullString
		if err := rows.Scan(&kind, &name, &statement); err != nil {
			return "", err
		}
		if statement.Valid {
			out.WriteString(reindent(statement.String) + ";\n\n")
			continue
		}
		out.WriteString("-- " + kind + " " + name + ", which SQLite maintains itself\n\n")
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return out.String(), nil
}

// reindent puts a stored statement on one indentation: the first line flush, every line under
// it one tab in, and a line closing the column list back at the margin.
func reindent(statement string) string {
	lines := strings.Split(statement, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimSpace(line)
		if i > 0 && !strings.HasPrefix(lines[i], ")") {
			lines[i] = "\t" + lines[i]
		}
	}
	return strings.Join(lines, "\n")
}

// Structure reduces a dump to the tokens in it, so that two databases holding the same layout
// compare equal however the statements that built them were laid out.
//
// Where a line breaks and whether a bracket is followed by a space are properties of the text
// SQLite was handed and of nothing else — one schema's tables may have been created from Go
// string literals and another's from .sql files — and the layout of neither is something a
// database can be wrong about. Every token survives, which is the whole content of the
// comparison: AUTOINCREMENT, WITHOUT ROWID, REFERENCES and the column types are visible only in
// this text, and a structural reading through PRAGMA table_info would drop the first two
// entirely.
func Structure(dump string) string {
	spaced := dump
	for _, punctuation := range []string{"(", ")", ","} {
		spaced = strings.ReplaceAll(spaced, punctuation, " "+punctuation+" ")
	}
	return strings.Join(strings.Fields(spaced), " ")
}
