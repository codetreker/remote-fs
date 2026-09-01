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
// database can be wrong about. Everything else survives, which is the whole content of the
// comparison: AUTOINCREMENT, WITHOUT ROWID, REFERENCES and the column types are visible only in
// this text, and a structural reading through PRAGMA table_info would drop the first two
// entirely.
//
// A quoted run is one token and is compared exactly. Inside quotes there is no syntax to
// normalise — it is a default value, a check constraint's operand or a quoted name — so
// splitting it on the whitespace it contains would make `DEFAULT 'not set'` and
// `DEFAULT 'not  set'` compare equal, which is the false negative this comparison exists to
// avoid.
func Structure(dump string) string {
	var (
		tokens []string
		plain  strings.Builder
	)
	flush := func() {
		tokens = append(tokens, strings.Fields(plain.String())...)
		plain.Reset()
	}
	for i := 0; i < len(dump); {
		switch c := dump[i]; c {
		case '\'', '"', '`':
			flush()
			end := pastClosingQuote(dump, i)
			tokens = append(tokens, dump[i:end])
			i = end
		case '(', ')', ',':
			plain.WriteByte(' ')
			plain.WriteByte(c)
			plain.WriteByte(' ')
			i++
		default:
			plain.WriteByte(c)
			i++
		}
	}
	flush()
	return strings.Join(tokens, " ")
}

// pastClosingQuote returns the index just past the run opened at start, where a doubled quote is
// one character of the value rather than the end of it — which is how SQLite escapes a quote
// inside all three of the marks it accepts.
//
// A run nothing closes takes the rest of the dump. That is not a schema SQLite would produce,
// and keeping it whole is the reading that cannot make two different dumps compare equal.
func pastClosingQuote(dump string, start int) int {
	quote := dump[start]
	for i := start + 1; i < len(dump); i++ {
		if dump[i] != quote {
			continue
		}
		if i+1 < len(dump) && dump[i+1] == quote {
			i++
			continue
		}
		return i + 1
	}
	return len(dump)
}
