package changes

import (
	"database/sql"
	"errors"
	"strings"
	"syscall"
	"testing"

	_ "modernc.org/sqlite"
)

func TestChangeMetadataDecoderNamesInvalidScalarStorage(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/metadata.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, _, _, err = scanChangeMetadata(db.QueryRowContext(t.Context(), `
		SELECT
			7, typeof(7), 0, typeof(0), 1, typeof(1), 0, typeof(0),
			1, typeof(1), 0, typeof(NULL),
			'bad-parent', typeof('bad-parent'), 0, typeof(NULL),
			NULL, typeof(NULL), NULL, typeof(NULL), NULL, typeof(NULL),
			NULL, typeof(NULL), NULL, typeof(NULL),
			NULL, typeof(NULL), NULL, typeof(NULL),
			0, typeof(NULL),
			0, typeof(0), 0, typeof(0)`), 1)
	if !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "change 7 stores from_parent as text") {
		t.Fatalf("decoding a text from_parent returned %v", err)
	}

	if _, err := requiredStoredInteger("position", "bad-position", "text"); !errors.Is(err, syscall.EIO) || !strings.Contains(err.Error(), "a change stores position as text") {
		t.Fatalf("decoding a text position returned %v", err)
	}
}
