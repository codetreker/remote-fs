package sqlite

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestUncertainCommitErrorPreservesItsCauseAndClassification(t *testing.T) {
	cause := errors.New("commit result unavailable")
	err := &uncertainCommitError{err: cause}
	if err.Error() != cause.Error() {
		t.Fatalf("uncertain commit says %q, want %q", err.Error(), cause.Error())
	}
	if !errors.Is(err, cause) {
		t.Fatalf("uncertain commit does not retain its cause: %v", err)
	}
	if !isUncertainCommit(errors.Join(errors.New("open failed"), err)) {
		t.Fatal("a joined uncertain commit was not classified as uncertain")
	}
}

func TestCoordinatorPoisonPreservesTheFirstDurabilityFailureAsEIO(t *testing.T) {
	coordinator := new(databaseCoordinator)
	coordinator.poisonWith(syscall.EEXIST)
	first := coordinator.healthy()
	if !errors.Is(first, syscall.EIO) || !errors.Is(first, syscall.EEXIST) {
		t.Fatalf("poisoned coordinator returned %v, want EIO retaining EEXIST", first)
	}
	coordinator.poisonWith(syscall.ENOSPC)
	second := coordinator.healthy()
	if !errors.Is(second, syscall.EEXIST) || errors.Is(second, syscall.ENOSPC) {
		t.Fatalf("second poison replaced the first durability failure: %v", second)
	}
}

func TestChangeMetadataDecoderNamesInvalidScalarStorage(t *testing.T) {
	db, err := openPool(t.Context(), t.TempDir()+"/metadata.db", true, 1)
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
