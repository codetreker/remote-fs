package advisory

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestRetiredSessionKeepsOnlyAdvancingCloseEpoch(t *testing.T) {
	coordinator := fixture(t, DefaultConfig())
	now := time.Unix(100, 0)
	coordinator.now = func() time.Time { return now }
	options := storage.DefaultFileSessionOptions()
	options.History = time.Minute
	session, err := coordinator.NewSession(options, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Retire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := session.History(t.Context()); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("ordinary history after retirement = %v", err)
	}
	if err := session.IOHealth(t.Context(), 1); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("ordinary I/O after retirement = %v", err)
	}
	first, err := session.CloseEpoch(t.Context())
	if err != nil || first != 1 {
		t.Fatalf("initial cleanup epoch = %d, %v", first, err)
	}
	now = now.Add(options.History + time.Nanosecond)
	second, err := session.CloseEpoch(t.Context())
	if err != nil || second != first+1 {
		t.Fatalf("advanced cleanup epoch = %d, %v", second, err)
	}
}
