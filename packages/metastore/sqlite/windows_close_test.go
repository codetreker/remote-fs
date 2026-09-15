package sqlite

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestWindowsFailedCloseStillReachesItsRetirementDeadline(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(fmt.Sprint(persistent), func(t *testing.T) {
			s, err := OpenLocking(t.Context(), lockingTestConfig(t))
			if err != nil {
				t.Fatal(err)
			}
			refused := errors.New("retirement accounting unavailable")
			refuse := func(int64, int64) (storage.PublicationSettlement, error) { return nil, refused }
			state, err := s.WindowsState(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			activation, err := storage.NewLockRequestID(state.ActionEpoch)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.EnableWindows(t.Context(), activation); err != nil {
				t.Fatal(err)
			}
			creation := t.Context()
			if persistent {
				creation = storage.WithPublicationAccounting(creation, refuse)
			}
			inner, err := s.NewWindowsSession(creation, storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			ws := inner.(*windowsSession)
			t.Cleanup(func() {
				if err := ws.Close(context.Background()); err != nil {
					if !persistent || !errors.Is(err, refused) {
						t.Error(err)
					}
					if err := s.Abort(); err != nil {
						t.Error(err)
					}
					return
				}
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			f := windowsOpen(t, ws, "file", storage.WindowsAllAccess, storage.WindowsShareAll)
			if _, err := f.SetDeletePending(t.Context(), true, windowsActionID(t, ws)); err != nil {
				t.Fatal(err)
			}
			closeAttempted, expired := make(chan struct{}), make(chan struct{})
			if err := s.coordinator.commit.acquire(t.Context()); err != nil {
				t.Fatal(err)
			}
			ws.timer.Stop()
			ws.expires = time.Now().Add(100 * time.Millisecond)
			ws.timer = time.AfterFunc(time.Until(ws.expires), func() { <-closeAttempted; ws.expire(); close(expired) })
			s.coordinator.commit.release()
			if err := ws.Close(storage.WithPublicationAccounting(t.Context(), refuse)); !errors.Is(err, refused) {
				close(closeAttempted)
				t.Fatalf("earlyclose=%v", err)
			}
			if ws.active || ws.closed || s.coordinator.healthy() != nil {
				close(closeAttempted)
				t.Fatal("known close refusal changed retirement outcome")
			}
			close(closeAttempted)
			select {
			case <-expired:
			case <-time.After(3 * time.Second):
				t.Fatal("abandoned close never reached its original deadline")
			}
			if persistent {
				if !errors.Is(s.coordinator.healthy(), refused) {
					t.Fatal("unresolved expiry did not fence the authority")
				}
				if _, err := s.Stat(t.Context(), "file"); !errors.Is(err, syscall.EIO) {
					t.Fatalf("fencedread=%v", err)
				}
				if ws.closed || len(ws.files) != 1 || s.fileDomain.windows.access.OpenCount(uint64(f.id)) != 1 {
					t.Fatal("unknown cleanup fabricated released ownership")
				}
			} else {
				if !ws.closed || len(ws.files) != 0 || len(s.fileDomain.windows.sessions) != 0 || s.fileDomain.windows.access.OpenCount(uint64(f.id)) != 0 {
					t.Fatal("deadline retained completed retirement")
				}
				if _, err := s.Stat(t.Context(), "file"); !errors.Is(err, syscall.ENOENT) {
					t.Fatalf("expiry deletion=%v", err)
				}
			}
		})
	}
}
