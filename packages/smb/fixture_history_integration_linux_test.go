//go:build linux

package smb

import (
	"context"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestCommandFixtureRenewsNativeActionEpochAfterHistoryRotation(t *testing.T) {
	for _, transport := range []string{"direct", "http"} {
		t.Run(transport, func(t *testing.T) {
			factory := newRealSMBFixture
			if transport == "http" {
				factory = newHTTPSMBFixture
			}
			fixture := factory(t)
			options := fixtureFileSessionOptions()
			options.History = 2 * time.Second
			raw, err := fixture.tree.export.share.Backend.NewFileSession(t.Context(), options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := raw.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			initial, err := raw.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			identity, err := raw.(storage.FileSessionIdentity).FileSessionIdentity(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			fixture.raw = raw
			fixture.tree.authority = &authoritySession{raw: raw, identity: identity, export: fixture.tree.export, actionEpoch: initial.ActionEpoch, deadline: time.Now().Add(initial.Remaining)}
			deadline := time.NewTimer(initial.HistoryRemaining)
			defer deadline.Stop()
			select {
			case <-deadline.C:
			case <-t.Context().Done():
				t.Fatal(t.Context().Err())
			}
			rotated, err := raw.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if rotated.ActionEpoch == initial.ActionEpoch {
				t.Fatalf("native history did not rotate: initial=%+v current=%+v", initial, rotated)
			}
			if _, status, _ := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("stale", 3, accessReadData)); status != statusSessionDeleted {
				t.Fatalf("old action epoch CREATE=%#x", status)
			}
			renewSMBCommandFixture(t, fixture)
			current, err := raw.Status(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			fixture.tree.authority.installMu.RLock()
			installed := fixture.tree.authority.actionEpoch
			fixture.tree.authority.installMu.RUnlock()
			if installed != current.ActionEpoch || installed == initial.ActionEpoch {
				t.Fatalf("renewed epoch installed=%d native=%d initial=%d", installed, current.ActionEpoch, initial.ActionEpoch)
			}
			_, status, id := fixture.connection.createFile(t.Context(), fixture.session, fixture.tree, createRequestForTest("fresh", 3, accessReadData))
			if status != statusOK {
				t.Fatalf("new action after renewal CREATE=%#x", status)
			}
			closeFixtureHandle(t, fixture, id)
		})
	}
}
