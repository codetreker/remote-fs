package locked_test

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/locking"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/lockcontract/memoryfixture"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

type inlineCloseStorageProbe struct {
	locked.Backend
	session  storage.FileSession
	sessions int
}

func (*inlineCloseStorageProbe) CheckPublicationAccounting() error    { return nil }
func (*inlineCloseStorageProbe) CheckFileStorage() error              { return nil }
func (*inlineCloseStorageProbe) Usage(context.Context) (int64, error) { return 0, nil }

func (p *inlineCloseStorageProbe) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, error) {
	p.sessions++
	return p.session, nil
}

type inlineCloseSessionProbe struct {
	storage.FileSession
	err          error
	closes       int
	closeContext context.Context
}

func (p *inlineCloseSessionProbe) CheckInlineCloseSettlement() error { return p.err }
func (p *inlineCloseSessionProbe) Close(ctx context.Context) error {
	_, err := p.CloseWithResult(ctx)
	return err
}
func (p *inlineCloseSessionProbe) CloseWithResult(ctx context.Context) (storage.ReferenceCloseResult, error) {
	p.closes++
	p.closeContext = ctx
	return storage.ReferenceCloseResult{Released: true, Determined: true}, nil
}

func TestInlineCloseSettlementFollowsNestedQuotaWrapper(t *testing.T) {
	meta, backend := memoryfixture.New(t, "ws", 0, locking.DefaultOptions())
	ctx := t.Context()
	root, err := backend.Stat(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	barrier, err := meta.Barrier(ctx, 1024)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := meta.ObjectStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("released close still owes an adapter barrier")
	for _, test := range []struct {
		name    string
		missing bool
		err     error
	}{
		{name: "missing", missing: true, err: syscall.EOPNOTSUPP},
		{name: "noninline", err: syscall.EOPNOTSUPP},
		{name: "failed", err: cause},
		{name: "inline"},
	} {
		if !t.Run(test.name, func(t *testing.T) {
			t.Cleanup(func() {
				if got, err := backend.Stat(ctx, ""); err != nil || !reflect.DeepEqual(got, root) {
					t.Fatalf("capability check changed root: %+v, %v", got, err)
				}
				if got, err := meta.Barrier(ctx, 1024); err != nil || got != barrier {
					t.Fatalf("capability check changed history: %+v, %v", got, err)
				}
				if got, err := meta.ObjectStatus(ctx); err != nil || got != objects {
					t.Fatalf("capability check changed object maintenance: %+v, %v", got, err)
				}
				if entries, err := backend.List(ctx, ""); err != nil || len(entries) != 0 {
					t.Fatalf("capability check changed tree: %+v, %v", entries, err)
				}
				if used, err := meta.Usage(ctx); err != nil || used != 0 {
					t.Fatalf("capability check changed usage: %d, %v", used, err)
				}
				if _, err := locked.New(backend); err != nil {
					t.Fatalf("healthy facade rejected shared backing: %v", err)
				}
			})
			probe := &inlineCloseSessionProbe{err: test.err}
			var nativeSession storage.FileSession = probe
			if test.missing {
				nativeSession = &struct{ storage.FileSession }{FileSession: probe}
			}
			backing := &inlineCloseStorageProbe{Backend: backend, session: nativeSession}
			quota, err := limited.New(t.Context(), backing, limited.MinLimit)
			if err != nil {
				t.Fatal(err)
			}
			facade, err := locked.New(quota)
			if err != nil {
				t.Fatal(err)
			}
			session, err := facade.NewFileSession(t.Context(), storage.DefaultFileSessionOptions())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				marker := new(int)
				ctx := context.WithValue(context.Background(), marker, true)
				if err := session.Close(ctx); err != nil || probe.closes != 1 {
					t.Errorf("closing child session = %v after %d calls", err, probe.closes)
				}
				if probe.closeContext == nil || probe.closeContext.Err() != nil || probe.closeContext.Value(marker) != true {
					t.Error("closing child session lost its cleanup context")
				}
			})
			if backing.sessions != 1 {
				t.Fatalf("native sessions = %d, want one", backing.sessions)
			}
			if err := session.(storage.InlineCloseSettlement).CheckInlineCloseSettlement(); !errors.Is(err, test.err) {
				t.Fatalf("nested inline close preflight = %v; expected %v", err, test.err)
			}
		}) {
			t.FailNow()
		}
	}
}
