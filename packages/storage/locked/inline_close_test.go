package locked_test

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/storage/limited"
	"github.com/codetreker/remote-fs/packages/storage/locked"
)

type inlineCloseStorageProbe struct {
	locked.Backend
	session storage.FileSession
}

func (*inlineCloseStorageProbe) CheckPublicationAccounting() error    { return nil }
func (*inlineCloseStorageProbe) CheckFileStorage() error              { return nil }
func (*inlineCloseStorageProbe) Usage(context.Context) (int64, error) { return 0, nil }

func (p *inlineCloseStorageProbe) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, error) {
	return p.session, nil
}

type inlineCloseSessionProbe struct {
	storage.FileSession
	err error
}

func (p *inlineCloseSessionProbe) CheckInlineCloseSettlement() error { return p.err }

func TestInlineCloseSettlementFollowsNestedQuotaWrapper(t *testing.T) {
	cause := errors.New("released close still owes an adapter barrier")
	for _, test := range []struct {
		name    string
		session storage.FileSession
		err     error
	}{
		{name: "missing", session: &struct{ storage.FileSession }{}, err: syscall.EOPNOTSUPP},
		{name: "noninline", session: &inlineCloseSessionProbe{err: syscall.EOPNOTSUPP}, err: syscall.EOPNOTSUPP},
		{name: "failed", session: &inlineCloseSessionProbe{err: cause}, err: cause},
		{name: "inline", session: &inlineCloseSessionProbe{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			quota, err := limited.New(t.Context(), &inlineCloseStorageProbe{Backend: pairedBackend(t), session: test.session}, limited.MinLimit)
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
			if err := session.(storage.InlineCloseSettlement).CheckInlineCloseSettlement(); !errors.Is(err, test.err) {
				t.Fatalf("nested inline close preflight = %v; expected %v", err, test.err)
			}
		})
	}
}
