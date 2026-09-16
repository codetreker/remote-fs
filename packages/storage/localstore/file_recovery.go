package localstore

import (
	"context"
	"time"

	"github.com/codetreker/remote-fs/packages/metastore/sqlite"
)

func (d *durableMetastore) bindFileRecovery(ctx context.Context, root *rootAnchor, identity string, started time.Time) error {
	pending, err := d.Store.FileLeaseInitializationPending(ctx)
	if err != nil {
		return err
	}
	anchor, err := sqlite.OpenLeaseAnchor(sqlite.LeaseAnchorConfig{
		Domain:        sqlite.LeaseDomainFile,
		Directory:     root.path,
		Name:          ".file-leases",
		Identity:      identity,
		BindingFD:     root.fd,
		RecoveryStart: started,
		Initialize:    pending,
	})
	if err != nil {
		return err
	}
	d.fileLeaseAnchor = anchor
	return d.Store.ConfigureFileLeaseRecovery(ctx, sqlite.LeaseRecoveryConfig{
		Witness:       anchor,
		RecoveryStart: started,
		StateID:       anchor.StateID(),
		Initialize:    anchor.Initializing(),
	})
}
