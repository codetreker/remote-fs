package smb

import (
	"github.com/codetreker/remote-fs/packages/storage"
	"syscall"
)

func checkTreeCapabilities(session storage.FileSession) error {
	if err := checkCreateCapabilities(session); err != nil {
		return err
	}
	identity, ok := session.(storage.FileSessionIdentity)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := identity.CheckFileSessionIdentity(); err != nil {
		return err
	}
	stable, ok := session.(storage.StableReferenceIdentity)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := stable.CheckStableReferenceIdentity(); err != nil {
		return err
	}
	metadata, ok := session.(storage.OpenMetadataAccess)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	if err := metadata.CheckOpenMetadataAccess(); err != nil {
		return err
	}
	recovery, ok := session.(storage.RecoverableReferenceClose)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return recovery.CheckRecoverableReferenceClose()
}
