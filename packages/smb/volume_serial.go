package smb

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/codetreker/remote-fs/packages/storage"
)

func stableVolumeSerial(volume storage.VolumeID) (uint64, error) {
	if err := (storage.BackendIdentityResult{Volume: volume, Authority: "serial-projection", RootNodeID: 1}).Check(); err != nil {
		return 0, err
	}
	// A finite-width projection can collide. Publish rejects distinct volumes
	// sharing this serial while either export still owns its reservation.
	digest := sha256.Sum256([]byte("remote-fs/smb/volume-serial/v1\x00" + string(volume)))
	return binary.LittleEndian.Uint64(digest[:8]), nil
}
