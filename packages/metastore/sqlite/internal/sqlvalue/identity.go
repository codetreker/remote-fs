package sqlvalue

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"syscall"

	"github.com/codetreker/remote-fs/packages/metastore"
)

// Object keys must remain unrelated to paths and unique across restored databases.
func NewKey() (metastore.Key, error) {
	value, err := RandomHex(16)
	if err != nil {
		return "", fmt.Errorf("%w: minting an object key: %w", syscall.EIO, err)
	}
	return metastore.Key(value), nil
}

func RandomHex(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}
