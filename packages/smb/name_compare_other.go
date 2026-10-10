//go:build !windows

package smb

func platformNameComparer() (nameComparer, error) {
	return portableNameCompare, nil
}
