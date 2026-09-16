//go:build !windows

package windows

import (
	"context"

	"github.com/codetreker/remote-fs/packages/smb"
)

func beginAuthentication(context.Context) (smb.Authentication, error) {
	return nil, ErrUnsupported
}

func currentUserSID() (string, error) { return "", ErrUnsupported }
