//go:build !windows

package windows

import (
	"context"

	"github.com/codetreker/remote-fs/packages/smb"
)

func beginAuthentication(context.Context) (smb.Authentication, error) {
	return nil, ErrUnsupported
}

func currentIdentity() (smb.Principal, error) { return smb.Principal{}, ErrUnsupported }
