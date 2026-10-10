package objectstore

import (
	"syscall"

	"github.com/codetreker/remote-fs/packages/storage"
)

var _ storage.InlineCloseSettlement = (*fileSession)(nil)

func (s *fileSession) CheckInlineCloseSettlement() error {
	if err := s.storage.CheckFileStorage(); err != nil {
		return err
	}
	native, ok := s.native.(storage.InlineCloseSettlement)
	if !ok {
		return syscall.EOPNOTSUPP
	}
	return native.CheckInlineCloseSettlement()
}
