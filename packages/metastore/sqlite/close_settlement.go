package sqlite

import "github.com/codetreker/remote-fs/packages/storage"

var _ storage.InlineCloseSettlement = (*Store)(nil)

func (s *Store) CheckInlineCloseSettlement() error { return s.CheckFileStore() }
