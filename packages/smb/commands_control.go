package smb

import "github.com/codetreker/remote-fs/packages/smb/internal/wire"

func (c *connection) connectControl(s *session, h *wire.Header) ([]byte, uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return nil, statusSessionDeleted
	}
	if len(s.trees) >= c.server.config.Limits.MaxTrees {
		return nil, statusResources
	}
	c.mu.Lock()
	c.nextTree++
	id := c.nextTree
	c.mu.Unlock()
	s.trees[id] = &tree{kind: controlTree, id: id, sessionID: s.id, done: make(chan struct{})}
	h.TreeID = id
	return wire.TreeConnectResponseBody(2, 0x30, 0, 0x001f01ff), 0
}

func (c *connection) control(s *session, t *tree, r wire.Request) ([]byte, uint32) {
	if r.Header.Command == wire.IOCTL {
		request, err := r.IOCTL()
		if err != nil {
			return nil, statusInvalid
		}
		// SMB 3.1.1 authenticates negotiation through its preauthentication hash.
		// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/0b7803eb-d561-48a4-8654-327803f59ec6
		if request.Code == 0x00140204 {
			c.cancel()
			_ = c.net.Close()
		}
		return nil, statusUnsupported
	}
	if r.Header.Command != wire.TreeDisconnect {
		return nil, statusUnsupported
	}
	if err := r.Empty(); err != nil {
		return nil, statusInvalid
	}
	if err := c.closeTree(t); err != nil {
		return nil, statusError(err)
	}
	s.mu.Lock()
	delete(s.trees, t.id)
	s.mu.Unlock()
	return wire.EmptyResponseBody(), 0
}
