package smb

import (
	"math"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func (c *connection) connectControl(s *session, header *wire.Header) ([]byte, uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return nil, statusSessionDeleted
	}
	if len(s.trees)+s.openingTrees >= c.server.config.Limits.MaxTrees {
		return nil, statusResources
	}
	c.mu.Lock()
	if c.nextTree == math.MaxUint32 {
		c.mu.Unlock()
		return nil, statusResources
	}
	c.nextTree++
	id := c.nextTree
	c.mu.Unlock()
	s.trees[id] = &tree{kind: controlTree, id: id, sessionID: s.id, session: s, done: make(chan struct{})}
	header.TreeID = id
	return wire.TreeConnectResponseBody(2, 0x30, 0, 0x001f01ff), statusOK
}

func (c *connection) control(s *session, tree *tree, request wire.Request) ([]byte, uint32) {
	if request.Header.Command != wire.TreeDisconnect {
		return nil, statusUnsupported
	}
	if err := request.Empty(); err != nil {
		return nil, statusInvalid
	}
	if err := c.closeTree(tree); err != nil {
		return nil, statusError(err)
	}
	s.mu.Lock()
	delete(s.trees, tree.id)
	s.mu.Unlock()
	return wire.EmptyResponseBody(), statusOK
}
