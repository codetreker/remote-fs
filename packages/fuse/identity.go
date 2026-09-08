package fuse

import (
	"sync"
	"syscall"
)

// Namespace IDs are stable through rename and are never reused. Reporting those IDs
// directly preserves inode numbers even when another mount moves a node to a name
// this mount has never seen. The tree records name membership for local invalidation.
type identities struct {
	mu     sync.Mutex
	handed uint64
}

type identity struct {
	all      *identities
	ino      uint64
	kind     uint32
	node     uint64
	serial   uint64
	children map[string]*identity
}

func rootIdentity(nodeID uint64) *identity {
	all := &identities{handed: 1}
	return &identity{
		all: all, ino: nodeID, node: nodeID, serial: 1,
		kind: syscall.S_IFDIR, children: map[string]*identity{},
	}
}

func (i *identity) child(name string, mode uint32, nodeID uint64) *identity {
	kind := mode & syscall.S_IFMT
	i.all.mu.Lock()
	defer i.all.mu.Unlock()

	if known := i.children[name]; known != nil && known.kind == kind && known.node == nodeID {
		return known
	}
	fresh := i.all.mint(kind, nodeID)
	i.children[name] = fresh
	return fresh
}

func (i *identity) forget(name string) {
	i.all.mu.Lock()
	defer i.all.mu.Unlock()
	delete(i.children, name)
}

// The local serial distinguishes membership learned during a listing from membership
// already known before it. Native IDs do not encode when this mount learned a name.
func (i *identity) given() uint64 {
	i.all.mu.Lock()
	defer i.all.mu.Unlock()
	return i.all.handed
}

func (i *identity) keepOnly(present map[string]struct{}, before uint64) {
	i.all.mu.Lock()
	defer i.all.mu.Unlock()
	for name, known := range i.children {
		if _, listed := present[name]; !listed && known.serial <= before {
			delete(i.children, name)
		}
	}
}

// A local rename carries the same identity tree to its destination. The destination's
// former inode can remain alive through retained descriptors, but loses this name.
func (i *identity) move(name string, to *identity, newName string) {
	i.all.mu.Lock()
	defer i.all.mu.Unlock()
	moved, named := i.children[name]
	delete(i.children, name)
	delete(to.children, newName)
	if named {
		to.children[newName] = moved
	}
}

func (all *identities) mint(kind uint32, nodeID uint64) *identity {
	all.handed++
	fresh := &identity{all: all, ino: nodeID, kind: kind, node: nodeID, serial: all.handed}
	if kind == syscall.S_IFDIR {
		fresh.children = map[string]*identity{}
	}
	return fresh
}
