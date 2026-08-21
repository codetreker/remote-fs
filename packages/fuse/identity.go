package fuse

import (
	"sync"
	"syscall"
)

// The kernel knows a node by a number, and it holds that number against everything else it
// has been told about the node. Two live nodes reported under one number are one node to
// the kernel: it maps one of them against the other's length, and the program reading
// there takes SIGBUS.
//
// Nothing arrives from below that could serve as that number. The namespace is addressed
// by path and says nothing about what a node is, and a path cannot stand in for an
// identity either — a rename moves a node to another name and frees the one it left, and
// the next node created there is a second, different node that would be given the first
// one's number. So the numbers are allocated here, and this is the record of which of them
// is in use for which name.
//
// The record is a tree of names rather than a table of paths. Naming one node costs the
// depth of its name and nothing costs the size of the record: a directory that moves
// carries the identities beneath it by being carried itself, and a name that is dropped
// takes the subtree hanging off it with it. Only a listing walks anything, and only the one
// directory it listed.
type identities struct {
	mu sync.Mutex

	// handed is the last number handed out. The next is one higher and no number is ever
	// handed out twice, so the number of a node that is gone names nothing afterwards.
	// Reaching the range the FUSE library numbers unidentified nodes from, which starts at
	// 1<<63, would take that many allocations.
	handed uint64
}

// identity is what one name refers to: the number the kernel knows that node by, the kind
// of node it is, and the identities of the names directly beneath it.
//
// The kind is part of it because a name whose node has been replaced by one of another
// kind refers to a different node. Handing that node the number the old one had would
// leave the kernel holding both — the old one is live for as long as anything has it
// open — under a single number.
type identity struct {
	all  *identities
	ino  uint64
	kind uint32

	// children is allocated for a directory and left nil for everything else. Only a
	// directory is ever asked for one: a name this mount reported as a file is a name the
	// kernel does not look inside or rename into.
	children map[string]*identity
}

// rootIdentity returns the identity of one mount's root, from which every other identity
// in that mount descends.
func rootIdentity() *identity {
	all := &identities{handed: 1}
	return &identity{all: all, ino: all.handed, kind: syscall.S_IFDIR, children: map[string]*identity{}}
}

// child returns the identity of the name directly beneath this one, giving it a number if
// this mount has not named it before.
//
// Two lookups of one name must agree on the answer, including two that run at the same
// time: a name that resolved to two numbers would be two nodes. That is what this record
// is for. The FUSE library reconciles two inodes made for one number into one inode;
// nothing reconciles two numbers.
func (i *identity) child(name string, mode uint32) *identity {
	kind := mode & syscall.S_IFMT

	i.all.mu.Lock()
	defer i.all.mu.Unlock()

	if known := i.children[name]; known != nil && known.kind == kind {
		return known
	}
	fresh := i.all.mint(kind)
	i.children[name] = fresh
	return fresh
}

// forget drops a name and everything beneath it, because the node it referred to is gone.
// A node created at that name afterwards is given an identity of its own.
func (i *identity) forget(name string) {
	i.all.mu.Lock()
	defer i.all.mu.Unlock()

	delete(i.children, name)
}

// given reports the last number this mount has handed out, so that a caller can tell the
// names that were already here from the ones that appeared while it was working.
func (i *identity) given() uint64 {
	i.all.mu.Lock()
	defer i.all.mu.Unlock()

	return i.all.handed
}

// keepOnly drops every name beneath this one that is not among present. It is how a
// directory listing prunes: the listing is the whole truth about one directory, and it is
// the only place a name removed by somebody this mount never heard from is noticed without
// being asked for.
//
// A name whose number is above before was given its identity after the caller read that
// number, which is to say alongside the listing rather than before it. The listing was
// taken when that name did not exist yet, so it is not the listing's to call gone —
// dropping it would leave the node that has it a second identity on the next lookup.
func (i *identity) keepOnly(present map[string]struct{}, before uint64) {
	i.all.mu.Lock()
	defer i.all.mu.Unlock()

	for name, known := range i.children {
		if _, listed := present[name]; !listed && known.ino <= before {
			delete(i.children, name)
		}
	}
}

// move carries an identity from one name to another, which is how a node's identity
// survives a rename: the FUSE library moves that node's inode to the new name, and this
// moves the number the inode holds along with it. A directory arrives with the identities
// beneath it still hanging off it.
//
// The name the node left refers to nothing afterwards, so that the next node created there
// is given an identity of its own; whatever the destination referred to is gone, replaced
// by what arrived.
func (i *identity) move(name string, to *identity, newName string) {
	i.all.mu.Lock()
	defer i.all.mu.Unlock()

	moved, named := i.children[name]
	delete(i.children, name)
	delete(to.children, newName)
	// A name nothing has resolved through this mount carries no identity to the
	// destination, and the destination's is gone just the same.
	if named {
		to.children[newName] = moved
	}
}

// mint hands out the next number. The caller holds the lock.
func (all *identities) mint(kind uint32) *identity {
	all.handed++
	fresh := &identity{all: all, ino: all.handed, kind: kind}
	if kind == syscall.S_IFDIR {
		fresh.children = map[string]*identity{}
	}
	return fresh
}
