package storage

import "bytes"

// Clone owns the raw child name and every namespace guard token.
func (s ChildSelection) Clone() ChildSelection {
	s.Name.RawLeaf = bytes.Clone(s.Name.RawLeaf)
	if s.Name.Parent.Scope != nil {
		scope := *s.Name.Parent.Scope
		s.Name.Parent.Scope = &scope
	}
	s.Guards = s.Guards.Clone()
	return s
}

// Clone owns the opaque revision token.
func (d DirectoryObservation) Clone() DirectoryObservation {
	d.Revision = bytes.Clone(d.Revision)
	return d
}

// Clone owns the raw platform-neutral leaf.
func (e ObservedEdge) Clone() ObservedEdge {
	e.RawLeaf = bytes.Clone(e.RawLeaf)
	return e
}

// Clone owns every mutable guard token and raw leaf. A nil guard set remains
// nil so optional request fields preserve their absence.
func (g *NamespaceGuards) Clone() *NamespaceGuards {
	if g == nil {
		return nil
	}
	clone := &NamespaceGuards{RootID: g.RootID}
	if g.Directories != nil {
		clone.Directories = make([]DirectoryObservation, len(g.Directories))
		for i, directory := range g.Directories {
			clone.Directories[i] = directory.Clone()
		}
	}
	if g.Edges != nil {
		clone.Edges = make([]ObservedEdge, len(g.Edges))
		for i, edge := range g.Edges {
			clone.Edges[i] = edge.Clone()
		}
	}
	return clone
}

// Clone owns the raw name and attributes retained for one observed entry.
func (e ObservedEntry) Clone() ObservedEntry {
	e.RawLeaf = bytes.Clone(e.RawLeaf)
	e.Attr = e.Attr.Clone()
	return e
}

// Clone owns the complete observed directory result.
func (d ObservedDirectory) Clone() ObservedDirectory {
	d.Observation = d.Observation.Clone()
	if d.Entries != nil {
		entries := make([]ObservedEntry, len(d.Entries))
		for i, entry := range d.Entries {
			entries[i] = entry.Clone()
		}
		d.Entries = entries
	}
	return d
}
