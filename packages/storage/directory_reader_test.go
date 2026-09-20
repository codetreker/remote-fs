package storage

import "context"

type namespaceAccessOnly struct{}

func (namespaceAccessOnly) CheckNamespaceAccess() error { return nil }

func (namespaceAccessOnly) LookupAt(context.Context, ChildName) (Attr, error) {
	return Attr{}, nil
}

func (namespaceAccessOnly) MutateName(context.Context, NameCommand) (NameResult, error) {
	return NameResult{}, nil
}

type directoryReaderOnly struct{}

func (directoryReaderOnly) CheckDirectoryRead() error { return nil }

func (directoryReaderOnly) ReadDirNode(context.Context, DirectoryTarget) (ObservedDirectory, error) {
	return ObservedDirectory{}, nil
}

func (directoryReaderOnly) ReadDirNodeBounded(context.Context, DirectoryTarget, *ListResult) (DirectoryObservation, error) {
	return DirectoryObservation{}, nil
}

var _ NamespaceAccess = namespaceAccessOnly{}
var _ DirectoryReader = directoryReaderOnly{}
