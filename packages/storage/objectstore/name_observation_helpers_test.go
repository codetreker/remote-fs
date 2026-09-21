package objectstore_test

import (
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func objectCapability[T any](t *testing.T, value any) T {
	t.Helper()
	capability, ok := value.(T)
	if !ok {
		t.Fatalf("%T does not provide %T", value, (*T)(nil))
	}
	return capability
}

func objectChild(parent uint64, name string) storage.ChildName {
	return storage.ChildName{Parent: storage.DirectoryTarget{NodeID: parent}, RawLeaf: []byte(name)}
}

func objectAction(t *testing.T) storage.FileActionID {
	t.Helper()
	action, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	return action
}

func objectScope(t *testing.T, reference any) storage.UseScope {
	t.Helper()
	capability := objectCapability[storage.ScopedReference](t, reference)
	if err := capability.CheckScopedReference(); err != nil {
		t.Fatal(err)
	}
	scope, err := capability.Scope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return scope
}
