//go:build windows

package smb

import (
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestSMBNativeOrdinalComparison(t *testing.T) {
	if err := nameComparisonAvailable(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		left, right string
		equal       bool
	}{{"file", "FILE", true}, {"é", "É", true}, {"σ", "Σ", true}, {"é", "é", false}, {"straße", "STRASSE", false}} {
		order, err := nativeNameCompare(nameUnits(test.left), nameUnits(test.right))
		if err != nil || (order == 0) != test.equal {
			t.Fatalf("ordinal %q/%q: %d %v", test.left, test.right, order, err)
		}
	}
	if _, err := nativeNameCompare(nil, []uint16{'a'}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("invalid comparison: %v", err)
	}
	entries := []storage.Entry{{Name: "é", Attr: storage.Attr{ID: 2, Kind: storage.NodeRegular}}, {Name: "É", Attr: storage.Attr{ID: 3, Kind: storage.NodeRegular}}}
	if _, err := projectDirectory(entries, nativeNameCompare); !errors.Is(err, errNameAmbiguous) {
		t.Fatalf("native Unicode collision accepted: %v", err)
	}
	backend, session := namespaceFixture()
	result, err := resolveName(t.Context(), backend, session, `folder\ACTUAL`, DefaultLimits(), allowNamespace)
	if err != nil || string(result.Target.RawLeaf) != "Actual" {
		t.Fatalf("native resolution: %+v %v", result, err)
	}
}
