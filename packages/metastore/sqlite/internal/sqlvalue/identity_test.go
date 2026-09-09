package sqlvalue

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestRandomHexPreservesRequestedByteLength(t *testing.T) {
	for _, size := range []int{0, 1, 16, 33} {
		t.Run(fmt.Sprintf("bytes=%d", size), func(t *testing.T) {
			encoded, err := RandomHex(size)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := hex.DecodeString(encoded)
			if err != nil || len(decoded) != size || len(encoded) != 2*size {
				t.Fatalf("RandomHex(%d) = %q: %d decoded bytes, %v", size, encoded, len(decoded), err)
			}
			if hex.EncodeToString(decoded) != encoded {
				t.Fatalf("RandomHex(%d) returned noncanonical hexadecimal %q", size, encoded)
			}
		})
	}
}

func TestNewKeyIssuesDistinctSixteenByteIdentities(t *testing.T) {
	seen := make(map[string]bool)
	for range 32 {
		key, err := NewKey()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := hex.DecodeString(string(key))
		if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != string(key) {
			t.Fatalf("NewKey returned invalid identity %q: %v", key, err)
		}
		if seen[string(key)] {
			t.Fatalf("NewKey issued identity %q twice", key)
		}
		seen[string(key)] = true
	}
}
