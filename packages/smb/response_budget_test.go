package smb

import (
	"testing"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
)

func TestFileResponseBudgetsCoverCompoundPadding(t *testing.T) {
	for _, test := range []struct {
		command uint16
		body    []byte
	}{
		{wire.Create, wire.CreateResponseBody(wire.CreateResponse{})},
		{wire.Close, wire.CloseResponseBody(wire.CloseResponse{})},
	} {
		request := wire.Request{Header: wire.Header{Command: test.command}}
		size := wire.HeaderSize + len(test.body)
		padded := (size + 7) &^ 7
		if got := responseBudget(request); got < padded {
			t.Fatalf("command %d budget %d below padded response %d", test.command, got, padded)
		}
	}
}
