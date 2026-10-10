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

func TestReadWriteCreditsAndResponseBoundUsePayloadLength(t *testing.T) {
	for _, length := range []uint32{0, 65536, 65537, 1 << 20} {
		want := max(1, int((uint64(length)+65535)/65536))
		read := readRequestForTest(wire.FileID{1}, 0, length, 0)
		if got := requiredCredits(read); got != want {
			t.Fatalf("READ length %d credits %d want %d", length, got, want)
		}
		if got := responseBudget(read); got < (80+int(length)+7)&^7 {
			t.Fatalf("READ length %d budget %d", length, got)
		}
		write := writeRequestForTest(wire.FileID{1}, 0, make([]byte, length))
		if got := requiredCredits(write); got != want {
			t.Fatalf("WRITE length %d credits %d want %d", length, got, want)
		}
		if got := responseBudget(write); got != 80 {
			t.Fatalf("WRITE budget %d", got)
		}
	}
	if got := responseBudget(flushRequestForTest(wire.FileID{1})); got != 72 {
		t.Fatalf("FLUSH budget %d", got)
	}
}
