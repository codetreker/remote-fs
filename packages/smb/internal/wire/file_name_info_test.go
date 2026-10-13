package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"testing"
)

func renameInformationForTest(name string, replace byte) []byte {
	encoded := EncodeUTF16(name)
	body := make([]byte, 20+len(encoded))
	body[0] = replace
	le.PutUint32(body[16:20], uint32(len(encoded)))
	copy(body[20:], encoded)
	return body
}

func TestRenameInformationCountsNameAndIgnoresReservedPadding(t *testing.T) {
	for _, replace := range []byte{0, 1, 255} {
		body := renameInformationForTest("dir\\A😀", replace)
		copy(body[1:8], []byte{1, 2, 3, 4, 5, 6, 7})
		body = append(body, 0xff, 0xee, 0xdd)
		original := bytes.Clone(body)
		got, err := ParseFileRenameInformation(body)
		if err != nil || got.Name != "dir\\A😀" || got.ReplaceIfExists != (replace != 0) {
			t.Fatalf("replace %d = %+v, %v", replace, got, err)
		}
		if !bytes.Equal(body, original) {
			t.Fatal("rename decoder changed signed payload")
		}
		body[20] = 'x'
		if got.Name != "dir\\A😀" {
			t.Fatal("rename name borrows frame bytes")
		}
	}
}

func TestRenameInformationRejectsMalformedRecords(t *testing.T) {
	for _, test := range []struct {
		name string
		data []byte
	}{
		{"short", make([]byte, 19)},
		{"empty", renameInformationForTest("", 0)},
		{"absolute", renameInformationForTest("\\file", 0)},
		{"nul", append(renameInformationForTest("a", 0)[:20], 0, 0)},
		{"surrogate", append(renameInformationForTest("a", 0)[:20], 0, 0xd8)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got, err := ParseFileRenameInformation(test.data); got != (FileRenameInfo{}) || !errors.Is(err, ErrMalformed) {
				t.Fatalf("malformed record = %+v, %v", got, err)
			}
		})
	}
	for _, mutate := range []func([]byte){
		func(data []byte) { le.PutUint64(data[8:16], 1) },
		func(data []byte) { le.PutUint32(data[16:20], 1) },
		func(data []byte) { le.PutUint32(data[16:20], 4) },
		func(data []byte) { le.PutUint32(data[16:20], math.MaxUint32) },
	} {
		body := renameInformationForTest("a", 0)
		mutate(body)
		if _, err := ParseFileRenameInformation(body); !errors.Is(err, ErrMalformed) {
			t.Fatalf("invalid counted record accepted: %x, %v", body, err)
		}
	}
}

func TestDispositionInformationUsesFirstBoolean(t *testing.T) {
	for _, flag := range []byte{0, 1, 255} {
		for _, data := range [][]byte{{flag}, {flag, 255, 255, 255}} {
			got, err := ParseFileDispositionInformation(data)
			if err != nil || got != (flag != 0) {
				t.Fatalf("disposition %x = %v, %v", data, got, err)
			}
		}
	}
	if _, err := ParseFileDispositionInformation(nil); !errors.Is(err, ErrMalformed) {
		t.Fatalf("empty disposition accepted: %v", err)
	}
}

func TestNormalizedNameInformationPreservesFullLengthAndEvenPrefix(t *testing.T) {
	for _, test := range []struct {
		name     string
		capacity uint32
		want     string
		overflow bool
	}{
		{"\\", 8, "020000005c00", false},
		{"\\A😀", 8, "080000005c004100", true},
		{"\\A😀", 9, "080000005c004100", true},
		{"\\A😀", 10, "080000005c0041003dd8", true},
		{"\\A😀", 11, "080000005c0041003dd8", true},
		{"\\A😀", 12, "080000005c0041003dd800de", false},
		{"\\A😀", math.MaxUint32, "080000005c0041003dd800de", false},
	} {
		body, overflow, err := FileNormalizedNameInformationBody(test.name, test.capacity)
		if err != nil || overflow != test.overflow || hex.EncodeToString(body) != test.want {
			t.Fatalf("name %q capacity %d = %x, %v, %v", test.name, test.capacity, body, overflow, err)
		}
		response, err := QueryInfoResponseBody(body)
		if err != nil || le.Uint32(response[4:8]) != uint32(len(body)) || !bytes.Equal(response[8:], body) {
			t.Fatalf("query envelope = %x, %v", response, err)
		}
	}
	for capacity := uint32(0); capacity < 8; capacity++ {
		if body, _, err := FileNormalizedNameInformationBody("\\file", capacity); body != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("short capacity %d = %x, %v", capacity, body, err)
		}
	}
	for _, name := range []string{"", "bad\xff", "nul\x00name"} {
		if body, _, err := FileNormalizedNameInformationBody(name, 256); body != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("invalid name %q = %x, %v", name, body, err)
		}
	}
}

func TestRenameSetInfoCannotReadFollowingCompoundCommand(t *testing.T) {
	payload := renameInformationForTest("file", 255)
	le.PutUint32(payload[16:20], uint32(len(payload)-20+8))
	first := setInfoPacket(payload, 96)
	first[67] = FileRenameInformation
	first = append(first, make([]byte, (-len(first))&7)...)
	le.PutUint32(first[20:24], uint32(len(first)))
	compound := append(first, requestPacket(Echo, 4, 4)...)
	requests, err := ParseFrame(compound, testLimits)
	if err != nil || len(requests) != 2 {
		t.Fatalf("compound parse = %v", err)
	}
	parsed, err := requests[0].SetInfo()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ParseFileRenameInformation(parsed.Buffer); !errors.Is(err, ErrMalformed) {
		t.Fatalf("counted name escaped command extent: %v", err)
	}
}
