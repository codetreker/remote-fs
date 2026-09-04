package main

import (
	"strconv"
	"strings"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage/limited"
)

// TestASizeIsReadInPowersOf1024 pins the denomination down. An allowance read in powers of
// 1000 would be up to a tenth smaller than the one that was asked for, and nothing
// downstream of the flag could tell.
func TestASizeIsReadInPowersOf1024(t *testing.T) {
	for _, c := range []struct {
		text string
		want int64
	}{
		{"0", 0},
		{"4096", 4096},
		{"4096b", 4096},
		{"4096B", 4096},
		{"5K", 5 * 1024},
		{"5k", 5 * 1024},
		{"5KiB", 5 * 1024},
		{"5kib", 5 * 1024},
		{"1M", 1 << 20},
		{"1MiB", 1 << 20},
		{"3G", 3 << 30},
		{"3gib", 3 << 30},
		{"2T", 2 << 40},
		{"1P", 1 << 50},
		// The largest a size can be. One pebibyte more overflows, and is refused below.
		{"8191P", 8191 << 50},
	} {
		t.Run(c.text, func(t *testing.T) {
			got, err := parseByteSize(c.text)
			if err != nil {
				t.Fatalf("%q was refused: %v", c.text, err)
			}
			if got != c.want {
				t.Fatalf("%q read as %d bytes, want %d", c.text, got, c.want)
			}
		})
	}
}

// TestASizeThatCannotBeReadIsRefused. Every one of these could be read as some number by
// guessing at what was meant, and a guess here becomes an allowance nobody chose. The
// complaint has to say which mistake it was, so each case names what it expects to be told.
func TestASizeThatCannotBeReadIsRefused(t *testing.T) {
	for _, c := range []struct {
		name    string
		text    string
		expects string
	}{
		{"nothing at all", "", "is not a size"},
		{"a suffix with no number", "M", "is not a size"},
		{"a negative size", "-1G", "is not a size"},
		{"a fraction", "1.5G", "is a fraction"},
		{"a fraction written with a comma", "1,5G", "is a fraction"},
		{"a decimal spelling", "5MB", "power of 1000"},
		{"a decimal spelling in lower case", "5kb", "power of 1000"},
		{"a suffix nobody defined", "5X", "not one of the suffixes"},
		{"a space before the suffix", "5 M", "not one of the suffixes"},
		{"a space after the size", "5M ", "not one of the suffixes"},
		{"hexadecimal", "0x10", "not one of the suffixes"},
		{"more than a size can hold", "8192P", "more bytes than a size can hold"},
		{"more digits than a size can hold", "99999999999999999999", "more bytes than a size can hold"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseByteSize(c.text)
			if err == nil {
				t.Fatalf("%q was read as %d bytes; it should have been refused", c.text, got)
			}
			if !strings.Contains(err.Error(), c.expects) {
				t.Fatalf("the complaint about %q does not mention %q: %v", c.text, c.expects, err)
			}
			// The size that was refused has to appear in the complaint, or the operator is
			// left looking for which of their arguments it was about.
			if c.text != "" && !strings.Contains(err.Error(), c.text) {
				t.Fatalf("the complaint about %q does not quote it: %v", c.text, err)
			}
			t.Log(err)
		})
	}
}

// TestTheAllowanceFlagRefusesWhatTheStorageWouldRefuse. limited.New refuses a limit below
// MinLimit, and refusing it here as well is what puts the complaint at the flag that
// carried the mistake rather than out of a storage three frames further on.
func TestTheAllowanceFlagRefusesWhatTheStorageWouldRefuse(t *testing.T) {
	var below sizeFlag
	err := below.Set(strconv.FormatInt(limited.MinLimit-1, 10))
	if err == nil {
		t.Fatalf("%d bytes was accepted, and limited.New would refuse it", limited.MinLimit-1)
	}
	if !strings.Contains(err.Error(), strconv.FormatInt(limited.MinLimit, 10)) {
		t.Fatalf("the complaint does not say what the smallest allowance is: %v", err)
	}
	if below.bytes != 0 {
		t.Fatalf("a refused size left %d bytes behind on the flag", below.bytes)
	}

	var accepted sizeFlag
	if err := accepted.Set("8M"); err != nil {
		t.Fatalf("8M was refused: %v", err)
	}
	if accepted.bytes != 8<<20 {
		t.Fatalf("8M reached the flag as %d bytes, want %d", accepted.bytes, 8<<20)
	}
	if accepted.String() != "8388608" {
		t.Fatalf("the flag reports itself as %q, want the byte count it holds", accepted.String())
	}

	// The zero value reports nothing, which is how the flag package tells that this flag has
	// no default to print — and there is none: without it the namespace is under no
	// allowance at all.
	var unset sizeFlag
	if unset.String() != "" {
		t.Fatalf("a flag that was never given reports %q", unset.String())
	}
}

func TestAByteBoundMustBePositive(t *testing.T) {
	bound := positiveSizeFlag{bytes: 8 << 20}
	if err := bound.Set("0"); err == nil {
		t.Fatal("a zero byte bound was accepted")
	}
	if bound.bytes != 8<<20 {
		t.Fatalf("a refused byte bound replaced the previous value with %d", bound.bytes)
	}
	if err := bound.Set("2G"); err != nil {
		t.Fatalf("2G was refused: %v", err)
	}
	if bound.bytes != 2<<30 {
		t.Fatalf("2G reached the flag as %d bytes", bound.bytes)
	}
}

func TestAnInheritedByteBoundHasNoIndependentDefault(t *testing.T) {
	var inherited inheritedSizeFlag
	if inherited.String() != "" {
		t.Fatalf("an inherited bound reports an independent default of %q", inherited.String())
	}
	if err := inherited.Set("4M"); err != nil {
		t.Fatal(err)
	}
	if inherited.bytes != 4<<20 || inherited.String() != "4194304" {
		t.Fatalf("the explicit inherited bound is %d and reports %q", inherited.bytes, inherited.String())
	}
}
