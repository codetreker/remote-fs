package main

import (
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/codetreker/remote-fs/packages/storage/limited"
)

// Sizes on this command line are powers of 1024, and the flag's own help says so.
//
// That is the denomination every figure a size ends up beside is counted in: the blocks a
// mount reports space in, the numbers df prints for the filesystem underneath, the reply
// statfs(2) hands back. A limit denominated the other way would be up to a tenth smaller
// than the one the operator believes they set, and the way they would find out is a
// volume that filled early.
const (
	kibi = 1 << 10
	mebi = 1 << 20
	gibi = 1 << 30
	tebi = 1 << 40
	pebi = 1 << 50
)

// byteUnits are the suffixes a size may carry, matched without regard to case. Both
// spellings of each are taken: the single letter is what an operator types, and the IEC
// form is the one that says out loud which power it means.
var byteUnits = map[string]int64{
	"": 1, "b": 1,
	"k": kibi, "kib": kibi,
	"m": mebi, "mib": mebi,
	"g": gibi, "gib": gibi,
	"t": tebi, "tib": tebi,
	"p": pebi, "pib": pebi,
}

// decimalUnits are the suffixes that name powers of 1000 elsewhere. They are listed so
// that one can be refused for what it is rather than as an unrecognised string: SI gives
// KB a thousand bytes and common usage gives it 1024, and neither reading may be picked on
// somebody else's behalf.
var decimalUnits = map[string]string{"kb": "K", "mb": "M", "gb": "G", "tb": "T", "pb": "P"}

// suffixes is how the accepted set is named to whoever got it wrong.
const suffixes = "B, K, M, G, T and P, or the IEC spellings KiB through PiB, all powers of 1024"

// parseByteSize reads a whole number of bytes, optionally carrying one of byteUnits.
//
// Anything else is refused rather than interpreted. A size read one way when it was meant
// another becomes an allowance nobody chose, and nothing downstream can tell.
func parseByteSize(text string) (int64, error) {
	digits := 0
	for digits < len(text) && '0' <= text[digits] && text[digits] <= '9' {
		digits++
	}
	if digits == 0 {
		return 0, fmt.Errorf("%q is not a size: a size is a whole number of bytes, optionally with one of the suffixes %s",
			text, suffixes)
	}

	suffix := strings.ToLower(text[digits:])
	unit, known := byteUnits[suffix]
	if !known {
		if letter, decimal := decimalUnits[suffix]; decimal {
			return 0, fmt.Errorf("%q is spelled as a power of 1000, and sizes here are powers of 1024: write %s%s for that, or write the decimal figure out in bytes",
				text, text[:digits], letter)
		}
		// A fraction is worth its own refusal: the correction is to say the same size in a
		// smaller unit, and a list of suffixes does not suggest that.
		if strings.HasPrefix(suffix, ".") || strings.HasPrefix(suffix, ",") {
			return 0, fmt.Errorf("%q is a fraction, and a size is a whole number of bytes: say it in a smaller unit instead", text)
		}
		return 0, fmt.Errorf("%q ends in %q, which is not one of the suffixes %s", text, text[digits:], suffixes)
	}

	count, err := strconv.ParseInt(text[:digits], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is more bytes than a size can hold, the largest being %d: %w", text, int64(math.MaxInt64), err)
	}
	if count > math.MaxInt64/unit {
		return 0, fmt.Errorf("%q is more bytes than a size can hold, the largest being %d", text, int64(math.MaxInt64))
	}
	return count * unit, nil
}

// sizeFlag is an allowance given on the command line. Its zero value is the flag not
// having been given at all, which Set cannot produce: the smallest size it accepts is
// limited.MinLimit.
type sizeFlag struct{ bytes int64 }

var _ flag.Value = (*sizeFlag)(nil)

func (f *sizeFlag) String() string {
	if f.bytes == 0 {
		return ""
	}
	return strconv.FormatInt(f.bytes, 10)
}

// Set refuses here what limited.New would refuse, so that the complaint arrives at the
// flag that carried the mistake rather than out of a storage three frames further on.
func (f *sizeFlag) Set(text string) error {
	size, err := parseByteSize(text)
	if err != nil {
		return err
	}
	if size < limited.MinLimit {
		return fmt.Errorf("%q is %d bytes, and %d is the smallest allowance a volume can be held under",
			text, size, limited.MinLimit)
	}
	f.bytes = size
	return nil
}

// positiveSizeFlag is a byte bound on a retained or admitted resource. Unlike an
// allowance, every one of these bounds must leave room for at least one byte.
type positiveSizeFlag struct{ bytes int64 }

var _ flag.Value = (*positiveSizeFlag)(nil)

func (f *positiveSizeFlag) String() string { return strconv.FormatInt(f.bytes, 10) }

func (f *positiveSizeFlag) Set(text string) error {
	size, err := parseByteSize(text)
	if err != nil {
		return err
	}
	if size <= 0 {
		return fmt.Errorf("%q is %d bytes, and a byte bound must be positive", text, size)
	}
	f.bytes = size
	return nil
}

// inheritedSizeFlag is a positive byte bound whose zero value delegates to another bound.
// String reports no numeric default because the effective value belongs to that other flag.
type inheritedSizeFlag struct{ positiveSizeFlag }

func (f *inheritedSizeFlag) String() string {
	if f.bytes == 0 {
		return ""
	}
	return f.positiveSizeFlag.String()
}
