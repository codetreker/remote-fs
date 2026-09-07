package main

import (
	"flag"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var clientByteUnits = map[string]int64{
	"": 1, "b": 1,
	"k": 1 << 10, "kib": 1 << 10,
	"m": 1 << 20, "mib": 1 << 20,
	"g": 1 << 30, "gib": 1 << 30,
	"t": 1 << 40, "tib": 1 << 40,
	"p": 1 << 50, "pib": 1 << 50,
}

var clientDecimalUnits = map[string]string{"kb": "K", "mb": "M", "gb": "G", "tb": "T", "pb": "P"}

const clientSizeSuffixes = "B, K, M, G, T and P, or the IEC spellings KiB through PiB, all powers of 1024"

func parseClientByteSize(text string) (int64, error) {
	digits := 0
	for digits < len(text) && '0' <= text[digits] && text[digits] <= '9' {
		digits++
	}
	if digits == 0 {
		return 0, fmt.Errorf("%q is not a whole-number byte size with one of the suffixes %s", text, clientSizeSuffixes)
	}
	suffix := strings.ToLower(text[digits:])
	unit, known := clientByteUnits[suffix]
	if !known {
		if letter, decimal := clientDecimalUnits[suffix]; decimal {
			return 0, fmt.Errorf("%q uses a decimal suffix; write %s%s for powers of 1024", text, text[:digits], letter)
		}
		return 0, fmt.Errorf("%q ends in %q, not one of %s", text, text[digits:], clientSizeSuffixes)
	}
	count, err := strconv.ParseInt(text[:digits], 10, 64)
	if err != nil || count > math.MaxInt64/unit {
		return 0, fmt.Errorf("%q exceeds the largest byte size %d", text, int64(math.MaxInt64))
	}
	return count * unit, nil
}

type clientPositiveSizeFlag struct{ bytes int64 }

var _ flag.Value = (*clientPositiveSizeFlag)(nil)

func (f *clientPositiveSizeFlag) String() string { return strconv.FormatInt(f.bytes, 10) }

func (f *clientPositiveSizeFlag) Set(text string) error {
	size, err := parseClientByteSize(text)
	if err != nil {
		return err
	}
	if size <= 0 {
		return fmt.Errorf("%q is %d bytes, and a byte bound must be positive", text, size)
	}
	f.bytes = size
	return nil
}
