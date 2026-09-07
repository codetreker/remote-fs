package main

import (
	"strings"
	"testing"
)

func TestClientByteSizeUsesTheServerSIZEGrammar(t *testing.T) {
	for text, want := range map[string]int64{
		"1":    1,
		"1B":   1,
		"2K":   2 << 10,
		"3KiB": 3 << 10,
		"4M":   4 << 20,
	} {
		got, err := parseClientByteSize(text)
		if err != nil || got != want {
			t.Errorf("parseClientByteSize(%q) = %d, %v; want %d", text, got, err, want)
		}
	}
}

func TestClientByteSizeRejectsZeroNegativeDecimalAndOverflow(t *testing.T) {
	for _, c := range []struct {
		text string
		want string
	}{
		{"0", "positive"},
		{"-1", "whole-number"},
		{"1KB", "decimal suffix"},
		{"9223372036854775808", "exceeds"},
	} {
		var value clientPositiveSizeFlag
		err := value.Set(c.text)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("client SIZE %q returned %v, want an error containing %q", c.text, err, c.want)
		}
	}
}
