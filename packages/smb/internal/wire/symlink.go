package wire

import (
	"strings"
	"unicode/utf8"
)

const SymlinkReparseTag uint32 = 0xa000000c
const maxReparseBytes = 16 * 1024

func validRelativeTarget(target string) bool {
	if target == "" || !utf8.ValidString(target) || strings.HasPrefix(target, "\\") || strings.ContainsAny(target, "/:\x00") {
		return false
	}
	return true
}

func relativeTarget(target string) ([]byte, error) {
	if !validRelativeTarget(target) || len(target) > maxReparseBytes {
		return nil, ErrMalformed
	}
	name := EncodeUTF16(target)
	if 20+len(name) > maxReparseBytes {
		return nil, ErrMalformed
	}
	return name, nil
}

// ReparseSymlinkData encodes an FSCC symbolic link relative to its parent.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fscc/b41f1cbf-10df-4a47-98d4-1c52a833d913
func ReparseSymlinkData(target string) ([]byte, error) {
	name, err := relativeTarget(target)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 20+len(name))
	le.PutUint32(b, SymlinkReparseTag)
	le.PutUint16(b[4:6], uint16(len(b)-8))
	le.PutUint16(b[10:12], uint16(len(name)))
	// Both names designate the same UTF-16 range. FSCC gives each name an
	// independent offset and length; duplicating that range wastes the 16KiB limit.
	le.PutUint16(b[14:16], uint16(len(name)))
	le.PutUint32(b[16:20], 1)
	copy(b[20:], name)
	return b, nil
}

func ParseSymlinkReparse(input []byte) (target string, relative bool, err error) {
	if len(input) < 20 || len(input) > maxReparseBytes || le.Uint32(input) != SymlinkReparseTag || int(le.Uint16(input[4:6])) != len(input)-8 {
		return "", false, ErrMalformed
	}
	flags := le.Uint32(input[16:20])
	if flags > 1 {
		return "", false, ErrMalformed
	}
	path := input[20:]
	name := func(off, length uint16) (string, error) {
		if off&1 != 0 || length&1 != 0 || uint32(off)+uint32(length) > uint32(len(path)) {
			return "", ErrMalformed
		}
		return DecodeUTF16(path[int(off) : int(off)+int(length)])
	}
	target, err = name(le.Uint16(input[8:10]), le.Uint16(input[10:12]))
	if err != nil {
		return "", false, err
	}
	if _, err = name(le.Uint16(input[12:14]), le.Uint16(input[14:16])); err != nil {
		return "", false, err
	}
	if target == "" {
		return "", false, ErrMalformed
	}
	if flags == 1 && !validRelativeTarget(target) {
		return "", false, ErrMalformed
	}
	return target, flags == 1, nil
}

// SymlinkErrorResponseBody returns the SMB3.1.1 error-context envelope, including
// byte counts measured in UTF-16 rather than UTF-8 or Unicode code points.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/f15ae37d-a787-4bf2-9940-025a8c9c0022
func SymlinkErrorResponseBody(target, unparsed string) ([]byte, error) {
	name, err := relativeTarget(target)
	if err != nil {
		return nil, err
	}
	if !utf8.ValidString(unparsed) || strings.ContainsRune(unparsed, 0) {
		return nil, ErrMalformed
	}
	unparsedBytes := EncodeUTF16(unparsed)
	if len(unparsedBytes) > 65535 {
		return nil, ErrMalformed
	}
	symlinkLength := 28 + len(name)
	b := make([]byte, 8+8+symlinkLength)
	le.PutUint16(b, 9)
	b[2] = 1
	le.PutUint32(b[4:8], uint32(8+symlinkLength))
	le.PutUint32(b[8:12], uint32(symlinkLength))
	p := b[16:]
	le.PutUint32(p, uint32(symlinkLength-4))
	le.PutUint32(p[4:8], 0x4c4d5953)
	le.PutUint32(p[8:12], SymlinkReparseTag)
	le.PutUint16(p[12:14], uint16(12+len(name)))
	le.PutUint16(p[14:16], uint16(len(unparsedBytes)))
	le.PutUint16(p[18:20], uint16(len(name)))
	le.PutUint16(p[22:24], uint16(len(name)))
	le.PutUint32(p[24:28], 1)
	copy(p[28:], name)
	return b, nil
}
