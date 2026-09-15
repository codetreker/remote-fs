package wire

import "bytes"

const DialectWildcard uint16 = 0x02ff

// ParseMultiProtocolNegotiate accepts only the SMB_COM_NEGOTIATE envelope
// advertising SMB 2.???. It admits a fresh SMB2 negotiation, not SMB1 operations.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/8f8190fb-7e22-4dc0-8ae7-de2780674721
func ParseMultiProtocolNegotiate(packet []byte) error {
	const dialectOffset = 35
	if len(packet) < dialectOffset || string(packet[:4]) != "\xffSMB" || packet[4] != 0x72 || packet[9]&0x80 != 0 || packet[32] != 0 {
		return ErrMalformed
	}
	byteCount := int(le.Uint16(packet[33:35]))
	if byteCount == 0 || byteCount != len(packet)-dialectOffset {
		return ErrMalformed
	}
	dialects := packet[dialectOffset:]
	found := false
	for len(dialects) != 0 {
		if dialects[0] != 2 {
			return ErrMalformed
		}
		end := bytes.IndexByte(dialects[1:], 0)
		if end <= 0 {
			return ErrMalformed
		}
		found = found || bytes.Equal(dialects[1:1+end], []byte("SMB 2.???"))
		dialects = dialects[2+end:]
	}
	if !found {
		return ErrMalformed
	}
	return nil
}
