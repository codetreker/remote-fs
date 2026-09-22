package wire

import (
	"errors"
	"testing"
)

func TestQueryDirectoryRequest(t *testing.T) {
	packet := requestPacket(QueryDirectory, 33, 34)
	packet[66] = 37
	packet[72] = 7
	le.PutUint16(packet[88:90], HeaderSize+32)
	le.PutUint16(packet[90:92], 2)
	le.PutUint32(packet[92:96], 1024)
	copy(packet[96:], EncodeUTF16("*"))
	request, err := parseOne(t, packet).QueryDirectory()
	if err != nil || request.Class != 37 || request.FileID[0] != 7 || request.Pattern != "*" || request.OutputLength != 1024 {
		t.Fatalf("QUERY_DIRECTORY = %+v, %v", request, err)
	}
	le.PutUint16(packet[90:92], 3)
	if _, err := parseOne(t, packet).QueryDirectory(); err == nil {
		t.Fatal("odd UTF-16 pattern accepted")
	}
}

func TestQueryInfoRequestInputRules(t *testing.T) {
	packet := requestPacket(QueryInfo, 41, 42)
	packet[66], packet[67] = 1, 15
	packet[88] = 42
	le.PutUint16(packet[72:74], HeaderSize+40)
	le.PutUint32(packet[76:80], 2)
	le.PutUint32(packet[80:84], 0x76543210)
	le.PutUint32(packet[84:88], 0xfedcba98)
	copy(packet[104:], "ea")
	request, err := parseOne(t, packet).QueryInfo()
	if err != nil || request.FileID[0] != 42 || request.Type != 1 || request.Class != 15 ||
		request.InputLength != 2 || string(request.Input) != "ea" || request.Additional != 0x76543210 || request.Flags != 0xfedcba98 {
		t.Fatalf("QUERY_INFO = %+v, %v", request, err)
	}
	le.PutUint16(packet[72:74], 0xffff)
	if _, err := parseOne(t, packet).QueryInfo(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("unbounded applicable input accepted: %v", err)
	}

	packet[67] = 4
	request, err = parseOne(t, packet).QueryInfo()
	if err != nil || request.InputLength != 2 || request.Input != nil {
		t.Fatalf("uninterpreted fixed query input = %+v, %v", request, err)
	}
}

func FuzzQueryRequestDecoders(f *testing.F) {
	f.Add(requestPacket(QueryDirectory, 33, 32))
	f.Add(requestPacket(QueryInfo, 41, 40))
	f.Fuzz(func(t *testing.T, packet []byte) {
		requests, err := ParseFrame(packet, testLimits)
		if err != nil {
			return
		}
		for _, request := range requests {
			switch request.Header.Command {
			case QueryDirectory:
				_, _ = request.QueryDirectory()
			case QueryInfo:
				_, _ = request.QueryInfo()
			}
		}
	})
}
