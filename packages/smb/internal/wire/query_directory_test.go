package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func queryDirectoryPacket(pattern []byte) []byte {
	packet := requestPacket(QueryDirectory, 33, 32+len(pattern))
	packet[66], packet[67] = FileNamesInformation, QueryDirectoryRestartScans|QueryDirectoryIndexSpecified
	le.PutUint32(packet[68:72], 0xfedcba98)
	copy(packet[72:88], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	if len(pattern) != 0 {
		le.PutUint16(packet[88:90], 96)
		le.PutUint16(packet[90:92], uint16(len(pattern)))
		copy(packet[96:], pattern)
	}
	le.PutUint32(packet[92:96], 4096)
	return packet
}

func TestQueryDirectoryRequest(t *testing.T) {
	for _, pattern := range []string{"", "*.TXT", "é😀?<>\""} {
		packet := queryDirectoryPacket(EncodeUTF16(pattern))
		before := bytes.Clone(packet)
		got, err := parseOne(t, packet).QueryDirectory()
		if err != nil || got.FileInformationClass != FileNamesInformation || got.Flags != QueryDirectoryRestartScans|QueryDirectoryIndexSpecified ||
			got.FileIndex != 0xfedcba98 || got.Pattern != pattern || got.OutputBufferLength != 4096 || got.FileID != (FileID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}) {
			t.Fatalf("pattern %q = %+v, %v", pattern, got, err)
		}
		if !bytes.Equal(packet, before) {
			t.Fatal("decoder modified the signed command")
		}
		packet[72] = 99
		if got.FileID[0] != 1 {
			t.Fatal("decoded FileId borrows signed bytes")
		}
	}
	for _, id := range []FileID{{}, InvalidFileID} {
		packet := queryDirectoryPacket(nil)
		copy(packet[72:88], id[:])
		le.PutUint32(packet[92:96], 0)
		if got, err := parseOne(t, packet).QueryDirectory(); err != nil || got.FileID != id || got.OutputBufferLength != 0 {
			t.Fatalf("placeholder or short output rejected before command validation: %+v, %v", got, err)
		}
	}
}

func TestQueryDirectoryRejectsMalformedRequest(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]byte)
	}{
		{"structure", func(packet []byte) { le.PutUint16(packet[64:66], 32) }},
		{"flags", func(packet []byte) { packet[67] = 0x08 }},
		{"before fixed fields", func(packet []byte) { le.PutUint16(packet[88:90], 94) }},
		{"odd offset", func(packet []byte) { le.PutUint16(packet[88:90], 97) }},
		{"odd length", func(packet []byte) { le.PutUint16(packet[90:92], 3) }},
		{"outside command", func(packet []byte) { le.PutUint16(packet[90:92], 6) }},
		{"empty nonzero offset", func(packet []byte) { le.PutUint16(packet[90:92], 0) }},
		{"nul", func(packet []byte) { le.PutUint16(packet[96:98], 0) }},
		{"unpaired high surrogate", func(packet []byte) { le.PutUint16(packet[96:98], 0xd83d) }},
		{"unpaired low surrogate", func(packet []byte) { le.PutUint16(packet[96:98], 0xde00) }},
		{"partial related id", func(packet []byte) { copy(packet[72:80], InvalidFileID[:8]) }},
		{"output extent", func(packet []byte) { le.PutUint32(packet[92:96], ^uint32(0)) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			packet := queryDirectoryPacket(EncodeUTF16("AB"))
			test.mutate(packet)
			if got, err := parseOne(t, packet).QueryDirectory(); got != (QueryDirectoryRequest{}) || !errors.Is(err, ErrMalformed) {
				t.Fatalf("malformed request = %+v, %v", got, err)
			}
		})
	}
	packet := queryDirectoryPacket(nil)
	request := parseOne(t, packet)
	request.Body = request.Body[:31]
	if _, err := request.QueryDirectory(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("short fixed body = %v", err)
	}
	request = parseOne(t, packet)
	request.Header.Command = QueryInfo
	if _, err := request.QueryDirectory(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("wrong command = %v", err)
	}
	packet = queryDirectoryPacket(EncodeUTF16("AB"))
	packet = append(packet, make([]byte, 4)...)
	le.PutUint32(packet[20:24], uint32(len(packet)))
	packet = append(packet, requestPacket(Echo, 4, 4)...)
	le.PutUint16(packet[90:92], 10)
	commands, err := ParseFrame(packet, testLimits)
	if err != nil || len(commands) != 2 {
		t.Fatalf("compound setup = %v", err)
	}
	if _, err := commands[0].QueryDirectory(); !errors.Is(err, ErrMalformed) {
		t.Fatalf("pattern borrowed subsequent command = %v", err)
	}
}

func directoryGoldenEntry() DirectoryEntry {
	return DirectoryEntry{Name: []uint16{0x41, 0xd83d, 0xde00}, CreationTime: 1, LastAccessTime: 2,
		LastWriteTime: 3, ChangeTime: 4, EndOfFile: 5, AllocationSize: 4096, Attributes: 0x400, EaSize: 0xa000000c, FileID: 0x1122334455667788}
}

func TestDirectoryPageGoldenBodies(t *testing.T) {
	base := "0000000000000000" + "0100000000000000" + "0200000000000000" +
		"0300000000000000" + "0400000000000000" + "0500000000000000" +
		"0010000000000000" + "00040000" + "06000000"
	name, tag, id := "41003dd800de", "0c0000a0", "8877665544332211"
	tests := []struct {
		class byte
		want  string
	}{
		{FileNamesInformation, "000000000000000006000000" + name},
		{FileDirectoryInformation, base + name},
		{FileFullDirectoryInformation, base + tag + name},
		{FileBothDirectoryInformation, base + tag + strings.Repeat("00", 26) + name},
		{FileIdFullDirectoryInformation, base + tag + "00000000" + id + name},
		{FileIdBothDirectoryInformation, base + tag + strings.Repeat("00", 28) + id + name},
	}
	for _, test := range tests {
		page, err := EncodeDirectoryPage(test.class, []DirectoryEntry{directoryGoldenEntry()}, 4096, false)
		if err != nil || page.Completed != 1 || page.Partial || hex.EncodeToString(page.Data) != test.want {
			t.Fatalf("class %#x = %+v, %v; want %s", test.class, page, err, test.want)
		}
	}
}

func TestDirectoryPagePartialFirstNameAndExactRetry(t *testing.T) {
	name := []byte{0x41, 0, 0x3d, 0xd8, 0, 0xde}
	for _, class := range []byte{FileNamesInformation, FileDirectoryInformation, FileFullDirectoryInformation, FileBothDirectoryInformation, FileIdFullDirectoryInformation, FileIdBothDirectoryInformation} {
		head, _ := DirectoryInformationHeadSize(class)
		if page, err := EncodeDirectoryPage(class, []DirectoryEntry{directoryGoldenEntry()}, uint32(head-1), false); page.Data != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("class %#x short fixed head = %+v, %v", class, page, err)
		}
		for copied := 0; copied <= len(name); copied++ {
			page, err := EncodeDirectoryPage(class, []DirectoryEntry{directoryGoldenEntry()}, uint32(head+copied), false)
			completed := 0
			if copied == len(name) {
				completed = 1
			}
			lengthOffset := 60
			if class == FileNamesInformation {
				lengthOffset = 8
			}
			if err != nil || len(page.Data) != head+copied || page.Partial != (copied < len(name)) ||
				page.Completed != completed || le.Uint32(page.Data[:4]) != 0 ||
				le.Uint32(page.Data[lengthOffset:lengthOffset+4]) != uint32(len(name)) || !bytes.Equal(page.Data[head:], name[:copied]) {
				t.Fatalf("class %#x copied %d = %+v, %v", class, copied, page, err)
			}
		}
	}
}

func TestDirectoryPageAlignedRecordsNoFinalPadding(t *testing.T) {
	first, second := directoryGoldenEntry(), directoryGoldenEntry()
	first.Name, second.Name = []uint16{'A'}, []uint16{'B', 'C'}
	for _, class := range []byte{FileNamesInformation, FileDirectoryInformation, FileFullDirectoryInformation, FileBothDirectoryInformation, FileIdFullDirectoryInformation, FileIdBothDirectoryInformation} {
		head, _ := DirectoryInformationHeadSize(class)
		aligned := (head + 2 + 7) &^ 7
		length := aligned + head + 4
		page, err := EncodeDirectoryPage(class, []DirectoryEntry{first, second}, uint32(length), false)
		if err != nil || page.Completed != 2 || page.Partial || len(page.Data) != length ||
			le.Uint32(page.Data[:4]) != uint32(aligned) || le.Uint32(page.Data[aligned:aligned+4]) != 0 ||
			!bytes.Equal(page.Data[head+2:aligned], make([]byte, aligned-head-2)) {
			t.Fatalf("class %#x aligned page = %+v, %v", class, page, err)
		}
		prefix, err := EncodeDirectoryPage(class, []DirectoryEntry{first, second}, uint32(length-1), false)
		if err != nil || prefix.Completed != 1 || prefix.Partial || len(prefix.Data) != head+2 || le.Uint32(prefix.Data[:4]) != 0 {
			t.Fatalf("class %#x complete prefix = %+v, %v", class, prefix, err)
		}
		single, err := EncodeDirectoryPage(class, []DirectoryEntry{first, second}, uint32(length), true)
		if err != nil || single.Completed != 1 || single.Partial || !bytes.Equal(single.Data, prefix.Data) {
			t.Fatalf("class %#x single = %+v, %v", class, single, err)
		}
	}
}

func TestDirectoryPageClassAndSourceValidation(t *testing.T) {
	for _, class := range []byte{0x3c, 0x4e, 0x4f, 0x50, 0x51} {
		if !IsDirectoryInformationClass(class) {
			t.Fatalf("documented class %#x classified as unknown", class)
		}
		if _, supported := DirectoryInformationHeadSize(class); supported {
			t.Fatalf("unsupported class %#x classified as implemented", class)
		}
	}
	for _, class := range []byte{0, 4, 0x24, 0x27, 0x64, 0xff} {
		if IsDirectoryInformationClass(class) {
			t.Fatalf("class %#x classified as directory class", class)
		}
	}
	if page, err := EncodeDirectoryPage(FileNamesInformation, nil, 12, false); err != nil || page.Data != nil || page.Completed != 0 || page.Partial {
		t.Fatalf("empty page = %+v, %v", page, err)
	}
	for _, class := range []byte{0, 0x3c} {
		if page, err := EncodeDirectoryPage(class, []DirectoryEntry{directoryGoldenEntry()}, 4096, false); page.Data != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("unimplemented class = %+v, %v", page, err)
		}
	}
	if page, err := EncodeDirectoryPage(FileNamesInformation, []DirectoryEntry{directoryGoldenEntry()}, ^uint32(0), false); page.Data != nil || !errors.Is(err, ErrMalformed) {
		t.Fatalf("unbounded output = %+v, %v", page, err)
	}
	for _, name := range [][]uint16{nil, {0}, {0xd83d}, {0xde00}, {0xd83d, 'A'}} {
		entry := directoryGoldenEntry()
		entry.Name = name
		if page, err := EncodeDirectoryPage(FileNamesInformation, []DirectoryEntry{entry}, 4096, false); page.Data != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("invalid name %x = %+v, %v", name, page, err)
		}
	}
	entry := directoryGoldenEntry()
	entry.CreationTime, entry.LastAccessTime, entry.LastWriteTime, entry.ChangeTime = -1, -1, -1, -1
	entry.EndOfFile, entry.AllocationSize, entry.FileID = -1, -1, 0
	if page, err := EncodeDirectoryPage(FileNamesInformation, []DirectoryEntry{entry}, 4096, false); err != nil || page.Completed != 1 {
		t.Fatalf("names inherited rich requirements = %+v, %v", page, err)
	}
	if page, err := EncodeDirectoryPage(FileDirectoryInformation, []DirectoryEntry{entry}, 4096, false); page.Data != nil || !errors.Is(err, ErrMalformed) {
		t.Fatalf("invalid rich fields = %+v, %v", page, err)
	}
	entry = directoryGoldenEntry()
	entry.FileID = 0
	for _, class := range []byte{FileIdFullDirectoryInformation, FileIdBothDirectoryInformation} {
		if page, err := EncodeDirectoryPage(class, []DirectoryEntry{entry}, 4096, false); page.Data != nil || !errors.Is(err, ErrMalformed) {
			t.Fatalf("zero stable ID class %#x = %+v, %v", class, page, err)
		}
	}
	entry = directoryGoldenEntry()
	before := append([]uint16(nil), entry.Name...)
	page, err := EncodeDirectoryPage(FileNamesInformation, []DirectoryEntry{entry}, 4096, false)
	if err != nil || len(entry.Name) != len(before) {
		t.Fatalf("owned source setup = %v", err)
	}
	for i := range before {
		if entry.Name[i] != before[i] {
			t.Fatal("codec changed captured name units")
		}
	}
	entry.Name[0] = 'Z'
	if !bytes.Equal(page.Data[12:], []byte{0x41, 0, 0x3d, 0xd8, 0, 0xde}) {
		t.Fatal("response borrows mutable captured name")
	}
}
