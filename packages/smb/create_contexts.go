package smb

import (
	"context"

	"github.com/codetreker/remote-fs/packages/smb/internal/wire"
	"github.com/codetreker/remote-fs/packages/storage"
)

type maximalAccessKey struct{}

func (c *connection) maximalAccess(ctx context.Context, t *tree, attr storage.WindowsAttr) (uint32, error) {
	intent := storage.WindowsOpenIntent{Disposition: storage.WindowsOpen, Share: storage.WindowsShareAll}
	if attr.IsDir() {
		intent.Kind = storage.WindowsDirectory
	}
	access, err := c.authorizeMaximumAccess(ctx, t, intent)
	return encodeAccess(access), err
}

func createContexts(ctx context.Context, request wire.CreateRequest, attr storage.WindowsAttr, volumeSerial uint64, lease *wire.LeaseResponse) ([]byte, error) {
	var encoded []byte
	previous := -1
	for _, item := range request.Contexts {
		var data []byte
		switch string(item.Name) {
		case "RqLs":
			if lease == nil {
				continue
			}
			var err error
			data, err = wire.LeaseResponseData(*lease)
			if err != nil {
				return nil, err
			}
		case "MxAc":
			data = make([]byte, 8)
			if len(item.Data) == 8 && smbLE.Uint64(item.Data) == windowsTime(attr.ChangeTime) {
				smbLE.PutUint32(data, 0xc0000073)
			} else {
				maximal, ok := ctx.Value(maximalAccessKey{}).(func(storage.WindowsAttr) (uint32, error))
				if !ok {
					smbLE.PutUint32(data, statusUnsupported)
				} else {
					mask, err := maximal(attr)
					smbLE.PutUint32(data, statusError(err))
					if err == nil {
						smbLE.PutUint32(data[4:], mask)
					}
				}
			}
		case "QFid":
			data = make([]byte, 32)
			smbLE.PutUint64(data, attr.ID)
			smbLE.PutUint64(data[8:], volumeSerial)
		default:
			continue
		}
		start := len(encoded)
		if previous >= 0 {
			smbLE.PutUint32(encoded[previous:], uint32(start-previous))
		}
		previous = start
		entry := make([]byte, (24+len(data)+7)&^7)
		smbLE.PutUint16(entry[4:], 16)
		smbLE.PutUint16(entry[6:], 4)
		smbLE.PutUint16(entry[10:], 24)
		smbLE.PutUint32(entry[12:], uint32(len(data)))
		copy(entry[16:], item.Name)
		copy(entry[24:], data)
		encoded = append(encoded, entry...)
	}
	return encoded, nil
}
