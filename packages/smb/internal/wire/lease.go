package wire

import "bytes"

const (
	OplockLease        byte   = 0xff
	LeaseVersion1      uint16 = 1
	LeaseVersion2      uint16 = 2
	LeaseReadCaching   uint32 = 1
	LeaseHandleCaching uint32 = 2
	LeaseWriteCaching  uint32 = 4
	LeaseParentKeySet  uint32 = 4
)

type LeaseRequest struct {
	Version   uint16
	Key       [16]byte
	State     uint32
	HasParent bool
	ParentKey [16]byte
	Epoch     uint16
}

// LeaseResponse contains only identity metadata. This endpoint cannot encode a
// cache grant by copying the requested state into a response.
type LeaseResponse struct {
	Version   uint16
	Key       [16]byte
	HasParent bool
	ParentKey [16]byte
	Epoch     uint16
}

type LeaseAckRequest struct {
	Key   [16]byte
	State uint32
}

// ParseLease selects a lease after CREATE's outer context bounds are validated.
// Missing RqLs falls back to an ordinary open; non-lease opens ignore RqLs.
func ParseLease(create CreateRequest) (LeaseRequest, bool, error) {
	if create.OplockLevel != OplockLease {
		return LeaseRequest{}, false, nil
	}
	var context *CreateContext
	for i := range create.Contexts {
		if !bytes.Equal(create.Contexts[i].Name, []byte("RqLs")) {
			continue
		}
		if context != nil {
			return LeaseRequest{}, false, ErrMalformed
		}
		context = &create.Contexts[i]
	}
	if context == nil {
		return LeaseRequest{}, false, nil
	}
	lease, err := ParseLeaseRequest(context.Data)
	if err != nil {
		return LeaseRequest{}, false, err
	}
	return lease, true, nil
}

// ParseLeaseRequest owns its decoded values and ignores the reserved fields
// required by MS-SMB2 2.2.13.2.8 and 2.2.13.2.10.
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/250a5100-f8b0-4b32-a202-f592ce4c05e7
// https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/32c16a84-123f-40a9-99a8-00d34964308f
func ParseLeaseRequest(data []byte) (LeaseRequest, error) {
	var out LeaseRequest
	switch len(data) {
	case 32:
		out.Version = LeaseVersion1
	case 52:
		out.Version = LeaseVersion2
	default:
		return out, ErrMalformed
	}
	copy(out.Key[:], data[:16])
	out.State = le.Uint32(data[16:20])
	if out.State & ^uint32(LeaseReadCaching|LeaseHandleCaching|LeaseWriteCaching) != 0 {
		return LeaseRequest{}, ErrMalformed
	}
	if out.Version == LeaseVersion2 {
		flags := le.Uint32(data[20:24])
		if flags & ^uint32(LeaseParentKeySet) != 0 {
			return LeaseRequest{}, ErrMalformed
		}
		out.HasParent = flags&LeaseParentKeySet != 0
		if out.HasParent {
			copy(out.ParentKey[:], data[32:48])
		}
		out.Epoch = le.Uint16(data[48:50])
	}
	return out, nil
}

// LeaseResponseData returns the RqLs data with NONE as the granted lease state.
// V1 responses omit V2 metadata without modifying the caller's lease record.
func LeaseResponseData(lease LeaseResponse) ([]byte, error) {
	var data []byte
	switch lease.Version {
	case LeaseVersion1:
		data = make([]byte, 32)
	case LeaseVersion2:
		data = make([]byte, 52)
		if lease.HasParent {
			le.PutUint32(data[20:24], LeaseParentKeySet)
			copy(data[32:48], lease.ParentKey[:])
		}
		le.PutUint16(data[48:50], lease.Epoch)
	default:
		return nil, ErrMalformed
	}
	copy(data[:16], lease.Key[:])
	return data, nil
}

func (r Request) LeaseAck() (LeaseAckRequest, error) {
	var out LeaseAckRequest
	if err := r.fixed(OplockBreak, 36, 36); err != nil {
		return out, err
	}
	copy(out.Key[:], r.Body[8:24])
	out.State = le.Uint32(r.Body[24:28])
	if out.State & ^uint32(LeaseReadCaching|LeaseHandleCaching|LeaseWriteCaching) != 0 {
		return LeaseAckRequest{}, ErrMalformed
	}
	return out, nil
}
