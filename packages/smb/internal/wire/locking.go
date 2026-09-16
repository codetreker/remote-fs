package wire

const (
	LockShared          uint32 = 1
	LockExclusive       uint32 = 2
	LockUnlock          uint32 = 4
	LockFailImmediately uint32 = 16
)

type LockElement struct {
	Offset uint64
	Length uint64
	Flags  uint32
}
type LockRequest struct {
	Sequence uint32
	FileID   FileID
	Elements []LockElement
}
type NotifyRequest struct {
	Flags        uint16
	OutputLength uint32
	FileID       FileID
	Filter       uint32
}

func (r Request) Lock() (LockRequest, error) {
	var out LockRequest
	if err := r.fixed(Lock, 48, 48); err != nil {
		return out, err
	}
	n := int(le.Uint16(r.Body[2:4]))
	if n == 0 || n > (len(r.Body)-24)/24 {
		return out, ErrMalformed
	}
	out.Sequence = le.Uint32(r.Body[4:8])
	copy(out.FileID[:], r.Body[8:24])
	out.Elements = make([]LockElement, n)
	for i := range out.Elements {
		p := r.Body[24+i*24:]
		out.Elements[i] = LockElement{Offset: le.Uint64(p), Length: le.Uint64(p[8:16]), Flags: le.Uint32(p[16:20])}
	}
	return out, nil
}

func (r Request) Notify() (NotifyRequest, error) {
	var out NotifyRequest
	if err := r.fixed(ChangeNotify, 32, 32); err != nil {
		return out, err
	}
	out.Flags = le.Uint16(r.Body[2:4])
	out.OutputLength = le.Uint32(r.Body[4:8])
	copy(out.FileID[:], r.Body[8:24])
	out.Filter = le.Uint32(r.Body[24:28])
	return out, nil
}
