package httprest

import (
	"bytes"
	"errors"
	"math"
	"syscall"
	"testing"
)

func TestSourcePayloadCapBoundsEveryChangeFrameShape(t *testing.T) {
	for _, kind := range []string{kindCreated, kindModified, kindRemoved, kindRenamed} {
		for nameBytes := 0; nameBytes <= 4; nameBytes++ {
			for fromBytes := 0; fromBytes <= 4; fromBytes++ {
				for contentBytes := 0; contentBytes <= 4; contentBytes++ {
					for notificationBytes := 1; notificationBytes <= 4; notificationBytes++ {
						instant := Time{UnixSec: math.MinInt64, Nanos: 999999999}
						wire := Change{Position: math.MaxInt64, Kind: kind, Parent: math.MaxInt64,
							Name: bytes.Repeat([]byte{0xff}, nameBytes), Notification: bytes.Repeat([]byte{0xff}, notificationBytes)}
						if kind == kindModified && nameBytes == 0 {
							wire.Name = nil
						}
						payloadBytes := nameBytes + notificationBytes
						if kind != kindRemoved {
							wire.Node = &Node{ID: math.MaxInt64, Mode: math.MaxUint32, Size: math.MaxInt64,
								AccessTime: instant, ModTime: instant, Content: bytes.Repeat([]byte{0xff}, contentBytes)}
							payloadBytes += contentBytes
						}
						if kind == kindRenamed {
							wire.From = &Location{Parent: math.MaxInt64, Name: bytes.Repeat([]byte{0xff}, fromBytes)}
							payloadBytes += fromBytes
						}
						bound, err := maxChangeFrameBytes(int64(payloadBytes))
						if err != nil {
							t.Fatal(err)
						}
						if _, err := marshalFrame(eventChange, wire, bound); err != nil {
							t.Fatalf("%s frame with payloads (%d,%d,%d,%d) exceeded derived bound %d: %v", kind, nameBytes, fromBytes, contentBytes, notificationBytes, bound, err)
						}
					}
				}
			}
		}
	}
}

func TestSourcePayloadCapRejectsInvalidOrUnrepresentableBounds(t *testing.T) {
	for _, test := range []struct {
		bound int64
		want  syscall.Errno
	}{
		{0, syscall.EIO}, {-1, syscall.EIO}, {math.MaxInt64, syscall.EFBIG},
	} {
		if size, err := maxChangeFrameBytes(test.bound); size != 0 || !errors.Is(err, test.want) {
			t.Fatalf("source bound %d returned %d, %v; want %v", test.bound, size, err, test.want)
		}
	}
}
