package smb

import (
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestStableVolumeSerialValidatesCanonicalIdentity(t *testing.T) {
	for _, volume := range []storage.VolumeID{"", "contains space", "\x00volume", "volumé", storage.VolumeID(strings.Repeat("v", storage.MaxFileIdentityBytes+1))} {
		if _, err := stableVolumeSerial(volume); !errors.Is(err, syscall.EINVAL) {
			t.Errorf("serial(%q) error = %v", volume, err)
		}
	}
	for _, volume := range []storage.VolumeID{"volume-a", "tenant:volume/path_1.2", storage.VolumeID(strings.Repeat("v", storage.MaxFileIdentityBytes))} {
		first, err := stableVolumeSerial(volume)
		if err != nil {
			t.Fatal(err)
		}
		second, err := stableVolumeSerial(volume)
		if err != nil || first != second {
			t.Errorf("serial(%q) changed: %x != %x, error %v", volume, first, second, err)
		}
	}
}

func TestPublishVolumeSerialFollowsDurableVolumeAcrossAliases(t *testing.T) {
	server, err := New(endpointConfig())
	if err != nil {
		t.Fatal(err)
	}
	var expected uint64
	var aliases []*Export
	for i, name := range []string{"First", "Alias"} {
		export, err := server.Publish(Share{Name: name, Volume: name, BackendVolume: "test-volume", RootNodeID: uint64(i + 1), Backend: &endpointStorage{}})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			expected = export.volumeSerial
		} else if export.volumeSerial != expected {
			t.Fatalf("alias serial = %x, want %x", export.volumeSerial, expected)
		}
		aliases = append(aliases, export)
	}
	for _, export := range aliases {
		if err := export.Unpublish(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	second, err := New(endpointConfig())
	if err != nil {
		t.Fatal(err)
	}
	export, err := second.Publish(Share{Name: "Restarted", Volume: "different-label", BackendVolume: "test-volume", RootNodeID: 3, Backend: &endpointStorage{}})
	if err != nil {
		t.Fatal(err)
	}
	if export.volumeSerial != expected {
		t.Fatalf("new server serial = %x, want %x", export.volumeSerial, expected)
	}
}

func TestPublishRejectsVolumeSerialCollisionWithReservation(t *testing.T) {
	server, err := New(endpointConfig())
	if err != nil {
		t.Fatal(err)
	}
	serial, err := stableVolumeSerial("test-volume")
	if err != nil {
		t.Fatal(err)
	}
	for _, published := range []bool{false, true} {
		server.exports["RESERVED"] = &Export{
			server: server, key: "RESERVED", share: Share{BackendVolume: "distinct-volume"},
			volumeSerial: serial, active: 1, published: published,
		}
		cause := errors.New("backend check must not run")
		_, err := server.Publish(Share{Name: "Incoming", Volume: "label", BackendVolume: "test-volume", RootNodeID: 1, Backend: &endpointStorage{checkErr: cause}})
		if !errors.Is(err, ErrConfig) || len(server.exports) != 1 {
			t.Fatalf("published=%v: collision error %v, exports %d", published, err, len(server.exports))
		}
	}
	delete(server.exports, "RESERVED")
	if _, err := server.Publish(Share{Name: "Incoming", Volume: "label", BackendVolume: "test-volume", RootNodeID: 1, Backend: &endpointStorage{}}); err != nil {
		t.Fatalf("publish after reservation release: %v", err)
	}
}

func TestPublishRejectsMalformedVolumeBeforeBackendCheck(t *testing.T) {
	server, err := New(endpointConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.Publish(Share{Name: "Malformed", Volume: "label", BackendVolume: "invalid identity", RootNodeID: 1, Backend: &endpointStorage{checkErr: errors.New("backend check must not run")}})
	if !errors.Is(err, ErrConfig) || len(server.exports) != 0 {
		t.Fatalf("malformed volume: error %v, exports %d", err, len(server.exports))
	}
}
