package httprest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestHTTPAllocationUnitGetterUsesExistingCapability(t *testing.T) {
	if unit, err := (&remoteFileSession{}).AllocationUnitBytes(t.Context()); !errors.Is(err, syscall.EOPNOTSUPP) || unit != 0 {
		t.Fatalf("missing capability unit=%d error=%v", unit, err)
	}
	unit := uint64(4096)
	response := fileResponse{Epoch: 1, Data: []byte{}, AllocationUnitBytes: &unit}
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	var decoded fileResponse
	if err := decodeFileJSON(body, &decoded); err != nil {
		t.Fatal(err)
	}
	request := fileRequest{Op: storage.OpFileAllocationUnit}
	if err := validateFileResponse(request, decoded); err != nil {
		t.Fatal(err)
	}
	for name, damaged := range map[string]string{
		"absent":     strings.Replace(string(body), `,"allocationUnitBytes":4096`, "", 1),
		"zero":       strings.Replace(string(body), `"allocationUnitBytes":4096`, `"allocationUnitBytes":0`, 1),
		"null":       strings.Replace(string(body), `"allocationUnitBytes":4096`, `"allocationUnitBytes":null`, 1),
		"duplicate":  strings.Replace(string(body), `"allocationUnitBytes":4096`, `"allocationUnitBytes":4096,"allocationUnitBytes":4096`, 1),
		"fractional": strings.Replace(string(body), `"allocationUnitBytes":4096`, `"allocationUnitBytes":4096.5`, 1),
		"wrong case": strings.Replace(string(body), `"allocationUnitBytes":4096`, `"AllocationUnitBytes":4096`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			decoded = fileResponse{}
			if err := decodeFileJSON([]byte(damaged), &decoded); err != nil {
				return
			}
			if err := validateFileResponse(request, decoded); err == nil {
				t.Fatalf("invalid unit response accepted: %s", damaged)
			}
		})
	}
	client, _, _ := retainedHTTPFixture(t, DefaultFileLimits())
	session := httpStateSession(t, client)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if unit, err := session.AllocationUnitBytes(ctx); !errors.Is(err, context.Canceled) || unit != 0 {
		t.Fatalf("cancelled unit=%d error=%v", unit, err)
	}
}

type allocationUnitSessionProbe struct {
	storage.FileSession
	unit          uint64
	unitErr       error
	identity      storage.FileSessionIdentityResult
	changed       storage.FileSessionIdentityResult
	identityCalls int
	cancel        context.CancelFunc
}

func (*allocationUnitSessionProbe) CheckAllocationReporting() error { return nil }
func (*allocationUnitSessionProbe) CheckFileSessionIdentity() error { return nil }
func (p *allocationUnitSessionProbe) AllocationUnitBytes(context.Context) (uint64, error) {
	if p.cancel != nil {
		p.cancel()
	}
	return p.unit, p.unitErr
}
func (p *allocationUnitSessionProbe) FileSessionIdentity(context.Context) (storage.FileSessionIdentityResult, error) {
	p.identityCalls++
	if p.identityCalls > 1 && p.changed.SessionEpoch != "" {
		return p.changed, nil
	}
	return p.identity, nil
}

func TestHTTPAllocationUnitRejectsFailedOrChangedAuthorityFacts(t *testing.T) {
	identity := storage.FileSessionIdentityResult{Backend: storage.BackendIdentityResult{Volume: "volume", Authority: "authority", RootNodeID: 1}, SessionEpoch: "epoch"}
	changed := identity
	changed.Backend.Volume = "other"
	for _, test := range []struct {
		name    string
		unit    uint64
		failure error
		changed storage.FileSessionIdentityResult
		cancel  bool
		want    error
	}{
		{name: "zero", want: syscall.EIO},
		{name: "getter error", unit: 4096, failure: syscall.ENOSPC, want: syscall.ENOSPC},
		{name: "authority changed", unit: 4096, changed: changed, want: syscall.EIO},
		{name: "cancelled after capture", unit: 4096, cancel: true, want: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			probe := &allocationUnitSessionProbe{unit: test.unit, unitErr: test.failure, identity: identity, changed: test.changed}
			if test.cancel {
				probe.cancel = cancel
			}
			response, err := (&Handler{}).performSessionCapability(ctx, probe, fileRequest{Op: storage.OpFileAllocationUnit})
			if !errors.Is(err, test.want) || response.AllocationUnitBytes != nil {
				t.Fatalf("failed unit response=%+v error=%v", response, err)
			}
		})
	}
}
