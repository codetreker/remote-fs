package windows

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"testing"
	"unicode/utf16"
)

func ptr[T any](value T) *T { return &value }

type mappingProbe struct {
	owner                logonIdentity
	current              *mappingRecord
	created              mappingRecord
	createErr, removeErr error
	createKnown          bool
	creates, removes     int
	forced               bool
}

func newMappingProbe() *mappingProbe {
	return &mappingProbe{owner: logonIdentity{SID: "S-1-5-21-1-2-3-1001", AuthenticationID: 17, SessionID: 2}, createKnown: true,
		created: mappingRecord{LocalPath: "R:", RemotePath: `\\127.0.0.1\work`, Status: ptr(uint32(0)), Device: "owned-device"}}
}

func (p *mappingProbe) identity(context.Context) (logonIdentity, error) { return p.owner, nil }
func (p *mappingProbe) query(context.Context, logonIdentity, string) (mappingRecord, bool, error) {
	if p.current == nil {
		return mappingRecord{}, false, nil
	}
	return *p.current, true, nil
}
func (p *mappingProbe) create(context.Context, logonIdentity, MappingOptions) (mappingRecord, bool, error) {
	p.creates++
	if p.createKnown {
		record := p.created
		p.current = &record
	}
	return p.created, p.createKnown, p.createErr
}
func (p *mappingProbe) remove(_ context.Context, _ logonIdentity, _ string, force bool) error {
	p.removes++
	p.forced = force
	if p.removeErr != nil {
		return p.removeErr
	}
	p.current = nil
	return nil
}

func TestMappingNeverTakesAnOccupiedDrive(t *testing.T) {
	p := newMappingProbe()
	p.current = &p.created
	if m, err := mapWithSystem(t.Context(), MappingOptions{"r:", "work", 1445}, p); m != nil || !errors.Is(err, ErrMappingBusy) || p.creates != 0 || p.removes != 0 {
		t.Fatalf("occupied Map = %v, %v; creates=%d removes=%d", m, err, p.creates, p.removes)
	}
}

func TestZeroMappingDoesNotClaimAnAcceptedSystemOperation(t *testing.T) {
	var m Mapping
	if m.Status().ParametersAccepted {
		t.Fatal("zero mapping invented accepted parameters")
	}
	if err := m.Unmount(t.Context()); !errors.Is(err, ErrMappingOwnership) {
		t.Fatalf("zero mapping Unmount = %v", err)
	}
}

func TestMappingValidatesObservedStateAndRollsBackOnlyConfirmedCreation(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*mappingRecord)
	}{
		{"missing status", func(r *mappingRecord) { r.Status = nil }},
		{"disconnected", func(r *mappingRecord) { r.Status = ptr(uint32(2)) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := newMappingProbe()
			test.change(&p.created)
			m, err := mapWithSystem(t.Context(), MappingOptions{"R:", "work", 1445}, p)
			if m != nil || !errors.Is(err, ErrMappingVerification) || p.removes != 1 || p.forced || p.current != nil {
				t.Fatalf("unverified Map = %v, %v; removes=%d force=%t", m, err, p.removes, p.forced)
			}
		})
	}
	p := newMappingProbe()
	p.createKnown = false
	p.createErr = ErrMappingUncertain
	if m, err := mapWithSystem(t.Context(), MappingOptions{"R:", "work", 1445}, p); m != nil || !errors.Is(err, ErrMappingUncertain) || p.removes != 0 {
		t.Fatalf("unknown creation = %v,%v removes=%d", m, err, p.removes)
	}
}

func TestMappingBusyUnmountPreservesOwnershipAndForcedRemovalIsExplicit(t *testing.T) {
	p := newMappingProbe()
	m, err := mapWithSystem(t.Context(), MappingOptions{"R:", "work", 1445}, p)
	if err != nil {
		t.Fatal(err)
	}
	options := m.Options()
	options.LocalPath = "S:"
	if m.Options().LocalPath != "R:" {
		t.Fatal("caller changed mapping ownership through its options copy")
	}
	status := m.Status()
	if !status.ParametersAccepted || status.Options.TCPPort != 1445 || status.ObservedRemotePath != `\\127.0.0.1\work` || status.DeviceTarget != "owned-device" || status.Closed {
		t.Fatalf("mapping status conflated accepted options and observed target: %+v", status)
	}
	*status.ConnectionStatus = 3
	if *m.Status().ConnectionStatus != 0 {
		t.Fatal("caller changed the retained mapping observation")
	}
	p.removeErr = ErrMappingBusy
	if err := m.Unmount(t.Context()); !errors.Is(err, ErrMappingBusy) || p.current == nil || p.forced {
		t.Fatalf("busy unmount = %v", err)
	}
	p.removeErr = nil
	if err := m.ForceUnmount(t.Context()); err != nil || p.current != nil || !p.forced {
		t.Fatalf("force unmount = %v", err)
	}
	if !m.Status().Closed {
		t.Fatal("successful removal did not retire mapping status")
	}
	count := p.removes
	p.current = &p.created
	if err := m.Unmount(t.Context()); err != nil || p.removes != count || p.current == nil {
		t.Fatalf("closed owner touched reused drive: %v", err)
	}
}

func TestMappingRefusesAnotherLogonOrReplacedTarget(t *testing.T) {
	for _, change := range []func(*mappingProbe){
		func(p *mappingProbe) { p.owner.AuthenticationID++ },
		func(p *mappingProbe) { p.owner.SessionID++ },
		func(p *mappingProbe) { p.current.Device = "another-device" },
		func(p *mappingProbe) { p.current.RemotePath = `\\127.0.0.1\other` },
	} {
		p := newMappingProbe()
		m, err := mapWithSystem(t.Context(), MappingOptions{"R:", "work", 1445}, p)
		if err != nil {
			t.Fatal(err)
		}
		change(p)
		if err := m.Unmount(t.Context()); !errors.Is(err, ErrMappingOwnership) || p.removes != 0 {
			t.Fatalf("foreign mapping unmount = %v; removes=%d", err, p.removes)
		}
	}
}

func TestMappingRollbackFailureReturnsItsOwnerForCleanup(t *testing.T) {
	p := newMappingProbe()
	p.created.Status = ptr(uint32(2))
	p.removeErr = ErrMappingBusy
	m, err := mapWithSystem(t.Context(), MappingOptions{"R:", "work", 1445}, p)
	if m == nil || !errors.Is(err, ErrMappingBusy) || !errors.Is(err, ErrMappingVerification) {
		t.Fatalf("failed rollback = %v, %v", m, err)
	}
	p.removeErr = nil
	if err := m.Unmount(t.Context()); err != nil || p.current != nil {
		t.Fatalf("retry rollback = %v", err)
	}
}

func TestMappingInputsAndCommandResultsAreExplicit(t *testing.T) {
	for _, options := range []MappingOptions{{"RR:", "work", 1445}, {"R:", "../work", 1445}, {"R:", "work", 0}, {"R:", "a\nshare", 1445}, {"R:", "work;Remove-Item", 1445}} {
		p := newMappingProbe()
		if m, err := mapWithSystem(t.Context(), options, p); m != nil || !errors.Is(err, ErrInvalidMapping) || p.creates != 0 {
			t.Errorf("invalid Map = %v,%v", m, err)
		}
	}
	for _, data := range []string{`{}`, `{"found":true,"created":false}`, `{"found":false,"created":false} {}`, `{"found":false,"created":false,"surprise":true}`} {
		if _, err := decodeMappingReply([]byte(data)); err == nil {
			t.Errorf("accepted incomplete command result %s", data)
		}
	}
}

func TestMappingCommandEncodingPreservesTheFixedPowerShellProgram(t *testing.T) {
	data, err := base64.StdEncoding.Strict().DecodeString(encodedMappingScript())
	if err != nil {
		t.Fatalf("mapping command is not valid base64: %v", err)
	}
	if len(data)%2 != 0 {
		t.Fatalf("mapping command has %d bytes, not complete UTF-16 code units", len(data))
	}
	words := make([]uint16, len(data)/2)
	if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, words); err != nil {
		t.Fatalf("decode mapping command as UTF-16LE: %v", err)
	}
	if decoded := string(utf16.Decode(words)); decoded != mappingScript {
		t.Fatal("mapping command does not preserve the complete fixed PowerShell program")
	}
}
