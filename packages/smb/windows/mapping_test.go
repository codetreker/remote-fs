package windows

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
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

func TestMappingDiagnosticPreservesNativeCauses(t *testing.T) {
	reply, err := decodeMappingReply([]byte(`{"found":false,"created":false,"error":"native","code":2148734208,"diagnostic":{"phase":"create","line":123,"category":13,"errorID":"NewSmbMappingFailed","detail":"mapping rejected","truncated":true,"exceptions":[{"type":"CimException","code":2148734208,"message":"outer message","miResult":5,"cimStatusCode":2},{"type":"Win32Exception","code":2147942405,"message":"Access denied","win32Code":5}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	failure := mappingCommandFailure(reply)
	if failure == nil {
		t.Fatal("native failure became success")
	}
	for _, text := range []string{"phase=create", "line=123", "category=13", `error_id="NewSmbMappingFailed"`, `detail="mapping rejected"`, "truncated=true", "HRESULT 0x80131500", "CimException", "outer message", "MI result=5", "CIM status=2", "Win32Exception", "HRESULT 0x80070005", "Access denied", "Win32 code=5"} {
		if !strings.Contains(failure.Error(), text) {
			t.Errorf("missing diagnostic %q in %v", text, failure)
		}
	}
	var outer *mappingExceptionCause
	if !errors.As(failure, &outer) || outer.value.Type != "CimException" {
		t.Fatalf("missing outer exception: %v", failure)
	}
	inner, ok := errors.Unwrap(outer).(*mappingExceptionCause)
	if !ok || inner.value.Type != "Win32Exception" || inner.value.Win32Code == nil || *inner.value.Win32Code != 5 || errors.Unwrap(inner) != nil || !errors.Is(failure, inner) {
		t.Fatalf("native inner cause not preserved: %#v", inner)
	}
}

func TestMappingDiagnosticKeepsOutcomeSentinels(t *testing.T) {
	for _, tc := range []struct {
		kind string
		want error
	}{{"owner", ErrMappingOwnership}, {"busy", ErrMappingBusy}, {"verification", ErrMappingVerification}, {"unknown", ErrMappingVerification}} {
		for _, diagnostic := range []*mappingDiagnostic{nil, {Phase: "verify"}} {
			failure := mappingCommandFailure(mappingReply{Error: tc.kind, Diagnostic: diagnostic})
			if !errors.Is(failure, tc.want) {
				t.Errorf("%s diagnostic=%v: %v, want %v", tc.kind, diagnostic != nil, failure, tc.want)
			}
		}
	}
	if err := mappingCommandFailure(mappingReply{Diagnostic: &mappingDiagnostic{Phase: "complete"}}); err != nil {
		t.Fatalf("successful reply became error: %v", err)
	}
	if err := mappingCommandFailure(mappingReply{Error: "native", Code: 5}); err == nil || !strings.Contains(err.Error(), "0x00000005") {
		t.Fatalf("native failure without diagnostic lost its code: %v", err)
	}
}

func TestMappingDiagnosticDecoderEnforcesBoundsAndTypes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit int
		set   func(*mappingDiagnostic, string)
	}{
		{"phase", 64, func(d *mappingDiagnostic, s string) { d.Phase = s }},
		{"error ID", 256, func(d *mappingDiagnostic, s string) { d.ErrorID = s }},
		{"detail", 1024, func(d *mappingDiagnostic, s string) { d.Detail = s }},
		{"exception type", 256, func(d *mappingDiagnostic, s string) { d.Exceptions[0].Type = s }},
		{"exception message", 1024, func(d *mappingDiagnostic, s string) { d.Exceptions[0].Message = s }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, extra := range []int{0, 1} {
				d := &mappingDiagnostic{Exceptions: []mappingException{{}}}
				tc.set(d, strings.Repeat("é", tc.limit/2)+strings.Repeat("x", extra))
				body, err := json.Marshal(mappingReply{Found: ptr(false), Created: ptr(false), Diagnostic: d})
				if err != nil {
					t.Fatal(err)
				}
				_, err = decodeMappingReply(body)
				if (err != nil) != (extra == 1) {
					t.Errorf("%d UTF8 bytes: %v", tc.limit+extra, err)
				}
			}
		})
	}
	for _, count := range []int{4, 5} {
		body, err := json.Marshal(mappingReply{Found: ptr(false), Created: ptr(false), Diagnostic: &mappingDiagnostic{Exceptions: make([]mappingException, count)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeMappingReply(body); (err != nil) != (count == 5) {
			t.Errorf("%d exceptions: %v", count, err)
		}
	}
	for _, diagnostic := range []string{
		`{"phase":7}`, `{"line":-1}`, `{"category":4294967296}`, `{"truncated":"true"}`, `{"unknown":1}`,
		`{"exceptions":[{"code":-1}]}`, `{"exceptions":[{"miResult":"5"}]}`, `{"exceptions":[{"cimStatusCode":4294967296}]}`,
		`{"exceptions":[{"win32Code":2147483648}]}`, `{"exceptions":[{"unknown":1}]}`,
	} {
		if _, err := decodeMappingReply([]byte(`{"found":false,"created":false,"diagnostic":` + diagnostic + `}`)); err == nil {
			t.Errorf("accepted malformed diagnostic %s", diagnostic)
		}
	}
}
