package storage

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestFileIdentityDescriptorsRejectMissingAndNoncanonicalFacts(t *testing.T) {
	backend := BackendIdentityResult{Volume: "opaque-volume/1", Authority: "opaque-authority", RootNodeID: 1}
	if err := backend.Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*BackendIdentityResult){
		func(r *BackendIdentityResult) { r.Volume = "" },
		func(r *BackendIdentityResult) { r.Volume = VolumeID(strings.Repeat("x", MaxFileIdentityBytes+1)) },
		func(r *BackendIdentityResult) { r.Volume = "volume\x00" },
		func(r *BackendIdentityResult) { r.Volume = "volume label" },
		func(r *BackendIdentityResult) { r.Authority = "" },
		func(r *BackendIdentityResult) {
			r.Authority = AuthorityIncarnation(strings.Repeat("x", MaxFileIdentityBytes+1))
		},
		func(r *BackendIdentityResult) { r.Authority = "authority\n" },
		func(r *BackendIdentityResult) { r.RootNodeID = 0 },
	} {
		invalid := backend
		change(&invalid)
		if !errors.Is(invalid.Check(), syscall.EINVAL) {
			t.Fatalf("invalid backend descriptor accepted: %+v", invalid)
		}
	}
	session := FileSessionIdentityResult{Backend: backend, SessionEpoch: "epoch"}
	if err := session.Check(); err != nil {
		t.Fatal(err)
	}
	for _, epoch := range []string{"", strings.Repeat("x", MaxFileIdentityBytes+1), "epoch\x00", "é"} {
		session.SessionEpoch = epoch
		if !errors.Is(session.Check(), syscall.EINVAL) {
			t.Fatalf("invalid epoch accepted: %q", epoch)
		}
	}
	session.Backend.RootNodeID = 0
	session.SessionEpoch = "epoch"
	if !errors.Is(session.Check(), syscall.EINVAL) {
		t.Fatal("invalid backend in session descriptor accepted")
	}
}

func TestAtomicOpenSeparatesBytePermissionsMetadataAndSharingUses(t *testing.T) {
	options := OpenAtOptions{Write: true, MetadataAccess: WriteMetadata, Action: validFileAction(t),
		Target: ChildCondition{State: Any}, Existing: Keep, Use: UseClaim{Uses: ReadData | WriteData}}
	if err := options.Check(); err != nil {
		t.Fatalf("execute and write sharing claim rejected: %v", err)
	}
	for _, permissions := range []MetadataPermissions{0, ReadMetadata, WriteMetadata, ReadMetadata | WriteMetadata} {
		options.MetadataAccess = permissions
		if err := options.Check(); err != nil {
			t.Fatalf("independent metadata permissions %d rejected: %v", permissions, err)
		}
	}
	for _, change := range []func(*OpenAtOptions){
		func(o *OpenAtOptions) { o.MetadataAccess = 4 },
		func(o *OpenAtOptions) { o.Use.Uses = ReadData },
		func(o *OpenAtOptions) { o.Read = true; o.Use.Uses = WriteData },
	} {
		invalid := options
		change(&invalid)
		if !errors.Is(invalid.Check(), syscall.EINVAL) {
			t.Fatalf("invalid byte/metadata/Use combination accepted: %+v", invalid)
		}
	}
}

func TestCloseSettlementPreservesSemanticAndTransientErrorChains(t *testing.T) {
	for _, state := range []CloseSettlementState{CloseSettlementPending, CloseSettlementUnknown} {
		for _, semantic := range []error{nil, syscall.ENOTEMPTY} {
			marker := &CloseSettlementError{State: state, SemanticErr: semantic, Cause: syscall.ETIMEDOUT}
			if err := marker.Check(); err != nil {
				t.Fatal(err)
			}
			wantMessage := "released close settlement is pending"
			if state == CloseSettlementUnknown {
				wantMessage = "released close settlement is unknown"
			}
			if marker.Error() != wantMessage {
				t.Fatalf("settlement message=%q", marker.Error())
			}
			if err := (ReferenceCloseResult{Released: true, Determined: true}).Check(marker); err != nil {
				t.Fatal(err)
			}
			if err := (ReferenceCloseResult{Determined: true}).Check(marker); !errors.Is(err, syscall.EINVAL) {
				t.Fatalf("settlement marker accepted without release: %v", err)
			}
			if !errors.Is(marker, syscall.ETIMEDOUT) || semantic != nil && !errors.Is(marker, semantic) {
				t.Fatalf("lost close error chain: %v", marker)
			}
			var found *CloseSettlementError
			if !errors.As(errors.Join(syscall.EIO, marker), &found) || found != marker {
				t.Fatal("wrapper erased settlement marker")
			}
		}
	}
	for _, invalid := range []*CloseSettlementError{nil, {}, {State: 3, Cause: syscall.EIO}, {State: CloseSettlementPending},
		{State: CloseSettlementPending, SemanticErr: &CloseSettlementError{}, Cause: syscall.EIO},
		{State: CloseSettlementPending, Cause: &CloseSettlementError{}},
	} {
		if !errors.Is(invalid.Check(), syscall.EINVAL) {
			t.Fatalf("invalid settlement marker accepted: %+v", invalid)
		}
	}
	if (&CloseSettlementError{State: 3}).Error() != "released close settlement state is invalid" {
		t.Fatal("invalid state was projected as a valid settlement fact")
	}
	if len((&CloseSettlementError{}).Unwrap()) != 0 {
		t.Fatal("invalid marker unwrap exposed nil errors")
	}
	if err := (ReferenceCloseResult{Released: true}).Check(&CloseSettlementError{}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("released result accepted malformed settlement marker: %v", err)
	}
}
