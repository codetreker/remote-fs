package fuse

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

type facetSession struct {
	storage.FileSession
	storage.NamespaceAccess
	storage.AtomicFileOpener
	storage.MetadataAccess
	storage.UseOwners
	storage.RangeControl
	storage.NodeReferences
	fail             string
	failure          error
	closes, statuses int
	clean            bool
}

func (s *facetSession) check(name string) error {
	if name == s.fail {
		return s.failure
	}
	return nil
}
func (s *facetSession) CheckNamespaceAccess() error { return s.check("namespace") }
func (s *facetSession) CheckAtomicFileOpen() error  { return s.check("open") }
func (s *facetSession) CheckMetadataAccess() error  { return s.check("metadata") }
func (s *facetSession) CheckUseOwners() error       { return s.check("owners") }
func (s *facetSession) CheckRangeControl() error    { return s.check("ranges") }
func (s *facetSession) CheckNodeReferences() error  { return s.check("references") }
func (s *facetSession) Close(ctx context.Context) error {
	s.closes++
	_, deadline := ctx.Deadline()
	s.clean = ctx.Err() == nil && deadline
	return nil
}
func (s *facetSession) Status(context.Context) (storage.FileSessionStatus, error) {
	s.statuses++
	return storage.FileSessionStatus{}, errors.New("unsupported session reached status")
}

type facetStorage struct {
	storage.FileStorage
	session storage.FileSession
}

func (s facetStorage) CheckFileStorage() error { return nil }
func (s facetStorage) NewFileSession(context.Context, storage.FileSessionOptions) (storage.FileSession, error) {
	return s.session, nil
}

func TestMountAdmissionRejectsMissingRequiredSessionCapabilities(t *testing.T) {
	for _, missing := range []string{"namespace", "open", "metadata", "owners", "ranges", "references"} {
		t.Run(missing, func(t *testing.T) {
			full := &facetSession{}
			var session storage.FileSession
			switch missing {
			case "namespace":
				session = &struct {
					storage.FileSession
					storage.AtomicFileOpener
					storage.MetadataAccess
					storage.UseOwners
					storage.RangeControl
				}{full, full, full, full, full}
			case "open":
				session = &struct {
					storage.FileSession
					storage.NamespaceAccess
					storage.MetadataAccess
					storage.UseOwners
					storage.RangeControl
				}{full, full, full, full, full}
			case "metadata":
				session = &struct {
					storage.FileSession
					storage.NamespaceAccess
					storage.AtomicFileOpener
					storage.UseOwners
					storage.RangeControl
				}{full, full, full, full, full}
			case "owners":
				session = &struct {
					storage.FileSession
					storage.NamespaceAccess
					storage.AtomicFileOpener
					storage.MetadataAccess
					storage.RangeControl
				}{full, full, full, full, full}
			case "ranges":
				session = &struct {
					storage.FileSession
					storage.NamespaceAccess
					storage.AtomicFileOpener
					storage.MetadataAccess
					storage.UseOwners
				}{full, full, full, full, full}
			case "references":
				session = &struct {
					storage.FileSession
					storage.NamespaceAccess
					storage.AtomicFileOpener
					storage.MetadataAccess
					storage.UseOwners
					storage.RangeControl
				}{full, full, full, full, full, full}
			}
			volume, err := newVolume(t.Context(), facetStorage{session: session}, Options{FlushTimeout: time.Second}, nil)
			if volume != nil || !errors.Is(err, syscall.EOPNOTSUPP) || full.closes != 1 || !full.clean || full.statuses != 0 {
				t.Fatalf("admission=%v closes=%d clean=%v statuses=%d", err, full.closes, full.clean, full.statuses)
			}
		})
	}
}

func TestMountAdmissionPreservesBackingCapabilityFailure(t *testing.T) {
	cause := errors.New("backing authority cannot provide capability")
	for _, capability := range []string{"namespace", "open", "metadata", "owners", "ranges", "references"} {
		t.Run(capability, func(t *testing.T) {
			session := &facetSession{fail: capability, failure: cause}
			volume, err := newVolume(t.Context(), facetStorage{session: session}, Options{FlushTimeout: time.Second}, nil)
			if volume != nil || !errors.Is(err, cause) || session.closes != 1 || !session.clean || session.statuses != 0 {
				t.Fatalf("admission=%v closes=%d clean=%v statuses=%d", err, session.closes, session.clean, session.statuses)
			}
		})
	}
	if err := checkSessionCapabilities(&facetSession{}); err != nil {
		t.Fatal(err)
	}
}
