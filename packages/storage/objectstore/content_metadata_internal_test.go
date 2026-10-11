package objectstore

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

type contentEnrollmentProbe struct {
	*identityAuthorityProbe
	contentErr error
}

func (p *contentEnrollmentProbe) CheckOpenContentMetadata() error { return p.contentErr }

func TestContentMetadataRequiresNativeEnrollmentAndObservation(t *testing.T) {
	missing := &fileSession{native: &identityAuthorityProbe{}}
	if err := missing.CheckOpenContentMetadata(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatal(err)
	}
	cause := errors.New("native content enrollment refused")
	native := &contentEnrollmentProbe{identityAuthorityProbe: &identityAuthorityProbe{}, contentErr: cause}
	session := &fileSession{native: native}
	if err := session.CheckOpenContentMetadata(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	native.identityAuthorityProbe.fileErr = cause
	if err := session.CheckOpenContentMetadata(); !errors.Is(err, cause) {
		t.Fatal(err)
	}
	file := &openFile{}
	if err := file.CheckContentMetadata(); !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatal(err)
	}
	if _, err := file.ObserveContentMetadata(context.Background(), 0); !errors.Is(err, syscall.EBADF) {
		t.Fatal(err)
	}
}

func TestContentActionDigestsBindEnrolledPayloadAndDeepCopySize(t *testing.T) {
	effect := storage.ContentMetadataEffect{Namespace: "test.flags", PayloadBytes: 1, AbsentPayload: []byte{0}, ClearMask: []byte{1}, SetMask: []byte{2}}
	opened := openAtActionInput{Options: storage.OpenAtOptions{ContentMetadataEffects: []storage.ContentMetadataEffect{effect}}}
	digest, err := fileActionDigest(storage.OpFileOpenAt, opened)
	if err != nil {
		t.Fatal(err)
	}
	canonical := canonicalFileActionInput(opened).(openAtActionInput)
	opened.Options.ContentMetadataEffects[0].SetMask[0] = 3
	changed, err := fileActionDigest(storage.OpFileOpenAt, opened)
	if err != nil {
		t.Fatal(err)
	}
	if changed == digest {
		t.Fatal("open digest omitted effect")
	}
	if canonical.Options.ContentMetadataEffects[0].SetMask[0] != 2 {
		t.Fatal("canonical action retained aliased descriptor")
	}
	size := int64(5)
	mutation := canonicalFileMutation(storage.FileMutation{ExpectedSize: &size, ContentEffects: []uint16{1, 0}})
	size = 6
	if *mutation.ExpectedSize != 5 {
		t.Fatal("canonical mutation retained aliased size")
	}
	a, err := fileActionDigest(storage.OpFileMutate, fileMutationActionInput{Command: mutation, Effects: canonical.Options.ContentMetadataEffects})
	if err != nil {
		t.Fatal(err)
	}
	canonical.Options.ContentMetadataEffects[0].SetMask[0] = 4
	b, err := fileActionDigest(storage.OpFileMutate, fileMutationActionInput{Command: mutation, Effects: canonical.Options.ContentMetadataEffects})
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("write digest omitted sealed effect")
	}
}
