package sqlite

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestFileFingerprintUsesStoredTimeFacts(t *testing.T) {
	for _, instant := range []time.Time{time.Date(10000, 6, 7, 8, 9, 10, 123, time.FixedZone("east", 9*3600)), time.Date(-500, 6, 7, 8, 9, 10, 123, time.UTC), time.Now()} {
		first, err := fileFingerprint(struct{ Time *time.Time }{&instant})
		if err != nil {
			t.Fatal(err)
		}
		same := instant.In(time.FixedZone("west", -7*3600)).Round(0)
		second, err := fileFingerprint(struct{ Time *time.Time }{&same})
		if err != nil {
			t.Fatal(err)
		}
		if first != second {
			t.Fatalf("same stored instant had different fingerprint: %v", instant)
		}
		changed := instant.Add(time.Nanosecond)
		other, err := fileFingerprint(struct{ Time *time.Time }{&changed})
		if err != nil || other == first {
			t.Fatalf("changed instant retained fingerprint: %v", err)
		}
	}
}

func TestFileFingerprintDistinguishesFramingTypesAndOptionalValues(t *testing.T) {
	zero := time.Time{}
	for _, pair := range [][2]any{
		{struct{ At *time.Time }{nil}, struct{ At *time.Time }{&zero}},
		{[]string{"ab", "c"}, []string{"a", "bc"}},
		{[]byte(nil), []byte{}},
		{int64(1), uint64(1)},
		{struct{ A string }{"x"}, struct{ B string }{"x"}},
		{any(nil), (*uint64)(nil)},
	} {
		a, err := fileFingerprint(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		b, err := fileFingerprint(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		if a == b {
			t.Fatalf("different request shapes collided: %#v %#v", pair[0], pair[1])
		}
	}
	type recursive struct{ Next *recursive }
	cycle := &recursive{}
	cycle.Next = cycle
	for _, value := range []any{func() {}, make(chan int), map[string]int{"x": 1}, struct{ hidden int }{1}, cycle} {
		fingerprint, err := fileFingerprint(value)
		if !errors.Is(err, syscall.EINVAL) || fingerprint != ([32]byte{}) {
			t.Fatalf("unsupported value produced usable fingerprint: %T %v", value, err)
		}
	}
}

func TestNativeFileActionsAcceptExtendedYearsAndCanonicalTimeReplay(t *testing.T) {
	s, existing := openPublicationFile(t)
	session := existing.session
	ctx := t.Context()
	large := time.Date(10000, 6, 7, 8, 9, 10, 123, time.FixedZone("east", 9*3600))
	request := storage.CreateAndRetainRequest{Target: publicationTarget(t, s, session, "dated"), Initial: storage.NodeInitial{Kind: storage.NodeRegular, CreationTime: &large, ModTime: &large}, Claim: storage.AccessClaim{Uses: storage.ReadContent | storage.WriteContent}}
	createID := fileActionID(t, session)
	created, err := session.CreateAndRetainAt(ctx, request, createID)
	if err != nil {
		t.Fatalf("year10000 create=%v", err)
	}
	if created.Observation.Attr.CreationTime == nil || !created.Observation.Attr.CreationTime.Equal(large) || !created.Observation.Attr.ModTime.Equal(large) {
		t.Fatalf("large stored times=%+v", created.Observation.Attr)
	}
	otherZone := large.In(time.FixedZone("other", -3*3600))
	request.Initial.CreationTime = &otherZone
	request.Initial.ModTime = &otherZone
	replay, err := session.CreateAndRetainAt(ctx, request, createID)
	if err != nil || replay.Reference != created.Reference || replay.Effects != created.Effects {
		t.Fatalf("equivalent zone create replay=%+v %v", replay, err)
	}
	native, live, err := session.Reference(ctx, created.Reference)
	if err != nil || !live {
		t.Fatalf("created reference=%v %v", live, err)
	}
	f := native.(*fileReference)
	negative := time.Date(-500, 6, 7, 8, 9, 10, 456, time.UTC)
	change := storage.AttrChange{ExpectedRevision: created.Observation.Attr.MetadataRevision, CreationTime: &negative, ModTime: &negative}
	changeID := fileActionID(t, session)
	updated, err := f.SetAttr(ctx, change, changeID)
	if err != nil {
		t.Fatalf("negative-year setattr=%v", err)
	}
	if updated.Observation.Attr.CreationTime == nil || !updated.Observation.Attr.CreationTime.Equal(negative) || !updated.Observation.Attr.ModTime.Equal(negative) {
		t.Fatalf("negative stored times=%+v", updated.Observation.Attr)
	}
	equivalent := negative.In(time.FixedZone("another", 4*3600))
	change.CreationTime = &equivalent
	change.ModTime = &equivalent
	replay, err = f.SetAttr(ctx, change, changeID)
	if err != nil || replay.Observation.Attr.MetadataRevision != updated.Observation.Attr.MetadataRevision {
		t.Fatalf("equivalent setattr replay=%+v %v", replay, err)
	}
	changed := equivalent.Add(time.Nanosecond)
	change.ModTime = &changed
	_, err = f.SetAttr(ctx, change, changeID)
	var rejected *storage.FileError
	if !errors.Is(err, syscall.EINVAL) || !errors.As(err, &rejected) || !rejected.NotAdmitted {
		t.Fatalf("changed instant did not reject before admission=%v", err)
	}
	observed, err := f.Stat(ctx, storage.ObservationOptions{})
	if err != nil || !observed.Attr.ModTime.Equal(negative) {
		t.Fatalf("rejected replay changed stored time=%+v %v", observed, err)
	}
}
