package storage_test

import (
	"bytes"
	"errors"
	"reflect"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func contentEffect() storage.ContentMetadataEffect {
	return storage.ContentMetadataEffect{Namespace: "test.flags", PayloadBytes: 2, PresentPrefix: []byte{0x31}, AbsentPayload: []byte{0x31, 0}, ClearMask: []byte{0, 0x10}, SetMask: []byte{0, 0x20}}
}

func TestContentMetadataDescriptorBoundsAndSealedPrefix(t *testing.T) {
	valid := contentEffect()
	if err := valid.Check(); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*storage.ContentMetadataEffect){
		func(e *storage.ContentMetadataEffect) { e.Namespace = "bad key" },
		func(e *storage.ContentMetadataEffect) { e.PayloadBytes = 0 },
		func(e *storage.ContentMetadataEffect) { e.PayloadBytes = storage.MaxMetadataValueBytes + 1 },
		func(e *storage.ContentMetadataEffect) { e.PresentPrefix = []byte{1, 2, 3} },
		func(e *storage.ContentMetadataEffect) { e.AbsentPayload[0] = 0 },
		func(e *storage.ContentMetadataEffect) { e.ClearMask[0] = 1 },
		func(e *storage.ContentMetadataEffect) { e.SetMask[0] = 1 },
		func(e *storage.ContentMetadataEffect) { e.ClearMask = nil },
		func(e *storage.ContentMetadataEffect) { e.SetMask = nil },
		func(e *storage.ContentMetadataEffect) { e.AbsentPayload = nil },
	} {
		e := storage.CloneContentMetadataEffects([]storage.ContentMetadataEffect{valid})[0]
		change(&e)
		if err := e.Check(); err == nil {
			t.Fatalf("accepted malformed descriptor: %+v", e)
		}
	}
	if err := storage.CheckContentMetadataEffects([]storage.ContentMetadataEffect{valid, valid}); !errors.Is(err, syscall.EINVAL) {
		t.Fatalf("duplicate namespace: %v", err)
	}
	large := contentEffect()
	large.PayloadBytes = storage.MaxMetadataValueBytes
	large.PresentPrefix = nil
	large.AbsentPayload = make([]byte, large.PayloadBytes)
	large.ClearMask = make([]byte, large.PayloadBytes)
	large.SetMask = make([]byte, large.PayloadBytes)
	if err := storage.CheckContentMetadataEffects([]storage.ContentMetadataEffect{large}); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("aggregate bound: %v", err)
	}
	if err := storage.CheckContentMetadataEffects(make([]storage.ContentMetadataEffect, storage.MaxContentMetadataEffects+1)); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("effect count bound: %v", err)
	}
}

func TestContentMetadataCloneObservationAndTransformation(t *testing.T) {
	original := contentEffect()
	effects := []storage.ContentMetadataEffect{original}
	cloned := storage.CloneContentMetadataEffects(effects)
	original.PresentPrefix[0] = 0
	original.AbsentPayload[0] = 0
	original.ClearMask[1] = 0
	original.SetMask[1] = 0
	got, err := cloned[0].Apply(&storage.OpaquePayload{Version: []byte{1}, Data: []byte{0x31, 0xd5}})
	if err != nil || !bytes.Equal(got, []byte{0x31, 0xe5}) {
		t.Fatalf("transform=%x,%v", got, err)
	}
	absent, err := cloned[0].Apply(nil)
	if err != nil || !bytes.Equal(absent, []byte{0x31, 0x20}) {
		t.Fatalf("absence=%x,%v", absent, err)
	}
	for _, value := range []*storage.OpaquePayload{{Data: []byte{0x31, 0}}, {Version: []byte{1}, Data: []byte{}}, {Version: []byte{1}, Data: []byte{0, 0}}, {Version: make([]byte, storage.MaxObservationTokenBytes+1), Data: []byte{0x31, 0}}} {
		if _, err := cloned[0].Apply(value); !errors.Is(err, syscall.EIO) {
			t.Fatalf("corrupt value accepted: %+v,%v", value, err)
		}
	}
	observed := storage.ContentMetadataObservation{NodeID: 4, Value: &storage.OpaquePayload{Version: []byte{1}, Data: []byte{0x31, 0}}}
	copy := observed.Clone()
	observed.Value.Data[0] = 0
	observed.Value.Version[0] = 0
	if err := copy.CheckEffect(4, cloned[0]); err != nil {
		t.Fatal(err)
	}
	if err := copy.CheckEffect(5, cloned[0]); !errors.Is(err, syscall.EIO) {
		t.Fatalf("wrong node: %v", err)
	}
	if err := (storage.ContentMetadataObservation{}).Check(); !errors.Is(err, syscall.EIO) {
		t.Fatal(err)
	}
	if err := (storage.ContentMetadataObservation{NodeID: 4}).CheckEffect(4, cloned[0]); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(storage.CloneContentMetadataEffects(nil), []storage.ContentMetadataEffect(nil)) {
		t.Fatal("nil descriptor vector lost")
	}
}

func TestContentMutationRequiresEnrolledNonemptyDataAndExplicitAbsence(t *testing.T) {
	id, err := storage.NewFileActionID(1)
	if err != nil {
		t.Fatal(err)
	}
	command := storage.FileMutation{Action: id, Kind: storage.MutateWriteAt, Data: []byte{1}, ContentEffects: []uint16{0}, ExpectedMetadata: map[string][]byte{"test.flags": nil}}
	effects := []storage.ContentMetadataEffect{contentEffect()}
	if err := storage.CheckContentMutation(command, effects); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []storage.FileMutation{
		{Action: id, Kind: storage.MutateWriteAt, Data: []byte{1}, ContentEffects: []uint16{0}},
		{Action: id, Kind: storage.MutateWriteAt, Data: []byte{1}, ContentEffects: []uint16{1}},
		{Action: id, Kind: storage.MutateWriteAt, Data: []byte{1}, ContentEffects: []uint16{0, 0}},
		{Action: id, Kind: storage.MutateWriteAt, ContentEffects: []uint16{0}},
		{Action: id, Kind: storage.MutateTruncate, ContentEffects: []uint16{0}},
		{Action: id, Kind: storage.MutateWriteAt, Data: []byte{1}, ContentEffects: []uint16{0}, Metadata: map[string]storage.OpaquePayload{"test.flags": {Data: []byte{1}}}},
	} {
		if err := storage.CheckContentMutation(bad, effects); !errors.Is(err, syscall.EINVAL) {
			t.Fatalf("accepted mutation %+v: %v", bad, err)
		}
	}
	if _, err := storage.ResolveContentMetadataEffects(effects, make([]uint16, storage.MaxContentMetadataEffects+1)); !errors.Is(err, syscall.EFBIG) {
		t.Fatal(err)
	}
	opts := storage.OpenAtOptions{Target: storage.ChildCondition{State: storage.Any}, Action: id, Write: true, Existing: storage.Keep, Use: storage.UseClaim{Uses: storage.WriteData}, ContentMetadataEffects: effects}
	if err := opts.Check(); err != nil {
		t.Fatal(err)
	}
	opts.Write = false
	opts.Read = true
	opts.Use.Uses = storage.ReadData
	if err := opts.Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
	if err := (storage.FileOpenOptions{OpenAccess: storage.OpenAccess{Write: true, ContentMetadataEffects: effects}}).Check(); !errors.Is(err, syscall.EINVAL) {
		t.Fatal(err)
	}
}

func TestContentMetadataObservationKeepsAbsenceDistinctFromPresentEmpty(t *testing.T) {
	valid := map[string]storage.ContentMetadataObservation{
		"absent":                {NodeID: 7},
		"present empty":         {NodeID: 7, Value: &storage.OpaquePayload{Version: []byte{1}}},
		"largest bounded value": {NodeID: 7, Value: &storage.OpaquePayload{Version: make([]byte, storage.MaxObservationTokenBytes), Data: make([]byte, storage.MaxMetadataValueBytes)}},
	}
	for name, observation := range valid {
		t.Run(name, func(t *testing.T) {
			if err := observation.Check(); err != nil {
				t.Fatal(err)
			}
			cloned := observation.Clone()
			if (cloned.Value == nil) != (observation.Value == nil) {
				t.Fatal("clone erased namespace presence")
			}
		})
	}
	for name, observation := range map[string]storage.ContentMetadataObservation{
		"unknown object":    {},
		"missing version":   {NodeID: 7, Value: &storage.OpaquePayload{}},
		"oversized version": {NodeID: 7, Value: &storage.OpaquePayload{Version: make([]byte, storage.MaxObservationTokenBytes+1)}},
		"oversized payload": {NodeID: 7, Value: &storage.OpaquePayload{Version: []byte{1}, Data: make([]byte, storage.MaxMetadataValueBytes+1)}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := observation.Check(); !errors.Is(err, syscall.EIO) {
				t.Fatalf("invalid fact accepted: %v", err)
			}
		})
	}
}

func TestOpenAccessCloneSeparatesPolicyIntentFromEnrolledEffects(t *testing.T) {
	access := storage.OpenAccess{Read: true, Write: true, Create: true, Truncate: true, Exclusive: true, ContentMetadataEffects: []storage.ContentMetadataEffect{contentEffect()}}
	cloned := access.Clone()
	if !reflect.DeepEqual(cloned, access) {
		t.Fatal("cloning changed open intent")
	}
	cloned.Read = false
	cloned.ContentMetadataEffects[0].Namespace = "test.other"
	cloned.ContentMetadataEffects[0].PresentPrefix[0] = 0
	cloned.ContentMetadataEffects[0].AbsentPayload[0] = 0
	cloned.ContentMetadataEffects[0].ClearMask[1] = 0
	cloned.ContentMetadataEffects[0].SetMask[1] = 0
	if !access.Read || !reflect.DeepEqual(access.ContentMetadataEffects, []storage.ContentMetadataEffect{contentEffect()}) {
		t.Fatal("policy intent mutation changed the sealed source")
	}
	access.ContentMetadataEffects[0].AbsentPayload[1] = 1
	if cloned.ContentMetadataEffects[0].AbsentPayload[1] != 0 {
		t.Fatal("source mutation changed policy copy")
	}
	if clone := (storage.OpenAccess{Write: true}).Clone(); !clone.Write || clone.ContentMetadataEffects != nil {
		t.Fatal("no-effect open clone changed intent")
	}
}
