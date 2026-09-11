package httprest_test

import (
	"errors"
	"io"
	"syscall"
	"testing"

	"github.com/codetreker/remote-fs/packages/storage"
)

func TestStreamAuthorizationFaultsPreserveErrnoAtEveryDeliveryBoundary(t *testing.T) {
	for name, tc := range map[string]struct {
		body  string
		errno syscall.Errno
	}{
		"denied":      {`{"message":"access denied","errno":"EACCES"}`, syscall.EACCES},
		"failed":      {`{"message":"authorization failed","errno":"EIO"}`, syscall.EIO},
		"generic":     {`{"message":"backend unavailable"}`, syscall.EIO},
		"unknown":     {`{"message":"access denied","errno":"UNKNOWN"}`, syscall.EIO},
		"other errno": {`{"message":"access denied","errno":"ENOENT"}`, syscall.EIO},
		"null":        {`{"message":"access denied","errno":null}`, syscall.EIO},
		"number":      {`{"message":"access denied","errno":13}`, syscall.EIO},
		"empty":       {`{"message":"access denied","errno":""}`, syscall.EIO},
		"duplicate":   {`{"message":"access denied","errno":"EIO","errno":"EACCES"}`, syscall.EIO},
	} {
		t.Run(name, func(t *testing.T) {
			check := func(err error) {
				t.Helper()
				if !errors.Is(err, tc.errno) || storage.ErrnoOf(err) != tc.errno || errors.Is(err, io.EOF) {
					t.Fatalf("fault = %v; want %v", err, tc.errno)
				}
			}
			initial := streamOf(t, frame("fault", tc.body))
			_, err := initial.Subscribe(t.Context())
			check(err)
			_, err = initial.Resubscribe(t.Context(), "log", 3)
			check(err)
			_, err = initial.Snapshot(t.Context())
			check(err)
			changes := streamOf(t, frame("start", `{"incarnation":"log","position":3,"tail":3}`)+frame("fault", tc.body))
			sub, err := changes.Subscribe(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Close()
			_, first := sub.Next()
			check(first)
			_, second := sub.Next()
			if second != first {
				t.Fatalf("terminal subscription fault changed: %v -> %v", first, second)
			}
			if sub.Position() != 3 {
				t.Fatalf("fault advanced subscription position: %d", sub.Position())
			}
			picture := streamOf(t, frame("open", `{"position":3}`)+frame("fault", tc.body))
			snap, err := picture.Snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer snap.Close()
			rows, first := snap.Next()
			check(first)
			if rows != nil {
				t.Fatalf("fault returned rows: %+v", rows)
			}
			_, second = snap.Next()
			if second != first {
				t.Fatalf("terminal snapshot fault changed: %v -> %v", first, second)
			}
			if snap.Position() != 3 {
				t.Fatalf("fault changed snapshot position: %d", snap.Position())
			}
		})
	}
}
