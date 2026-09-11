package httprest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestStreamFaultErrnoPreservesTypedAndLegacyFailures(t *testing.T) {
	for name, test := range map[string]struct {
		body string
		want syscall.Errno
	}{
		"denied": {`{"message":"access denied","errno":"EACCES"}`, syscall.EACCES},
		"failed": {`{"message":"authorization failed","errno":"EIO"}`, syscall.EIO},
		"legacy": {`{"message":"storage unavailable"}`, syscall.EIO},
	} {
		t.Run(name, func(t *testing.T) {
			err := faultOf([]byte(test.body))
			if !errors.Is(err, test.want) {
				t.Fatalf("fault = %v, want %v", err, test.want)
			}
			wrapped := fmt.Errorf("stream boundary: %w", err)
			if errno, ok := streamFaultErrno(wrapped); !ok || errno != test.want {
				t.Fatalf("typed fault = %v, %v", errno, ok)
			}
			reader := newFrameReader(strings.NewReader("event: fault\ndata: "+test.body+"\n\n"), time.Second, 1024, func() { t.Error("an in-memory frame became silent") })
			if err := reader.decode(eventOpen, &SnapshotOpen{}); !errors.Is(err, test.want) {
				t.Fatalf("initial frame fault = %v", err)
			}
		})
	}
	if _, ok := streamFaultErrno(errors.New("access denied")); ok {
		t.Fatal("ordinary error text became a typed fault")
	}
}

func TestStreamFaultRejectsAmbiguousErrnoAsProtocolFailure(t *testing.T) {
	for name, body := range map[string]string{
		"unknown":           `{"message":"failed","errno":"ENOENT"}`,
		"empty":             `{"message":"failed","errno":""}`,
		"null":              `{"message":"failed","errno":null}`,
		"number":            `{"message":"failed","errno":13}`,
		"object":            `{"message":"failed","errno":{}}`,
		"duplicate":         `{"message":"failed","errno":"EIO","errno":"EACCES"}`,
		"same duplicate":    `{"message":"failed","errno":"EACCES","errno":"EACCES"}`,
		"escaped duplicate": `{"message":"failed","errno":"EIO","\u0065rrno":"EACCES"}`,
		"duplicate message": `{"message":"first","message":"second","errno":"EACCES"}`,
		"missing message":   `{"errno":"EACCES"}`,
		"empty message":     `{"message":"","errno":"EACCES"}`,
		"null frame":        `null`,
		"array frame":       `[]`,
		"incomplete":        `{"message":"failed","errno":`,
	} {
		t.Run(name, func(t *testing.T) {
			err := faultOf([]byte(body))
			if !errors.Is(err, syscall.EIO) || errors.Is(err, syscall.EACCES) {
				t.Fatalf("malformed fault = %v, want protocol EIO", err)
			}
			if _, ok := streamFaultErrno(err); ok {
				t.Fatal("malformed fault acquired a trusted wire errno")
			}
		})
	}
}

type unrenderableStreamPolicyError struct{}

func (unrenderableStreamPolicyError) Error() string { panic("policy error text must stay local") }

func TestStreamAuthorizationFaultUsesOnlyTrustedSafeFields(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EACCES, syscall.EIO} {
		t.Run(errno.Error(), func(t *testing.T) {
			response := httptest.NewRecorder()
			out, err := openStream(response, 1024)
			if err != nil {
				t.Fatal(err)
			}
			out.authorize = func() error { t.Fatal("terminal fault recursively asked policy"); return nil }
			out.fault(&authzFailure{errno: errno, cause: unrenderableStreamPolicyError{}})
			reader := newFrameReader(bytes.NewReader(response.Body.Bytes()), time.Second, 1024, func() { t.Error("an in-memory fault became silent") })
			var fault StreamFault
			if err := reader.decode(eventFault, &fault); err != nil {
				t.Fatal(err)
			}
			wantName, wantMessage := "EIO", "authorization failed"
			if errno == syscall.EACCES {
				wantName, wantMessage = "EACCES", "access denied"
			}
			if fault.Errno == nil || *fault.Errno != wantName || fault.Message != wantMessage {
				t.Fatalf("fault = %+v", fault)
			}
		})
	}
}

func TestStreamFaultOptionalErrnoRoundTrips(t *testing.T) {
	denied, failed := "EACCES", "EIO"
	for _, errno := range []*string{nil, &denied, &failed} {
		original := StreamFault{Message: "failure", Errno: errno}
		encoded, err := json.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		var decoded StreamFault
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.Message != original.Message || (decoded.Errno == nil) != (errno == nil) {
			t.Fatalf("decoded = %+v", decoded)
		}
		if errno != nil && *decoded.Errno != *errno {
			t.Fatalf("errno = %q, want %q", *decoded.Errno, *errno)
		}
		if errno == nil && bytes.Contains(encoded, []byte("errno")) {
			t.Fatalf("absent errno encoded as %s", encoded)
		}
	}
}
