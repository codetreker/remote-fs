package windows

import (
	"context"
	"crypto/subtle"
	"errors"
	"runtime"
	"testing"
	"time"
	"unsafe"

	win "golang.org/x/sys/windows"
)

func TestNativeSSPINegotiateAuthenticatesCurrentWindowsIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	server, err := NewAuthenticator().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
	client := &authentication{credentials: invalidSecurityHandle(), security: invalidSecurityHandle()}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	name, err := win.UTF16PtrFromString("Negotiate")
	if err != nil {
		t.Fatal(err)
	}
	var expiry securityTimestamp
	if err := nativeSecurityCall(acquireCredentials, 0, uintptr(unsafe.Pointer(name)), 2, 0, 0, 0, 0, uintptr(unsafe.Pointer(&client.credentials)), uintptr(unsafe.Pointer(&expiry))); err != nil {
		t.Fatal(err)
	}
	runtime.KeepAlive(name)
	input, complete := nativeClientStep(t, client, nil)
	for attempt := 0; attempt < 8; attempt++ {
		result, err := server.Step(ctx, input)
		clear(input)
		if err != nil {
			t.Fatal(err)
		}
		if result.Continue {
			input, complete = nativeClientStep(t, client, result.Token)
			clear(result.Token)
			continue
		}
		defer clear(result.SessionKey)
		if len(result.Token) > 0 {
			final, done := nativeClientStep(t, client, result.Token)
			clear(result.Token)
			if len(final) != 0 || !done {
				clear(final)
				t.Fatal("SSPI final server token did not complete client authentication")
			}
			complete = done
		}
		if !complete {
			t.Fatal("SSPI client did not finish")
		}
		want, err := CurrentUserSID()
		if err != nil {
			t.Fatal(err)
		}
		if result.Principal.SID != want || result.Principal.Name == "" {
			t.Fatal("SSPI principal does not identify the actual Windows user")
		}
		var key struct {
			size uint32
			data *byte
		}
		if err := nativeSecurityCall(queryContext, uintptr(unsafe.Pointer(&client.security)), 9, uintptr(unsafe.Pointer(&key))); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if key.data != nil {
				if key.size <= 4096 {
					clear(unsafe.Slice(key.data, int(key.size)))
				}
				if err := nativeSecurityCall(freeSecurityBuffer, uintptr(unsafe.Pointer(key.data))); err != nil {
					t.Error(err)
				}
			}
		}()
		if key.data == nil || key.size < 16 || key.size > 4096 {
			t.Fatal("client SSPI signing key is invalid")
		}
		clientKey := unsafe.Slice(key.data, int(key.size))
		equal := subtle.ConstantTimeCompare(result.SessionKey, clientKey) == 1
		if !equal {
			t.Fatal("client and server SSPI signing keys differ")
		}
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := server.Step(ctx, []byte{1}); !errors.Is(err, ErrAuthenticationClosed) {
			t.Fatalf("closed exchange accepted another step: %v", err)
		}
		return
	}
	clear(input)
	t.Fatal("SSPI negotiation exceeded eight exchanges")
}

func nativeClientStep(t *testing.T, a *authentication, input []byte) ([]byte, bool) {
	t.Helper()
	proc := securityDLL.NewProc("InitializeSecurityContextW")
	if err := proc.Find(); err != nil {
		t.Fatal(err)
	}
	target, err := win.UTF16PtrFromString("cifs/127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	var previous uintptr
	var inputs *securityBuffers
	if a.security.valid() {
		previous = uintptr(unsafe.Pointer(&a.security))
	}
	var in securityBuffer
	var buffers securityBuffers
	if len(input) > 0 {
		in = securityBuffer{size: uint32(len(input)), kind: 2, data: unsafe.Pointer(&input[0])}
		buffers = securityBuffers{count: 1, buffers: &in}
		inputs = &buffers
	}
	out := securityBuffer{kind: 2}
	outputs := securityBuffers{count: 1, buffers: &out}
	var attributes uint32
	status, _, _ := proc.Call(uintptr(unsafe.Pointer(&a.credentials)), previous, uintptr(unsafe.Pointer(target)),
		ascAllocateMemory|ascConnection|0x00010000, 0, 0x10, uintptr(unsafe.Pointer(inputs)), 0,
		uintptr(unsafe.Pointer(&a.security)), uintptr(unsafe.Pointer(&outputs)), uintptr(unsafe.Pointer(&attributes)), 0)
	runtime.KeepAlive(input)
	runtime.KeepAlive(target)
	defer func() {
		if out.data != nil {
			if err := nativeSecurityCall(freeSecurityBuffer, uintptr(out.data)); err != nil {
				t.Error(err)
			}
		}
	}()
	code := uint32(status)
	if code != 0 && code != secContinue && code != secComplete && code != secCompleteContinue {
		t.Fatal(&SecurityError{Operation: "InitializeSecurityContext", Status: code})
	}
	if code == secComplete || code == secCompleteContinue {
		if err := nativeSecurityCall(completeToken, uintptr(unsafe.Pointer(&a.security)), uintptr(unsafe.Pointer(&outputs))); err != nil {
			t.Fatal(err)
		}
	}
	if out.size > maxSecurityToken || out.size > 0 && out.data == nil {
		t.Fatal("client SSPI output is invalid")
	}
	var token []byte
	if out.size > 0 {
		token = append([]byte(nil), unsafe.Slice((*byte)(out.data), int(out.size))...)
	}
	complete := code == 0 || code == secComplete
	if complete && attributes&0x00010000 == 0 {
		clear(token)
		t.Fatal("client SSPI context has no integrity")
	}
	return token, complete
}

func TestNativeSSPICancellationRetiresItsContext(t *testing.T) {
	exchange, err := NewAuthenticator().Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := exchange.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := exchange.Step(ctx, []byte{1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Step = %v", err)
	}
	if _, err := exchange.Step(t.Context(), []byte{1}); !errors.Is(err, ErrAuthenticationClosed) {
		t.Fatalf("canceled exchange survived: %v", err)
	}
}
