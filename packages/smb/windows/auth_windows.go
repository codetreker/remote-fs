//go:build windows

package windows

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"unsafe"

	"github.com/codetreker/remote-fs/packages/smb"
	win "golang.org/x/sys/windows"
)

const (
	secContinue         = 0x00090312
	secComplete         = 0x00090313
	secCompleteContinue = 0x00090314
	ascAllocateMemory   = 0x00000100
	ascConnection       = 0x00000800
	ascIntegrity        = 0x00020000
	ascNullSession      = 0x00100000
	maxSecurityToken    = 65535
)

var securityDLL = win.NewLazySystemDLL("secur32.dll")
var acquireCredentials = securityDLL.NewProc("AcquireCredentialsHandleW")
var freeCredentials = securityDLL.NewProc("FreeCredentialsHandle")
var acceptContext = securityDLL.NewProc("AcceptSecurityContext")
var completeToken = securityDLL.NewProc("CompleteAuthToken")
var deleteContext = securityDLL.NewProc("DeleteSecurityContext")
var queryContext = securityDLL.NewProc("QueryContextAttributesW")
var queryContextToken = securityDLL.NewProc("QuerySecurityContextToken")
var freeSecurityBuffer = securityDLL.NewProc("FreeContextBuffer")

type securityHandle struct{ lower, upper uintptr }
type securityBuffer struct {
	size, kind uint32
	data       unsafe.Pointer
}
type securityBuffers struct {
	version, count uint32
	buffers        *securityBuffer
}
type securityTimestamp struct {
	low  uint32
	high int32
}

func invalidSecurityHandle() securityHandle { return securityHandle{^uintptr(0), ^uintptr(0)} }
func (h securityHandle) valid() bool        { return h.lower != ^uintptr(0) && h.upper != ^uintptr(0) }

type authentication struct {
	mu                    sync.Mutex
	credentials, security securityHandle
	closed, complete      bool
	closeErr              error
}

// Pointer arguments must escape before Find can grow the Go stack. Keeping this
// annotation at the forwarding boundary preserves the native output addresses.
//
//go:uintptrescapes
func nativeSecurityCall(proc *win.LazyProc, args ...uintptr) error {
	if err := proc.Find(); err != nil {
		return err
	}
	status, _, _ := proc.Call(args...)
	if uint32(status) != 0 {
		return &SecurityError{Operation: proc.Name, Status: uint32(status)}
	}
	return nil
}

func beginAuthentication(ctx context.Context) (smb.Authentication, error) {
	a := &authentication{credentials: invalidSecurityHandle(), security: invalidSecurityHandle()}
	name, err := win.UTF16PtrFromString("Negotiate")
	if err != nil {
		return nil, err
	}
	var expiry securityTimestamp
	err = nativeSecurityCall(acquireCredentials, 0, uintptr(unsafe.Pointer(name)), 1,
		0, 0, 0, 0, uintptr(unsafe.Pointer(&a.credentials)), uintptr(unsafe.Pointer(&expiry)))
	runtime.KeepAlive(name)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return nil, errors.Join(err, a.Close())
	}
	return a, nil
}

// SSPI contexts are not safe for concurrent AcceptSecurityContext calls. Close
// shares this lock, so a canceled exchange cannot delete a context still in use.
func (a *authentication) Step(ctx context.Context, token []byte) (result smb.AuthenticationResult, returned error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.complete {
		return result, ErrAuthenticationClosed
	}
	if err := ctx.Err(); err != nil {
		return result, errors.Join(err, a.closeLocked())
	}
	if len(token) == 0 || len(token) > maxSecurityToken {
		return result, errors.Join(errors.New("Windows authentication token has invalid length"), a.closeLocked())
	}
	input := append([]byte(nil), token...)
	defer clear(input)
	in := securityBuffer{size: uint32(len(input)), kind: 2, data: unsafe.Pointer(&input[0])}
	inputBuffers := securityBuffers{count: 1, buffers: &in}
	out := securityBuffer{kind: 2}
	outputBuffers := securityBuffers{count: 1, buffers: &out}
	var previous uintptr
	if a.security.valid() {
		previous = uintptr(unsafe.Pointer(&a.security))
	}
	var attributes uint32
	if err := acceptContext.Find(); err != nil {
		return result, errors.Join(err, a.closeLocked())
	}
	status, _, _ := acceptContext.Call(uintptr(unsafe.Pointer(&a.credentials)), previous,
		uintptr(unsafe.Pointer(&inputBuffers)), ascAllocateMemory|ascConnection|ascIntegrity, 0x10,
		uintptr(unsafe.Pointer(&a.security)), uintptr(unsafe.Pointer(&outputBuffers)),
		uintptr(unsafe.Pointer(&attributes)), 0)
	runtime.KeepAlive(input)
	defer func() {
		if out.data != nil {
			returned = errors.Join(returned, nativeSecurityCall(freeSecurityBuffer, uintptr(out.data)))
		}
		if returned != nil {
			clear(result.Token)
			clear(result.SessionKey)
			result = smb.AuthenticationResult{}
			returned = errors.Join(returned, a.closeLocked())
		}
	}()
	code := uint32(status)
	if code != 0 && code != secContinue && code != secComplete && code != secCompleteContinue {
		return result, &SecurityError{Operation: "AcceptSecurityContext", Status: code}
	}
	if code == secComplete || code == secCompleteContinue {
		if err := nativeSecurityCall(completeToken, uintptr(unsafe.Pointer(&a.security)), uintptr(unsafe.Pointer(&outputBuffers))); err != nil {
			return result, err
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if out.size > maxSecurityToken || out.size != 0 && out.data == nil {
		return result, errors.New("Windows authentication returned an invalid token buffer")
	}
	if out.size != 0 {
		result.Token = append([]byte(nil), unsafe.Slice((*byte)(out.data), int(out.size))...)
	}
	result.Continue = code == secContinue || code == secCompleteContinue
	if result.Continue {
		return result, nil
	}
	if attributes&ascIntegrity == 0 || attributes&ascNullSession != 0 {
		return result, errors.New("Windows authentication did not establish signed non-anonymous access")
	}
	principal, key, err := a.identityAndKey()
	if err != nil {
		return result, err
	}
	result.Principal, result.SessionKey = principal, key
	if err := ctx.Err(); err != nil {
		return result, err
	}
	a.complete = true
	return result, nil
}

func (a *authentication) identityAndKey() (principal smb.Principal, key []byte, returned error) {
	var token win.Token
	if err := nativeSecurityCall(queryContextToken, uintptr(unsafe.Pointer(&a.security)), uintptr(unsafe.Pointer(&token))); err != nil {
		return principal, nil, err
	}
	defer func() {
		returned = errors.Join(returned, token.Close())
		if returned != nil {
			clear(key)
			key = nil
		}
	}()
	user, err := token.GetTokenUser()
	if err != nil {
		return principal, nil, err
	}
	if user.User.Sid == nil || !user.User.Sid.IsValid() || user.User.Sid.IsWellKnown(win.WinAnonymousSid) {
		return principal, nil, errors.New("Windows authentication returned no non-anonymous user")
	}
	// A local/domain Guest account may have been renamed. Its RID and Guests
	// membership are identity facts; a displayed account name is not a security check.
	sid := user.User.Sid
	count := uint32(sid.SubAuthorityCount())
	if sid.IdentifierAuthority() == (win.SidIdentifierAuthority{Value: [6]byte{0, 0, 0, 0, 0, 5}}) &&
		count >= 2 && sid.SubAuthority(0) == 21 && sid.SubAuthority(count-1) == 501 {
		return principal, nil, errors.New("Windows guest authentication is not accepted")
	}
	groups, err := token.GetTokenGroups()
	if err != nil {
		return principal, nil, err
	}
	for _, group := range groups.AllGroups() {
		if group.Sid == nil || !group.Sid.IsValid() {
			return principal, nil, errors.New("Windows authentication returned an invalid group SID")
		}
		if group.Sid.IsWellKnown(win.WinBuiltinGuestsSid) {
			return principal, nil, errors.New("Windows guest authentication is not accepted")
		}
	}
	principal.SID = sid.String()
	if !canonicalSID(principal.SID) {
		return principal, nil, ErrInvalidSID
	}
	var names struct{ name *uint16 }
	if err := nativeSecurityCall(queryContext, uintptr(unsafe.Pointer(&a.security)), 1, uintptr(unsafe.Pointer(&names))); err != nil {
		return principal, nil, err
	}
	if names.name == nil {
		return principal, nil, errors.New("Windows authentication returned no account name")
	}
	defer func() {
		returned = errors.Join(returned, nativeSecurityCall(freeSecurityBuffer, uintptr(unsafe.Pointer(names.name))))
	}()
	principal.Name = win.UTF16PtrToString(names.name)
	if principal.Name == "" {
		return principal, nil, errors.New("Windows authentication returned an empty account name")
	}
	var sessionKey struct {
		size uint32
		data *byte
	}
	if err := nativeSecurityCall(queryContext, uintptr(unsafe.Pointer(&a.security)), 9, uintptr(unsafe.Pointer(&sessionKey))); err != nil {
		return principal, nil, err
	}
	defer func() {
		if sessionKey.data != nil {
			if sessionKey.size <= 4096 {
				clear(unsafe.Slice(sessionKey.data, int(sessionKey.size)))
			}
			returned = errors.Join(returned, nativeSecurityCall(freeSecurityBuffer, uintptr(unsafe.Pointer(sessionKey.data))))
		}
	}()
	if sessionKey.size < 16 || sessionKey.size > 4096 || sessionKey.data == nil {
		return principal, nil, errors.New("Windows authentication returned no usable SMB signing key")
	}
	key = append([]byte(nil), unsafe.Slice(sessionKey.data, int(sessionKey.size))...)
	return principal, key, nil
}

func (a *authentication) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closeLocked()
}

func (a *authentication) closeLocked() error {
	if a.closed {
		return a.closeErr
	}
	a.closed = true
	if a.security.valid() {
		a.closeErr = errors.Join(a.closeErr, nativeSecurityCall(deleteContext, uintptr(unsafe.Pointer(&a.security))))
	}
	if a.credentials.valid() {
		a.closeErr = errors.Join(a.closeErr, nativeSecurityCall(freeCredentials, uintptr(unsafe.Pointer(&a.credentials))))
	}
	return a.closeErr
}

func currentUserSID() (string, error) {
	user, err := win.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", err
	}
	if user.User.Sid == nil || !user.User.Sid.IsValid() {
		return "", ErrInvalidSID
	}
	sid := user.User.Sid.String()
	if !canonicalSID(sid) {
		return "", ErrInvalidSID
	}
	return sid, nil
}
