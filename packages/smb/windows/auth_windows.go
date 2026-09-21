//go:build windows

package windows

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
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

type tokenStatistics struct {
	tokenID            win.LUID
	authenticationID   win.LUID
	expirationTime     int64
	tokenType          uint32
	impersonationLevel uint32
	dynamicCharged     uint32
	dynamicAvailable   uint32
	groupCount         uint32
	privilegeCount     uint32
	modifiedID         win.LUID
}

func invalidSecurityHandle() securityHandle { return securityHandle{^uintptr(0), ^uintptr(0)} }
func (h securityHandle) valid() bool        { return h.lower != ^uintptr(0) && h.upper != ^uintptr(0) }

type authentication struct {
	mu                    sync.Mutex
	credentials, security securityHandle
	closed, complete      bool
	pendingBuffers        []unsafe.Pointer
	pendingTokens         []win.Token
	freeBuffer            func(unsafe.Pointer) error
	closeToken            func(win.Token) error
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
		if closeErr := a.Close(); closeErr != nil {
			return a, errors.Join(err, closeErr)
		}
		return nil, err
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
	var expiry securityTimestamp
	if err := acceptContext.Find(); err != nil {
		return result, errors.Join(err, a.closeLocked())
	}
	status, _, _ := acceptContext.Call(uintptr(unsafe.Pointer(&a.credentials)), previous,
		uintptr(unsafe.Pointer(&inputBuffers)), ascAllocateMemory|ascConnection|ascIntegrity, 0x10,
		uintptr(unsafe.Pointer(&a.security)), uintptr(unsafe.Pointer(&outputBuffers)),
		uintptr(unsafe.Pointer(&attributes)), uintptr(unsafe.Pointer(&expiry)))
	runtime.KeepAlive(input)
	defer func() {
		if out.data != nil {
			if out.size <= maxSecurityToken {
				clear(unsafe.Slice((*byte)(out.data), int(out.size)))
			}
			returned = errors.Join(returned, a.releaseBuffer(out.data))
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
	expiresAt, err := securityExpiry(expiry)
	if err != nil || !expiresAt.After(time.Now()) {
		clear(key)
		if err != nil {
			return result, err
		}
		return result, errors.New("Windows authentication returned an expired security context")
	}
	result.Principal, result.SessionKey, result.ExpiresAt = principal, key, expiresAt
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
		returned = errors.Join(returned, a.releaseToken(token))
		if returned != nil {
			clear(key)
			key = nil
		}
	}()
	var err error
	principal, err = principalFromToken(token)
	if err != nil {
		return principal, nil, err
	}
	var names struct{ name *uint16 }
	if err := nativeSecurityCall(queryContext, uintptr(unsafe.Pointer(&a.security)), 1, uintptr(unsafe.Pointer(&names))); err != nil {
		return principal, nil, err
	}
	if names.name == nil {
		return principal, nil, errors.New("Windows authentication returned no account name")
	}
	defer func() {
		returned = errors.Join(returned, a.releaseBuffer(unsafe.Pointer(names.name)))
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
			returned = errors.Join(returned, a.releaseBuffer(unsafe.Pointer(sessionKey.data)))
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
	a.closed = true
	if err := a.retryTemporaryCleanup(); err != nil {
		return err
	}
	if a.security.valid() {
		if err := nativeSecurityCall(deleteContext, uintptr(unsafe.Pointer(&a.security))); err != nil {
			return err
		}
		a.security = invalidSecurityHandle()
	}
	if a.credentials.valid() {
		if err := nativeSecurityCall(freeCredentials, uintptr(unsafe.Pointer(&a.credentials))); err != nil {
			return err
		}
		a.credentials = invalidSecurityHandle()
	}
	return nil
}

func (a *authentication) releaseBuffer(buffer unsafe.Pointer) error {
	if buffer == nil {
		return nil
	}
	free := a.freeBuffer
	if free == nil {
		free = func(pointer unsafe.Pointer) error {
			return nativeSecurityCall(freeSecurityBuffer, uintptr(pointer))
		}
	}
	if err := free(buffer); err != nil {
		a.pendingBuffers = append(a.pendingBuffers, buffer)
		return err
	}
	return nil
}

func (a *authentication) releaseToken(token win.Token) error {
	closeToken := a.closeToken
	if closeToken == nil {
		closeToken = func(token win.Token) error { return token.Close() }
	}
	if err := closeToken(token); err != nil {
		a.pendingTokens = append(a.pendingTokens, token)
		return err
	}
	return nil
}

func (a *authentication) retryTemporaryCleanup() error {
	free := a.freeBuffer
	if free == nil {
		free = func(pointer unsafe.Pointer) error {
			return nativeSecurityCall(freeSecurityBuffer, uintptr(pointer))
		}
	}
	for len(a.pendingBuffers) > 0 {
		if err := free(a.pendingBuffers[0]); err != nil {
			return err
		}
		a.pendingBuffers[0] = nil
		a.pendingBuffers = a.pendingBuffers[1:]
	}
	closeToken := a.closeToken
	if closeToken == nil {
		closeToken = func(token win.Token) error { return token.Close() }
	}
	for len(a.pendingTokens) > 0 {
		if err := closeToken(a.pendingTokens[0]); err != nil {
			return err
		}
		a.pendingTokens[0] = 0
		a.pendingTokens = a.pendingTokens[1:]
	}
	return nil
}

func securityExpiry(value securityTimestamp) (time.Time, error) {
	if value.high < 0 || value.high == 0 && value.low == 0 {
		return time.Time{}, errors.New("Windows authentication returned an invalid security-context expiry")
	}
	const ticksPerSecond = int64(10_000_000)
	const windowsToUnixSeconds = int64(11_644_473_600)
	ticks := int64(uint64(uint32(value.high))<<32 | uint64(value.low))
	seconds := ticks/ticksPerSecond - windowsToUnixSeconds
	nanoseconds := ticks % ticksPerSecond * 100
	return time.Unix(seconds, nanoseconds).UTC(), nil
}

func principalFromToken(token win.Token) (smb.Principal, error) {
	user, err := token.GetTokenUser()
	if err != nil {
		return smb.Principal{}, err
	}
	if user.User.Sid == nil || !user.User.Sid.IsValid() {
		return smb.Principal{}, errors.New("Windows authentication returned an invalid user SID")
	}
	sid := user.User.Sid
	sidText := sid.String()
	if forbiddenAccountSID(sidText) {
		return smb.Principal{}, ErrIdentityNotAllowed
	}
	// A local/domain Guest account may have been renamed. Its RID and Guests
	// membership are identity facts; a displayed account name is not a security check.
	count := uint32(sid.SubAuthorityCount())
	if sid.IdentifierAuthority() == (win.SidIdentifierAuthority{Value: [6]byte{0, 0, 0, 0, 0, 5}}) &&
		count >= 2 && sid.SubAuthority(0) == 21 && sid.SubAuthority(count-1) == 501 {
		return smb.Principal{}, errors.New("Windows guest authentication is not accepted")
	}
	groups, err := token.GetTokenGroups()
	if err != nil {
		return smb.Principal{}, err
	}
	for _, group := range groups.AllGroups() {
		if group.Sid == nil || !group.Sid.IsValid() {
			return smb.Principal{}, errors.New("Windows authentication returned an invalid group SID")
		}
		if group.Sid.IsWellKnown(win.WinBuiltinGuestsSid) {
			return smb.Principal{}, errors.New("Windows guest authentication is not accepted")
		}
		if group.Sid.IsWellKnown(win.WinServiceSid) {
			return smb.Principal{}, errors.New("Windows service authentication is not accepted")
		}
	}
	principal := smb.Principal{SID: sidText}
	if !canonicalSID(principal.SID) {
		return smb.Principal{}, ErrInvalidSID
	}
	principal.LogonSession, err = tokenLogonSession(token)
	if err != nil {
		return smb.Principal{}, err
	}
	return principal, nil
}

func tokenLogonSession(token win.Token) (smb.LogonSessionID, error) {
	var statistics tokenStatistics
	var returned uint32
	if err := win.GetTokenInformation(token, win.TokenStatistics, (*byte)(unsafe.Pointer(&statistics)), uint32(unsafe.Sizeof(statistics)), &returned); err != nil {
		return "", err
	}
	if returned < uint32(unsafe.Sizeof(statistics)) {
		return "", errors.New("Windows authentication returned incomplete token statistics")
	}
	id := formatLogonSession(statistics.authenticationID)
	if !id.Valid() {
		return "", ErrInvalidLogonSession
	}
	return id, nil
}

func formatLogonSession(id win.LUID) smb.LogonSessionID {
	return smb.LogonSessionID(fmt.Sprintf("%08x%08x", uint32(id.HighPart), id.LowPart))
}

func currentIdentity() (smb.Principal, error) {
	return principalFromToken(win.GetCurrentProcessToken())
}
