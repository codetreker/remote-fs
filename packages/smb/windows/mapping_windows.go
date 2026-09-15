//go:build windows

package windows

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	win "golang.org/x/sys/windows"
)

var networkDLL = win.NewLazySystemDLL("mpr.dll")
var cancelConnection = networkDLL.NewProc("WNetCancelConnection2W")

type windowsMappings struct{}

func newMappingSystem() (mappingSystem, error) {
	version := win.RtlGetVersion()
	if version.MajorVersion != 10 || version.BuildNumber < 26100 || version.ProductType != 1 {
		return nil, ErrUnsupported
	}
	return windowsMappings{}, nil
}

type tokenStatistics struct {
	TokenID, AuthenticationID                                    win.LUID
	Expiration                                                   int64
	TokenType, ImpersonationLevel                                uint32
	DynamicCharged, DynamicAvailable, GroupCount, PrivilegeCount uint32
	ModifiedID                                                   win.LUID
}

func tokenIdentity(token win.Token) (logonIdentity, error) {
	var identity logonIdentity
	user, err := token.GetTokenUser()
	if err != nil {
		return identity, err
	}
	if user.User.Sid == nil || !user.User.Sid.IsValid() {
		return identity, ErrInvalidSID
	}
	identity.SID = user.User.Sid.String()
	var statistics tokenStatistics
	var length uint32
	if err := win.GetTokenInformation(token, win.TokenStatistics, (*byte)(unsafe.Pointer(&statistics)), uint32(unsafe.Sizeof(statistics)), &length); err != nil {
		return identity, err
	}
	if length != uint32(unsafe.Sizeof(statistics)) {
		return identity, errors.New("Windows token statistics has unexpected length")
	}
	identity.AuthenticationID = uint64(uint32(statistics.AuthenticationID.HighPart))<<32 | uint64(statistics.AuthenticationID.LowPart)
	if err := win.GetTokenInformation(token, win.TokenSessionId, (*byte)(unsafe.Pointer(&identity.SessionID)), 4, &length); err != nil {
		return identity, err
	}
	if length != 4 {
		return identity, errors.New("Windows token session has unexpected length")
	}
	return identity, nil
}

func (windowsMappings) identity(ctx context.Context) (logonIdentity, error) {
	if err := ctx.Err(); err != nil {
		return logonIdentity{}, err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	process, err := tokenIdentity(win.GetCurrentProcessToken())
	if err != nil {
		return process, err
	}
	thread, err := tokenIdentity(win.GetCurrentThreadEffectiveToken())
	if err != nil {
		return process, err
	}
	if process != thread {
		return process, ErrMappingOwnership
	}
	return process, nil
}

func deviceTarget(local string) (string, bool, error) {
	name, err := win.UTF16PtrFromString(local)
	if err != nil {
		return "", false, err
	}
	for size := 256; size <= 32768; size *= 2 {
		buffer := make([]uint16, size)
		n, err := win.QueryDosDevice(name, &buffer[0], uint32(len(buffer)))
		if err == win.ERROR_FILE_NOT_FOUND {
			return "", false, nil
		}
		if err == win.ERROR_INSUFFICIENT_BUFFER {
			continue
		}
		if err != nil {
			return "", false, err
		}
		return win.UTF16ToString(buffer[:n]), true, nil
	}
	return "", false, errors.New("Windows DOS-device mapping exceeds its query bound")
}

func (w windowsMappings) query(ctx context.Context, owner logonIdentity, local string) (mappingRecord, bool, error) {
	reply, err := runMappingCommand(ctx, mappingCommand{Operation: "query", LocalPath: local, OwnerSID: owner.SID, SessionID: owner.SessionID})
	if err != nil {
		return mappingRecord{}, false, err
	}
	device, exists, err := deviceTarget(local)
	if err != nil {
		return mappingRecord{}, false, err
	}
	if !*reply.Found {
		return mappingRecord{LocalPath: local, Device: device}, exists, nil
	}
	if !exists {
		return mappingRecord{}, false, ErrMappingOwnership
	}
	record := *reply.Record
	record.Device = device
	return record, true, nil
}

func (w windowsMappings) create(ctx context.Context, owner logonIdentity, options MappingOptions) (mappingRecord, bool, error) {
	current, err := w.identity(ctx)
	if err != nil {
		return mappingRecord{}, false, err
	}
	if current != owner {
		return mappingRecord{}, false, ErrMappingOwnership
	}
	if _, occupied, err := deviceTarget(options.LocalPath); err != nil {
		return mappingRecord{}, false, err
	} else if occupied {
		return mappingRecord{}, false, ErrMappingBusy
	}
	reply, err := runMappingCommand(ctx, mappingCommand{Operation: "create", LocalPath: options.LocalPath, RemotePath: options.remotePath(), TCPPort: options.TCPPort, OwnerSID: owner.SID, SessionID: owner.SessionID})
	created := reply.Created != nil && *reply.Created
	var record mappingRecord
	if reply.Record != nil {
		record = *reply.Record
	}
	if created {
		var deviceErr error
		record.Device, _, deviceErr = deviceTarget(options.LocalPath)
		err = errors.Join(err, deviceErr)
	}
	return record, created, err
}

func (w windowsMappings) remove(ctx context.Context, owner logonIdentity, local string, force bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	current, err := w.identity(ctx)
	if err != nil {
		return err
	}
	if current != owner {
		return ErrMappingOwnership
	}
	name, err := win.UTF16PtrFromString(local)
	if err != nil {
		return err
	}
	if err := cancelConnection.Find(); err != nil {
		return err
	}
	var forced uintptr
	if force {
		forced = 1
	}
	status, _, _ := cancelConnection.Call(uintptr(unsafe.Pointer(name)), 0, forced)
	runtime.KeepAlive(name)
	if status == 0 {
		return nil
	}
	native := syscall.Errno(uint32(status))
	if native == win.ERROR_OPEN_FILES || native == win.ERROR_DEVICE_IN_USE {
		return errors.Join(ErrMappingBusy, native)
	}
	return fmt.Errorf("Windows mapping removal failed: %w", native)
}

type boundedCommandOutput struct {
	bytes.Buffer
	overflow bool
}

func (b *boundedCommandOutput) Write(data []byte) (int, error) {
	if len(data) > 65536-b.Len() {
		b.overflow = true
		return 0, errors.New("Windows mapping response exceeds its bound")
	}
	return b.Buffer.Write(data)
}

func runMappingCommand(ctx context.Context, request mappingCommand) (mappingReply, error) {
	var reply mappingReply
	if err := ctx.Err(); err != nil {
		return reply, err
	}
	directory, err := win.GetSystemDirectory()
	if err != nil {
		return reply, err
	}
	input, err := json.Marshal(request)
	if err != nil {
		return reply, err
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(bounded, filepath.Join(directory, "WindowsPowerShell", "v1.0", "powershell.exe"),
		"-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", encodedMappingScript())
	command.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	command.WaitDelay = time.Second
	command.Stdin = bytes.NewReader(input)
	var output boundedCommandOutput
	command.Stdout = &output
	// Native failures are returned as numeric structured status. PowerShell error
	// rendering can include caller values, so it never enters returned diagnostics.
	command.Stderr = io.Discard
	if err := command.Start(); err != nil {
		return reply, err
	}
	commandErr := command.Wait()
	if output.overflow {
		commandErr = errors.Join(commandErr, ErrMappingVerification)
	}
	reply, decodeErr := decodeMappingReply(output.Bytes())
	if decodeErr != nil {
		if request.Operation == "create" {
			return mappingReply{}, errors.Join(ErrMappingUncertain, commandErr, bounded.Err(), decodeErr)
		}
		return mappingReply{}, errors.Join(ErrMappingVerification, commandErr, bounded.Err(), decodeErr)
	}
	var operationErr error
	switch reply.Error {
	case "":
	case "owner":
		operationErr = ErrMappingOwnership
	case "busy":
		operationErr = ErrMappingBusy
	case "verification":
		operationErr = ErrMappingVerification
	case "native":
		operationErr = fmt.Errorf("Windows SMB mapping command failed with HRESULT 0x%08x", reply.Code)
	default:
		operationErr = ErrMappingVerification
	}
	return reply, errors.Join(operationErr, commandErr, bounded.Err())
}
