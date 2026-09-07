package localstore

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/codetreker/remote-fs/packages/storage/objectstore/localdisk"
)

const (
	completionFilename    = "LOCALSTORE"
	completionStage       = ".LOCALSTORE.stage"
	completionVersion     = 1
	completionHeaderBytes = 40
	completionDigestBytes = sha256.Size
	completionMinBytes    = completionHeaderBytes + completionDigestBytes
	completionMaxBytes    = completionMinBytes + MaxWorkspaceBytes
)

var completionMagic = [8]byte{'R', 'F', 'S', 'L', 'O', 'C', 'A', 'L'}

func (a *rootAnchor) Completed(id localdisk.ID, workspace string) (bool, error) {
	if err := a.removeCompletionStage(); err != nil {
		return false, err
	}
	fd, err := unix.Openat(a.fd, completionFilename,
		unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		if errors.Is(err, syscall.ELOOP) {
			return false, &os.PathError{Op: "validate local store completion marker",
				Path: filepath.Join(a.path, completionFilename),
				Err:  fmt.Errorf("the marker is a symbolic link: %w", syscall.EIO)}
		}
		return false, &os.PathError{Op: "open local store completion marker",
			Path: filepath.Join(a.path, completionFilename), Err: err}
	}
	stat, validateErr := validatePrivateFile(
		fd, filepath.Join(a.path, completionFilename), a.device, a.mount, false, a.statx,
	)
	var encoded []byte
	readErr := error(nil)
	if validateErr == nil {
		if stat.Size < completionMinBytes || stat.Size > completionMaxBytes {
			validateErr = fmt.Errorf("the local store completion marker is %d bytes, want between %d and %d: %w",
				stat.Size, completionMinBytes, completionMaxBytes, syscall.EIO)
		} else {
			encoded = make([]byte, int(stat.Size))
			readErr = preadFull(fd, encoded)
		}
	}
	closeErr := unix.Close(fd)
	if err := errors.Join(validateErr, readErr, pathFailure("close local store completion marker",
		filepath.Join(a.path, completionFilename), closeErr)); err != nil {
		return false, err
	}
	if err := validateCompletion(encoded, id, workspace); err != nil {
		return false, err
	}
	return true, nil
}

func encodeCompletion(id localdisk.ID, workspace string) []byte {
	return encodeBinding(completionMagic, id, workspace)
}

func encodeBinding(magic [8]byte, id localdisk.ID, workspace string) []byte {
	encoded := make([]byte, completionMinBytes+len(workspace))
	copy(encoded[:8], magic[:])
	binary.BigEndian.PutUint16(encoded[8:10], completionVersion)
	binary.BigEndian.PutUint16(encoded[10:12], completionHeaderBytes)
	binary.BigEndian.PutUint32(encoded[12:16], uint32(len(encoded)))
	binary.BigEndian.PutUint32(encoded[16:20], uint32(len(workspace)))
	copy(encoded[24:40], id[:])
	copy(encoded[completionHeaderBytes:], workspace)
	digestOffset := completionHeaderBytes + len(workspace)
	digest := sha256.Sum256(encoded[:digestOffset])
	copy(encoded[digestOffset:], digest[:])
	return encoded
}

func validateCompletion(encoded []byte, id localdisk.ID, workspace string) error {
	return validateBinding(encoded, completionMagic, "completion marker", id, workspace)
}

func validateBinding(encoded []byte, magic [8]byte, what string, id localdisk.ID, workspace string) error {
	if len(encoded) < completionMinBytes || len(encoded) > completionMaxBytes {
		return fmt.Errorf("the local store %s has an impossible length: %w", what, syscall.EIO)
	}
	if string(encoded[:8]) != string(magic[:]) ||
		binary.BigEndian.Uint16(encoded[8:10]) != completionVersion ||
		binary.BigEndian.Uint16(encoded[10:12]) != completionHeaderBytes ||
		binary.BigEndian.Uint32(encoded[12:16]) != uint32(len(encoded)) ||
		binary.BigEndian.Uint32(encoded[20:24]) != 0 {
		return fmt.Errorf("the local store %s has an unknown header: %w", what, syscall.EIO)
	}
	workspaceBytes := int(binary.BigEndian.Uint32(encoded[16:20]))
	if workspaceBytes > MaxWorkspaceBytes || len(encoded) != completionMinBytes+workspaceBytes {
		return fmt.Errorf("the local store %s has an invalid workspace length: %w", what, syscall.EIO)
	}
	if string(encoded[24:40]) != string(id[:]) {
		return fmt.Errorf("the local store %s belongs to another object store: %w", what, syscall.EIO)
	}
	storedWorkspace := string(encoded[completionHeaderBytes : completionHeaderBytes+workspaceBytes])
	if storedWorkspace != workspace {
		return fmt.Errorf("the local store %s binds workspace %q, not %q: %w",
			what, storedWorkspace, workspace, syscall.EIO)
	}
	digestOffset := completionHeaderBytes + workspaceBytes
	digest := sha256.Sum256(encoded[:digestOffset])
	if string(encoded[digestOffset:]) != string(digest[:]) {
		return fmt.Errorf("the local store %s checksum does not match its contents: %w", what, syscall.EIO)
	}
	return nil
}

func preadFull(fd int, content []byte) error {
	for offset := int64(0); len(content) > 0; {
		read, err := unix.Pread(fd, content, offset)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		if read == 0 {
			return syscall.EIO
		}
		content = content[read:]
		offset += int64(read)
	}
	return nil
}

func (a *rootAnchor) removeCompletionStage() error {
	if err := unix.Unlinkat(a.fd, completionStage, 0); err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return nil
		}
		return &os.PathError{Op: "remove interrupted local store marker",
			Path: filepath.Join(a.path, completionStage), Err: err}
	}
	if err := unix.Fsync(a.fd); err != nil {
		return &os.PathError{Op: "sync local store root", Path: a.path, Err: err}
	}
	return nil
}

func (a *rootAnchor) PublishCompletion(id localdisk.ID, workspace string) error {
	if err := a.removeCompletionStage(); err != nil {
		return err
	}
	stagePath := filepath.Join(a.path, completionStage)
	fd, err := unix.Openat(a.fd, completionStage,
		unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
	if err != nil {
		return &os.PathError{Op: "create local store completion marker", Path: stagePath, Err: err}
	}
	encoded := encodeCompletion(id, workspace)
	writeErr := unix.Fchmod(fd, 0o600)
	if writeErr == nil {
		writeErr = writeFull(fd, encoded)
	}
	if writeErr == nil {
		writeErr = unix.Fsync(fd)
	}
	closeErr := unix.Close(fd)
	if writeErr != nil || closeErr != nil {
		cleanupErr := unix.Unlinkat(a.fd, completionStage, 0)
		if errors.Is(cleanupErr, syscall.ENOENT) {
			cleanupErr = nil
		}
		return errors.Join(
			pathFailure("write local store completion marker", stagePath, writeErr),
			pathFailure("close local store completion marker", stagePath, closeErr),
			pathFailure("remove failed local store completion marker", stagePath, cleanupErr),
		)
	}
	if err := unix.Linkat(a.fd, completionStage, a.fd, completionFilename, 0); err != nil {
		return &os.PathError{Op: "publish local store completion marker",
			Path: filepath.Join(a.path, completionFilename), Err: err}
	}
	if err := unix.Fsync(a.fd); err != nil {
		return &os.PathError{Op: "sync published local store completion marker", Path: a.path, Err: err}
	}
	if err := unix.Unlinkat(a.fd, completionStage, 0); err != nil {
		return &os.PathError{Op: "remove published local store marker stage", Path: stagePath, Err: err}
	}
	if err := unix.Fsync(a.fd); err != nil {
		return &os.PathError{Op: "sync local store marker cleanup", Path: a.path, Err: err}
	}
	return nil
}

func writeFull(fd int, content []byte) error {
	for len(content) > 0 {
		written, err := unix.Write(fd, content)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		if written == 0 {
			return syscall.EIO
		}
		content = content[written:]
	}
	return nil
}
