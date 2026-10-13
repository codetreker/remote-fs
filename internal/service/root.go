package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
	"golang.org/x/sys/unix"
)

var ErrRootBusy = errors.New("service state root is owned by another process")

type stateRoot struct {
	Path           string
	DataPath       string
	ManagementPath string
	VolumesPath    string
	AnchorID       management.ID
	DatabaseID     management.ID
	root           *os.File
	data           *os.File
	anchor         *os.File
	directories    []*os.File
	closeMu        sync.Mutex
	closed         bool
	closeErr       error
}

type rootProof struct {
	Version    int           `json:"version"`
	StateRoot  string        `json:"state_root"`
	AnchorID   management.ID `json:"anchor_id"`
	DatabaseID management.ID `json:"database_id"`
}

// Every process acquires the same private anchor before inspecting service data.
func openRoot(ctx context.Context, config Config, create bool) (_ *stateRoot, returned error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r := &stateRoot{Path: config.StateRoot, DataPath: filepath.Join(config.StateRoot, "data")}
	r.ManagementPath = filepath.Join(r.DataPath, "management.sqlite")
	r.VolumesPath = filepath.Join(r.DataPath, "volumes")
	defer func() {
		if returned != nil {
			returned = errors.Join(returned, r.Close())
		}
	}()
	var err error
	r.root, err = openTrustedRoot(r.Path, create)
	if err != nil {
		return nil, err
	}
	r.directories = append(r.directories, r.root)
	flags := unix.O_RDWR | unix.O_NOFOLLOW | unix.O_CLOEXEC | unix.O_NONBLOCK
	if create {
		flags |= unix.O_CREAT
	}
	fd, err := unix.Openat(int(r.root.Fd()), "service.lock", flags, 0600)
	if err != nil {
		return nil, fmt.Errorf("open stable service anchor: %w", err)
	}
	r.anchor = os.NewFile(uintptr(fd), filepath.Join(r.Path, "service.lock"))
	if err := requirePrivateFile(r.anchor, "service anchor"); err != nil {
		return nil, err
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrRootBusy, r.Path)
		}
		return nil, fmt.Errorf("lock stable service anchor: %w", err)
	}
	if create {
		if err := errors.Join(r.anchor.Sync(), r.root.Sync()); err != nil {
			return nil, fmt.Errorf("persist stable service anchor: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.readAnchor(create); err != nil {
		return nil, err
	}
	if create && r.DatabaseID != "" {
		return nil, errors.New("stable root is already initialized")
	}
	if err := r.openData(create); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *stateRoot) openData(create bool) error {
	var err error
	r.data, err = openPrivateDirectory(r.root, "data", create)
	if err != nil {
		return err
	}
	r.directories = append(r.directories, r.data)
	for _, name := range []string{"volumes"} {
		directory, err := openPrivateDirectory(r.data, name, create)
		if err != nil {
			return err
		}
		r.directories = append(r.directories, directory)
	}
	if create {
		if err := r.requireFresh(); err != nil {
			return err
		}
	} else if err := r.readProof(); err != nil {
		return err
	}
	return nil
}

func (r *stateRoot) readAnchor(create bool) error {
	content, err := io.ReadAll(io.NewSectionReader(r.anchor, 0, 4097))
	if err != nil {
		return err
	}
	if create && len(content) == 0 {
		r.AnchorID, err = management.NewID()
		return err
	}
	var proof rootProof
	if err := json.Unmarshal(content, &proof); err != nil {
		return fmt.Errorf("decode stable anchor identity: %w", err)
	}
	canonical, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	canonical = append(canonical, '\n')
	if len(content) > 4096 || string(content) != string(canonical) || proof.Version != 1 || proof.StateRoot != r.Path || !proof.AnchorID.Valid() || !proof.DatabaseID.Valid() {
		return fmt.Errorf("stable anchor identity has an unknown binding: %w", management.ErrCorrupt)
	}
	r.AnchorID, r.DatabaseID = proof.AnchorID, proof.DatabaseID
	return nil
}

func openTrustedRoot(path string, create bool) (_ *os.File, returned error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	current := os.NewFile(uintptr(fd), "/")
	defer func() {
		if returned != nil {
			returned = errors.Join(returned, current.Close())
		}
	}()
	components := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for i, name := range components {
		if err := requireTrustedAncestor(current); err != nil {
			return nil, err
		}
		final := i == len(components)-1
		if final && create {
			if err := unix.Mkdirat(int(current.Fd()), name, 0700); err != nil && !errors.Is(err, unix.EEXIST) {
				return nil, fmt.Errorf("create private state root: %w", err)
			}
			if err := current.Sync(); err != nil {
				return nil, err
			}
		}
		nextFD, err := unix.Openat(int(current.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, fmt.Errorf("open trusted state root component %q: %w", name, err)
		}
		next := os.NewFile(uintptr(nextFD), filepath.Join(current.Name(), name))
		if err := current.Close(); err != nil {
			current = next
			return nil, err
		}
		current = next
	}
	if err := requirePrivateDirectory(current); err != nil {
		return nil, err
	}
	return current, nil
}

func requireTrustedAncestor(directory *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &stat); err != nil {
		return err
	}
	uid := uint32(os.Geteuid())
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || (stat.Uid != 0 && stat.Uid != uid) {
		return fmt.Errorf("state root ancestor is not controlled by the service or root: %s", directory.Name())
	}
	// A root-owned sticky directory prevents other users from replacing the
	// service-owned private child; this admits /tmp without admitting writable
	// deployment ancestors that permit arbitrary rename or replacement.
	if stat.Mode&0022 != 0 && !(stat.Uid == 0 && stat.Mode&unix.S_ISVTX != 0) {
		return fmt.Errorf("state root ancestor permits untrusted modification: %s", directory.Name())
	}
	return nil
}

func requirePrivateDirectory(directory *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &stat); err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&07777 != 0700 || stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("service directory must be owned by the service with mode 0700: %s", directory.Name())
	}
	return nil
}

func requirePrivateFile(file *os.File, kind string) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&07777 != 0600 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 {
		return fmt.Errorf("%s must be a service-owned regular file with mode 0600 and one link", kind)
	}
	return nil
}

func openPrivateDirectory(parent *os.File, name string, create bool) (_ *os.File, returned error) {
	if create {
		if err := unix.Mkdirat(int(parent.Fd()), name, 0700); err != nil && !errors.Is(err, unix.EEXIST) {
			return nil, fmt.Errorf("create private directory %s: %w", name, err)
		}
		if err := parent.Sync(); err != nil {
			return nil, err
		}
	}
	fd, err := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open private directory %s: %w", name, err)
	}
	directory := os.NewFile(uintptr(fd), filepath.Join(parent.Name(), name))
	defer func() {
		if returned != nil {
			returned = errors.Join(returned, directory.Close())
		}
	}()
	if err := requirePrivateDirectory(directory); err != nil {
		return nil, err
	}
	var parentStat, childStat unix.Stat_t
	if err := unix.Fstat(int(parent.Fd()), &parentStat); err != nil {
		return nil, err
	}
	if err := unix.Fstat(fd, &childStat); err != nil {
		return nil, err
	}
	if parentStat.Dev != childStat.Dev {
		return nil, fmt.Errorf("service directory crosses backing filesystems: %w", syscall.EXDEV)
	}
	return directory, nil
}

func (r *stateRoot) requireFresh() error {
	entries, err := r.data.ReadDir(-1)
	if err != nil {
		return err
	}
	if len(entries) != 1 {
		return errors.New("initialization requires an empty data root; existing state requires maintenance")
	}
	for _, entry := range entries {
		if entry.Name() != "volumes" {
			return errors.New("initialization cannot adopt existing or unknown data")
		}
	}
	for _, directory := range r.directories[2:] {
		entries, err := directory.ReadDir(-1)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			return errors.New("initialization cannot adopt existing volume state")
		}
	}
	return nil
}

func (r *stateRoot) readProof() (returned error) {
	fd, err := unix.Openat(int(r.data.Fd()), "root.json", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("read durable state-root identity: %w", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(r.DataPath, "root.json"))
	defer func() { returned = errors.Join(returned, file.Close()) }()
	if err := requirePrivateFile(file, "state-root identity"); err != nil {
		return err
	}
	content, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return err
	}
	if len(content) > 4096 {
		return fmt.Errorf("state-root identity exceeds its bound: %w", management.ErrCorrupt)
	}
	var proof rootProof
	if err := json.Unmarshal(content, &proof); err != nil {
		return fmt.Errorf("decode state-root identity: %w", err)
	}
	canonical, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	canonical = append(canonical, '\n')
	if string(content) != string(canonical) || proof.Version != 1 || proof.StateRoot != r.Path || proof.AnchorID != r.AnchorID || proof.DatabaseID != r.DatabaseID {
		return fmt.Errorf("state-root identity is noncanonical or has an unknown binding: %w", management.ErrCorrupt)
	}
	return nil
}

func (r *stateRoot) BindDatabase(id management.ID) (returned error) {
	if !id.Valid() || r.DatabaseID != "" {
		return fmt.Errorf("cannot replace an established state-root identity: %w", management.ErrCorrupt)
	}
	content, err := json.Marshal(rootProof{Version: 1, StateRoot: r.Path, AnchorID: r.AnchorID, DatabaseID: id})
	if err != nil {
		return err
	}
	content = append(content, '\n')
	if len(content) > 4096 {
		return errors.New("state-root identity exceeds its durable evidence bound")
	}
	fd, err := unix.Openat(int(r.data.Fd()), "root.json", unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return fmt.Errorf("create durable state-root identity: %w", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(r.DataPath, "root.json"))
	defer func() { returned = errors.Join(returned, file.Close()) }()
	if err := requirePrivateFile(file, "state-root identity"); err != nil {
		return err
	}
	if _, err := file.Write(content); err != nil {
		return err
	}
	if err := errors.Join(file.Sync(), r.data.Sync()); err != nil {
		return fmt.Errorf("persist state-root identity: %w", err)
	}
	info, err := r.anchor.Stat()
	if err != nil {
		return err
	}
	if info.Size() != 0 {
		return fmt.Errorf("cannot rewrite the established stable anchor: %w", management.ErrCorrupt)
	}
	if _, err := r.anchor.WriteAt(content, 0); err != nil {
		return err
	}
	if err := errors.Join(r.anchor.Sync(), r.root.Sync()); err != nil {
		return fmt.Errorf("persist stable anchor identity: %w", err)
	}
	r.DatabaseID = id
	return nil
}

func (r *stateRoot) Close() error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closed {
		return r.closeErr
	}
	for i := len(r.directories) - 1; i >= 0; i-- {
		r.closeErr = errors.Join(r.closeErr, r.directories[i].Close())
	}
	if r.anchor != nil {
		r.closeErr = errors.Join(r.closeErr, unix.Flock(int(r.anchor.Fd()), unix.LOCK_UN), r.anchor.Close())
	}
	r.closed = true
	return r.closeErr
}

func sameOpenedFile(a, b *os.File) (bool, error) {
	var first, second unix.Stat_t
	if err := unix.Fstat(int(a.Fd()), &first); err != nil {
		return false, err
	}
	if err := unix.Fstat(int(b.Fd()), &second); err != nil {
		return false, err
	}
	return first.Dev == second.Dev && first.Ino == second.Ino, nil
}

func (r *stateRoot) validateAnchor() (returned error) {
	current, err := openTrustedRoot(r.Path, false)
	if err != nil {
		return err
	}
	defer func() { returned = errors.Join(returned, current.Close()) }()
	same, err := sameOpenedFile(current, r.root)
	if err != nil {
		return err
	}
	if !same {
		return fmt.Errorf("stable state root was replaced: %w", management.ErrCorrupt)
	}
	fd, err := unix.Openat(int(current.Fd()), "service.lock", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	anchor := os.NewFile(uintptr(fd), filepath.Join(r.Path, "service.lock"))
	defer func() { returned = errors.Join(returned, anchor.Close()) }()
	if err := requirePrivateFile(anchor, "service anchor"); err != nil {
		return err
	}
	same, err = sameOpenedFile(anchor, r.anchor)
	if err != nil {
		return err
	}
	if !same {
		return fmt.Errorf("stable service anchor was replaced: %w", management.ErrCorrupt)
	}
	if r.DatabaseID == "" {
		info, err := anchor.Stat()
		if err != nil {
			return err
		}
		if info.Size() != 0 {
			return fmt.Errorf("uninitialized stable anchor was modified: %w", management.ErrCorrupt)
		}
		return nil
	}
	currentProof := &stateRoot{Path: r.Path, anchor: anchor}
	if err := currentProof.readAnchor(false); err != nil {
		return err
	}
	if currentProof.AnchorID != r.AnchorID || currentProof.DatabaseID != r.DatabaseID {
		return fmt.Errorf("stable service anchor identity was modified: %w", management.ErrCorrupt)
	}
	return nil
}
