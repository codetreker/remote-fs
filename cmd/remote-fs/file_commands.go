package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/internal/serviceapi"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
	"golang.org/x/sys/unix"
)

type cliEntry struct {
	RawLeaf string           `json:"rawLeafBase64"`
	Name    string           `json:"name"`
	ID      string           `json:"nodeId"`
	Kind    storage.NodeKind `json:"kind"`
	Size    string           `json:"sizeBytes"`
}

func entryDTO(entry storage.ObservedEntry) cliEntry {
	return cliEntry{base64.StdEncoding.EncodeToString(entry.RawLeaf), strconv.QuoteToASCII(string(entry.RawLeaf)), strconv.FormatUint(entry.Attr.ID, 10), entry.Attr.Kind, strconv.FormatInt(entry.Attr.Size, 10)}
}

type fileNavigator struct {
	session storage.FileSession
	names   storage.NamespaceAccess
	dirs    storage.DirectoryReader
	root    storage.Attr
}

func newNavigator(ctx context.Context, volume *httprest.Storage, options storage.FileSessionOptions) (fileNavigator, error) {
	root, err := volume.Stat(ctx, "")
	if err != nil {
		return fileNavigator{}, err
	}
	session, err := volume.NewFileSession(ctx, options)
	if err != nil {
		return fileNavigator{}, err
	}
	names, ok := session.(storage.NamespaceAccess)
	if !ok {
		session.Close(ctx)
		return fileNavigator{}, syscall.EOPNOTSUPP
	}
	dirs, ok := session.(storage.DirectoryReader)
	if !ok {
		session.Close(ctx)
		return fileNavigator{}, syscall.EOPNOTSUPP
	}
	return fileNavigator{session, names, dirs, root}, nil
}
func (n fileNavigator) resolveDirectory(ctx context.Context, raw string) (storage.DirectoryTarget, *storage.NamespaceGuards, error) {
	clean, err := storage.CleanPath(raw)
	if err != nil {
		return storage.DirectoryTarget{}, nil, err
	}
	target := storage.DirectoryTarget{NodeID: n.root.ID}
	guards := &storage.NamespaceGuards{RootID: n.root.ID}
	if clean == "" {
		return target, guards, nil
	}
	for _, leaf := range strings.Split(clean, "/") {
		capture, err := n.dirs.ReadDirNode(ctx, target)
		if err != nil {
			return storage.DirectoryTarget{}, nil, err
		}
		var selected *storage.ObservedEntry
		for i := range capture.Entries {
			if string(capture.Entries[i].RawLeaf) == leaf {
				selected = &capture.Entries[i]
				break
			}
		}
		if selected == nil {
			return storage.DirectoryTarget{}, nil, syscall.ENOENT
		}
		if selected.Attr.Kind != storage.NodeDirectory {
			return storage.DirectoryTarget{}, nil, syscall.ENOTDIR
		}
		guards.Directories = append(guards.Directories, capture.Observation)
		guards.Edges = append(guards.Edges, storage.ObservedEdge{ParentID: target.NodeID, RawLeaf: []byte(leaf), ChildID: selected.Attr.ID})
		target = storage.DirectoryTarget{NodeID: selected.Attr.ID}
	}
	return target, guards, nil
}
func (n fileNavigator) selectChild(ctx context.Context, raw string) (storage.ChildSelection, storage.ChildCondition, storage.Attr, error) {
	clean, err := storage.CleanPath(raw)
	if err != nil {
		return storage.ChildSelection{}, storage.ChildCondition{}, storage.Attr{}, err
	}
	if clean == "" {
		return storage.ChildSelection{}, storage.ChildCondition{}, storage.Attr{}, syscall.EBUSY
	}
	parent := path.Dir(clean)
	if parent == "." {
		parent = ""
	}
	target, guards, err := n.resolveDirectory(ctx, parent)
	if err != nil {
		return storage.ChildSelection{}, storage.ChildCondition{}, storage.Attr{}, err
	}
	capture, err := n.dirs.ReadDirNode(ctx, target)
	if err != nil {
		return storage.ChildSelection{}, storage.ChildCondition{}, storage.Attr{}, err
	}
	guards.Directories = append(guards.Directories, capture.Observation)
	leaf := []byte(path.Base(clean))
	selection := storage.ChildSelection{Name: storage.ChildName{Parent: target, RawLeaf: leaf}, Guards: guards}
	for _, entry := range capture.Entries {
		if string(entry.RawLeaf) == string(leaf) {
			return selection, storage.ChildCondition{State: storage.SameNode, NodeID: entry.Attr.ID}, entry.Attr, nil
		}
	}
	return selection, storage.ChildCondition{State: storage.Absent}, storage.Attr{}, nil
}
func (n fileNavigator) action(ctx context.Context) (storage.FileActionID, error) {
	status, err := n.session.Status(ctx)
	if err != nil {
		return "", err
	}
	return storage.NewFileActionID(status.ActionEpoch)
}
func fileCommand(ctx context.Context, client *serviceapi.Client, credential credentialRecord, c cliOptions, command string, args []string, replace bool, out, errOut io.Writer) (result error) {
	if c.pathBase64 {
		args = append([]string(nil), args...)
		for index, encoded := range args {
			if command == "files upload" && index == 0 || command == "files download" && index == 1 {
				continue
			}
			decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
			if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded {
				return errors.New("remote path must be canonical padded standard base64")
			}
			args[index] = string(decoded)
		}
	}
	if c.volume == "" {
		return errors.New("file commands require -volume")
	}
	address, err := client.VolumeURL(c.volume)
	if err != nil {
		return err
	}
	volume, err := httprest.Dial(address, client.HTTPClient())
	if err != nil {
		return err
	}
	if command == "files query" || command == "files resume" || command == "files cancel" {
		if len(args) != 0 {
			return errors.New("transfer recovery uses -action-file and no positional arguments")
		}
		return recoverImport(ctx, volume, credential, c, command, out)
	}
	capability, err := client.Capabilities(ctx)
	if err != nil {
		return err
	}
	c.maxTransferBytes, err = capability.MaxTransferBytes.Int64()
	if err != nil {
		return err
	}
	options, err := capability.FileSession.NativeOptions()
	if err != nil {
		return err
	}
	n, err := newNavigator(ctx, volume, options)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		result = errors.Join(result, n.session.Close(cleanup))
	}()
	switch command {
	case "files ls":
		if len(args) > 1 {
			return errors.New("ls accepts at most one relative directory path")
		}
		directory := ""
		if len(args) == 1 {
			directory = args[0]
		}
		target, _, err := n.resolveDirectory(ctx, directory)
		if err != nil {
			return err
		}
		listing, err := n.dirs.ReadDirNode(ctx, target)
		if err != nil {
			return err
		}
		entries := make([]cliEntry, 0, len(listing.Entries))
		for _, entry := range listing.Entries {
			entries = append(entries, entryDTO(entry))
		}
		return printCLI(out, c.json, entries)
	case "files mkdir", "files rm":
		if len(args) != 1 {
			return errors.New("provide exactly one relative volume path")
		}
		selected, expected, attr, err := n.selectChild(ctx, args[0])
		if err != nil {
			return err
		}
		kind := storage.NameMkdir
		if command == "files mkdir" {
			if expected.State != storage.Absent {
				return syscall.EEXIST
			}
		} else {
			if expected.State == storage.Absent {
				return syscall.ENOENT
			}
			kind = storage.NameRemove
			if attr.Kind == storage.NodeDirectory {
				kind = storage.NameRemoveDir
			}
		}
		action, err := n.action(ctx)
		if err != nil {
			return err
		}
		mutation, err := n.mutate(ctx, c.timeout, storage.NameCommand{Kind: kind, Action: action, Name: selected.Name, Target: expected})
		if err != nil {
			return fmt.Errorf("file action %s has failed or remains unknown: %w", action, err)
		}
		return printCLI(out, c.json, map[string]any{"actionId": string(action), "state": "completed", "result": mutationDTO(mutation.Attr)})
	case "files move", "files rename":
		if len(args) != 2 {
			return errors.New("provide the source and destination relative volume paths")
		}
		selected, expected, _, err := n.selectChild(ctx, args[0])
		if err != nil {
			return err
		}
		if expected.State == storage.Absent {
			return syscall.ENOENT
		}
		destination, target, _, err := n.selectChild(ctx, args[1])
		if err != nil {
			return err
		}
		if target.State != storage.Absent {
			return syscall.EEXIST
		}
		action, err := n.action(ctx)
		if err != nil {
			return err
		}
		mutation, err := n.mutate(ctx, c.timeout, storage.NameCommand{Kind: storage.NameRename, Action: action, Name: selected.Name, Target: expected, Destination: &storage.RenameTarget{Parent: destination.Name.Parent, ObservedLeaf: destination.Name.RawLeaf, OutputLeaf: destination.Name.RawLeaf, Expected: target}})
		if err != nil {
			return fmt.Errorf("file action %s has failed or remains unknown: %w", action, err)
		}
		return printCLI(out, c.json, map[string]any{"actionId": string(action), "state": "completed", "result": mutationDTO(mutation.Attr)})
	case "files upload":
		if len(args) != 2 {
			return errors.New("upload requires LOCAL_FILE and RELATIVE_VOLUME_PATH")
		}
		return uploadFile(ctx, client, volume, n, credential, c, args[0], args[1], replace, out, errOut)
	case "files download":
		if len(args) != 2 {
			return errors.New("download requires RELATIVE_VOLUME_PATH and LOCAL_FILE")
		}
		_, expected, _, err := n.selectChild(ctx, args[0])
		if err != nil {
			return err
		}
		if expected.State == storage.Absent {
			return syscall.ENOENT
		}
		return downloadFile(ctx, volume, expected.NodeID, args[1], c, out)
	}
	return errors.New("unsupported file command")
}
func uploadFile(ctx context.Context, client *serviceapi.Client, volume *httprest.Storage, n fileNavigator, credential credentialRecord, c cliOptions, local, remote string, replace bool, out, errOut io.Writer) (result error) {
	if c.actionFile == "" {
		nonce, err := randomHex()
		if err != nil {
			return err
		}
		c.actionFile = filepath.Join(filepath.Dir(c.credentials), "actions", nonce+".json")
	}
	unlock, err := lockPrivate(c.actionFile)
	if err != nil {
		return err
	}
	defer unlock()
	if c.actionFile != "" {
		if _, err := os.Lstat(c.actionFile); err == nil {
			return errors.New("upload journal exists; use files query or files resume")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	file, err := os.Open(local)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("upload source must be a regular file")
	}
	if c.maxTransferBytes <= 0 {
		return errors.New("missing negotiated transfer size limit")
	}
	if info.Size() > c.maxTransferBytes || info.Size() == math.MaxInt64 {
		return syscall.EFBIG
	}
	hash := sha256.New()
	size, err := io.CopyBuffer(hash, contextReader{ctx: ctx, reader: io.LimitReader(file, info.Size()+1)}, make([]byte, 64<<10))
	if err != nil {
		return err
	}
	if size != info.Size() {
		return errors.New("upload source changed while its content was sealed")
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	selected, expected, _, err := n.selectChild(ctx, remote)
	if err != nil {
		return err
	}
	mode := storage.ImportCreate
	if replace {
		if expected.State != storage.SameNode {
			return errors.New("-replace requires an existing destination")
		}
		mode = storage.ImportReplaceName
	} else if expected.State != storage.Absent {
		return syscall.EEXIST
	}
	journal, err := newAction(ctx, client, credential, &c, "upload", c.volume)
	if err != nil {
		return err
	}
	action, err := journal.nativeAction()
	if err != nil {
		return err
	}
	command := storage.ImportCommand{Action: action, Target: selected, Expected: expected, Mode: mode, DeclaredBytes: size}
	copy(command.Digest[:], hash.Sum(nil))
	command.Fingerprint = command.EffectFingerprint()
	journal.Import = &command
	if err = writePrivate(c.actionFile, journal); err != nil {
		return err
	}
	fmt.Fprintf(errOut, "remote-fs: durable upload journal %s\n", c.actionFile)
	transfer, err := volume.BeginImport(ctx, command)
	if err != nil {
		return fmt.Errorf("upload admission requires recovery with -action-file %s: %w", c.actionFile, err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		err := settleImport(cleanup, transfer)
		result = errors.Join(result, err)
	}()
	if _, err = transfer.Receive(ctx, file); err != nil {
		return fmt.Errorf("upload receive requires recovery with -action-file %s: %w", c.actionFile, err)
	}
	receipt, err := transfer.Publish(ctx)
	if err != nil {
		return fmt.Errorf("upload action %s remains queryable in %s: %w", journal.Nonce, c.actionFile, err)
	}
	if err = printImport(out, c.json, receipt); err != nil {
		return err
	}
	return importOutcome(receipt)
}
func recoverImport(ctx context.Context, volume *httprest.Storage, credential credentialRecord, c cliOptions, command string, out io.Writer) (result error) {
	if c.actionFile == "" {
		return errors.New("-action-file is required")
	}
	unlock, err := lockPrivate(c.actionFile)
	if err != nil {
		return err
	}
	defer unlock()
	journal, err := loadAction(c.actionFile, credential)
	if err != nil {
		return err
	}
	if journal.Kind != "upload" || journal.VolumeID != c.volume || journal.Import == nil {
		return errors.New("journal is not an upload for this volume")
	}
	action, err := journal.nativeAction()
	if err != nil {
		return err
	}
	if command == "files query" {
		receipt, err := volume.QueryImport(ctx, action)
		if err != nil {
			return err
		}
		if err = printImport(out, c.json, receipt); err != nil {
			return err
		}
		return importOutcome(receipt)
	}
	transfer, err := volume.ResumeImport(ctx, action)
	if err != nil {
		return err
	}
	if command == "files cancel" {
		settlement, err := transfer.Cancel(ctx)
		if err != nil {
			return err
		}
		return printCLI(out, c.json, settlement)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		_, err := transfer.Detach(cleanup)
		result = errors.Join(result, err)
	}()
	receipt, err := transfer.Publish(ctx)
	if err != nil {
		return err
	}
	if err = printImport(out, c.json, receipt); err != nil {
		return err
	}
	return importOutcome(receipt)
}
func importOutcome(receipt storage.ImportReceipt) error {
	switch receipt.Outcome {
	case storage.DurableActionCompleted:
		return nil
	case storage.DurableActionNotExecuted:
		if receipt.Failure != 0 {
			return receipt.Failure
		}
		return errors.New("upload was not executed")
	default:
		return fmt.Errorf("%w: upload outcome is pending or unknown; query the same action journal", errUnknownOutcome)
	}
}
func printImport(out io.Writer, json bool, receipt storage.ImportReceipt) error {
	value := map[string]any{"subjectId": receipt.Action.SubjectID, "namespace": strconv.FormatUint(receipt.Action.Namespace, 10), "nonce": receipt.Action.Nonce, "outcome": receipt.Outcome, "failure": receipt.Failure.Error()}
	if receipt.Outcome == storage.DurableActionCompleted {
		value["nodeId"] = strconv.FormatUint(receipt.Attr.ID, 10)
		value["sizeBytes"] = strconv.FormatInt(receipt.Attr.Size, 10)
	}
	return printCLI(out, json, value)
}
func downloadFile(ctx context.Context, volume storage.FileTransfer, node uint64, destination string, c cliOptions, out io.Writer) (result error) {
	if _, err := os.Lstat(destination); err == nil {
		return syscall.EEXIST
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	transfer, err := volume.ExportFile(ctx, storage.ExportCommand{NodeID: node})
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			cleanup, cancel := context.WithTimeout(context.Background(), c.timeout)
			defer cancel()
			_, err := transfer.CloseWithResult(cleanup)
			result = errors.Join(result, err)
		}
	}()
	parentFD, err := unix.Open(filepath.Dir(destination), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	parent := os.NewFile(uintptr(parentFD), filepath.Dir(destination))
	defer parent.Close()
	nonce, err := randomHex()
	if err != nil {
		return err
	}
	scratchLeaf := ".remote-fs-download-" + nonce
	if err = unix.Mkdirat(parentFD, scratchLeaf, 0700); err != nil {
		return err
	}
	scratchFD, err := unix.Openat(parentFD, scratchLeaf, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	scratch := os.NewFile(uintptr(scratchFD), scratchLeaf)
	defer scratch.Close()
	var scratchStat unix.Stat_t
	if err = unix.Fstat(scratchFD, &scratchStat); err != nil {
		return err
	}
	if scratchStat.Uid != uint32(os.Geteuid()) || scratchStat.Mode&0777 != 0700 {
		return errors.New("download staging directory was replaced")
	}
	defer func() {
		unlinkErr := unix.Unlinkat(scratchFD, "content", 0)
		if errors.Is(unlinkErr, unix.ENOENT) {
			unlinkErr = nil
		}
		var named unix.Stat_t
		statErr := unix.Fstatat(parentFD, scratchLeaf, &named, unix.AT_SYMLINK_NOFOLLOW)
		if statErr == nil && named.Dev == scratchStat.Dev && named.Ino == scratchStat.Ino {
			statErr = unix.Unlinkat(parentFD, scratchLeaf, unix.AT_REMOVEDIR)
		} else if statErr == nil {
			statErr = errors.New("download staging directory moved during cleanup")
		}
		result = errors.Join(result, unlinkErr, statErr, parent.Sync())
	}()
	fd, err := unix.Openat(scratchFD, "content", unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	temp := os.NewFile(uintptr(fd), "content")
	if err = transfer.WriteTo(ctx, temp); err != nil {
		return errors.Join(err, temp.Close())
	}
	if err = temp.Sync(); err != nil {
		return errors.Join(err, temp.Close())
	}
	if err = temp.Close(); err != nil {
		return err
	}
	info := transfer.Info()
	if !info.DigestAvailable {
		return errors.New("export did not establish a verified digest")
	}
	settlement, err := transfer.CloseWithResult(ctx)
	if err != nil {
		return err
	}
	if !settlement.Released || !settlement.Determined {
		return errors.New("export pin cleanup remains unresolved")
	}
	closed = true
	if err = unix.Renameat2(scratchFD, "content", parentFD, filepath.Base(destination), unix.RENAME_NOREPLACE); err != nil {
		return err
	}
	if err = parent.Sync(); err != nil {
		return err
	}
	return printCLI(out, c.json, map[string]any{"nodeId": strconv.FormatUint(info.Attr.ID, 10), "sizeBytes": strconv.FormatInt(info.Attr.Size, 10), "sha256": hex.EncodeToString(info.Digest[:]), "destination": destination})
}

func mutationDTO(attr *storage.Attr) any {
	if attr == nil {
		return nil
	}
	return map[string]any{"nodeId": strconv.FormatUint(attr.ID, 10), "kind": attr.Kind, "sizeBytes": strconv.FormatInt(attr.Size, 10)}
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}

func settleImport(ctx context.Context, transfer storage.FileImport) error {
	settlement, err := transfer.Detach(ctx)
	if errors.Is(err, syscall.EBUSY) && settlement.Determined && !settlement.Released {
		_, err = transfer.Cancel(ctx)
	}
	return err
}

func (n fileNavigator) mutate(ctx context.Context, timeout time.Duration, command storage.NameCommand) (storage.NameResult, error) {
	result, err := n.names.MutateName(ctx, command)
	if err == nil {
		return result, nil
	}
	if storage.ErrnoOf(err) != syscall.EIO {
		return result, err
	}
	actions, ok := n.session.(storage.FileActions)
	if !ok {
		return result, err
	}
	reconcile, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	receipt, queryErr := actions.QueryFileAction(reconcile, command.Action)
	if queryErr != nil {
		return result, errors.Join(errUnknownOutcome, err, queryErr)
	}
	if receipt.Outcome == storage.FileActionCompleted || receipt.Outcome == storage.FileActionNotExecuted {
		return n.names.MutateName(reconcile, command)
	}
	return result, errors.Join(errUnknownOutcome, err)
}
