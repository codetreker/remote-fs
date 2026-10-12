package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	passwordinput "github.com/codetreker/remote-fs/cmd/internal/password"
	"github.com/codetreker/remote-fs/internal/serviceapi"
)

var errUnknownOutcome = errors.New("result_unknown")

func run(args []string, errOut io.Writer) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return runLegacy(args, errOut)
	}
	return runServiceCommand(args, os.Stdin, os.Stdout, errOut)
}

type cliOptions struct {
	maxTransferBytes                        int64
	server, credentials, volume, actionFile string
	json, insecure, noWait, pathBase64      bool
	timeout                                 time.Duration
}

func commonFlags(f *flag.FlagSet, c *cliOptions) {
	f.StringVar(&c.server, "server", "", "service origin")
	f.StringVar(&c.credentials, "credential-file", defaultCredentialFile, "private credential file")
	f.StringVar(&c.volume, "volume", "", "immutable volume ID")
	f.StringVar(&c.actionFile, "action-file", "", "private durable action journal; reuse it to recover the same action")
	f.BoolVar(&c.json, "json", false, "write structured output with exact decimal strings")
	f.BoolVar(&c.pathBase64, "path-base64", false, "decode remote path arguments from canonical padded standard base64")
	f.BoolVar(&c.insecure, "allow-insecure-http", false, "allow HTTP for an explicit development connection")
	f.BoolVar(&c.noWait, "no-wait", false, "report accepted management action without waiting for completion")
	f.DurationVar(&c.timeout, "timeout", 30*time.Second, "maximum time for one HTTP exchange")
}
func parseCLI(f *flag.FlagSet, args []string) error {
	if err := f.Parse(interspersedFlags(f, args)); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return flag.ErrHelp
		}
		return errUsage
	}
	return nil
}
func interspersedFlags(f *flag.FlagSet, args []string) []string {
	options := make([]string, 0, len(args))
	positionals := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if argument == "--" {
			positionals = append(positionals, args[index+1:]...)
			break
		}
		if argument == "-" || !strings.HasPrefix(argument, "-") {
			positionals = append(positionals, argument)
			continue
		}
		options = append(options, argument)
		name, _, assigned := strings.Cut(strings.TrimLeft(argument, "-"), "=")
		option := f.Lookup(name)
		if option == nil || assigned {
			continue
		}
		boolean, ok := option.Value.(interface{ IsBoolFlag() bool })
		if ok && boolean.IsBoolFlag() {
			continue
		}
		if index+1 < len(args) {
			index++
			options = append(options, args[index])
		}
	}
	return append(append(options, "--"), positionals...)
}

func printCLI(out io.Writer, jsonOutput bool, value any) error {
	if jsonOutput {
		return json.NewEncoder(out).Encode(value)
	}
	encoded, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(encoded))
	return err
}
func serviceClient(c cliOptions) (*serviceapi.Client, credentialRecord, error) {
	record, err := loadCredential(c.credentials)
	if err != nil {
		return nil, record, err
	}
	server := record.Server
	if c.server != "" {
		server, err = validateServiceURL(c.server, c.insecure)
		if err != nil {
			return nil, record, err
		}
	}
	if server != record.Server {
		return nil, record, errors.New("-server does not match the credential file")
	}
	if c.timeout <= 0 {
		return nil, record, errors.New("-timeout must be positive")
	}
	client, err := serviceapi.NewClient(server, callerClient(c.timeout), serviceapi.Credential{ID: record.ID, Secret: record.Secret})
	return client, record, err
}
func runServiceCommand(args []string, in io.Reader, out, errOut io.Writer) error {
	command := args[0]
	args = args[1:]
	if command == "help" {
		_, err := fmt.Fprintln(out, "usage: remote-fs <login|logout|status|scope|rotate|revoke|volume|user|operation|files|mount> [flags]\nUse <command> -h for flags. Volume and file commands take a subcommand before flags.")
		return err
	}
	if command == "volume" || command == "operation" || command == "files" || command == "user" {
		if len(args) == 0 {
			return fmt.Errorf("%s requires a subcommand", command)
		}
		command += " " + args[0]
		args = args[1:]
	}
	f := flag.NewFlagSet("remote-fs "+command, flag.ContinueOnError)
	f.SetOutput(errOut)
	var c cliOptions
	commonFlags(f, &c)
	var login, name, quota, passwordFile, label, scopes, volumeIDs, mountpoint string
	var passwordStdin, allOwned, replace bool
	f.StringVar(&name, "name", "", "volume display name")
	f.StringVar(&quota, "quota", "", "quota in bytes or SIZE (for example 10GiB)")
	switch command {
	case "login":
		f.StringVar(&login, "login", "", "administrator login name")
		f.StringVar(&label, "label", "CLI", "credential label")
		f.StringVar(&passwordFile, "password-file", "", "private 0600 file containing the password")
		f.BoolVar(&passwordStdin, "password-stdin", false, "read the password from standard input")
		f.StringVar(&scopes, "scopes", "", "comma-separated explicit credential scopes")
		f.StringVar(&volumeIDs, "volume-ids", "", "comma-separated explicit volume IDs")
		f.BoolVar(&allOwned, "all-owned-volumes", false, "allow explicit scopes on all owned volumes")
	case "mount":
		f.StringVar(&mountpoint, "mountpoint", "", "existing directory to mount at")
	case "files upload":
		f.BoolVar(&replace, "replace", false, "replace the exactly observed existing name")
	}
	if err := parseCLI(f, args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	positional := f.Args()
	if c.timeout <= 0 {
		return errors.New("-timeout must be positive")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if command == "login" {
		if len(positional) != 0 || login == "" || c.server == "" {
			return errors.New("login requires -server and -login, with no positional arguments")
		}
		if passwordStdin && passwordFile != "" {
			return errors.New("choose at most one of -password-stdin and -password-file; otherwise the password is read from TTY")
		}
		server, err := validateServiceURL(c.server, c.insecure)
		if err != nil {
			return err
		}
		unlock, err := lockPrivate(c.credentials)
		if err != nil {
			return err
		}
		defer unlock()
		if _, err = os.Lstat(c.credentials); err == nil {
			return errors.New("credential file already exists; revoke it before logging in again")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		var password string
		switch {
		case passwordFile != "":
			password, err = passwordinput.ReadPrivateFile(ctx, passwordFile, 1024)
		case passwordStdin:
			password, err = passwordinput.ReadLine(in, 1024)
		default:
			password, err = readTTYPassword(ctx, errOut)
		}

		if err != nil {
			return err
		}
		client, err := serviceapi.NewClient(server, callerClient(c.timeout), serviceapi.Credential{})
		if err != nil {
			return err
		}
		request := serviceapi.CreateCredentialRequest{LoginName: login, Password: password, Label: label}
		if scopes != "" {
			request.Scopes = strings.Split(scopes, ",")
			request.AllOwnedVolumes = &allOwned
			if volumeIDs != "" {
				request.VolumeIDs = strings.Split(volumeIDs, ",")
			}
		} else if allOwned || volumeIDs != "" {
			return errors.New("volume selection requires explicit -scopes")
		}
		result, err := client.Login(ctx, request)
		if err != nil {
			return err
		}
		record := credentialRecord{Version: 1, Server: server, ID: result.Credential.ID, Secret: result.Secret, SubjectID: result.User.ID, Generation: string(result.Credential.Generation)}
		if err = writePrivate(c.credentials, record); err != nil {
			return err
		}
		return printCLI(out, c.json, result.Credential)
	}
	client, record, err := serviceClient(c)
	if err != nil {
		return err
	}
	switch command {
	case "status", "scope":
		if len(positional) != 0 {
			return errors.New("unexpected positional arguments")
		}
		result, err := client.Credential(ctx, record.ID)
		if err != nil {
			return err
		}
		return printCLI(out, c.json, result)
	case "rotate":
		return rotateCredential(ctx, client, record, c, out)
	case "logout", "revoke":
		if len(positional) > 1 {
			return errors.New("revoke accepts at most one credential ID")
		}
		id := record.ID
		if len(positional) == 1 {
			id = positional[0]
		}
		unlock, err := lockPrivate(c.credentials)
		if err != nil {
			return err
		}
		defer unlock()
		if err = client.RevokeCredential(ctx, id); err != nil {
			return err
		}
		if id == record.ID {
			if err = os.Remove(c.credentials); err != nil {
				return err
			}
			if err = syncParent(c.credentials); err != nil {
				return err
			}
		}
		return printCLI(out, c.json, map[string]string{"credentialId": id, "state": "revoked"})
	case "mount":
		if c.volume == "" || mountpoint == "" || len(positional) != 0 {
			return errors.New("mount requires -volume and -mountpoint")
		}
		address, err := client.VolumeURL(c.volume)
		if err != nil {
			return err
		}
		return runLegacy([]string{"-server", address, "-mountpoint", mountpoint, "-credential-file", c.credentials, "-timeout", c.timeout.String()}, errOut)
	case "user list":
		if len(positional) != 0 {
			return errors.New("user list takes no positional arguments")
		}
		users, err := client.ListUsers(ctx)
		if err != nil {
			return err
		}
		return printCLI(out, c.json, users)
	case "user volumes":
		if len(positional) != 1 {
			return errors.New("user volumes requires one user ID")
		}
		volumes, err := client.ListUserVolumes(ctx, positional[0])
		if err != nil {
			return err
		}
		return printCLI(out, c.json, volumes)
	case "volume list":
		result, err := client.ListVolumes(ctx)
		if err != nil {
			return err
		}
		return printCLI(out, c.json, result)
	case "volume show":
		id, err := requiredVolume(c, positional)
		if err != nil {
			return err
		}
		result, err := client.GetVolume(ctx, id)
		if err != nil {
			return err
		}
		return printCLI(out, c.json, result)
	case "volume create", "volume rename", "volume quota", "volume delete":
		return volumeMutation(ctx, client, record, c, command, name, quota, positional, out, errOut)
	case "operation show", "operation wait", "operation recover":
		return operationCommand(ctx, client, record, c, command, positional, out, errOut)
	case "files ls", "files mkdir", "files move", "files rename", "files rm", "files upload", "files download", "files query", "files resume", "files cancel":
		return fileCommand(ctx, client, record, c, command, positional, replace, out, errOut)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}
func requiredVolume(c cliOptions, args []string) (string, error) {
	if c.volume != "" && len(args) == 0 {
		return c.volume, nil
	}
	if c.volume == "" && len(args) == 1 {
		return args[0], nil
	}
	return "", errors.New("provide one volume ID, either positional or -volume")
}
func syncParent(name string) error {
	dir, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
func rotateCredential(ctx context.Context, client *serviceapi.Client, record credentialRecord, c cliOptions, out io.Writer) error {
	unlock, err := lockPrivate(c.credentials)
	if err != nil {
		return err
	}
	defer unlock()
	record, err = loadCredential(c.credentials)
	if err != nil {
		return err
	}
	client, err = client.WithCredential(serviceapi.Credential{ID: record.ID, Secret: record.Secret})
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 2; attempt++ {
		if record.Pending == nil {
			secret, err := randomSecret()
			if err != nil {
				return err
			}
			action, err := randomHex()
			if err != nil {
				return err
			}
			record.Pending = &pendingRotation{secret, action, record.Generation}
			if err = writePrivate(c.credentials, record); err != nil {
				return err
			}
		}
		pending := record.Pending
		prepare := serviceapi.PrepareRotationRequest{ActionID: pending.ActionID, ExpectedGeneration: serviceapi.Decimal(pending.ExpectedGeneration), Secret: pending.Secret}
		_, prepareErr := client.PrepareRotation(ctx, record.ID, prepare)
		rotated, err := client.WithCredential(serviceapi.Credential{ID: record.ID, Secret: pending.Secret})
		if err != nil {
			return err
		}
		result, activateErr := rotated.ActivateRotation(ctx, record.ID, serviceapi.ActivateRotationRequest{ActionID: pending.ActionID, ExpectedGeneration: prepare.ExpectedGeneration})
		if activateErr == nil {
			if result.RotationActionID != pending.ActionID {
				return errors.New("rotation receipt does not match the pending action")
			}
			record.Secret = pending.Secret
			record.Generation = string(result.Generation)
			record.Pending = nil
			if err = writePrivate(c.credentials, record); err != nil {
				return err
			}
			return printCLI(out, c.json, result)
		}
		current, queryErr := client.Credential(ctx, record.ID)
		if queryErr != nil {
			return errors.Join(prepareErr, activateErr, queryErr)
		}
		expected, parseErr := prepare.ExpectedGeneration.Int64()
		if parseErr != nil {
			return parseErr
		}
		generation, parseErr := current.Generation.Int64()
		if parseErr != nil {
			return parseErr
		}
		activatedGeneration := int64(0)
		if current.RotationGeneration != "" {
			activatedGeneration, parseErr = current.RotationGeneration.Int64()
			if parseErr != nil {
				return parseErr
			}
		}
		if attempt == 0 && generation == expected+1 && current.PendingActionID == "" && current.RotationActionID != pending.ActionID && activatedGeneration <= expected {
			// Expiring an unactivated prepare advances generation without changing the active verifier.
			record.Generation = string(current.Generation)
			record.Pending = nil
			if err = writePrivate(c.credentials, record); err != nil {
				return err
			}
			continue
		}
		return errors.Join(prepareErr, activateErr)
	}
	return errors.New("credential rotation could not be reconciled")
}

func parseQuota(value string) (serviceapi.Decimal, error) {
	if value == "0" {
		return "0", nil
	}
	var size clientPositiveSizeFlag
	if err := size.Set(value); err != nil {
		return "", err
	}
	return serviceapi.Decimal(strconv.FormatInt(size.bytes, 10)), nil
}

func readTTYPassword(ctx context.Context, errOut io.Writer) (_ string, returned error) {
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return "", errors.New("cannot open a TTY; use -password-stdin or -password-file")
	}
	defer func() { returned = errors.Join(returned, tty.Close()) }()
	return passwordinput.ReadTTY(ctx, tty, errOut, "Administrator password: ", 1024)
}
