package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	passwordinput "github.com/codetreker/remote-fs/cmd/internal/password"
	"github.com/codetreker/remote-fs/internal/serviceapi"
)

func run(args []string, errOut io.Writer) error {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return runLegacy(args, errOut)
	}
	return runServiceCommand(args, os.Stdin, os.Stdout, errOut)
}

type cliOptions struct {
	server, credentials, volume string
	json, insecure              bool
	timeout                     time.Duration
}

func commonFlags(f *flag.FlagSet, c *cliOptions) {
	f.StringVar(&c.server, "server", "", "service origin")
	f.StringVar(&c.credentials, "credential-file", defaultCredentialFile, "private credential file")
	f.StringVar(&c.volume, "volume", "", "immutable volume ID")
	f.BoolVar(&c.json, "json", false, "write structured output with exact decimal strings")
	f.BoolVar(&c.insecure, "allow-insecure-http", false, "allow HTTP for an explicit development connection")
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
		_, err := fmt.Fprintln(out, "usage: remote-fs <login|logout|status|volume|user|files> [flags]\nUse <command> -h for flags. Volume and file commands take a subcommand before flags.")
		return err
	}
	if command == "volume" || command == "files" || command == "user" {
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
	var login, name, quota, passwordFile, label, remotePath string
	var passwordStdin bool
	f.StringVar(&name, "name", "", "volume display name")
	f.StringVar(&quota, "quota", "", "quota in bytes or SIZE (for example 10GiB)")
	switch command {
	case "login":
		f.StringVar(&login, "login", "", "administrator login name")
		f.StringVar(&label, "label", "CLI", "credential label")
		f.StringVar(&passwordFile, "password-file", "", "private 0600 file containing the password")
		f.BoolVar(&passwordStdin, "password-stdin", false, "read the password from standard input")
	case "files ls":
		f.StringVar(&remotePath, "path-base64", "", "opaque directory path returned by a previous listing")
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
			return errors.New("credential file already exists; log out before logging in again")
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
		result, err := client.Login(ctx, request)
		if err != nil {
			return err
		}
		record := credentialRecord{Version: 1, Server: server, ID: result.Credential.ID, Secret: result.Secret, SubjectID: result.User.ID}
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
	case "status":
		if len(positional) != 0 {
			return errors.New("unexpected positional arguments")
		}
		result, err := client.Credential(ctx, record.ID)
		if err != nil {
			return err
		}
		return printCLI(out, c.json, result)
	case "logout":
		if len(positional) != 0 {
			return errors.New("logout takes no positional arguments")
		}
		id := record.ID
		unlock, err := lockPrivate(c.credentials)
		if err != nil {
			return err
		}
		defer unlock()
		revokeErr := client.RevokeCredential(ctx, id)
		if revokeErr != nil {
			var problem *serviceapi.APIError
			if !errors.As(revokeErr, &problem) || problem.Status != http.StatusUnauthorized || problem.Code != "authentication_required" {
				return revokeErr
			}
		}
		if err = os.Remove(c.credentials); err != nil {
			return errors.Join(revokeErr, err)
		}
		if err = syncParent(c.credentials); err != nil {
			return errors.Join(revokeErr, err)
		}
		if revokeErr != nil {
			return fmt.Errorf("local credential removed; remote revocation was not confirmed: %w", revokeErr)
		}
		return printCLI(out, c.json, map[string]string{"credentialId": id, "state": "revoked"})
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
		if len(positional) != 0 {
			return errors.New("volume list takes no positional arguments")
		}
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
	case "volume create":
		if len(positional) != 0 || name == "" {
			return errors.New("volume create requires -name and -quota, with no positional arguments")
		}
		q, err := parseQuota(quota)
		if err != nil {
			return err
		}
		result, err := client.CreateVolume(ctx, serviceapi.CreateVolumeRequest{Name: name, QuotaBytes: q})
		if err != nil {
			return err
		}
		return printCLI(out, c.json, result)
	case "volume rename":
		id, err := requiredVolume(c, positional)
		if err != nil {
			return err
		}
		if name == "" {
			return errors.New("-name is required")
		}
		result, err := client.RenameVolume(ctx, id, serviceapi.RenameVolumeRequest{Name: name})
		if err != nil {
			return err
		}
		return printCLI(out, c.json, result)
	case "volume delete":
		id, err := requiredVolume(c, positional)
		if err != nil {
			return err
		}
		if err := client.DeleteVolume(ctx, id); err != nil {
			return err
		}
		return printCLI(out, c.json, map[string]string{"volumeId": id, "state": "deleted"})
	case "files ls":
		if c.volume == "" || len(positional) != 0 {
			return errors.New("files ls requires -volume and accepts an optional -path-base64 token")
		}
		result, err := client.ListFiles(ctx, c.volume, remotePath)
		if err != nil {
			return err
		}
		entries := make([]struct {
			serviceapi.FileEntry
			PathBase64 string `json:"pathBase64"`
		}, len(result.Entries))
		for index, entry := range result.Entries {
			entries[index].FileEntry = entry
			entries[index].PathBase64 = entry.PathBase64
		}
		return printCLI(out, c.json, struct {
			PathBase64 string `json:"pathBase64"`
			Entries    any    `json:"entries"`
		}{result.PathBase64, entries})
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
func parseQuota(value string) (serviceapi.Decimal, error) {
	var size clientPositiveSizeFlag
	if err := size.Set(value); err != nil {
		return "", err
	}
	return serviceapi.DecimalFromInt64(size.bytes), nil
}

func readTTYPassword(ctx context.Context, errOut io.Writer) (_ string, returned error) {
	tty, err := os.OpenFile("/dev/tty", os.O_RDWR, 0)
	if err != nil {
		return "", errors.New("cannot open a TTY; use -password-stdin or -password-file")
	}
	defer func() { returned = errors.Join(returned, tty.Close()) }()
	return passwordinput.ReadTTY(ctx, tty, errOut, "Administrator password: ", 1024)
}
