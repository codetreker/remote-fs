package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/codetreker/remote-fs/internal/serviceapi"
	"github.com/codetreker/remote-fs/packages/storage"
)

type actionRecord struct {
	Version     int                    `json:"version"`
	Server      string                 `json:"server"`
	SubjectID   string                 `json:"subjectId"`
	Kind        string                 `json:"kind"`
	VolumeID    string                 `json:"volumeId"`
	Namespace   serviceapi.Decimal     `json:"namespace"`
	Nonce       string                 `json:"nonce"`
	Input       json.RawMessage        `json:"input,omitempty"`
	OperationID string                 `json:"operationId,omitempty"`
	Import      *storage.ImportCommand `json:"import,omitempty"`
}

func (r actionRecord) action() serviceapi.Action {
	return serviceapi.Action{Namespace: r.Namespace, Nonce: r.Nonce}
}
func (r actionRecord) nativeAction() (storage.ActionKey, error) {
	sequence, err := r.Namespace.Uint64()
	return storage.ActionKey{SubjectID: r.SubjectID, Namespace: sequence, Nonce: r.Nonce}, err
}
func newAction(ctx context.Context, client *serviceapi.Client, credential credentialRecord, c *cliOptions, kind, volume string) (actionRecord, error) {
	namespace, err := client.IssueNamespace(ctx)
	if err != nil {
		return actionRecord{}, err
	}
	if namespace.SubjectID != credential.SubjectID {
		return actionRecord{}, errors.New("namespace does not belong to the authenticated user")
	}
	nonce, err := randomHex()
	if err != nil {
		return actionRecord{}, err
	}
	if c.actionFile == "" {
		c.actionFile = filepath.Join(filepath.Dir(c.credentials), "actions", nonce+".json")
	}
	return actionRecord{Version: 1, Server: credential.Server, SubjectID: credential.SubjectID, Kind: kind, VolumeID: volume, Namespace: namespace.Namespace, Nonce: nonce}, nil
}
func loadAction(path string, credential credentialRecord) (actionRecord, error) {
	var record actionRecord
	if path == "" {
		return record, errors.New("-action-file is required")
	}
	if err := readPrivate(path, &record); err != nil {
		return record, err
	}
	if record.Version != 1 || record.Server != credential.Server || record.SubjectID != credential.SubjectID || record.Nonce == "" {
		return record, errors.New("action journal does not match the current service and user")
	}
	action, err := record.nativeAction()
	if err != nil {
		return record, err
	}
	if err = action.Check(); err != nil {
		return record, err
	}
	return record, nil
}
func volumeMutation(ctx context.Context, client *serviceapi.Client, credential credentialRecord, c cliOptions, command, name, quota string, args []string, out, errOut io.Writer) error {
	kind := command[len("volume "):]
	volume := ""
	var err error
	if kind != "create" {
		volume, err = requiredVolume(c, args)
		if err != nil {
			return err
		}
	} else if len(args) != 0 {
		return errors.New("create takes flags only")
	}
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
	var journal actionRecord
	if c.actionFile != "" {
		journal, err = loadAction(c.actionFile, credential)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if journal.Version == 0 {
		journal, err = newAction(ctx, client, credential, &c, kind, volume)
		if err != nil {
			return err
		}
		var payload any
		switch kind {
		case "create":
			q, err := parseQuota(quota)
			if err != nil {
				return err
			}
			if name == "" {
				return errors.New("-name is required")
			}
			payload = serviceapi.CreateVolumeRequest{Action: journal.action(), Name: name, QuotaBytes: q}
		default:
			current, err := client.GetVolume(ctx, volume)
			if err != nil {
				return err
			}
			switch kind {
			case "rename":
				if name == "" {
					return errors.New("-name is required")
				}
				payload = serviceapi.RenameVolumeRequest{Action: journal.action(), ExpectedRevision: current.Revision, Name: name}
			case "quota":
				q, err := parseQuota(quota)
				if err != nil {
					return err
				}
				if current.Summary == nil || current.SummaryError != nil {
					return errors.New("quota change requires a confirmed native quota summary")
				}
				payload = serviceapi.SetQuotaRequest{Action: journal.action(), ExpectedRevision: current.Summary.QuotaRevision, QuotaBytes: q}
			case "delete":
				payload = serviceapi.DeleteVolumeRequest{Action: journal.action(), ExpectedRevision: current.Revision}
			}
		}
		journal.Input, err = json.Marshal(payload)
		if err != nil {
			return err
		}
		if err = writePrivate(c.actionFile, journal); err != nil {
			return err
		}
	} else {
		if journal.Kind != kind || journal.VolumeID != volume {
			return errors.New("action journal has a different kind or target; use operation recover")
		}
		switch kind {
		case "create":
			var saved serviceapi.CreateVolumeRequest
			if err = json.Unmarshal(journal.Input, &saved); err != nil {
				return err
			}
			if name != "" && name != saved.Name {
				return errors.New("-name differs from the journaled action")
			}
			if quota != "" {
				q, err := parseQuota(quota)
				if err != nil {
					return err
				}
				if q != saved.QuotaBytes {
					return errors.New("-quota differs from the journaled action")
				}
			}
		case "rename":
			var saved serviceapi.RenameVolumeRequest
			if err = json.Unmarshal(journal.Input, &saved); err != nil {
				return err
			}
			if name != "" && name != saved.Name {
				return errors.New("-name differs from the journaled action")
			}
		case "quota":
			var saved serviceapi.SetQuotaRequest
			if err = json.Unmarshal(journal.Input, &saved); err != nil {
				return err
			}
			if quota != "" {
				q, err := parseQuota(quota)
				if err != nil {
					return err
				}
				if q != saved.QuotaBytes {
					return errors.New("-quota differs from the journaled action")
				}
			}
		}
	}

	fmt.Fprintf(errOut, "remote-fs: durable action journal %s\n", c.actionFile)
	result, err := submitManagementAction(ctx, client, journal)
	if err != nil {
		return fmt.Errorf("action outcome requires recovery with -action-file %s: %w", c.actionFile, err)
	}
	journal.OperationID = result.ID
	if err = writePrivate(c.actionFile, journal); err != nil {
		return err
	}
	return finishOperation(ctx, client, result, c, out, errOut)
}
func submitManagementAction(ctx context.Context, client *serviceapi.Client, j actionRecord) (serviceapi.Operation, error) {
	switch j.Kind {
	case "create":
		var r serviceapi.CreateVolumeRequest
		if err := json.Unmarshal(j.Input, &r); err != nil {
			return serviceapi.Operation{}, err
		}
		return client.CreateVolume(ctx, r)
	case "rename":
		var r serviceapi.RenameVolumeRequest
		if err := json.Unmarshal(j.Input, &r); err != nil {
			return serviceapi.Operation{}, err
		}
		return client.RenameVolume(ctx, j.VolumeID, r)
	case "quota":
		var r serviceapi.SetQuotaRequest
		if err := json.Unmarshal(j.Input, &r); err != nil {
			return serviceapi.Operation{}, err
		}
		return client.SetQuota(ctx, j.VolumeID, r)
	case "delete":
		var r serviceapi.DeleteVolumeRequest
		if err := json.Unmarshal(j.Input, &r); err != nil {
			return serviceapi.Operation{}, err
		}
		return client.DeleteVolume(ctx, j.VolumeID, r)
	default:
		return serviceapi.Operation{}, errors.New("journal is not a management action")
	}
}
func finishOperation(ctx context.Context, client *serviceapi.Client, operation serviceapi.Operation, c cliOptions, out, errOut io.Writer) error {
	fmt.Fprintf(errOut, "remote-fs: operation %s\n", operation.ID)
	if !c.noWait && operation.State != "succeeded" && operation.State != "failed" {
		var err error
		operation, err = client.WaitOperation(ctx, operation.ID, 250*time.Millisecond)
		if err != nil {
			return fmt.Errorf("operation %s remains queryable: %w", operation.ID, err)
		}
	}
	if err := printCLI(out, c.json, operation); err != nil {
		return err
	}
	if operation.State == "failed" {
		return fmt.Errorf("operation %s failed", operation.ID)
	}
	return nil
}
func operationCommand(ctx context.Context, client *serviceapi.Client, credential credentialRecord, c cliOptions, command string, args []string, out, errOut io.Writer) error {
	var result serviceapi.Operation
	var err error
	if command == "operation recover" {
		if c.actionFile == "" {
			return errors.New("-action-file is required")
		}
		if len(args) != 0 {
			return errors.New("recover requires -action-file, with no operation ID")
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
		result, err = client.FindOperation(ctx, journal.action())
		var api *serviceapi.APIError
		if errors.As(err, &api) && api.Status == 404 {
			result, err = submitManagementAction(ctx, client, journal)
		}
		if err != nil {
			return err
		}
		journal.OperationID = result.ID
		if err = writePrivate(c.actionFile, journal); err != nil {
			return err
		}
		return finishOperation(ctx, client, result, c, out, errOut)
	}
	if len(args) != 1 {
		return errors.New("provide exactly one operation ID")
	}
	if command == "operation wait" {
		result, err = client.WaitOperation(ctx, args[0], 250*time.Millisecond)
	} else {
		result, err = client.Operation(ctx, args[0])
	}
	if err != nil {
		return err
	}
	if err = printCLI(out, c.json, result); err != nil {
		return err
	}
	if result.State == "failed" {
		return fmt.Errorf("operation %s failed", result.ID)
	}
	return nil
}
