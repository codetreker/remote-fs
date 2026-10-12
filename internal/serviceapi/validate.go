package serviceapi

import (
	"errors"
	"reflect"
)

func requirePositive(value Decimal) error {
	n, err := value.Uint64()
	if err != nil || n == 0 {
		return errors.New("response requires a positive decimal")
	}
	return nil
}

func validateUser(user User) error {
	if user.DisplayName == "" {
		return errors.New("missing user display name")
	}
	if err := validateID(user.ID); err != nil {
		return err
	}
	if user.Role != "admin" && user.Role != "user" {
		return errors.New("invalid user role")
	}
	return nil
}

func validateSummary(summary *VolumeSummary, problem *ErrorDetail) error {
	if (summary == nil) == (problem == nil) {
		return errors.New("volume summary must contain statistics or an error")
	}
	if problem != nil {
		if !validErrorCode(problem.Code) {
			return errors.New("invalid summary error")
		}
		return nil
	}
	for _, d := range []Decimal{summary.FileCount, summary.DirectoryCount} {
		if _, err := d.Uint64(); err != nil {
			return err
		}
	}
	for _, d := range []Decimal{summary.UsedBytes, summary.QuotaBytes} {
		if _, err := d.Int64(); err != nil {
			return err
		}
	}
	return requirePositive(summary.QuotaRevision)
}

func validateVolume(volume Volume) error {
	if volume.Name == "" {
		return errors.New("missing volume name")
	}
	if err := validateID(volume.ID); err != nil {
		return err
	}
	if err := validateID(volume.OwnerUserID); err != nil {
		return err
	}
	if volume.LastOperationID != "" {
		if err := validateID(volume.LastOperationID); err != nil {
			return err
		}
	}
	if err := requirePositive(volume.Revision); err != nil {
		return err
	}
	if _, err := volume.Revision.Int64(); err != nil {
		return err
	}
	if volume.CreatedAt.IsZero() || volume.UpdatedAt.IsZero() {
		return errors.New("missing volume timestamp")
	}
	switch volume.State {
	case "creating", "active", "deleting", "deleted", "failed":
	default:
		return errors.New("invalid volume state")
	}
	return validateSummary(volume.Summary, volume.SummaryError)
}

func validateOperation(operation Operation) error {
	if err := validateID(operation.ID); err != nil {
		return err
	}
	if err := validateID(operation.VolumeID); err != nil {
		return err
	}
	if err := requirePositive(operation.Revision); err != nil {
		return err
	}
	if _, err := operation.Revision.Int64(); err != nil {
		return err
	}
	if operation.CreatedAt.IsZero() || operation.UpdatedAt.IsZero() {
		return errors.New("missing operation timestamp")
	}
	switch operation.Kind {
	case "create", "rename", "quota", "delete":
	default:
		return errors.New("invalid operation kind")
	}
	switch operation.State {
	case "pending", "running", "succeeded", "failed":
	default:
		return errors.New("invalid operation state")
	}
	switch operation.Phase {
	case "accepted", "initializing", "bound", "applying", "frozen", "verified-close", "quarantined", "reclaiming", "completed":
	default:
		return errors.New("invalid operation phase")
	}
	if operation.State == "succeeded" && !operation.Settled {
		return errors.New("successful operation has unsettled responsibility")
	}
	if operation.State == "succeeded" && operation.Phase != "completed" {
		return errors.New("terminal operation has incomplete phase")
	}
	if operation.Phase == "completed" && operation.State != "succeeded" {
		return errors.New("incomplete operation claims completed phase")
	}
	if operation.State != "failed" && operation.Error != nil {
		return errors.New("nonfailed operation contains an error")
	}
	if operation.State == "failed" && operation.Error == nil {
		return errors.New("failed operation omits its error")
	}
	if operation.Settled && operation.State != "failed" && operation.State != "succeeded" {
		return errors.New("nonterminal operation claims settlement")
	}
	if operation.State == "pending" && operation.Phase != "accepted" {
		return errors.New("pending operation claims an advanced phase")
	}
	allowed := false
	switch operation.Kind {
	case "create":
		allowed = operation.Phase == "accepted" || operation.Phase == "initializing" || operation.Phase == "bound" || operation.Phase == "completed"
	case "rename":
		allowed = operation.Phase == "completed"
	case "quota":
		allowed = operation.Phase == "accepted" || operation.Phase == "applying" || operation.Phase == "completed"
	case "delete":
		allowed = operation.Phase == "accepted" || operation.Phase == "frozen" || operation.Phase == "verified-close" || operation.Phase == "quarantined" || operation.Phase == "reclaiming" || operation.Phase == "completed"
	}
	if !allowed {
		return errors.New("operation phase is invalid for its kind")
	}
	if operation.Error != nil && !validErrorCode(operation.Error.Code) {
		return errors.New("invalid operation error")
	}
	return nil
}

func validateCredential(credential CredentialInfo) error {
	if len(credential.Scopes) == 0 {
		return errors.New("credential has no scopes")
	}
	if err := validateID(credential.ID); err != nil {
		return err
	}
	if err := requirePositive(credential.Generation); err != nil {
		return err
	}
	if _, err := credential.Generation.Int64(); err != nil {
		return err
	}
	if credential.ExpiresAt.IsZero() {
		return errors.New("missing credential expiry")
	}
	seen := make(map[string]bool)
	for _, scope := range credential.Scopes {
		if seen[scope] {
			return errors.New("duplicate credential scope")
		}
		seen[scope] = true
		switch scope {
		case "volumes:read", "volumes:write", "files", "admin:summaries":
		default:
			return errors.New("invalid credential scope")
		}
	}
	if credential.AllOwnedVolumes && len(credential.VolumeIDs) != 0 {
		return errors.New("ambiguous credential volume selection")
	}
	for _, id := range credential.VolumeIDs {
		if err := validateID(id); err != nil {
			return err
		}
	}
	if credential.PendingActionID != "" {
		if err := validateID(credential.PendingActionID); err != nil {
			return err
		}
		if credential.PendingExpiresAt == nil {
			return errors.New("missing pending expiry")
		}
	}
	if credential.RotationActionID != "" {
		if err := validateID(credential.RotationActionID); err != nil {
			return err
		}
		if err := requirePositive(credential.RotationGeneration); err != nil {
			return err
		}
		if _, err := credential.RotationGeneration.Int64(); err != nil {
			return err
		}
	}
	return nil
}

func validateResponse(value any) error {
	switch result := value.(type) {
	case *Capabilities:
		if _, err := result.FileSession.NativeOptions(); err != nil {
			return err
		}
		if _, err := result.MaxTransferBytes.Int64(); err != nil {
			return err
		}
		return requirePositive(result.MaxTransferBytes)
	case *CredentialLogin:
		if err := validateCredential(result.Credential); err != nil {
			return err
		}
		return validateUser(result.User)
	case *CredentialInfo:
		return validateCredential(*result)
	case *ActionNamespace:
		if err := validateID(result.SubjectID); err != nil {
			return err
		}
		if result.AcceptUntil.IsZero() {
			return errors.New("missing action acceptance expiry")
		}
		if _, err := result.Namespace.Int64(); err != nil {
			return err
		}
		return requirePositive(result.Namespace)
	case *Volume:
		return validateVolume(*result)
	case *Operation:
		return validateOperation(*result)
	case *VolumeList:
		if result.Volumes == nil {
			return errors.New("volume list is missing")
		}
		for _, volume := range result.Volumes {
			if err := validateVolume(volume); err != nil {
				return err
			}
		}
	case *UserList:
		if result.Users == nil {
			return errors.New("user list is missing")
		}
		for _, user := range result.Users {
			if err := validateUser(user); err != nil {
				return err
			}
			if user.Role != "user" {
				return errors.New("admin directory includes a nonordinary user")
			}
		}
	case *UserVolumeList:
		if result.Volumes == nil {
			return errors.New("user volume list is missing")
		}
		for _, volume := range result.Volumes {
			if volume.Name == "" {
				return errors.New("missing user volume name")
			}
			if (volume.Summary == nil) == (volume.SummaryError == nil) {
				return errors.New("user volume requires statistics or error")
			}
			if volume.SummaryError != nil {
				if !validErrorCode(volume.SummaryError.Code) {
					return errors.New("invalid summary error")
				}
				continue
			}
			for _, d := range []Decimal{volume.Summary.FileCount, volume.Summary.DirectoryCount} {
				if _, err := d.Uint64(); err != nil {
					return err
				}
			}
			for _, d := range []Decimal{volume.Summary.UsedBytes, volume.Summary.QuotaBytes} {
				if _, err := d.Int64(); err != nil {
					return err
				}
			}
		}
	default:
		return errors.New("unsupported service response type: " + reflect.TypeOf(value).String())
	}
	return nil
}
