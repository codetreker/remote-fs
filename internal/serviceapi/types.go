// Package serviceapi defines the standalone service's HTTP contract and client.
package serviceapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

const Prefix = "/api/service/v1"

// Decimal preserves every integer bit across JSON consumers. Its wire form is
// a canonical nonnegative decimal string; use Int64 for signed storage limits.
type Decimal string

func (d Decimal) Uint64() (uint64, error) {
	if len(d) == 0 || (len(d) > 1 && d[0] == '0') {
		return 0, errors.New("invalid decimal string")
	}
	for _, c := range d {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid decimal string")
		}
	}
	return strconv.ParseUint(string(d), 10, 64)
}

func (d Decimal) Int64() (int64, error) {
	if _, err := d.Uint64(); err != nil {
		return 0, err
	}
	return strconv.ParseInt(string(d), 10, 64)
}

func DecimalFromUint64(value uint64) Decimal { return Decimal(strconv.FormatUint(value, 10)) }
func DecimalFromInt64(value int64) Decimal   { return Decimal(strconv.FormatInt(value, 10)) }

func (d Decimal) MarshalJSON() ([]byte, error) {
	if _, err := d.Uint64(); err != nil {
		return nil, err
	}
	return json.Marshal(string(d))
}

func (d *Decimal) UnmarshalJSON(data []byte) error {
	var value string
	if len(data) == 0 || data[0] != '"' {
		return errors.New("decimal must be a JSON string")
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	parsed := Decimal(value)
	if _, err := parsed.Uint64(); err != nil {
		return err
	}
	*d = parsed
	return nil
}

type ErrorDetail struct {
	Code string `json:"code"`
}
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// APIError contains the closed public error code, without server diagnostics.
type APIError struct {
	Status int
	Code   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("service returned HTTP %d (%s)", e.Status, e.Code)
}

type User struct {
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	Role        string `json:"role"`
}

type Session struct {
	User              User      `json:"user"`
	CSRFToken         string    `json:"csrfToken"`
	IdleExpiresAt     time.Time `json:"idleExpiresAt"`
	AbsoluteExpiresAt time.Time `json:"absoluteExpiresAt"`
}

type VolumeSummary struct {
	FileCount      Decimal `json:"fileCount"`
	DirectoryCount Decimal `json:"directoryCount"`
	UsedBytes      Decimal `json:"usedBytes"`
	QuotaBytes     Decimal `json:"quotaBytes"`
}

type Volume struct {
	ID           string         `json:"id"`
	OwnerUserID  string         `json:"ownerUserId"`
	Name         string         `json:"name"`
	State        string         `json:"state"`
	Revision     Decimal        `json:"revision"`
	CreatedAt    time.Time      `json:"createdAt"`
	UpdatedAt    time.Time      `json:"updatedAt"`
	Summary      *VolumeSummary `json:"summary,omitempty"`
	SummaryError *ErrorDetail   `json:"summaryError,omitempty"`
}

type UserVolumeStats struct {
	FileCount      Decimal `json:"fileCount"`
	DirectoryCount Decimal `json:"directoryCount"`
	UsedBytes      Decimal `json:"usedBytes"`
	QuotaBytes     Decimal `json:"quotaBytes"`
}

type UserVolumeSummary struct {
	Name         string           `json:"name"`
	Summary      *UserVolumeStats `json:"summary,omitempty"`
	SummaryError *ErrorDetail     `json:"summaryError,omitempty"`
}

type VolumeList struct {
	Volumes   []Volume `json:"volumes"`
	NextAfter string   `json:"nextAfter,omitempty"`
}
type UserList struct {
	Users     []User `json:"users"`
	NextAfter string `json:"nextAfter,omitempty"`
}
type UserVolumeList struct {
	Volumes   []UserVolumeSummary `json:"volumes"`
	NextAfter string              `json:"nextAfter,omitempty"`
}

type CreateVolumeRequest struct {
	Name       string  `json:"name"`
	QuotaBytes Decimal `json:"quotaBytes"`
}

type RenameVolumeRequest struct {
	Name string `json:"name"`
}

type FileEntry struct {
	Name       string    `json:"name"`
	NameBase64 string    `json:"nameBase64"`
	PathBase64 string    `json:"-"`
	NodeID     Decimal   `json:"nodeId"`
	Kind       string    `json:"kind"`
	SizeBytes  Decimal   `json:"sizeBytes"`
	ModifiedAt time.Time `json:"modifiedAt"`
}

type FileDirectory struct {
	PathBase64 string      `json:"pathBase64"`
	Entries    []FileEntry `json:"entries"`
}

type CredentialInfo struct {
	ID                 string     `json:"id"`
	Label              string     `json:"label"`
	Scopes             []string   `json:"scopes"`
	AllOwnedVolumes    bool       `json:"allOwnedVolumes"`
	VolumeIDs          []string   `json:"volumeIds"`
	Generation         Decimal    `json:"generation"`
	ExpiresAt          time.Time  `json:"expiresAt"`
	PendingActionID    string     `json:"pendingActionId,omitempty"`
	PendingExpiresAt   *time.Time `json:"pendingExpiresAt,omitempty"`
	RotationActionID   string     `json:"rotationActionId,omitempty"`
	RotationGeneration Decimal    `json:"rotationGeneration,omitempty"`
}

type CreateCredentialRequest struct {
	LoginName       string     `json:"loginName"`
	Password        string     `json:"password"`
	Label           string     `json:"label"`
	Scopes          []string   `json:"scopes,omitempty"`
	AllOwnedVolumes *bool      `json:"allOwnedVolumes,omitempty"`
	VolumeIDs       []string   `json:"volumeIds,omitempty"`
	ExpiresAt       *time.Time `json:"expiresAt,omitempty"`
}

type CredentialLogin struct {
	Credential CredentialInfo `json:"credential"`
	Secret     string         `json:"secret"`
	User       User           `json:"user"`
}

type PrepareRotationRequest struct {
	ActionID           string  `json:"actionId"`
	ExpectedGeneration Decimal `json:"expectedGeneration"`
	Secret             string  `json:"secret"`
}

type ActivateRotationRequest struct {
	ActionID           string  `json:"actionId"`
	ExpectedGeneration Decimal `json:"expectedGeneration"`
}
