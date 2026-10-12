package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
)

type credentialResponse struct {
	ID                 management.ID   `json:"id"`
	Label              string          `json:"label"`
	Scopes             []string        `json:"scopes"`
	AllOwnedVolumes    bool            `json:"allOwnedVolumes"`
	VolumeIDs          []management.ID `json:"volumeIds"`
	Generation         string          `json:"generation"`
	ExpiresAt          time.Time       `json:"expiresAt"`
	PendingActionID    management.ID   `json:"pendingActionId,omitempty"`
	PendingExpiresAt   *time.Time      `json:"pendingExpiresAt,omitempty"`
	RotationActionID   management.ID   `json:"rotationActionId,omitempty"`
	RotationGeneration string          `json:"rotationGeneration,omitempty"`
}

func publicCredential(c management.Credential) credentialResponse {
	ids := append([]management.ID{}, c.VolumeIDs...)
	result := credentialResponse{ID: c.ID, Label: c.Label, Scopes: append([]string{}, c.Scopes...), AllOwnedVolumes: c.AllOwnedVolumes, VolumeIDs: ids, Generation: strconv.FormatInt(c.Generation, 10), ExpiresAt: c.ExpiresAt, PendingActionID: c.PendingActionID, PendingExpiresAt: c.PendingExpiresAt, RotationActionID: c.RotationActionID}
	if c.RotationGeneration > 0 {
		result.RotationGeneration = strconv.FormatInt(c.RotationGeneration, 10)
	}
	return result
}

type createCredentialRequest struct {
	LoginName       string          `json:"loginName"`
	Password        string          `json:"password"`
	Label           string          `json:"label"`
	Scopes          []string        `json:"scopes"`
	AllOwnedVolumes *bool           `json:"allOwnedVolumes"`
	VolumeIDs       []management.ID `json:"volumeIds"`
	ExpiresAt       *time.Time      `json:"expiresAt"`
}

func (a *Authenticator) createCredential(w http.ResponseWriter, r *http.Request) {
	if len(r.Header.Values("Authorization")) > 0 {
		writeAuthError(w, ErrForbidden)
		return
	}
	var browser *Principal
	if len(r.CookiesNamed(authCookieName)) > 0 {
		p, err := a.Authenticate(r.Context(), r)
		if err != nil {
			writeAuthError(w, err)
			return
		}
		browser = &p
	}
	var input createCredentialRequest
	if err := decodeAuthRequest(w, r, &input); err != nil {
		writeAuthError(w, err)
		return
	}
	user, err := a.verifyLogin(r.Context(), r, input.LoginName, input.Password)
	if err != nil {
		writeAuthError(w, err)
		return
	}
	if browser != nil && browser.UserID != user.ID {
		writeAuthError(w, ErrForbidden)
		return
	}
	if !validLoginName(input.Label) {
		writeAuthError(w, errAuthRequest)
		return
	}
	scopes := input.Scopes
	if scopes == nil {
		scopes = []string{ScopeVolumesRead, ScopeVolumesWrite, ScopeFiles}
		if user.Role == management.RoleAdmin {
			scopes = append(scopes, ScopeAdminSummaries)
		}
	}
	allOwned := false
	if input.AllOwnedVolumes == nil && input.VolumeIDs == nil && input.Scopes == nil {
		allOwned = true
	}
	if input.AllOwnedVolumes != nil {
		allOwned = *input.AllOwnedVolumes
	}
	if allOwned && len(input.VolumeIDs) > 0 {
		writeAuthError(w, errAuthRequest)
		return
	}
	for _, scope := range scopes {
		if (scope == ScopeVolumesRead || scope == ScopeVolumesWrite || scope == ScopeFiles) && !allOwned && len(input.VolumeIDs) == 0 {
			writeAuthError(w, errAuthRequest)
			return
		}
	}
	expires := a.now().Add(credentialLifetime)
	if input.ExpiresAt != nil {
		expires = *input.ExpiresAt
	}
	secret, err := randomAuthSecret()
	if err != nil {
		writeAuthError(w, err)
		return
	}
	digest, err := authSecretDigest(secret)
	if err != nil {
		writeAuthError(w, err)
		return
	}
	credential, err := a.store.CreateCredential(r.Context(), management.CredentialInput{UserID: user.ID, AuthEpoch: user.AuthEpoch, Label: input.Label, SecretDigest: digest, Scopes: scopes, AllOwnedVolumes: allOwned, VolumeIDs: input.VolumeIDs, ExpiresAt: expires})
	if err != nil {
		writeAuthError(w, err)
		return
	}
	writeAuthJSON(w, http.StatusCreated, struct {
		Credential credentialResponse `json:"credential"`
		Secret     string             `json:"secret"`
		User       authUserResponse   `json:"user"`
	}{publicCredential(credential), secret, authUser(principalFromCredential(user, credential))})
}

func (a *Authenticator) ownedCredential(r *http.Request) (Principal, management.Credential, error) {
	p, err := a.Authenticate(r.Context(), r)
	if err != nil {
		return Principal{}, management.Credential{}, err
	}
	id := management.ID(r.PathValue("id"))
	if !validAuthID(id) {
		return Principal{}, management.Credential{}, errAuthRequest
	}
	if p.Kind == "cli" && p.AuthenticationID != id {
		return Principal{}, management.Credential{}, ErrForbidden
	}
	credential, err := a.store.GetCredential(r.Context(), id)
	if err != nil {
		return Principal{}, management.Credential{}, err
	}
	if credential.UserID != p.UserID {
		return Principal{}, management.Credential{}, ErrForbidden
	}
	return p, credential, nil
}

func (a *Authenticator) credential(w http.ResponseWriter, r *http.Request) {
	_, credential, err := a.ownedCredential(r)
	if err != nil {
		writeAuthError(w, err)
		return
	}
	writeAuthJSON(w, http.StatusOK, publicCredential(credential))
}

func (a *Authenticator) revokeCredential(w http.ResponseWriter, r *http.Request) {
	p, credential, err := a.ownedCredential(r)
	if err != nil {
		writeAuthError(w, err)
		return
	}
	if err := a.store.RevokeCredential(r.Context(), p.UserID, credential.ID); err != nil {
		writeAuthError(w, err)
		return
	}
	a.cancelAccess(credential.ID)
	w.WriteHeader(http.StatusNoContent)
}

type prepareRotationRequest struct {
	ActionID           management.ID `json:"actionId"`
	ExpectedGeneration string        `json:"expectedGeneration"`
	Secret             string        `json:"secret"`
}

type activateRotationRequest struct {
	ActionID           management.ID `json:"actionId"`
	ExpectedGeneration string        `json:"expectedGeneration"`
}

func (a *Authenticator) prepareRotation(w http.ResponseWriter, r *http.Request) {
	p, credential, err := a.ownedCredential(r)
	if err != nil {
		writeAuthError(w, err)
		return
	}
	var input prepareRotationRequest
	if err := decodeAuthRequest(w, r, &input); err != nil {
		writeAuthError(w, err)
		return
	}
	generation, err := positiveAuthDecimal(input.ExpectedGeneration)
	if err != nil || !validAuthID(input.ActionID) {
		writeAuthError(w, errAuthRequest)
		return
	}
	digest, err := authSecretDigest(input.Secret)
	if err != nil {
		writeAuthError(w, errAuthRequest)
		return
	}
	payload, err := json.Marshal(struct {
		CredentialID management.ID
		ActionID     management.ID
		Generation   int64
		Digest       []byte
	}{credential.ID, input.ActionID, generation, digest})
	if err != nil {
		writeAuthError(w, err)
		return
	}
	fingerprint := sha256.Sum256(payload)
	rotated, err := a.store.PrepareCredentialRotation(r.Context(), management.RotationInput{UserID: p.UserID, CredentialID: credential.ID, ActionID: input.ActionID, ExpectedGeneration: generation, PendingDigest: digest, Fingerprint: fingerprint[:]})
	if err != nil {
		writeAuthError(w, err)
		return
	}
	writeAuthJSON(w, http.StatusOK, publicCredential(rotated))
}

func (a *Authenticator) activateRotation(w http.ResponseWriter, r *http.Request) {
	if len(r.CookiesNamed(authCookieName)) > 0 || len(r.Header.Values("Authorization")) != 1 {
		writeAuthError(w, ErrUnauthenticated)
		return
	}
	id, secret, err := parseBearer(r.Header.Get("Authorization"))
	if err != nil {
		writeAuthError(w, err)
		return
	}
	if string(id) != r.PathValue("id") {
		writeAuthError(w, ErrForbidden)
		return
	}
	var input activateRotationRequest
	if err := decodeAuthRequest(w, r, &input); err != nil {
		writeAuthError(w, err)
		return
	}
	generation, err := positiveAuthDecimal(input.ExpectedGeneration)
	if err != nil || !validAuthID(input.ActionID) {
		writeAuthError(w, errAuthRequest)
		return
	}
	digest, err := authSecretDigest(secret)
	if err != nil {
		writeAuthError(w, err)
		return
	}
	credential, err := a.store.ActivateCredentialRotation(r.Context(), id, input.ActionID, digest, generation)
	if err != nil {
		writeAuthError(w, authStateError(err))
		return
	}
	writeAuthJSON(w, http.StatusOK, publicCredential(credential))
}

func validAuthID(id management.ID) bool {
	if len(id) != 32 {
		return false
	}
	raw, err := hex.DecodeString(string(id))
	return err == nil && hex.EncodeToString(raw) == string(id)
}

func positiveAuthDecimal(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 1 || strconv.FormatInt(parsed, 10) != value {
		return 0, errAuthRequest
	}
	return parsed, nil
}
