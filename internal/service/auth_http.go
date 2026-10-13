package service

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
)

var errAuthRequest = errors.New("invalid authentication request")

func (a *Authenticator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/service/v1/auth/login", a.login)
	mux.HandleFunc("POST /api/service/v1/auth/logout", a.logout)
	mux.HandleFunc("GET /api/service/v1/auth/session", a.session)
	mux.HandleFunc("POST /api/service/v1/auth/cli-credentials", a.createCredential)
	mux.HandleFunc("GET /api/service/v1/auth/cli-credentials/{id}", a.credential)
	mux.HandleFunc("DELETE /api/service/v1/auth/cli-credentials/{id}", a.revokeCredential)
	mux.HandleFunc("POST /api/service/v1/auth/cli-credentials/{id}/rotation/prepare", a.prepareRotation)
	mux.HandleFunc("POST /api/service/v1/auth/cli-credentials/{id}/rotation/activate", a.activateRotation)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})
}

type authUserResponse struct {
	ID          management.ID       `json:"id"`
	DisplayName string              `json:"displayName"`
	Role        management.UserRole `json:"role"`
}

func authUser(p Principal) authUserResponse {
	return authUserResponse{ID: p.UserID, DisplayName: p.DisplayName, Role: p.Role}
}

func sessionResponse(p Principal, session management.Session) any {
	return struct {
		User              authUserResponse `json:"user"`
		CSRFToken         string           `json:"csrfToken"`
		IdleExpiresAt     time.Time        `json:"idleExpiresAt"`
		AbsoluteExpiresAt time.Time        `json:"absoluteExpiresAt"`
	}{authUser(p), session.CSRFToken, session.IdleExpiresAt, session.AbsoluteExpiresAt}
}

func (a *Authenticator) login(w http.ResponseWriter, r *http.Request) {
	if err := a.checkOrigin(r); err != nil {
		writeAuthError(w, err)
		return
	}
	if !constantHeader(r, "X-RFS-Request", "1") || len(r.Header.Values("Authorization")) > 0 || len(r.CookiesNamed(authCookieName)) > 1 {
		writeAuthError(w, errAuthRequest)
		return
	}
	var input struct {
		LoginName string `json:"loginName"`
		Password  string `json:"password"`
	}
	if err := decodeAuthRequest(w, r, &input); err != nil {
		writeAuthError(w, err)
		return
	}
	user, err := a.verifyLogin(r.Context(), r, input.LoginName, input.Password)
	if err != nil {
		writeAuthError(w, err)
		return
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
	csrf, err := randomAuthSecret()
	if err != nil {
		writeAuthError(w, err)
		return
	}
	session, err := a.store.CreateSession(r.Context(), management.SessionInput{UserID: user.ID, AuthEpoch: user.AuthEpoch, SecretDigest: digest, CSRFToken: csrf})
	if err != nil {
		writeAuthError(w, err)
		return
	}
	http.SetCookie(w, authCookie(string(session.ID)+"."+secret, session.AbsoluteExpiresAt))
	writeAuthJSON(w, http.StatusOK, sessionResponse(principalFromUser(user, session.ID, "browser"), session))
}

func (a *Authenticator) session(w http.ResponseWriter, r *http.Request) {
	p, session, err := a.browserSession(r.Context(), r)
	if err != nil {
		writeAuthError(w, err)
		return
	}
	writeAuthJSON(w, http.StatusOK, sessionResponse(p, session))
}

func (a *Authenticator) logout(w http.ResponseWriter, r *http.Request) {
	p, _, err := a.browserSession(r.Context(), r)
	if err != nil {
		writeAuthError(w, err)
		return
	}
	if err := a.store.RevokeSession(r.Context(), p.UserID, p.AuthenticationID); err != nil {
		writeAuthError(w, err)
		return
	}
	a.cancelAccess(p.AuthenticationID)
	clearAuthCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

func writeAuthJSON(w http.ResponseWriter, status int, value any) {
	// Encode before publishing the status so unsupported values cannot produce success.
	body, err := json.Marshal(value)
	if err != nil {
		writeAuthError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}

func writeAuthError(w http.ResponseWriter, err error) {
	status, code := http.StatusServiceUnavailable, "authentication_unavailable"
	switch {
	case errors.Is(err, ErrUnauthenticated):
		status, code = http.StatusUnauthorized, "authentication_required"
	case errors.Is(err, ErrForbidden), errors.Is(err, management.ErrDenied):
		status, code = http.StatusForbidden, "access_denied"
	case errors.Is(err, errAuthRequest):
		status, code = http.StatusBadRequest, "invalid_request"
	case errors.Is(err, ErrAuthCapacity), errors.Is(err, management.ErrCapacity):
		status, code = http.StatusTooManyRequests, "authentication_capacity"
	case errors.Is(err, management.ErrNotFound):
		status, code = http.StatusNotFound, "not_found"
	case errors.Is(err, management.ErrConflict):
		status, code = http.StatusConflict, "condition_conflict"
	case errors.Is(err, management.ErrExpired):
		status, code = http.StatusGone, "acceptance_expired"
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "60")
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, "{\"error\":{\"code\":"+strconv.Quote(code)+"}}\n")
}

func equalAuthValue(a, b string) bool { return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1 }

func decodeAuthRequest(w http.ResponseWriter, r *http.Request, value any) error {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return errAuthRequest
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64*1024))
	if err != nil {
		return errAuthRequest
	}
	tokens := json.NewDecoder(bytes.NewReader(body))
	tokens.UseNumber()
	if err := checkAuthJSONValue(tokens, 0); err != nil {
		return errAuthRequest
	}
	if _, err := tokens.Token(); !errors.Is(err, io.EOF) {
		return errAuthRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return errAuthRequest
	}
	return nil
}

func checkAuthJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 32 {
		return errAuthRequest
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	switch delim {
	case '{':
		keys := make(map[string]bool)
		for decoder.More() {
			token, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok || keys[key] {
				return errAuthRequest
			}
			keys[key] = true
			if err := checkAuthJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		token, err := decoder.Token()
		if err != nil || token != json.Delim('}') {
			return errAuthRequest
		}
	case '[':
		for decoder.More() {
			if err := checkAuthJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		token, err := decoder.Token()
		if err != nil || token != json.Delim(']') {
			return errAuthRequest
		}
	default:
		return errAuthRequest
	}
	return nil
}
