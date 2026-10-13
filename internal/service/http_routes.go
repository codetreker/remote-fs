package service

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
)

const managementPageLimit = 50

type managementRoutes struct {
	auth           *Authenticator
	store          *management.Store
	summary        func(context.Context, management.Volume) (VolumeSummary, error)
	process        func(context.Context, management.Volume) (management.Volume, error)
	cursors        cipher.AEAD
	summaryWorkers chan struct{}
}

func newManagementHandler(auth *Authenticator, store *management.Store, summary func(context.Context, management.Volume) (VolumeSummary, error), process func(context.Context, management.Volume) (management.Volume, error)) (http.Handler, error) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	cursors, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	routes := &managementRoutes{auth: auth, store: store, summary: summary, process: process}
	routes.cursors = cursors
	routes.summaryWorkers = make(chan struct{}, 4)
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+serviceapi.Prefix+"/me", routes.me)
	mux.HandleFunc("GET "+serviceapi.Prefix+"/volumes", routes.listVolumes)
	mux.HandleFunc("POST "+serviceapi.Prefix+"/volumes", routes.createVolume)
	mux.HandleFunc("GET "+serviceapi.Prefix+"/volumes/{id}", routes.getVolume)
	mux.HandleFunc("PATCH "+serviceapi.Prefix+"/volumes/{id}", routes.renameVolume)
	mux.HandleFunc("DELETE "+serviceapi.Prefix+"/volumes/{id}", routes.deleteVolume)
	mux.HandleFunc("GET "+serviceapi.Prefix+"/admin/users", routes.listUsers)
	mux.HandleFunc("GET "+serviceapi.Prefix+"/admin/users/{id}/volumes", routes.listUserVolumes)
	return auth.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(w, r)
	})), nil
}

func (h *managementRoutes) principal(r *http.Request, scope string) (Principal, error) {
	p, ok := CurrentPrincipal(r.Context())
	if !ok {
		return Principal{}, ErrUnauthenticated
	}
	if scope == "" {
		return p, h.auth.CheckCurrent(r.Context(), p)
	}
	return p, h.auth.RequireScope(r.Context(), p, scope)
}

func (h *managementRoutes) respond(w http.ResponseWriter, r *http.Request, p Principal, status int, value any) {
	if err := h.auth.CheckCurrent(r.Context(), p); err != nil {
		writeManagementError(w, err)
		return
	}
	writeAuthJSON(w, status, value)
}

func writeManagementError(w http.ResponseWriter, err error) {
	status, code := http.StatusServiceUnavailable, "storage_unavailable"
	switch {
	case errors.Is(err, ErrUnauthenticated):
		status, code = http.StatusUnauthorized, "authentication_required"
	case errors.Is(err, ErrForbidden), errors.Is(err, management.ErrDenied):
		status, code = http.StatusForbidden, "access_denied"
	case errors.Is(err, syscall.EIO):
		status, code = http.StatusServiceUnavailable, "storage_unavailable"
	case errors.Is(err, errAuthRequest), errors.Is(err, syscall.EINVAL), errors.Is(err, syscall.ENOTDIR):
		status, code = http.StatusBadRequest, "invalid_request"
	case errors.Is(err, management.ErrNotFound), errors.Is(err, syscall.ENOENT):
		status, code = http.StatusNotFound, "not_found"
	case errors.Is(err, management.ErrConflict), errors.Is(err, syscall.EBUSY):
		status, code = http.StatusConflict, "condition_conflict"
	case errors.Is(err, management.ErrExpired):
		status, code = http.StatusGone, "acceptance_expired"
	case errors.Is(err, management.ErrCapacity), errors.Is(err, ErrAuthCapacity):
		status, code = http.StatusTooManyRequests, "resource_capacity"
	}
	if status == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "1")
	}
	writeAuthJSON(w, status, serviceapi.ErrorResponse{Error: serviceapi.ErrorDetail{Code: code}})
}

func managementID(raw string) (management.ID, error) {
	id := management.ID(raw)
	if !id.Valid() {
		return "", errAuthRequest
	}
	return id, nil
}

func onlyQuery(query url.Values, allowed ...string) error {
	for key, values := range query {
		valid := false
		for _, candidate := range allowed {
			if key == candidate {
				valid = true
				break
			}
		}
		if !valid || len(values) != 1 {
			return errAuthRequest
		}
	}
	return nil
}

func managementPage(r *http.Request) (management.ID, int, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || onlyQuery(query, "after", "limit") != nil {
		return "", 0, errAuthRequest
	}
	var after management.ID
	if value, exists := query["after"]; exists {
		after, err = managementID(value[0])
		if err != nil {
			return "", 0, err
		}
	}
	limit := managementPageLimit
	if value, exists := query["limit"]; exists {
		limit, err = strconv.Atoi(value[0])
		if err != nil || strconv.Itoa(limit) != value[0] || limit < 1 || limit > managementPageLimit {
			return "", 0, errAuthRequest
		}
	}
	return after, limit, nil
}

func noManagementQuery(r *http.Request) error {
	if r.URL.RawQuery != "" {
		return errAuthRequest
	}
	return nil
}

func decodeManagementRequest(w http.ResponseWriter, r *http.Request, target any) error {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return errAuthRequest
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64*1024))
	if err != nil {
		return errAuthRequest
	}
	if err := serviceapi.DecodeJSON(body, target); err != nil {
		return errAuthRequest
	}
	return nil
}

func (h *managementRoutes) me(w http.ResponseWriter, r *http.Request) {
	p, err := h.principal(r, "")
	if err == nil {
		err = noManagementQuery(r)
	}
	if err != nil {
		writeManagementError(w, err)
		return
	}
	h.respond(w, r, p, http.StatusOK, serviceapi.User{ID: string(p.UserID), DisplayName: p.DisplayName, Role: string(p.Role)})
}
