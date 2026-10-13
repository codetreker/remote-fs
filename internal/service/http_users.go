package service

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
)

type adminSummaryCursor struct {
	Actor     management.ID
	Target    management.ID
	After     management.ID
	ExpiresAt time.Time
}

func (h *managementRoutes) summaryCursor(actor, target, after management.ID) (string, error) {
	plain, err := json.Marshal(adminSummaryCursor{Actor: actor, Target: target, After: after, ExpiresAt: time.Now().Add(10 * time.Minute)})
	if err != nil {
		return "", err
	}
	nonce := make([]byte, h.cursors.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(h.cursors.Seal(nonce, nonce, plain, []byte("admin-volume-summary"))), nil
}

func (h *managementRoutes) summaryPage(r *http.Request, actor, target management.ID) (management.ID, int, error) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || onlyQuery(query, "after", "limit") != nil {
		return "", 0, errAuthRequest
	}
	limit := managementPageLimit
	if values, present := query["limit"]; present {
		limit, err = strconv.Atoi(values[0])
		if err != nil || strconv.Itoa(limit) != values[0] || limit < 1 || limit > managementPageLimit {
			return "", 0, errAuthRequest
		}
	}
	if values, present := query["after"]; present {
		if len(values[0]) > 1024 {
			return "", 0, errAuthRequest
		}
		sealed, err := base64.RawURLEncoding.DecodeString(values[0])
		if err != nil || base64.RawURLEncoding.EncodeToString(sealed) != values[0] || len(sealed) < h.cursors.NonceSize()+h.cursors.Overhead() {
			return "", 0, errAuthRequest
		}
		plain, err := h.cursors.Open(nil, sealed[:h.cursors.NonceSize()], sealed[h.cursors.NonceSize():], []byte("admin-volume-summary"))
		if err != nil {
			return "", 0, errAuthRequest
		}
		var cursor adminSummaryCursor
		if err := json.Unmarshal(plain, &cursor); err != nil || cursor.Actor != actor || cursor.Target != target || !cursor.After.Valid() || !time.Now().Before(cursor.ExpiresAt) {
			return "", 0, errAuthRequest
		}
		return cursor.After, limit, nil
	}
	return "", limit, nil
}

func (h *managementRoutes) admin(r *http.Request) (Principal, error) {
	p, err := h.principal(r, ScopeAdminSummaries)
	if err == nil && p.Role != management.RoleAdmin {
		err = ErrForbidden
	}
	return p, err
}

func (h *managementRoutes) listUsers(w http.ResponseWriter, r *http.Request) {
	p, err := h.admin(r)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	after, limit, err := managementPage(r)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	users, err := h.store.ListOrdinaryUsers(r.Context(), after, limit)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	response := serviceapi.UserList{Users: make([]serviceapi.User, len(users))}
	for index, user := range users {
		response.Users[index] = serviceapi.User{ID: string(user.ID), DisplayName: user.DisplayName, Role: string(user.Role)}
	}
	if len(users) == limit {
		response.NextAfter = string(users[len(users)-1].ID)
	}
	h.respond(w, r, p, http.StatusOK, response)
}

func (h *managementRoutes) listUserVolumes(w http.ResponseWriter, r *http.Request) {
	p, err := h.admin(r)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	id, err := managementID(r.PathValue("id"))
	if err != nil {
		writeManagementError(w, err)
		return
	}
	user, err := h.store.GetUser(r.Context(), id)
	if err == nil && user.Role != management.RoleUser {
		err = ErrForbidden
	}
	if err != nil {
		writeManagementError(w, err)
		return
	}
	after, limit, err := h.summaryPage(r, p.UserID, id)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	volumes, err := h.store.ListVolumes(r.Context(), id, after, limit)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	summaries := h.volumeSummaries(r.Context(), volumes)
	response := serviceapi.UserVolumeList{Volumes: make([]serviceapi.UserVolumeSummary, len(summaries))}
	for index, summary := range summaries {
		value := serviceapi.UserVolumeSummary{Name: summary.Name, SummaryError: summary.SummaryError}
		if summary.Summary != nil {
			value.Summary = &serviceapi.UserVolumeStats{FileCount: summary.Summary.FileCount, DirectoryCount: summary.Summary.DirectoryCount, UsedBytes: summary.Summary.UsedBytes, QuotaBytes: summary.Summary.QuotaBytes}
		}
		response.Volumes[index] = value
	}
	if len(volumes) == limit {
		response.NextAfter, err = h.summaryCursor(p.UserID, id, volumes[len(volumes)-1].ID)
		if err != nil {
			writeManagementError(w, err)
			return
		}
	}
	h.respond(w, r, p, http.StatusOK, response)
}
