package service

import (
	"net/http"
	"net/url"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
)

func operationDTO(operation management.Operation) serviceapi.Operation {
	value := serviceapi.Operation{ID: string(operation.ID), VolumeID: string(operation.VolumeID), Kind: string(operation.Kind), State: string(operation.State), Phase: string(operation.Phase), Revision: serviceapi.DecimalFromInt64(operation.Revision), Settled: operation.Settled, CreatedAt: operation.CreatedAt, UpdatedAt: operation.UpdatedAt}
	if operation.ErrorCode != "" {
		code := "storage_unavailable"
		switch operation.ErrorCode {
		case "volume-busy", "condition_conflict":
			code = "condition_conflict"
		case "resource_capacity", "history_expired", "acceptance_expired", "access_denied", "invalid_request":
			code = operation.ErrorCode
		}
		value.Error = &serviceapi.ErrorDetail{Code: code}
	}
	return value
}

func (h *managementRoutes) acceptOperation(w http.ResponseWriter, r *http.Request, p Principal, operation management.Operation, err error) {
	if err != nil {
		writeManagementError(w, err)
		return
	}
	status := http.StatusAccepted
	if operation.State == management.OperationSucceeded || operation.State == management.OperationFailed {
		status = http.StatusOK
	} else if operation.State == management.OperationPending {
		h.accepted(operation)
	}
	w.Header().Set("Location", serviceapi.Prefix+"/operations/"+string(operation.ID))
	h.respond(w, r, p, status, operationDTO(operation))
}

func (h *managementRoutes) issueNamespace(w http.ResponseWriter, r *http.Request) {
	p, err := h.principal(r, "")
	if err == nil && !p.HasScope(ScopeVolumesWrite) && !p.HasScope(ScopeFiles) {
		err = ErrForbidden
	}
	if err == nil {
		err = noManagementQuery(r)
	}
	if err != nil {
		writeManagementError(w, err)
		return
	}
	var request struct{}
	if err := decodeManagementRequest(w, r, &request); err != nil {
		writeManagementError(w, err)
		return
	}
	if err := h.auth.CheckCurrent(r.Context(), p); err != nil {
		writeManagementError(w, err)
		return
	}
	namespace, err := h.issuer(r.Context(), p.UserID)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	if namespace.Actor != p.UserID || namespace.Sequence < 1 || namespace.AcceptUntil.IsZero() {
		writeManagementError(w, management.ErrCorrupt)
		return
	}
	h.respond(w, r, p, http.StatusCreated, serviceapi.ActionNamespace{SubjectID: string(namespace.Actor), Namespace: serviceapi.DecimalFromInt64(namespace.Sequence), AcceptUntil: namespace.AcceptUntil})
}

func (h *managementRoutes) getOperation(w http.ResponseWriter, r *http.Request) {
	p, err := h.principal(r, ScopeVolumesRead)
	if err == nil {
		err = noManagementQuery(r)
	}
	if err != nil {
		writeManagementError(w, err)
		return
	}
	id, err := managementID(r.PathValue("id"))
	if err != nil {
		writeManagementError(w, err)
		return
	}
	operation, err := h.store.Operation(r.Context(), p.UserID, id)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	if !p.AllowsVolume(operation.VolumeID) {
		writeManagementError(w, ErrForbidden)
		return
	}
	h.respond(w, r, p, http.StatusOK, operationDTO(operation))
}

func (h *managementRoutes) findOperation(w http.ResponseWriter, r *http.Request) {
	p, err := h.principal(r, ScopeVolumesRead)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || onlyQuery(query, "namespace", "nonce") != nil || len(query["namespace"]) != 1 || len(query["nonce"]) != 1 {
		writeManagementError(w, errAuthRequest)
		return
	}
	action, err := managementAction(serviceapi.Action{Namespace: serviceapi.Decimal(query.Get("namespace")), Nonce: query.Get("nonce")})
	if err != nil {
		writeManagementError(w, err)
		return
	}
	operation, err := h.store.FindOperation(r.Context(), p.UserID, action)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	if !p.AllowsVolume(operation.VolumeID) {
		writeManagementError(w, ErrForbidden)
		return
	}
	h.respond(w, r, p, http.StatusOK, operationDTO(operation))
}
