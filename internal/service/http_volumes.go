package service

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (h *managementRoutes) owner(r *http.Request, scope string) (Principal, management.ID, error) {
	p, err := h.principal(r, scope)
	if err != nil {
		return p, "", err
	}
	id, err := managementID(r.PathValue("id"))
	if err != nil {
		return p, "", err
	}
	if !p.AllowsVolume(id) {
		return p, "", ErrForbidden
	}
	return p, id, h.auth.CheckVolumeOwner(r.Context(), p, id)
}

func volumeDTO(volume management.Volume) serviceapi.Volume {
	return serviceapi.Volume{ID: string(volume.ID), OwnerUserID: string(volume.OwnerUserID), Name: volume.DisplayName, State: string(volume.State), Revision: serviceapi.DecimalFromInt64(volume.ManagementRevision), LastOperationID: string(volume.LastOperationID), CreatedAt: volume.CreatedAt, UpdatedAt: volume.UpdatedAt}
}

func (h *managementRoutes) volumeSummaries(ctx context.Context, volumes []management.Volume) []serviceapi.Volume {
	result := make([]serviceapi.Volume, len(volumes))
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	jobs := make(chan int)
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				volume := volumes[index]
				value := volumeDTO(volume)
				if volume.State != management.VolumeActive {
					value.SummaryError = &serviceapi.ErrorDetail{Code: "volume_unavailable"}
				} else {
					select {
					case h.summaryWorkers <- struct{}{}:
					case <-ctx.Done():
						value.SummaryError = &serviceapi.ErrorDetail{Code: "metadata_unavailable"}
						result[index] = value
						continue
					}
					p, ok := CurrentPrincipal(ctx)
					var err error
					if !ok {
						err = ErrUnauthenticated
					} else {
						err = h.auth.CheckCurrent(ctx, p)
					}
					var summary storage.VolumeSummaryResult
					if err == nil {
						summary, err = h.summary(ctx, volume)
					}
					<-h.summaryWorkers
					if err == nil {
						err = summary.Check()
					}
					if err != nil {
						value.SummaryError = &serviceapi.ErrorDetail{Code: "metadata_unavailable"}
					} else {
						value.Summary = &serviceapi.VolumeSummary{FileCount: serviceapi.DecimalFromUint64(summary.FileCount), DirectoryCount: serviceapi.DecimalFromUint64(summary.DirectoryCount), UsedBytes: serviceapi.DecimalFromInt64(summary.AllocationUsed), QuotaBytes: serviceapi.DecimalFromInt64(summary.QuotaBytes), QuotaRevision: serviceapi.DecimalFromUint64(summary.QuotaRevision)}
					}
				}
				result[index] = value
			}
		}()
	}
	for index := range volumes {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	return result
}

func (h *managementRoutes) selectedVolumes(ctx context.Context, p Principal, after management.ID, limit int) ([]management.Volume, error) {
	if p.Kind == "browser" || p.AllOwnedVolumes {
		return h.store.ListVolumes(ctx, p.UserID, after, limit)
	}
	ids := append([]management.ID(nil), p.VolumeIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]management.Volume, 0, limit)
	for _, id := range ids {
		if id <= after {
			continue
		}
		volume, err := h.store.GetVolume(ctx, p.UserID, id)
		if err != nil {
			return nil, err
		}
		result = append(result, volume)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (h *managementRoutes) listVolumes(w http.ResponseWriter, r *http.Request) {
	p, err := h.principal(r, ScopeVolumesRead)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	after, limit, err := managementPage(r)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	volumes, err := h.selectedVolumes(r.Context(), p, after, limit)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	response := serviceapi.VolumeList{Volumes: h.volumeSummaries(r.Context(), volumes)}
	if len(volumes) == limit {
		response.NextAfter = string(volumes[len(volumes)-1].ID)
	}
	h.respond(w, r, p, http.StatusOK, response)
}

func (h *managementRoutes) getVolume(w http.ResponseWriter, r *http.Request) {
	p, id, err := h.owner(r, ScopeVolumesRead)
	if err == nil {
		err = noManagementQuery(r)
	}
	if err != nil {
		writeManagementError(w, err)
		return
	}
	volume, err := h.store.GetVolume(r.Context(), p.UserID, id)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	h.respond(w, r, p, http.StatusOK, h.volumeSummaries(r.Context(), []management.Volume{volume})[0])
}

func (h *managementRoutes) createVolume(w http.ResponseWriter, r *http.Request) {
	p, err := h.principal(r, ScopeVolumesWrite)
	if err == nil && p.Kind != "browser" && !p.AllOwnedVolumes {
		err = ErrForbidden
	}
	if err == nil {
		err = noManagementQuery(r)
	}
	if err != nil {
		writeManagementError(w, err)
		return
	}
	var request serviceapi.CreateVolumeRequest
	if err := decodeManagementRequest(w, r, &request); err != nil {
		writeManagementError(w, err)
		return
	}
	action, err := managementAction(request.Action)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	quota, err := request.QuotaBytes.Int64()
	if err != nil {
		writeManagementError(w, errAuthRequest)
		return
	}
	if err := h.auth.CheckCurrent(r.Context(), p); err != nil {
		writeManagementError(w, err)
		return
	}
	operation, err := h.store.CreateVolume(r.Context(), p.UserID, action, request.Name, quota)
	h.acceptOperation(w, r, p, operation, err)
}

func (h *managementRoutes) renameVolume(w http.ResponseWriter, r *http.Request) {
	p, id, err := h.owner(r, ScopeVolumesWrite)
	if err == nil {
		err = noManagementQuery(r)
	}
	if err != nil {
		writeManagementError(w, err)
		return
	}
	var request serviceapi.RenameVolumeRequest
	if err := decodeManagementRequest(w, r, &request); err != nil {
		writeManagementError(w, err)
		return
	}
	action, err := managementAction(request.Action)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	revision, err := request.ExpectedRevision.Int64()
	if err != nil {
		writeManagementError(w, errAuthRequest)
		return
	}
	if err := h.auth.CheckCurrent(r.Context(), p); err != nil {
		writeManagementError(w, err)
		return
	}
	operation, err := h.store.RenameVolume(r.Context(), p.UserID, action, id, revision, request.Name)
	h.acceptOperation(w, r, p, operation, err)
}

func (h *managementRoutes) setQuota(w http.ResponseWriter, r *http.Request) {
	p, id, err := h.owner(r, ScopeVolumesWrite)
	if err == nil {
		err = noManagementQuery(r)
	}
	if err != nil {
		writeManagementError(w, err)
		return
	}
	var request serviceapi.SetQuotaRequest
	if err := decodeManagementRequest(w, r, &request); err != nil {
		writeManagementError(w, err)
		return
	}
	action, err := managementAction(request.Action)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	revision, err := request.ExpectedRevision.Int64()
	if err != nil {
		writeManagementError(w, errAuthRequest)
		return
	}
	quota, err := request.QuotaBytes.Int64()
	if err != nil {
		writeManagementError(w, errAuthRequest)
		return
	}
	if err := h.auth.CheckCurrent(r.Context(), p); err != nil {
		writeManagementError(w, err)
		return
	}
	operation, err := h.store.ChangeQuota(r.Context(), p.UserID, action, id, revision, quota)
	h.acceptOperation(w, r, p, operation, err)
}

func (h *managementRoutes) deleteVolume(w http.ResponseWriter, r *http.Request) {
	p, id, err := h.owner(r, ScopeVolumesWrite)
	if err == nil {
		err = noManagementQuery(r)
	}
	if err != nil {
		writeManagementError(w, err)
		return
	}
	var request serviceapi.DeleteVolumeRequest
	if err := decodeManagementRequest(w, r, &request); err != nil {
		writeManagementError(w, err)
		return
	}
	action, err := managementAction(request.Action)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	revision, err := request.ExpectedRevision.Int64()
	if err != nil {
		writeManagementError(w, errAuthRequest)
		return
	}
	if err := h.auth.CheckCurrent(r.Context(), p); err != nil {
		writeManagementError(w, err)
		return
	}
	operation, err := h.store.DeleteVolume(r.Context(), p.UserID, action, id, revision)
	h.acceptOperation(w, r, p, operation, err)
}
