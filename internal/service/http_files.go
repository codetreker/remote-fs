package service

import (
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"
	"syscall"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/internal/serviceapi"
	"github.com/codetreker/remote-fs/packages/storage"
)

func (s *Service) listFiles(w http.ResponseWriter, r *http.Request) {
	p, ok := CurrentPrincipal(r.Context())
	if !ok {
		writeManagementError(w, ErrUnauthenticated)
		return
	}
	id, err := managementID(r.PathValue("id"))
	if err != nil {
		writeManagementError(w, err)
		return
	}
	if err := s.auth.CheckFileAccess(r.Context(), p, id); err != nil {
		writeManagementError(w, err)
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || onlyQuery(query, "path") != nil {
		writeManagementError(w, errAuthRequest)
		return
	}
	encoded := query.Get("path")
	raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.StdEncoding.EncodeToString(raw) != encoded || !fileMetadataPath(string(raw)) {
		writeManagementError(w, errAuthRequest)
		return
	}
	volume, err := s.maintenance.store.GetVolume(r.Context(), p.UserID, id)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	if volume.State != management.VolumeActive {
		writeManagementError(w, syscall.EBUSY)
		return
	}
	lease, err := s.runtimes.acquire(r.Context(), volume)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	defer lease.Release()
	attr, err := lease.Store.Stat(r.Context(), string(raw))
	if err != nil {
		writeManagementError(w, err)
		return
	}
	if !attr.IsDir() {
		writeManagementError(w, syscall.ENOTDIR)
		return
	}
	entries, err := lease.Store.List(r.Context(), string(raw))
	if err != nil {
		writeManagementError(w, err)
		return
	}
	result := serviceapi.FileDirectory{PathBase64: encoded, Entries: make([]serviceapi.FileEntry, 0, len(entries))}
	for _, entry := range entries {
		kind, err := fileMetadataKind(entry.Attr.Kind)
		if err != nil || entry.Attr.ID == 0 || entry.Attr.Size < 0 || !fileMetadataLeaf(entry.Name) {
			writeManagementError(w, syscall.EIO)
			return
		}
		result.Entries = append(result.Entries, serviceapi.FileEntry{
			Name: strings.ToValidUTF8(entry.Name, "\ufffd"), NameBase64: base64.StdEncoding.EncodeToString([]byte(entry.Name)),
			NodeID: serviceapi.DecimalFromUint64(entry.Attr.ID),
			Kind:   kind, SizeBytes: serviceapi.DecimalFromInt64(entry.Attr.Size), ModifiedAt: entry.Attr.ModTime,
		})
	}
	if err := s.auth.CheckFileAccess(r.Context(), p, id); err != nil {
		writeManagementError(w, err)
		return
	}
	writeAuthJSON(w, http.StatusOK, result)
}

func fileMetadataPath(path string) bool {
	if path == "" {
		return true
	}
	for _, leaf := range strings.Split(path, "/") {
		if !fileMetadataLeaf(leaf) {
			return false
		}
	}
	return true
}
func fileMetadataLeaf(leaf string) bool {
	return leaf != "" && leaf != "." && leaf != ".." && !strings.ContainsAny(leaf, "/\x00")
}
func fileMetadataKind(kind storage.NodeKind) (string, error) {
	switch kind {
	case storage.NodeRegular:
		return "file", nil
	case storage.NodeDirectory:
		return "directory", nil
	case storage.NodeSymlink:
		return "symlink", nil
	default:
		return "", syscall.EIO
	}
}
