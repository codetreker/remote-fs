package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/codetreker/remote-fs/internal/management"
	"github.com/codetreker/remote-fs/packages/storage"
	"github.com/codetreker/remote-fs/packages/transport/httprest"
)

func (s *Service) serveVolume(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/api/volumes/"), "/", 2)
	if len(parts) != 2 || parts[0] == "" || !strings.HasPrefix("/"+parts[1], httprest.Prefix) {
		http.NotFound(w, r)
		return
	}
	id := management.ID(parts[0])
	p, err := s.auth.Authenticate(r.Context(), r)
	if err != nil {
		writeFileAdmissionError(w, err)
		return
	}
	ctx := WithPrincipal(r.Context(), p)
	if err := s.auth.CheckFileAccess(ctx, p, id); err != nil {
		writeFileAdmissionError(w, err)
		return
	}
	volume, err := s.maintenance.store.GetVolume(ctx, p.UserID, id)
	if err != nil {
		writeFileAdmissionError(w, err)
		return
	}
	if volume.State != management.VolumeActive {
		writeFileAdmissionError(w, syscall.EBUSY)
		return
	}
	output, cancel, err := s.auth.OutputContext(ctx, p)
	if err != nil {
		writeFileAdmissionError(w, err)
		return
	}
	defer cancel()
	lease, err := s.runtimes.acquireFile(output, volume, false)
	if err != nil {
		writeFileAdmissionError(w, err)
		return
	}
	defer lease.Release()
	forward := r.Clone(output)
	urlCopy := *r.URL
	urlCopy.Path = "/" + parts[1]
	urlCopy.RawPath = ""
	forward.URL = &urlCopy
	writer := &accessResponseWriter{ResponseWriter: w, auth: s.auth, ctx: output, principal: p, volumeID: id}
	if strings.HasSuffix(urlCopy.Path, "/file-transfer-data") {
		controller := http.NewResponseController(w)
		interrupt := context.AfterFunc(output, func() { _ = controller.SetReadDeadline(time.Now()) })
		defer func() { interrupt(); _ = controller.SetReadDeadline(time.Time{}) }()
	}
	lease.Handler.ServeHTTP(writer, forward)
}

func writeFileAdmissionError(w http.ResponseWriter, err error) {
	errno := storage.ErrnoOf(err)
	if errors.Is(err, ErrForbidden) || errors.Is(err, ErrUnauthenticated) || errors.Is(err, management.ErrDenied) {
		errno = syscall.EACCES
	}
	if errors.Is(err, management.ErrNotFound) {
		errno = syscall.ENOENT
	}
	if errors.Is(err, ErrAuthCapacity) {
		errno = syscall.EAGAIN
	}
	w.Header().Set(httprest.HeaderProtocol, httprest.Version)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httprest.StatusStorageError)
	_ = json.NewEncoder(w).Encode(httprest.ErrorResponse{Errno: storage.ErrnoNameOf(errno), Message: errno.Error()})
}

type accessResponseWriter struct {
	http.ResponseWriter
	auth      *Authenticator
	ctx       context.Context
	principal Principal
	volumeID  management.ID
}

func (w *accessResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *accessResponseWriter) check() error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if err := w.auth.CheckFileAccess(w.ctx, w.principal, w.volumeID); err != nil {
		return err
	}
	return w.auth.CheckOutput(w.ctx, w.ResponseWriter)
}

func (w *accessResponseWriter) Write(body []byte) (int, error) {
	written := 0
	for len(body) > 0 {
		if err := w.check(); err != nil {
			return written, err
		}
		unit := min(len(body), 64<<10)
		n, err := w.ResponseWriter.Write(body[:unit])
		written += n
		body = body[n:]
		if err != nil {
			return written, err
		}
		if n != unit {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}

func (w *accessResponseWriter) Flush() { _ = w.FlushError() }
func (w *accessResponseWriter) FlushError() error {
	if err := w.check(); err != nil {
		return err
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}
