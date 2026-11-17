// Package api is the HTTP interface: create an export job (202 at once), poll its status, download the file.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/job"
)

// MaxBody bounds a create request body.
const MaxBody = 4 << 10

// Handler serves the export API.
type Handler struct {
	jobs   job.Store
	reg    dataset.Registry
	outDir string
	log    *slog.Logger
	mux    *http.ServeMux
}

// New wires the routes.
func New(jobs job.Store, reg dataset.Registry, outDir string, log *slog.Logger) *Handler {
	h := &Handler{jobs: jobs, reg: reg, outDir: outDir, log: log, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /v1/exports", h.create)
	h.mux.HandleFunc("GET /v1/exports/{id}", h.status)
	h.mux.HandleFunc("GET /v1/exports/{id}/file", h.download)
	h.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return h
}

// ServeHTTP routes a request.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// Status is the JSON view of a job.
type Status struct {
	ID         string     `json:"id"`
	Status     job.Status `json:"status"`
	AccountID  int64      `json:"account_id"`
	Dataset    string     `json:"dataset"`
	Format     string     `json:"format"`
	Rows       int64      `json:"rows"`
	Pages      int64      `json:"pages"`
	Bytes      int64      `json:"bytes,omitempty"`
	SHA256     string     `json:"sha256,omitempty"`
	Error      string     `json:"error,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	StatusURL  string     `json:"status_url"`
	FileURL    string     `json:"file_url,omitempty"`
}

func view(j job.Job) Status {
	s := Status{ID: j.ID, Status: j.Status, AccountID: j.Request.AccountID, Dataset: j.Request.Dataset, Format: j.Request.Format,
		Rows: j.Rows, Pages: j.Pages, Bytes: j.Bytes, SHA256: j.SHA256, Error: j.Error,
		CreatedAt: j.CreatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt, StatusURL: "/v1/exports/" + j.ID}
	if j.Status == job.Succeeded {
		s.FileURL = "/v1/exports/" + j.ID + "/file"
	}
	return s
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	// The status line is already sent; an encode error here means the client went away.
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]apiError{"error": {Code: kind, Message: msg}})
}

// create validates the request and stores a queued job; the export itself runs later on a worker.
func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		fail(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if err := job.ValidateKey(key); err != nil {
		fail(w, http.StatusBadRequest, "bad_idempotency_key", err.Error())
		return
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBody))
	dec.DisallowUnknownFields()
	var req job.Request
	if err := dec.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			fail(w, http.StatusRequestEntityTooLarge, "too_large", fmt.Sprintf("body over %d bytes", MaxBody))
			return
		}
		fail(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		fail(w, http.StatusBadRequest, "bad_json", "trailing data after the JSON object")
		return
	}
	if err := req.Validate(h.reg); err != nil {
		fail(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	j, created, err := h.jobs.Create(r.Context(), key, req)
	switch {
	case errors.Is(err, job.ErrKeyReused):
		fail(w, http.StatusUnprocessableEntity, "idempotency_key_reused", err.Error())
		return
	case errors.Is(err, job.ErrStoreFull):
		fail(w, http.StatusServiceUnavailable, "store_full", err.Error())
		return
	case err != nil:
		h.log.Error("create job", "err", err)
		fail(w, http.StatusInternalServerError, "internal", "could not store the job")
		return
	}
	w.Header().Set("Location", "/v1/exports/"+j.ID)
	if created {
		writeJSON(w, http.StatusAccepted, view(j))
		return
	}
	w.Header().Set("Idempotent-Replayed", "true")
	writeJSON(w, http.StatusOK, view(j))
}

// lookup reads the job named in the path, answering 404 for malformed or unknown ids.
func (h *Handler) lookup(w http.ResponseWriter, r *http.Request) (job.Job, bool) {
	id := r.PathValue("id")
	if !job.ValidID(id) {
		fail(w, http.StatusNotFound, "not_found", "no such export")
		return job.Job{}, false
	}
	j, err := h.jobs.Get(r.Context(), id)
	if errors.Is(err, job.ErrNotFound) {
		fail(w, http.StatusNotFound, "not_found", "no such export")
		return job.Job{}, false
	}
	if err != nil {
		h.log.Error("get job", "err", err)
		fail(w, http.StatusInternalServerError, "internal", "could not read the job")
		return job.Job{}, false
	}
	return j, true
}

// status returns the job's state and progress.
func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	if j, ok := h.lookup(w, r); ok {
		writeJSON(w, http.StatusOK, view(j))
	}
}

// download streams the finished CSV; 409 until the job has succeeded.
func (h *Handler) download(w http.ResponseWriter, r *http.Request) {
	j, ok := h.lookup(w, r)
	if !ok {
		return
	}
	if j.Status != job.Succeeded {
		fail(w, http.StatusConflict, "not_ready", fmt.Sprintf("export is %s", j.Status))
		return
	}
	if j.File != j.ID+".csv" {
		h.log.Error("stored file name does not match the job id", "job", j.ID)
		fail(w, http.StatusInternalServerError, "internal", "bad stored file name")
		return
	}
	f, err := os.Open(filepath.Join(h.outDir, j.File))
	if err != nil {
		h.log.Error("open export file", "job", j.ID, "err", err)
		fail(w, http.StatusGone, "file_missing", "the export file is no longer available")
		return
	}
	defer func() { _ = f.Close() }() // read-only file; a close error changes nothing for the client
	st, err := f.Stat()
	if err != nil {
		fail(w, http.StatusInternalServerError, "internal", "could not stat the export file")
		return
	}
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-%s.csv"`, j.Request.Dataset, j.ID))
	w.Header().Set("X-Content-SHA256", j.SHA256)
	http.ServeContent(w, r, "", st.ModTime(), f)
}
