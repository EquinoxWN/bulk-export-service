package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/job"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func registry(t *testing.T) dataset.Registry {
	t.Helper()
	other := dataset.Orders
	other.Name = "orders_archive"
	reg, err := dataset.NewRegistry(dataset.Orders, other)
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func do(t *testing.T, h http.Handler, method, path, key, ctype, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if ctype != "" {
		r.Header.Set("Content-Type", ctype)
	}
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func decode(t *testing.T, w *httptest.ResponseRecorder) Status {
	t.Helper()
	var s Status
	if err := json.Unmarshal(w.Body.Bytes(), &s); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return s
}

const body = `{"account_id":7,"dataset":"orders","format":"csv"}`

func TestCreateReturns202ThenReplays(t *testing.T) {
	h := New(job.NewMemory(10), registry(t), t.TempDir(), quiet)
	w := do(t, h, "POST", "/v1/exports", "k1", "application/json", body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("code %d: %s", w.Code, w.Body)
	}
	s := decode(t, w)
	if s.Status != job.Queued || s.Rows != 0 || s.StatusURL != "/v1/exports/"+s.ID || w.Header().Get("Location") != s.StatusURL || s.FileURL != "" {
		t.Fatalf("status %+v, location %q", s, w.Header().Get("Location"))
	}
	w = do(t, h, "POST", "/v1/exports", "k1", "application/json; charset=utf-8", body)
	if w.Code != http.StatusOK || w.Header().Get("Idempotent-Replayed") != "true" || decode(t, w).ID != s.ID {
		t.Fatalf("replay: %d %s", w.Code, w.Body)
	}
	w = do(t, h, "POST", "/v1/exports", "k1", "application/json", `{"account_id":7,"dataset":"orders_archive","format":"csv"}`)
	if w.Code != http.StatusUnprocessableEntity || !strings.Contains(w.Body.String(), "idempotency_key_reused") {
		t.Fatalf("key reuse: %d %s", w.Code, w.Body)
	}
	w = do(t, h, "GET", s.StatusURL, "", "", "")
	if w.Code != http.StatusOK || decode(t, w).ID != s.ID {
		t.Fatalf("status: %d %s", w.Code, w.Body)
	}
}

func TestCreateRejectsBadRequests(t *testing.T) {
	h := New(job.NewMemory(10), registry(t), t.TempDir(), quiet)
	cases := []struct {
		name, key, ctype, body string
		code                   int
		kind                   string
	}{
		{"no content type", "k", "", body, 415, "unsupported_media_type"},
		{"text content type", "k", "text/plain", body, 415, "unsupported_media_type"},
		{"no key", "", "application/json", body, 400, "bad_idempotency_key"},
		{"bad key", "a b", "application/json", body, 400, "bad_idempotency_key"},
		{"bad json", "k", "application/json", `{"account_id":`, 400, "bad_json"},
		{"unknown field", "k", "application/json", `{"account_id":7,"dataset":"orders","format":"csv","x":1}`, 400, "bad_json"},
		{"trailing data", "k", "application/json", body + `{}`, 400, "bad_json"},
		{"too large", "k", "application/json", `{"dataset":"` + strings.Repeat("a", MaxBody) + `"}`, 413, "too_large"},
		{"account 0", "k", "application/json", `{"account_id":0,"dataset":"orders","format":"csv"}`, 400, "invalid_request"},
		{"unknown dataset", "k", "application/json", `{"account_id":7,"dataset":"users","format":"csv"}`, 400, "invalid_request"},
		{"parquet", "k", "application/json", `{"account_id":7,"dataset":"orders","format":"parquet"}`, 400, "invalid_request"},
		{"empty body", "k", "application/json", ``, 400, "bad_json"},
	}
	for _, c := range cases {
		w := do(t, h, "POST", "/v1/exports", c.key, c.ctype, c.body)
		if w.Code != c.code || !strings.Contains(w.Body.String(), `"code":"`+c.kind+`"`) {
			t.Errorf("%s: %d %s, want %d %s", c.name, w.Code, w.Body, c.code, c.kind)
		}
	}
	if w := do(t, h, "GET", "/v1/exports/"+strings.Repeat("a", 32), "", "", ""); w.Code != 404 {
		t.Errorf("no job was stored by the rejected requests, yet GET answers %d", w.Code)
	}
}

// broken is a store whose every call fails.
type broken struct{ job.Store }

func (broken) Create(context.Context, string, job.Request) (job.Job, bool, error) {
	return job.Job{}, false, errors.New("db down")
}
func (broken) Get(context.Context, string) (job.Job, error) { return job.Job{}, errors.New("db down") }

func TestStoreFailures(t *testing.T) {
	full := job.NewMemory(1)
	h := New(full, registry(t), t.TempDir(), quiet)
	do(t, h, "POST", "/v1/exports", "a", "application/json", body)
	if w := do(t, h, "POST", "/v1/exports", "b", "application/json", body); w.Code != 503 {
		t.Fatalf("full store: %d", w.Code)
	}
	h = New(broken{}, registry(t), t.TempDir(), quiet)
	if w := do(t, h, "POST", "/v1/exports", "a", "application/json", body); w.Code != 500 || strings.Contains(w.Body.String(), "db down") {
		t.Fatalf("store error: %d %s (internal errors must not leak)", w.Code, w.Body)
	}
	if w := do(t, h, "GET", "/v1/exports/"+strings.Repeat("a", 32), "", "", ""); w.Code != 500 {
		t.Fatalf("store error on GET: %d", w.Code)
	}
}

func TestStatusAndDownload(t *testing.T) {
	ctx := context.Background()
	store := job.NewMemory(10)
	dir := t.TempDir()
	h := New(store, registry(t), dir, quiet)
	for _, p := range []string{"/v1/exports/nope", "/v1/exports/" + strings.Repeat("a", 32), "/v1/exports/..%2f..%2fetc/file"} {
		if w := do(t, h, "GET", p, "", "", ""); w.Code != 404 {
			t.Errorf("GET %s: %d", p, w.Code)
		}
	}
	s := decode(t, do(t, h, "POST", "/v1/exports", "k", "application/json", body))
	if w := do(t, h, "GET", s.StatusURL+"/file", "", "", ""); w.Code != 409 || !strings.Contains(w.Body.String(), "queued") {
		t.Fatalf("download before done: %d %s", w.Code, w.Body)
	}
	j, _, _ := store.Claim(ctx, "w")
	content := "id\n1\n"
	if err := os.WriteFile(filepath.Join(dir, j.ID+".csv"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	res := job.Result{Progress: job.Progress{Rows: 1, Pages: 1}, Bytes: int64(len(content)), SHA256: strings.Repeat("c", 64), File: j.ID + ".csv"}
	if err := store.Finish(ctx, j.ID, "w", res); err != nil {
		t.Fatal(err)
	}
	st := decode(t, do(t, h, "GET", s.StatusURL, "", "", ""))
	if st.Status != job.Succeeded || st.FileURL != s.StatusURL+"/file" || st.Rows != 1 || st.FinishedAt == nil {
		t.Fatalf("status after finish %+v", st)
	}
	w := do(t, h, "GET", st.FileURL, "", "", "")
	if w.Code != 200 || w.Body.String() != content || w.Header().Get("Content-Type") != "text/csv; charset=utf-8" ||
		w.Header().Get("X-Content-SHA256") != res.SHA256 || !strings.Contains(w.Header().Get("Content-Disposition"), "attachment") {
		t.Fatalf("download: %d %q %v", w.Code, w.Body, w.Header())
	}
	if err := os.Remove(filepath.Join(dir, j.ID+".csv")); err != nil {
		t.Fatal(err)
	}
	if w := do(t, h, "GET", st.FileURL, "", "", ""); w.Code != http.StatusGone {
		t.Fatalf("missing file: %d", w.Code)
	}
}

// renamed stores a succeeded job whose file name does not match its id.
type renamed struct{ job.Store }

func (renamed) Get(_ context.Context, id string) (job.Job, error) {
	return job.Job{ID: id, Status: job.Succeeded, File: "../../secret.csv"}, nil
}

func TestDownloadRefusesStoredPathsOutsideTheJob(t *testing.T) {
	h := New(renamed{}, registry(t), t.TempDir(), quiet)
	if w := do(t, h, "GET", "/v1/exports/"+strings.Repeat("b", 32)+"/file", "", "", ""); w.Code != 500 {
		t.Fatalf("stored path traversal: %d", w.Code)
	}
}

func TestHealthz(t *testing.T) {
	h := New(job.NewMemory(1), registry(t), t.TempDir(), quiet)
	if w := do(t, h, "GET", "/healthz", "", "", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "ok") {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if w := do(t, h, "DELETE", "/v1/exports", "", "", ""); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE: %d", w.Code)
	}
}
