package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/job"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
	"github.com/EquinoxWN/bulk-export-service/internal/sample"
	"github.com/EquinoxWN/bulk-export-service/internal/source"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func cfg(t *testing.T) Config {
	return Config{ID: "w1", OutDir: t.TempDir(), PageSize: 100, PageTimeout: time.Second, Retries: 2,
		Backoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond, Idle: time.Millisecond, FormulaGuard: true}
}

// failing fails the first n reads with err, then reads from the inner source.
type failing struct {
	inner source.Reader
	n     int
	err   error
	mu    sync.Mutex
	calls int
}

func (f *failing) ReadPage(ctx context.Context, ds *dataset.Dataset, a int64, after keyset.Cursor, limit int) (source.Page, error) {
	f.mu.Lock()
	f.calls++
	fail := f.calls <= f.n
	f.mu.Unlock()
	if fail {
		return source.Page{}, f.err
	}
	return f.inner.ReadPage(ctx, ds, a, after, limit)
}

// recording wraps a store and keeps every progress call.
type recording struct {
	job.Store
	mu       sync.Mutex
	progress []job.Progress
	lose     bool // Progress answers ErrNotOwner
}

func (r *recording) Progress(ctx context.Context, id, w string, p job.Progress) error {
	if r.lose {
		return job.ErrNotOwner
	}
	r.mu.Lock()
	r.progress = append(r.progress, p)
	r.mu.Unlock()
	return r.Store.Progress(ctx, id, w, p)
}

func setup(t *testing.T, accounts, per int) (*source.Memory, *recording, string) {
	t.Helper()
	rows, err := sample.Orders(accounts, per, 9)
	if err != nil {
		t.Fatal(err)
	}
	mem := source.NewMemory()
	if err := mem.Insert(&dataset.Orders, rows...); err != nil {
		t.Fatal(err)
	}
	store := &recording{Store: job.NewMemory(100)}
	j, _, err := store.Create(context.Background(), "k", job.Request{AccountID: 1, Dataset: "orders", Format: "csv"})
	if err != nil {
		t.Fatal(err)
	}
	return mem, store, j.ID
}

func newWorker(t *testing.T, c Config, s job.Store, src source.Reader) *Worker {
	t.Helper()
	w, err := New(c, s, src, dataset.Default(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	w.sleep = func(ctx context.Context, d time.Duration) error { return ctx.Err() } // no real waiting in tests
	return w
}

func readCSV(t *testing.T, path string) ([][]string, []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := csv.NewReader(bytes.NewReader(b)).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return recs, b
}

func TestExportWritesEveryRowInKeyOrderWithProgressPerPage(t *testing.T) {
	mem, store, id := setup(t, 2, 450)
	c := cfg(t)
	w := newWorker(t, c, store, mem)
	did, err := w.RunOnce(context.Background())
	if err != nil || !did {
		t.Fatalf("RunOnce: %v %v", did, err)
	}
	j, _ := store.Get(context.Background(), id)
	if j.Status != job.Succeeded || j.Rows != 450 || j.Pages != 5 || j.File != id+".csv" {
		t.Fatalf("job %+v", j)
	}
	recs, raw := readCSV(t, filepath.Join(c.OutDir, j.File))
	if len(recs) != 451 || strings.Join(recs[0], ",") != "id,account_id,created_at,status,amount_cents,note" {
		t.Fatalf("%d records, header %v", len(recs), recs[0])
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != j.SHA256 || int64(len(raw)) != j.Bytes {
		t.Fatal("sha256 or byte count does not match the file")
	}
	prev := ""
	for _, r := range recs[1:] {
		if r[1] != "1" {
			t.Fatalf("row of account %s in account 1's export", r[1])
		}
		k := r[2] + "/" + strings.Repeat("0", 12-len(r[0])) + r[0]
		if k <= prev {
			t.Fatalf("rows not in (created_at, id) order: %s after %s", k, prev)
		}
		prev = k
	}
	// One progress record per page, rows growing, cursor equal to the last row written.
	if len(store.progress) != 5 || store.progress[4].Rows != 450 || store.progress[0].Rows != 100 {
		t.Fatalf("progress %+v", store.progress)
	}
	last := recs[len(recs)-1]
	if c := store.progress[4].Cursor; c[0] != last[2] || c[1] != last[0] {
		t.Fatalf("cursor %v, last row %v", c, last)
	}
	if _, err := os.Stat(filepath.Join(c.OutDir, j.File+".part")); !os.IsNotExist(err) {
		t.Fatal(".part file left behind")
	}
	if did, err := w.RunOnce(context.Background()); did || err != nil {
		t.Fatalf("second RunOnce with no job: %v %v", did, err)
	}
}

func TestEmptyAccountGivesHeaderOnly(t *testing.T) {
	mem, store, id := setup(t, 0, 0)
	c := cfg(t)
	w := newWorker(t, c, store, mem)
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := store.Get(context.Background(), id)
	if got.Status != job.Succeeded || got.Rows != 0 || got.Pages != 1 {
		t.Fatalf("empty export %+v", got)
	}
	recs, _ := readCSV(t, filepath.Join(c.OutDir, got.File))
	if len(recs) != 1 {
		t.Fatalf("%d records, want the header only", len(recs))
	}
}

func TestTransientFailuresAreRetried(t *testing.T) {
	mem, store, id := setup(t, 1, 250)
	src := &failing{inner: mem, n: 2, err: errors.New("connection reset")}
	w := newWorker(t, cfg(t), store, src)
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	j, _ := store.Get(context.Background(), id)
	if j.Status != job.Succeeded || j.Rows != 250 {
		t.Fatalf("job %+v", j)
	}
	if src.calls != 5 { // 2 failures + 3 pages
		t.Fatalf("%d reads, want 5", src.calls)
	}
}

func TestRetriesAreBoundedAndFailureIsRecorded(t *testing.T) {
	mem, store, id := setup(t, 1, 250)
	src := &failing{inner: mem, n: 100, err: errors.New("replica down")}
	c := cfg(t)
	w := newWorker(t, c, store, src)
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	j, _ := store.Get(context.Background(), id)
	if j.Status != job.Failed || !strings.Contains(j.Error, "replica down") || !strings.Contains(j.Error, "attempt 3 of 3") {
		t.Fatalf("job %+v", j)
	}
	if src.calls != 3 {
		t.Fatalf("%d reads, want 1 + 2 retries", src.calls)
	}
	entries, _ := os.ReadDir(c.OutDir)
	if len(entries) != 0 {
		t.Fatalf("files left after a failed export: %v", entries)
	}
}

func TestPermanentErrorsAreNotRetried(t *testing.T) {
	mem, store, id := setup(t, 1, 10)
	src := &failing{inner: mem, n: 100, err: keyset.ErrBadCursor}
	w := newWorker(t, cfg(t), store, src)
	if _, err := w.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	j, _ := store.Get(context.Background(), id)
	if j.Status != job.Failed || src.calls != 1 {
		t.Fatalf("status %s after %d reads, want failed after 1", j.Status, src.calls)
	}
}

func TestCancelledExportIsRecordedAsFailed(t *testing.T) {
	mem, store, id := setup(t, 1, 300)
	ctx, cancel := context.WithCancel(context.Background())
	src := &cancelAfter{inner: mem, cancel: cancel}
	w := newWorker(t, cfg(t), store, src)
	if _, err := w.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	j, _ := store.Get(context.Background(), id)
	if j.Status != job.Failed || !strings.Contains(j.Error, "canceled") {
		t.Fatalf("job %+v", j)
	}
}

// cancelAfter cancels the context after the first page.
type cancelAfter struct {
	inner  source.Reader
	cancel func()
}

func (c *cancelAfter) ReadPage(ctx context.Context, ds *dataset.Dataset, a int64, after keyset.Cursor, limit int) (source.Page, error) {
	p, err := c.inner.ReadPage(ctx, ds, a, after, limit)
	c.cancel()
	return p, err
}

func TestLostOwnershipStopsTheExport(t *testing.T) {
	mem, store, id := setup(t, 1, 300)
	store.lose = true
	w := newWorker(t, cfg(t), store, mem)
	_, err := w.RunOnce(context.Background())
	if !errors.Is(err, job.ErrNotOwner) {
		t.Fatalf("RunOnce: %v", err)
	}
	j, _ := store.Get(context.Background(), id)
	if j.Status != job.Running {
		t.Fatalf("a worker that lost the job must not finish or fail it, got %s", j.Status)
	}
}

func TestFormulaGuard(t *testing.T) {
	w := &Worker{cfg: Config{FormulaGuard: true}}
	row := []any{int64(-5), int64(1), sample.Base, "=cmd", int64(1), "@x"}
	rec, err := w.record(&dataset.Orders, row)
	if err != nil {
		t.Fatal(err)
	}
	if rec[0] != "-5" || rec[3] != "'=cmd" || rec[5] != "'@x" {
		t.Fatalf("guarded record %v (numbers must stay as they are)", rec)
	}
	for _, s := range []string{"+1", "-x", "\tx", "\rx"} {
		row[5] = s
		rec, _ = w.record(&dataset.Orders, row)
		if rec[5] != "'"+s {
			t.Errorf("%q not guarded", s)
		}
	}
	row[5] = "plain"
	if rec, _ = w.record(&dataset.Orders, row); rec[5] != "plain" {
		t.Error("plain text changed")
	}
	w.cfg.FormulaGuard = false
	row[5] = "=x"
	if rec, _ = w.record(&dataset.Orders, row); rec[5] != "=x" {
		t.Error("guard applied while off")
	}
	row[0] = "bad"
	if _, err := w.record(&dataset.Orders, row); err == nil {
		t.Error("bad value accepted")
	}
}

func TestBackoffIsBoundedAndGrows(t *testing.T) {
	w := &Worker{cfg: Config{Backoff: 10 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}}
	for attempt := range 12 {
		for range 200 {
			d := w.backoff(attempt)
			base := min(10*time.Millisecond<<attempt, 100*time.Millisecond)
			if d < base || d > 100*time.Millisecond || d > base+base/2 {
				t.Fatalf("attempt %d: %s outside [%s, min(%s, 100ms)]", attempt, d, base, base+base/2)
			}
		}
	}
}

func TestConfigValidate(t *testing.T) {
	good := cfg(t)
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, f := range map[string]func(*Config){
		"id":          func(c *Config) { c.ID = "a b" },
		"out":         func(c *Config) { c.OutDir = "" },
		"page 0":      func(c *Config) { c.PageSize = 0 },
		"page big":    func(c *Config) { c.PageSize = keyset.MaxPageSize + 1 },
		"timeout":     func(c *Config) { c.PageTimeout = 0 },
		"retries":     func(c *Config) { c.Retries = 11 },
		"neg retries": func(c *Config) { c.Retries = -1 },
		"backoff":     func(c *Config) { c.Backoff = 0 },
		"max backoff": func(c *Config) { c.MaxBackoff = time.Microsecond },
		"pause":       func(c *Config) { c.PagePause = -1 },
		"idle":        func(c *Config) { c.Idle = 0 },
	} {
		c := good
		f(&c)
		if c.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestRunStopsWhenContextEnds(t *testing.T) {
	mem, store, id := setup(t, 1, 50)
	c := cfg(t)
	w, err := New(c, store, mem, dataset.Default(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		j, _ := store.Get(context.Background(), id)
		if j.Status == job.Succeeded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job not done")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestSleep(t *testing.T) {
	if err := Sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
