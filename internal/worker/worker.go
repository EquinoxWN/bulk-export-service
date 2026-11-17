// Package worker claims export jobs and writes each one to a CSV file, page by page in keyset order,
// reading only through the replica source.
package worker

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"time"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/job"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
	"github.com/EquinoxWN/bulk-export-service/internal/source"
)

// Config sets one worker's limits.
type Config struct {
	ID          string        // worker id stored on claimed jobs
	OutDir      string        // finished files are written here (atomic rename from .part)
	PageSize    int           // rows per keyset page
	PageTimeout time.Duration // limit for one page read
	Retries     int           // extra attempts per page after the first
	Backoff     time.Duration // first retry delay; doubles per attempt, with jitter
	MaxBackoff  time.Duration // cap on one retry delay
	PagePause   time.Duration // pause between pages to cap the load on the replica
	Idle        time.Duration // wait when no job is queued or the store fails
	// FormulaGuard prefixes text cells that start with = + - @ tab or CR with a single quote, so spreadsheets do not run them.
	FormulaGuard bool
}

// Validate checks the limits.
func (c Config) Validate() error {
	switch {
	case !job.ValidWorker(c.ID):
		return fmt.Errorf("worker id %q: 1-64 characters from A-Z a-z 0-9 . _ -", c.ID)
	case c.OutDir == "":
		return errors.New("output directory is empty")
	case c.PageSize < 1 || c.PageSize > keyset.MaxPageSize:
		return fmt.Errorf("page size %d out of range 1..%d", c.PageSize, keyset.MaxPageSize)
	case c.PageTimeout <= 0:
		return errors.New("page timeout must be positive")
	case c.Retries < 0 || c.Retries > 10:
		return fmt.Errorf("retries %d out of range 0..10", c.Retries)
	case c.Backoff <= 0 || c.MaxBackoff < c.Backoff:
		return errors.New("backoff must be positive and not above max backoff")
	case c.PagePause < 0:
		return errors.New("page pause must not be negative")
	case c.Idle <= 0:
		return errors.New("idle wait must be positive")
	}
	return nil
}

// Worker runs export jobs.
type Worker struct {
	cfg   Config
	jobs  job.Store
	src   source.Reader
	reg   dataset.Registry
	log   *slog.Logger
	sleep func(context.Context, time.Duration) error
}

// New checks the config and returns a worker.
func New(cfg Config, jobs job.Store, src source.Reader, reg dataset.Registry, log *slog.Logger) (*Worker, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.OutDir, 0o750); err != nil {
		return nil, fmt.Errorf("output directory: %w", err)
	}
	return &Worker{cfg: cfg, jobs: jobs, src: src, reg: reg, log: log, sleep: Sleep}, nil
}

// Sleep waits for d or until ctx ends.
func Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Run claims and runs jobs until ctx ends; store errors are logged and retried after the idle wait.
func (w *Worker) Run(ctx context.Context) {
	for ctx.Err() == nil {
		did, err := w.RunOnce(ctx)
		if err != nil && ctx.Err() == nil {
			w.log.Error("worker step failed", "worker", w.cfg.ID, "err", err)
		}
		if err != nil || !did {
			if w.sleep(ctx, w.cfg.Idle) != nil {
				return
			}
		}
	}
}

// RunOnce claims one job and runs it; it reports whether a job was claimed.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	j, ok, err := w.jobs.Claim(ctx, w.cfg.ID)
	if err != nil || !ok {
		return false, err
	}
	start := time.Now()
	res, err := w.export(ctx, j)
	if err == nil {
		err = w.jobs.Finish(ctx, j.ID, w.cfg.ID, res)
		if err == nil {
			w.log.Info("export done", "job", j.ID, "rows", res.Rows, "pages", res.Pages, "bytes", res.Bytes, "took", time.Since(start).Round(time.Millisecond))
			return true, nil
		}
	}
	if errors.Is(err, job.ErrNotOwner) {
		return true, fmt.Errorf("job %s: lost ownership: %w", j.ID, err)
	}
	// Record the failure even when ctx was cancelled, with its own deadline.
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if ferr := w.jobs.Fail(fctx, j.ID, w.cfg.ID, err.Error()); ferr != nil {
		return true, fmt.Errorf("job %s failed (%v) and the failure was not recorded: %w", j.ID, err, ferr)
	}
	w.log.Warn("export failed", "job", j.ID, "err", err)
	return true, nil
}

type counter struct{ n int64 }

func (c *counter) Write(p []byte) (int, error) { c.n += int64(len(p)); return len(p), nil }

// export writes the job's rows to <OutDir>/<id>.csv.part, recording progress after each page, then renames it.
func (w *Worker) export(ctx context.Context, j job.Job) (res job.Result, err error) {
	ds, ok := w.reg[j.Request.Dataset]
	if !ok {
		return res, fmt.Errorf("unknown dataset %q", j.Request.Dataset)
	}
	name := j.ID + ".csv"
	final := filepath.Join(w.cfg.OutDir, name)
	part := final + ".part"
	f, err := os.OpenFile(part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return res, fmt.Errorf("create %s: %w", part, err)
	}
	closed := false
	defer func() {
		if err != nil {
			if !closed {
				_ = f.Close() // the write already failed; that error is the one returned
			}
			_ = os.Remove(part) // best effort: a stale .part is overwritten by the next attempt (O_TRUNC)
		}
	}()
	h, n := sha256.New(), &counter{}
	bw := bufio.NewWriterSize(io.MultiWriter(f, h, n), 64<<10)
	cw := csv.NewWriter(bw)
	header := make([]string, len(ds.Columns))
	for i, c := range ds.Columns {
		header[i] = c.Name
	}
	if err = cw.Write(header); err != nil {
		return res, fmt.Errorf("write header: %w", err)
	}
	var p job.Progress
	for {
		page, rerr := w.readPage(ctx, ds, j.Request.AccountID, p.Cursor, p.Pages)
		if rerr != nil {
			return res, rerr
		}
		for _, r := range page.Rows {
			rec, ferr := w.record(ds, r)
			if ferr != nil {
				return res, ferr
			}
			if err = cw.Write(rec); err != nil {
				return res, fmt.Errorf("write row: %w", err)
			}
		}
		cw.Flush()
		if err = cw.Error(); err != nil {
			return res, fmt.Errorf("write page: %w", err)
		}
		p = job.Progress{Rows: p.Rows + int64(len(page.Rows)), Pages: p.Pages + 1, Cursor: page.Next}
		if err = w.jobs.Progress(ctx, j.ID, w.cfg.ID, p); err != nil {
			return res, fmt.Errorf("record progress: %w", err)
		}
		if page.Last {
			break
		}
		if w.cfg.PagePause > 0 {
			if err = w.sleep(ctx, w.cfg.PagePause); err != nil {
				return res, err
			}
		}
	}
	if err = bw.Flush(); err != nil {
		return res, fmt.Errorf("flush: %w", err)
	}
	if err = f.Sync(); err != nil {
		return res, fmt.Errorf("sync: %w", err)
	}
	closed = true
	if err = f.Close(); err != nil {
		return res, fmt.Errorf("close: %w", err)
	}
	if err = os.Rename(part, final); err != nil {
		return res, fmt.Errorf("rename: %w", err)
	}
	return job.Result{Progress: p, Bytes: n.n, SHA256: hex.EncodeToString(h.Sum(nil)), File: name}, nil
}

// permanent reports errors that a retry cannot fix.
func permanent(err error) bool {
	return errors.Is(err, keyset.ErrBadCursor) || errors.Is(err, dataset.ErrBadValue)
}

// readPage reads one page with a per-attempt timeout and bounded retries with capped, jittered backoff.
func (w *Worker) readPage(ctx context.Context, ds *dataset.Dataset, account int64, after keyset.Cursor, pageNo int64) (source.Page, error) {
	for attempt := 0; ; attempt++ {
		pctx, cancel := context.WithTimeout(ctx, w.cfg.PageTimeout)
		page, err := w.src.ReadPage(pctx, ds, account, after, w.cfg.PageSize)
		cancel()
		if err == nil {
			return page, nil
		}
		if ctx.Err() != nil {
			return source.Page{}, ctx.Err()
		}
		if permanent(err) || attempt >= w.cfg.Retries {
			return source.Page{}, fmt.Errorf("read page %d (attempt %d of %d): %w", pageNo+1, attempt+1, w.cfg.Retries+1, err)
		}
		w.log.Warn("page read failed, retrying", "page", pageNo+1, "attempt", attempt+1, "err", err)
		if serr := w.sleep(ctx, w.backoff(attempt)); serr != nil {
			return source.Page{}, serr
		}
	}
}

// backoff is Backoff * 2^attempt capped at MaxBackoff, plus up to 50% jitter (still capped).
func (w *Worker) backoff(attempt int) time.Duration {
	d := w.cfg.Backoff
	for range attempt {
		d *= 2
		if d >= w.cfg.MaxBackoff {
			d = w.cfg.MaxBackoff
			break
		}
	}
	d += time.Duration(rand.Int64N(int64(d)/2 + 1))
	return min(d, w.cfg.MaxBackoff)
}

// record formats one row for CSV, guarding text cells against spreadsheet formulas when enabled.
func (w *Worker) record(ds *dataset.Dataset, row []any) ([]string, error) {
	out := make([]string, len(ds.Columns))
	for i, c := range ds.Columns {
		s, err := dataset.Format(c.Type, row[i])
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", c.Name, err)
		}
		if w.cfg.FormulaGuard && c.Type == dataset.Text && s != "" && isFormulaStart(s[0]) {
			s = "'" + s
		}
		out[i] = s
	}
	return out, nil
}

func isFormulaStart(b byte) bool {
	return b == '=' || b == '+' || b == '-' || b == '@' || b == '\t' || b == '\r'
}
