package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/EquinoxWN/bulk-export-service/internal/api"
	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/job"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
	"github.com/EquinoxWN/bulk-export-service/internal/sample"
	"github.com/EquinoxWN/bulk-export-service/internal/source"
	"github.com/EquinoxWN/bulk-export-service/internal/worker"
)

// demoClient talks to the in-process API over real HTTP on 127.0.0.1.
type demoClient struct {
	base string
	http *http.Client
}

// post sends one create request and returns the status code, the replay header and the decoded body.
func (c demoClient) post(ctx context.Context, key, body string) (int, bool, api.Status, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/v1/exports", bytes.NewBufferString(body))
	if err != nil {
		return 0, false, api.Status{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, false, api.Status{}, err
	}
	defer func() { _ = resp.Body.Close() }() // body fully read below; close error is irrelevant
	var st api.Status
	if resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			return 0, false, api.Status{}, err
		}
	} else if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return 0, false, api.Status{}, err
	}
	return resp.StatusCode, resp.Header.Get("Idempotent-Replayed") == "true", st, nil
}

// get fetches a path and returns the status code, body and the X-Content-SHA256 header.
func (c demoClient) get(ctx context.Context, path string) (int, []byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return 0, nil, "", err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, "", err
	}
	defer func() { _ = resp.Body.Close() }() // body fully read below; close error is irrelevant
	b, err := io.ReadAll(io.LimitReader(resp.Body, 512<<20))
	return resp.StatusCode, b, resp.Header.Get("X-Content-SHA256"), err
}

// demo seeds an in-memory replica, exports every account through the API and workers, and verifies each file.
func demo(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.SetOutput(out)
	accounts := fs.Int("accounts", 20, "accounts to export (one job each)")
	perAccount := fs.Int("rows", 5000, "orders per account")
	workers := fs.Int("workers", 4, "export workers (1..64)")
	seed := fs.Uint64("seed", 1, "data seed")
	failEvery := fs.Int("fail-every", 7, "make every n-th page read fail once, to exercise retries (0 = never)")
	var cfg worker.Config
	workerFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *accounts < 1 || *perAccount < 0 || *failEvery < 0 {
		return errors.New("accounts must be positive; rows and fail-every must not be negative")
	}
	cfg.ID, cfg.Backoff, cfg.MaxBackoff, cfg.Idle = "w", time.Millisecond, 10*time.Millisecond, 5*time.Millisecond
	dir, err := os.MkdirTemp("", "bulk-export-demo-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }() // scratch files of this run only
	cfg.OutDir = dir

	reg := dataset.Default()
	ds := reg["orders"]
	rows, err := sample.Orders(*accounts, *perAccount, *seed)
	if err != nil {
		return err
	}
	mem := source.NewMemory()
	if err := mem.Insert(ds, rows...); err != nil {
		return err
	}
	src := &flaky{inner: mem, every: *failEvery}
	jobs := job.NewMemory(*accounts * 2)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	fmt.Fprintf(out, "config: accounts=%d orders/account=%d page=%d workers=%d page-read failures=every %d\n",
		*accounts, *perAccount, cfg.PageSize, *workers, *failEvery)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: api.New(jobs, reg, dir, log), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }() // Serve returns ErrServerClosed after Shutdown below
	defer func() { _ = srv.Shutdown(context.WithoutCancel(ctx)) }()
	c := demoClient{base: "http://" + ln.Addr().String(), http: &http.Client{Timeout: 30 * time.Second}}

	// Create every job before any worker runs: a 202 must not wait for the export.
	ids := make([]string, *accounts)
	replays := 0
	createStart := time.Now()
	for a := 1; a <= *accounts; a++ {
		body := fmt.Sprintf(`{"account_id":%d,"dataset":"orders","format":"csv"}`, a)
		code, _, st, err := c.post(ctx, fmt.Sprintf("demo-%d", a), body)
		if err != nil || code != http.StatusAccepted || st.Status != job.Queued {
			return fmt.Errorf("create account %d: code %d status %s: %v", a, code, st.Status, err)
		}
		ids[a-1] = st.ID
		code, replayed, again, err := c.post(ctx, fmt.Sprintf("demo-%d", a), body)
		if err != nil || code != http.StatusOK || !replayed || again.ID != st.ID {
			return fmt.Errorf("retry for account %d: code %d replayed %v id %s: %v", a, code, replayed, again.ID, err)
		}
		replays++
	}
	fmt.Fprintf(out, "create: %d jobs queued (202) and %d retries replayed the same id (200) in %s, before any worker started\n",
		len(ids), replays, time.Since(createStart).Round(time.Millisecond))

	start := time.Now()
	wctx, cancel := context.WithCancel(ctx)
	wait, err := startWorkers(wctx, *workers, cfg, jobs, src, reg, log)
	if err != nil {
		cancel()
		return err
	}
	defer func() { cancel(); wait() }()

	final, polls, err := pollAll(ctx, c, ids, 2*time.Minute)
	if err != nil {
		return err
	}
	took := time.Since(start)
	var succeeded, failed, pages, total int64
	for _, st := range final {
		if st.Status == job.Succeeded {
			succeeded++
		} else {
			failed++
		}
		pages += st.Pages
		total += st.Rows
	}
	fmt.Fprintf(out, "exports: %d succeeded, %d failed, %d rows in %d pages, %d status polls, %s, %d page reads retried\n",
		succeeded, failed, total, pages, polls, took.Round(time.Millisecond), src.retried())

	keysetRows := mem.Examined()
	offsetRows, err := offsetCost(ctx, mem, ds, *accounts, cfg.PageSize)
	if err != nil {
		return err
	}
	ratio := 0.0
	if keysetRows > 0 {
		ratio = float64(offsetRows) / float64(keysetRows)
	}
	fmt.Fprintf(out, "rows examined (in-memory model, not a database measurement): keyset %d, OFFSET for the same pages %d (%.1fx)\n",
		keysetRows, offsetRows, ratio)

	v, err := verify(ctx, c, mem, ds, final, cfg.FormulaGuard)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "verify: files=%d rows_checked=%d row_mismatches=%d count_mismatches=%d sha_mismatches=%d failed_jobs=%d\n",
		v.files, v.rows, v.rowMismatch, v.countMismatch, v.shaMismatch, failed)
	if v.rowMismatch+v.countMismatch+v.shaMismatch+int(failed) != 0 {
		return errors.New("verification failed")
	}
	return nil
}

// flaky fails every n-th page read once (before reading), so the demo exercises the worker's retries.
type flaky struct {
	inner  source.Reader
	every  int
	mu     sync.Mutex
	calls  int
	failed int
}

// errInjected is the failure flaky returns.
var errInjected = errors.New("injected page-read failure")

// ReadPage fails every n-th call, otherwise reads from the inner source.
func (f *flaky) ReadPage(ctx context.Context, ds *dataset.Dataset, account int64, after keyset.Cursor, limit int) (source.Page, error) {
	f.mu.Lock()
	f.calls++
	fail := f.every > 0 && f.calls%f.every == 0
	if fail {
		f.failed++
	}
	f.mu.Unlock()
	if fail {
		return source.Page{}, errInjected
	}
	return f.inner.ReadPage(ctx, ds, account, after, limit)
}

// retried returns how many reads were failed on purpose.
func (f *flaky) retried() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failed
}

// pollAll polls every job until it ends or the deadline passes.
func pollAll(ctx context.Context, c demoClient, ids []string, limit time.Duration) ([]api.Status, int, error) {
	deadline := time.Now().Add(limit)
	final := make([]api.Status, len(ids))
	done := make([]bool, len(ids))
	polls, left := 0, len(ids)
	for left > 0 {
		if time.Now().After(deadline) {
			return nil, polls, fmt.Errorf("%d exports not finished after %s", left, limit)
		}
		for i, id := range ids {
			if done[i] {
				continue
			}
			code, body, _, err := c.get(ctx, "/v1/exports/"+id)
			polls++
			if err != nil || code != http.StatusOK {
				return nil, polls, fmt.Errorf("poll %s: code %d: %v", id, code, err)
			}
			var st api.Status
			if err := json.Unmarshal(body, &st); err != nil {
				return nil, polls, err
			}
			if st.Status == job.Succeeded || st.Status == job.Failed {
				final[i], done[i] = st, true
				left--
			}
		}
		if left > 0 {
			if err := worker.Sleep(ctx, 10*time.Millisecond); err != nil {
				return nil, polls, err
			}
		}
	}
	return final, polls, nil
}

// offsetCost reads the same pages with OFFSET and returns the rows a database would have examined.
func offsetCost(ctx context.Context, mem *source.Memory, ds *dataset.Dataset, accounts, page int) (int64, error) {
	for a := 1; a <= accounts; a++ {
		for off := int64(0); ; off += int64(page) {
			rows, err := mem.ReadOffsetPage(ctx, ds, int64(a), off, page)
			if err != nil {
				return 0, err
			}
			if len(rows) < page {
				break
			}
		}
	}
	return mem.Examined(), nil
}

type verdict struct {
	files, rows                             int
	rowMismatch, countMismatch, shaMismatch int
}

// verify downloads every file and compares it, row by row, with a full ordered read of the source.
func verify(ctx context.Context, c demoClient, mem *source.Memory, ds *dataset.Dataset, final []api.Status, guard bool) (verdict, error) {
	var v verdict
	for _, st := range final {
		if st.Status != job.Succeeded {
			continue
		}
		code, body, sha, err := c.get(ctx, st.FileURL)
		if err != nil || code != http.StatusOK {
			return v, fmt.Errorf("download %s: code %d: %v", st.ID, code, err)
		}
		v.files++
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != sha || sha != st.SHA256 || int64(len(body)) != st.Bytes {
			v.shaMismatch++
		}
		got, err := csv.NewReader(bytes.NewReader(body)).ReadAll()
		if err != nil {
			return v, fmt.Errorf("parse %s: %w", st.ID, err)
		}
		want, err := expected(ctx, mem, ds, st.AccountID, guard)
		if err != nil {
			return v, err
		}
		if len(got) != len(want) || int64(len(got)-1) != st.Rows {
			v.countMismatch++
		}
		for i := 0; i < len(got) && i < len(want); i++ {
			if !slices.Equal(got[i], want[i]) {
				v.rowMismatch++
			}
		}
		v.rows += len(got) - 1
	}
	return v, nil
}

// expected builds the CSV records an account's file must contain (header first), straight from the source.
func expected(ctx context.Context, mem *source.Memory, ds *dataset.Dataset, account int64, guard bool) ([][]string, error) {
	header := make([]string, len(ds.Columns))
	for i, col := range ds.Columns {
		header[i] = col.Name
	}
	out := [][]string{header}
	p, err := mem.ReadPage(ctx, ds, account, nil, 1)
	for ; err == nil; p, err = mem.ReadPage(ctx, ds, account, p.Next, 1) {
		for _, r := range p.Rows {
			rec := make([]string, len(r))
			for i, col := range ds.Columns {
				s, ferr := dataset.Format(col.Type, r[i])
				if ferr != nil {
					return nil, ferr
				}
				if guard && col.Type == dataset.Text && s != "" && bytes.ContainsAny([]byte{s[0]}, "=+-@\t\r") {
					s = "'" + s
				}
				rec[i] = s
			}
			out = append(out, rec)
		}
		if p.Last {
			return out, nil
		}
	}
	return nil, err
}
