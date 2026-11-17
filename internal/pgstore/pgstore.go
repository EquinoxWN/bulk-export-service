// Package pgstore keeps export jobs in PostgreSQL on the primary; it never reads exported rows.
package pgstore

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EquinoxWN/bulk-export-service/internal/job"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
)

//go:embed schema.sql
var schema string

// QueryTimeout bounds every store statement.
const QueryTimeout = 5 * time.Second

// Store is the job store on the primary database.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to the primary (bounded pool, connect timeout) and creates the jobs table if missing.
func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	if maxConns < 1 || maxConns > 64 {
		return nil, fmt.Errorf("primary pool size %d out of range 1..64", maxConns)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("primary DSN: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.ConnConfig.ConnectTimeout = 10 * time.Second
	cfg.ConnConfig.RuntimeParams["application_name"] = "bulk-export-jobs"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("primary pool: %w", err)
	}
	s := &Store{pool: pool}
	mctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(mctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("create export_jobs: %w", err)
	}
	return s, nil
}

// Close releases the pool.
func (s *Store) Close() { s.pool.Close() }

const cols = `id, account_id, dataset, format, idem_key, status, worker, row_count, page_count, cursor, bytes, sha256, file, error,
	created_at, updated_at, started_at, finished_at`

// scan reads one job row.
func scan(row pgx.Row) (job.Job, error) {
	var j job.Job
	var cursor []byte
	var status string
	err := row.Scan(&j.ID, &j.Request.AccountID, &j.Request.Dataset, &j.Request.Format, &j.IdempotencyKey, &status,
		&j.Worker, &j.Rows, &j.Pages, &cursor, &j.Bytes, &j.SHA256, &j.File, &j.Error,
		&j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt)
	if err != nil {
		return job.Job{}, err
	}
	j.Status = job.Status(status)
	if cursor != nil {
		var c keyset.Cursor
		if err := json.Unmarshal(cursor, &c); err != nil {
			return job.Job{}, fmt.Errorf("stored cursor: %w", err)
		}
		j.Cursor = c
	}
	j.CreatedAt, j.UpdatedAt = j.CreatedAt.UTC(), j.UpdatedAt.UTC()
	if j.StartedAt != nil {
		t := j.StartedAt.UTC()
		j.StartedAt = &t
	}
	if j.FinishedAt != nil {
		t := j.FinishedAt.UTC()
		j.FinishedAt = &t
	}
	return j, nil
}

// Create inserts a queued job once per (account, key): ON CONFLICT DO NOTHING, then compare with the stored request.
func (s *Store) Create(ctx context.Context, key string, req job.Request) (job.Job, bool, error) {
	if err := job.ValidateKey(key); err != nil {
		return job.Job{}, false, err
	}
	id, err := job.NewID()
	if err != nil {
		return job.Job{}, false, err
	}
	ctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	j, err := scan(s.pool.QueryRow(ctx, `INSERT INTO export_jobs (id, account_id, dataset, format, idem_key, status)
		VALUES ($1, $2, $3, $4, $5, 'queued') ON CONFLICT (account_id, idem_key) DO NOTHING RETURNING `+cols,
		id, req.AccountID, req.Dataset, req.Format, key))
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, fmt.Errorf("insert job: %w", err)
	}
	j, err = scan(s.pool.QueryRow(ctx, `SELECT `+cols+` FROM export_jobs WHERE account_id = $1 AND idem_key = $2`, req.AccountID, key))
	if err != nil {
		return job.Job{}, false, fmt.Errorf("read existing job: %w", err)
	}
	if j.Request != req {
		return job.Job{}, false, job.ErrKeyReused
	}
	return j, false, nil
}

// Get reads one job.
func (s *Store) Get(ctx context.Context, id string) (job.Job, error) {
	if !job.ValidID(id) {
		return job.Job{}, job.ErrNotFound
	}
	ctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	j, err := scan(s.pool.QueryRow(ctx, `SELECT `+cols+` FROM export_jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrNotFound
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("get job: %w", err)
	}
	return j, nil
}

// Claim takes the oldest queued job with FOR UPDATE SKIP LOCKED, so concurrent workers never take the same one.
func (s *Store) Claim(ctx context.Context, worker string) (job.Job, bool, error) {
	if !job.ValidWorker(worker) {
		return job.Job{}, false, fmt.Errorf("%w: worker id %q", job.ErrInvalidArgs, worker)
	}
	ctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	j, err := scan(s.pool.QueryRow(ctx, `UPDATE export_jobs SET status = 'running', worker = $1, started_at = now(), updated_at = now()
		WHERE id = (SELECT id FROM export_jobs WHERE status = 'queued' ORDER BY seq LIMIT 1 FOR UPDATE SKIP LOCKED)
		RETURNING `+cols, worker))
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, nil
	}
	if err != nil {
		return job.Job{}, false, fmt.Errorf("claim job: %w", err)
	}
	return j, true, nil
}

// owned explains why a fenced update touched no row.
func (s *Store) owned(ctx context.Context, id string) error {
	if _, err := s.Get(ctx, id); err != nil {
		return err
	}
	return job.ErrNotOwner
}

// Progress records the page progress; the WHERE clause fences it to the owning worker and forbids going backwards.
func (s *Store) Progress(ctx context.Context, id, worker string, p job.Progress) error {
	if p.Rows < 0 || p.Pages < 0 {
		return fmt.Errorf("%w: negative progress", job.ErrInvalidArgs)
	}
	cur, err := json.Marshal(p.Cursor)
	if err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `UPDATE export_jobs SET row_count = $3, page_count = $4, cursor = $5, updated_at = now()
		WHERE id = $1 AND worker = $2 AND status = 'running' AND row_count <= $3 AND page_count <= $4`, id, worker, p.Rows, p.Pages, cur)
	if err != nil {
		return fmt.Errorf("record progress: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return s.why(ctx, id, worker, p)
}

// why turns a fenced update that changed nothing into ErrNotFound, ErrNotOwner or a backwards-progress error.
func (s *Store) why(ctx context.Context, id, worker string, p job.Progress) error {
	j, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if j.Status != job.Running || j.Worker != worker {
		return job.ErrNotOwner
	}
	return job.CheckProgress(j, p)
}

// Finish marks the job succeeded, fenced to the owning worker.
func (s *Store) Finish(ctx context.Context, id, worker string, r job.Result) error {
	if r.File == "" || len(r.SHA256) != 64 || r.Bytes <= 0 || r.Rows < 0 || r.Pages < 0 {
		return fmt.Errorf("%w: incomplete result", job.ErrInvalidArgs)
	}
	cur, err := json.Marshal(r.Cursor)
	if err != nil {
		return fmt.Errorf("cursor: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `UPDATE export_jobs SET status = 'succeeded', row_count = $3, page_count = $4, cursor = $5, bytes = $6,
		sha256 = $7, file = $8, finished_at = now(), updated_at = now()
		WHERE id = $1 AND worker = $2 AND status = 'running' AND row_count <= $3 AND page_count <= $4`,
		id, worker, r.Rows, r.Pages, cur, r.Bytes, r.SHA256, r.File)
	if err != nil {
		return fmt.Errorf("finish job: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return s.why(ctx, id, worker, r.Progress)
}

// Fail marks the job failed, fenced to the owning worker.
func (s *Store) Fail(ctx context.Context, id, worker, msg string) error {
	ctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `UPDATE export_jobs SET status = 'failed', error = $3, finished_at = now(), updated_at = now()
		WHERE id = $1 AND worker = $2 AND status = 'running'`, id, worker, job.Clip(msg))
	if err != nil {
		return fmt.Errorf("fail job: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	return s.owned(ctx, id)
}
