package job

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

type idemKey struct {
	account int64
	key     string
}

// Memory is the in-process job store (one mutex); jobs are claimed in creation order.
type Memory struct {
	mu    sync.Mutex
	max   int
	jobs  map[string]*Job
	byKey map[idemKey]string
	queue []string // queued job ids in creation order
	now   func() time.Time
}

// NewMemory returns a store that holds at most max jobs.
func NewMemory(max int) *Memory {
	return &Memory{max: max, jobs: map[string]*Job{}, byKey: map[idemKey]string{}, now: func() time.Time { return time.Now().UTC() }}
}

// Create stores a queued job once per (account, key); a retry returns the first job and false.
func (m *Memory) Create(ctx context.Context, key string, req Request) (Job, bool, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, false, err
	}
	if err := ValidateKey(key); err != nil {
		return Job{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := idemKey{req.AccountID, key}
	if id, ok := m.byKey[k]; ok {
		j := m.jobs[id]
		if j.Request != req {
			return Job{}, false, ErrKeyReused
		}
		return clone(j), false, nil
	}
	if len(m.jobs) >= m.max {
		return Job{}, false, ErrStoreFull
	}
	id, err := NewID()
	if err != nil {
		return Job{}, false, err
	}
	now := m.now()
	j := &Job{ID: id, Request: req, IdempotencyKey: key, Status: Queued, CreatedAt: now, UpdatedAt: now}
	m.jobs[id] = j
	m.byKey[k] = id
	m.queue = append(m.queue, id)
	return clone(j), true, nil
}

// Get returns a copy of one job.
func (m *Memory) Get(ctx context.Context, id string) (Job, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return clone(j), nil
}

// Claim moves the oldest queued job to running for this worker.
func (m *Memory) Claim(ctx context.Context, worker string) (Job, bool, error) {
	if err := ctx.Err(); err != nil {
		return Job{}, false, err
	}
	if !ValidWorker(worker) {
		return Job{}, false, fmt.Errorf("%w: worker id %q", ErrInvalidArgs, worker)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.queue) == 0 {
		return Job{}, false, nil
	}
	id := m.queue[0]
	m.queue = m.queue[1:]
	j := m.jobs[id]
	now := m.now()
	j.Status, j.Worker, j.StartedAt, j.UpdatedAt = Running, worker, &now, now
	return clone(j), true, nil
}

// owned returns the job if it is running on this worker.
func (m *Memory) owned(id, worker string) (*Job, error) {
	j, ok := m.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	if j.Status != Running || j.Worker != worker {
		return nil, ErrNotOwner
	}
	return j, nil
}

// Progress records rows, pages and the cursor after a page.
func (m *Memory) Progress(ctx context.Context, id, worker string, p Progress) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.owned(id, worker)
	if err != nil {
		return err
	}
	if err := CheckProgress(*j, p); err != nil {
		return err
	}
	j.Rows, j.Pages, j.Cursor, j.UpdatedAt = p.Rows, p.Pages, slices.Clone(p.Cursor), m.now()
	return nil
}

// Finish marks the job succeeded with its file details.
func (m *Memory) Finish(ctx context.Context, id, worker string, r Result) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.File == "" || len(r.SHA256) != 64 || r.Bytes <= 0 {
		return fmt.Errorf("%w: incomplete result", ErrInvalidArgs)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.owned(id, worker)
	if err != nil {
		return err
	}
	if err := CheckProgress(*j, r.Progress); err != nil {
		return err
	}
	now := m.now()
	j.Status, j.Rows, j.Pages, j.Cursor = Succeeded, r.Rows, r.Pages, slices.Clone(r.Cursor)
	j.Bytes, j.SHA256, j.File, j.FinishedAt, j.UpdatedAt = r.Bytes, r.SHA256, r.File, &now, now
	return nil
}

// Fail marks the job failed with a short reason.
func (m *Memory) Fail(ctx context.Context, id, worker, msg string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	j, err := m.owned(id, worker)
	if err != nil {
		return err
	}
	now := m.now()
	j.Status, j.Error, j.FinishedAt, j.UpdatedAt = Failed, Clip(msg), &now, now
	return nil
}

// clone copies a job so callers never share the store's memory.
func clone(j *Job) Job {
	c := *j
	c.Cursor = slices.Clone(j.Cursor)
	if j.StartedAt != nil {
		t := *j.StartedAt
		c.StartedAt = &t
	}
	if j.FinishedAt != nil {
		t := *j.FinishedAt
		c.FinishedAt = &t
	}
	return c
}
