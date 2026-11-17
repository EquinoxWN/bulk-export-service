// Package job holds export jobs: the request, its validation, the status life cycle and the store contract.
package job

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
)

// Status is a job's place in its life cycle: queued -> running -> succeeded | failed.
type Status string

// Job states.
const (
	Queued    Status = "queued"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
)

// Request is what a client asks to export.
type Request struct {
	AccountID int64  `json:"account_id"`
	Dataset   string `json:"dataset"`
	Format    string `json:"format"`
}

// Job is one export and its progress.
type Job struct {
	ID             string
	Request        Request
	IdempotencyKey string
	Status         Status
	Worker         string
	Rows           int64
	Pages          int64
	Cursor         keyset.Cursor
	Bytes          int64
	SHA256         string
	File           string
	Error          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

// Progress is what a worker records after each page.
type Progress struct {
	Rows   int64
	Pages  int64
	Cursor keyset.Cursor
}

// Result is what a worker records when the file is complete.
type Result struct {
	Progress
	Bytes  int64
	SHA256 string
	File   string
}

// Store errors.
var (
	ErrNotFound    = errors.New("job not found")
	ErrKeyReused   = errors.New("idempotency key already used with another request")
	ErrNotOwner    = errors.New("job is not running on this worker")
	ErrStoreFull   = errors.New("job store is full")
	ErrInvalid     = errors.New("invalid request")
	ErrInvalidArgs = errors.New("invalid store arguments")
)

// Store keeps jobs. Create is idempotent per (account, key); Claim hands each queued job to one worker;
// Progress, Finish and Fail succeed only for the worker that holds the running job.
type Store interface {
	Create(ctx context.Context, key string, req Request) (Job, bool, error)
	Get(ctx context.Context, id string) (Job, error)
	Claim(ctx context.Context, worker string) (Job, bool, error)
	Progress(ctx context.Context, id, worker string, p Progress) error
	Finish(ctx context.Context, id, worker string, r Result) error
	Fail(ctx context.Context, id, worker, msg string) error
}

var (
	keyRe    = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	idRe     = regexp.MustCompile(`^[0-9a-f]{32}$`)
	workerRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
)

// MaxErrorLen bounds the stored error text.
const MaxErrorLen = 512

// Formats lists the output formats M1 writes.
var Formats = map[string]bool{"csv": true}

// ValidateKey checks an idempotency key: 1 to 128 characters from [A-Za-z0-9._:-].
func ValidateKey(key string) error {
	if !keyRe.MatchString(key) {
		return fmt.Errorf("%w: Idempotency-Key must be 1-128 characters from A-Z a-z 0-9 . _ : -", ErrInvalid)
	}
	return nil
}

// ValidID reports whether s has the shape of a job id.
func ValidID(s string) bool { return idRe.MatchString(s) }

// ValidWorker reports whether s is an acceptable worker id.
func ValidWorker(s string) bool { return workerRe.MatchString(s) }

// Validate checks a request against the dataset registry.
func (r Request) Validate(reg dataset.Registry) error {
	if r.AccountID <= 0 {
		return fmt.Errorf("%w: account_id must be a positive integer", ErrInvalid)
	}
	if _, ok := reg[r.Dataset]; !ok {
		return fmt.Errorf("%w: unknown dataset %q", ErrInvalid, r.Dataset)
	}
	if !Formats[r.Format] {
		return fmt.Errorf("%w: format %q not supported (M1 writes csv)", ErrInvalid, r.Format)
	}
	return nil
}

// NewID returns a random 128-bit job id in hex.
func NewID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("job id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// Clip shortens an error message to MaxErrorLen bytes.
func Clip(msg string) string {
	if len(msg) <= MaxErrorLen {
		return msg
	}
	return msg[:MaxErrorLen]
}

// CheckProgress refuses progress that goes backwards or is negative.
func CheckProgress(cur Job, p Progress) error {
	if p.Rows < 0 || p.Pages < 0 {
		return fmt.Errorf("%w: negative progress", ErrInvalidArgs)
	}
	if p.Rows < cur.Rows || p.Pages < cur.Pages {
		return fmt.Errorf("%w: progress went backwards (%d rows, %d pages after %d, %d)", ErrInvalidArgs, p.Rows, p.Pages, cur.Rows, cur.Pages)
	}
	return nil
}
