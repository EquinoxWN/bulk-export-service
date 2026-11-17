// Package jobtest is the shared contract test for job.Store implementations (memory and PostgreSQL).
package jobtest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EquinoxWN/bulk-export-service/internal/job"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
)

var req = job.Request{AccountID: 7, Dataset: "orders", Format: "csv"}

func result(rows int64) job.Result {
	return job.Result{
		Progress: job.Progress{Rows: rows, Pages: 2, Cursor: keyset.Cursor{"2026-01-01T00:00:00.000000Z", "9"}},
		Bytes:    100, SHA256: strings.Repeat("a", 64), File: "x.csv",
	}
}

// Run runs every contract case against fresh stores from newStore.
func Run(t *testing.T, newStore func(t *testing.T) job.Store) {
	ctx := context.Background()

	t.Run("CreateIsIdempotentPerAccountAndKey", func(t *testing.T) {
		s := newStore(t)
		a, created, err := s.Create(ctx, "k1", req)
		if err != nil || !created {
			t.Fatalf("first create: %v %v", created, err)
		}
		if a.Status != job.Queued || !job.ValidID(a.ID) || a.IdempotencyKey != "k1" || a.Request != req {
			t.Fatalf("bad job %+v", a)
		}
		if a.CreatedAt.Location() != time.UTC || a.CreatedAt.IsZero() {
			t.Fatalf("created_at not UTC: %v", a.CreatedAt)
		}
		b, created, err := s.Create(ctx, "k1", req)
		if err != nil || created || b.ID != a.ID {
			t.Fatalf("retry: created=%v id=%s err=%v, want replay of %s", created, b.ID, err, a.ID)
		}
		other := req
		other.Dataset = "orders_v2"
		if _, _, err := s.Create(ctx, "k1", other); !errors.Is(err, job.ErrKeyReused) {
			t.Fatalf("key reuse: %v", err)
		}
		acct2 := req
		acct2.AccountID = 8
		c, created, err := s.Create(ctx, "k1", acct2)
		if err != nil || !created || c.ID == a.ID {
			t.Fatalf("same key, other account must be a new job: %v %v", created, err)
		}
	})

	t.Run("CreateRejectsBadKeys", func(t *testing.T) {
		s := newStore(t)
		for _, k := range []string{"", "has space", strings.Repeat("k", 129), "semi;colon"} {
			if _, _, err := s.Create(ctx, k, req); !errors.Is(err, job.ErrInvalid) {
				t.Errorf("key %q: %v", k, err)
			}
		}
	})

	t.Run("ConcurrentCreateStoresOneJob", func(t *testing.T) {
		s := newStore(t)
		var wg sync.WaitGroup
		ids := make([]string, 16)
		createdN := make([]bool, 16)
		errs := make([]error, 16)
		for i := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				j, c, err := s.Create(ctx, "race", req)
				ids[i], createdN[i], errs[i] = j.ID, c, err
			}()
		}
		wg.Wait()
		n := 0
		for i := range 16 {
			if errs[i] != nil {
				t.Fatalf("create %d: %v", i, errs[i])
			}
			if ids[i] != ids[0] {
				t.Fatalf("two jobs for one key: %s %s", ids[0], ids[i])
			}
			if createdN[i] {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%d creates reported created, want 1", n)
		}
	})

	t.Run("GetUnknown", func(t *testing.T) {
		s := newStore(t)
		if _, err := s.Get(ctx, strings.Repeat("0", 32)); !errors.Is(err, job.ErrNotFound) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("ClaimInCreationOrderOnce", func(t *testing.T) {
		s := newStore(t)
		if _, ok, err := s.Claim(ctx, "w1"); ok || err != nil {
			t.Fatalf("claim on empty store: %v %v", ok, err)
		}
		var want []string
		for i := range 3 {
			j, _, err := s.Create(ctx, fmt.Sprintf("k%d", i), req)
			if err != nil {
				t.Fatal(err)
			}
			want = append(want, j.ID)
		}
		for i := range 3 {
			j, ok, err := s.Claim(ctx, "w1")
			if err != nil || !ok {
				t.Fatalf("claim %d: %v %v", i, ok, err)
			}
			if j.ID != want[i] || j.Status != job.Running || j.Worker != "w1" || j.StartedAt == nil {
				t.Fatalf("claim %d got %+v, want running %s", i, j, want[i])
			}
		}
		if _, ok, err := s.Claim(ctx, "w1"); ok || err != nil {
			t.Fatalf("claim after all taken: %v %v", ok, err)
		}
		if _, _, err := s.Claim(ctx, "bad worker"); !errors.Is(err, job.ErrInvalidArgs) {
			t.Fatalf("bad worker id: %v", err)
		}
	})

	t.Run("ConcurrentClaimsTakeEachJobOnce", func(t *testing.T) {
		s := newStore(t)
		const jobs, workers = 40, 8
		for i := range jobs {
			if _, _, err := s.Create(ctx, fmt.Sprintf("c%d", i), req); err != nil {
				t.Fatal(err)
			}
		}
		var mu sync.Mutex
		seen := map[string]string{}
		var wg sync.WaitGroup
		var firstErr error
		for w := range workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				name := fmt.Sprintf("w%d", w)
				for {
					j, ok, err := s.Claim(ctx, name)
					mu.Lock()
					if err != nil && firstErr == nil {
						firstErr = err
					}
					if err != nil || !ok {
						mu.Unlock()
						return
					}
					if prev, dup := seen[j.ID]; dup {
						firstErr = fmt.Errorf("job %s claimed by %s and %s", j.ID, prev, name)
					}
					seen[j.ID] = name
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if firstErr != nil {
			t.Fatal(firstErr)
		}
		if len(seen) != jobs {
			t.Fatalf("%d jobs claimed, want %d", len(seen), jobs)
		}
	})

	t.Run("ProgressOnlyByOwnerAndForward", func(t *testing.T) {
		s := newStore(t)
		created, _, err := s.Create(ctx, "p", req)
		if err != nil {
			t.Fatal(err)
		}
		cur := keyset.Cursor{"2026-01-01T00:00:00.000000Z", "5"}
		if err := s.Progress(ctx, created.ID, "w1", job.Progress{Rows: 1, Pages: 1, Cursor: cur}); !errors.Is(err, job.ErrNotOwner) {
			t.Fatalf("progress on a queued job: %v", err)
		}
		j, _, err := s.Claim(ctx, "w1")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Progress(ctx, j.ID, "w2", job.Progress{Rows: 1, Pages: 1, Cursor: cur}); !errors.Is(err, job.ErrNotOwner) {
			t.Fatalf("progress by another worker: %v", err)
		}
		if err := s.Progress(ctx, j.ID, "w1", job.Progress{Rows: 500, Pages: 1, Cursor: cur}); err != nil {
			t.Fatal(err)
		}
		if err := s.Progress(ctx, j.ID, "w1", job.Progress{Rows: 400, Pages: 2, Cursor: cur}); !errors.Is(err, job.ErrInvalidArgs) {
			t.Fatalf("backwards rows: %v", err)
		}
		if err := s.Progress(ctx, j.ID, "w1", job.Progress{Rows: 500, Pages: 0, Cursor: cur}); !errors.Is(err, job.ErrInvalidArgs) {
			t.Fatalf("backwards pages: %v", err)
		}
		got, err := s.Get(ctx, j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Rows != 500 || got.Pages != 1 || !slices.Equal(got.Cursor, cur) || got.Status != job.Running {
			t.Fatalf("after progress: %+v", got)
		}
		if err := s.Progress(ctx, strings.Repeat("f", 32), "w1", job.Progress{}); !errors.Is(err, job.ErrNotFound) {
			t.Fatalf("progress on unknown job: %v", err)
		}
	})

	t.Run("FinishAndFail", func(t *testing.T) {
		s := newStore(t)
		for _, k := range []string{"f1", "f2"} {
			if _, _, err := s.Create(ctx, k, req); err != nil {
				t.Fatal(err)
			}
		}
		a, _, _ := s.Claim(ctx, "w1")
		b, _, _ := s.Claim(ctx, "w1")
		if err := s.Finish(ctx, a.ID, "w1", job.Result{Progress: job.Progress{Rows: 1, Pages: 1}}); !errors.Is(err, job.ErrInvalidArgs) {
			t.Fatalf("incomplete result: %v", err)
		}
		if err := s.Finish(ctx, a.ID, "w2", result(10)); !errors.Is(err, job.ErrNotOwner) {
			t.Fatalf("finish by another worker: %v", err)
		}
		if err := s.Finish(ctx, a.ID, "w1", result(10)); err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(ctx, a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != job.Succeeded || got.Rows != 10 || got.Bytes != 100 || got.File != "x.csv" || got.FinishedAt == nil {
			t.Fatalf("after finish: %+v", got)
		}
		if got.FinishedAt.Location() != time.UTC {
			t.Fatalf("finished_at not UTC")
		}
		if err := s.Finish(ctx, a.ID, "w1", result(10)); !errors.Is(err, job.ErrNotOwner) {
			t.Fatalf("second finish: %v", err)
		}
		if err := s.Fail(ctx, a.ID, "w1", "late"); !errors.Is(err, job.ErrNotOwner) {
			t.Fatalf("fail after finish: %v", err)
		}
		long := strings.Repeat("e", 2000)
		if err := s.Fail(ctx, b.ID, "w1", long); err != nil {
			t.Fatal(err)
		}
		got, err = s.Get(ctx, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != job.Failed || len(got.Error) != job.MaxErrorLen {
			t.Fatalf("after fail: status %s, error length %d", got.Status, len(got.Error))
		}
	})
}
