package job_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/job"
	"github.com/EquinoxWN/bulk-export-service/internal/jobtest"
)

func TestMemoryStoreContract(t *testing.T) {
	jobtest.Run(t, func(*testing.T) job.Store { return job.NewMemory(1000) })
}

func TestMemoryStoreFullAndCancelled(t *testing.T) {
	s := job.NewMemory(1)
	ctx := context.Background()
	req := job.Request{AccountID: 1, Dataset: "orders", Format: "csv"}
	if _, _, err := s.Create(ctx, "a", req); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Create(ctx, "b", req); !errors.Is(err, job.ErrStoreFull) {
		t.Fatalf("second job in a store of one: %v", err)
	}
	if _, created, err := s.Create(ctx, "a", req); err != nil || created {
		t.Fatalf("a replay must still work when full: %v %v", created, err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := s.Create(cctx, "c", req); !errors.Is(err, context.Canceled) {
		t.Fatalf("create with cancelled context: %v", err)
	}
	if _, _, err := s.Claim(cctx, "w"); !errors.Is(err, context.Canceled) {
		t.Fatalf("claim with cancelled context: %v", err)
	}
}

func TestMemoryReturnsCopies(t *testing.T) {
	s := job.NewMemory(10)
	ctx := context.Background()
	j, _, err := s.Create(ctx, "a", job.Request{AccountID: 1, Dataset: "orders", Format: "csv"})
	if err != nil {
		t.Fatal(err)
	}
	c, _, _ := s.Claim(ctx, "w")
	cur := []string{"2026-01-01T00:00:00.000000Z", "1"}
	if err := s.Progress(ctx, j.ID, "w", job.Progress{Rows: 1, Pages: 1, Cursor: cur}); err != nil {
		t.Fatal(err)
	}
	cur[1] = "changed"
	c.StartedAt = nil
	got, _ := s.Get(ctx, j.ID)
	if got.Cursor[1] != "1" || got.StartedAt == nil {
		t.Fatal("caller changes leaked into the store")
	}
	got.Cursor[1] = "x"
	again, _ := s.Get(ctx, j.ID)
	if again.Cursor[1] != "1" {
		t.Fatal("returned job shares the store's cursor")
	}
}

func TestRequestValidate(t *testing.T) {
	reg := dataset.Default()
	ok := job.Request{AccountID: 1, Dataset: "orders", Format: "csv"}
	if err := ok.Validate(reg); err != nil {
		t.Fatal(err)
	}
	for name, r := range map[string]job.Request{
		"zero account":     {AccountID: 0, Dataset: "orders", Format: "csv"},
		"negative account": {AccountID: -5, Dataset: "orders", Format: "csv"},
		"unknown dataset":  {AccountID: 1, Dataset: "users", Format: "csv"},
		"empty dataset":    {AccountID: 1, Format: "csv"},
		"parquet":          {AccountID: 1, Dataset: "orders", Format: "parquet"},
		"empty format":     {AccountID: 1, Dataset: "orders"},
	} {
		if err := r.Validate(reg); !errors.Is(err, job.ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestHelpers(t *testing.T) {
	a, err := job.NewID()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := job.NewID()
	if !job.ValidID(a) || a == b {
		t.Fatalf("ids %s %s", a, b)
	}
	for _, s := range []string{"", strings.Repeat("A", 32), strings.Repeat("a", 31), "../" + strings.Repeat("a", 29)} {
		if job.ValidID(s) {
			t.Errorf("ValidID(%q)", s)
		}
	}
	if job.Clip("short") != "short" || len(job.Clip(strings.Repeat("x", 600))) != job.MaxErrorLen {
		t.Fatal("Clip")
	}
	if job.ValidWorker("") || job.ValidWorker(strings.Repeat("w", 65)) || !job.ValidWorker("host-1.w_2") {
		t.Fatal("ValidWorker")
	}
	if err := job.ValidateKey(strings.Repeat("k", 128)); err != nil {
		t.Fatalf("128-char key: %v", err)
	}
	cur := job.Job{Rows: 10, Pages: 2}
	if job.CheckProgress(cur, job.Progress{Rows: 10, Pages: 2}) != nil || job.CheckProgress(cur, job.Progress{Rows: 9, Pages: 3}) == nil ||
		job.CheckProgress(cur, job.Progress{Rows: 11, Pages: 1}) == nil || job.CheckProgress(job.Job{}, job.Progress{Rows: -1}) == nil {
		t.Fatal("CheckProgress")
	}
}
