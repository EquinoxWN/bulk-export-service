package pgstore

import (
	"context"
	"errors"
	"testing"

	"github.com/EquinoxWN/bulk-export-service/internal/job"
	"github.com/EquinoxWN/bulk-export-service/internal/jobtest"
	"github.com/EquinoxWN/bulk-export-service/internal/pgtest"
)

func TestPGStoreContract(t *testing.T) {
	db := pgtest.Setup(t, "pgstore")
	jobtest.Run(t, func(t *testing.T) job.Store {
		s, err := Open(context.Background(), db.Primary, 8)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		// Each contract case starts from an empty jobs table in the test schema.
		if _, err := s.pool.Exec(context.Background(), "DELETE FROM export_jobs"); err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestPGOpenIsRepeatableAndRejectsBadSettings(t *testing.T) {
	ctx := context.Background()
	if _, err := Open(ctx, "postgres://x@127.0.0.1:1/x", 0); err == nil {
		t.Error("pool size 0 accepted")
	}
	if _, err := Open(ctx, "::bad::", 2); err == nil {
		t.Error("bad DSN accepted")
	}
	db := pgtest.Setup(t, "pgstore_open")
	for range 2 { // CREATE ... IF NOT EXISTS: a second start on the same database works
		s, err := Open(ctx, db.Primary, 2)
		if err != nil {
			t.Fatal(err)
		}
		s.Close()
	}
	s, err := Open(ctx, db.Primary, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Get(ctx, "not-an-id"); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("malformed id: %v", err)
	}
}
