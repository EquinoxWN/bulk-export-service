package worker

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/job"
	"github.com/EquinoxWN/bulk-export-service/internal/pgstore"
	"github.com/EquinoxWN/bulk-export-service/internal/pgtest"
	"github.com/EquinoxWN/bulk-export-service/internal/source"
)

// Jobs on the primary connection, rows from the read-only replica role (same server in CI): two workers
// export four accounts and every file matches an ordered SELECT.
func TestPGEndToEndJobsOnPrimaryRowsFromReplica(t *testing.T) {
	db := pgtest.Setup(t, "worker_e2e")
	pgtest.SeedOrders(t, db, 4, 3000, 11)
	ctx := context.Background()
	store, err := pgstore.Open(ctx, db.Primary, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	replica, err := source.OpenPostgres(ctx, db.Replica, 4, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()

	ids := map[int64]string{}
	for a := int64(1); a <= 4; a++ {
		j, created, err := store.Create(ctx, fmt.Sprintf("e2e-%d", a), job.Request{AccountID: a, Dataset: "orders", Format: "csv"})
		if err != nil || !created {
			t.Fatal(created, err)
		}
		ids[a] = j.ID
	}
	c := cfg(t)
	c.PageSize = 700
	workers := make([]*Worker, 2)
	for i := range workers {
		cc := c
		cc.ID = fmt.Sprintf("pg-w%d", i)
		workers[i] = newWorker(t, cc, store, replica)
	}
	done := make(chan error, 2)
	for _, w := range workers {
		go func() {
			for {
				did, err := w.RunOnce(ctx)
				if err != nil || !did {
					done <- err
					return
				}
			}
		}()
	}
	for range workers {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}

	conn, err := pgx.Connect(ctx, db.Primary)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }() // test connection
	for a, id := range ids {
		j, err := store.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status != job.Succeeded || j.Rows != 3000 || j.Pages != 5 {
			t.Fatalf("account %d: %+v", a, j)
		}
		recs, _ := readCSV(t, filepath.Join(c.OutDir, j.File))
		rows, err := conn.Query(ctx, "SELECT id FROM orders WHERE account_id = $1 ORDER BY created_at, id", a)
		if err != nil {
			t.Fatal(err)
		}
		want, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			t.Fatal(err)
		}
		if len(recs)-1 != len(want) {
			t.Fatalf("account %d: %d rows in file, %d in table", a, len(recs)-1, len(want))
		}
		for i, id := range want {
			got, _ := dataset.Parse(dataset.Int64, recs[i+1][0])
			if got != id {
				t.Fatalf("account %d row %d: id %v, want %d", a, i, got, id)
			}
		}
	}
	// Every read of orders came through the reader role: its session runs read-only.
	var readOnly string
	rconn, err := pgx.Connect(ctx, db.Replica)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rconn.Close(ctx) }() // test connection
	if err := rconn.QueryRow(ctx, "SHOW default_transaction_read_only").Scan(&readOnly); err != nil || readOnly != "on" {
		t.Fatalf("reader role default_transaction_read_only = %q, %v", readOnly, err)
	}
}
