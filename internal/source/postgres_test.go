package source

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
	"github.com/EquinoxWN/bulk-export-service/internal/pgtest"
)

func openReplica(t *testing.T, db pgtest.DB) *Postgres {
	t.Helper()
	p, err := OpenPostgres(context.Background(), db.Replica, 2, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestPGKeysetWalkMatchesOrderedSelect(t *testing.T) {
	db := pgtest.Setup(t, "source_walk")
	pgtest.SeedOrders(t, db, 3, 2500, 5)
	p := openReplica(t, db)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db.Primary)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }() // test connection
	for a := int64(1); a <= 3; a++ {
		var want []int64
		rows, err := conn.Query(ctx, "SELECT id FROM orders WHERE account_id = $1 ORDER BY created_at, id", a)
		if err != nil {
			t.Fatal(err)
		}
		want, err = pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			t.Fatal(err)
		}
		for _, page := range []int{1, 7, 1000, 2500, 10000} {
			if page == 1 && a > 1 {
				continue // one row per page is slow; account 1 covers it
			}
			got := walk(t, p, a, page)
			if !slices.Equal(got, want) {
				t.Fatalf("account %d page %d: %d ids, want %d (first difference matters)", a, page, len(got), len(want))
			}
		}
	}
}

func TestPGKeysetEqualsOffsetOnAStaticTable(t *testing.T) {
	db := pgtest.Setup(t, "source_offset")
	pgtest.SeedOrders(t, db, 1, 1200, 6)
	p := openReplica(t, db)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db.Replica)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }() // test connection
	var after keyset.Cursor
	for off := int64(0); ; off += 250 {
		page, err := p.ReadPage(ctx, &dataset.Orders, 1, after, 250)
		if err != nil {
			t.Fatal(err)
		}
		q, args, err := keyset.OffsetSQL(&dataset.Orders, 1, off, 250)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := conn.Query(ctx, q, args...)
		if err != nil {
			t.Fatal(err)
		}
		var offIDs []int64
		for rows.Next() {
			v, err := rows.Values()
			if err != nil {
				t.Fatal(err)
			}
			offIDs = append(offIDs, v[0].(int64))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		var ids []int64
		for _, r := range page.Rows {
			ids = append(ids, r[0].(int64))
		}
		if !slices.Equal(ids, offIDs) {
			t.Fatalf("offset %d: keyset and OFFSET pages differ", off)
		}
		if page.Last {
			break
		}
		after = page.Next
	}
}

func TestPGReplicaPoolCannotWrite(t *testing.T) {
	db := pgtest.Setup(t, "source_readonly")
	pgtest.SeedOrders(t, db, 1, 10, 1)
	p := openReplica(t, db)
	ctx := context.Background()
	_, err := p.pool.Exec(ctx, "INSERT INTO orders VALUES (999999, 1, now(), 'paid', 1, '')")
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || (pe.Code != "25006" && pe.Code != "42501") {
		t.Fatalf("write through the replica pool: %v, want read-only (25006) or permission (42501) error", err)
	}
	inRecovery, err := p.InRecovery(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pg_is_in_recovery() = %v (the CI stand-in is a read-only role on a primary)", inRecovery)
}

func TestPGKeysetPlanUsesTheIndexWithoutSort(t *testing.T) {
	db := pgtest.Setup(t, "source_plan")
	pgtest.SeedOrders(t, db, 5, 10000, 7)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, db.Replica)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }() // test connection
	q, args, err := keyset.PageSQL(&dataset.Orders, 3, keyset.Cursor{"2026-01-01T00:30:00.000000Z", "0"}, 1000)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, "EXPLAIN "+q, args...)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(lines, "\n")
	t.Logf("plan:\n%s", plan)
	if !strings.Contains(plan, "orders_keyset") || strings.Contains(plan, "Sort") || strings.Contains(plan, "Seq Scan") {
		t.Fatalf("keyset page is not an ordered index range scan:\n%s", plan)
	}
}

func TestOpenPostgresRejectsBadSettings(t *testing.T) {
	ctx := context.Background()
	if _, err := OpenPostgres(ctx, "postgres://x@127.0.0.1:1/x", 0, time.Second); err == nil {
		t.Error("pool size 0 accepted")
	}
	if _, err := OpenPostgres(ctx, "postgres://x@127.0.0.1:1/x", 2, time.Millisecond); err == nil {
		t.Error("1ms statement timeout accepted")
	}
	if _, err := OpenPostgres(ctx, "::not a dsn::", 2, time.Second); err == nil {
		t.Error("bad DSN accepted")
	}
	// Nothing listens on port 1: the ping fails within the connect timeout.
	start := time.Now()
	if _, err := OpenPostgres(ctx, "postgres://x@127.0.0.1:1/x?connect_timeout=2", 2, time.Second); err == nil {
		t.Error("unreachable server accepted")
	}
	if time.Since(start) > ConnectTimeout+5*time.Second {
		t.Error("connect not bounded")
	}
}

func TestNormalize(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 1, 0, 0, 0, time.FixedZone("x", 3600))
	got, err := normalize(&dataset.Orders, []any{int32(1), int16(2), t0, "s", int64(3), nil})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != int64(1) || got[1] != int64(2) || got[2].(time.Time).Location() != time.UTC || got[5] != nil {
		t.Fatalf("normalize: %#v", got)
	}
	if _, err := normalize(&dataset.Orders, []any{int64(1)}); err == nil {
		t.Fatal("short row accepted")
	}
	if _, err := normalize(&dataset.Orders, []any{"x", int64(1), t0, "s", int64(3), nil}); err == nil {
		t.Fatal("text id accepted")
	}
}
