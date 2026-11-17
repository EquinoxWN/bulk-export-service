// Package pgtest prepares a PostgreSQL schema, a seeded orders table and a read-only "replica" role for
// the server tests. The tests skip unless BULK_EXPORT_TEST_DATABASE_URL is set (CI service container).
package pgtest

import (
	"context"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/EquinoxWN/bulk-export-service/internal/sample"
)

// EnvVar names the test database URL; the server must trust the reader role without a password (as CI does).
const EnvVar = "BULK_EXPORT_TEST_DATABASE_URL"

// DB holds the DSNs of one test schema.
type DB struct {
	Primary string // superuser connection, search_path = the schema
	Replica string // read-only role, search_path = the schema
	Schema  string
}

// Setup creates a fresh schema bes_<name> and a reader role that may only SELECT from it.
// The role is a stand-in for a streaming replica: same server, read-only by grants and by default.
func Setup(t *testing.T, name string) DB {
	t.Helper()
	base := os.Getenv(EnvVar)
	if base == "" {
		t.Skip(EnvVar + " not set: PostgreSQL tests run in CI (make test-pg)")
	}
	schema, role := "bes_"+name, "bes_reader_"+name
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }() // test setup connection; close errors do not matter
	for _, q := range []string{
		"DROP SCHEMA IF EXISTS " + schema + " CASCADE",
		"DROP ROLE IF EXISTS " + role,
		"CREATE SCHEMA " + schema,
		"CREATE ROLE " + role + " LOGIN",
		"ALTER ROLE " + role + " SET default_transaction_read_only = on",
		"GRANT USAGE ON SCHEMA " + schema + " TO " + role,
		"ALTER DEFAULT PRIVILEGES IN SCHEMA " + schema + " GRANT SELECT ON TABLES TO " + role,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse %s: %v", EnvVar, err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	primary := u.String()
	u.User = url.User(role)
	return DB{Primary: primary, Replica: u.String(), Schema: schema}
}

// SeedOrders creates the orders table with its keyset index and loads sample.Orders(accounts, perAccount, seed).
func SeedOrders(t *testing.T, db DB, accounts, perAccount int, seed uint64) [][]any {
	t.Helper()
	rows, err := sample.Orders(accounts, perAccount, seed)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, db.Primary)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(context.Background()) }() // test setup connection; close errors do not matter
	if _, err := conn.Exec(ctx, `CREATE TABLE orders (
		id bigint PRIMARY KEY, account_id bigint NOT NULL, created_at timestamptz NOT NULL,
		status text NOT NULL, amount_cents bigint NOT NULL, note text)`); err != nil {
		t.Fatalf("create orders: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE INDEX orders_keyset ON orders (account_id, created_at, id)`); err != nil {
		t.Fatalf("create index: %v", err)
	}
	n, err := conn.CopyFrom(ctx, pgx.Identifier{"orders"}, []string{"id", "account_id", "created_at", "status", "amount_cents", "note"}, pgx.CopyFromRows(rows))
	if err != nil || int(n) != len(rows) {
		t.Fatalf("copy orders: %d of %d: %v", n, len(rows), err)
	}
	if _, err := conn.Exec(ctx, "ANALYZE orders"); err != nil {
		t.Fatalf("analyze: %v", err)
	}
	return rows
}
