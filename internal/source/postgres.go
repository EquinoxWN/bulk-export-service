package source

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
)

// ConnectTimeout bounds every new database connection.
const ConnectTimeout = 10 * time.Second

// Postgres reads pages from the read-replica pool; every page runs in its own READ ONLY transaction
// with a statement timeout, and the session default is read-only too.
type Postgres struct {
	pool             *pgxpool.Pool
	statementTimeout time.Duration
}

// OpenPostgres connects the replica pool (small, bounded) and checks it answers.
func OpenPostgres(ctx context.Context, dsn string, maxConns int32, statementTimeout time.Duration) (*Postgres, error) {
	if maxConns < 1 || maxConns > 64 {
		return nil, fmt.Errorf("replica pool size %d out of range 1..64", maxConns)
	}
	if statementTimeout < 100*time.Millisecond || statementTimeout > time.Hour {
		return nil, fmt.Errorf("statement timeout %s out of range 100ms..1h", statementTimeout)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("replica DSN: %w", err)
	}
	cfg.MaxConns = maxConns
	cfg.ConnConfig.ConnectTimeout = ConnectTimeout
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	cfg.ConnConfig.RuntimeParams["application_name"] = "bulk-export-reader"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("replica pool: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("replica ping: %w", err)
	}
	return &Postgres{pool: pool, statementTimeout: statementTimeout}, nil
}

// Close releases the pool.
func (p *Postgres) Close() { p.pool.Close() }

// InRecovery reports whether the server is a standby (pg_is_in_recovery()).
func (p *Postgres) InRecovery(ctx context.Context) (bool, error) {
	var r bool
	if err := p.pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&r); err != nil {
		return false, fmt.Errorf("pg_is_in_recovery: %w", err)
	}
	return r, nil
}

// ReadPage runs the keyset query for one page in a READ ONLY transaction.
func (p *Postgres) ReadPage(ctx context.Context, ds *dataset.Dataset, account int64, after keyset.Cursor, limit int) (Page, error) {
	q, args, err := keyset.PageSQL(ds, account, after, limit)
	if err != nil {
		return Page{}, err
	}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return Page{}, fmt.Errorf("begin read-only: %w", err)
	}
	// Rollback after a read-only transaction is the normal end; its error carries no information.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	timeout := fmt.Sprintf("%dms", p.statementTimeout.Milliseconds())
	if _, err := tx.Exec(ctx, "SELECT set_config('statement_timeout', $1, true)", timeout); err != nil {
		return Page{}, fmt.Errorf("set statement_timeout: %w", err)
	}
	rows, err := tx.Query(ctx, q, args...)
	if err != nil {
		return Page{}, fmt.Errorf("page query: %w", err)
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return Page{}, fmt.Errorf("page row: %w", err)
		}
		row, err := normalize(ds, vals)
		if err != nil {
			return Page{}, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return Page{}, fmt.Errorf("page rows: %w", err)
	}
	return finish(ds, out, after, limit)
}

// normalize converts driver values to the dataset's Go types (int64, string, time.Time, or nil).
func normalize(ds *dataset.Dataset, vals []any) ([]any, error) {
	if len(vals) != len(ds.Columns) {
		return nil, fmt.Errorf("query returned %d columns, want %d", len(vals), len(ds.Columns))
	}
	out := make([]any, len(vals))
	for i, v := range vals {
		c := ds.Columns[i]
		switch x := v.(type) {
		case nil:
			out[i] = nil
		case int16:
			out[i] = int64(x)
		case int32:
			out[i] = int64(x)
		case time.Time:
			out[i] = x.UTC()
		default:
			out[i] = x
		}
		if _, err := dataset.Format(c.Type, out[i]); err != nil {
			return nil, fmt.Errorf("column %s: %w", c.Name, err)
		}
	}
	return out, nil
}

// ErrNotStandby is returned when a standby is required but the replica DSN points at a primary.
var ErrNotStandby = errors.New("replica DSN points at a server that is not in recovery")
