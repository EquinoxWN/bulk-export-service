// Package source reads export rows page by page: an in-memory table for tests and demos, and PostgreSQL
// through a read-replica pool in read-only transactions.
package source

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
)

// Page is one keyset page: typed rows in key order and the cursor after the last one.
type Page struct {
	Rows [][]any
	Next keyset.Cursor // cursor after the last row; equal to the input cursor when Rows is empty
	Last bool          // fewer rows than asked for: nothing follows
}

// Reader reads one page of an account's rows after a cursor.
type Reader interface {
	ReadPage(ctx context.Context, ds *dataset.Dataset, account int64, after keyset.Cursor, limit int) (Page, error)
}

// finish fills Next and Last for the rows of a page.
func finish(ds *dataset.Dataset, rows [][]any, after keyset.Cursor, limit int) (Page, error) {
	p := Page{Rows: rows, Next: after, Last: len(rows) < limit}
	if len(rows) > 0 {
		c, err := keyset.FromRow(ds, rows[len(rows)-1])
		if err != nil {
			return Page{}, err
		}
		p.Next = c
	}
	return p, nil
}

// Memory is an in-process table per (dataset, account), kept sorted by key; it counts the rows it
// examines so keyset and OFFSET reads can be compared without a database.
type Memory struct {
	mu       sync.RWMutex
	tables   map[string]map[int64][][]any
	examined int64
}

// NewMemory returns an empty in-memory source.
func NewMemory() *Memory { return &Memory{tables: map[string]map[int64][][]any{}} }

// Insert adds typed rows, keeping each account's rows sorted by key; a duplicate key is refused.
func (m *Memory) Insert(ds *dataset.Dataset, rows ...[]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	acctIdx := -1
	for i, c := range ds.Columns {
		if c.Name == ds.AccountColumn {
			acctIdx = i
		}
	}
	if acctIdx < 0 {
		return fmt.Errorf("dataset %s does not export its account column", ds.Name)
	}
	if m.tables[ds.Name] == nil {
		m.tables[ds.Name] = map[int64][][]any{}
	}
	touched := map[int64]bool{}
	for _, r := range rows {
		if len(r) != len(ds.Columns) {
			return fmt.Errorf("row has %d values, want %d", len(r), len(ds.Columns))
		}
		for i, c := range ds.Columns {
			if _, err := dataset.Format(c.Type, r[i]); err != nil {
				return fmt.Errorf("column %s: %w", c.Name, err)
			}
		}
		if _, err := keyset.FromRow(ds, r); err != nil {
			return err
		}
		acct, ok := r[acctIdx].(int64)
		if !ok {
			return fmt.Errorf("account column %s is not int64", ds.AccountColumn)
		}
		m.tables[ds.Name][acct] = append(m.tables[ds.Name][acct], r)
		touched[acct] = true
	}
	for acct := range touched {
		t := m.tables[ds.Name][acct]
		sort.SliceStable(t, func(i, j int) bool { return keyset.CompareKey(ds, t[i], t[j]) < 0 })
		for i := 1; i < len(t); i++ {
			if keyset.CompareKey(ds, t[i-1], t[i]) == 0 {
				return fmt.Errorf("duplicate key in dataset %s account %d", ds.Name, acct)
			}
		}
	}
	return nil
}

// Count returns the number of rows an account has in a dataset.
func (m *Memory) Count(ds *dataset.Dataset, account int64) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.tables[ds.Name][account])
}

// Examined returns the rows read so far (what a database would scan), and resets the counter.
func (m *Memory) Examined() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.examined
	m.examined = 0
	return n
}

// ReadPage seeks to the first row after the cursor (binary search, like an index) and returns up to limit rows.
func (m *Memory) ReadPage(ctx context.Context, ds *dataset.Dataset, account int64, after keyset.Cursor, limit int) (Page, error) {
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	if limit < 1 || limit > keyset.MaxPageSize {
		return Page{}, fmt.Errorf("page size %d out of range 1..%d", limit, keyset.MaxPageSize)
	}
	var vals []any
	if after != nil {
		v, err := after.Typed(ds)
		if err != nil {
			return Page{}, err
		}
		vals = v
	}
	m.mu.Lock()
	t := m.tables[ds.Name][account]
	start := 0
	if vals != nil {
		start = sort.Search(len(t), func(i int) bool { return keyset.CompareToCursor(ds, t[i], vals) > 0 })
	}
	end := min(start+limit, len(t))
	rows := make([][]any, end-start)
	copy(rows, t[start:end])
	m.examined += int64(end - start)
	m.mu.Unlock()
	return finish(ds, rows, after, limit)
}

// ReadOffsetPage is the OFFSET baseline: it walks past offset rows before returning up to limit rows.
func (m *Memory) ReadOffsetPage(ctx context.Context, ds *dataset.Dataset, account int64, offset int64, limit int) ([][]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit < 1 || limit > keyset.MaxPageSize || offset < 0 {
		return nil, fmt.Errorf("bad offset page (offset %d, limit %d)", offset, limit)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tables[ds.Name][account]
	start := min(int(offset), len(t))
	end := min(start+limit, len(t))
	m.examined += int64(end) // a database reads and discards the first offset rows
	rows := make([][]any, end-start)
	copy(rows, t[start:end])
	return rows, nil
}
