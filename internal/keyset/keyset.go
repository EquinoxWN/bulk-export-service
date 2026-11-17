// Package keyset builds keyset ("seek") page queries and the cursors that link the pages.
package keyset

import (
	"errors"
	"fmt"
	"strings"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
)

// MaxPageSize bounds one page so a worker's memory and one query's run time stay bounded.
const MaxPageSize = 10000

// Cursor holds the key values of the last row read, in canonical text form; nil means "from the start".
type Cursor []string

// ErrBadCursor is returned for a cursor that does not fit the dataset's key.
var ErrBadCursor = errors.New("bad cursor")

// Typed parses the cursor into typed key values for the dataset.
func (c Cursor) Typed(ds *dataset.Dataset) ([]any, error) {
	types := ds.KeyTypes()
	if len(c) != len(types) {
		return nil, fmt.Errorf("%w: %d values for a %d-column key", ErrBadCursor, len(c), len(types))
	}
	out := make([]any, len(c))
	for i, s := range c {
		v, err := dataset.Parse(types[i], s)
		if err != nil {
			return nil, fmt.Errorf("%w: key %s: %v", ErrBadCursor, ds.Key[i], err)
		}
		out[i] = v
	}
	return out, nil
}

// FromRow returns the cursor that points just after a typed row.
func FromRow(ds *dataset.Dataset, row []any) (Cursor, error) {
	if len(row) != len(ds.Columns) {
		return nil, fmt.Errorf("%w: row has %d values, dataset has %d columns", ErrBadCursor, len(row), len(ds.Columns))
	}
	idx := ds.KeyIndexes()
	out := make(Cursor, len(idx))
	for i, j := range idx {
		if row[j] == nil {
			return nil, fmt.Errorf("%w: key %s is NULL", ErrBadCursor, ds.Key[i])
		}
		s, err := dataset.Format(ds.Columns[j].Type, row[j])
		if err != nil {
			return nil, fmt.Errorf("key %s: %w", ds.Key[i], err)
		}
		out[i] = s
	}
	return out, nil
}

// CompareKey orders two rows by the dataset's key columns.
func CompareKey(ds *dataset.Dataset, a, b []any) int {
	types := ds.KeyTypes()
	for i, j := range ds.KeyIndexes() {
		if c := dataset.Compare(types[i], a[j], b[j]); c != 0 {
			return c
		}
	}
	return 0
}

// CompareToCursor orders a row against typed cursor key values.
func CompareToCursor(ds *dataset.Dataset, row []any, after []any) int {
	types := ds.KeyTypes()
	for i, j := range ds.KeyIndexes() {
		if c := dataset.Compare(types[i], row[j], after[i]); c != 0 {
			return c
		}
	}
	return 0
}

func quote(id string) string { return `"` + id + `"` }

func selectFrom(ds *dataset.Dataset) string {
	cols := make([]string, len(ds.Columns))
	for i, c := range ds.Columns {
		cols[i] = quote(c.Name)
	}
	return "SELECT " + strings.Join(cols, ", ") + " FROM " + quote(ds.Table) + " WHERE " + quote(ds.AccountColumn) + " = $1"
}

func orderBy(ds *dataset.Dataset) string {
	keys := make([]string, len(ds.Key))
	for i, k := range ds.Key {
		keys[i] = quote(k)
	}
	return " ORDER BY " + strings.Join(keys, ", ")
}

func checkLimit(limit int) error {
	if limit < 1 || limit > MaxPageSize {
		return fmt.Errorf("page size %d out of range 1..%d", limit, MaxPageSize)
	}
	return nil
}

// PageSQL builds the keyset query for the page after the cursor: a row-value comparison on the key, so
// the database seeks in the (account, key...) index instead of reading and discarding earlier rows.
func PageSQL(ds *dataset.Dataset, account int64, after Cursor, limit int) (string, []any, error) {
	if err := checkLimit(limit); err != nil {
		return "", nil, err
	}
	args := []any{account}
	q := selectFrom(ds)
	if after != nil {
		vals, err := after.Typed(ds)
		if err != nil {
			return "", nil, err
		}
		keys := make([]string, len(ds.Key))
		ph := make([]string, len(ds.Key))
		for i, k := range ds.Key {
			keys[i] = quote(k)
			args = append(args, vals[i])
			ph[i] = fmt.Sprintf("$%d", len(args))
		}
		q += " AND (" + strings.Join(keys, ", ") + ") > (" + strings.Join(ph, ", ") + ")"
	}
	args = append(args, limit)
	q += orderBy(ds) + fmt.Sprintf(" LIMIT $%d", len(args))
	return q, args, nil
}

// OffsetSQL builds the OFFSET query for the same page, kept only as the baseline that keyset is compared with.
func OffsetSQL(ds *dataset.Dataset, account int64, offset int64, limit int) (string, []any, error) {
	if err := checkLimit(limit); err != nil {
		return "", nil, err
	}
	if offset < 0 {
		return "", nil, fmt.Errorf("negative offset %d", offset)
	}
	return selectFrom(ds) + orderBy(ds) + " LIMIT $2 OFFSET $3", []any{account, limit, offset}, nil
}
