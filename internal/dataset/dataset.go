// Package dataset describes the tables an export may read: columns, types and the keyset order.
package dataset

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// Type is the column type an export knows how to read, format and compare.
type Type int

// Supported column types.
const (
	Int64 Type = iota + 1
	Text
	Timestamp
)

// TimeLayout is the fixed UTC text form of timestamps in CSV files and cursors (microseconds, as PostgreSQL stores them).
const TimeLayout = "2006-01-02T15:04:05.000000Z"

// Column is one exported column.
type Column struct {
	Name string
	Type Type
}

// Dataset is one exportable table, scoped by an account column and read in keyset order.
type Dataset struct {
	Name          string
	Table         string
	AccountColumn string
	Columns       []Column
	// Key lists the columns of the keyset order; together with the account they must be unique.
	Key []string
}

var identRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Validate checks identifiers and that every key column is an exported column.
func (d *Dataset) Validate() error {
	if !identRe.MatchString(d.Name) {
		return fmt.Errorf("dataset name %q: want lower-case letters, digits and _", d.Name)
	}
	for _, id := range []string{d.Table, d.AccountColumn} {
		if !identRe.MatchString(id) {
			return fmt.Errorf("dataset %s: bad identifier %q", d.Name, id)
		}
	}
	if len(d.Columns) == 0 {
		return fmt.Errorf("dataset %s: no columns", d.Name)
	}
	seen := map[string]bool{}
	for _, c := range d.Columns {
		if !identRe.MatchString(c.Name) {
			return fmt.Errorf("dataset %s: bad column name %q", d.Name, c.Name)
		}
		if seen[c.Name] {
			return fmt.Errorf("dataset %s: duplicate column %q", d.Name, c.Name)
		}
		if c.Type < Int64 || c.Type > Timestamp {
			return fmt.Errorf("dataset %s: column %s has unknown type", d.Name, c.Name)
		}
		seen[c.Name] = true
	}
	if len(d.Key) == 0 {
		return fmt.Errorf("dataset %s: empty key", d.Name)
	}
	keySeen := map[string]bool{}
	for _, k := range d.Key {
		if !seen[k] {
			return fmt.Errorf("dataset %s: key column %q is not an exported column", d.Name, k)
		}
		if keySeen[k] {
			return fmt.Errorf("dataset %s: key column %q repeated", d.Name, k)
		}
		keySeen[k] = true
	}
	return nil
}

// KeyIndexes returns the position of each key column in Columns.
func (d *Dataset) KeyIndexes() []int {
	out := make([]int, len(d.Key))
	for i, k := range d.Key {
		for j, c := range d.Columns {
			if c.Name == k {
				out[i] = j
			}
		}
	}
	return out
}

// KeyTypes returns the type of each key column.
func (d *Dataset) KeyTypes() []Type {
	idx := d.KeyIndexes()
	out := make([]Type, len(idx))
	for i, j := range idx {
		out[i] = d.Columns[j].Type
	}
	return out
}

// ErrBadValue is returned when a value does not match its column type.
var ErrBadValue = errors.New("value does not match column type")

// Format renders one typed value as its canonical text; NULL (nil) becomes the empty string.
func Format(t Type, v any) (string, error) {
	if v == nil {
		return "", nil
	}
	switch t {
	case Int64:
		switch n := v.(type) {
		case int64:
			return strconv.FormatInt(n, 10), nil
		case int32:
			return strconv.FormatInt(int64(n), 10), nil
		case int:
			return strconv.Itoa(n), nil
		}
	case Text:
		if s, ok := v.(string); ok {
			return s, nil
		}
	case Timestamp:
		if ts, ok := v.(time.Time); ok {
			return ts.UTC().Format(TimeLayout), nil
		}
	}
	return "", fmt.Errorf("%w: %T", ErrBadValue, v)
}

// Parse turns canonical text back into a typed value.
func Parse(t Type, s string) (any, error) {
	switch t {
	case Int64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadValue, err)
		}
		return n, nil
	case Text:
		return s, nil
	case Timestamp:
		ts, err := time.Parse(TimeLayout, s)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBadValue, err)
		}
		return ts, nil
	}
	return nil, fmt.Errorf("%w: unknown type %d", ErrBadValue, t)
}

// Compare orders two typed values of the same column type (-1, 0, 1).
func Compare(t Type, a, b any) int {
	switch t {
	case Int64:
		x, y := a.(int64), b.(int64)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	case Timestamp:
		return a.(time.Time).Compare(b.(time.Time))
	default:
		x, y := a.(string), b.(string)
		switch {
		case x < y:
			return -1
		case x > y:
			return 1
		}
		return 0
	}
}

// Registry maps dataset names to their definitions.
type Registry map[string]*Dataset

// NewRegistry validates the datasets and indexes them by name.
func NewRegistry(ds ...Dataset) (Registry, error) {
	r := Registry{}
	for i := range ds {
		d := ds[i]
		if err := d.Validate(); err != nil {
			return nil, err
		}
		if _, dup := r[d.Name]; dup {
			return nil, fmt.Errorf("dataset %s defined twice", d.Name)
		}
		r[d.Name] = &d
	}
	return r, nil
}

// Orders is the example dataset: one row per order, keyset order (created_at, id).
var Orders = Dataset{
	Name:          "orders",
	Table:         "orders",
	AccountColumn: "account_id",
	Columns: []Column{
		{Name: "id", Type: Int64},
		{Name: "account_id", Type: Int64},
		{Name: "created_at", Type: Timestamp},
		{Name: "status", Type: Text},
		{Name: "amount_cents", Type: Int64},
		{Name: "note", Type: Text},
	},
	Key: []string{"created_at", "id"},
}

// Default returns the registry of the built-in datasets.
func Default() Registry {
	r, err := NewRegistry(Orders)
	if err != nil {
		panic(err) // the built-in definitions are constants; a failure is a programming error
	}
	return r
}
