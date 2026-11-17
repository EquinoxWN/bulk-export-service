package source

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/keyset"
	"github.com/EquinoxWN/bulk-export-service/internal/sample"
)

var ds = &dataset.Orders

func seeded(t *testing.T, accounts, per int, seed uint64) (*Memory, [][]any) {
	t.Helper()
	rows, err := sample.Orders(accounts, per, seed)
	if err != nil {
		t.Fatal(err)
	}
	m := NewMemory()
	if err := m.Insert(ds, rows...); err != nil {
		t.Fatal(err)
	}
	return m, rows
}

// walk reads an account to the end with the given page size and returns the ids in order.
func walk(t *testing.T, r Reader, account int64, page int) []int64 {
	t.Helper()
	var ids []int64
	var after keyset.Cursor
	for i := 0; ; i++ {
		if i > 1_000_000 {
			t.Fatal("walk does not end")
		}
		p, err := r.ReadPage(context.Background(), ds, account, after, page)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Rows) > page {
			t.Fatalf("page of %d rows, limit %d", len(p.Rows), page)
		}
		for _, row := range p.Rows {
			ids = append(ids, row[0].(int64))
		}
		after = p.Next
		if p.Last {
			return ids
		}
	}
}

func TestKeysetWalkReturnsEveryRowOnceInKeyOrderRandomized(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for trial := range 40 {
		accounts, per := 1+r.IntN(4), r.IntN(300)
		m, rows := seeded(t, accounts, per, uint64(trial))
		for a := 1; a <= accounts; a++ {
			page := 1 + r.IntN(40)
			ids := walk(t, m, int64(a), page)
			if len(ids) != per {
				t.Fatalf("trial %d account %d page %d: %d rows, want %d", trial, a, page, len(ids), per)
			}
			seen := map[int64]bool{}
			for _, id := range ids {
				if seen[id] {
					t.Fatalf("trial %d: id %d twice", trial, id)
				}
				seen[id] = true
			}
			// Check against a direct sort of the generated rows.
			var want [][]any
			for _, row := range rows {
				if row[1].(int64) == int64(a) {
					want = append(want, row)
				}
			}
			for i := 1; i < len(want); i++ {
				for j := i; j > 0 && keyset.CompareKey(ds, want[j], want[j-1]) < 0; j-- {
					want[j], want[j-1] = want[j-1], want[j]
				}
			}
			for i, row := range want {
				if ids[i] != row[0].(int64) {
					t.Fatalf("trial %d account %d: position %d has id %d, want %d", trial, a, i, ids[i], row[0])
				}
			}
		}
	}
}

func TestPageFlagsAndCursor(t *testing.T) {
	m, _ := seeded(t, 1, 6, 1)
	ctx := context.Background()
	p, err := m.ReadPage(ctx, ds, 1, nil, 3)
	if err != nil || len(p.Rows) != 3 || p.Last {
		t.Fatalf("first page: %d rows, last %v, %v", len(p.Rows), p.Last, err)
	}
	want, _ := keyset.FromRow(ds, p.Rows[2])
	if len(p.Next) != 2 || p.Next[0] != want[0] || p.Next[1] != want[1] {
		t.Fatalf("next %v, want %v", p.Next, want)
	}
	p, err = m.ReadPage(ctx, ds, 1, p.Next, 3)
	if err != nil || len(p.Rows) != 3 || p.Last {
		t.Fatalf("second page: %d rows, last %v, %v", len(p.Rows), p.Last, err)
	}
	end, err := m.ReadPage(ctx, ds, 1, p.Next, 3)
	if err != nil || len(end.Rows) != 0 || !end.Last || end.Next[1] != p.Next[1] {
		t.Fatalf("empty last page: %+v %v", end, err)
	}
	empty, err := m.ReadPage(ctx, ds, 99, nil, 3)
	if err != nil || len(empty.Rows) != 0 || !empty.Last || empty.Next != nil {
		t.Fatalf("unknown account: %+v %v", empty, err)
	}
}

func TestReadPageErrors(t *testing.T) {
	m, _ := seeded(t, 1, 3, 1)
	for _, n := range []int{0, keyset.MaxPageSize + 1} {
		if _, err := m.ReadPage(context.Background(), ds, 1, nil, n); err == nil {
			t.Errorf("limit %d accepted", n)
		}
	}
	if _, err := m.ReadPage(context.Background(), ds, 1, keyset.Cursor{"bad"}, 2); !errors.Is(err, keyset.ErrBadCursor) {
		t.Errorf("bad cursor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.ReadPage(ctx, ds, 1, nil, 2); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled context: %v", err)
	}
	if _, err := m.ReadOffsetPage(ctx, ds, 1, 0, 2); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled context (offset): %v", err)
	}
	if _, err := m.ReadOffsetPage(context.Background(), ds, 1, -1, 2); err == nil {
		t.Error("negative offset accepted")
	}
}

func TestInsertValidates(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := NewMemory()
	good := []any{int64(1), int64(1), t0, "paid", int64(5), nil}
	if err := m.Insert(ds, good); err != nil {
		t.Fatalf("row with NULL note: %v", err)
	}
	if err := m.Insert(ds, []any{int64(1), int64(1), t0, "paid", int64(5), "dup"}); err == nil {
		t.Error("duplicate key accepted")
	}
	for name, row := range map[string][]any{
		"short":       {int64(1)},
		"bad type":    {"1", int64(1), t0, "paid", int64(5), ""},
		"null key":    {int64(2), int64(1), nil, "paid", int64(5), ""},
		"bad account": {int64(3), nil, t0, "paid", int64(5), ""},
	} {
		if err := m.Insert(ds, row); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	noAcct := *ds
	noAcct.AccountColumn = "owner"
	if err := m.Insert(&noAcct, good); err == nil {
		t.Error("dataset without its account column accepted")
	}
}

func TestKeysetIsStableUnderInsertsWhereOffsetIsNot(t *testing.T) {
	m, _ := seeded(t, 1, 10, 1)
	ctx := context.Background()
	p1, err := m.ReadPage(ctx, ds, 1, nil, 5)
	if err != nil {
		t.Fatal(err)
	}
	off1, err := m.ReadOffsetPage(ctx, ds, 1, 0, 5)
	if err != nil {
		t.Fatal(err)
	}
	// A row that sorts before everything read so far arrives between page 1 and page 2.
	early := []any{int64(1_000_000), int64(1), sample.Base.Add(-time.Hour), "paid", int64(1), ""}
	if err := m.Insert(ds, early); err != nil {
		t.Fatal(err)
	}
	p2, err := m.ReadPage(ctx, ds, 1, p1.Next, 5)
	if err != nil {
		t.Fatal(err)
	}
	off2, err := m.ReadOffsetPage(ctx, ds, 1, 5, 5)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]int{}
	for _, r := range append(p1.Rows, p2.Rows...) {
		seen[r[0].(int64)]++
	}
	if len(seen) != 10 {
		t.Fatalf("keyset: %d distinct rows over two pages, want 10 (no repeat, no gap)", len(seen))
	}
	offSeen := map[int64]int{}
	for _, r := range append(off1, off2...) {
		offSeen[r[0].(int64)]++
	}
	if len(offSeen) != 9 {
		t.Fatalf("OFFSET: %d distinct rows, want 9 (the insert shifts one row into the second page again)", len(offSeen))
	}
}

func TestExaminedCountsKeysetVersusOffset(t *testing.T) {
	m, _ := seeded(t, 1, 100, 1)
	ctx := context.Background()
	walk(t, m, 1, 10)
	if n := m.Examined(); n != 100 {
		t.Fatalf("keyset examined %d rows, want 100", n)
	}
	for off := int64(0); off < 100; off += 10 {
		if _, err := m.ReadOffsetPage(ctx, ds, 1, off, 10); err != nil {
			t.Fatal(err)
		}
	}
	// 10 + 20 + ... + 100
	if n := m.Examined(); n != 550 {
		t.Fatalf("OFFSET examined %d rows, want 550", n)
	}
	if m.Examined() != 0 {
		t.Fatal("Examined does not reset")
	}
	if m.Count(ds, 1) != 100 || m.Count(ds, 2) != 0 {
		t.Fatal("Count wrong")
	}
}
