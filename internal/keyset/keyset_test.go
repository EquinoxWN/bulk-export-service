package keyset

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
)

var ds = &dataset.Orders

const cols = `SELECT "id", "account_id", "created_at", "status", "amount_cents", "note" FROM "orders" WHERE "account_id" = $1`

func TestPageSQLFirstPage(t *testing.T) {
	q, args, err := PageSQL(ds, 7, nil, 500)
	if err != nil {
		t.Fatal(err)
	}
	want := cols + ` ORDER BY "created_at", "id" LIMIT $2`
	if q != want {
		t.Fatalf("got\n%s\nwant\n%s", q, want)
	}
	if !reflect.DeepEqual(args, []any{int64(7), 500}) {
		t.Fatalf("args %#v", args)
	}
}

func TestPageSQLAfterCursorSeeksWithRowComparison(t *testing.T) {
	c := Cursor{"2026-01-01T00:00:05.000005Z", "42"}
	q, args, err := PageSQL(ds, 7, c, 500)
	if err != nil {
		t.Fatal(err)
	}
	want := cols + ` AND ("created_at", "id") > ($2, $3) ORDER BY "created_at", "id" LIMIT $4`
	if q != want {
		t.Fatalf("got\n%s\nwant\n%s", q, want)
	}
	at := time.Date(2026, 1, 1, 0, 0, 5, 5000, time.UTC)
	if len(args) != 4 || args[0] != int64(7) || !args[1].(time.Time).Equal(at) || args[2] != int64(42) || args[3] != 500 {
		t.Fatalf("args %#v", args)
	}
	if strings.Contains(strings.ToUpper(q), "OFFSET") {
		t.Fatal("keyset query uses OFFSET")
	}
}

func TestPageSQLRejectsBadInput(t *testing.T) {
	for _, n := range []int{0, -1, MaxPageSize + 1} {
		if _, _, err := PageSQL(ds, 1, nil, n); err == nil {
			t.Errorf("limit %d accepted", n)
		}
	}
	if _, _, err := PageSQL(ds, 1, nil, MaxPageSize); err != nil {
		t.Errorf("limit %d refused: %v", MaxPageSize, err)
	}
	for _, c := range []Cursor{{"x"}, {"2026-01-01T00:00:00.000000Z", "nan"}, {"yesterday", "1"}, {"a", "b", "c"}, {}} {
		if _, _, err := PageSQL(ds, 1, c, 10); !errors.Is(err, ErrBadCursor) {
			t.Errorf("cursor %v: %v", c, err)
		}
	}
}

func TestOffsetSQL(t *testing.T) {
	q, args, err := OffsetSQL(ds, 3, 2000, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if q != cols+` ORDER BY "created_at", "id" LIMIT $2 OFFSET $3` || !reflect.DeepEqual(args, []any{int64(3), 1000, int64(2000)}) {
		t.Fatalf("%s %#v", q, args)
	}
	if _, _, err := OffsetSQL(ds, 3, -1, 10); err == nil {
		t.Fatal("negative offset accepted")
	}
	if _, _, err := OffsetSQL(ds, 3, 0, 0); err == nil {
		t.Fatal("zero limit accepted")
	}
}

func row(id int64, at time.Time) []any {
	return []any{id, int64(1), at, "paid", int64(10), ""}
}

func TestFromRowAndCompare(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c, err := FromRow(ds, row(9, t0))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(c, Cursor{"2026-01-01T00:00:00.000000Z", "9"}) {
		t.Fatalf("cursor %v", c)
	}
	typed, err := c.Typed(ds)
	if err != nil {
		t.Fatal(err)
	}
	// Same timestamp: the id breaks the tie.
	if CompareToCursor(ds, row(10, t0), typed) <= 0 || CompareToCursor(ds, row(8, t0), typed) >= 0 || CompareToCursor(ds, row(9, t0), typed) != 0 {
		t.Fatal("id tiebreaker wrong")
	}
	// The timestamp is compared first, whatever the id.
	if CompareKey(ds, row(1, t0.Add(time.Microsecond)), row(99, t0)) <= 0 || CompareKey(ds, row(99, t0), row(1, t0.Add(time.Microsecond))) >= 0 {
		t.Fatal("timestamp must dominate the id")
	}
	if _, err := FromRow(ds, []any{int64(1)}); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("short row: %v", err)
	}
	if _, err := FromRow(ds, row(1, time.Time{})[:6:6]); err != nil {
		t.Fatalf("zero time is a valid key: %v", err)
	}
	nullKey := row(1, t0)
	nullKey[2] = nil
	if _, err := FromRow(ds, nullKey); !errors.Is(err, ErrBadCursor) {
		t.Fatalf("NULL key: %v", err)
	}
	badType := row(1, t0)
	badType[0] = "1"
	if _, err := FromRow(ds, badType); !errors.Is(err, dataset.ErrBadValue) {
		t.Fatalf("string id: %v", err)
	}
}
