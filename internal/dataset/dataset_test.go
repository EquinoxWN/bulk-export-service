package dataset

import (
	"errors"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func TestValidateRejectsBadDefinitions(t *testing.T) {
	ok := Orders
	cases := map[string]func(d *Dataset){
		"bad name":            func(d *Dataset) { d.Name = "Orders" },
		"bad table":           func(d *Dataset) { d.Table = `orders"; DROP TABLE x; --` },
		"bad account column":  func(d *Dataset) { d.AccountColumn = "" },
		"no columns":          func(d *Dataset) { d.Columns = nil },
		"bad column":          func(d *Dataset) { d.Columns = []Column{{Name: "a b", Type: Text}} },
		"duplicate column":    func(d *Dataset) { d.Columns = []Column{{"id", Int64}, {"id", Int64}}; d.Key = []string{"id"} },
		"unknown type":        func(d *Dataset) { d.Columns = []Column{{"id", Type(9)}}; d.Key = []string{"id"} },
		"empty key":           func(d *Dataset) { d.Key = nil },
		"key not a column":    func(d *Dataset) { d.Key = []string{"created_at", "missing"} },
		"key column repeated": func(d *Dataset) { d.Key = []string{"id", "id"} },
	}
	for name, mutate := range cases {
		d := ok
		d.Columns = append([]Column(nil), ok.Columns...)
		mutate(&d)
		if err := d.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := ok.Validate(); err != nil {
		t.Fatalf("Orders: %v", err)
	}
}

func TestKeyIndexesAndTypes(t *testing.T) {
	idx := Orders.KeyIndexes()
	if len(idx) != 2 || idx[0] != 2 || idx[1] != 0 {
		t.Fatalf("key indexes %v, want [2 0]", idx)
	}
	types := Orders.KeyTypes()
	if types[0] != Timestamp || types[1] != Int64 {
		t.Fatalf("key types %v", types)
	}
}

func TestFormatParseRoundTripRandomized(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 5000 {
		n := r.Int64() - r.Int64()
		s, err := Format(Int64, n)
		if err != nil {
			t.Fatal(err)
		}
		if v, err := Parse(Int64, s); err != nil || v.(int64) != n {
			t.Fatalf("int %d -> %q -> %v %v", n, s, v, err)
		}
		ts := time.Unix(r.Int64N(4e9), r.Int64N(1e6)*1000).UTC()
		s, err = Format(Timestamp, ts.In(time.FixedZone("x", 3600*5)))
		if err != nil {
			t.Fatal(err)
		}
		v, err := Parse(Timestamp, s)
		if err != nil || !v.(time.Time).Equal(ts) {
			t.Fatalf("time %v -> %q -> %v %v", ts, s, v, err)
		}
		if !strings.HasSuffix(s, "Z") || len(s) != len(TimeLayout) {
			t.Fatalf("time text %q not fixed-width UTC", s)
		}
	}
}

func TestFormatAndParseErrors(t *testing.T) {
	if s, err := Format(Text, nil); err != nil || s != "" {
		t.Fatalf("NULL: %q %v", s, err)
	}
	for _, c := range []struct {
		t Type
		v any
	}{{Int64, "1"}, {Text, 1}, {Timestamp, "2026"}, {Type(9), 1}} {
		if _, err := Format(c.t, c.v); !errors.Is(err, ErrBadValue) {
			t.Errorf("Format(%v, %#v): %v", c.t, c.v, err)
		}
	}
	for _, c := range []struct {
		t Type
		s string
	}{{Int64, "x"}, {Int64, "99999999999999999999"}, {Timestamp, "2026-01-01"}, {Timestamp, "2026-01-01T00:00:00Z"}, {Type(9), "1"}} {
		if _, err := Parse(c.t, c.s); !errors.Is(err, ErrBadValue) {
			t.Errorf("Parse(%v, %q): %v", c.t, c.s, err)
		}
	}
	if v, err := Format(Int64, int32(-4)); err != nil || v != "-4" {
		t.Fatalf("int32: %q %v", v, err)
	}
}

func TestCompare(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 1000, time.UTC)
	cases := []struct {
		t    Type
		a, b any
		want int
	}{
		{Int64, int64(1), int64(2), -1}, {Int64, int64(2), int64(2), 0}, {Int64, int64(3), int64(2), 1},
		{Text, "a", "b", -1}, {Text, "b", "b", 0}, {Text, "c", "b", 1},
		{Timestamp, t0, t0.Add(time.Microsecond), -1}, {Timestamp, t0, t0, 0}, {Timestamp, t0.Add(time.Microsecond), t0, 1},
	}
	for _, c := range cases {
		if got := Compare(c.t, c.a, c.b); got != c.want {
			t.Errorf("Compare(%v, %v, %v) = %d, want %d", c.t, c.a, c.b, got, c.want)
		}
	}
}

func TestRegistry(t *testing.T) {
	if _, err := NewRegistry(Orders, Orders); err == nil {
		t.Fatal("duplicate dataset accepted")
	}
	bad := Orders
	bad.Table = "1orders"
	if _, err := NewRegistry(bad); err == nil {
		t.Fatal("invalid dataset accepted")
	}
	r := Default()
	if r["orders"] == nil || len(r) != 1 {
		t.Fatalf("default registry %v", r)
	}
}
