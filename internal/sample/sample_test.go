package sample

import (
	"testing"
	"time"
)

func TestOrdersShapeAndDeterminism(t *testing.T) {
	a, err := Orders(3, 10, 4)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Orders(3, 10, 4)
	if len(a) != 30 {
		t.Fatalf("%d rows", len(a))
	}
	ids := map[int64]bool{}
	sameTime := 0
	for i, r := range a {
		if len(r) != 6 {
			t.Fatalf("row %d has %d values", i, len(r))
		}
		id := r[0].(int64)
		if ids[id] {
			t.Fatalf("id %d twice", id)
		}
		ids[id] = true
		if r[2] != b[i][2] || r[5] != b[i][5] || r[0] != b[i][0] {
			t.Fatal("same seed, different rows")
		}
		if i > 0 && r[1] == a[i-1][1] && r[2].(time.Time).Equal(a[i-1][2].(time.Time)) {
			sameTime++
			if r[0].(int64) > a[i-1][0].(int64) {
				t.Fatal("ids within a timestamp group should run backwards (so the tiebreaker is tested)")
			}
		}
	}
	if sameTime == 0 {
		t.Fatal("no shared timestamps")
	}
}

func TestOrdersBounds(t *testing.T) {
	for _, c := range [][2]int{{-1, 1}, {1, -1}, {1000, 10000}} {
		if _, err := Orders(c[0], c[1], 1); err == nil {
			t.Errorf("%v accepted", c)
		}
	}
	if r, err := Orders(0, 0, 1); err != nil || len(r) != 0 {
		t.Fatalf("empty: %v %v", r, err)
	}
}
