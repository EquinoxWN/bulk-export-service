// Package sample generates deterministic synthetic orders for the demo, the memory mode and the tests.
package sample

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// Base is the first created_at of generated orders.
var Base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

var statuses = []string{"paid", "shipped", "refunded", "cancelled"}

// notes include CSV-hostile and spreadsheet-formula values on purpose.
var notes = []string{"", "plain", "comma, inside", `quote "inside"`, "line\nbreak", "=SUM(A1:A9)", "+1 call", "@cmd", "-2 discount", "ünïcödé ✓"}

// Orders returns rows for accounts 1..accounts, perAccount each, in the column order of dataset.Orders.
// Every three consecutive orders of an account share one created_at, so the id tiebreaker matters;
// ids are unique across accounts and not in created_at order.
func Orders(accounts, perAccount int, seed uint64) ([][]any, error) {
	if accounts < 0 || perAccount < 0 || accounts*perAccount > 5_000_000 {
		return nil, fmt.Errorf("sample size %d x %d out of range (at most 5,000,000 rows)", accounts, perAccount)
	}
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	out := make([][]any, 0, accounts*perAccount)
	for a := 1; a <= accounts; a++ {
		for i := range perAccount {
			// ids interleave accounts and run backwards within a timestamp group.
			id := int64(i/3*3+(2-i%3))*int64(accounts) + int64(a)
			at := Base.Add(time.Duration(i/3)*time.Second + time.Duration((i/3)%997)*time.Microsecond)
			out = append(out, []any{
				id, int64(a), at,
				statuses[r.IntN(len(statuses))],
				int64(r.IntN(100_000)),
				notes[r.IntN(len(notes))],
			})
		}
	}
	return out, nil
}
