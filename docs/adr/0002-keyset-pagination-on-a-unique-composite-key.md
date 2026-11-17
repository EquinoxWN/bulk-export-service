# ADR 0002: Keyset pagination on a unique composite key

- **Status:** Accepted

## Context

A worker reads an account's rows in pages. With `LIMIT n OFFSET k` the database must produce and
discard `k` rows before each page, so a full export of `N` rows reads about `N² / 2n` rows, and a row
inserted or deleted before the current offset shifts every later page (repeats or gaps).

Keyset ("seek") pagination remembers the last row's key and asks for rows after it. It needs a sort
key that is **unique** (otherwise rows that share the last value are skipped or repeated at a page
boundary) and an index that matches the sort.

Exports are most useful in time order, but `created_at` alone is not unique: batch inserts share a
timestamp.

## Decision

- Every dataset declares an ordered key; together with the account column it must be unique. For
  `orders` it is `(created_at, id)`, served by the index `(account_id, created_at, id)`.
- The page predicate is a row-value comparison, `(created_at, id) > ($2, $3)`, which PostgreSQL turns
  into a single index range condition (not `created_at > $2 OR (created_at = $2 AND id > $3)`, which
  planners handle less well).
- The cursor is the last row's key in canonical text (timestamps in UTC with exactly six fractional
  digits, matching PostgreSQL's microsecond precision), so it round-trips without loss and can be
  stored as JSON on the job.
- Only ascending order is supported in M1.

## Consequences

- Each page costs about `n` index entries wherever it is in the export. The in-process demo shows the
  difference in rows examined (11.5x fewer for 20-page exports); the database measurement is M3.
- The sample data gives every three orders the same `created_at` and runs their ids backwards, so the
  tiebreaker is exercised by every test that walks pages; a mutation that ignores the id is killed.
- A dataset without a unique key cannot be exported; adding one means adding a key column and index.
- Keyset alone is not a snapshot: rows inserted behind the cursor during an export are missed and an
  updated key can move a row. M2 adds a snapshot.
