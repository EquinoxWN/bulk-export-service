# RFC 0001: bulk-export-service design

- **Status:** Accepted for M1 (M2 and M3 parts still draft)
- **Author:** EquinoxWN
- **Created:** 2026-11-17

## Problem

"Export to CSV" is usually a synchronous request handler that runs one big query, or a loop of
`LIMIT n OFFSET k` queries, against the primary database. For a large account this fails in three
ways:

1. **The request times out.** A load balancer or client gives up long before millions of rows are
   formatted, and the user retries, which starts the whole export again.
2. **It loads the primary.** The export competes with the writes the product depends on.
3. **OFFSET gets slower page by page.** To return page *p* the database reads and throws away the
   `p * n` rows before it, so a full export reads O(N²/n) rows. Rows inserted or deleted during the
   export also shift the offsets, so rows are repeated or skipped.

**Users:** backend engineers who own a "download all my data" feature (CSV for finance, GDPR data
access requests, account migration), and people preparing the pagination-at-scale interview question.

### Critical-to-quality measures (Define)

| CTQ | Target | M1 status |
|---|---|---|
| Create call does not wait for the export | 202 with a job id before any row is read | met: the demo creates every job before a worker starts (`create:` line) |
| Every row exported exactly once, in key order | 0 missing, 0 repeated rows on a static table | met in-process (400,000 rows, `verify:` line); PostgreSQL: CI tests, not run yet |
| Rows read per page independent of page position | about `n` rows per page (keyset), not `p * n` (OFFSET) | met in the in-memory model; PostgreSQL index-only range plan asserted in a CI test, not run yet |
| Export reads never reach the primary | 0 statements on the primary that read exported rows | by construction: the worker holds only the replica reader; a CI test checks the reader cannot write |
| Retry of a create stores one job | 1 job per (account, key) | met: memory store and API tests; PostgreSQL store: CI test |
| Worker memory flat in the number of rows | one page plus a 64 KiB buffer | by design; not measured yet (M3 benchmark) |

## Goals

- An export request creates a job and returns immediately; clients poll a status endpoint. (M1)
- Workers read with keyset pagination (not OFFSET) from a read replica, protecting the primary. (M1)
- A consistent snapshot ensures one export never mixes data from before and after concurrent changes. (M2)
- Rows stream straight into compressed CSV or Parquet in object storage with constant memory. (M2)
- Jobs checkpoint progress, so a worker crash resumes instead of restarting. (M3)
- A 50M-row benchmark compares OFFSET pagination with keyset pagination. (M3)

## Non-goals

- Running as a hosted production service: no authentication or tenancy checks in M1.
- Arbitrary user-defined queries or filters: datasets are a fixed, reviewed registry.
- Exactly-once delivery of files to third parties (email, webhooks): M3 "notify" at most.

## Design (M1)

```
client --POST /v1/exports (Idempotency-Key)--> API --insert--> jobs table (primary)
client --GET /v1/exports/{id}--------------->  API --select--> jobs table (primary)
worker --claim / progress / finish---------->  jobs table (primary)
worker --keyset page queries (READ ONLY)---->  replica (orders)
worker --CSV .part -> fsync -> rename------->  local directory  --GET .../file--> client
```

- **Request.** `POST /v1/exports` with `{account_id, dataset, format}` and an `Idempotency-Key`
  header. The body is strict JSON (unknown fields and trailing data refused, 4 KiB limit) and is
  validated against the dataset registry. The job is stored as `queued` and the response is 202 with
  `Location: /v1/exports/{id}`. A retry with the same key and body returns the same job with 200 and
  `Idempotent-Replayed: true`; the same key with another body is 422.
- **Job store** (`job.Store`): in memory (one mutex) or PostgreSQL on the primary (`export_jobs`,
  `UNIQUE (account_id, idem_key)`, `INSERT ... ON CONFLICT DO NOTHING`). Workers claim the oldest
  queued job (`FOR UPDATE SKIP LOCKED` ordered by an identity column, not a timestamp); progress,
  finish and fail updates are fenced with `worker = $me AND status = 'running'` and may not move
  progress backwards.
- **Keyset read.** Each dataset declares its key, for `orders` `(created_at, id)`; the pair is unique
  and backed by the index `(account_id, created_at, id)`. A page is
  `WHERE account_id = $1 AND (created_at, id) > ($2, $3) ORDER BY created_at, id LIMIT $4`, which
  PostgreSQL answers with an index range scan starting at the cursor. The cursor is the last row's
  key in a fixed text form (timestamps as UTC with microseconds) and is stored with the job after
  every page.
- **Replica routing.** The service takes two DSNs. Jobs use the primary pool; rows are read only
  through `source.Postgres`, a separate pool whose sessions default to
  `default_transaction_read_only = on`, and every page runs in its own `READ ONLY` transaction with
  `statement_timeout`. The service refuses to start with the same DSN for both unless
  `-allow-shared-db`, and `-require-standby` refuses a replica that is not `pg_is_in_recovery()`.
  `-page-pause` caps the load a worker puts on the replica.
- **Failure handling.** Each page read has a timeout and at most `-retries` extra attempts with
  doubling, jittered, capped backoff; cursor and type errors are not retried. A failed export records
  the reason (512 bytes max) and deletes its `.part` file.
- **Output (M1).** CSV with a header, written through a 64 KiB buffer to `<out>/<id>.csv.part`,
  fsynced and renamed, with its SHA-256 and byte count stored on the job. Text cells that start with
  `= + - @`, tab or CR get a leading `'` (formula guard). `GET /v1/exports/{id}/file` serves it.

## Alternatives considered

| Option | Why not (yet) |
|---|---|
| Synchronous streaming response (`COPY ... TO STDOUT` piped to the HTTP response) | Simplest and fast, but the export lives only as long as one HTTP connection: a proxy timeout or client drop loses all work, and nothing can be retried or resumed. Kept as the inner loop idea for M2 (stream to object storage instead of the socket). |
| OFFSET pagination | Reads `p * n` rows for page `p` (the demo's model: 11.5x more rows than keyset for 20-page exports) and repeats or skips rows when rows are inserted or deleted mid-export (`TestKeysetIsStableUnderInsertsWhereOffsetIsNot`). See ADR 0002. |
| One long transaction with a server-side cursor (`DECLARE ... CURSOR`) | Gives a consistent snapshot for free, but holds one transaction open for the whole export, which on a replica conflicts with recovery (`max_standby_streaming_delay` cancels it, or `hot_standby_feedback` bloats the primary). Short per-page transactions plus an exported snapshot is the M2 plan. |
| Read from the primary with a low-priority role | No separation at all under load; a slow export still holds buffers and I/O on the machine that serves writes. See ADR 0003. |

## Measurement plan

- **M1 (done, in-process):** `make bench` exports 20 accounts x 20,000 orders with 4 workers, injects
  a failure on every 7th page read, downloads every file over HTTP and compares it row by row with a
  fresh ordered read, and counts rows examined by keyset pages against OFFSET pages over the same
  in-memory table. Numbers are in `docs/results/m1.md`.
- **M1 (CI, not run yet):** PostgreSQL 17 service container: keyset walk equals `ORDER BY` for page
  sizes 1 to 10,000; keyset pages equal OFFSET pages on a static table; `EXPLAIN` of a keyset page
  uses `orders_keyset` with no `Sort` and no `Seq Scan`; the reader role cannot write; two workers
  export four accounts end to end.
- **M3:** 50M rows on PostgreSQL, OFFSET vs keyset: export time, rows read (`pg_stat_statements`
  shared blocks), and worker RSS sampled over the run.

## Milestones

- **M1:** An export request creates a job and returns immediately. Workers read with keyset pagination (not OFFSET) from a read replica, protecting the primary.
- **M2:** A consistent snapshot ensures one export never mixes data from before and after concurrent changes. Rows stream straight into compressed CSV or Parquet in object storage with constant memory.
- **M3:** Jobs checkpoint progress, so a worker crash resumes instead of restarting. A 50M-row benchmark compares OFFSET pagination with keyset pagination.

## Risks and open questions

- **No snapshot in M1.** Each page is its own transaction, so a row inserted behind the cursor during
  an export is not included, and an updated key can move a row past or behind the cursor. M2 uses
  `pg_export_snapshot()` on the replica (or a key upper bound captured at start) to fix this.
- **Replica lag.** A replica behind the primary exports older data; the job does not yet record the
  replica's replay position.
- **Crashed workers.** A job whose worker dies stays `running`; M3 adds leases and resume from the
  stored cursor.
- **CI stand-in for a replica.** CI uses a read-only role on the same PostgreSQL server, which proves
  the routing and the read-only guarantees but not behaviour on a real hot standby (query conflicts).
