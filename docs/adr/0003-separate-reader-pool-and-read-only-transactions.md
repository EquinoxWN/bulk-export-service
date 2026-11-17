# ADR 0003: A separate reader pool with read-only transactions, jobs on the primary

- **Status:** Accepted

## Context

Exports must not load the primary. Two kinds of database work happen: small job bookkeeping (create,
claim, progress per page, finish) that must be durable and consistent, and large row reads.

Options considered:

1. **Everything on the primary.** Simplest, but the reads compete with production writes.
2. **Everything on the replica.** The replica is read-only, so the job state cannot live there.
3. **Split by kind of work.** Bookkeeping on the primary, row reads on the replica, enforced in code
   and in the database.

## Decision

Option 3.

- The service takes `BULK_EXPORT_PRIMARY_DSN` and `BULK_EXPORT_REPLICA_DSN`. The worker gets the job
  store (primary) and a `source.Reader` (replica) and has no other handle, so it cannot read rows
  from the primary by mistake.
- The reader pool sets `default_transaction_read_only = on` for every session and runs each page in
  a `READ ONLY` transaction with `statement_timeout` (default 30 s) and a small pool (default 4), so a
  bug cannot write through it and a slow page cannot run forever.
- Startup refuses identical DSNs unless `-allow-shared-db` (development), and `-require-standby`
  refuses a replica that is not `pg_is_in_recovery()`.
- Per-page progress writes go to the primary. They are one small `UPDATE` per page (1,000 rows by
  default), which is the price of a status endpoint that shows real progress and of the M3 resume.

## Consequences

- A primary outage stops new jobs and progress updates even if the replica is healthy; a replica
  outage fails page reads after bounded retries and the job is marked failed.
- Locally there is no PostgreSQL (no Docker on this machine): the in-memory source and store stand in
  for both. In CI the "replica" is a role on the same `postgres:17.11` service container that can only
  `SELECT` and defaults to read-only. That proves the routing and the read-only guarantee, not hot
  standby behaviour (recovery conflicts, lag), which is left for a later milestone.
- Replica lag means an export can be older than the primary; the job does not yet record the
  replica's replay LSN.
