# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub:
**Security > Report a vulnerability** on this repository
(`https://github.com/EquinoxWN/bulk-export-service/security/advisories/new`). Do not open a public issue.

Include what you found, how to reproduce it, and the impact you expect. You will get an answer
within 7 days. Fixes are released as soon as they are ready, and you are credited unless you ask
not to be.

## Supported versions

This project is pre-1.0. Only the latest commit on `main` receives fixes.

## Scope

- **In scope:** SQL injection through dataset or request fields, a download that serves a file other
  than the job's own, writes reaching the database through the replica connection, a job run twice by
  two workers, crashes on malformed JSON, unbounded memory from untrusted input, and data races.
- **Out of scope:** The missing authentication (M1 has none: any caller can export any account, so
  run it only on a trusted network), performance problems, and features listed on the roadmap.

## How this repository protects itself

- Table and column names come only from the built-in dataset registry, checked against
  `^[a-z_][a-z0-9_]{0,62}$` and quoted; every value is a bind parameter.
- Export rows are read through a separate pool whose sessions default to read-only, inside
  `READ ONLY` transactions with a `statement_timeout`.
- Text cells that start with `=`, `+`, `-`, `@`, tab or CR get a leading `'` so spreadsheets do not
  evaluate them (`-formula-guard`, on by default).
- Every GitHub Action is pinned to a full commit SHA, the PostgreSQL service image to a digest, and
  workflows run with read-only permissions and without persisted credentials.
- Dependabot update pull requests are paused (`open-pull-requests-limit: 0`) so the repository keeps a
  single `main` branch; dependencies are updated by hand and checked by the CI audit.
- CI runs lint, tests and a known-vulnerability audit (`make audit`) on every push and pull request.
