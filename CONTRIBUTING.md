# Contributing

Thanks for helping. This is a Go project; issues and pull requests are welcome.

## Set up and check your change

Needs Go 1.26 or newer (CI uses 1.26.11). The only third-party module is pgx (the PostgreSQL
driver). Caches stay in git-ignored folders inside the repo.

```bash
make setup   # go mod download
make lint    # gofmt and go vet
make test    # all tests with the race detector (needs cgo; without a C compiler: go test ./...)
make audit   # known-vulnerability check (as in CI)
make demo    # in-process export run, verified file by file
```

PostgreSQL tests skip unless `BULK_EXPORT_TEST_DATABASE_URL` points at a server that trusts the
test roles without a password (CI uses a throwaway `postgres:17.11` service container); `make
test-pg` runs them and fails on any skip.

A pull request is ready when `make lint` and `make test` pass and CI is green.

## Guidelines

- Keep each change focused; explain the problem it solves in the pull request.
- Add or update a test for every behaviour change, and update `docs/results/` when numbers change.
- Significant design changes need an ADR in `docs/adr/` (copy `0001` for the format).
- Keep function comments to one short phrase; let names and tests explain the rest.
- Export rows are read only through `source.Reader`; nothing in the worker may use the primary pool.
- Report security problems privately, as described in [SECURITY.md](SECURITY.md).

By contributing you agree that your work is released under the [MIT License](LICENSE).
