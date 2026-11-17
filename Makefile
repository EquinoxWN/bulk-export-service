.PHONY: setup lint test test-pg bench demo serve audit ci

setup:
	go mod download

lint:
	@files="$$(gofmt -l cmd internal)"; test -z "$$files" || (echo "$$files"; echo "run gofmt -w cmd internal"; exit 1)
	go vet ./...

# Unit tests; PostgreSQL tests skip unless BULK_EXPORT_TEST_DATABASE_URL is set.
test:
	go test -race -count=1 ./...

# Every test against a running PostgreSQL (CI service container); fails on any skip.
test-pg:
	@test -n "$$BULK_EXPORT_TEST_DATABASE_URL" || (echo "set BULK_EXPORT_TEST_DATABASE_URL"; exit 1)
	go test -race -count=1 -p 1 -v ./... > test-pg.log 2>&1 || (tail -60 test-pg.log; exit 1)
	@! grep -q -- '--- SKIP' test-pg.log
	@grep -E '^(ok|FAIL)' test-pg.log

# In-process run behind the README numbers: 20 accounts x 20,000 orders, verified file by file.
# The 50M-row OFFSET vs keyset benchmark on PostgreSQL is milestone M3.
bench:
	go run ./cmd/bulk-export-service demo -rows 20000

demo:
	go run ./cmd/bulk-export-service demo

serve:
	go run ./cmd/bulk-export-service serve

# Known vulnerabilities in called code (Go 1.26.11 as in CI); any finding fails.
audit:
	bash scripts/vulncheck.sh

ci: setup lint test
