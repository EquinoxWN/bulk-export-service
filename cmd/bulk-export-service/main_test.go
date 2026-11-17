package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestDemoRunsEndToEndAndVerifies(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"demo", "-accounts", "4", "-rows", "1234", "-page-size", "100", "-workers", "3", "-fail-every", "5"}, &out, env(nil))
	if err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	s := out.String()
	for _, want := range []string{
		"create: 4 jobs queued (202) and 4 retries replayed the same id (200)",
		"exports: 4 succeeded, 0 failed, 4936 rows",
		"verify: files=4 rows_checked=4936 row_mismatches=0 count_mismatches=0 sha_mismatches=0 failed_jobs=0",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("output lacks %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, " 0 page reads retried") {
		t.Errorf("no page read was retried:\n%s", s)
	}
}

func TestBadArguments(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer
	for _, args := range [][]string{
		nil,
		{"export"},
		{"demo", "-accounts", "0"},
		{"demo", "-page-size", "0"},
		{"demo", "-nope"},
		{"serve", "-page-size", "20000"},
		{"serve", "-workers", "0", "-addr", "127.0.0.1:0"},
	} {
		if err := run(ctx, args, &out, env(nil)); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
	if err := run(ctx, []string{"help"}, &out, env(nil)); err != nil || !strings.Contains(out.String(), "usage") {
		t.Errorf("help: %v", err)
	}
}

func TestServeRefusesHalfOrSharedDatabaseConfig(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer
	if err := run(ctx, []string{"serve"}, &out, env(map[string]string{"BULK_EXPORT_PRIMARY_DSN": "postgres://a@h/db"})); err == nil ||
		!strings.Contains(err.Error(), "both") {
		t.Errorf("primary only: %v", err)
	}
	if err := run(ctx, []string{"serve"}, &out, env(map[string]string{"BULK_EXPORT_REPLICA_DSN": "postgres://a@h/db"})); err == nil {
		t.Error("replica only accepted")
	}
	same := map[string]string{"BULK_EXPORT_PRIMARY_DSN": "postgres://a@h/db", "BULK_EXPORT_REPLICA_DSN": "postgres://a@h/db"}
	if err := run(ctx, []string{"serve"}, &out, env(same)); err == nil || !strings.Contains(err.Error(), "equals the primary") {
		t.Errorf("same DSN: %v", err)
	}
}

func TestServeInMemoryStartsAndStops(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	var out bytes.Buffer
	args := []string{"serve", "-addr", "127.0.0.1:0", "-out", t.TempDir(), "-seed-accounts", "1", "-seed-rows", "10"}
	if err := serve(ctx, args, &out, env(nil)); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "listening") || !strings.Contains(out.String(), "in-memory mode") {
		t.Fatalf("log:\n%s", out.String())
	}
}

func TestSanitizeHost(t *testing.T) {
	if got := sanitizeHost("my host/ä.example"); got != "myhost.example" {
		t.Fatalf("got %q", got)
	}
	if len(sanitizeHost(strings.Repeat("h", 100))) != 30 {
		t.Fatal("not capped")
	}
}
