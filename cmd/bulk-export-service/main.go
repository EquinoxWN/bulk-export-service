// Command bulk-export-service runs the export API and workers (serve) or an in-process verified run (demo).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/EquinoxWN/bulk-export-service/internal/api"
	"github.com/EquinoxWN/bulk-export-service/internal/dataset"
	"github.com/EquinoxWN/bulk-export-service/internal/job"
	"github.com/EquinoxWN/bulk-export-service/internal/pgstore"
	"github.com/EquinoxWN/bulk-export-service/internal/sample"
	"github.com/EquinoxWN/bulk-export-service/internal/source"
	"github.com/EquinoxWN/bulk-export-service/internal/worker"
)

const usage = `usage: bulk-export-service <command> [flags]

commands:
  serve   HTTP API and export workers (PostgreSQL when BULK_EXPORT_PRIMARY_DSN and
          BULK_EXPORT_REPLICA_DSN are set, otherwise in-memory with generated orders)
  demo    in-process run: create jobs over HTTP, poll, download and verify every file
`

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Getenv); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run dispatches a subcommand.
func run(ctx context.Context, args []string, out io.Writer, getenv func(string) string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	switch args[0] {
	case "serve":
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
		defer stop()
		return serve(ctx, args[1:], out, getenv)
	case "demo":
		return demo(ctx, args[1:], out)
	case "-h", "--help", "help":
		_, err := fmt.Fprint(out, usage)
		return err
	}
	return fmt.Errorf("unknown command %q\n%s", args[0], usage)
}

// workerFlags registers the worker limits shared by serve and demo.
func workerFlags(fs *flag.FlagSet, cfg *worker.Config) {
	fs.IntVar(&cfg.PageSize, "page-size", 1000, "rows per keyset page (1..10000)")
	fs.DurationVar(&cfg.PageTimeout, "page-timeout", 30*time.Second, "timeout for one page read")
	fs.IntVar(&cfg.Retries, "retries", 3, "extra attempts per failed page read (0..10)")
	fs.DurationVar(&cfg.Backoff, "backoff", 200*time.Millisecond, "first retry delay (doubles, with jitter)")
	fs.DurationVar(&cfg.MaxBackoff, "max-backoff", 5*time.Second, "cap on one retry delay")
	fs.DurationVar(&cfg.PagePause, "page-pause", 0, "pause between pages, to cap the load on the replica")
	fs.DurationVar(&cfg.Idle, "idle", 500*time.Millisecond, "wait when no job is queued")
	fs.BoolVar(&cfg.FormulaGuard, "formula-guard", true, "prefix text cells starting with = + - @ with a quote")
}

// startWorkers runs n workers until ctx ends; wait blocks until they have stopped.
func startWorkers(ctx context.Context, n int, cfg worker.Config, jobs job.Store, src source.Reader, reg dataset.Registry, log *slog.Logger) (wait func(), err error) {
	if n < 1 || n > 64 {
		return nil, fmt.Errorf("workers %d out of range 1..64", n)
	}
	var wg sync.WaitGroup
	host, _ := os.Hostname() // only used to make worker ids readable; empty is fine
	run, err := job.NewID()  // a random part keeps ids unique across processes on one host
	if err != nil {
		return nil, err
	}
	ws := make([]*worker.Worker, n)
	for i := range n {
		c := cfg
		c.ID = fmt.Sprintf("%s-w%d", run[:8], i+1)
		if h := sanitizeHost(host); h != "" {
			c.ID = h + "-" + c.ID
		}
		w, err := worker.New(c, jobs, src, reg, log)
		if err != nil {
			return nil, err
		}
		ws[i] = w
	}
	for _, w := range ws {
		wg.Add(1)
		go func() { defer wg.Done(); w.Run(ctx) }()
	}
	return wg.Wait, nil
}

// sanitizeHost keeps the characters a worker id may contain, at most 30 of them.
func sanitizeHost(h string) string {
	b := make([]byte, 0, len(h))
	for i := 0; i < len(h) && len(b) < 30; i++ {
		c := h[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '.' || c == '_' {
			b = append(b, c)
		}
	}
	return string(b)
}

// serve runs the HTTP API and the workers until ctx ends.
func serve(ctx context.Context, args []string, out io.Writer, getenv func(string) string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(out)
	addr := fs.String("addr", "127.0.0.1:8080", "listen address")
	outDir := fs.String("out", "data/exports", "directory for finished export files")
	workers := fs.Int("workers", 2, "export workers (1..64)")
	maxJobs := fs.Int("max-jobs", 10000, "in-memory mode: job store capacity")
	seedAccounts := fs.Int("seed-accounts", 5, "in-memory mode: generated accounts")
	seedRows := fs.Int("seed-rows", 20000, "in-memory mode: generated orders per account")
	allowShared := fs.Bool("allow-shared-db", false, "allow the replica DSN to equal the primary DSN (development only)")
	requireStandby := fs.Bool("require-standby", false, "refuse to start unless the replica reports pg_is_in_recovery() = true")
	replicaConns := fs.Int("replica-conns", 4, "replica pool size (1..64)")
	stmtTimeout := fs.Duration("statement-timeout", 30*time.Second, "statement_timeout for each page query")
	var cfg worker.Config
	workerFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg.OutDir = *outDir
	cfg.ID = "w"
	if err := cfg.Validate(); err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(out, nil))
	reg := dataset.Default()

	var jobs job.Store
	var src source.Reader
	primary, replica := getenv("BULK_EXPORT_PRIMARY_DSN"), getenv("BULK_EXPORT_REPLICA_DSN")
	switch {
	case primary == "" && replica == "":
		if *maxJobs < 1 {
			return errors.New("max-jobs must be positive")
		}
		mem := source.NewMemory()
		rows, err := sample.Orders(*seedAccounts, *seedRows, 1)
		if err != nil {
			return err
		}
		if err := mem.Insert(reg["orders"], rows...); err != nil {
			return err
		}
		jobs, src = job.NewMemory(*maxJobs), mem
		log.Info("in-memory mode", "accounts", *seedAccounts, "orders_per_account", *seedRows)
	case primary == "" || replica == "":
		return errors.New("set both BULK_EXPORT_PRIMARY_DSN and BULK_EXPORT_REPLICA_DSN, or neither")
	default:
		if primary == replica && !*allowShared {
			return errors.New("the replica DSN equals the primary DSN; exports would load the primary (use -allow-shared-db only in development)")
		}
		store, err := pgstore.Open(ctx, primary, 4)
		if err != nil {
			return err
		}
		defer store.Close()
		pg, err := source.OpenPostgres(ctx, replica, int32(*replicaConns), *stmtTimeout)
		if err != nil {
			return err
		}
		defer pg.Close()
		standby, err := pg.InRecovery(ctx)
		if err != nil {
			return err
		}
		if *requireStandby && !standby {
			return source.ErrNotStandby
		}
		jobs, src = store, pg
		log.Info("postgres mode", "replica_in_recovery", standby)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wait, err := startWorkers(wctx, *workers, cfg, jobs, src, reg, log)
	if err != nil {
		_ = ln.Close() // startup already failed; that error is returned
		return err
	}
	srv := &http.Server{Handler: api.New(jobs, reg, *outDir, log), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Minute, IdleTimeout: time.Minute}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("listening", "addr", ln.Addr().String(), "workers", *workers, "out", *outDir)
	select {
	case err = <-errc:
	case <-ctx.Done():
		sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		err = srv.Shutdown(sctx)
		scancel()
	}
	cancel()
	wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
