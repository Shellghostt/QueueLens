// Command queuelens is the QueueLens collector and query API.
//
// Subcommands:
//
//	queuelens serve     run the collector and API
//	queuelens loadgen   send synthetic events at a target rate
//	queuelens version   print the version
//
// See docs/PLAN.md for the specification this implements.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shellghostt/queuelens/internal/adapter"
	"github.com/shellghostt/queuelens/internal/adapter/bullmq"
	"github.com/shellghostt/queuelens/internal/api"
	"github.com/shellghostt/queuelens/internal/collector"
	"github.com/shellghostt/queuelens/internal/config"
	"github.com/shellghostt/queuelens/internal/loadgen"
	"github.com/shellghostt/queuelens/internal/metrics"
	"github.com/shellghostt/queuelens/internal/poller"
	"github.com/shellghostt/queuelens/internal/queues"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

// version is overridden at build time with:
//
//	go build -ldflags "-X main.version=$(git describe --tags --always)"
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return &config.Error{Problem: "no subcommand given"}
	}

	switch args[0] {
	case "serve":
		return runServe(args[1:])
	case "loadgen":
		return runLoadgen(args[1:])
	case "version", "--version", "-v":
		fmt.Printf("queuelens %s\n", version)
		return nil
	case "help", "--help", "-h":
		printUsage()
		return nil
	default:
		printUsage()
		return &config.Error{Problem: fmt.Sprintf("unknown subcommand %q", args[0])}
	}
}

func printUsage() {
	fmt.Println(`queuelens - observability for background job systems

Usage:
  queuelens serve      run the collector and the query API
  queuelens loadgen    send synthetic events to a collector
  queuelens version    print the version

Environment:
  QUEUELENS_HTTP_ADDR        listen address (default :8080)
  QUEUELENS_POSTGRES_DSN     PostgreSQL connection string (required)
  QUEUELENS_REDIS_URL        Redis URL (required for the bullmq adapter)
  QUEUELENS_ADMIN_TOKEN      bearer token, at least 16 characters (required)
  QUEUELENS_ADAPTERS         comma separated adapters (default bullmq)
  QUEUELENS_POLL_INTERVAL    adapter poll cadence (default 5s)
  QUEUELENS_BUFFER_SIZE      collector buffer size (default 10000)
  QUEUELENS_BATCH_SIZE       events per database round trip (default 500)
  QUEUELENS_FLUSH_INTERVAL   maximum time an event waits in the buffer (default 250ms)
  QUEUELENS_LOG_LEVEL        debug, info, warn or error (default info)`)
}

func runServe(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	skipMigrate := flags.Bool("skip-migrate", false, "do not apply database migrations at startup")
	if err := flags.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := newLogger(cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	if !*skipMigrate {
		if err := db.Migrate(ctx); err != nil {
			return err
		}
	}
	if err := db.EnsurePartitions(ctx, time.Now().UTC()); err != nil {
		return err
	}
	backgroundCtx, cancelBackground := context.WithCancel(ctx)
	var background sync.WaitGroup
	defer func() { cancelBackground(); background.Wait() }()
	background.Go(func() { db.RunProjection(backgroundCtx, cfg.Lifecycle, logger) })
	background.Go(func() { db.RunMaintenance(backgroundCtx, cfg.Lifecycle, logger) })

	metricSet := metrics.New()
	ingest := collector.New(db, collector.Options{
		BufferSize: cfg.BufferSize,
		BatchSize:  cfg.BatchSize,
		FlushEvery: cfg.FlushEvery,
		Limits:     protocol.DefaultLimits(),
		Logger:     logger,
	}, metricSet)
	ingest.Start(ctx)
	defer ingest.Stop()

	queueStore := queues.NewStore()

	adapters, err := buildAdapters(ctx, cfg)
	if err != nil {
		return err
	}
	if len(adapters) > 0 {
		poll := poller.New(adapters, queueStore, metricSet, poller.Options{
			Every:  cfg.PollEvery,
			Logger: logger,
		})
		go poll.Run(ctx)
	}

	handler := api.New(api.Options{
		Collector: ingest,
		Queues:    queueStore,
		Metrics:   metricSet,
		Ping:      db.Ping,
		Token:     cfg.AdminToken,
		Version:   version,
		Logger:    logger,
		Heartbeat: db.Heartbeat,
		Lifecycle: store.NewReader(db, cfg.Lifecycle, cfg.AdminToken),
	})

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("queuelens listening", "addr", cfg.HTTPAddr, "version", version)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		logger.Info("shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}

func buildAdapters(ctx context.Context, cfg config.Config) ([]adapter.Adapter, error) {
	var adapters []adapter.Adapter

	for _, name := range cfg.Adapters {
		switch name {
		case "bullmq":
			options, err := redis.ParseURL(cfg.RedisURL)
			if err != nil {
				return nil, fmt.Errorf("parse redis url: %w", err)
			}
			client := redis.NewClient(options)
			if err := client.Ping(ctx).Err(); err != nil {
				// A broken Redis must not stop the collector: the adapter is
				// registered anyway and its failures are counted and served
				// as stale state.
				slog.Warn("redis is not reachable yet; the adapter will keep retrying", "error", err)
			}
			adapters = append(adapters, bullmq.New(client, bullmq.Options{}))
		default:
			return nil, &config.Error{
				Problem: fmt.Sprintf("unknown adapter %q; supported adapters: bullmq", name),
			}
		}
	}
	return adapters, nil
}

func runLoadgen(args []string) error {
	flags := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	url := flags.String("url", "http://127.0.0.1:8080", "collector base URL")
	token := flags.String("token", "", "bearer token (defaults to $QUEUELENS_ADMIN_TOKEN)")
	rate := flags.Int("rate", 1000, "target events per second (0 means as fast as possible)")
	duration := flags.Duration("duration", 10*time.Second, "how long to run")
	batch := flags.Int("batch", 100, "events per request")
	workers := flags.Int("workers", 4, "concurrent senders")
	queue := flags.String("queue", "loadgen", "queue name used in the generated events")
	if err := flags.Parse(args); err != nil {
		return err
	}

	bearer := *token
	if bearer == "" {
		bearer = os.Getenv(config.EnvAdminToken)
	}
	if bearer == "" {
		return &config.Error{Problem: "a bearer token is required: pass --token or set " + config.EnvAdminToken}
	}

	result, err := loadgen.Run(context.Background(), loadgen.Options{
		URL:       strings.TrimRight(*url, "/"),
		Token:     bearer,
		Rate:      *rate,
		Duration:  *duration,
		BatchSize: *batch,
		Workers:   *workers,
		Queue:     *queue,
	})
	if err != nil {
		return err
	}

	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(encoded))
	return nil
}

func newLogger(level string) *slog.Logger {
	var parsed slog.Level
	switch strings.ToLower(level) {
	case "debug":
		parsed = slog.LevelDebug
	case "warn", "warning":
		parsed = slog.LevelWarn
	case "error":
		parsed = slog.LevelError
	default:
		parsed = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: parsed}))
}
