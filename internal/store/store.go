// Package store writes protocol events to PostgreSQL.
//
// The collector is the only writer. It uses a pooled connection, writes whole
// batches, and never holds a transaction open across a batch boundary.
package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/shellghostt/queuelens/migrations"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

// Store is the PostgreSQL-backed event store.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to PostgreSQL and verifies the connection.
func New(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}
	if cfg.MaxConns < 2 {
		cfg.MaxConns = 4
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() {
	s.pool.Close()
}

// Ping checks that the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Migrate applies every embedded migration that has not run yet.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name       text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		var applied bool
		if err := s.pool.QueryRow(
			ctx,
			"SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)",
			name,
		).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if applied {
			continue
		}

		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}

		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(body)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := tx.Exec(
			ctx,
			"INSERT INTO schema_migrations (name) VALUES ($1)",
			name,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", name, err)
		}
	}
	return nil
}

// EnsurePartition creates the monthly partition that covers the given month
// if it does not exist yet.
//
// Partition maintenance (creating partitions two months ahead, dropping old
// ones) is M3 work. M1 creates the partition for the current month and the
// next one at startup, which is enough for the ingest path to work.
func (s *Store) EnsurePartition(ctx context.Context, month time.Time) error {
	start := time.Date(month.Year(), month.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	name := fmt.Sprintf("queue_events_%04d_%02d", start.Year(), int(start.Month()))

	// Partition bounds are part of the DDL, so PostgreSQL does not accept
	// bind parameters here. The values are computed from a UTC calendar
	// month, not from user input, so writing them as literals is safe.
	statement := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF queue_events FOR VALUES FROM ('%s') TO ('%s')",
		name,
		start.Format("2006-01-02 15:04:05Z07:00"),
		end.Format("2006-01-02 15:04:05Z07:00"),
	)
	if _, err := s.pool.Exec(ctx, statement); err != nil {
		return fmt.Errorf("create partition %s: %w", name, err)
	}
	return nil
}

// WriteBatch writes one batch of events and returns how many rows were
// written.
func (s *Store) WriteBatch(ctx context.Context, events []protocol.Event) (int64, error) {
	if len(events) == 0 {
		return 0, nil
	}

	rows := make([][]any, 0, len(events))
	for _, event := range events {
		var duration any
		if event.DurationMS != nil {
			duration = *event.DurationMS
		}
		var worker any
		if event.WorkerID != "" {
			worker = event.WorkerID
		}
		var failure any
		if event.Error != "" {
			failure = event.Error
		}
		rows = append(rows, []any{
			event.TS,
			event.Queue,
			event.JobID,
			event.JobName,
			int16(event.Attempt),
			string(event.State),
			worker,
			duration,
			failure,
		})
	}

	count, err := s.pool.CopyFrom(
		ctx,
		pgx.Identifier{"queue_events"},
		[]string{"ts", "queue", "job_id", "job_name", "attempt", "state", "worker_id", "duration_ms", "error"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return 0, fmt.Errorf("copy events: %w", err)
	}
	return count, nil
}

// CountEvents returns the number of stored events. It exists for the
// integration tests and for a future admin command.
func (s *Store) CountEvents(ctx context.Context) (int64, error) {
	var count int64
	if err := s.pool.QueryRow(ctx, "SELECT count(*) FROM queue_events").Scan(&count); err != nil {
		return 0, fmt.Errorf("count events: %w", err)
	}
	return count, nil
}
