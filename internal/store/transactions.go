package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func (s *Store) begin(ctx context.Context, deadline time.Duration, options ...pgx.TxOptions) (context.Context, context.CancelFunc, pgx.Tx, error) {
	ctx, cancel := context.WithTimeout(ctx, deadline)
	var opts pgx.TxOptions
	if len(options) > 0 {
		opts = options[0]
	}
	tx, err := s.pool.BeginTx(ctx, opts)
	if err == nil {
		statementDeadline := deadline - time.Second
		if statementDeadline <= 0 {
			statementDeadline = deadline / 2
		}
		_, err = tx.Exec(ctx, `SELECT set_config('statement_timeout', $1, true),
			set_config('lock_timeout', '2000ms', true), set_config('TimeZone', 'UTC', true)`,
			fmt.Sprintf("%dms", statementDeadline.Milliseconds()))
	}
	if err != nil {
		if tx != nil {
			rollback(tx)
		}
		cancel()
	}
	return ctx, cancel, tx, err
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// Locks are scoped to the database/schema, including isolated test schemas.
func advisory(ctx context.Context, tx pgx.Tx, lane string, shared bool) error {
	function := "pg_advisory_xact_lock"
	if shared {
		function += "_shared"
	}
	_, err := tx.Exec(ctx, "SELECT "+function+
		"(hashtextextended(current_database() || ':' || current_schema() || ':' || $1, 0))", lane)
	return err
}
