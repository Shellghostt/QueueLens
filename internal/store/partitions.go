package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
)

type partition struct {
	Schema, Name string
	Start, End   time.Time
}

var partitionName = regexp.MustCompile(`^queue_events_([0-9]{4})_([0-9]{2})$`)
var partitionBounds = regexp.MustCompile(`^FOR VALUES FROM [(]'([^']+)'[)] TO [(]'([^']+)'[)]$`)

func monthStart(at time.Time) time.Time {
	at = at.UTC()
	return time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func checkedPartition(schema, name, bound string, attached bool) (partition, error) {
	matches := partitionName.FindStringSubmatch(name)
	if !attached || matches == nil {
		return partition{}, fmt.Errorf("refuse unrecognized partition %q", name)
	}
	start, err := time.Parse("2006-01", matches[1]+"-"+matches[2])
	if err != nil {
		return partition{}, fmt.Errorf("refuse invalid partition month %q", name)
	}
	limits := partitionBounds.FindStringSubmatch(bound)
	if limits == nil {
		return partition{}, fmt.Errorf("refuse unrecognized bounds for %q", name)
	}
	from, fromErr := parsePartitionTime(limits[1])
	to, toErr := parsePartitionTime(limits[2])
	if fromErr != nil || toErr != nil || !from.Equal(start) || !to.Equal(start.AddDate(0, 1, 0)) {
		return partition{}, fmt.Errorf("refuse mismatched calendar bounds for %q", name)
	}
	return partition{Schema: schema, Name: name, Start: start, End: to}, nil
}

func parsePartitionTime(value string) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05Z07", "2006-01-02 15:04:05Z07:00", time.RFC3339Nano} {
		if at, err := time.Parse(layout, value); err == nil {
			return at, nil
		}
	}
	return time.Time{}, errors.New("unrecognized partition timestamp")
}

func inspectPartition(ctx context.Context, tx pgx.Tx, schema, name string) (partition, error) {
	var namespace, bound string
	var attached bool
	err := tx.QueryRow(ctx, `SELECT n.nspname, COALESCE(pg_get_expr(c.relpartbound,c.oid),''),
		EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid=c.oid
			AND i.inhparent=to_regclass($2))
		FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=to_regclass($1)`,
		pgx.Identifier{schema, name}.Sanitize(), pgx.Identifier{schema, "queue_events"}.Sanitize()).
		Scan(&namespace, &bound, &attached)
	if err != nil {
		return partition{}, err
	}
	if namespace != schema {
		return partition{}, fmt.Errorf("refuse partition outside schema %q", schema)
	}
	return checkedPartition(schema, name, bound, attached)
}

func ensurePartitionTx(ctx context.Context, tx pgx.Tx, month time.Time) error {
	start := monthStart(month)
	if start.Year() < 1000 || start.Year() > 9999 {
		return errors.New("partition year is outside the supported calendar")
	}
	name := fmt.Sprintf("queue_events_%04d_%02d", start.Year(), int(start.Month()))
	var schema string
	if err := tx.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		return err
	}
	if _, err := inspectPartition(ctx, tx, schema, name); !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	statement := fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
		pgx.Identifier{schema, name}.Sanitize(), pgx.Identifier{schema, "queue_events"}.Sanitize(),
		start.Format(time.RFC3339), start.AddDate(0, 1, 0).Format(time.RFC3339))
	if _, err := tx.Exec(ctx, statement); err != nil {
		return err
	}
	_, err := inspectPartition(ctx, tx, schema, name)
	return err
}

// EnsurePartition remains compatible with M1/M2 callers.
func (s *Store) EnsurePartition(ctx context.Context, month time.Time) error {
	ctx, cancel, tx, err := s.begin(ctx, 15*time.Second)
	if err != nil {
		return err
	}
	defer cancel()
	defer rollback(tx)
	if err = advisory(ctx, tx, "partitions", false); err == nil {
		err = ensurePartitionTx(ctx, tx, month)
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// EnsurePartitions creates at most four required calendar months after downtime.
func (s *Store) EnsurePartitions(ctx context.Context, now time.Time) error {
	ctx, cancel, tx, err := s.begin(ctx, 15*time.Second)
	if err != nil {
		return err
	}
	defer cancel()
	defer rollback(tx)
	if err = advisory(ctx, tx, "partitions", false); err != nil {
		return err
	}
	start := monthStart(now)
	months := []time.Time{start, start.AddDate(0, 1, 0), start.AddDate(0, 2, 0)}
	late := monthStart(now.Add(-7 * 24 * time.Hour))
	if !late.Equal(start) {
		months = append(months, late)
	}
	for _, month := range months {
		if err = ensurePartitionTx(ctx, tx, month); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// A persisted scan position is never an acknowledgement; wrap and revisit.
func nextPartition(ctx context.Context, tx pgx.Tx, after string) (partition, bool, error) {
	var schema, name, bound, ownSchema string
	err := tx.QueryRow(ctx, `SELECT n.nspname,c.relname,pg_get_expr(c.relpartbound,c.oid),current_schema()
		FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid
		JOIN pg_namespace n ON n.oid=c.relnamespace
		WHERE i.inhparent=to_regclass('queue_events')
		ORDER BY CASE WHEN c.relname COLLATE "C" > $1 COLLATE "C" THEN 0 ELSE 1 END,
			c.relname COLLATE "C" LIMIT 1`, after).Scan(&schema, &name, &bound, &ownSchema)
	if errors.Is(err, pgx.ErrNoRows) {
		return partition{}, true, nil
	}
	if err != nil {
		return partition{}, false, err
	}
	if schema != ownSchema {
		return partition{}, false, errors.New("raw partition is outside the parent namespace")
	}
	p, err := checkedPartition(schema, name, bound, true)
	return p, name <= after, err
}
