package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/internal/store"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

type lifecycleFixture struct {
	db   *store.Store
	conn *pgx.Conn
	dsn  string
	now  time.Time
}

const lifecycleToken = "m3-integration-admin-token"

func lifecycleContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newLifecycleFixture(t *testing.T, migrate bool) lifecycleFixture {
	t.Helper()
	ctx := lifecycleContext(t)
	base := databaseURL(t)
	root, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	var random [8]byte
	if _, err = rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := "ql_m3_" + hex.EncodeToString(random[:])
	if _, err = root.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	dsn := parsed.String()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	db, err := store.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = conn.Close(cleanup)
		// Only these tables in this freshly generated test schema are removed.
		if strings.HasPrefix(schema, "ql_m3_") && len(schema) == 22 {
			for _, name := range []string{"job_attempts", "pruned_jobs", "job_runs", "event_identities",
				"projection_state", "workers", "queue_events", "schema_migrations"} {
				if _, dropErr := root.Exec(cleanup, "DROP TABLE IF EXISTS "+pgx.Identifier{schema, name}.Sanitize()); dropErr != nil {
					t.Errorf("remove disposable table: %v", dropErr)
				}
			}
			if _, dropErr := root.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()); dropErr != nil {
				t.Errorf("remove disposable schema: %v", dropErr)
			}
		}
		_ = root.Close(cleanup)
	})
	fixture := lifecycleFixture{db: db, conn: conn, dsn: dsn, now: time.Now().UTC().Truncate(time.Microsecond)}
	if migrate {
		if err = db.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
		if err = db.EnsurePartitions(ctx, fixture.now); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}

func lifecycleEvent(at time.Time, id, job string, attempt int, state protocol.State) protocol.Event {
	return protocol.Event{V: 1, ID: id, TS: at, Queue: "m3", JobID: job, Attempt: attempt, State: state}
}

func mustWrite(t *testing.T, f lifecycleFixture, events ...protocol.Event) int64 {
	t.Helper()
	count, err := f.db.WriteBatch(lifecycleContext(t), events)
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func mustSQL(t *testing.T, f lifecycleFixture, statement string, args ...any) {
	t.Helper()
	if _, err := f.conn.Exec(lifecycleContext(t), statement, args...); err != nil {
		t.Fatal(err)
	}
}

func sqlCount(t *testing.T, f lifecycleFixture, statement string, args ...any) int {
	t.Helper()
	var count int
	if err := f.conn.QueryRow(lifecycleContext(t), statement, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
