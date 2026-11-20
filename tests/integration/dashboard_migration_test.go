package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/migrations"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func TestDashboardMigrationBackfillsM3WithoutChangingSummaries(t *testing.T) {
	f := newLifecycleFixture(t, false)
	for _, name := range []string{"0001_queue_events.sql", "0002_workers.sql", "0003_lifecycle.sql"} {
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		mustSQL(t, f, string(body))
	}
	attempt, _ := lifecycle.FromFact(lifecycle.Fact{Queue: "upgrade", JobID: "job", Attempt: 1, State: protocol.StateFailed,
		TS: f.now.Add(-time.Minute), ReceivedAt: f.now, IdentityKind: "provided"})
	completed, _ := lifecycle.FromFact(lifecycle.Fact{Attempt: 1, State: protocol.StateCompleted, TS: f.now.Add(999 * time.Nanosecond), ReceivedAt: f.now})
	attempt = lifecycle.MergeAttempt(attempt, completed)
	body, err := json.Marshal(attempt)
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, f, `INSERT INTO job_attempts(queue,job_id,attempt,summary) VALUES('upgrade','job',1,$1)`, body)
	mustSQL(t, f, `CREATE TABLE schema_migrations(name text PRIMARY KEY,applied_at timestamptz NOT NULL DEFAULT now())`)
	for _, name := range []string{"0001_queue_events.sql", "0002_workers.sql", "0003_lifecycle.sql"} {
		mustSQL(t, f, `INSERT INTO schema_migrations(name) VALUES($1)`, name)
	}
	if err = f.db.Migrate(lifecycleContext(t)); err != nil {
		t.Fatal(err)
	}
	if err = f.db.Migrate(lifecycleContext(t)); err != nil {
		t.Fatal(err)
	}
	var state string
	var ts time.Time
	var conflict, same bool
	if err = f.conn.QueryRow(lifecycleContext(t), `SELECT selected_state,selected_at,selected_conflict,summary=$1::jsonb FROM job_attempts`, body).Scan(&state, &ts, &conflict, &same); err != nil {
		t.Fatal(err)
	}
	if state != "completed" || !ts.Equal(f.now) || !conflict || !same {
		t.Fatalf("cache backfill changed evidence: %s %s %t %t", state, ts, conflict, same)
	}
	if _, err = f.conn.Exec(lifecycleContext(t), `UPDATE job_attempts SET selected_state='dead'`); err == nil {
		t.Fatal("invalid cache state accepted")
	}
	// M3 writers that omit the new cache still work; metric reads explicitly report
	// unavailable until a projector merge repairs that bounded missing-cache row.
	mustSQL(t, f, `INSERT INTO job_attempts(queue,job_id,attempt,summary) VALUES('upgrade','legacy',1,$1)`, body)
}
