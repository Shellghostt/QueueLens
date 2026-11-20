package integration

import (
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func TestDashboardMetricWorkLimit(t *testing.T) {
	if os.Getenv("QUEUELENS_RUN_M4_BENCH") != "1" {
		t.Skip("opt-in 500,000-row metric work bound: make m4-bench")
	}
	f := newLifecycleFixture(t, true)
	policy := lifecycle.DefaultPolicy()
	at := policy.EvaluationAt(f.now).Add(-time.Minute)
	attempt, _ := lifecycle.FromFact(lifecycle.Fact{Attempt: 1, State: protocol.StateCompleted, TS: at, ReceivedAt: at, IdentityKind: "provided"})
	run := lifecycle.ReplaceAttempt(lifecycle.Run{Queue: "limits", JobID: "job-0"}, nil, attempt)
	body, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, f, `WITH grouped AS (
		SELECT (n-1)/32767 AS bucket,max((n-1)%32767+1) AS attempt,count(*)::int AS attempts FROM generate_series(1,500001) n GROUP BY 1)
		INSERT INTO job_runs(queue,job_id,reported_attempt,observed_attempts,incomplete_attempts,state,first_received_at,last_received_at,complete_observed,summary)
		SELECT 'limits','job-'||bucket,attempt::smallint,attempts,attempts,'completed',$2,$2,false,
		jsonb_set(jsonb_set(jsonb_set(jsonb_set($1::jsonb,'{job_id}',to_jsonb('job-'||bucket)),
		'{current,attempt}',to_jsonb(attempt)),'{observed_attempts}',to_jsonb(attempts)),'{incomplete_attempts}',to_jsonb(attempts)) FROM grouped`, body, at)
	body, err = json.Marshal(attempt)
	if err != nil {
		t.Fatal(err)
	}
	mustSQL(t, f, `INSERT INTO job_attempts(queue,job_id,attempt,summary,selected_state,selected_at,selected_conflict)
		SELECT 'limits','job-'||((n-1)/32767),((n-1)%32767+1)::smallint,jsonb_set($1::jsonb,'{attempt}',to_jsonb((n-1)%32767+1)),
		'completed',$2,false FROM generate_series(1,500001) n`, body, at)
	mustSQL(t, f, `VACUUM (PARALLEL 0, ANALYZE) job_attempts`)
	server := dashboardServer(f, policy, nil)
	defer server.Close()
	values := url.Values{"queue": {"limits"}, "window": {"1h"}}
	metric := dashboardGET(t, server, "queue", values).Metrics
	if metric.Available || metric.Reason != "terminal_work_limit" || metric.Completed != nil || metric.CompletionsPerSecond != nil {
		t.Fatalf("500,001 eligible rows did not return explicit bounded unknown: %+v", metric)
	}
	mustSQL(t, f, `DELETE FROM job_attempts WHERE queue='limits' AND job_id='job-15' AND attempt=(SELECT max(attempt) FROM job_attempts WHERE job_id='job-15')`)
	metric = dashboardGET(t, server, "queue", values).Metrics
	if !metric.Available || metric.Completed == nil || *metric.Completed != 500000 || *metric.Failed != 0 || metric.FailureFraction == nil || *metric.FailureFraction != 0 {
		t.Fatalf("exact work bound should retain complete selected counts: %+v", metric)
	}
}
