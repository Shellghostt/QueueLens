package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shellghostt/queuelens/internal/api"
	"github.com/shellghostt/queuelens/internal/collector"
	"github.com/shellghostt/queuelens/pkg/protocol"
	sdk "github.com/shellghostt/queuelens/sdk/go"
)

func TestBothSDKsPersistEventsAndHeartbeats(t *testing.T) {
	db := newStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.EnsurePartition(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	before, err := db.CountEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ingest := collector.New(db, collector.Options{Limits: protocol.DefaultLimits()}, nil)
	ingest.Start(ctx)
	server := httptest.NewServer(api.New(api.Options{Token: "m2-sdk-integration-token", Collector: ingest, Heartbeat: db.Heartbeat}))
	defer server.Close()
	worker := fmt.Sprintf("m2-go-%d", time.Now().UnixNano())
	c, err := sdk.New(sdk.Options{URL: server.URL, Token: "m2-sdk-integration-token", WorkerID: worker})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		c.Emit(protocol.Event{Queue: "m2-sdk", JobID: fmt.Sprintf("go-%d", i), State: protocol.StateStarted})
	}
	if err = c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Stats().Sent != 40 || c.Stats().Heartbeats < 1 {
		t.Fatal(c.Stats())
	}
	if _, err = os.Stat("../../sdk/typescript/dist/index.js"); err != nil {
		t.Fatal("build the TypeScript SDK before running the SDK integration test")
	}
	command := exec.CommandContext(ctx, "node", "sdk.mjs")
	command.Env = append(os.Environ(), "QUEUELENS_TEST_COLLECTOR_URL="+server.URL, "QUEUELENS_TEST_COLLECTOR_TOKEN=m2-sdk-integration-token", "QUEUELENS_TEST_WORKER_ID="+worker+"-ts")
	if out, err := command.CombinedOutput(); err != nil {
		t.Fatalf("TypeScript SDK: %v %s", err, out)
	}
	ingest.Stop()
	after, err := db.CountEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after-before != 80 {
		t.Fatalf("persisted %d events, want 80", after-before)
	}
	conn, err := pgx.Connect(ctx, databaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	var count int
	if err = conn.QueryRow(ctx, "SELECT count(*) FROM workers WHERE worker_id=ANY($1)", []string{worker, worker + "-ts"}).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("heartbeats: %d", count)
	}
}
