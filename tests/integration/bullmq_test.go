package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shellghostt/queuelens/internal/adapter"
	"github.com/shellghostt/queuelens/internal/adapter/bullmq"
)

func redisURL(t *testing.T) string {
	t.Helper()
	value := os.Getenv("QUEUELENS_TEST_REDIS_URL")
	if value == "" {
		t.Skip("QUEUELENS_TEST_REDIS_URL is not set; skipping the Redis integration test")
	}
	return value
}

// bullMQCounts is what BullMQ itself reports for a queue.
type bullMQCounts struct {
	Waiting     int64 `json:"waiting"`
	Active      int64 `json:"active"`
	Completed   int64 `json:"completed"`
	Failed      int64 `json:"failed"`
	Delayed     int64 `json:"delayed"`
	Paused      int64 `json:"paused"`
	Prioritized int64 `json:"prioritized"`
}

// seedBullMQ runs the committed BullMQ fixture and returns the counts BullMQ
// reports.
//
// The fixture needs Node and its dependencies. When they are missing the test
// skips with a message that says exactly what to install, because a skipped
// test that looks green is worse than one that explains itself.
func seedBullMQ(t *testing.T, redisAddress string) map[string]bullMQCounts {
	t.Helper()

	fixture := filepath.Join("bullmq")
	if _, err := os.Stat(filepath.Join(fixture, "node_modules")); err != nil {
		t.Skipf("the BullMQ fixture is not installed; run `npm install` in tests/integration/bullmq")
	}
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("node is not installed; the BullMQ fixture needs it")
	}

	host, port := splitRedisAddress(t, redisAddress)
	database := strconv.Itoa(redisOptions(t, redisAddress).DB)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	command := exec.CommandContext(ctx, "node", "seed.js")
	command.Dir = fixture
	command.Env = append(os.Environ(),
		"REDIS_HOST="+host,
		"REDIS_PORT="+port,
		"REDIS_DB="+database,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("the BullMQ fixture failed: %v\n%s", err, output)
	}

	counts := map[string]bullMQCounts{}
	if err := json.Unmarshal(trimJSONOutput(output), &counts); err != nil {
		t.Fatalf("decode the fixture output %q: %v", output, err)
	}
	return counts
}

func trimJSONOutput(output []byte) []byte {
	text := strings.TrimSpace(string(output))
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start < 0 || end < start {
		return []byte(text)
	}
	return []byte(text[start : end+1])
}

func splitRedisAddress(t *testing.T, address string) (string, string) {
	t.Helper()

	options, err := redis.ParseURL(address)
	if err != nil {
		t.Fatalf("parse the redis url: %v", err)
	}
	host, port, err := splitHostPort(options.Addr)
	if err != nil {
		t.Fatalf("split the redis address: %v", err)
	}
	return host, port
}

func redisOptions(t *testing.T, address string) *redis.Options {
	t.Helper()

	options, err := redis.ParseURL(address)
	if err != nil {
		t.Fatalf("parse the redis url: %v", err)
	}
	return options
}

func splitHostPort(address string) (string, string, error) {
	index := strings.LastIndex(address, ":")
	if index < 0 {
		return "", "", errors.New("expected host:port")
	}
	return address[:index], address[index+1:], nil
}

func TestTheBullMQAdapterMatchesWhatBullMQReports(t *testing.T) {
	address := redisURL(t)
	expected := seedBullMQ(t, address)

	client := redis.NewClient(redisOptions(t, address))
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bullAdapter := bullmq.New(client, bullmq.Options{})
	counts, err := bullAdapter.Poll(ctx)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	byName := map[string]adapter.Counts{}
	for _, entry := range counts {
		byName[entry.Queue] = entry
	}

	if len(byName) != len(expected) {
		t.Fatalf("expected %d queues, found %d: %v", len(expected), len(byName), byName)
	}

	for name, want := range expected {
		got, ok := byName[name]
		if !ok {
			t.Fatalf("queue %q was not discovered", name)
		}
		if got.Waiting != want.Waiting {
			t.Errorf("queue %q: expected %d waiting, got %d", name, want.Waiting, got.Waiting)
		}
		if got.Active != want.Active {
			t.Errorf("queue %q: expected %d active, got %d", name, want.Active, got.Active)
		}
		if got.Completed != want.Completed {
			t.Errorf("queue %q: expected %d completed, got %d", name, want.Completed, got.Completed)
		}
		if got.Failed != want.Failed {
			t.Errorf("queue %q: expected %d failed, got %d", name, want.Failed, got.Failed)
		}
		if got.Delayed != want.Delayed {
			t.Errorf("queue %q: expected %d delayed, got %d", name, want.Delayed, got.Delayed)
		}
	}
}

func TestTheAdapterFailsCleanlyWhenRedisIsUnreachable(t *testing.T) {
	// Port 1 is not going to have a Redis server on it.
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 500 * time.Millisecond,
		MaxRetries:  -1,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bullAdapter := bullmq.New(client, bullmq.Options{})
	if _, err := bullAdapter.Poll(ctx); err == nil {
		t.Fatal("expected the poll to fail")
	}
}
