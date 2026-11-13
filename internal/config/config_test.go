package config

import (
	"strings"
	"testing"
	"time"
)

func baseEnv() []string {
	return []string{
		"QUEUELENS_POSTGRES_DSN=postgresql://queuelens:queuelens@localhost:5432/queuelens",
		"QUEUELENS_REDIS_URL=redis://localhost:6379/0",
		"QUEUELENS_ADMIN_TOKEN=0123456789abcdef",
	}
}

func TestLoadUsesTheDocumentedDefaults(t *testing.T) {
	cfg, err := LoadFrom(baseEnv())
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.HTTPAddr != DefaultHTTPAddr {
		t.Fatalf("expected the default address %q, got %q", DefaultHTTPAddr, cfg.HTTPAddr)
	}
	if cfg.PollEvery != 5*time.Second {
		t.Fatalf("expected a 5s poll interval, got %s", cfg.PollEvery)
	}
	if cfg.BufferSize != 10000 {
		t.Fatalf("expected a buffer of 10000, got %d", cfg.BufferSize)
	}
	if cfg.BatchSize != 500 {
		t.Fatalf("expected a batch size of 500, got %d", cfg.BatchSize)
	}
	if cfg.FlushEvery != 250*time.Millisecond {
		t.Fatalf("expected a 250ms flush interval, got %s", cfg.FlushEvery)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("expected log level info, got %q", cfg.LogLevel)
	}
	if len(cfg.Adapters) != 1 || cfg.Adapters[0] != "bullmq" {
		t.Fatalf("expected the bullmq adapter, got %v", cfg.Adapters)
	}
}

func TestLoadAcceptsThePlansSpellingAsAnAlias(t *testing.T) {
	env := []string{
		"QUELENS_POSTGRES_DSN=postgresql://queuelens:queuelens@localhost:5432/queuelens",
		"QUELENS_REDIS_URL=redis://localhost:6379/0",
		"QUELENS_ADMIN_TOKEN=0123456789abcdef",
		"QUELENS_HTTP_ADDR=:9999",
		"QUELENS_POLL_INTERVAL=7s",
	}

	cfg, err := LoadFrom(env)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTPAddr != ":9999" {
		t.Fatalf("expected :9999, got %q", cfg.HTTPAddr)
	}
	if cfg.PollEvery != 7*time.Second {
		t.Fatalf("expected 7s, got %s", cfg.PollEvery)
	}
}

func TestCanonicalNameWinsOverTheAlias(t *testing.T) {
	env := append(baseEnv(),
		"QUEUELENS_HTTP_ADDR=:1111",
		"QUELENS_HTTP_ADDR=:2222",
	)

	cfg, err := LoadFrom(env)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.HTTPAddr != ":1111" {
		t.Fatalf("expected the canonical variable to win, got %q", cfg.HTTPAddr)
	}
}

func TestLoadRequiresThePostgresDSN(t *testing.T) {
	env := []string{
		"QUEUELENS_REDIS_URL=redis://localhost:6379/0",
		"QUEUELENS_ADMIN_TOKEN=0123456789abcdef",
	}

	_, err := LoadFrom(env)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), EnvPostgresDSN) {
		t.Fatalf("expected the error to name %s, got %q", EnvPostgresDSN, err)
	}
}

func TestLoadRequiresTheAdminToken(t *testing.T) {
	env := []string{
		"QUEUELENS_POSTGRES_DSN=postgresql://queuelens:queuelens@localhost:5432/queuelens",
		"QUEUELENS_REDIS_URL=redis://localhost:6379/0",
	}

	_, err := LoadFrom(env)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), EnvAdminToken) {
		t.Fatalf("expected the error to name %s, got %q", EnvAdminToken, err)
	}
}

func TestLoadRejectsAShortAdminToken(t *testing.T) {
	env := []string{
		"QUEUELENS_POSTGRES_DSN=postgresql://queuelens:queuelens@localhost:5432/queuelens",
		"QUEUELENS_REDIS_URL=redis://localhost:6379/0",
		"QUEUELENS_ADMIN_TOKEN=short",
	}

	_, err := LoadFrom(env)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "at least") {
		t.Fatalf("expected the error to explain the minimum length, got %q", err)
	}
}

func TestLoadRequiresRedisForTheBullMQAdapter(t *testing.T) {
	env := []string{
		"QUEUELENS_POSTGRES_DSN=postgresql://queuelens:queuelens@localhost:5432/queuelens",
		"QUEUELENS_ADMIN_TOKEN=0123456789abcdef",
	}

	_, err := LoadFrom(env)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), EnvRedisURL) {
		t.Fatalf("expected the error to name %s, got %q", EnvRedisURL, err)
	}
}

func TestLoadDoesNotRequireRedisWithoutAdapters(t *testing.T) {
	env := []string{
		"QUEUELENS_POSTGRES_DSN=postgresql://queuelens:queuelens@localhost:5432/queuelens",
		"QUEUELENS_ADMIN_TOKEN=0123456789abcdef",
		"QUEUELENS_ADAPTERS=",
	}

	cfg, err := LoadFrom(env)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Adapters) != 0 {
		t.Fatalf("expected no adapters, got %v", cfg.Adapters)
	}
}

func TestLoadRejectsBadNumbersAndDurations(t *testing.T) {
	cases := []struct {
		name  string
		entry string
	}{
		{"buffer size is not a number", "QUEUELENS_BUFFER_SIZE=lots"},
		{"buffer size is zero", "QUEUELENS_BUFFER_SIZE=0"},
		{"buffer size is negative", "QUEUELENS_BUFFER_SIZE=-1"},
		{"batch size is not a number", "QUEUELENS_BATCH_SIZE=many"},
		{"poll interval is not a duration", "QUEUELENS_POLL_INTERVAL=soon"},
		{"flush interval is not a duration", "QUEUELENS_FLUSH_INTERVAL=often"},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			env := append(baseEnv(), test.entry)
			if _, err := LoadFrom(env); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestLoadRejectsAnOversizedBatch(t *testing.T) {
	env := append(baseEnv(), "QUEUELENS_BATCH_SIZE=1001")

	_, err := LoadFrom(env)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "1000") {
		t.Fatalf("expected the error to name the limit, got %q", err)
	}
}

func TestLoadSplitsTheAdapterList(t *testing.T) {
	env := append(baseEnv(), "QUEUELENS_ADAPTERS=bullmq, bullmq")

	cfg, err := LoadFrom(env)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Adapters) != 2 {
		t.Fatalf("expected two adapters, got %v", cfg.Adapters)
	}
}
