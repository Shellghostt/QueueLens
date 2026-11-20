// Package config resolves the runtime configuration from the environment.
//
// There is no configuration file: a deployment is described by environment
// variables, which is what a container platform and a CI job both give you.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/shellghostt/queuelens/internal/lifecycle"
)

// Environment variable names.
//
// The plan writes these as QUELENS_*. QUEUELENS_ is the canonical prefix and
// the QUELENS_ spelling is accepted as an alias, because the plan is the
// specification and a tool that ignores its own spec is a bug. See ADR 0002.
const (
	EnvHTTPAddr         = "QUEUELENS_HTTP_ADDR"
	EnvPostgresDSN      = "QUEUELENS_POSTGRES_DSN"
	EnvRedisURL         = "QUEUELENS_REDIS_URL"
	EnvAdminToken       = "QUEUELENS_ADMIN_TOKEN"
	EnvAdapters         = "QUEUELENS_ADAPTERS"
	EnvPollInt          = "QUEUELENS_POLL_INTERVAL"
	EnvBufferSize       = "QUEUELENS_BUFFER_SIZE"
	EnvBatchSize        = "QUEUELENS_BATCH_SIZE"
	EnvLogLevel         = "QUEUELENS_LOG_LEVEL"
	EnvFlushInt         = "QUEUELENS_FLUSH_INTERVAL"
	EnvRawRetention     = "QUEUELENS_RETENTION_RAW_DAYS"
	EnvHistoryRetention = "QUEUELENS_RETENTION_HISTORY_DAYS"
	EnvStallAfter       = "QUEUELENS_STALL_AFTER"
	EnvEvaluationInt    = "QUEUELENS_EVALUATION_INTERVAL"
	EnvMaintenanceInt   = "QUEUELENS_MAINTENANCE_INTERVAL"
	EnvProjectionInt    = "QUEUELENS_PROJECTION_INTERVAL"
	EnvProjectionBatch  = "QUEUELENS_PROJECTION_BATCH_SIZE"
)

// Aliases kept for compatibility with the plan's spelling.
const (
	aliasHTTPAddr    = "QUELENS_HTTP_ADDR"
	aliasPostgresDSN = "QUELENS_POSTGRES_DSN"
	aliasRedisURL    = "QUELENS_REDIS_URL"
	aliasAdminToken  = "QUELENS_ADMIN_TOKEN"
	aliasAdapters    = "QUELENS_ADAPTERS"
	aliasPollInt     = "QUELENS_POLL_INTERVAL"
	aliasBufferSize  = "QUELENS_BUFFER_SIZE"
	aliasBatchSize   = "QUELENS_BATCH_SIZE"
	aliasLogLevel    = "QUELENS_LOG_LEVEL"
	aliasFlushInt    = "QUELENS_FLUSH_INTERVAL"
)

// Defaults.
const (
	DefaultHTTPAddr    = ":8080"
	DefaultAdapters    = "bullmq"
	DefaultPollInt     = 5 * time.Second
	DefaultBufferSize  = 10000
	DefaultBatchSize   = 500
	DefaultFlushInt    = 250 * time.Millisecond
	DefaultLogLevel    = "info"
	MinAdminTokenLen   = 16
	MaxBatchEvents     = 1000
	MaxBatchBodyBytes  = 1 << 20 // 1 MiB
	MaxQueueNameLength = 256
)

// Config is the resolved configuration.
type Config struct {
	HTTPAddr    string
	PostgresDSN string
	RedisURL    string
	AdminToken  string
	Adapters    []string
	PollEvery   time.Duration
	BufferSize  int
	BatchSize   int
	FlushEvery  time.Duration
	LogLevel    string
	Lifecycle   lifecycle.Policy
	Dashboard   DashboardAccess
}

// Error is a configuration problem. It is a usage error, not a runtime one.
type Error struct{ Problem string }

func (e *Error) Error() string { return e.Problem }

func lookup(env map[string]string, canonical, alias string) (string, bool) {
	if value, ok := env[canonical]; ok && value != "" {
		return value, true
	}
	if alias == "" {
		return "", false
	}
	value, ok := env[alias]
	if !ok || value == "" {
		return "", false
	}
	return value, true
}

// lookupPresent behaves like lookup but reports an explicitly empty value as
// present. It is used for the adapter list, where "no adapters" is a
// deliberate configuration and not a missing one.
func lookupPresent(env map[string]string, canonical, alias string) (string, bool) {
	if value, ok := env[canonical]; ok {
		return value, true
	}
	if alias == "" {
		return "", false
	}
	value, ok := env[alias]
	if !ok {
		return "", false
	}
	return value, true
}

// Load reads the configuration from the process environment.
func Load() (Config, error) {
	return LoadFrom(os.Environ())
}

// LoadFrom reads the configuration from an explicit environment. It exists so
// that tests can describe a deployment without touching the process.
func LoadFrom(environ []string) (Config, error) {
	env := make(map[string]string, len(environ))
	for _, entry := range environ {
		name, value, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		env[name] = value
	}

	cfg := Config{
		HTTPAddr:   DefaultHTTPAddr,
		Adapters:   []string{DefaultAdapters},
		PollEvery:  DefaultPollInt,
		BufferSize: DefaultBufferSize,
		BatchSize:  DefaultBatchSize,
		FlushEvery: DefaultFlushInt,
		LogLevel:   DefaultLogLevel,
		Lifecycle:  lifecycle.DefaultPolicy(),
	}

	if value, ok := lookup(env, EnvHTTPAddr, aliasHTTPAddr); ok {
		cfg.HTTPAddr = value
	}
	if value, ok := lookup(env, EnvPostgresDSN, aliasPostgresDSN); ok {
		cfg.PostgresDSN = value
	}
	if value, ok := lookup(env, EnvRedisURL, aliasRedisURL); ok {
		cfg.RedisURL = value
	}
	if value, ok := lookup(env, EnvAdminToken, aliasAdminToken); ok {
		cfg.AdminToken = value
	}
	if value, ok := lookupPresent(env, EnvAdapters, aliasAdapters); ok {
		// An explicitly empty list means "no adapters", not "the default".
		cfg.Adapters = splitList(value)
	}
	if value, ok := lookup(env, EnvPollInt, aliasPollInt); ok {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, &Error{Problem: fmt.Sprintf("%s: %q is not a duration", EnvPollInt, value)}
		}
		cfg.PollEvery = parsed
	}
	if value, ok := lookup(env, EnvBufferSize, aliasBufferSize); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return Config{}, &Error{Problem: fmt.Sprintf("%s: %q is not a positive integer", EnvBufferSize, value)}
		}
		cfg.BufferSize = parsed
	}
	if value, ok := lookup(env, EnvBatchSize, aliasBatchSize); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return Config{}, &Error{Problem: fmt.Sprintf("%s: %q is not a positive integer", EnvBatchSize, value)}
		}
		cfg.BatchSize = parsed
	}
	if value, ok := lookup(env, EnvLogLevel, aliasLogLevel); ok {
		cfg.LogLevel = value
	}
	if value, ok := lookup(env, EnvFlushInt, aliasFlushInt); ok {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, &Error{Problem: fmt.Sprintf("%s: %s is not a duration", EnvFlushInt, value)}
		}
		cfg.FlushEvery = parsed
	}

	if err := loadLifecycle(env, &cfg.Lifecycle); err != nil {
		return Config{}, err
	}
	if err := loadDashboard(env, &cfg); err != nil {
		return Config{}, err
	}
	if cfg.PostgresDSN == "" {
		return Config{}, &Error{Problem: fmt.Sprintf("%s is required", EnvPostgresDSN)}
	}
	if cfg.AdminToken == "" {
		return Config{}, &Error{Problem: fmt.Sprintf("%s is required", EnvAdminToken)}
	}
	if len(cfg.AdminToken) < MinAdminTokenLen {
		return Config{}, &Error{
			Problem: fmt.Sprintf("%s must be at least %d characters", EnvAdminToken, MinAdminTokenLen),
		}
	}
	if cfg.BatchSize > MaxBatchEvents {
		return Config{}, &Error{
			Problem: fmt.Sprintf("%s must not exceed %d", EnvBatchSize, MaxBatchEvents),
		}
	}
	for _, adapter := range cfg.Adapters {
		if adapter == "bullmq" && cfg.RedisURL == "" {
			return Config{}, &Error{
				Problem: fmt.Sprintf("%s is required when the bullmq adapter is enabled", EnvRedisURL),
			}
		}
	}
	return cfg, nil
}

func loadLifecycle(env map[string]string, policy *lifecycle.Policy) error {
	for _, field := range []struct {
		name        string
		destination *time.Duration
	}{
		{EnvRawRetention, &policy.RawRetention}, {EnvHistoryRetention, &policy.HistoryRetention},
	} {
		if value, ok := lookup(env, field.name, strings.Replace(field.name, "QUEUELENS_", "QUELENS_", 1)); ok {
			days, err := strconv.ParseInt(value, 10, 64)
			if err != nil || days < 1 || days > int64((1<<63-1)/(24*time.Hour)) {
				return &Error{Problem: field.name + " must be a positive number of days within the duration range"}
			}
			*field.destination = time.Duration(days) * 24 * time.Hour
		}
	}
	for _, field := range []struct {
		name        string
		destination *time.Duration
	}{
		{EnvStallAfter, &policy.StallAfter}, {EnvEvaluationInt, &policy.EvaluationEvery},
		{EnvMaintenanceInt, &policy.MaintenanceEvery}, {EnvProjectionInt, &policy.PollEvery},
	} {
		if value, ok := lookup(env, field.name, strings.Replace(field.name, "QUEUELENS_", "QUELENS_", 1)); ok {
			parsed, err := time.ParseDuration(value)
			if err != nil {
				return &Error{Problem: field.name + " must be a duration"}
			}
			*field.destination = parsed
		}
	}
	if value, ok := lookup(env, EnvProjectionBatch, "QUELENS_PROJECTION_BATCH_SIZE"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return &Error{Problem: EnvProjectionBatch + " must be an integer"}
		}
		policy.BatchSize = parsed
	}
	if err := policy.Validate(); err != nil {
		return &Error{Problem: err.Error()}
	}
	return nil
}

func splitList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
