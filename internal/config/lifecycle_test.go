package config

import (
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/lifecycle"
)

func lifecycleEnvironment() []string {
	return []string{EnvPostgresDSN + "=postgresql://localhost/test", EnvAdminToken + "=m3-test-admin-token", EnvAdapters + "="}
}

func TestLifecycleDefaultsAndBothEnvironmentSpellings(t *testing.T) {
	cfg, err := LoadFrom(lifecycleEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Lifecycle != lifecycle.DefaultPolicy() {
		t.Fatal(cfg.Lifecycle)
	}
	env := append(lifecycleEnvironment(), "QUELENS_RETENTION_RAW_DAYS=40",
		EnvRawRetention+"=31", "QUELENS_RETENTION_HISTORY_DAYS=15",
		"QUELENS_STALL_AFTER=0s", "QUELENS_EVALUATION_INTERVAL=1s",
		"QUELENS_MAINTENANCE_INTERVAL=30s", "QUELENS_PROJECTION_INTERVAL=10ms",
		"QUELENS_PROJECTION_BATCH_SIZE=10")
	cfg, err = LoadFrom(env)
	if err != nil {
		t.Fatal(err)
	}
	p := cfg.Lifecycle
	if p.RawRetention != 31*24*time.Hour || p.HistoryRetention != 15*24*time.Hour ||
		p.StallAfter != 0 || p.EvaluationEvery != time.Second || p.MaintenanceEvery != 30*time.Second ||
		p.PollEvery != 10*time.Millisecond || p.BatchSize != 10 {
		t.Fatal(p)
	}
}

func TestLifecycleRejectsOverflowAndUnboundedOrInvalidCadences(t *testing.T) {
	for _, entry := range []string{EnvRawRetention + "=0", EnvHistoryRetention + "=-1",
		EnvRawRetention + "=9223372036854775807", EnvRawRetention + "=1.5",
		EnvStallAfter + "=-1s", EnvEvaluationInt + "=500ms", EnvEvaluationInt + "=2m",
		EnvMaintenanceInt + "=0", EnvProjectionInt + "=0", EnvProjectionBatch + "=0",
		EnvProjectionBatch + "=1001"} {
		t.Run(entry, func(t *testing.T) {
			if _, err := LoadFrom(append(lifecycleEnvironment(), entry)); err == nil {
				t.Fatal("accepted invalid lifecycle policy")
			}
		})
	}
}
