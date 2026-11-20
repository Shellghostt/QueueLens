package config

import (
	"strings"
	"testing"
)

func TestDashboardConfigurationOptInAliasesAndOriginPolicy(t *testing.T) {
	base := baseEnv()
	if cfg, err := LoadFrom(base); err != nil || cfg.Dashboard.Password != "" {
		t.Fatal("dashboard should be disabled by default", err)
	}
	env := append(append([]string{}, base...), "QUELENS_DASHBOARD_PASSWORD=legacy-password-123", "QUELENS_DASHBOARD_ORIGIN=https://legacy.example",
		"QUEUELENS_DASHBOARD_PASSWORD=canonical-password-123", "QUEUELENS_DASHBOARD_ORIGIN=https://queues.example")
	if cfg, err := LoadFrom(env); err != nil || cfg.Dashboard.Password != "canonical-password-123" || cfg.Dashboard.Origin != "https://queues.example" {
		t.Fatal("canonical precedence", cfg.Dashboard.Origin, err)
	}
	for _, extra := range [][]string{
		{"QUEUELENS_DASHBOARD_PASSWORD=short", "QUEUELENS_DASHBOARD_ORIGIN=https://queues.example"},
		{"QUEUELENS_DASHBOARD_PASSWORD=0123456789abcdef", "QUEUELENS_DASHBOARD_ORIGIN=https://queues.example"},
		{"QUEUELENS_DASHBOARD_ORIGIN=https://queues.example"},
		{"QUEUELENS_DASHBOARD_PASSWORD=" + strings.Repeat("x", 257), "QUEUELENS_DASHBOARD_ORIGIN=https://queues.example"},
		{"QUEUELENS_DASHBOARD_PASSWORD=viewer-password-123", "QUEUELENS_DASHBOARD_ORIGIN=http://localhost:8080"},
		{"QUEUELENS_DASHBOARD_PASSWORD=viewer-password-123", "QUEUELENS_DASHBOARD_ORIGIN=https://queues.example/path"},
		{"QUEUELENS_DASHBOARD_PASSWORD=viewer-password-123", "QUEUELENS_DASHBOARD_ORIGIN=https://user@queues.example"},
		{"QUEUELENS_DASHBOARD_PASSWORD=viewer-password-123", "QUEUELENS_DASHBOARD_ORIGIN=http://queues.example", "QUEUELENS_DASHBOARD_INSECURE_DEV=true"},
		{"QUEUELENS_DASHBOARD_INSECURE_DEV=maybe"},
	} {
		if _, err := LoadFrom(append(append([]string{}, base...), extra...)); err == nil {
			t.Fatal("accepted invalid dashboard configuration", extra)
		}
	}
	for _, origin := range []string{"http://127.0.0.1:8080", "http://localhost:5173", "http://[::1]:8080"} {
		if _, err := LoadFrom(append(append([]string{}, base...), "QUEUELENS_DASHBOARD_PASSWORD=viewer-password-123",
			"QUEUELENS_DASHBOARD_ORIGIN="+origin, "QUEUELENS_DASHBOARD_INSECURE_DEV=true")); err != nil {
			t.Fatal(origin, err)
		}
	}
}
