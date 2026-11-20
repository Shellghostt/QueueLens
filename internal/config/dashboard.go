package config

import (
	"net"
	"net/url"
	"strconv"
)

const EnvDashboardPassword = "QUEUELENS_DASHBOARD_PASSWORD"
const EnvDashboardOrigin = "QUEUELENS_DASHBOARD_ORIGIN"
const EnvDashboardInsecureDev = "QUEUELENS_DASHBOARD_INSECURE_DEV"

type DashboardAccess struct {
	Password    string
	Origin      string
	InsecureDev bool
}

func (d DashboardAccess) Validate(adminToken string) error {
	if d.Password == "" && d.Origin == "" && !d.InsecureDev {
		return nil
	}
	if len(d.Password) < 12 || len(d.Password) > 256 || d.Password == adminToken {
		return &Error{Problem: "dashboard password must be 12–256 bytes and differ from the admin token"}
	}
	u, err := url.Parse(d.Origin)
	if err != nil || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" {
		return &Error{Problem: "dashboard origin must be an exact scheme://host[:port] origin without credentials, path, query or fragment"}
	}
	if d.InsecureDev {
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "http" || (u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
			return &Error{Problem: "insecure dashboard development requires an HTTP loopback origin"}
		}
	} else if u.Scheme != "https" {
		return &Error{Problem: "dashboard origin requires HTTPS; HTTP loopback requires explicit insecure development mode"}
	}
	return nil
}

func loadDashboard(env map[string]string, cfg *Config) error {
	cfg.Dashboard.Password, _ = lookup(env, EnvDashboardPassword, "QUELENS_DASHBOARD_PASSWORD")
	cfg.Dashboard.Origin, _ = lookup(env, EnvDashboardOrigin, "QUELENS_DASHBOARD_ORIGIN")
	if value, ok := lookup(env, EnvDashboardInsecureDev, "QUELENS_DASHBOARD_INSECURE_DEV"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return &Error{Problem: EnvDashboardInsecureDev + " must be a boolean"}
		}
		cfg.Dashboard.InsecureDev = parsed
	}
	return cfg.Dashboard.Validate(cfg.AdminToken)
}
