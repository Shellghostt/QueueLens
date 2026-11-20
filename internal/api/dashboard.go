package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/shellghostt/queuelens/internal/dashboard"
	"github.com/shellghostt/queuelens/internal/lifecycle"
)

func strictQuery(r *http.Request, allowed ...string) (url.Values, error) {
	if len(r.URL.RawQuery) > dashboard.MaxQueryBytes {
		return nil, lifecycle.ErrInvalidQuery
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return nil, lifecycle.ErrInvalidQuery
	}
	for key, entries := range values {
		known := false
		for _, name := range allowed {
			known = known || key == name
		}
		if !known || len(entries) != 1 {
			return nil, lifecycle.ErrInvalidQuery
		}
	}
	return values, nil
}

func dashboardQuery(r *http.Request, view string) (dashboard.Query, error) {
	allowed := []string{"limit", "cursor"}
	switch view {
	case "overview":
		allowed = append(allowed, "window")
	case "queue":
		allowed = append(allowed, "queue", "state", "window")
	case "jobs":
		allowed = append(allowed, "queue", "state")
	case "job":
		allowed = append(allowed, "queue", "job_id")
	}
	v, err := strictQuery(r, allowed...)
	if err != nil {
		return dashboard.Query{}, err
	}
	q := dashboard.Query{Queue: v.Get("queue"), JobID: v.Get("job_id"), State: v.Get("state"), Window: v.Get("window"), Cursor: v.Get("cursor")}
	for _, name := range []string{"state", "window"} {
		if v.Has(name) && v.Get(name) == "" {
			return q, lifecycle.ErrInvalidQuery
		}
	}
	for _, name := range []string{"queue", "job_id"} {
		if v.Has(name) && !dashboard.ValidIdentifier(v.Get(name)) {
			return q, lifecycle.ErrInvalidQuery
		}
	}
	if v.Has("limit") {
		q.Limit, err = strconv.Atoi(v.Get("limit"))
		if err != nil || q.Limit < 1 {
			return q, lifecycle.ErrInvalidQuery
		}
	}
	return q.Normalize(view)
}

func (s *Server) handleDashboard(view string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q, err := dashboardQuery(r, view)
		if err != nil {
			s.dashboardError(w, err)
			return
		}
		if s.dashboard == nil {
			s.dashboardError(w, errors.New("dashboard reader unavailable"))
			return
		}
		result, err := s.dashboard.QueryDashboard(r.Context(), view, q, s.queues, s.now())
		if err != nil {
			s.dashboardError(w, err)
			return
		}
		writeDashboardJSON(w, result)
	}
}

func (s *Server) dashboardError(w http.ResponseWriter, err error) {
	code := http.StatusServiceUnavailable
	message := "dashboard query unavailable"
	if errors.Is(err, lifecycle.ErrInvalidQuery) {
		code, message = http.StatusBadRequest, "invalid dashboard query or cursor"
	}
	writeJSON(w, code, map[string]string{"error": message})
}

type boundedJSON struct{ bytes.Buffer }

func (b *boundedJSON) Write(p []byte) (int, error) {
	if len(p) > dashboard.MaxResponseBytes-b.Len() {
		return 0, errors.New("dashboard response bound exceeded")
	}
	return b.Buffer.Write(p)
}

func writeDashboardJSON(w http.ResponseWriter, value any) {
	var body boundedJSON
	if err := json.NewEncoder(&body).Encode(value); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "dashboard response unavailable or too large"})
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body.Bytes())
}
