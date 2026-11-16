package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/shellghostt/queuelens/internal/lifecycle"
)

func pageQuery(r *http.Request) (lifecycle.PageQuery, error) {
	values := r.URL.Query()
	query := lifecycle.PageQuery{Queue: values.Get("queue"), Status: values.Get("status"), Cursor: values.Get("cursor")}
	if values.Has("limit") {
		limit, err := strconv.Atoi(values.Get("limit"))
		if err != nil || limit < 1 || limit > lifecycle.PageLimit {
			return query, lifecycle.ErrInvalidQuery
		}
		query.Limit = limit
	}
	if len(query.Cursor) > lifecycle.MaxCursorBytes {
		return query, lifecycle.ErrInvalidQuery
	}
	return query, nil
}

func (s *Server) handleFailedJobs(w http.ResponseWriter, r *http.Request) {
	s.handleJobEvidence(w, r, "failed")
}
func (s *Server) handleStalledJobs(w http.ResponseWriter, r *http.Request) {
	s.handleJobEvidence(w, r, "stalled")
}
func (s *Server) handleSuspectedStalls(w http.ResponseWriter, r *http.Request) {
	s.handleJobEvidence(w, r, "suspected-stalled")
}

func (s *Server) handleJobEvidence(w http.ResponseWriter, r *http.Request, view string) {
	if s.lifecycle == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "lifecycle queries unavailable"})
		return
	}
	query, err := pageQuery(r)
	if err != nil {
		s.lifecycleError(w, err)
		return
	}
	page, err := s.lifecycle.QueryJobs(r.Context(), view, query, s.now())
	if err != nil {
		s.lifecycleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleWorkers(w http.ResponseWriter, r *http.Request) {
	if s.lifecycle == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "lifecycle queries unavailable"})
		return
	}
	query, err := pageQuery(r)
	if err != nil {
		s.lifecycleError(w, err)
		return
	}
	page, err := s.lifecycle.QueryWorkers(r.Context(), query, s.now())
	if err != nil {
		s.lifecycleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) lifecycleError(w http.ResponseWriter, err error) {
	if errors.Is(err, lifecycle.ErrInvalidQuery) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": lifecycle.ErrInvalidQuery.Error()})
		return
	}
	s.logger.Error("query lifecycle evidence", "error", err)
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "lifecycle queries unavailable"})
}
