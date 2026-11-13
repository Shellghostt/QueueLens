// Package api serves the collector and query HTTP endpoints.
//
// Only the endpoints that exist are registered. The OpenAPI document lists
// exactly these paths; a path in the document that is not registered here is
// a documentation bug.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/shellghostt/queuelens/internal/collector"
	"github.com/shellghostt/queuelens/internal/config"
	"github.com/shellghostt/queuelens/internal/metrics"
	"github.com/shellghostt/queuelens/internal/queues"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

// Ping checks that the database is reachable.
type Ping func(ctx context.Context) error

// Server holds the dependencies of the HTTP API.
type Server struct {
	collector *collector.Collector
	queues    *queues.Store
	metrics   *metrics.Metrics
	ping      Ping
	token     string
	version   string
	logger    *slog.Logger
	now       func() time.Time
}

// Options configures a server.
type Options struct {
	Collector *collector.Collector
	Queues    *queues.Store
	Metrics   *metrics.Metrics
	Ping      Ping
	Token     string
	Version   string
	Logger    *slog.Logger
	Now       func() time.Time
}

// New builds the HTTP handler.
func New(opts Options) http.Handler {
	if opts.Metrics == nil {
		opts.Metrics = metrics.New()
	}
	if opts.Queues == nil {
		opts.Queues = queues.NewStore()
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Version == "" {
		opts.Version = "dev"
	}

	server := &Server{
		collector: opts.Collector,
		queues:    opts.Queues,
		metrics:   opts.Metrics,
		ping:      opts.Ping,
		token:     opts.Token,
		version:   opts.Version,
		logger:    opts.Logger,
		now:       opts.Now,
	}

	router := chi.NewRouter()
	router.Use(server.recoverer)

	router.Get("/healthz", server.handleHealth)
	router.Get("/readyz", server.handleReady)
	router.Get("/metrics", server.handleMetrics)

	router.Group(func(private chi.Router) {
		private.Use(server.requireToken)
		private.Post("/v1/events", server.handleIngest)
		private.Get("/v1/queues", server.handleQueues)
	})

	return router
}

// recoverer turns a panic into a 500 and a log line, so one bad request does
// not take the collector down.
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				s.logger.Error("panic in handler", "path", r.URL.Path, "panic", recovered)
				writeJSON(w, http.StatusInternalServerError, map[string]string{
					"error": "internal error",
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// requireToken enforces the bearer token. The comparison is constant time so
// that the token cannot be guessed one byte at a time.
func (s *Server) requireToken(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if len(header) <= len(prefix) || header[:len(prefix)] != prefix {
			writeJSON(w, http.StatusUnauthorized, map[string]string{
				"error": "missing bearer token",
			})
			return
		}
		presented := header[len(prefix):]
		if subtle.ConstantTimeCompare([]byte(presented), []byte(s.token)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{
				"error": "invalid bearer token",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"version": s.version,
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.ping != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.ping(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{
				"status": "not ready",
				"error":  err.Error(),
			})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if err := s.metrics.WritePrometheus(w); err != nil {
		s.logger.Error("write metrics", "error", err)
	}
}

type ingestResponse struct {
	Accepted int                   `json:"accepted"`
	Dropped  int                   `json:"dropped"`
	Rejected []collector.Rejection `json:"rejected"`
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, config.MaxBatchBodyBytes)

	decoder := json.NewDecoder(r.Body)
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("could not read the request body: %v", err),
		})
		return
	}
	if decoder.More() {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "the request body must contain exactly one JSON document",
		})
		return
	}

	events, err := decodeEvents(raw)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if len(events) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "the batch contains no events"})
		return
	}
	if len(events) > config.MaxBatchEvents {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": fmt.Sprintf("the batch contains %d events; the limit is %d", len(events), config.MaxBatchEvents),
		})
		return
	}

	result := s.collector.Ingest(r.Context(), events)
	response := ingestResponse{
		Accepted: result.Accepted,
		Dropped:  result.Dropped,
		Rejected: result.Rejected,
	}

	switch {
	case result.Dropped > 0:
		// Saturation is a load signal, not a client error: the events that
		// made it in are still counted, and the drops are counted too.
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, response)
	case result.Accepted == 0 && len(result.Rejected) > 0:
		writeJSON(w, http.StatusBadRequest, response)
	default:
		writeJSON(w, http.StatusAccepted, response)
	}
}

// decodeEvents accepts either a JSON array of events or a single event
// object. Unknown fields are ignored, as the protocol requires.
func decodeEvents(raw json.RawMessage) ([]protocol.Event, error) {
	trimmed := trimSpace(raw)
	if len(trimmed) == 0 {
		return nil, errors.New("the request body is empty")
	}

	switch trimmed[0] {
	case '[':
		var events []protocol.Event
		if err := json.Unmarshal(trimmed, &events); err != nil {
			return nil, fmt.Errorf("could not decode the event array: %w", err)
		}
		return events, nil
	case '{':
		var event protocol.Event
		if err := json.Unmarshal(trimmed, &event); err != nil {
			return nil, fmt.Errorf("could not decode the event: %w", err)
		}
		return []protocol.Event{event}, nil
	default:
		return nil, errors.New("the request body must be an event or an array of events")
	}
}

func trimSpace(raw []byte) []byte {
	start := 0
	for start < len(raw) && (raw[start] == ' ' || raw[start] == '\n' || raw[start] == '\r' || raw[start] == '\t') {
		start++
	}
	end := len(raw)
	for end > start && (raw[end-1] == ' ' || raw[end-1] == '\n' || raw[end-1] == '\r' || raw[end-1] == '\t') {
		end--
	}
	return raw[start:end]
}

type queuesResponse struct {
	GeneratedAt time.Time         `json:"generated_at"`
	Queues      []queues.View     `json:"queues"`
	AdapterErrs map[string]string `json:"adapter_errors,omitempty"`
}

func (s *Server) handleQueues(w http.ResponseWriter, _ *http.Request) {
	views := s.queues.List()
	if views == nil {
		views = []queues.View{}
	}
	writeJSON(w, http.StatusOK, queuesResponse{
		GeneratedAt: s.now().UTC(),
		Queues:      views,
		AdapterErrs: s.queues.Errors(),
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		// The status line is already written, so there is nothing left to do
		// but record it.
		slog.Default().Error("write json response", "error", err)
	}
}
