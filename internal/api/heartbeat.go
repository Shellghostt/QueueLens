package api

import (
	"context"
	"encoding/json"
	"github.com/shellghostt/queuelens/pkg/protocol"
	"io"
	"net/http"
	"time"
)

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	s.metrics.HeartbeatRequests.Add(1)
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	decoder := json.NewDecoder(r.Body)
	var h protocol.Heartbeat
	if err := decoder.Decode(&h); err != nil || decoder.Decode(new(any)) != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expected one heartbeat JSON object"})
		return
	}
	if err := h.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if s.heartbeat == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "heartbeat storage unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.heartbeat(ctx, h); err != nil {
		s.metrics.HeartbeatErrors.Add(1)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "could not persist heartbeat"})
		return
	}
	s.metrics.HeartbeatsAccepted.Add(1)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
}
