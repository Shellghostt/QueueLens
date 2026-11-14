package protocol

import (
	"fmt"
	"strings"
)

// Heartbeat records liveness at collector receipt time; client clocks are not trusted.
type Heartbeat struct {
	V        int    `json:"v"`
	WorkerID string `json:"worker_id"`
	Pool     string `json:"pool,omitempty"`
}

func (h Heartbeat) Validate() error {
	if h.V != Version {
		return fmt.Errorf("v: must be 1")
	}
	if strings.TrimSpace(h.WorkerID) == "" || len(h.WorkerID) > 128 {
		return fmt.Errorf("worker_id: must contain 1 to 128 bytes")
	}
	if len(h.Pool) > 128 {
		return fmt.Errorf("pool: must not exceed 128 bytes")
	}
	return nil
}
