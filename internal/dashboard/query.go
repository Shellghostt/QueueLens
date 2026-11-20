// Package dashboard defines bounded M4 reads without extending the M3 reader.
package dashboard

import (
	"context"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/shellghostt/queuelens/internal/lifecycle"
	"github.com/shellghostt/queuelens/internal/queues"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

const MaxResponseBytes = 2 << 20
const MaxQueryBytes = 12 << 10
const MetricRowLimit = 500000
const MetricTimeout = 150 * time.Millisecond

type Query struct {
	Queue, JobID, State, Window, Cursor string
	Limit                               int
}

type Reader interface {
	QueryDashboard(context.Context, string, Query, *queues.Store, time.Time) (Response, error)
}

type QueuePage struct {
	Items         []queues.View     `json:"items"`
	NextCursor    string            `json:"next_cursor,omitempty"`
	AdapterErrors map[string]string `json:"adapter_errors"`
	Source        string            `json:"source"`
}

type JobPage struct {
	Items      []lifecycle.JobItem `json:"items"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

type JobDetail struct {
	Queue        string              `json:"queue"`
	JobID        string              `json:"job_id"`
	Availability string              `json:"availability"`
	Summary      *lifecycle.JobItem  `json:"summary"`
	Attempts     []lifecycle.JobItem `json:"attempts"`
	NextCursor   string              `json:"next_cursor,omitempty"`
	PrunedAt     *time.Time          `json:"pruned_at"`
}

type Metrics struct {
	Available            bool      `json:"available"`
	Reason               string    `json:"reason,omitempty"`
	Window               string    `json:"window"`
	From                 time.Time `json:"from"`
	Until                time.Time `json:"until"`
	Source               string    `json:"source"`
	Completed            *int64    `json:"completed"`
	Failed               *int64    `json:"failed"`
	Conflicting          *int64    `json:"conflicting"`
	CompletionsPerSecond *float64  `json:"completions_per_second"`
	FailureFraction      *float64  `json:"failure_fraction"`
	Limitations          []string  `json:"limitations"`
}

type Response struct {
	lifecycle.PageEvidence
	Queues  *QueuePage   `json:"queues,omitempty"`
	Broker  *queues.View `json:"broker,omitempty"`
	Metrics *Metrics     `json:"metrics,omitempty"`
	Jobs    *JobPage     `json:"jobs,omitempty"`
	Job     *JobDetail   `json:"job,omitempty"`
}

func ValidIdentifier(value string) bool {
	return value != "" && len(value) <= 256 && utf8.ValidString(value) && strings.TrimSpace(value) != "" && !strings.ContainsRune(value, 0)
}

func WindowDuration(value string) (time.Duration, bool) {
	switch value {
	case "1h":
		return time.Hour, true
	case "24h":
		return 24 * time.Hour, true
	case "7d":
		return 7 * 24 * time.Hour, true
	case "30d":
		return 30 * 24 * time.Hour, true
	default:
		return 0, false
	}
}

func (q Query) Normalize(view string) (Query, error) {
	if q.State == "" {
		q.State = "all"
	}
	if q.Window == "" {
		q.Window = "1h"
	}
	validState := q.State == "all"
	for _, state := range protocol.States() {
		validState = validState || q.State == string(state)
	}
	_, validWindow := WindowDuration(q.Window)
	if !validState || !validWindow || q.Limit < 0 || q.Limit > lifecycle.PageLimit || len(q.Cursor) > lifecycle.MaxCursorBytes ||
		(q.Queue != "" && !ValidIdentifier(q.Queue)) || (q.JobID != "" && !ValidIdentifier(q.JobID)) {
		return q, lifecycle.ErrInvalidQuery
	}
	switch view {
	case "overview", "queues":
		if q.Queue != "" || q.JobID != "" || q.State != "all" || (view == "queues" && q.Window != "1h") {
			return q, lifecycle.ErrInvalidQuery
		}
	case "jobs":
		if q.JobID != "" || q.Window != "1h" {
			return q, lifecycle.ErrInvalidQuery
		}
	case "queue":
		if !ValidIdentifier(q.Queue) || q.JobID != "" {
			return q, lifecycle.ErrInvalidQuery
		}
	case "job":
		if !ValidIdentifier(q.Queue) || !ValidIdentifier(q.JobID) || q.State != "all" || q.Window != "1h" {
			return q, lifecycle.ErrInvalidQuery
		}
	default:
		return q, lifecycle.ErrInvalidQuery
	}
	return q, nil
}

// Binding uses an unambiguous encoding even when identifiers contain delimiters.
func (q Query) Binding() string {
	value, _ := json.Marshal([]string{q.State, q.Window, q.JobID})
	return string(value)
}
