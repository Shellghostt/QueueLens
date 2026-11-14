// Package protocol defines the QueueLens wire protocol, version 1.
//
// The protocol is the contract between an adapter (or an SDK) and the
// collector. It is deliberately a small, boring JSON document so that an
// adapter written in another language never has to import a Go package.
//
// See docs/PLAN.md section 9 for the specification this implements.
package protocol

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Version is the only protocol version this build accepts.
const Version = 1

// State is a lifecycle state of one job.
type State string

// The states defined by the protocol. The same list is the CHECK constraint
// on the storage table.
const (
	StateEnqueued  State = "enqueued"
	StateStarted   State = "started"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
	StateRetried   State = "retried"
	StateStalled   State = "stalled"
	StateDrained   State = "drained"
)

// States lists every valid state, in the order used by reports.
func States() []State {
	return []State{
		StateEnqueued,
		StateStarted,
		StateCompleted,
		StateFailed,
		StateRetried,
		StateStalled,
		StateDrained,
	}
}

// Valid reports whether s is a known state.
func (s State) Valid() bool {
	switch s {
	case StateEnqueued, StateStarted, StateCompleted, StateFailed,
		StateRetried, StateStalled, StateDrained:
		return true
	default:
		return false
	}
}

// Limits bounds what a single event may contain. They exist so that one
// client cannot fill the database with a 10 MiB error string.
type Limits struct {
	MaxQueueLen   int
	MaxJobIDLen   int
	MaxJobNameLen int
	MaxWorkerLen  int
	MaxErrorLen   int
	// MaxAttempt is the largest attempt number that fits in the storage
	// column. The plan stores it as a smallint, so the protocol refuses
	// anything larger instead of letting it overflow.
	MaxAttempt    int
	MaxPastAge    time.Duration
	MaxFutureSkew time.Duration
}

// DefaultLimits are the limits the collector enforces.
func DefaultLimits() Limits {
	return Limits{
		MaxQueueLen:   256,
		MaxJobIDLen:   256,
		MaxJobNameLen: 256,
		MaxWorkerLen:  128,
		MaxErrorLen:   1024,
		MaxAttempt:    32767,
		MaxPastAge:    7 * 24 * time.Hour,
		MaxFutureSkew: time.Hour,
	}
}

// Event is one lifecycle fact about one job.
//
// Unknown JSON fields are ignored on purpose: a newer adapter must not break
// an older collector.
type Event struct {
	V          int       `json:"v"`
	ID         string    `json:"id,omitempty"`
	TS         time.Time `json:"ts"`
	Queue      string    `json:"queue"`
	JobID      string    `json:"job_id"`
	JobName    string    `json:"job_name,omitempty"`
	Attempt    int       `json:"attempt,omitempty"`
	State      State     `json:"state"`
	WorkerID   string    `json:"worker_id,omitempty"`
	DurationMS *int      `json:"duration_ms,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// ValidationError explains why one event was rejected. The field is the
// JSON name, so the reason can be shown to whoever sent the event.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return e.Reason
	}
	return e.Field + ": " + e.Reason
}

// ErrTooOld is returned for an event older than the allowed age. It has its
// own type because it is counted separately: a clock-skewed producer is a
// different problem from a malformed event.
var ErrTooOld = errors.New("event is older than the accepted age")

// TooOldError marks an event that was rejected because of its timestamp.
type TooOldError struct {
	Event Event
	Age   time.Duration
}

func (e *TooOldError) Error() string {
	return fmt.Sprintf("event is older than the accepted age (%s)", e.Age)
}

func (e *TooOldError) Unwrap() error { return ErrTooOld }

// Validate checks one event against the limits.
//
// now is injected so that the age check is testable and so that every event
// in one batch is judged against the same instant.
func (e *Event) Validate(now time.Time, limits Limits) error {
	if e.V != Version {
		return &ValidationError{Field: "v", Reason: fmt.Sprintf("must be %d", Version)}
	}
	if strings.TrimSpace(e.Queue) == "" {
		return &ValidationError{Field: "queue", Reason: "is required"}
	}
	if len(e.Queue) > limits.MaxQueueLen {
		return &ValidationError{
			Field:  "queue",
			Reason: fmt.Sprintf("is longer than %d characters", limits.MaxQueueLen),
		}
	}
	if strings.TrimSpace(e.JobID) == "" {
		return &ValidationError{Field: "job_id", Reason: "is required"}
	}
	if len(e.JobID) > limits.MaxJobIDLen {
		return &ValidationError{
			Field:  "job_id",
			Reason: fmt.Sprintf("is longer than %d characters", limits.MaxJobIDLen),
		}
	}
	if len(e.JobName) > limits.MaxJobNameLen {
		return &ValidationError{
			Field:  "job_name",
			Reason: fmt.Sprintf("is longer than %d characters", limits.MaxJobNameLen),
		}
	}
	if len(e.WorkerID) > limits.MaxWorkerLen {
		return &ValidationError{
			Field:  "worker_id",
			Reason: fmt.Sprintf("is longer than %d characters", limits.MaxWorkerLen),
		}
	}
	if !e.State.Valid() {
		return &ValidationError{
			Field:  "state",
			Reason: fmt.Sprintf("must be one of %s", stateList()),
		}
	}
	if e.Attempt < 0 {
		return &ValidationError{Field: "attempt", Reason: "must not be negative"}
	}
	if e.Attempt > limits.MaxAttempt {
		return &ValidationError{
			Field:  "attempt",
			Reason: fmt.Sprintf("must not exceed %d", limits.MaxAttempt),
		}
	}
	if e.DurationMS != nil && *e.DurationMS < 0 {
		return &ValidationError{Field: "duration_ms", Reason: "must not be negative"}
	}
	if e.DurationMS != nil && *e.DurationMS > 2147483647 {
		return &ValidationError{Field: "duration_ms", Reason: "must not exceed 2147483647"}
	}
	if e.TS.IsZero() {
		return &ValidationError{Field: "ts", Reason: "is required"}
	}
	if e.TS.Before(now.Add(-limits.MaxPastAge)) {
		return &TooOldError{Event: *e, Age: now.Sub(e.TS)}
	}
	if e.TS.After(now.Add(limits.MaxFutureSkew)) {
		return &ValidationError{
			Field:  "ts",
			Reason: "is too far in the future; check the producer's clock",
		}
	}
	return nil
}

// Normalize fills in the defaults the protocol allows to be omitted and
// applies the best-effort redaction of the error string.
func (e *Event) Normalize(limits Limits) {
	if e.Attempt == 0 {
		e.Attempt = 1
	}
	if len(e.Error) > limits.MaxErrorLen {
		e.Error = e.Error[:limits.MaxErrorLen]
		for !utf8.ValidString(e.Error) {
			e.Error = e.Error[:len(e.Error)-1]
		}
	}
	e.Error = RedactSecrets(e.Error)
}

func stateList() string {
	states := States()
	names := make([]string, 0, len(states))
	for _, state := range states {
		names = append(names, string(state))
	}
	return strings.Join(names, ", ")
}

var secretPatterns = []*regexp.Regexp{
	// Bearer and Basic credentials in headers.
	regexp.MustCompile(`(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}`),
	// key=value and key: value pairs, for the names that show up in stack
	// traces and connection strings. The optional quotes let the pattern
	// match a JSON body such as {"api_key": "..."} and leave it valid JSON.
	regexp.MustCompile(`(?i)\b(token|password|passwd|secret|api[_-]?key|access[_-]?key)\b(["']?)(\s*[:=]\s*)(["']?)([^\s"',;]+)(["']?)`),
	// An authorization header whose value is a raw token rather than a
	// scheme. The value has to look like a token (at least eight characters,
	// no spaces), so the word "Bearer" is left alone for the pattern above.
	regexp.MustCompile(`(?i)\bauthorization\b(["']?)(\s*[:=]\s*)(["']?)([A-Za-z0-9._~+/=-]{8,})(["']?)`),
	// PostgreSQL and Redis URLs with inline credentials.
	regexp.MustCompile(`(?i)\b([a-z][a-z0-9+.-]*)://[^/\s:@]+:[^/\s@]+@`),
}

// secretReplacements keeps the shape of the redacted text so that a JSON
// error body stays a JSON error body.
var secretReplacements = []string{
	"${1} [redacted]",
	"${1}${2}${3}${4}[redacted]${6}",
	"authorization${1}${2}${3}[redacted]${5}",
	"${1}://[redacted]@",
}

// RedactSecrets removes common credential shapes from a free-text error.
//
// This is best effort by design: it is a safety net for the common cases, not
// a guarantee. The documented mitigation in the threat model says exactly
// that, and the error string is also capped in length.
func RedactSecrets(text string) string {
	if text == "" {
		return text
	}
	for i, pattern := range secretPatterns {
		text = pattern.ReplaceAllString(text, secretReplacements[i])
	}
	return text
}
