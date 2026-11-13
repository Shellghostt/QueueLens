package protocol

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func validEvent(now time.Time) Event {
	return Event{
		V:        Version,
		TS:       now,
		Queue:    "emails",
		JobID:    "8891",
		JobName:  "send-welcome",
		Attempt:  2,
		State:    StateFailed,
		WorkerID: "w-7",
	}
}

func TestValidateAcceptsAWellFormedEvent(t *testing.T) {
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	event := validEvent(now)

	if err := event.Validate(now, DefaultLimits()); err != nil {
		t.Fatalf("expected the event to be accepted, got %v", err)
	}
}

func TestValidateRejectsTheWrongProtocolVersion(t *testing.T) {
	now := time.Now()
	event := validEvent(now)
	event.V = 2

	err := event.Validate(now, DefaultLimits())
	if err == nil {
		t.Fatal("expected an error for version 2")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected a ValidationError, got %T", err)
	}
	if validation.Field != "v" {
		t.Fatalf("expected the offending field to be v, got %q", validation.Field)
	}
}

func TestValidateRequiresQueueAndJobID(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name  string
		event Event
		field string
	}{
		{"empty queue", Event{V: Version, TS: now, JobID: "1", State: StateEnqueued}, "queue"},
		{"blank queue", Event{V: Version, TS: now, Queue: "   ", JobID: "1", State: StateEnqueued}, "queue"},
		{"empty job id", Event{V: Version, TS: now, Queue: "emails", State: StateEnqueued}, "job_id"},
		{"blank job id", Event{V: Version, TS: now, Queue: "emails", JobID: "  ", State: StateEnqueued}, "job_id"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.event.Validate(now, DefaultLimits())
			if err == nil {
				t.Fatal("expected an error")
			}
			var validation *ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("expected a ValidationError, got %T", err)
			}
			if validation.Field != test.field {
				t.Fatalf("expected field %q, got %q", test.field, validation.Field)
			}
		})
	}
}

func TestValidateRejectsAnUnknownState(t *testing.T) {
	now := time.Now()
	event := validEvent(now)
	event.State = State("exploded")

	err := event.Validate(now, DefaultLimits())
	if err == nil {
		t.Fatal("expected an error")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected a ValidationError, got %T", err)
	}
	if validation.Field != "state" {
		t.Fatalf("expected field state, got %q", validation.Field)
	}
	if !strings.Contains(validation.Reason, "enqueued") {
		t.Fatalf("expected the reason to list the valid states, got %q", validation.Reason)
	}
}

func TestValidateRejectsAnOldEventWithADistinctError(t *testing.T) {
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	event := validEvent(now)
	event.TS = now.Add(-8 * 24 * time.Hour)

	err := event.Validate(now, DefaultLimits())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !errors.Is(err, ErrTooOld) {
		t.Fatalf("expected ErrTooOld, got %v", err)
	}
	var tooOld *TooOldError
	if !errors.As(err, &tooOld) {
		t.Fatalf("expected a TooOldError, got %T", err)
	}
	if tooOld.Age < 8*24*time.Hour {
		t.Fatalf("expected the age to be reported, got %s", tooOld.Age)
	}
}

func TestValidateAcceptsAnEventAtTheEdgeOfTheAgeLimit(t *testing.T) {
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	event := validEvent(now)
	event.TS = now.Add(-7 * 24 * time.Hour)

	if err := event.Validate(now, DefaultLimits()); err != nil {
		t.Fatalf("expected the event to be accepted, got %v", err)
	}
}

func TestValidateRejectsAFutureTimestamp(t *testing.T) {
	now := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	event := validEvent(now)
	event.TS = now.Add(2 * time.Hour)

	err := event.Validate(now, DefaultLimits())
	if err == nil {
		t.Fatal("expected an error")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected a ValidationError, got %T", err)
	}
	if validation.Field != "ts" {
		t.Fatalf("expected field ts, got %q", validation.Field)
	}
}

func TestValidateRejectsNegativeNumbers(t *testing.T) {
	now := time.Now()
	negative := -1

	event := validEvent(now)
	event.Attempt = -1
	if err := event.Validate(now, DefaultLimits()); err == nil {
		t.Fatal("expected an error for a negative attempt")
	}

	event = validEvent(now)
	event.DurationMS = &negative
	if err := event.Validate(now, DefaultLimits()); err == nil {
		t.Fatal("expected an error for a negative duration")
	}
}

func TestValidateRejectsOversizedStrings(t *testing.T) {
	now := time.Now()
	limits := DefaultLimits()

	event := validEvent(now)
	event.Queue = strings.Repeat("q", limits.MaxQueueLen+1)
	if err := event.Validate(now, limits); err == nil {
		t.Fatal("expected an error for an oversized queue name")
	}

	event = validEvent(now)
	event.JobID = strings.Repeat("j", limits.MaxJobIDLen+1)
	if err := event.Validate(now, limits); err == nil {
		t.Fatal("expected an error for an oversized job id")
	}
}

func TestValidateRejectsAnAttemptThatDoesNotFitTheStorageColumn(t *testing.T) {
	now := time.Now()
	limits := DefaultLimits()

	event := validEvent(now)
	event.Attempt = limits.MaxAttempt + 1
	if err := event.Validate(now, limits); err == nil {
		t.Fatal("expected an error for an oversized attempt")
	}

	event = validEvent(now)
	event.Attempt = limits.MaxAttempt
	if err := event.Validate(now, limits); err != nil {
		t.Fatalf("expected the largest allowed attempt to be accepted, got %v", err)
	}
}

func TestValidateRequiresATimestamp(t *testing.T) {
	now := time.Now()
	event := validEvent(now)
	event.TS = time.Time{}

	err := event.Validate(now, DefaultLimits())
	if err == nil {
		t.Fatal("expected an error")
	}
	var validation *ValidationError
	if !errors.As(err, &validation) {
		t.Fatalf("expected a ValidationError, got %T", err)
	}
	if validation.Field != "ts" {
		t.Fatalf("expected field ts, got %q", validation.Field)
	}
}

func TestNormalizeAppliesDefaultsAndCapsTheError(t *testing.T) {
	limits := DefaultLimits()
	event := Event{Attempt: 0, Error: strings.Repeat("e", limits.MaxErrorLen+500)}

	event.Normalize(limits)

	if event.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", event.Attempt)
	}
	if len(event.Error) != limits.MaxErrorLen {
		t.Fatalf("expected the error to be capped at %d bytes, got %d", limits.MaxErrorLen, len(event.Error))
	}
}

func TestRedactSecretsRemovesCommonCredentialShapes(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		absent  string
		present string
	}{
		{
			name:    "password in a key value pair",
			input:   "connect failed password=hunter2 port=5432",
			absent:  "hunter2",
			present: "port=5432",
		},
		{
			name:    "api key in a json body",
			input:   `{"api_key": "sk-live-abcdef123456"}`,
			absent:  "sk-live-abcdef123456",
			present: "api_key",
		},
		{
			name:    "bearer token",
			input:   "authorization: Bearer abcdefghijklmnop",
			absent:  "abcdefghijklmnop",
			present: "Bearer [redacted]",
		},
		{
			name:    "credentials in a postgres url",
			input:   "cannot reach postgresql://user:secretpass@db:5432/app",
			absent:  "secretpass",
			present: "postgresql://[redacted]@",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := RedactSecrets(test.input)
			if strings.Contains(got, test.absent) {
				t.Fatalf("expected the secret to be removed, got %q", got)
			}
			if !strings.Contains(got, test.present) {
				t.Fatalf("expected %q to survive, got %q", test.present, got)
			}
		})
	}
}

func TestRedactSecretsLeavesOrdinaryTextAlone(t *testing.T) {
	input := "SMTP timeout while talking to mail.example.com"
	if got := RedactSecrets(input); got != input {
		t.Fatalf("expected the text to be unchanged, got %q", got)
	}
}

func TestStatesAreAllValid(t *testing.T) {
	for _, state := range States() {
		if !state.Valid() {
			t.Fatalf("state %q should be valid", state)
		}
	}
	if State("nope").Valid() {
		t.Fatal("an unknown state should not be valid")
	}
}
