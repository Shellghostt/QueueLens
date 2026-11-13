package metrics

import (
	"strings"
	"testing"
)

func TestWritePrometheusEmitsEveryCounter(t *testing.T) {
	metricSet := New()
	metricSet.IngestRequests.Add(3)
	metricSet.EventsAccepted.Add(7)
	metricSet.EventsDroppedBuffer.Add(2)

	var builder strings.Builder
	if err := metricSet.WritePrometheus(&builder); err != nil {
		t.Fatalf("write: %v", err)
	}
	output := builder.String()

	for _, name := range []string{
		"queuelens_ingest_requests_total",
		"queuelens_events_accepted_total",
		"queuelens_events_rejected_total",
		"queuelens_events_too_old_total",
		"queuelens_events_persisted_total",
		"queuelens_buffer_dropped_total",
		"queuelens_batches_persisted_total",
		"queuelens_batch_errors_total",
		"queuelens_adapter_polls_total",
		"queuelens_adapter_poll_errors_total",
	} {
		if !strings.Contains(output, "# TYPE "+name+" counter") {
			t.Fatalf("expected the exposition to declare %s", name)
		}
	}

	if !strings.Contains(output, "queuelens_ingest_requests_total 3\n") {
		t.Fatalf("expected the ingest counter to be 3, got:\n%s", output)
	}
	if !strings.Contains(output, "queuelens_events_accepted_total 7\n") {
		t.Fatalf("expected the accepted counter to be 7, got:\n%s", output)
	}
	if !strings.Contains(output, "queuelens_buffer_dropped_total 2\n") {
		t.Fatalf("expected the dropped counter to be 2, got:\n%s", output)
	}
}

func TestWritePrometheusFailsWhenTheWriterFails(t *testing.T) {
	metricSet := New()
	if err := metricSet.WritePrometheus(failingWriter{}); err == nil {
		t.Fatal("expected an error")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errWrite
}

var errWrite = &writeError{}

type writeError struct{}

func (*writeError) Error() string { return "write failed" }
