// Package metrics holds the collector's counters.
//
// The counters are plain atomics rather than a client library: the collector
// must not spend its own overhead budget on instrumentation, and the number of
// metrics is small and fixed.
package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Metrics is the set of counters the collector exposes.
type Metrics struct {
	HeartbeatRequests   atomic.Uint64
	HeartbeatsAccepted  atomic.Uint64
	HeartbeatErrors     atomic.Uint64
	IngestRequests      atomic.Uint64
	EventsAccepted      atomic.Uint64
	EventsRejected      atomic.Uint64
	EventsTooOld        atomic.Uint64
	EventsPersisted     atomic.Uint64
	EventsDroppedBuffer atomic.Uint64
	BatchesPersisted    atomic.Uint64
	BatchErrors         atomic.Uint64
	EventsLost          atomic.Uint64
	AdapterPolls        atomic.Uint64
	AdapterPollErrors   atomic.Uint64
}

// New returns a zeroed metric set.
func New() *Metrics { return &Metrics{} }

type metric struct {
	name  string
	help  string
	value func(*Metrics) uint64
}

var metrics = []metric{
	{"queuelens_heartbeat_requests_total", "Authenticated heartbeat requests.", func(m *Metrics) uint64 { return m.HeartbeatRequests.Load() }},
	{"queuelens_heartbeats_accepted_total", "Persisted worker heartbeats.", func(m *Metrics) uint64 { return m.HeartbeatsAccepted.Load() }},
	{"queuelens_heartbeat_errors_total", "Heartbeat storage failures.", func(m *Metrics) uint64 { return m.HeartbeatErrors.Load() }},
	{"queuelens_events_lost_total", "Accepted events lost on failed storage writes.", func(m *Metrics) uint64 { return m.EventsLost.Load() }},
	{"queuelens_ingest_requests_total", "Ingest requests received.", func(m *Metrics) uint64 { return m.IngestRequests.Load() }},
	{"queuelens_events_accepted_total", "Events accepted into the buffer.", func(m *Metrics) uint64 { return m.EventsAccepted.Load() }},
	{"queuelens_events_rejected_total", "Events rejected by validation.", func(m *Metrics) uint64 { return m.EventsRejected.Load() }},
	{"queuelens_events_too_old_total", "Events rejected because their timestamp was too old.", func(m *Metrics) uint64 { return m.EventsTooOld.Load() }},
	{"queuelens_events_persisted_total", "Events written to storage.", func(m *Metrics) uint64 { return m.EventsPersisted.Load() }},
	{"queuelens_buffer_dropped_total", "Events dropped because the buffer was full.", func(m *Metrics) uint64 { return m.EventsDroppedBuffer.Load() }},
	{"queuelens_batches_persisted_total", "Batches written to storage.", func(m *Metrics) uint64 { return m.BatchesPersisted.Load() }},
	{"queuelens_batch_errors_total", "Batches that failed to write.", func(m *Metrics) uint64 { return m.BatchErrors.Load() }},
	{"queuelens_adapter_polls_total", "Adapter polls performed.", func(m *Metrics) uint64 { return m.AdapterPolls.Load() }},
	{"queuelens_adapter_poll_errors_total", "Adapter polls that failed.", func(m *Metrics) uint64 { return m.AdapterPollErrors.Load() }},
}

// WritePrometheus writes the counters in the Prometheus text exposition
// format.
func (m *Metrics) WritePrometheus(w io.Writer) error {
	for _, entry := range metrics {
		if _, err := fmt.Fprintf(
			w,
			"# HELP %s %s\n# TYPE %s counter\n%s %d\n",
			entry.name,
			entry.help,
			entry.name,
			entry.name,
			entry.value(m),
		); err != nil {
			return fmt.Errorf("write metrics: %w", err)
		}
	}
	return nil
}
