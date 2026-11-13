// Package collector receives protocol events, validates them, buffers them
// and writes them to storage.
//
// The collector never blocks a producer for longer than the HTTP request and
// never applies business logic: validation, buffering and persistence are the
// whole job.
package collector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shellghostt/queuelens/internal/metrics"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

// ErrBufferFull is returned when the bounded buffer cannot take another
// event. It is a saturation signal, not a bug: the caller gets 429 and the
// drop counter goes up.
var ErrBufferFull = errors.New("collector buffer is full")

// Sink persists a batch of events.
type Sink interface {
	WriteBatch(ctx context.Context, events []protocol.Event) (int64, error)
}

// Rejection explains why one event was not accepted.
type Rejection struct {
	Index  int    `json:"index"`
	ID     string `json:"id,omitempty"`
	Reason string `json:"reason"`
}

// Result is the outcome of one ingest request.
type Result struct {
	Accepted int         `json:"accepted"`
	Dropped  int         `json:"dropped"`
	Rejected []Rejection `json:"rejected"`
}

// Options configures a collector.
type Options struct {
	BufferSize int
	BatchSize  int
	FlushEvery time.Duration
	Limits     protocol.Limits
	Logger     *slog.Logger
	Now        func() time.Time
}

// Collector is the ingest pipeline.
type Collector struct {
	sink       Sink
	buffer     chan protocol.Event
	metrics    *metrics.Metrics
	limits     protocol.Limits
	batchSize  int
	flushEvery time.Duration
	logger     *slog.Logger
	now        func() time.Time

	stopOnce sync.Once
	started  atomic.Bool
	stopped  chan struct{}
}

// New creates a collector. BufferSize is the number of events that may wait
// for storage; BatchSize is the number of events written per round trip.
func New(sink Sink, opts Options, m *metrics.Metrics) *Collector {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 10000
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	if opts.FlushEvery <= 0 {
		opts.FlushEvery = 250 * time.Millisecond
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if m == nil {
		m = metrics.New()
	}
	return &Collector{
		sink:       sink,
		buffer:     make(chan protocol.Event, opts.BufferSize),
		metrics:    m,
		limits:     opts.Limits,
		batchSize:  opts.BatchSize,
		flushEvery: opts.FlushEvery,
		logger:     opts.Logger,
		now:        opts.Now,
		stopped:    make(chan struct{}),
	}
}

// Metrics exposes the counters, mostly for tests.
func (c *Collector) Metrics() *metrics.Metrics { return c.metrics }

// Start runs the writer until the context is cancelled.
func (c *Collector) Start(ctx context.Context) {
	c.started.Store(true)
	go c.writeLoop(ctx)
}

// Stop closes the buffer and waits for the writer to finish. It is safe to
// call more than once, and safe to call on a collector that was never
// started.
func (c *Collector) Stop() {
	c.stopOnce.Do(func() {
		close(c.buffer)
	})
	if !c.started.Load() {
		return
	}
	<-c.stopped
}

// Ingest validates a batch and puts the valid events into the buffer.
//
// It never blocks: if the buffer is full the remaining events are dropped and
// counted. The caller decides the HTTP status.
func (c *Collector) Ingest(ctx context.Context, events []protocol.Event) Result {
	c.metrics.IngestRequests.Add(1)

	now := c.now().UTC()
	result := Result{Rejected: []Rejection{}}

	for index, event := range events {
		if err := event.Validate(now, c.limits); err != nil {
			reason := err.Error()
			if errors.Is(err, protocol.ErrTooOld) {
				c.metrics.EventsTooOld.Add(1)
			}
			c.metrics.EventsRejected.Add(1)
			result.Rejected = append(result.Rejected, Rejection{
				Index:  index,
				ID:     event.ID,
				Reason: reason,
			})
			continue
		}

		event.Normalize(c.limits)
		if event.ID == "" {
			generated, err := protocol.NewEventID()
			if err != nil {
				c.logger.Error("generate event id", "error", err)
				c.metrics.EventsRejected.Add(1)
				result.Rejected = append(result.Rejected, Rejection{
					Index:  index,
					Reason: "could not generate an event id",
				})
				continue
			}
			event.ID = generated
		}

		select {
		case c.buffer <- event:
			c.metrics.EventsAccepted.Add(1)
			result.Accepted++
		default:
			c.metrics.EventsDroppedBuffer.Add(1)
			result.Dropped++
		}
	}

	return result
}

// Buffered returns the number of events waiting to be written.
func (c *Collector) Buffered() int { return len(c.buffer) }

func (c *Collector) writeLoop(ctx context.Context) {
	defer close(c.stopped)

	batch := make([]protocol.Event, 0, c.batchSize)
	ticker := time.NewTicker(c.flushEvery)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		written, err := c.sink.WriteBatch(ctx, batch)
		if err != nil {
			c.metrics.BatchErrors.Add(1)
			c.logger.Error(
				"write batch failed; events are lost and counted",
				"events", len(batch),
				"error", err,
			)
		} else {
			c.metrics.EventsPersisted.Add(uint64(written))
			c.metrics.BatchesPersisted.Add(1)
		}
		batch = batch[:0]
	}

	for {
		select {
		case event, ok := <-c.buffer:
			if !ok {
				flush()
				return
			}
			batch = append(batch, event)
			if len(batch) >= c.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ctx.Done():
			flush()
			return
		}
	}
}
