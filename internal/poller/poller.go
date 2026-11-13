// Package poller drives the adapters on a fixed interval.
//
// A failing adapter is isolated: the error is counted and logged, the last
// known queue state is kept and marked stale, and the poller keeps running.
package poller

import (
	"context"
	"log/slog"
	"time"

	"github.com/shellghostt/queuelens/internal/adapter"
	"github.com/shellghostt/queuelens/internal/metrics"
	"github.com/shellghostt/queuelens/internal/queues"
)

// Poller polls every adapter and updates the queue store.
type Poller struct {
	adapters []adapter.Adapter
	store    *queues.Store
	metrics  *metrics.Metrics
	every    time.Duration
	logger   *slog.Logger
	now      func() time.Time
}

// Options configures a poller.
type Options struct {
	Every  time.Duration
	Logger *slog.Logger
	Now    func() time.Time
}

// New creates a poller.
func New(adapters []adapter.Adapter, store *queues.Store, m *metrics.Metrics, opts Options) *Poller {
	if opts.Every <= 0 {
		opts.Every = 5 * time.Second
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if m == nil {
		m = metrics.New()
	}
	return &Poller{
		adapters: adapters,
		store:    store,
		metrics:  m,
		every:    opts.Every,
		logger:   opts.Logger,
		now:      opts.Now,
	}
}

// Run polls until the context is cancelled. It polls once immediately, so
// the API has data before the first interval elapses.
func (p *Poller) Run(ctx context.Context) {
	p.PollOnce(ctx)

	ticker := time.NewTicker(p.every)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.PollOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// PollOnce polls every adapter once.
func (p *Poller) PollOnce(ctx context.Context) {
	for _, item := range p.adapters {
		p.metrics.AdapterPolls.Add(1)
		counts, err := item.Poll(ctx)
		if err != nil {
			p.metrics.AdapterPollErrors.Add(1)
			p.logger.Error("adapter poll failed", "adapter", item.Name(), "error", err)
			p.store.MarkFailure(item.Name(), err)
			continue
		}
		p.store.Apply(item.Name(), counts, p.now())
	}
}
