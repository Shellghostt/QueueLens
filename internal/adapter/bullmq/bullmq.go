// Package bullmq reads queue state from a Redis server that holds BullMQ
// queues.
//
// BullMQ is a Node library, but its state is plain Redis: a list per active
// and waiting queue, a sorted set per completed, failed, delayed and
// prioritised queue, and a `meta` hash that marks the queue as existing.
// The adapter reads those keys directly, with SCAN rather than KEYS, so a
// large Redis instance is not blocked.
//
// The key layout was verified against BullMQ 6.7.6 by the integration test in
// tests/integration, which seeds a real queue with the real library.
package bullmq

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/shellghostt/queuelens/internal/adapter"
)

// DefaultPrefix is BullMQ's default key prefix.
const DefaultPrefix = "bull"

// Adapter reads BullMQ queues from Redis.
type Adapter struct {
	client *redis.Client
	prefix string
}

// Options configures the adapter.
type Options struct {
	// Prefix is the BullMQ key prefix. Empty means the BullMQ default.
	Prefix string
}

// New creates an adapter for an existing Redis client.
func New(client *redis.Client, opts Options) *Adapter {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return &Adapter{client: client, prefix: prefix}
}

// Name implements adapter.Adapter.
func (a *Adapter) Name() string { return "bullmq" }

// Poll implements adapter.Adapter.
func (a *Adapter) Poll(ctx context.Context) ([]adapter.Counts, error) {
	names, err := a.discover(ctx)
	if err != nil {
		return nil, err
	}

	counts := make([]adapter.Counts, 0, len(names))
	for _, name := range names {
		queueCounts, err := a.counts(ctx, name)
		if err != nil {
			return nil, err
		}
		counts = append(counts, queueCounts)
	}
	return counts, nil
}

// discover finds the queues by their meta key, using SCAN so the server is
// never blocked by KEYS.
func (a *Adapter) discover(ctx context.Context) ([]string, error) {
	pattern := a.prefix + ":*:meta"
	seen := map[string]bool{}
	var cursor uint64

	for {
		keys, next, err := a.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan for bullmq queues: %w", err)
		}
		for _, key := range keys {
			name := strings.TrimSuffix(strings.TrimPrefix(key, a.prefix+":"), ":meta")
			if name != "" {
				seen[name] = true
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (a *Adapter) counts(ctx context.Context, queue string) (adapter.Counts, error) {
	key := func(suffix string) string {
		return a.prefix + ":" + queue + ":" + suffix
	}

	pipe := a.client.Pipeline()
	waiting := pipe.LLen(ctx, key("wait"))
	active := pipe.LLen(ctx, key("active"))
	paused := pipe.LLen(ctx, key("paused"))
	completed := pipe.ZCard(ctx, key("completed"))
	failed := pipe.ZCard(ctx, key("failed"))
	delayed := pipe.ZCard(ctx, key("delayed"))
	prioritized := pipe.ZCard(ctx, key("prioritized"))

	if _, err := pipe.Exec(ctx); err != nil {
		return adapter.Counts{}, fmt.Errorf("read counts for queue %q: %w", queue, err)
	}

	return adapter.Counts{
		Queue:       queue,
		Waiting:     waiting.Val(),
		Active:      active.Val(),
		Completed:   completed.Val(),
		Failed:      failed.Val(),
		Delayed:     delayed.Val(),
		Paused:      paused.Val(),
		Prioritized: prioritized.Val(),
	}, nil
}
