// Package loadgen is the ingest load generator.
//
// It exists to answer one question with numbers instead of adjectives: how
// many events per second can the collector accept, and what happens when it
// cannot. The result is reported, not guessed.
package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shellghostt/queuelens/pkg/protocol"
)

// Options configures a run.
type Options struct {
	// URL is the collector base URL, for example http://127.0.0.1:8080.
	URL string
	// Token is the bearer token.
	Token string
	// Rate is the target events per second. Zero means "as fast as possible".
	Rate int
	// Duration is how long to run. Zero means run until the context ends.
	Duration time.Duration
	// BatchSize is the number of events per request.
	BatchSize int
	// Workers is the number of concurrent senders.
	Workers int
	// Queue is the queue name used in the generated events.
	Queue string
	// Now is injectable for tests.
	Now func() time.Time
}

// Result is what a run produced.
type Result struct {
	Sent      int64         `json:"sent"`
	Accepted  int64         `json:"accepted"`
	Rejected  int64         `json:"rejected"`
	Dropped   int64         `json:"dropped"`
	Failed    int64         `json:"failed_requests"`
	Requests  int64         `json:"requests"`
	Duration  time.Duration `json:"duration"`
	Rate      float64       `json:"events_per_second"`
	P50       time.Duration `json:"p50_request"`
	P99       time.Duration `json:"p99_request"`
	Status429 int64         `json:"status_429"`
	// LastError is the most recent request failure, so that a run that
	// sent nothing says why instead of only showing zero.
	LastError string `json:"last_error,omitempty"`
}

// Run sends events until the duration elapses or the context is cancelled.
func Run(ctx context.Context, opts Options) (Result, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Queue == "" {
		opts.Queue = "loadgen"
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Duration)
		defer cancel()
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        opts.Workers * 2,
			MaxIdleConnsPerHost: opts.Workers * 2,
			MaxConnsPerHost:     opts.Workers * 2,
		},
	}

	var (
		sent      atomic.Int64
		accepted  atomic.Int64
		rejected  atomic.Int64
		dropped   atomic.Int64
		failed    atomic.Int64
		requests  atomic.Int64
		status429 atomic.Int64
		lastError atomic.Pointer[string]
	)

	latencies := make([][]time.Duration, opts.Workers)
	var wg sync.WaitGroup

	start := opts.Now()

	for worker := 0; worker < opts.Workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			samples := make([]time.Duration, 0, 1024)
			perWorkerRate := 0
			if opts.Rate > 0 {
				perWorkerRate = opts.Rate / opts.Workers
				if perWorkerRate < 1 {
					perWorkerRate = 1
				}
			}

			// The pacing is open loop: every worker has a deadline and
			// advances it by exactly one interval per batch. Sleeping for a
			// fixed duration after every request would drift, and the
			// measured rate would always be below the target.
			next := time.Now()
			interval := time.Duration(0)
			if perWorkerRate > 0 {
				perBatch := time.Duration(opts.BatchSize)
				interval = time.Duration(float64(perBatch) / float64(perWorkerRate) * float64(time.Second))
			}

			for {
				if ctx.Err() != nil {
					latencies[worker] = samples
					return
				}

				batch := makeBatch(opts, worker)
				payload, err := json.Marshal(batch)
				if err != nil {
					failed.Add(1)
					continue
				}

				requestStart := time.Now()
				acceptedBatch, rejectedBatch, droppedBatch, tooMany, err := post(ctx, client, opts, payload)
				requestDuration := time.Since(requestStart)
				samples = append(samples, requestDuration)
				requests.Add(1)

				if err != nil {
					failed.Add(1)
					message := err.Error()
					lastError.Store(&message)
				} else {
					sent.Add(int64(len(batch)))
					accepted.Add(acceptedBatch)
					rejected.Add(rejectedBatch)
					dropped.Add(droppedBatch)
					if tooMany {
						status429.Add(1)
					}
				}

				if interval > 0 {
					next = next.Add(interval)
					if wait := time.Until(next); wait > 0 {
						timer := time.NewTimer(wait)
						select {
						case <-timer.C:
						case <-ctx.Done():
							timer.Stop()
							latencies[worker] = samples
							return
						}
					}
				}
			}
		}(worker)
	}

	wg.Wait()

	elapsed := time.Since(start)
	result := Result{
		Sent:      sent.Load(),
		Accepted:  accepted.Load(),
		Rejected:  rejected.Load(),
		Dropped:   dropped.Load(),
		Failed:    failed.Load(),
		Requests:  requests.Load(),
		Duration:  elapsed,
		Status429: status429.Load(),
	}
	if elapsed > 0 {
		result.Rate = float64(result.Sent) / elapsed.Seconds()
	}

	all := make([]time.Duration, 0, 1024)
	for _, samples := range latencies {
		all = append(all, samples...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	result.P50 = percentile(all, 0.50)
	result.P99 = percentile(all, 0.99)

	if pointer := lastError.Load(); pointer != nil {
		result.LastError = *pointer
	}

	return result, nil
}

func makeBatch(opts Options, worker int) []protocol.Event {
	batch := make([]protocol.Event, 0, opts.BatchSize)
	now := opts.Now().UTC()
	for i := 0; i < opts.BatchSize; i++ {
		id, err := protocol.NewEventID()
		if err != nil {
			id = fmt.Sprintf("loadgen-%d-%d-%d", now.UnixNano(), worker, i)
		}
		batch = append(batch, protocol.Event{
			V:       protocol.Version,
			ID:      id,
			TS:      now,
			Queue:   opts.Queue,
			JobID:   fmt.Sprintf("job-%d-%d", worker, i),
			JobName: "load-generator",
			Attempt: 1,
			State:   protocol.StateEnqueued,
		})
	}
	return batch
}

func post(
	ctx context.Context,
	client *http.Client,
	opts Options,
	payload []byte,
) (accepted int64, rejected int64, dropped int64, tooMany bool, err error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, opts.URL+"/v1/events", bytes.NewReader(payload))
	if err != nil {
		return 0, 0, 0, false, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+opts.Token)

	response, err := client.Do(request)
	if err != nil {
		return 0, 0, 0, false, err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return 0, 0, 0, response.StatusCode == http.StatusTooManyRequests, err
	}

	var decoded struct {
		Accepted int64 `json:"accepted"`
		Rejected []struct {
			Index  int    `json:"index"`
			Reason string `json:"reason"`
		} `json:"rejected"`
		Dropped int64 `json:"dropped"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return 0, 0, 0, response.StatusCode == http.StatusTooManyRequests, fmt.Errorf("decode response: %w", err)
	}
	return decoded.Accepted, int64(len(decoded.Rejected)), decoded.Dropped, response.StatusCode == http.StatusTooManyRequests, nil
}

func percentile(sorted []time.Duration, fraction float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(fraction * float64(len(sorted)-1))
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}
