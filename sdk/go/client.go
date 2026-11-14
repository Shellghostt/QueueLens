// Package queuelens provides best-effort, asynchronous worker instrumentation.
package queuelens

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shellghostt/queuelens/pkg/protocol"
)

type Options struct {
	URL, Token, WorkerID, Pool                 string
	BufferSize, BatchSize                      int
	FlushEvery, HeartbeatEvery, RequestTimeout time.Duration
}
type Stats struct {
	Emitted, Sent, Dropped, Rejected, Heartbeats, HeartbeatErrors uint64
	Buffered                                                      int
}
type queued struct {
	event       protocol.Event
	duration    int
	hasDuration bool
}
type Client struct {
	opts                                                          Options
	http                                                          *http.Client
	buffer                                                        chan queued
	stop, done                                                    chan struct{}
	ctx                                                           context.Context
	cancel                                                        context.CancelFunc
	closeOnce                                                     sync.Once
	closed                                                        atomic.Bool
	unavailable                                                   atomic.Bool
	emitted, sent, dropped, rejected, heartbeats, heartbeatErrors atomic.Uint64
}

func New(opts Options) (*Client, error) {
	u, err := url.Parse(opts.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("collector URL must be an HTTP(S) base URL without credentials, query or fragment")
	}
	if len(opts.Token) < 16 || strings.ContainsAny(opts.Token, "\r\n") {
		return nil, errors.New("token must contain at least 16 characters and no newlines")
	}
	opts.URL = strings.TrimRight(opts.URL, "/")
	if opts.WorkerID == "" {
		opts.WorkerID, err = protocol.NewEventID()
		if err != nil {
			return nil, err
		}
	}
	if err = (protocol.Heartbeat{V: 1, WorkerID: opts.WorkerID, Pool: opts.Pool}).Validate(); err != nil {
		return nil, err
	}
	if opts.BufferSize == 0 {
		opts.BufferSize = 10000
	}
	if opts.BatchSize == 0 {
		opts.BatchSize = 500
	}
	if opts.BufferSize < 1 || opts.BufferSize > 1000000 || opts.BatchSize < 1 || opts.BatchSize > 1000 {
		return nil, errors.New("invalid buffer or batch size")
	}
	if opts.FlushEvery == 0 {
		opts.FlushEvery = 250 * time.Millisecond
	}
	if opts.HeartbeatEvery == 0 {
		opts.HeartbeatEvery = 15 * time.Second
	}
	if opts.RequestTimeout == 0 {
		opts.RequestTimeout = 2 * time.Second
	}
	if opts.FlushEvery < 0 || opts.HeartbeatEvery < 0 || opts.RequestTimeout < 0 {
		return nil, errors.New("intervals must be positive")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{opts: opts, http: &http.Client{Timeout: opts.RequestTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}, buffer: make(chan queued, opts.BufferSize), stop: make(chan struct{}), done: make(chan struct{}), ctx: ctx, cancel: cancel}
	go c.run()
	return c, nil
}

// Emit copies metadata into a bounded channel without validation, I/O, or waiting.
// Stop producers before Close to guarantee a complete final drain.
func (c *Client) Emit(e protocol.Event) bool {
	c.emitted.Add(1)
	if c.closed.Load() || c.unavailable.Load() || len(c.buffer) == cap(c.buffer) {
		c.dropped.Add(1)
		return false
	}
	if e.V == 0 {
		e.V = 1
	}
	if e.TS.IsZero() {
		e.TS = time.Now().UTC()
	}
	if e.Attempt == 0 {
		e.Attempt = 1
	}
	if e.WorkerID == "" {
		e.WorkerID = c.opts.WorkerID
	}
	q := queued{event: e}
	if e.DurationMS != nil {
		q.duration = *e.DurationMS
		q.hasDuration = true
		q.event.DurationMS = nil
	}
	select {
	case c.buffer <- q:
		return true
	default:
		c.dropped.Add(1)
		return false
	}
}
func (c *Client) Stats() Stats {
	return Stats{Emitted: c.emitted.Load(), Sent: c.sent.Load(), Dropped: c.dropped.Load(), Rejected: c.rejected.Load(), Heartbeats: c.heartbeats.Load(), HeartbeatErrors: c.heartbeatErrors.Load(), Buffered: len(c.buffer)}
}
func (c *Client) Close(ctx context.Context) error {
	c.closeOnce.Do(func() { c.closed.Store(true); close(c.stop) })
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		c.cancel()
		return ctx.Err()
	}
}
func (c *Client) WritePrometheus(w io.Writer) error {
	_, err := fmt.Fprintf(w, "# TYPE queuelens_dropped_total counter\nqueuelens_dropped_total %d\n", c.dropped.Load())
	return err
}

func (c *Client) post(path string, payload any) ([]byte, int, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, err
	}
	if len(data) > 1<<20 {
		return nil, 0, errors.New("batch exceeds 1 MiB")
	}
	r, err := http.NewRequestWithContext(c.ctx, http.MethodPost, c.opts.URL+path, bytes.NewReader(data))
	if err != nil {
		return nil, 0, err
	}
	r.Header.Set("Authorization", "Bearer "+c.opts.Token)
	r.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(r)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err = io.ReadAll(io.LimitReader(resp.Body, 64<<10+1))
	if len(data) > 64<<10 {
		return nil, resp.StatusCode, errors.New("response too large")
	}
	return data, resp.StatusCode, err
}
func (c *Client) heartbeat() {
	_, status, err := c.post("/v1/workers/heartbeat", protocol.Heartbeat{V: 1, WorkerID: c.opts.WorkerID, Pool: c.opts.Pool})
	if err != nil || status != 202 {
		c.heartbeatErrors.Add(1)
	} else {
		c.heartbeats.Add(1)
	}
}
func (c *Client) run() {
	defer close(c.done)
	defer c.cancel()
	defer c.http.CloseIdleConnections()
	tick := time.NewTicker(c.opts.FlushEvery)
	defer tick.Stop()
	hb := time.NewTicker(c.opts.HeartbeatEvery)
	defer hb.Stop()
	batch := make([]protocol.Event, 0, c.opts.BatchSize)
	add := func(q queued) {
		e := q.event
		if q.hasDuration {
			d := q.duration
			e.DurationMS = &d
		}
		batch = append(batch, e)
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		data, status, err := c.post("/v1/events", batch)
		var ack struct {
			Accepted, Dropped int
			Rejected          []json.RawMessage
		}
		if err != nil || (status != 202 && status != 429 && status != 400) || json.Unmarshal(data, &ack) != nil || ack.Accepted < 0 || ack.Dropped < 0 || ack.Accepted+ack.Dropped+len(ack.Rejected) != len(batch) {
			c.dropped.Add(uint64(len(batch)))
			// Failed transport must not turn a dead collector into continuous
			// encoding/connection work. Drop new events until the next flush tick.
			c.unavailable.Store(true)
			for draining := true; draining; {
				select {
				case <-c.buffer:
					c.dropped.Add(1)
				default:
					draining = false
				}
			}
		} else {
			c.sent.Add(uint64(ack.Accepted))
			c.dropped.Add(uint64(ack.Dropped))
			c.rejected.Add(uint64(len(ack.Rejected)))
		}
		clear(batch)
		batch = batch[:0]
	}
	c.heartbeat()
	for {
		select {
		case q := <-c.buffer:
			if c.unavailable.Load() {
				c.dropped.Add(1)
				continue
			}
			add(q)
			if len(batch) >= c.opts.BatchSize {
				flush()
			}
		case <-tick.C:
			c.unavailable.Store(false)
			flush()
		case <-hb.C:
			c.heartbeat()
		case <-c.stop:
			for {
				select {
				case q := <-c.buffer:
					add(q)
					if len(batch) >= c.opts.BatchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}
		case <-c.ctx.Done():
			c.dropped.Add(uint64(len(batch)))
			for {
				select {
				case <-c.buffer:
					c.dropped.Add(1)
				default:
					return
				}
			}
		}
	}
}
