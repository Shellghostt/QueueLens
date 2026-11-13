// Package adapter defines the interface every queue adapter implements.
//
// An adapter translates one broker's state into protocol events or queue
// counts. It must never block the collector and must never apply business
// logic: what a count means is decided by the protocol and the rules, not by
// the adapter.
package adapter

import "context"

// Counts is the state of one queue at one moment.
type Counts struct {
	Queue       string `json:"queue"`
	Waiting     int64  `json:"waiting"`
	Active      int64  `json:"active"`
	Completed   int64  `json:"completed"`
	Failed      int64  `json:"failed"`
	Delayed     int64  `json:"delayed"`
	Paused      int64  `json:"paused"`
	Prioritized int64  `json:"prioritized"`
}

// Depth is how many jobs are not being worked on right now: waiting,
// delayed, paused and prioritised jobs.
func (c Counts) Depth() int64 {
	return c.Waiting + c.Delayed + c.Paused + c.Prioritized
}

// Adapter reads one queue system.
type Adapter interface {
	// Name identifies the adapter in logs and metrics.
	Name() string
	// Poll returns the current state of every queue it knows about.
	//
	// It returns an error when the broker cannot be reached. The caller keeps
	// the last known state and records the failure; an adapter error must
	// never take the collector down.
	Poll(ctx context.Context) ([]Counts, error)
}
