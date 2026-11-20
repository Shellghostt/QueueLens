// Package queues keeps the last known state of every queue.
//
// The state is in memory on purpose: it is a live view of the broker, not
// history. The durable history lives in the event table.
package queues

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/shellghostt/queuelens/internal/adapter"
)

// View is one queue as the API reports it.
type View struct {
	Queue       string    `json:"queue"`
	Waiting     int64     `json:"waiting"`
	Active      int64     `json:"active"`
	Completed   int64     `json:"completed"`
	Failed      int64     `json:"failed"`
	Delayed     int64     `json:"delayed"`
	Paused      int64     `json:"paused"`
	Prioritized int64     `json:"prioritized"`
	Depth       int64     `json:"depth"`
	UpdatedAt   time.Time `json:"updated_at"`
	Stale       bool      `json:"stale"`
	Error       string    `json:"error,omitempty"`
}

// Store holds the last poll result of every adapter.
type Store struct {
	mu    sync.RWMutex
	views map[string]View
	// errors holds the last error per adapter name.
	errors map[string]string
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{
		views:  map[string]View{},
		errors: map[string]string{},
	}
}

// Apply replaces the state of the queues an adapter reported. Queues the
// adapter no longer reports are removed, because a deleted queue must not
// linger in the API.
func (s *Store) Apply(adapterName string, counts []adapter.Counts, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	seen := map[string]bool{}
	for _, entry := range counts {
		seen[entry.Queue] = true
		s.views[entry.Queue] = View{
			Queue:       entry.Queue,
			Waiting:     entry.Waiting,
			Active:      entry.Active,
			Completed:   entry.Completed,
			Failed:      entry.Failed,
			Delayed:     entry.Delayed,
			Paused:      entry.Paused,
			Prioritized: entry.Prioritized,
			Depth:       entry.Depth(),
			UpdatedAt:   at.UTC(),
			Stale:       false,
		}
	}

	for queue := range s.views {
		if !seen[queue] {
			delete(s.views, queue)
		}
	}
	delete(s.errors, adapterName)
}

// MarkFailure records that an adapter could not be polled. The last known
// state stays available and is marked stale, because a broken broker must not
// look like an empty queue.
func (s *Store) MarkFailure(adapterName string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	message := ""
	if err != nil {
		message = err.Error()
	}
	s.errors[adapterName] = message
	for queue, view := range s.views {
		view.Stale = true
		if message != "" {
			view.Error = message
		}
		s.views[queue] = view
	}
}

// List returns the queues, sorted by name.
func (s *Store) List() []View {
	s.mu.RLock()
	defer s.mu.RUnlock()

	views := make([]View, 0, len(s.views))
	for _, view := range s.views {
		views = append(views, view)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].Queue < views[j].Queue })
	return views
}

// Page visits at most 10,000 views and retains only limit+1 ordered candidates.
// Go string order compares UTF-8 bytes, matching PostgreSQL COLLATE "C".
func (s *Store) Page(after string, limit int) ([]View, map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if limit < 1 || limit > 200 || len(s.views) > 10000 || len(s.errors) > 64 {
		return nil, nil, errors.New("bounded broker page unavailable")
	}
	views := make([]View, 0, limit+1)
	for name, view := range s.views {
		if name <= after {
			continue
		}
		index := sort.Search(len(views), func(i int) bool { return views[i].Queue >= name })
		if index > limit {
			continue
		}
		if len(views) < limit+1 {
			views = append(views, View{})
		}
		copy(views[index+1:], views[index:])
		views[index] = view
	}
	errorsCopy := make(map[string]string, len(s.errors))
	for name, message := range s.errors {
		errorsCopy[name] = message
	}
	return views, errorsCopy, nil
}

func (s *Store) Get(name string) *View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	view, found := s.views[name]
	if !found {
		return nil
	}
	return &view
}

// Errors returns the last error of every adapter that failed.
func (s *Store) Errors() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]string, len(s.errors))
	for name, message := range s.errors {
		out[name] = message
	}
	return out
}
