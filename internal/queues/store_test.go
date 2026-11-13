package queues

import (
	"errors"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/adapter"
)

func TestApplyStoresTheCountsAndComputesTheDepth(t *testing.T) {
	store := NewStore()
	at := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)

	store.Apply("bullmq", []adapter.Counts{
		{Queue: "emails", Waiting: 10, Active: 2, Completed: 5, Failed: 1, Delayed: 3, Paused: 1, Prioritized: 4},
	}, at)

	views := store.List()
	if len(views) != 1 {
		t.Fatalf("expected one queue, got %d", len(views))
	}
	view := views[0]
	if view.Queue != "emails" {
		t.Fatalf("expected emails, got %q", view.Queue)
	}
	if view.Depth != 18 {
		t.Fatalf("expected a depth of 18 (10+3+1+4), got %d", view.Depth)
	}
	if view.Stale {
		t.Fatal("a fresh view must not be stale")
	}
	if !view.UpdatedAt.Equal(at) {
		t.Fatalf("expected the update time %s, got %s", at, view.UpdatedAt)
	}
}

func TestListIsSortedByQueueName(t *testing.T) {
	store := NewStore()
	store.Apply("bullmq", []adapter.Counts{
		{Queue: "reports"},
		{Queue: "emails"},
		{Queue: "audits"},
	}, time.Now())

	views := store.List()
	if len(views) != 3 {
		t.Fatalf("expected three queues, got %d", len(views))
	}
	if views[0].Queue != "audits" || views[1].Queue != "emails" || views[2].Queue != "reports" {
		t.Fatalf("expected alphabetical order, got %v", views)
	}
}

func TestAQueueThatDisappearsIsRemoved(t *testing.T) {
	store := NewStore()
	store.Apply("bullmq", []adapter.Counts{{Queue: "emails"}, {Queue: "reports"}}, time.Now())

	store.Apply("bullmq", []adapter.Counts{{Queue: "emails"}}, time.Now())

	views := store.List()
	if len(views) != 1 || views[0].Queue != "emails" {
		t.Fatalf("expected only emails to remain, got %v", views)
	}
}

func TestAFailedPollKeepsTheLastKnownStateAndMarksItStale(t *testing.T) {
	store := NewStore()
	store.Apply("bullmq", []adapter.Counts{{Queue: "emails", Waiting: 7}}, time.Now())

	store.MarkFailure("bullmq", errors.New("connection refused"))

	views := store.List()
	if len(views) != 1 {
		t.Fatalf("expected the queue to survive the failure, got %d queues", len(views))
	}
	if !views[0].Stale {
		t.Fatal("expected the view to be marked stale")
	}
	if views[0].Waiting != 7 {
		t.Fatalf("expected the last known count to survive, got %d", views[0].Waiting)
	}
	if views[0].Error == "" {
		t.Fatal("expected the error to be reported on the view")
	}

	errorsByAdapter := store.Errors()
	if errorsByAdapter["bullmq"] == "" {
		t.Fatalf("expected the adapter error to be recorded, got %v", errorsByAdapter)
	}
}

func TestASuccessfulPollClearsTheAdapterError(t *testing.T) {
	store := NewStore()
	store.MarkFailure("bullmq", errors.New("connection refused"))
	store.Apply("bullmq", []adapter.Counts{{Queue: "emails"}}, time.Now())

	if len(store.Errors()) != 0 {
		t.Fatalf("expected the adapter error to be cleared, got %v", store.Errors())
	}
}
