package queuelens

import (
	"context"
	"github.com/shellghostt/queuelens/pkg/protocol"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestCollectorRedirectDoesNotForwardToken(t *testing.T) {
	var forwarded atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { forwarded.Add(1); w.WriteHeader(202) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	c, err := New(Options{URL: source.URL, Token: "0123456789abcdef"})
	if err != nil {
		t.Fatal(err)
	}
	c.Emit(protocol.Event{Queue: "q", JobID: "j", State: protocol.StateStarted})
	_ = c.Close(context.Background())
	if forwarded.Load() != 0 || c.Stats().Dropped != 1 || c.Stats().HeartbeatErrors != 1 {
		t.Fatal(forwarded.Load(), c.Stats())
	}
}
