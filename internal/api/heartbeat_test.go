package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/shellghostt/queuelens/pkg/protocol"
)

func TestHeartbeatValidationAuthenticationAndPersistence(t *testing.T) {
	calls := 0
	handler := New(Options{Token: testToken, Heartbeat: func(ctx context.Context, h protocol.Heartbeat) error {
		calls++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("no deadline")
		}
		if h.WorkerID != "w" {
			return errors.New("failure")
		}
		return nil
	}})
	for _, tc := range []struct {
		body, token string
		status      int
	}{
		{`{"v":1,"worker_id":"w"}`, testToken, 202},
		{`{"v":1,"worker_id":"w"}`, "wrong", 401},
		{`{"v":2,"worker_id":"w"}`, testToken, 400},
		{`{"v":1,"worker_id":""}`, testToken, 400},
		{`{"v":1,"worker_id":"w"} {}`, testToken, 400},
		{`{"v":1,"worker_id":"fail"}`, testToken, 503},
	} {
		r := httptest.NewRequest("POST", "/v1/workers/heartbeat", strings.NewReader(tc.body))
		r.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s: got %d body %s", tc.body, w.Code, w.Body.String())
		}
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}
