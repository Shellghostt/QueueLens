package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shellghostt/queuelens/internal/config"
	"github.com/shellghostt/queuelens/internal/dashboard"
	"github.com/shellghostt/queuelens/internal/queues"
)

const browserPassword = "m4-synthetic-viewer-password"
const browserToken = "m4-synthetic-admin-token"

type dashboardStub struct{ response dashboard.Response }

func (d dashboardStub) QueryDashboard(context.Context, string, dashboard.Query, *queues.Store, time.Time) (dashboard.Response, error) {
	return d.response, nil
}

func browserRequest(h http.Handler, method, path, origin, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://127.0.0.1:8080"+path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if cookie != nil {
		r.AddCookie(cookie)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestBrowserSessionReadOnlyOriginLogoutExpiryAndRestart(t *testing.T) {
	now := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)
	opts := Options{Token: browserToken, Now: func() time.Time { return now }, Dashboard: dashboardStub{},
		DashboardAccess: config.DashboardAccess{Password: browserPassword, Origin: "http://127.0.0.1:8080", InsecureDev: true}}
	h := New(opts)
	request := func(method, path, origin, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
		return browserRequest(h, method, path, origin, body, cookie)
	}
	if w := request("GET", "/dashboard/session", "", "", nil); w.Code != 200 || !strings.Contains(w.Body.String(), `"authenticated":false`) {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, origin := range []string{"", "https://attacker.example", "null"} {
		if w := request("POST", "/dashboard/session", origin, `{"password":"`+browserPassword+`"}`, nil); w.Code != 403 {
			t.Fatal("origin accepted", origin, w.Code)
		}
	}
	if w := request("POST", "/dashboard/session", opts.DashboardAccess.Origin, `{"password":"wrong-password-123"}`, nil); w.Code != 401 {
		t.Fatal(w.Code)
	}
	login := request("POST", "/dashboard/session", opts.DashboardAccess.Origin, `{"password":"`+browserPassword+`"}`, nil)
	if login.Code != 200 || len(login.Result().Cookies()) != 1 {
		t.Fatal(login.Code, login.Body.String())
	}
	cookie := login.Result().Cookies()[0]
	if !cookie.HttpOnly || cookie.Secure || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/" || cookie.Domain != "" || cookie.MaxAge != 14400 || len(cookie.Value) != 43 {
		t.Fatalf("dev cookie: %+v", cookie)
	}
	if w := request("GET", "/dashboard/api/overview", "", "", cookie); w.Code != 200 {
		t.Fatal(w.Code)
	}
	wrongHost := httptest.NewRequest("GET", "http://attacker.example/dashboard/api/overview", nil)
	wrongHost.AddCookie(cookie)
	wrongHostResult := httptest.NewRecorder()
	h.ServeHTTP(wrongHostResult, wrongHost)
	if wrongHostResult.Code != 403 {
		t.Fatal("wrong host was accepted", wrongHostResult.Code)
	}
	if w := request("GET", "/dashboard/api/overview", "https://attacker.example", "", cookie); w.Code != 403 || w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("cross origin read", w.Code)
	}
	for _, path := range []string{"/v1/queues", "/v1/dashboard/overview", "/v1/events", "/v1/workers/heartbeat"} {
		method := "GET"
		if strings.Contains(path, "events") || strings.Contains(path, "heartbeat") {
			method = "POST"
		}
		if w := request(method, path, opts.DashboardAccess.Origin, `{}`, cookie); w.Code != 401 {
			t.Fatal("viewer reached admin route", path, w.Code)
		}
	}
	for _, path := range []string{"/dashboard/api/events", "/dashboard/api/workers/heartbeat", "/dashboard/api/http://evil.example"} {
		if w := request("POST", path, opts.DashboardAccess.Origin, `{}`, cookie); w.Code == 200 || w.Code == 202 {
			t.Fatal("mutation proxy", path)
		}
	}
	if w := request("GET", "/dashboard/api/workers?limit=1&limit=2", "", "", cookie); w.Code != 400 {
		t.Fatal("worker duplicate bound", w.Code)
	}
	if w := browserRequest(New(opts), "GET", "/dashboard/api/overview", "", "", cookie); w.Code != 401 {
		t.Fatal("restart kept session", w.Code)
	}
	if w := request("DELETE", "/dashboard/session", opts.DashboardAccess.Origin, "", cookie); w.Code != 204 || w.Result().Cookies()[0].MaxAge != -1 {
		t.Fatal("logout", w.Code)
	}
	if w := request("GET", "/dashboard/api/overview", "", "", cookie); w.Code != 401 {
		t.Fatal("logout not revoked")
	}
	login = request("POST", "/dashboard/session", opts.DashboardAccess.Origin, `{"password":"`+browserPassword+`"}`, nil)
	cookie = login.Result().Cookies()[0]
	now = now.Add(sessionLifetime)
	if w := request("GET", "/dashboard/api/overview", "", "", cookie); w.Code != 401 {
		t.Fatal("fixed expiry not enforced")
	}
}

func TestBrowserSessionBoundsAndProductionCookie(t *testing.T) {
	access := config.DashboardAccess{Password: browserPassword, Origin: "https://queues.example"}
	b := newBrowserSessions(access, browserToken)
	if b == nil {
		t.Fatal("production configuration rejected")
	}
	cookie := b.cookie("test", time.Now().Add(sessionLifetime), false)
	if !cookie.Secure || cookie.Name != "__Host-queuelens_session" || cookie.Domain != "" || !cookie.HttpOnly {
		t.Fatal("production cookie policy")
	}
	now := time.Now()
	r := httptest.NewRequest("POST", "https://queues.example/dashboard/session", nil)
	for i := 0; i < loginAttempts; i++ {
		if !b.allowLogin(r, now) {
			t.Fatal("early login limit")
		}
	}
	if b.allowLogin(r, now) {
		t.Fatal("login attempt bound")
	}
	if !b.allowLogin(r, now.Add(loginWindow)) {
		t.Fatal("login window did not expire")
	}
	for i := 1; i < loginClientLimit; i++ {
		r.RemoteAddr = "client-" + strings.Repeat("x", i%32) + string(rune(i))
		if !b.allowLogin(r, now) {
			t.Fatal("early client bound", i)
		}
	}
	r.RemoteAddr = "one-more-client"
	if b.allowLogin(r, now) || len(b.clients) != loginClientLimit {
		t.Fatal("unbounded clients")
	}
	b.sessions = make(map[[32]byte]time.Time)
	for i := 0; i < sessionLimit; i++ {
		var key [32]byte
		key[0] = byte(i)
		b.sessions[key] = now.Add(sessionLifetime)
	}
	s := &Server{now: func() time.Time { return now }}
	r = httptest.NewRequest("POST", "https://queues.example/dashboard/session", strings.NewReader(`{"password":"`+browserPassword+`"}`))
	r.Header.Set("Origin", access.Origin)
	r.Header.Set("Content-Type", "application/json")
	r.RemoteAddr = "capacity-test-client"
	b.clients = make(map[[32]byte]loginClient)
	w := httptest.NewRecorder()
	s.loginBrowser(w, r, b)
	if w.Code != 503 || len(b.sessions) != sessionLimit {
		t.Fatal("session capacity not bounded", w.Code)
	}
	dev := config.DashboardAccess{Password: browserPassword, Origin: "http://127.0.0.1:8080", InsecureDev: true}
	h := New(Options{Token: browserToken, DashboardAccess: dev, Dashboard: dashboardStub{response: dashboard.Response{
		Broker: &queues.View{Error: strings.Repeat("x", dashboard.MaxResponseBytes)}}}})
	login := browserRequest(h, "POST", "/dashboard/session", dev.Origin, `{"password":"`+browserPassword+`"}`, nil)
	if w := browserRequest(h, "GET", "/dashboard/api/overview", "", "", login.Result().Cookies()[0]); w.Code != 503 || w.Body.Len() > 256 {
		t.Fatal("partial oversized success", w.Code, w.Body.Len())
	}
	for _, body := range []string{`{"password":"` + browserPassword + `","extra":1}`, `{"password":"` + browserPassword + `"}{}`, `{"password":"` + strings.Repeat("x", 2048) + `"}`} {
		if w := browserRequest(h, "POST", "/dashboard/session", dev.Origin, body, nil); w.Code != 400 {
			t.Fatal("login body bound", w.Code)
		}
	}
	if w := browserRequest(h, "GET", "/", "", "", nil); w.Code != 503 {
		t.Fatal("missing assets should be explicit", w.Code)
	}
}
