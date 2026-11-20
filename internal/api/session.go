package api

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/shellghostt/queuelens/internal/config"
)

const sessionLifetime = 4 * time.Hour
const sessionLimit = 256
const loginClientLimit = 1024
const loginWindow = 10 * time.Minute
const loginAttempts = 20

type loginClient struct {
	Until    time.Time
	Attempts int
}

type browserSessions struct {
	mu                       sync.Mutex
	password                 [32]byte
	origin, host, cookieName string
	secure                   bool
	sessions                 map[[32]byte]time.Time
	clients                  map[[32]byte]loginClient
}

func newBrowserSessions(access config.DashboardAccess, token string) *browserSessions {
	if access.Password == "" || access.Validate(token) != nil {
		return nil
	}
	u, _ := url.Parse(access.Origin)
	name := "__Host-queuelens_session"
	if access.InsecureDev {
		name = "queuelens_dev_session"
	}
	return &browserSessions{password: sha256.Sum256([]byte(access.Password)), origin: access.Origin,
		host: u.Host, cookieName: name, secure: !access.InsecureDev,
		sessions: make(map[[32]byte]time.Time), clients: make(map[[32]byte]loginClient)}
}

func (b *browserSessions) sameOrigin(r *http.Request, mutation bool) bool {
	if r.Host != b.host {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin != "" && origin != b.origin || mutation && origin != b.origin {
		return false
	}
	site := r.Header.Get("Sec-Fetch-Site")
	return site == "" || site == "same-origin" || site == "none"
}

func (b *browserSessions) cookie(value string, until time.Time, clear bool) *http.Cookie {
	c := &http.Cookie{Name: b.cookieName, Value: value, Path: "/", HttpOnly: true,
		Secure: b.secure, SameSite: http.SameSiteStrictMode, Expires: until, MaxAge: int(sessionLifetime.Seconds())}
	if clear {
		c.MaxAge = -1
		c.Expires = time.Unix(1, 0)
	}
	return c
}

func (b *browserSessions) identity(r *http.Request) ([32]byte, bool) {
	c, err := r.Cookie(b.cookieName)
	if err != nil || len(c.Value) != 43 {
		return [32]byte{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(c.Value)
	if err != nil || len(raw) != 32 {
		return [32]byte{}, false
	}
	return sha256.Sum256([]byte(c.Value)), true
}

func (b *browserSessions) authenticated(r *http.Request, now time.Time) (time.Time, bool) {
	id, ok := b.identity(r)
	if !ok {
		return time.Time{}, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	until, ok := b.sessions[id]
	if ok && !until.After(now) {
		delete(b.sessions, id)
		ok = false
	}
	return until, ok
}

func (b *browserSessions) allowLogin(r *http.Request, now time.Time) bool {
	client, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		client = r.RemoteAddr
	}
	id := sha256.Sum256([]byte(client))
	b.mu.Lock()
	defer b.mu.Unlock()
	for key, value := range b.clients {
		if !value.Until.After(now) {
			delete(b.clients, key)
		}
	}
	value, exists := b.clients[id]
	if !exists {
		if len(b.clients) >= loginClientLimit {
			return false
		}
		value.Until = now.Add(loginWindow)
	}
	if value.Attempts >= loginAttempts {
		return false
	}
	value.Attempts++
	b.clients[id] = value
	return true
}

func (s *Server) registerBrowser(router chi.Router, access config.DashboardAccess, assets fs.FS) {
	b := newBrowserSessions(access, s.token)
	if b == nil {
		return
	}
	router.Get("/dashboard/session", func(w http.ResponseWriter, r *http.Request) {
		if !b.sameOrigin(r, false) || r.URL.RawQuery != "" {
			writeJSON(w, 403, map[string]string{"error": "dashboard origin rejected"})
			return
		}
		until, authenticated := b.authenticated(r, s.now())
		var expiry *time.Time
		if authenticated {
			expiry = &until
		}
		writeDashboardJSON(w, struct {
			Authenticated bool       `json:"authenticated"`
			ExpiresAt     *time.Time `json:"expires_at"`
		}{authenticated, expiry})
	})
	router.Post("/dashboard/session", func(w http.ResponseWriter, r *http.Request) { s.loginBrowser(w, r, b) })
	router.Delete("/dashboard/session", func(w http.ResponseWriter, r *http.Request) {
		if !b.sameOrigin(r, true) || r.URL.RawQuery != "" {
			writeJSON(w, 403, map[string]string{"error": "dashboard origin rejected"})
			return
		}
		if id, ok := b.identity(r); ok {
			b.mu.Lock()
			delete(b.sessions, id)
			b.mu.Unlock()
		}
		http.SetCookie(w, b.cookie("", time.Time{}, true))
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusNoContent)
	})
	for _, view := range []string{"overview", "queues", "queue", "jobs", "job", "workers"} {
		handler := s.handleDashboard(view)
		if view == "workers" {
			handler = s.handleDashboardWorkers
		}
		router.Get("/dashboard/api/"+view, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			if !b.sameOrigin(r, false) {
				writeJSON(w, 403, map[string]string{"error": "dashboard origin rejected"})
				return
			}
			if _, ok := b.authenticated(r, s.now()); !ok {
				writeJSON(w, 401, map[string]string{"error": "dashboard session required"})
				return
			}
			handler(w, r)
		})
	}
	assetHandler := func(w http.ResponseWriter, r *http.Request) {
		if !b.sameOrigin(r, false) {
			http.Error(w, "dashboard origin rejected", http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "no-store")
		if assets == nil {
			http.Error(w, "dashboard assets unavailable; build with make build", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/" {
			body, err := fs.ReadFile(assets, "index.html")
			if err != nil {
				http.Error(w, "dashboard assets unavailable", http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(body)
			return
		}
		// Hashed production assets are immutable. No directory listings or arbitrary
		// server paths are exposed; only Vite's assets subtree is mounted.
		path := strings.TrimPrefix(r.URL.Path, "/")
		entry, err := fs.Stat(assets, path)
		if err != nil || entry.IsDir() {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		http.FileServer(http.FS(assets)).ServeHTTP(w, r)
	}
	router.Get("/", assetHandler)
	router.Get("/assets/*", assetHandler)
}

func (s *Server) loginBrowser(w http.ResponseWriter, r *http.Request, b *browserSessions) {
	w.Header().Set("Cache-Control", "no-store")
	if !b.sameOrigin(r, true) || r.URL.RawQuery != "" {
		writeJSON(w, 403, map[string]string{"error": "dashboard origin rejected"})
		return
	}
	if !b.allowLogin(r, s.now()) {
		w.Header().Set("Retry-After", "600")
		writeJSON(w, 429, map[string]string{"error": "dashboard login rate limited"})
		return
	}
	contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" {
		writeJSON(w, 415, map[string]string{"error": "JSON credentials required"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var credentials struct {
		Password string `json:"password"`
	}
	err = decoder.Decode(&credentials)
	if err == nil {
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			err = errors.New("extra login JSON")
		}
	}
	if err != nil || len(credentials.Password) < 12 || len(credentials.Password) > 256 {
		writeJSON(w, 400, map[string]string{"error": "invalid bounded login input"})
		return
	}
	digest := sha256.Sum256([]byte(credentials.Password))
	if subtle.ConstantTimeCompare(digest[:], b.password[:]) != 1 {
		writeJSON(w, 401, map[string]string{"error": "invalid dashboard credentials"})
		return
	}
	var random [32]byte
	if _, err = rand.Read(random[:]); err != nil {
		writeJSON(w, 503, map[string]string{"error": "dashboard session unavailable"})
		return
	}
	value := base64.RawURLEncoding.EncodeToString(random[:])
	id, now := sha256.Sum256([]byte(value)), s.now()
	until := now.Add(sessionLifetime)
	b.mu.Lock()
	for key, expiry := range b.sessions {
		if !expiry.After(now) {
			delete(b.sessions, key)
		}
	}
	full := len(b.sessions) >= sessionLimit
	if !full {
		b.sessions[id] = until
	}
	b.mu.Unlock()
	if full {
		writeJSON(w, 503, map[string]string{"error": "dashboard session capacity reached"})
		return
	}
	http.SetCookie(w, b.cookie(value, until, false))
	writeDashboardJSON(w, struct {
		Authenticated bool      `json:"authenticated"`
		ExpiresAt     time.Time `json:"expires_at"`
	}{true, until})
}

func (s *Server) handleDashboardWorkers(w http.ResponseWriter, r *http.Request) {
	if _, err := strictQuery(r, "limit", "cursor", "status"); err != nil {
		s.dashboardError(w, err)
		return
	}
	query, err := pageQuery(r)
	if err != nil {
		s.dashboardError(w, err)
		return
	}
	if s.lifecycle == nil {
		s.dashboardError(w, errors.New("worker reader unavailable"))
		return
	}
	page, err := s.lifecycle.QueryWorkers(r.Context(), query, s.now())
	if err != nil {
		s.dashboardError(w, err)
		return
	}
	writeDashboardJSON(w, page)
}
