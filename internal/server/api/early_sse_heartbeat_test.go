package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestParseEarlyHeartbeatModels(t *testing.T) {
	if parseEarlyHeartbeatModels("") != nil {
		t.Fatal("empty configuration must disable the feature")
	}

	if parseEarlyHeartbeatModels("  ,  ,") != nil {
		t.Fatal("blank entries only must disable the feature")
	}

	matcher := parseEarlyHeartbeatModels("a, b*, ")
	if matcher == nil {
		t.Fatal("expected a matcher")
	}

	if len(matcher.exact) != 1 || len(matcher.prefixes) != 1 {
		t.Fatalf("unexpected matcher contents: %+v", matcher)
	}
}

func TestEarlyHeartbeatMatcher(t *testing.T) {
	cases := []struct {
		name  string
		raw   string
		model string
		want  bool
	}{
		{"disabled", "", "any-model", false},
		{"exact hit", "[anyrouter]/claude-opus-5-5", "[anyrouter]/claude-opus-5-5", true},
		{"exact miss", "[anyrouter]/claude-opus-5-5", "gpt-4o", false},
		{"case insensitive", "[AnyRouter]/Claude", "[anyrouter]/claude", true},
		{"surrounding spaces", " gpt-4o ", "gpt-4o", true},
		{"multiple entries", "a,b,c", "b", true},
		{"prefix wildcard", "[anyrouter]/*", "[anyrouter]/claude-opus-5-5", true},
		{"prefix wildcard miss", "[anyrouter]/*", "[openai]/gpt-4o", false},
		{"match all", "*", "whatever", true},
		{"empty model never matches", "*", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseEarlyHeartbeatModels(tc.raw).matches(tc.model); got != tc.want {
				t.Fatalf("matches(%q) = %v, want %v", tc.model, got, tc.want)
			}
		})
	}
}

func TestEarlyHeartbeatMatcherCacheFollowsConfig(t *testing.T) {
	first := earlyHeartbeatMatcherFor("model-a")
	if !first.matches("model-a") {
		t.Fatal("expected model-a to match")
	}

	if earlyHeartbeatMatcherFor("model-a") != first {
		t.Fatal("expected the cached matcher to be reused")
	}

	second := earlyHeartbeatMatcherFor("model-b")
	if second == first || second.matches("model-a") {
		t.Fatal("expected the cache to follow the configuration")
	}

	if earlyHeartbeatMatcherFor("") != nil {
		t.Fatal("expected an empty configuration to disable the feature")
	}
}

func TestEarlySSEHeartbeatNilIsSafe(t *testing.T) {
	var hb *earlySSEHeartbeat

	hb.Stop()

	if hb.Started() {
		t.Fatal("a nil heartbeat must not report as started")
	}

	if hb.Format() != sseHeartbeatNone {
		t.Fatal("a nil heartbeat must not report a format")
	}
}

// The watchdog must stay completely passive until the first tick, so requests
// that finish quickly keep their real HTTP status code.
func TestEarlySSEHeartbeatStaysPassiveBeforeFirstTick(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	hb := &earlySSEHeartbeat{
		c:        c,
		format:   sseHeartbeatAnthropic,
		interval: time.Hour,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go hb.run(c.Request.Context())

	hb.Stop()

	if hb.Started() {
		t.Fatal("headers must not be committed before the first tick")
	}

	if recorder.Body.Len() != 0 {
		t.Fatalf("nothing should have been written, got %q", recorder.Body.String())
	}
}

// Once the first tick fires the headers are committed and heartbeats flow.
func TestEarlySSEHeartbeatWritesAfterFirstTick(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	hb := &earlySSEHeartbeat{
		c:        c,
		format:   sseHeartbeatAnthropic,
		interval: 5 * time.Millisecond,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go hb.run(c.Request.Context())

	deadline := time.Now().Add(2 * time.Second)
	for !hb.Started() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	hb.Stop()

	if !hb.Started() {
		t.Fatal("expected the headers to be committed after the first tick")
	}

	if got := recorder.Header().Get("Content-Type"); got == "" {
		t.Fatal("expected SSE headers to be set")
	}

	if body := recorder.Body.String(); body == "" {
		t.Fatal("expected at least one heartbeat to be written")
	}
}

func TestCompactJSONOrFallback(t *testing.T) {
	compacted := compactJSONOrFallback([]byte("{\n  \"a\": 1\n}"), sseHeartbeatAnthropic)
	if string(compacted) != `{"a":1}` {
		t.Fatalf("unexpected compacted payload: %s", compacted)
	}

	for _, format := range []sseHeartbeatFormat{sseHeartbeatOpenAI, sseHeartbeatAnthropic} {
		fallback := compactJSONOrFallback([]byte("not json"), format)
		if !json.Valid(fallback) {
			t.Fatalf("fallback payload must be valid JSON, got %s", fallback)
		}
	}

	if !json.Valid(compactJSONOrFallback(nil, sseHeartbeatOpenAI)) {
		t.Fatal("empty body must fall back to valid JSON")
	}
}
