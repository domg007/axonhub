package api

// Early SSE heartbeat (local fork feature).
//
// Problem: for slow upstreams the orchestrator can block inside Process() for
// minutes before a single byte reaches the client. Proxies in front of AxonHub
// (Cloudflare, nginx, ...) close such connections with a gateway timeout even
// though sse_keep_alive is enabled, because the stock heartbeat only starts
// after Process() returns.
//
// Fix: for an explicit allow list of downstream model IDs, start a watchdog
// before calling Process(). The watchdog stays completely passive until the
// first tick (sse_keep_alive.interval). Only if Process() is still running by
// then does it flush the SSE response headers and begin emitting heartbeats.
//
// The delay matters: requests that fail fast (invalid key, quota, rate limit,
// no available channel) return long before the first tick, so they keep the
// stock behaviour including their real HTTP status code, client-side retries
// and access-log accuracy. Only requests that are already slow enough to risk
// a proxy timeout pay the trade-off below.
//
// Trade-off: once the headers are flushed the HTTP status code is pinned to
// 200, so a later failure is reported as an SSE "error" event instead of a
// 4xx/5xx status.
//
// Configuration (empty value keeps the stock upstream behaviour):
//
//	server.early_sse_heartbeat.models  /  AXONHUB_SERVER_EARLY_SSE_HEARTBEAT_MODELS
//
// Comma separated list of downstream model IDs, i.e. the value the client puts
// in the request body, after channel prefixes are applied. A trailing "*"
// matches by prefix, e.g. "[anyrouter]/*".

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"

	"github.com/looplj/axonhub/internal/log"
	"github.com/looplj/axonhub/llm/httpclient"
)

// earlyHeartbeatMatcher matches downstream model IDs against the configured
// allow list. Matching is case-insensitive.
type earlyHeartbeatMatcher struct {
	exact    map[string]struct{}
	prefixes []string
}

func parseEarlyHeartbeatModels(raw string) *earlyHeartbeatMatcher {
	if strings.TrimSpace(raw) == "" {
		return nil
	}

	matcher := &earlyHeartbeatMatcher{exact: make(map[string]struct{})}

	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		if strings.HasSuffix(item, "*") {
			matcher.prefixes = append(matcher.prefixes, strings.ToLower(strings.TrimSuffix(item, "*")))
			continue
		}

		matcher.exact[strings.ToLower(item)] = struct{}{}
	}

	if len(matcher.exact) == 0 && len(matcher.prefixes) == 0 {
		return nil
	}

	return matcher
}

func (m *earlyHeartbeatMatcher) matches(model string) bool {
	if m == nil {
		return false
	}

	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" {
		return false
	}

	if _, ok := m.exact[model]; ok {
		return true
	}

	for _, prefix := range m.prefixes {
		if strings.HasPrefix(model, prefix) {
			return true
		}
	}

	return false
}

// The configuration string is immutable for the lifetime of a process, so the
// parsed matcher is cached behind an atomic pointer. Reads on the request hot
// path are lock free.
type earlyHeartbeatCacheEntry struct {
	raw     string
	matcher *earlyHeartbeatMatcher
}

var earlyHeartbeatCache atomic.Pointer[earlyHeartbeatCacheEntry]

func earlyHeartbeatMatcherFor(raw string) *earlyHeartbeatMatcher {
	if entry := earlyHeartbeatCache.Load(); entry != nil && entry.raw == raw {
		return entry.matcher
	}

	matcher := parseEarlyHeartbeatModels(raw)
	earlyHeartbeatCache.Store(&earlyHeartbeatCacheEntry{raw: raw, matcher: matcher})

	return matcher
}

// earlySSEHeartbeat owns the goroutine that may take over the response while
// the orchestrator is still working.
//
// Ownership of the response writer is guarded by mu: the goroutine only writes
// while holding it, and Stop takes the same mutex before marking the heartbeat
// stopped, so the caller is guaranteed exclusive access once Stop returns.
type earlySSEHeartbeat struct {
	mu      sync.Mutex
	started bool
	stopped bool
	count   int

	c        *gin.Context
	format   sseHeartbeatFormat
	interval time.Duration

	stop     chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// Started reports whether the SSE response headers were already committed.
// Only meaningful after Stop has returned.
func (hb *earlySSEHeartbeat) Started() bool {
	if hb == nil {
		return false
	}

	hb.mu.Lock()
	defer hb.mu.Unlock()

	return hb.started
}

// Format returns the heartbeat format the response was started with.
func (hb *earlySSEHeartbeat) Format() sseHeartbeatFormat {
	if hb == nil {
		return sseHeartbeatNone
	}

	return hb.format
}

// Stop ends the heartbeat goroutine and waits for it to return, so the caller
// can safely write to the response afterwards.
func (hb *earlySSEHeartbeat) Stop() {
	if hb == nil {
		return
	}

	hb.mu.Lock()
	hb.stopped = true
	hb.mu.Unlock()

	hb.stopOnce.Do(func() {
		close(hb.stop)
	})

	<-hb.done
}

func (hb *earlySSEHeartbeat) run(ctx context.Context) {
	defer close(hb.done)

	ticker := time.NewTicker(hb.interval)
	defer ticker.Stop()

	for {
		select {
		case <-hb.stop:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !hb.beat(ctx) {
				return
			}
		}
	}
}

// beat commits the SSE headers on the first tick and writes one heartbeat.
// It reports whether the heartbeat loop should keep running.
func (hb *earlySSEHeartbeat) beat(ctx context.Context) bool {
	hb.mu.Lock()
	defer hb.mu.Unlock()

	if hb.stopped {
		return false
	}

	if !hb.started {
		hb.c.Header("Access-Control-Allow-Origin", "*")
		setSSEHeaders(hb.c)
		hb.c.Writer.WriteHeaderNow()

		// The status line is on the wire from here on, whether or not the
		// flush below succeeds.
		hb.started = true

		if err := flushSSE(ctx, hb.c.Writer); err != nil {
			log.Warn(ctx, "Failed to flush early SSE headers", log.Cause(err))
			return false
		}

		log.Info(ctx, "Early SSE heartbeat started",
			log.String("heartbeat_format", sseHeartbeatFormatName(hb.format)),
			log.Duration("interval", hb.interval),
		)
	}

	if err := writeSSEHeartbeatEvent(ctx, hb.c.Writer, hb.format); err != nil {
		log.Warn(ctx, "Failed to write early SSE heartbeat", log.Cause(err))
		return false
	}

	hb.count++
	log.Debug(ctx, "Early SSE heartbeat sent",
		log.Int("heartbeat_count", hb.count),
		log.String("heartbeat_format", sseHeartbeatFormatName(hb.format)),
		log.Duration("interval", hb.interval),
	)

	return true
}

// startEarlySSEHeartbeat arms the early heartbeat watchdog when the request
// targets a configured model. It returns nil when the feature is off or the
// request does not qualify, in which case the stock behaviour is kept.
//
// Nothing is written to the response here; see beat.
func (handlers *ChatCompletionHandlers) startEarlySSEHeartbeat(
	c *gin.Context,
	genericReq *httpclient.Request,
) *earlySSEHeartbeat {
	if handlers == nil || c == nil || genericReq == nil || len(genericReq.Body) == 0 {
		return nil
	}

	keepAlive := handlers.sseKeepAlive
	if !keepAlive.Enabled || keepAlive.Interval <= 0 {
		return nil
	}

	format := handlers.sseHeartbeatFormat
	if format == sseHeartbeatNone {
		return nil
	}

	matcher := earlyHeartbeatMatcherFor(keepAlive.EarlyHeartbeatModels)
	if matcher == nil {
		return nil
	}

	// One pass over the request body for both fields.
	fields := gjson.GetManyBytes(genericReq.Body, "stream", "model")

	// Never touch non-streaming requests: flushing SSE headers would corrupt a
	// plain JSON response.
	if !fields[0].Bool() {
		return nil
	}

	if !matcher.matches(fields[1].String()) {
		return nil
	}

	hb := &earlySSEHeartbeat{
		c:        c,
		format:   format,
		interval: keepAlive.Interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}

	go hb.run(c.Request.Context())

	return hb
}

// writeEarlySSEHeartbeatError reports an orchestrator failure inside an SSE
// stream whose headers were already committed.
func writeEarlySSEHeartbeatError(c *gin.Context, format sseHeartbeatFormat, body []byte) {
	ctx := c.Request.Context()

	payload := compactJSONOrFallback(body, format)
	if err := writeSSEEvent(ctx, c.Writer, "error", json.RawMessage(payload)); err != nil {
		log.Warn(ctx, "Failed to write early SSE error event", log.Cause(err))
		return
	}

	writeEarlySSEStreamEnd(ctx, c, format)
}

// writeEarlySSENonStreamResult handles the unexpected case where the
// orchestrator answers a streaming request with a buffered response after the
// SSE headers were already committed. The status code and content type can no
// longer be changed, so the payload is delivered inside the stream.
func writeEarlySSENonStreamResult(c *gin.Context, format sseHeartbeatFormat, statusCode int, body []byte) {
	ctx := c.Request.Context()

	log.Error(ctx, "Non-stream response after early SSE headers were committed",
		log.Int("upstream_status", statusCode),
	)

	if statusCode >= 400 {
		writeEarlySSEHeartbeatError(c, format, body)
		return
	}

	payload := compactJSONOrFallback(body, format)
	if err := writeSSEEvent(ctx, c.Writer, "", json.RawMessage(payload)); err != nil {
		log.Warn(ctx, "Failed to write early SSE non-stream payload", log.Cause(err))
		return
	}

	writeEarlySSEStreamEnd(ctx, c, format)
}

// writeEarlySSEStreamEnd writes the terminal marker a client expects for the
// given wire format. Anthropic streams end at EOF and have no such marker.
func writeEarlySSEStreamEnd(ctx context.Context, c *gin.Context, format sseHeartbeatFormat) {
	if format != sseHeartbeatOpenAI {
		return
	}

	if err := writeSSEEvent(ctx, c.Writer, "", "[DONE]"); err != nil {
		log.Warn(ctx, "Failed to write early SSE termination", log.Cause(err))
	}
}

func compactJSONOrFallback(body []byte, format sseHeartbeatFormat) []byte {
	if len(body) > 0 && json.Valid(body) {
		var buf bytes.Buffer
		if err := json.Compact(&buf, body); err == nil {
			return buf.Bytes()
		}
	}

	if format == sseHeartbeatOpenAI {
		return []byte(`{"error":{"type":"api_error","message":"upstream request failed"}}`)
	}

	return []byte(`{"type":"error","error":{"type":"api_error","message":"upstream request failed"}}`)
}
