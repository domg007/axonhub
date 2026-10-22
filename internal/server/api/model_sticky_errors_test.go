package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm/pipeline"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer/openai"
	"github.com/stretchr/testify/require"
)

func stickyAPIError() error {
	return &orchestrator.ModelStickyRoutingError{Response: &llm.ResponseError{StatusCode: 503, Detail: llm.ErrorDetail{Type: "model_sticky_cooldown", Code: "model_sticky_cooldown", Message: "Retry in 30 seconds."}}, RetryAfterSeconds: 30}
}
func TestModelStickyHTTPErrorPreservesCodeAndOnlyLocalRetryHeader(t *testing.T) {
	r := gin.New()
	r.GET("/local", func(c *gin.Context) {
		err := stickyAPIError()
		orch := &orchestrator.ChatCompletionOrchestrator{Inbound: openai.NewInboundTransformer()}
		transformed := transformOrchestratorError(c.Request.Context(), err, orch)
		writeModelStickyRetryAfter(c, err)
		c.JSON(transformed.StatusCode, json.RawMessage(transformed.Body))
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/local", nil))
	require.Equal(t, 503, w.Code)
	require.Equal(t, "30", w.Header().Get("Retry-After"))
	require.Contains(t, w.Body.String(), "model_sticky_cooldown")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	writeModelStickyRetryAfter(c, &httpclient.Error{StatusCode: 429, Headers: http.Header{"Retry-After": []string{"3600"}}})
	require.Empty(t, c.Writer.Header().Get("Retry-After"), "default/upstream errors must retain their original behavior")
}
func TestModelStickyCommittedSSEUsesErrorBodyNotLateHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	c.Status(200)
	c.Writer.Flush()
	err := stickyAPIError()
	transformed := transformOrchestratorError(context.Background(), err, &orchestrator.ChatCompletionOrchestrator{Inbound: openai.NewInboundTransformer()})
	writeModelStickyRetryAfter(c, err)
	writeEarlySSEHeartbeatError(c, sseHeartbeatOpenAI, transformed.Body)
	require.Equal(t, 200, w.Code)
	require.Empty(t, w.Header().Get("Retry-After"))
	require.Contains(t, w.Body.String(), "model_sticky_cooldown")
	require.Contains(t, w.Body.String(), "30 seconds")
}
func TestModelStickyMetadataWrapperSupportsMultipleChoicesWithoutDONE(t *testing.T) {
	for _, terminalErr := range []error{io.ErrUnexpectedEOF, context.Canceled} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
		original := &errorAfterStream{items: []*httpclient.StreamEvent{{Data: []byte(`{"choices":[{"index":0,"finish_reason":"stop"}]}`)}, {Data: []byte(`{"choices":[{"index":1,"finish_reason":"stop"}]}`)}}, err: terminalErr}
		stream := httpclient.WithResponseHeadersAndExpectedChoices(original, http.Header{"X-Request-Id": []string{"local"}}, 2)
		WriteSSEStream(c, stream)
		require.NotContains(t, w.Body.String(), "event:error")
		require.Contains(t, w.Body.String(), `"index":1`)
		require.True(t, errors.Is(stream.Err(), terminalErr))
	}
}

func TestModelStickyRealChatHandlerWritesRetryAfter(t *testing.T) {
	ctx, system := setupUpstreamErrorPolicyTest(t, biz.UpstreamErrorPolicy{})
	orch := &orchestrator.ChatCompletionOrchestrator{Inbound: openai.NewInboundTransformer(), SystemService: system, PipelineFactory: pipeline.NewFactory(nil), Middlewares: []pipeline.Middleware{pipeline.OnLlmRequest("local-routing-failure", func(context.Context, *llm.Request) (*llm.Request, error) { return nil, stickyAPIError() })}}
	handler := NewChatCompletionHandlers(orch)
	router := gin.New()
	router.POST("/v1/chat/completions", handler.ChatCompletion)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"A","messages":[{"role":"user","content":"test"}]}`)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	require.Equal(t, 503, w.Code)
	require.Equal(t, "30", w.Header().Get("Retry-After"))
	require.Contains(t, w.Body.String(), "model_sticky_cooldown")
}
