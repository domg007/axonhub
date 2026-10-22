package api

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/looplj/axonhub/internal/server/orchestrator"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
)

func modelStickyHTTPError(ctx context.Context, err error, orch *orchestrator.ChatCompletionOrchestrator) (*httpclient.Error, bool) {
	var routing *orchestrator.ModelStickyRoutingError
	if !errors.As(err, &routing) {
		return nil, false
	}
	if orch != nil && orch.Inbound != nil {
		return orch.Inbound.TransformError(ctx, routing.Response), true
	}
	body, _ := json.Marshal(struct {
		Error llm.ErrorDetail `json:"error"`
	}{Error: routing.Response.Detail})
	return &httpclient.Error{StatusCode: routing.Response.StatusCode, Body: body}, true
}

func writeModelStickyRetryAfter(c *gin.Context, err error) {
	var routing *orchestrator.ModelStickyRoutingError
	if !c.Writer.Written() && errors.As(err, &routing) && routing.RetryAfterSeconds > 0 {
		c.Header("Retry-After", strconv.Itoa(routing.RetryAfterSeconds))
	}
}
