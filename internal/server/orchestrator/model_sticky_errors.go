package orchestrator

import (
	"fmt"
	"strings"

	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
)

// ModelStickyRoutingError is a local routing error, never an upstream 429 policy.
// The typed envelope lets the API forward only its safe Retry-After value.
type ModelStickyRoutingError struct {
	Response          *llm.ResponseError
	RetryAfterSeconds int
}

func (e *ModelStickyRoutingError) Error() string { return e.Response.Error() }
func (e *ModelStickyRoutingError) Unwrap() error { return e.Response }
func newModelStickyRoutingError(status int, code, message string, seconds int, cause error) error {
	if !strings.HasPrefix(message, biz.ModelStickyMessagePrefix) {
		message = biz.ModelStickyMessagePrefix + message
	}
	return &ModelStickyRoutingError{Response: &llm.ResponseError{StatusCode: status, Detail: llm.ErrorDetail{Type: code, Code: code, Message: message}, Cause: cause}, RetryAfterSeconds: seconds}
}

func newModelStickyStreamConflictError(model string) error {
	return fmt.Errorf(biz.ModelStickyMessagePrefix+"所有候选渠道都要求流式响应，但当前 Decisions 请求不能自动聚合；本次尚未请求上游。原始错误详情：%w", NewStreamPolicyConflictError(model))
}
