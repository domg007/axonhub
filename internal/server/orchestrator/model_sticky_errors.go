package orchestrator

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
)

// A model missing from one upstream is not a reason to disable that channel's
// other models. Only explicit structured model errors qualify; bare 400/403/404,
// context-length and content-policy failures are not guessed from substrings.
func modelStickyModelUnavailable(err error) bool {
	isModelError := func(detail llm.ErrorDetail) bool {
		for _, code := range []string{detail.Code, detail.Type} {
			switch strings.ToLower(code) {
			case "model_not_found", "model_not_supported", "unsupported_model", "model_unavailable":
				return true
			}
		}
		return detail.Type == "not_found_error" && strings.HasPrefix(strings.ToLower(strings.TrimSpace(detail.Message)), "model:")
	}
	var responseErr *llm.ResponseError
	if errors.As(err, &responseErr) && isModelError(responseErr.Detail) {
		return true
	}
	var rawErr *httpclient.Error
	if errors.As(err, &rawErr) && len(rawErr.Body) <= 64*1024 {
		var envelope struct {
			Error llm.ErrorDetail `json:"error"`
		}
		if json.Unmarshal(rawErr.Body, &envelope) == nil {
			return isModelError(envelope.Error)
		}
	}
	return false
}

// ModelStickyRoutingError is a local routing error, never an upstream 429 policy.
// The typed envelope lets the API forward only its safe Retry-After value.
type ModelStickyRoutingError struct {
	Response          *llm.ResponseError
	RetryAfterSeconds int
}

func (e *ModelStickyRoutingError) Error() string { return e.Response.Error() }
func (e *ModelStickyRoutingError) Unwrap() error { return e.Response }
func newModelStickyRoutingError(status int, code, message string, seconds int, cause error) error {
	return &ModelStickyRoutingError{Response: &llm.ResponseError{StatusCode: status, Detail: llm.ErrorDetail{Type: code, Code: code, Message: message}, Cause: cause}, RetryAfterSeconds: seconds}
}
