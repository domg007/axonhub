package orchestrator

import (
	"context"
	"net/http"
	"testing"

	"github.com/looplj/axonhub/llm"
	"github.com/stretchr/testify/require"
)

// A plugin overload (upstream rc1+) is a local admission rejection that wraps a
// client-facing 429. It never reached the provider, so model-sticky must not
// count it as a channel failure.
func TestModelStickyPluginOverloadIsNotChannelFailure(t *testing.T) {
	state := stickyTestState(2)
	state.CurrentCandidate = state.ChannelModelsCandidates[0]
	err := NewPluginOverloadedError(&llm.ResponseError{StatusCode: http.StatusTooManyRequests})
	require.False(t, modelStickyRetryable(context.Background(), state, err))
}
