package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/transformer/shared"
)

// A Responses continuation that cannot be served is a conversation-state signal.
// Upstream returns it to the client at once and skips every health penalty, so
// model-sticky must not cool the pinned channel down for it either. Every other
// upstream error, including an ordinary 400, keeps the availability-first rule.
func TestModelStickyResponsesRecoveryIsNotChannelFailure(t *testing.T) {
	providerNotFound := &httpclient.Error{
		StatusCode: http.StatusBadRequest,
		Body:       []byte(`{"error":{"code":"previous_response_not_found","message":"gone"}}`),
	}
	ordinaryBadRequest := &httpclient.Error{
		StatusCode: http.StatusBadRequest,
		Body:       []byte(`{"error":{"code":"invalid_request","message":"bad"}}`),
	}

	cases := []struct {
		name      string
		err       error
		retryable bool
	}{
		{name: "detected locally before sending", err: shared.ResponsesRecoveryError(), retryable: false},
		{name: "provider previous_response_not_found", err: providerNotFound, retryable: false},
		{name: "provider previous_response_not_found as response error", err: &llm.ResponseError{StatusCode: http.StatusBadRequest, Detail: llm.ErrorDetail{Code: "previous_response_not_found"}}, retryable: false},
		{name: "ordinary upstream 400 still fails over", err: ordinaryBadRequest, retryable: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stickyFreshRuntime(t)
			s := stickyTestState(2)
			require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
			require.NotNil(t, s.modelSticky.lease)
			// An attempt was made on the pinned channel (normally set by TransformRequest).
			s.CurrentCandidate = s.ChannelModelsCandidates[0]
			require.Equal(t, tc.retryable, modelStickyRetryable(context.Background(), s, tc.err))

			err := finishModelSticky(context.Background(), s, nil, tc.err)
			require.True(t, errors.Is(err, tc.err) || err == tc.err)
			require.Nil(t, s.modelSticky.lease)

			snapshots := biz.ModelStickyRuntime.Snapshots()
			require.Len(t, snapshots, 1)
			if tc.retryable {
				require.NotEmpty(t, snapshots[0].Cooldowns, "an ordinary upstream error must still cool the channel down")
				return
			}
			require.Empty(t, snapshots[0].Cooldowns, "a continuation recovery signal must not cool the channel down")
		})
	}
}
