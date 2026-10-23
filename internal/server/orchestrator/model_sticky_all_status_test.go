package orchestrator

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/stretchr/testify/require"
)

// Exercise the real pipeline, not just the retry predicate. No external requests.
type stickyStatusExecutor struct {
	attempts []int
	statuses map[int]int
}

func (e *stickyStatusExecutor) Do(_ context.Context, req *httpclient.Request) (*httpclient.Response, error) {
	id, _ := strconv.Atoi(req.URL)
	e.attempts = append(e.attempts, id)
	status := e.statuses[id]
	if status == 0 {
		return &httpclient.Response{StatusCode: http.StatusOK}, nil
	}
	return nil, &httpclient.Error{StatusCode: status, Body: []byte(`{"error":{"code":"insufficient_credits","message":"[NonbiriAPI] insufficient credits"}}`)}
}

func (*stickyStatusExecutor) DoStream(context.Context, *httpclient.Request) (streams.Stream[*httpclient.StreamEvent], error) {
	panic("unexpected network stream")
}

func stickyStatusProcess(t *testing.T, state *PersistenceState, e *stickyStatusExecutor, cross, same int) error {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, prepareModelSticky(ctx, state, &llm.Request{Model: "A"}))
	out := &PersistentOutboundTransformer{state: state}
	result, err := pipeline.NewFactory(e).Pipeline(&stickyTestInbound{}, out, pipeline.WithRetry(cross, same, 0)).Process(ctx, &httpclient.Request{})
	return finishModelSticky(ctx, state, result, err)
}

func TestModelStickyAllUpstreamStatusErrorsRetry(t *testing.T) {
	state := stickyTestState(1)
	state.CurrentCandidate = state.ChannelModelsCandidates[0]
	for status := 100; status <= 599; status++ {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			for _, err := range []error{
				&httpclient.Error{StatusCode: status, Body: []byte("not JSON: wrong endpoint")},
				&llm.ResponseError{StatusCode: status, Detail: llm.ErrorDetail{Code: "unknown_provider_code"}},
			} {
				require.True(t, modelStickyRetryable(context.Background(), state, fmt.Errorf("upstream: %w", err)))
			}
		})
	}
	// A provider can put a structured error inside a successful HTTP stream or omit status.
	require.True(t, modelStickyRetryable(context.Background(), state, &llm.ResponseError{Detail: llm.ErrorDetail{Message: "provider stream error"}}))
	require.False(t, modelStickyRetryable(context.Background(), state, nil), "normal successful responses do not retry")
}

func TestModelStickyEvery4xxAnd5xxUsesPipelineBudgetAndRelays(t *testing.T) {
	for status := 400; status <= 599; status++ {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			stickyFreshRuntime(t)
			e := &stickyStatusExecutor{statuses: map[int]int{1: status}}
			require.NoError(t, stickyStatusProcess(t, stickyTestState(2), e, 1, 1))
			require.Equal(t, []int{1, 1, 2}, e.attempts)
			row := biz.ModelStickyRuntime.Snapshots()[0]
			require.Equal(t, 2, row.ChannelID)
			require.Len(t, row.Cooldowns, 1, "first unsuccessful channel must cool down")
			e.attempts = nil
			require.NoError(t, stickyStatusProcess(t, stickyTestState(2), e, 1, 1))
			require.Equal(t, []int{2}, e.attempts, "client retry must not return to the failed channel")
		})
	}
}

func TestModelStickyProduction502403404ContinuesAcrossClientRequests(t *testing.T) {
	stickyFreshRuntime(t)
	e := &stickyStatusExecutor{statuses: map[int]int{1: 502, 2: 403, 3: 404}}
	err := stickyStatusProcess(t, stickyTestState(4), e, 1, 1)
	require.Error(t, err)
	require.Equal(t, 403, ExtractStatusCodeFromError(err), "preserve the last original upstream error")
	require.Equal(t, []int{1, 1, 2, 2}, e.attempts)
	row := biz.ModelStickyRuntime.Snapshots()[0]
	require.ElementsMatch(t, []int{1, 2}, row.Attempted)
	require.Len(t, row.Cooldowns, 2, "final budget-exhausting 403 must be recorded")
	e.attempts = nil
	require.NoError(t, stickyStatusProcess(t, stickyTestState(4), e, 1, 1))
	require.Equal(t, []int{3, 3, 4}, e.attempts)
	row = biz.ModelStickyRuntime.Snapshots()[0]
	require.Equal(t, 4, row.ChannelID)
	require.Len(t, row.Cooldowns, 3)
}

func TestModelStickyUpstream400RetiresPreviouslySuccessfulPin(t *testing.T) {
	stickyFreshRuntime(t)
	e := &stickyStatusExecutor{statuses: map[int]int{}}
	require.NoError(t, stickyStatusProcess(t, stickyTestState(2), e, 1, 1))
	require.Equal(t, 1, biz.ModelStickyRuntime.Snapshots()[0].ChannelID)
	e.statuses[1] = 400
	e.attempts = nil
	require.NoError(t, stickyStatusProcess(t, stickyTestState(2), e, 1, 1))
	require.Equal(t, []int{1, 1, 2}, e.attempts)
	require.Equal(t, 2, biz.ModelStickyRuntime.Snapshots()[0].ChannelID)
}

func TestModelStickyAllStatusStillHonorsZeroBudgets(t *testing.T) {
	stickyFreshRuntime(t)
	e := &stickyStatusExecutor{statuses: map[int]int{1: 403}}
	require.Error(t, stickyStatusProcess(t, stickyTestState(2), e, 0, 0))
	require.Equal(t, []int{1}, e.attempts)
	require.Len(t, biz.ModelStickyRuntime.Snapshots()[0].Cooldowns, 1)
	e.attempts = nil
	require.NoError(t, stickyStatusProcess(t, stickyTestState(2), e, 0, 0))
	require.Equal(t, []int{2}, e.attempts)
}

func TestModelStickyAllStatusDoesNotChangeOtherStrategyRetryPredicate(t *testing.T) {
	state := stickyTestState(1)
	state.CurrentCandidate = state.ChannelModelsCandidates[0]
	out := &PersistentOutboundTransformer{state: state}
	for _, status := range []int{400, 401, 402, 403, 404, 405, 408, 409, 413, 415, 422, 425, 429, 500, 502, 503} {
		err := &httpclient.Error{StatusCode: status}
		state.RoutingPolicy.LoadBalancerStrategy = biz.LoadBalancerStrategyModelSticky
		require.True(t, out.CanRetry(err), "sticky status %d", status)
		state.RoutingPolicy.LoadBalancerStrategy = biz.LoadBalancerStrategyAdaptive
		want := status >= 500 // Existing default skips same-channel retries for 429.
		require.Equal(t, want, out.CanRetry(err), "adaptive status %d", status)
	}
}

func TestModelStickyAllStatusKeepsLocalErrorsAndCancellationOutOfHealth(t *testing.T) {
	for name, makeError := range map[string]func(*PersistenceState) error{
		"queue": func(s *PersistenceState) error {
			return asChannelQueueError(s.CurrentCandidate.Channel, ErrChannelQueueFull)
		},
		"rpm": func(s *PersistenceState) error { return newLocalRPMExhaustedError(s.CurrentCandidate.Channel, 5) },
		"routing": func(*PersistenceState) error {
			return newModelStickyRoutingError(409, "model_sticky_settings_changed", "配置已变化", 0, &httpclient.Error{StatusCode: 403})
		},
		"cancellation": func(*PersistenceState) error { return context.Canceled },
	} {
		t.Run(name, func(t *testing.T) {
			stickyFreshRuntime(t)
			s := stickyTestState(2)
			require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
			s.CurrentCandidate = s.ChannelModelsCandidates[0]
			err := fmt.Errorf("wrapped: %w", makeError(s))
			require.False(t, modelStickyRetryable(context.Background(), s, err))
			require.Error(t, finishModelSticky(context.Background(), s, nil, err))
			row := biz.ModelStickyRuntime.Snapshots()[0]
			require.Empty(t, row.Cooldowns)
			require.Empty(t, row.Attempted)
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := stickyTestState(1)
	s.CurrentCandidate = s.ChannelModelsCandidates[0]
	require.False(t, modelStickyRetryable(ctx, s, &httpclient.Error{StatusCode: 503}))
	// Channel rules may add retryable transport messages, but cannot exclude a status.
	s.CurrentCandidate.Channel.Settings = &objects.ChannelSettings{RetryableStatusCodes: []int{503}}
	require.True(t, modelStickyRetryable(context.Background(), s, &httpclient.Error{StatusCode: 403}))
}
