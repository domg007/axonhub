package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/stretchr/testify/require"
)

type stickyRevisionPolicy struct{ strategy string }

func (p stickyRevisionPolicy) RetryPolicyOrDefault(context.Context) *biz.RetryPolicy {
	return &biz.RetryPolicy{LoadBalancerStrategy: p.strategy}
}
func (p stickyRevisionPolicy) QuotaRoutingSettingsOrDefault(context.Context) biz.QuotaRoutingSettings {
	return biz.QuotaRoutingSettings{}
}

func TestModelStickyCompatibilityOnlyChangesNewStrategy(t *testing.T) {
	for _, strategy := range []string{biz.LoadBalancerStrategyModelSticky, biz.LoadBalancerStrategyAdaptive, biz.LoadBalancerStrategyFailover, biz.LoadBalancerStrategyRoundRobin, biz.LoadBalancerStrategyCircuitBreaker} {
		t.Run(strategy, func(t *testing.T) {
			state := stickyTestState(2)
			state.ChannelModelsCandidates[0].Channel.Policies.Stream = objects.CapabilityPolicyRequire
			selector := &modelStickyCompatibilitySelector{wrapped: &mockSelector{candidates: state.ChannelModelsCandidates}, policy: stickyRevisionPolicy{strategy}, key: state.APIKey}
			got, err := selector.Select(context.Background(), &llm.Request{Model: "A", APIFormat: llm.APIFormatOpenAIChatCompletion})
			require.NoError(t, err)
			if strategy == biz.LoadBalancerStrategyModelSticky {
				require.Len(t, got, 2)
				require.Equal(t, 1, got[0].Channel.ID)
			} else {
				require.Len(t, got, 1)
				require.Equal(t, 2, got[0].Channel.ID)
			}
			state.ChannelModelsCandidates[0].Channel.Policies.Stream = objects.CapabilityPolicyForbid
			streaming := true
			got, err = selector.Select(context.Background(), &llm.Request{Model: "A", Stream: &streaming})
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, 2, got[0].Channel.ID)
		})
	}
}

func TestModelStickyRealSelectionKeepsConvertibleTitleOnPinnedChannel(t *testing.T) {
	stickyFreshRuntime(t)
	for _, streaming := range []bool{true, false} {
		state := stickyTestState(2)
		cs := state.ChannelModelsCandidates
		cs[0].Channel.Policies.Stream = objects.CapabilityPolicyRequire
		state.ChannelModelsCandidates = nil
		state.CandidateSelector = &mockSelector{candidates: cs}
		policy := stickyRevisionPolicy{biz.LoadBalancerStrategyModelSticky}
		state.RetryPolicyProvider = policy
		state.LoadBalancers = map[string]*LoadBalancer{biz.LoadBalancerStrategyAdaptive: nil}
		req := &llm.Request{Model: "A", Stream: &streaming, APIFormat: llm.APIFormatOpenAIChatCompletion}
		_, err := selectCandidates(&PersistentInboundTransformer{state: state}, nil, policy).OnInboundLlmRequest(context.Background(), req)
		require.NoError(t, err)
		require.Len(t, state.ChannelModelsCandidates, 1)
		require.Equal(t, 1, state.ChannelModelsCandidates[0].Channel.ID)
		require.NoError(t, finishModelSticky(context.Background(), state, &pipeline.Result{Response: &httpclient.Response{}}, nil))
	}
}

func TestModelStickyManualControlStopsSameChannelRetryAndOutbound(t *testing.T) {
	stickyFreshRuntime(t)
	s := stickyTestState(2)
	require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
	s.CurrentCandidate = s.ChannelModelsCandidates[0]
	require.NoError(t, biz.ModelStickyRuntime.Control(s.modelSticky.key, 2))
	p := &PersistentOutboundTransformer{state: s}
	require.ErrorIs(t, p.PrepareForRetry(context.Background()), biz.ErrModelStickySettingsChanged)
	_, err := p.TransformRequest(context.Background(), &llm.Request{Model: "A"})
	require.ErrorIs(t, err, biz.ErrModelStickySettingsChanged)
	require.Empty(t, biz.ModelStickyRuntime.Snapshots()[0].Cooldowns)
}

func TestModelStickyProbeWaitBoundDoesNotReleaseOwnerOrProbeNext(t *testing.T) {
	stickyFreshRuntime(t)
	owner := stickyTestState(2)
	require.NoError(t, prepareModelSticky(context.Background(), owner, &llm.Request{Model: "A"}))
	defer owner.modelSticky.release(false, false)
	waiter := &modelStickyRequest{key: owner.modelSticky.key, keyName: "waiter", public: owner.modelSticky.public, candidates: owner.modelSticky.candidates, tried: map[int]bool{}, waitTimeout: 5 * time.Millisecond}
	_, err := waiter.acquire(context.Background(), true)
	var routing *ModelStickyRoutingError
	require.ErrorAs(t, err, &routing)
	require.Equal(t, "model_sticky_probe_busy", routing.Response.Detail.Code)
	require.True(t, owner.modelSticky.lease.Active())
	row := biz.ModelStickyRuntime.Snapshots()[0]
	require.True(t, row.Probing)
	require.Empty(t, row.Attempted)
	require.Empty(t, row.Cooldowns)
	owner.modelSticky.release(true, false)
	next := stickyTestState(2)
	require.NoError(t, prepareModelSticky(context.Background(), next, &llm.Request{Model: "A"}))
	require.Equal(t, 1, next.modelSticky.lease.ChannelID)
	next.modelSticky.release(true, false)
}

func TestModelStickyOuterStreamKeepsChoicesAndHeaders(t *testing.T) {
	for _, headers := range []http.Header{nil, {"X-Request-Id": []string{"id"}}} {
		stickyFreshRuntime(t)
		s := stickyTestState(1)
		require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
		original := httpclient.WithResponseHeadersAndExpectedChoices(&reviewMetadataStream{streams.SliceStream([]*httpclient.StreamEvent{})}, headers, 2)
		result := &pipeline.Result{Stream: true, EventStream: original}
		require.NoError(t, finishModelSticky(context.Background(), s, result, nil))
		provider, ok := result.EventStream.(interface{ ExpectedStreamChoices() int })
		require.True(t, ok)
		require.Equal(t, 2, provider.ExpectedStreamChoices())
		require.Equal(t, headers.Get("X-Request-Id"), httpclient.GetResponseHeaders(result.EventStream).Get("X-Request-Id"))
		require.NoError(t, result.EventStream.Close())
	}
}

type stickyModelOutbound struct{ stickyTestOutbound }

func (s *stickyModelOutbound) TransformRequest(_ context.Context, req *llm.Request) (*httpclient.Request, error) {
	return &httpclient.Request{URL: req.Model}, nil
}

type stickyModelExecutor struct {
	stickyTestExecutor
	models []string
}

func (e *stickyModelExecutor) Do(_ context.Context, req *httpclient.Request) (*httpclient.Response, error) {
	e.models = append(e.models, req.URL)
	if req.URL == "backup-A" {
		return &httpclient.Response{StatusCode: 200}, nil
	}
	return nil, &httpclient.Error{StatusCode: 429}
}
func TestModelStickyMergedFallbackActuallySucceedsWithinSameChannelBudget(t *testing.T) {
	stickyFreshRuntime(t)
	s := stickyTestState(1)
	first := s.ChannelModelsCandidates[0]
	first.Channel.Outbound = &stickyModelOutbound{}
	second := *first
	second.Priority = 1
	second.Models = []biz.ChannelModelEntry{{RequestModel: "A", ActualModel: "backup-A"}}
	second.DefaultMaxTokens = 123
	s.ChannelModelsCandidates = sortModelStickyCandidates([]*ChannelModelsCandidate{first, &second})
	require.Len(t, first.Models, 1, "input must not be mutated")
	require.Equal(t, []int64{0, 123}, s.ChannelModelsCandidates[0].modelStickyDefaults)
	require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
	executor := &stickyModelExecutor{}
	p := pipeline.NewFactory(executor).Pipeline(&stickyTestInbound{}, &PersistentOutboundTransformer{state: s}, pipeline.WithRetry(0, 1, 0))
	result, err := p.Process(context.Background(), &httpclient.Request{})
	err = finishModelSticky(context.Background(), s, result, err)
	require.NoError(t, err)
	require.Equal(t, []string{"actual-A", "backup-A"}, executor.models)
	require.Equal(t, 1, biz.ModelStickyRuntime.Snapshots()[0].ChannelID)
}

func TestModelStickyFinalCooldownIsNotHiddenByPreviousUpstream429(t *testing.T) {
	stickyFreshRuntime(t)
	s := stickyTestState(1)
	require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
	p := pipeline.NewFactory(&stickyTestExecutor{}).Pipeline(&stickyTestInbound{}, &PersistentOutboundTransformer{state: s}, pipeline.WithRetry(2, 0, 0))
	result, err := p.Process(context.Background(), &httpclient.Request{})
	err = finishModelSticky(context.Background(), s, result, err)
	var routing *ModelStickyRoutingError
	require.True(t, errors.As(err, &routing))
	require.Equal(t, 503, routing.Response.StatusCode)
	require.Equal(t, "model_sticky_cooldown", routing.Response.Detail.Code)
	require.Positive(t, routing.RetryAfterSeconds)
}

func TestModelStickyRecoverChoicesAlreadyHiddenByPipelineHeaders(t *testing.T) {
	stickyFreshRuntime(t)
	s := stickyTestState(1)
	require.NoError(t, prepareModelSticky(context.Background(), s, &llm.Request{Model: "A"}))
	s.Request = &ent.Request{RequestBody: []byte(`{"stream":true,"n":2}`)}
	// Existing pipeline WithResponseHeaders intentionally retains its old behavior.
	original := httpclient.WithResponseHeaders(&reviewMetadataStream{streams.SliceStream([]*httpclient.StreamEvent{})}, http.Header{"X-Request-Id": []string{"id"}})
	_, visible := original.(interface{ ExpectedStreamChoices() int })
	require.False(t, visible)
	result := &pipeline.Result{Stream: true, EventStream: original}
	require.NoError(t, finishModelSticky(context.Background(), s, result, nil))
	require.Equal(t, 2, result.EventStream.(interface{ ExpectedStreamChoices() int }).ExpectedStreamChoices())
	require.NoError(t, result.EventStream.Close())
}

type stickyRevisionErrorStream struct {
	streams.Stream[*httpclient.StreamEvent]
	err error
}

func (s *stickyRevisionErrorStream) Err() error { return s.err }
func TestModelStickyStreamFailureClassificationTable(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		cancel bool
		cool   bool
	}{
		{"400", &httpclient.Error{StatusCode: 400}, false, true},
		{"429", &httpclient.Error{StatusCode: 429}, false, true},
		{"incomplete", llm.ErrStreamIncomplete, false, true},
		{"cancel", context.Canceled, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stickyFreshRuntime(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := stickyTestState(1)
			require.NoError(t, prepareModelSticky(ctx, s, &llm.Request{Model: "A"}))
			s.CurrentCandidate = s.ChannelModelsCandidates[0]
			s.OutboundStreamTerminal = streamTerminalFailed
			if tc.cancel {
				cancel()
			}
			result := &pipeline.Result{Stream: true, EventStream: &stickyRevisionErrorStream{Stream: streams.SliceStream([]*httpclient.StreamEvent{}), err: tc.err}}
			require.NoError(t, finishModelSticky(ctx, s, result, nil))
			require.NoError(t, result.EventStream.Close())
			require.Equal(t, tc.cool, len(biz.ModelStickyRuntime.Snapshots()[0].Cooldowns) > 0)
		})
	}
}
