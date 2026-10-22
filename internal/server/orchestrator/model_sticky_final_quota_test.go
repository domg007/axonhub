package orchestrator

import (
	"context"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestModelStickyFinalBackpressureMustRecognizeExistingModelPin(t *testing.T) {
	stickyFreshRuntime(t)
	ctx := context.Background()
	seed := stickyTestState(2)
	require.NoError(t, prepareModelSticky(ctx, seed, &llm.Request{Model: "A"}))
	require.NoError(t, finishModelSticky(ctx, seed, &pipeline.Result{Response: &httpclient.Response{}}, nil))
	state := stickyTestState(2)
	candidates := state.ChannelModelsCandidates
	state.ChannelModelsCandidates = nil
	state.CandidateSelector = &mockSelector{candidates: candidates}
	state.RetryPolicyProvider = stickyRevisionPolicy{biz.LoadBalancerStrategyModelSticky}
	state.LoadBalancers = map[string]*LoadBalancer{biz.LoadBalancerStrategyAdaptive: nil}
	provider := &mockQuotaStatusProvider{statuses: map[int]*biz.QuotaChannelStatus{1: quotaStickyOnlyStatus()}}
	settings := &mockQuotaRoutingSettingsProvider{settings: biz.QuotaRoutingSettings{DefaultMode: objects.QuotaRoutingModeBackpressure}}
	_, err := selectCandidates(&PersistentInboundTransformer{state: state}, provider, settings).OnInboundLlmRequest(ctx, &llm.Request{Model: "A"})
	require.NoError(t, err, "backpressure is not exhausted: existing model pin must remain eligible")
	defer state.modelSticky.release(false, false)
	require.Equal(t, 1, state.ChannelModelsCandidates[0].Channel.ID)
}
