package orchestrator

import (
	"context"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestModelStickyFinalQuotaColdManualExhaustedAndConcurrentControl(t *testing.T) {
	for _, mode := range []string{"cold", "manual", "exhausted", "concurrent-control"} {
		t.Run(mode, func(t *testing.T) {
			stickyFreshRuntime(t)
			if mode != "cold" {
				seed := stickyTestState(2)
				require.NoError(t, prepareModelSticky(context.Background(), seed, &llm.Request{Model: "A"}))
				seed.modelSticky.release(true, false)
				if mode == "manual" {
					require.NoError(t, biz.ModelStickyRuntime.Control(biz.ModelStickyKey{APIKeyID: 1, Model: "A"}, 1))
				}
			}
			state := stickyTestState(2)
			candidates := state.ChannelModelsCandidates
			state.ChannelModelsCandidates = nil
			state.CandidateSelector = &mockSelector{candidates: candidates}
			state.RetryPolicyProvider = stickyRevisionPolicy{biz.LoadBalancerStrategyModelSticky}
			state.LoadBalancers = map[string]*LoadBalancer{biz.LoadBalancerStrategyAdaptive: nil}
			status := quotaStickyOnlyStatus()
			if mode == "exhausted" || mode == "concurrent-control" {
				status = quotaExhaustedStatus()
			}
			provider := &mockQuotaStatusProvider{statuses: map[int]*biz.QuotaChannelStatus{1: status}}
			settings := &mockQuotaRoutingSettingsProvider{settings: biz.QuotaRoutingSettings{DefaultMode: objects.QuotaRoutingModeBackpressure}}
			if mode == "concurrent-control" {
				require.NoError(t, biz.ModelStickyRuntime.Control(biz.ModelStickyKey{APIKeyID: 1, Model: "A"}, 2))
			}
			_, err := selectCandidates(&PersistentInboundTransformer{state: state}, provider, settings).OnInboundLlmRequest(context.Background(), &llm.Request{Model: "A"})
			switch mode {
			case "manual":
				require.NoError(t, err, "quota-blocked manual target relays without existing-session privilege")
				require.Equal(t, 2, state.modelSticky.lease.ChannelID)
				require.Zero(t, biz.ModelStickyRuntime.Snapshots()[0].PreferredChannelID)
				state.modelSticky.release(true, false)
			case "concurrent-control":
				require.Error(t, err, "configuration captured before manual switch must fail admission without overwriting it")
				require.Equal(t, 2, biz.ModelStickyRuntime.Snapshots()[0].PreferredChannelID)
			default:
				require.NoError(t, err)
				require.Equal(t, 2, state.modelSticky.lease.ChannelID)
				state.modelSticky.release(true, false)
			}
		})
	}
}

func TestModelStickyFinalManualTargetSurvivesCancelAndEarlyClose(t *testing.T) {
	for _, mode := range []string{"cancel", "early-close", "stream-400"} {
		t.Run(mode, func(t *testing.T) {
			stickyFreshRuntime(t)
			seed := stickyTestState(2)
			require.NoError(t, prepareModelSticky(context.Background(), seed, &llm.Request{Model: "A"}))
			seed.modelSticky.release(true, false)
			require.NoError(t, biz.ModelStickyRuntime.Control(biz.ModelStickyKey{APIKeyID: 1, Model: "A"}, 2))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			state := stickyTestState(2)
			require.NoError(t, prepareModelSticky(ctx, state, &llm.Request{Model: "A"}))
			require.Equal(t, 2, state.modelSticky.lease.ChannelID)
			if mode == "cancel" {
				cancel()
				require.Eventually(t, func() bool { return !biz.ModelStickyRuntime.Snapshots()[0].Probing }, time.Second, time.Millisecond)
				require.Error(t, finishModelSticky(ctx, state, nil, context.Canceled))
			} else {
				var stream streams.Stream[*httpclient.StreamEvent] = streams.SliceStream([]*httpclient.StreamEvent{})
				if mode == "stream-400" {
					stream = &reviewBadRequestStream{stream}
				}
				result := &pipeline.Result{Stream: true, EventStream: stream}
				require.NoError(t, finishModelSticky(ctx, state, result, nil))
				require.NoError(t, result.EventStream.Close())
			}
			row := biz.ModelStickyRuntime.Snapshots()[0]
			require.Equal(t, 2, row.PreferredChannelID)
			require.False(t, row.Probing)
			require.Empty(t, row.Cooldowns)
			next := stickyTestState(2)
			require.NoError(t, prepareModelSticky(context.Background(), next, &llm.Request{Model: "A"}))
			require.Equal(t, 2, next.modelSticky.lease.ChannelID)
			next.modelSticky.release(true, false)
			require.Zero(t, biz.ModelStickyRuntime.Snapshots()[0].PreferredChannelID)
		})
	}
}

func TestModelStickyFinalManualTargetStillFailsOver(t *testing.T) {
	stickyFreshRuntime(t)
	// a failed in this round; select b manually, then b fails before succeeding.
	seed := stickyTestState(3)
	require.NoError(t, prepareModelSticky(context.Background(), seed, &llm.Request{Model: "A"}))
	seed.modelSticky.release(false, true)
	require.NoError(t, biz.ModelStickyRuntime.Control(biz.ModelStickyKey{APIKeyID: 1, Model: "A"}, 2))
	b := stickyTestState(3)
	require.NoError(t, prepareModelSticky(context.Background(), b, &llm.Request{Model: "A"}))
	require.Equal(t, 2, b.modelSticky.lease.ChannelID)
	b.CurrentCandidate = b.ChannelModelsCandidates[0]
	require.Error(t, finishModelSticky(context.Background(), b, nil, &httpclient.Error{StatusCode: 429}))
	next := stickyTestState(3)
	require.NoError(t, prepareModelSticky(context.Background(), next, &llm.Request{Model: "A"}))
	require.Equal(t, 3, next.modelSticky.lease.ChannelID)
	next.modelSticky.release(true, false)
	// b had no valid response: it is cooling and not repeatedly inserted between c/d/e.
	require.Len(t, biz.ModelStickyRuntime.Snapshots()[0].Cooldowns, 2)
}
