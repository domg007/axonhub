package orchestrator

import (
	"context"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/server/biz"
)

// ModelRoutingPolicy is attached only by AxonHub model resolution, including
// inherited/default model settings. Direct-channel and legacy fallback
// candidates deliberately carry no such provenance. Never infer it from a
// model name: the same name can exist in several channels.
func modelStickyModelPolicy(candidates []*ChannelModelsCandidate) *ModelRoutingPolicy {
	if len(candidates) == 0 || candidates[0] == nil {
		return nil
	}
	return candidates[0].ModelRoutingPolicy
}

func modelStickyRequestEligible(ctx context.Context, key *ent.APIKey, candidates []*ChannelModelsCandidate) bool {
	source, _ := contexts.GetSource(ctx)
	return key != nil && resolvedModelRoutingPolicy(ctx, candidates) != nil && source != request.SourceTest && source != request.SourcePlayground
}

// Resolve overrides exactly as before. Only the newly selected strategy is
// scoped: excluded requests use the existing default adaptive algorithm,
// without unknown-strategy warnings or any sticky state access.
func deriveRequestRoutingPolicy(ctx context.Context, retry *biz.RetryPolicy, key *ent.APIKey, candidates []*ChannelModelsCandidate) EffectiveRoutingPolicy {
	policy := deriveRoutingPolicy(retry, key, resolvedModelRoutingPolicy(ctx, candidates))
	if policy.LoadBalancerStrategy == biz.LoadBalancerStrategyModelSticky && !modelStickyRequestEligible(ctx, key, candidates) {
		policy.LoadBalancerStrategy = biz.LoadBalancerStrategyAdaptive
	}
	return policy
}
