package orchestrator

import (
	"context"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
)

// This boundary runs after authorization/When filtering, before soft transport
// preferences. Legacy strategies still use the original decorators unchanged.
type modelStickyCompatibilitySelector struct {
	wrapped CandidateSelector
	policy  RetryPolicyProvider
	key     *ent.APIKey
}

type modelStickyResolvedCandidates struct{ candidates []*ChannelModelsCandidate }

func (s modelStickyResolvedCandidates) Select(context.Context, *llm.Request) ([]*ChannelModelsCandidate, error) {
	return s.candidates, nil
}

func (s *modelStickyCompatibilitySelector) Select(ctx context.Context, req *llm.Request) ([]*ChannelModelsCandidate, error) {
	candidates, err := s.wrapped.Select(ctx, req)
	if err != nil {
		return nil, err
	}
	sticky := s.policy != nil && deriveRequestRoutingPolicy(ctx, s.policy.RetryPolicyOrDefault(ctx), s.key, candidates).LoadBalancerStrategy == biz.LoadBalancerStrategyModelSticky
	var selector CandidateSelector = modelStickyResolvedCandidates{candidates}
	// Native server tools are not ordinary client function tools. Preserve their
	// established capability preference instead of inventing unsupported services.
	if req.APIFormat == llm.APIFormatGeminiContents {
		selector = WithGoogleNativeToolsSelector(selector)
	}
	if req.APIFormat == llm.APIFormatAnthropicMessage {
		selector = WithAnthropicNativeToolsSelector(selector)
	}
	if !sticky {
		return WithStreamPolicySelector(selector).Select(ctx, req)
	}
	candidates, err = selector.Select(ctx, req)
	if err != nil {
		return nil, err
	}
	filtered := make([]*ChannelModelsCandidate, 0, len(candidates))
	streaming := req.Stream != nil && *req.Stream
	for _, c := range candidates {
		policy := streamPolicyOf(c)
		if streaming && policy == objects.CapabilityPolicyForbid {
			continue
		}
		if !streaming && policy == objects.CapabilityPolicyRequire && !supportsAutoAggregateRequest(req) {
			continue
		}
		filtered = append(filtered, c)
	}
	if len(filtered) == 0 && len(candidates) > 0 && req.RequestType == llm.RequestTypeDecisions {
		return nil, newModelStickyStreamConflictError(req.Model)
	}
	return filtered, nil
}
