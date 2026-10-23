package orchestrator

import (
	"context"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
)

// One request-local carrier preserves provenance even when When/capability or
// quota filtering leaves zero candidates. No shared cache shape is changed.
type modelStickyResolutionContextKey struct{}
type modelStickyResolutionHolder struct{ resolution *modelStickyResolution }
type modelStickyResolution struct {
	entity   *ent.Model
	settings *biz.SystemModelSettings
	resolved []*resolvedAssociationCandidate
	policy   *ModelRoutingPolicy
}

func newModelStickyResolution(ctx context.Context, entity *ent.Model, settings *biz.SystemModelSettings, resolved []*resolvedAssociationCandidate) *modelStickyResolution {
	policy := &ModelRoutingPolicy{LoadBalancerStrategy: objects.RoutingPolicyDefault, TraceStickyMode: objects.RoutingPolicyDefault}
	if entity.Settings != nil {
		policy.LoadBalancerStrategy = objects.NormalizeRoutingPolicyValue(entity.Settings.LoadBalancerStrategy)
		policy.TraceStickyMode = objects.NormalizeRoutingPolicyValue(entity.Settings.TraceStickyMode)
	}
	result := &modelStickyResolution{entity: entity, settings: settings, resolved: resolved, policy: policy}
	policy.modelStickyResolution = result
	if h, ok := ctx.Value(modelStickyResolutionContextKey{}).(*modelStickyResolutionHolder); ok {
		h.resolution = result
	}
	return result
}
func resolvedModelStickyFacts(ctx context.Context, candidates []*ChannelModelsCandidate) *modelStickyResolution {
	if p := modelStickyModelPolicy(candidates); p != nil && p.modelStickyResolution != nil {
		return p.modelStickyResolution
	}
	if h, ok := ctx.Value(modelStickyResolutionContextKey{}).(*modelStickyResolutionHolder); ok {
		return h.resolution
	}
	return nil
}
func resolvedModelRoutingPolicy(ctx context.Context, candidates []*ChannelModelsCandidate) *ModelRoutingPolicy {
	if p := modelStickyModelPolicy(candidates); p != nil {
		return p
	}
	if facts := resolvedModelStickyFacts(ctx, candidates); facts != nil {
		return facts.policy
	}
	return nil
}

// Reuse existing permission decorators on the unconditioned association facts.
// No When, protocol, stream or quota decision is reimplemented by the runtime.
func populateModelStickyAdmission(ctx context.Context, state *PersistenceState, req *llm.Request, a *biz.ModelStickyAdmission) error {
	facts := resolvedModelStickyFacts(ctx, state.ChannelModelsCandidates)
	if facts == nil {
		return nil
	} // synthetic state-machine fixtures
	a.ModelEntityID = facts.entity.ID
	a.ModelFingerprint = biz.ModelStickyModelFingerprint(facts.entity)
	a.InheritanceFingerprint = biz.ModelStickyInheritanceFingerprint(facts.settings)
	a.ModelStrategy = facts.policy.LoadBalancerStrategy
	var selector CandidateSelector = modelStickyResolvedCandidates{aggregateChannelModelCandidates(facts.resolved)}
	apply := func(ids []int, tags []string, mode objects.ChannelTagsMatchMode) {
		if len(ids) > 0 {
			selector = WithSelectedChannelsSelector(selector, ids)
		}
		if len(tags) > 0 {
			selector = WithChannelTagsFilterSelector(selector, tags, mode)
		}
	}
	if project := state.APIKey.Edges.Project; project != nil {
		if p := project.GetActiveProfile(); p != nil {
			apply(p.ChannelIDs, p.ChannelTags, p.ChannelTagsMatchMode)
		}
	}
	if p := state.APIKey.GetActiveProfile(); p != nil {
		apply(p.ChannelIDs, p.ChannelTags, p.ChannelTagsMatchMode)
	}
	candidates, err := selector.Select(ctx, req)
	if err != nil {
		return err
	}
	a.StructuralCandidates = []biz.ModelStickyCandidate{}
	for _, c := range sortModelStickyCandidates(candidates) {
		a.StructuralCandidates = append(a.StructuralCandidates, biz.ModelStickyCandidate{ID: c.Channel.ID, Name: c.Channel.Name, Models: c.Models})
	}
	return biz.ModelStickyRuntime.ReconcileResolved(ctx, biz.ModelStickyKey{APIKeyID: state.APIKey.ID, Model: req.Model}, a)
}

// Explicit disabled configured models must not be revived via legacy fallback
// only when their effective policy is our new strategy. Other policies retain
// the upstream fallback behavior and error contract.
func (s *DefaultSelector) modelStickyBlocksDisabledFallback(ctx context.Context, req *llm.Request) bool {
	key, ok := contexts.GetAPIKey(ctx)
	source, _ := contexts.GetSource(ctx)
	if !ok || source == request.SourceTest || source == request.SourcePlayground {
		return false
	}

	entity, err := s.ModelService.ModelStickyUnavailableModel(ctx, req.Model)
	if err != nil {
		return false
	}
	policy := &ModelRoutingPolicy{LoadBalancerStrategy: objects.RoutingPolicyDefault}
	if entity.Settings != nil {
		policy.LoadBalancerStrategy = entity.Settings.LoadBalancerStrategy
	}
	return deriveRoutingPolicy(s.SystemService.RetryPolicyOrDefault(ctx), key, policy).LoadBalancerStrategy == biz.LoadBalancerStrategyModelSticky
}
