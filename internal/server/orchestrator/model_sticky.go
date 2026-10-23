package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/looplj/axonhub/llm/pipeline"
	"github.com/looplj/axonhub/llm/streams"
)

type modelStickyRequest struct {
	key         biz.ModelStickyKey
	keyName     string
	candidates  []*ChannelModelsCandidate
	public      []biz.ModelStickyCandidate
	tried       map[int]bool
	lease       *biz.ModelStickyLease
	anchor      *biz.ModelStickyLease
	stopCancel  func() bool
	lastErr     error
	finalErr    error
	waitTimeout time.Duration
	admission   *biz.ModelStickyAdmission
}

func modelStickyEnabled(state *PersistenceState) bool {
	return state != nil && state.APIKey != nil && modelStickyModelPolicy(state.ChannelModelsCandidates) != nil && state.RoutingPolicy.LoadBalancerStrategy == biz.LoadBalancerStrategyModelSticky
}

func sortModelStickyCandidates(candidates []*ChannelModelsCandidate) []*ChannelModelsCandidate {
	result := slices.Clone(candidates)
	slices.SortStableFunc(result, func(a, b *ChannelModelsCandidate) int {
		if a.Priority != b.Priority {
			if a.Priority < b.Priority {
				return -1
			}
			return 1
		}
		if a.Channel.OrderingWeight != b.Channel.OrderingWeight {
			if a.Channel.OrderingWeight > b.Channel.OrderingWeight {
				return -1
			}
			return 1
		}
		if a.Channel.ID < b.Channel.ID {
			return -1
		}
		if a.Channel.ID > b.Channel.ID {
			return 1
		}
		return 0
	})
	// One channel turn, but retain every association's ordered model metadata.
	grouped := make([]*ChannelModelsCandidate, 0, len(result))
	seen := make(map[int]*ChannelModelsCandidate)
	for _, c := range result {
		target := seen[c.Channel.ID]
		if target == nil {
			clone := *c
			clone.Models = nil
			clone.modelAPIFormats = nil
			clone.modelStickyDefaults = nil
			target = &clone
			seen[c.Channel.ID] = target
			grouped = append(grouped, target)
		}
		for i, entry := range c.Models {
			format := c.APIFormat
			if i < len(c.modelAPIFormats) {
				format = c.modelAPIFormats[i]
			}
			duplicate := false
			for j, existing := range target.Models {
				if existing == entry && target.modelAPIFormats[j] == format && target.modelStickyDefaults[j] == c.DefaultMaxTokens {
					duplicate = true
					break
				}
			}
			if duplicate {
				continue
			}
			target.Models = append(target.Models, entry)
			target.modelAPIFormats = append(target.modelAPIFormats, format)
			target.modelStickyDefaults = append(target.modelStickyDefaults, c.DefaultMaxTokens)
		}
	}
	return grouped
}

func prepareModelSticky(ctx context.Context, state *PersistenceState, req *llm.Request) error {
	if state == nil || state.RoutingPolicy.LoadBalancerStrategy != biz.LoadBalancerStrategyModelSticky {
		return nil
	}
	if !modelStickyRequestEligible(ctx, state.APIKey, state.ChannelModelsCandidates) {
		state.RoutingPolicy.LoadBalancerStrategy = biz.LoadBalancerStrategyAdaptive
		return nil
	}
	session := &modelStickyRequest{key: biz.ModelStickyKey{APIKeyID: state.APIKey.ID, Model: req.Model}, keyName: state.APIKey.Name,
		candidates: state.ChannelModelsCandidates, tried: make(map[int]bool), waitTimeout: 30 * time.Second}
	if state.RetryPolicyProvider != nil {
		policy := state.RetryPolicyProvider.RetryPolicyOrDefault(ctx)
		seconds := policy.NonStreamResponseTimeoutSeconds
		if req.Stream != nil && *req.Stream {
			seconds = policy.StreamFirstEventTimeoutSeconds
		}
		if seconds > 0 {
			session.waitTimeout = time.Duration(min(seconds, 600)) * time.Second
		}
	}
	for _, c := range session.candidates {
		session.public = append(session.public, biz.ModelStickyCandidate{ID: c.Channel.ID, Name: c.Channel.Name, Models: slices.Clone(c.Models)})
	}
	inherited := true
	if profile := state.APIKey.GetActiveProfile(); profile != nil && profile.LoadBalanceStrategy != nil {
		strategy := objects.NormalizeRoutingPolicyValue(*profile.LoadBalanceStrategy)
		if strategy != objects.RoutingPolicyDefault && objects.IsValidLoadBalancerStrategy(strategy) {
			inherited = false
		}
	}
	if policy := resolvedModelRoutingPolicy(ctx, session.candidates); policy != nil {
		strategy := objects.NormalizeRoutingPolicyValue(policy.LoadBalancerStrategy)
		if strategy != objects.RoutingPolicyDefault && objects.IsValidLoadBalancerStrategy(strategy) {
			inherited = false
		}
	}
	if state.modelStickyConfig == nil {
		state.modelStickyConfig = biz.ModelStickyRuntime.ConfigSnapshot(ctx)
	}
	systemStrategy := ""
	if state.RetryPolicyProvider != nil {
		systemStrategy = state.RetryPolicyProvider.RetryPolicyOrDefault(ctx).LoadBalancerStrategy
	}
	session.admission = biz.NewModelStickyAdmission(state.modelStickyConfig, state.APIKey, inherited, systemStrategy)
	session.admission.QuotaExcludedChannelID = state.RoutingPolicy.modelStickyQuotaExcluded
	if err := populateModelStickyAdmission(ctx, state, req, session.admission); err != nil {
		return newModelStickyRoutingError(http.StatusConflict, "model_sticky_settings_changed", biz.ErrModelStickySettingsChanged.Error(), 0, err)
	}
	state.modelSticky = session
	candidate, err := session.acquire(ctx, true)
	if err != nil {
		return err
	}

	state.ChannelModelsCandidates = []*ChannelModelsCandidate{candidate}
	state.CurrentCandidateIndex = 0
	if state.ChannelService != nil {
		state.ChannelService.IncrementChannelSelection(candidate.Channel.ID)
	}
	return nil
}

func (s *modelStickyRequest) acquire(ctx context.Context, fresh bool) (*ChannelModelsCandidate, error) {
	timeout := s.waitTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	lease, err := biz.ModelStickyRuntime.Acquire(biz.WithModelStickyAdmission(waitCtx, s.admission), s.key, s.keyName, s.public, s.tried, fresh, s.anchor)
	cancel() // Only waiting is bounded; the acquired lease belongs to original ctx.
	if err != nil {
		if errors.Is(err, biz.ErrModelStickyNoCandidates) {
			return nil, newModelStickyRoutingError(http.StatusServiceUnavailable, "model_sticky_no_available_channel", "当前没有可用候选渠道，请检查模型关联、渠道权限及冷却状态。", 0, err)
		}
		var cooling *biz.ModelStickyUnavailable
		if errors.As(err, &cooling) {
			seconds := max(1, int((cooling.RetryAfter+time.Second-1)/time.Second))
			return nil, newModelStickyRoutingError(http.StatusServiceUnavailable, "model_sticky_cooldown", fmt.Sprintf("符合条件的渠道已尝试或处于冷却中，请在 %d 秒后重试。", seconds), seconds, err)
		}
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, newModelStickyRoutingError(http.StatusServiceUnavailable, "model_sticky_probe_busy", "等待当前渠道探测超时，本次尚未获得探测执行权，请稍后重试。", 1, err)
		}
		if errors.Is(err, biz.ErrModelStickyRequestConflict) {
			return nil, newModelStickyRoutingError(http.StatusConflict, "model_sticky_request_conflict", err.Error(), 0, err)
		}
		if errors.Is(err, biz.ErrModelStickySettingsChanged) {
			return nil, newModelStickyRoutingError(http.StatusConflict, "model_sticky_settings_changed", err.Error(), 0, err)
		}
		return nil, err
	}
	s.lease = lease
	if s.anchor == nil {
		s.anchor = lease
	}
	s.stopCancel = context.AfterFunc(ctx, func() { lease.Abandon() })
	s.tried[lease.ChannelID] = true
	s.lastErr = nil
	for _, candidate := range s.candidates {
		if candidate.Channel.ID == lease.ChannelID {
			if len(lease.Models) > 0 && len(lease.Models) != len(candidate.Models) {
				clone := *candidate
				clone.Models = nil
				clone.modelAPIFormats = nil
				clone.modelStickyDefaults = nil
				for i, entry := range candidate.Models {
					if !slices.Contains(lease.Models, entry) {
						continue
					}
					clone.Models = append(clone.Models, entry)
					format := candidate.APIFormat
					if i < len(candidate.modelAPIFormats) {
						format = candidate.modelAPIFormats[i]
					}
					defaults := candidate.DefaultMaxTokens
					if i < len(candidate.modelStickyDefaults) {
						defaults = candidate.modelStickyDefaults[i]
					}
					clone.modelAPIFormats = append(clone.modelAPIFormats, format)
					clone.modelStickyDefaults = append(clone.modelStickyDefaults, defaults)
				}
				return &clone, nil
			}
			return candidate, nil
		}
	}
	s.release(false, false)
	return nil, fmt.Errorf("【新增策略：模型固定渠道】路由选择结果不在本次授权候选范围内，已阻止调用。")
}

func (s *modelStickyRequest) release(success, failure bool) {
	if s == nil || s.lease == nil {
		return
	}
	if s.stopCancel != nil {
		s.stopCancel()
		s.stopCancel = nil
	}
	if success {
		s.lease.Success()
	} else if failure {
		s.lease.Failure()
	} else {
		s.lease.Abandon()
	}
	s.lease = nil
}

func checkModelStickyAttempt(state *PersistenceState) error {
	if state.modelSticky != nil && !state.modelSticky.lease.Active() {
		return newModelStickyRoutingError(http.StatusConflict, "model_sticky_settings_changed", biz.ErrModelStickySettingsChanged.Error(), 0, biz.ErrModelStickySettingsChanged)
	}
	return nil
}

func modelStickyRetryable(ctx context.Context, state *PersistenceState, err error) bool {
	if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return false
	}
	if isChannelQueueError(err) || isLocalRPMExhaustedError(err) {
		return false
	}
	if errors.Is(err, pipeline.ErrEmptyResponse) || errors.Is(err, pipeline.ErrEmptyStreamChunks) || errors.Is(err, pipeline.ErrEmptyAggregatedBody) || errors.Is(err, llm.ErrStreamIncomplete) ||
		errors.Is(err, pipeline.ErrStreamFirstEventTimeout) || errors.Is(err, pipeline.ErrNonStreamResponseTimeout) {
		return true
	}
	if state.CurrentCandidate == nil {
		return false
	}
	return modelStickyModelUnavailable(err) || isRetryableErrorForChannel(err, state.CurrentCandidate.Channel)
}

// ObserveAttempt is intentionally a no-op for the existing routing strategies.
func (p *PersistentOutboundTransformer) ObserveAttempt(_ context.Context, err error) {
	if p.state.modelSticky != nil {
		p.state.modelSticky.lastErr = err
	}
}

func (p *PersistentOutboundTransformer) nextModelStickyChannel(ctx context.Context) error {
	s := p.state.modelSticky
	retryable := modelStickyRetryable(ctx, p.state, s.lastErr)
	s.release(false, retryable)
	if !retryable {
		return errors.New("【新增策略：模型固定渠道】本次错误不符合故障接力条件，不再切换渠道。")
	}
	candidate, err := s.acquire(ctx, false)
	if err != nil {
		s.finalErr = err
		return err
	}
	// Keep normal candidate/model reset, persistence, format and accounting logic.
	current := p.state.CurrentCandidate
	p.state.ChannelModelsCandidates = []*ChannelModelsCandidate{current, candidate}
	p.state.CurrentCandidateIndex = 0
	return p.advanceCandidate(ctx)
}

func (p *PersistentOutboundTransformer) NextChannel(ctx context.Context) error {
	if p.state.modelSticky != nil {
		return p.nextModelStickyChannel(ctx)
	}
	return p.advanceCandidate(ctx)
}

// finishModelSticky is called even when retry budgets are zero or exhausted.
func finishModelSticky(ctx context.Context, state *PersistenceState, result *pipeline.Result, err error) error {
	s := state.modelSticky
	if s == nil {
		return err
	}
	if err != nil {
		s.release(false, modelStickyRetryable(ctx, state, err))
		if s.finalErr != nil {
			return s.finalErr
		}
		return err
	}
	if result != nil && result.Stream && result.EventStream != nil {
		headers := httpclient.GetResponseHeaders(result.EventStream)
		expected := ExpectedStreamChoices(state.Request)
		if provider, ok := result.EventStream.(interface{ ExpectedStreamChoices() int }); ok {
			expected = provider.ExpectedStreamChoices()
		}
		result.EventStream = httpclient.WithResponseHeadersAndExpectedChoices(&modelStickyStream{Stream: result.EventStream, ctx: ctx, state: state}, headers, expected)
		return nil
	}
	s.release(result != nil && result.Response != nil, false)
	return nil
}

// A cold probe stays exclusive until a validated stream terminal. Normal
// requests on an already healthy pin are never serialized by this wrapper.
//
//nolint:containedctx // The stream owns the request lifetime.
type modelStickyStream struct {
	streams.Stream[*httpclient.StreamEvent]
	ctx      context.Context
	state    *PersistenceState
	once     sync.Once
	closeErr error
}

func (s *modelStickyStream) Next() bool {
	if s.Stream.Next() {
		return true
	}
	_ = s.Close()
	return false
}
func (s *modelStickyStream) Close() error {
	s.once.Do(func() {
		err := s.Stream.Err()
		s.closeErr = s.Stream.Close()
		healthy := s.state.Perf != nil && s.state.Perf.RequestCompleted && s.state.Perf.Success
		if healthy {
			s.state.modelSticky.release(true, false)
			return
		}
		// A committed stream cannot be replayed. Its abnormal EOF still affects
		// the next request, unless the client canceled or closed it early.
		if err == nil {
			err = s.closeErr
		}
		if err == nil && s.state.OutboundStreamTerminal == streamTerminalFailed {
			err = llm.ErrStreamIncomplete
		}
		s.state.modelSticky.release(false, modelStickyRetryable(s.ctx, s.state, err))
	})
	return s.closeErr
}
