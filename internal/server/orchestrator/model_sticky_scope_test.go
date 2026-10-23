package orchestrator

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/model"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/stretchr/testify/require"
)

func TestModelStickyRequestScopeAndOverrides(t *testing.T) {
	strategies := []string{biz.LoadBalancerStrategyAdaptive, biz.LoadBalancerStrategyFailover, biz.LoadBalancerStrategyCircuitBreaker, biz.LoadBalancerStrategyRoundRobin, biz.LoadBalancerStrategyModelSticky}
	for _, system := range strategies {
		for _, modelStrategy := range append([]string{"default"}, strategies...) {
			for _, keyStrategy := range append([]string{"default"}, strategies...) {
				for _, source := range []request.Source{request.SourceAPI, request.SourceTest, request.SourcePlayground} {
					for _, hasKey := range []bool{false, true} {
						for _, resolved := range []bool{false, true} {
							retry := &biz.RetryPolicy{LoadBalancerStrategy: system, TraceStickyMode: biz.TraceStickyPreferPreviousChannel}
							var key *ent.APIKey
							if hasKey {
								key = &ent.APIKey{ID: 42, Profiles: &objects.APIKeyProfiles{ActiveProfile: "default", Profiles: []objects.APIKeyProfile{{Name: "default", LoadBalanceStrategy: &keyStrategy}}}}
							}
							var mp *ModelRoutingPolicy
							if resolved {
								mp = &ModelRoutingPolicy{LoadBalancerStrategy: modelStrategy}
							}
							candidates := []*ChannelModelsCandidate{{ModelRoutingPolicy: mp}}
							ctx := contexts.WithSource(context.Background(), source)
							want := deriveRoutingPolicy(retry, key, mp)
							if want.LoadBalancerStrategy == biz.LoadBalancerStrategyModelSticky && (!hasKey || !resolved || source != request.SourceAPI) {
								want.LoadBalancerStrategy = biz.LoadBalancerStrategyAdaptive
							}
							require.Equal(t, want, deriveRequestRoutingPolicy(ctx, retry, key, candidates), "system=%s model=%s key=%s source=%s hasKey=%v resolved=%v", system, modelStrategy, keyStrategy, source, hasKey, resolved)
						}
					}
				}
			}
		}
	}
	// No invented API Key and no premature activation before model resolution.
	require.Nil(t, modelStickyModelPolicy(nil))
	require.Nil(t, modelStickyModelPolicy([]*ChannelModelsCandidate{nil}))
}

func TestModelStickyRealModelProvenanceAndProcess(t *testing.T) {
	stickyFreshRuntime(t)
	ctx, client := setupTest(t)
	project := createTestProject(t, ctx, client)
	ctx = contexts.WithProjectID(ctx, project.ID)
	user, err := client.User.Create().SetEmail("sticky-entry@example.com").SetPassword("test-password").Save(ctx)
	require.NoError(t, err)
	key, err := client.APIKey.Create().SetName("entry-key").SetKey("sk-entry-fixture").SetProjectID(project.ID).SetUserID(user.ID).Save(ctx)
	require.NoError(t, err)
	ctx = contexts.WithAPIKey(ctx, key)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(buildMockOpenAIResponse("entry", "same-name", "hello", 1, 1))
	}))
	t.Cleanup(srv.Close)
	ch, err := client.Channel.Create().SetType(channel.TypeOpenai).SetName("entry-channel").SetBaseURL(srv.URL + "/v1").SetCredentials(objects.ChannelCredentials{APIKey: "fixture-upstream"}).SetSupportedModels([]string{"same-name", "channel-only"}).SetDefaultTestModel("same-name").SetStatus(channel.StatusEnabled).Save(ctx)
	require.NoError(t, err)
	_, err = client.Model.Create().SetDeveloper("openai").SetModelID("same-name").SetType(model.TypeChat).SetName("same-name").SetIcon("openai").SetGroup("entry").SetModelCard(&objects.ModelCard{}).SetSettings(&objects.ModelSettings{Associations: []*objects.ModelAssociation{{Type: "channel_model", ChannelModel: &objects.ChannelModelAssociation{ChannelID: ch.ID, ModelID: "same-name"}}}}).SetStatus(model.StatusEnabled).Save(ctx)
	require.NoError(t, err)
	cs := newTestChannelServiceForChannels(client)
	ss := newTestSystemService(client)
	require.NoError(t, ss.SetModelSettings(ctx, biz.SystemModelSettings{FallbackToChannelsOnModelNotFound: true}))
	require.NoError(t, ss.SetRetryPolicy(ctx, &biz.RetryPolicy{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky}))
	selector := NewDefaultSelector(cs, newTestModelService(client), ss)
	specified := NewSpecifiedChannelSelector(cs, objects.GUID{Type: "Channel", ID: ch.ID})
	for _, tc := range []struct {
		name, model string
		selector    CandidateSelector
		resolved    bool
	}{
		{"configured", "same-name", selector, true},
		{"specified-same-name", "same-name", specified, false},
		{"legacy-channel-fallback", "channel-only", selector, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidates, err := tc.selector.Select(ctx, &llm.Request{Model: tc.model, RequestType: llm.RequestTypeChat, APIFormat: llm.APIFormatOpenAIChatCompletion})
			require.NoError(t, err)
			require.NotEmpty(t, candidates)
			require.Equal(t, tc.resolved, modelStickyModelPolicy(candidates) != nil)
			process := newTestOrchestrator(t, tc.selector, client, httpclient.NewHttpClient())
			process.SystemService = ss
			protection := biz.NewPromptProtectionRuleService(biz.PromptProtectionRuleServiceParams{Ent: client})
			t.Cleanup(protection.Stop)
			process.PromptProtecter = protection
			before := biz.ModelStickyRuntime.Snapshots()
			result, err := process.Process(ctx, buildTestRequest(tc.model, "hello", false))
			require.NoError(t, err)
			require.NotNil(t, result.ChatCompletion)
			if tc.resolved {
				require.Len(t, biz.ModelStickyRuntime.Snapshots(), 1)
			} else {
				require.Equal(t, before, biz.ModelStickyRuntime.Snapshots(), "direct channel requests must not touch existing Key/model state")
			}
		})
	}
	require.Equal(t, int32(3), calls.Load())
}

func TestModelStickyExcludedPreparePreservesExistingState(t *testing.T) {
	for _, source := range []request.Source{request.SourceAPI, request.SourceTest, request.SourcePlayground} {
		for _, hasKey := range []bool{false, true} {
			for _, resolved := range []bool{false, true} {
				if source == request.SourceAPI && hasKey && resolved {
					continue
				}
				stickyFreshRuntime(t)
				ctx := contexts.WithSource(context.Background(), source)
				key := biz.ModelStickyKey{APIKeyID: 1, Model: "A"}
				lease, err := biz.ModelStickyRuntime.Acquire(ctx, key, "existing", []biz.ModelStickyCandidate{{ID: 2}}, nil, true)
				require.NoError(t, err)
				lease.Success()
				before := biz.ModelStickyRuntime.Snapshots()
				state := stickyTestState(2)
				if !hasKey {
					state.APIKey = nil
				}
				if !resolved {
					for _, c := range state.ChannelModelsCandidates {
						c.ModelRoutingPolicy = nil
					}
				}
				require.NoError(t, prepareModelSticky(ctx, state, &llm.Request{Model: "A"}))
				require.Nil(t, state.modelSticky)
				require.Equal(t, biz.LoadBalancerStrategyAdaptive, state.RoutingPolicy.LoadBalancerStrategy)
				require.Equal(t, before, biz.ModelStickyRuntime.Snapshots())
			}
		}
	}
}

func TestModelStickyChineseErrorContracts(t *testing.T) {
	cause := biz.ErrModelStickyRequestConflict
	for _, message := range []string{"固定渠道不符合本次请求条件。", biz.ModelStickyMessagePrefix + "固定渠道不符合本次请求条件。"} {
		err := newModelStickyRoutingError(409, "model_sticky_request_conflict", message, 7, cause)
		var typed *ModelStickyRoutingError
		var response *llm.ResponseError
		require.ErrorAs(t, err, &typed)
		require.ErrorAs(t, err, &response)
		require.ErrorIs(t, err, cause)
		require.Equal(t, 409, response.StatusCode)
		require.Equal(t, 7, typed.RetryAfterSeconds)
		require.Equal(t, "model_sticky_request_conflict", response.Detail.Code)
		require.True(t, strings.HasPrefix(response.Detail.Message, biz.ModelStickyMessagePrefix))
		require.Equal(t, 1, strings.Count(response.Detail.Message, biz.ModelStickyMessagePrefix))
	}
	require.Contains(t, (&biz.ModelStickyUnavailable{RetryAfter: 1500 * time.Millisecond}).Error(), "2 秒")
	require.Contains(t, (&biz.ModelStickyUnavailable{}).Error(), "1 秒")
	for _, err := range []error{biz.ErrModelStickyNoCandidates, biz.ErrModelStickyRequestConflict, biz.ErrModelStickySettingsChanged} {
		require.True(t, strings.HasPrefix(err.Error(), biz.ModelStickyMessagePrefix))
	}
	upstream := &httpclient.Error{StatusCode: 429, Body: []byte(`{"error":{"message":"original provider rate limit"}}`)}
	require.False(t, errors.Is(upstream, biz.ErrModelStickyRequestConflict))
	require.NotContains(t, upstream.Error(), biz.ModelStickyMessagePrefix)
}

func TestModelStickyStreamConflictHasScopedChineseMessage(t *testing.T) {
	for _, strategy := range []string{biz.LoadBalancerStrategyAdaptive, biz.LoadBalancerStrategyModelSticky} {
		candidate := &ChannelModelsCandidate{ModelRoutingPolicy: &ModelRoutingPolicy{}, Channel: &biz.Channel{Channel: &ent.Channel{ID: 1, Policies: objects.ChannelPolicies{Stream: objects.CapabilityPolicyRequire}}}}
		selector := &modelStickyCompatibilitySelector{wrapped: modelStickyResolvedCandidates{[]*ChannelModelsCandidate{candidate}}, policy: stickyRevisionPolicy{strategy: strategy}, key: &ent.APIKey{ID: 42}}
		_, err := selector.Select(context.Background(), &llm.Request{Model: "A", RequestType: llm.RequestTypeDecisions})
		require.Error(t, err)
		var conflict *StreamPolicyConflictError
		require.ErrorAs(t, err, &conflict)
		require.Equal(t, "A", conflict.ModelName)
		require.Equal(t, strategy == biz.LoadBalancerStrategyModelSticky, strings.HasPrefix(err.Error(), biz.ModelStickyMessagePrefix))
	}
}
