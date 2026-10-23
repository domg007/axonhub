package orchestrator

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/looplj/axonhub/internal/contexts"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/request"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/internal/server/biz"
	"github.com/looplj/axonhub/llm"
	"github.com/looplj/axonhub/llm/httpclient"
	"github.com/stretchr/testify/require"
)

// Management entry regression: every request must retain its original semantics.
func TestModelStickyManagementEntries(t *testing.T) {
	for _, retryEnabled := range []bool{false, true} {
		for _, streaming := range []bool{false, true} {
			for _, strategy := range []string{biz.LoadBalancerStrategyAdaptive, biz.LoadBalancerStrategyModelSticky, "model-override"} {
				for _, entry := range []string{"channel-test", "single-upstream-key", "batch-upstream-keys", "playground-auto", "playground-specified"} {
					if strategy == "model-override" && entry != "playground-auto" {
						continue
					}
					t.Run(fmt.Sprintf("retry=%t/stream=%t/%s/%s", retryEnabled, streaming, strategy, entry), func(t *testing.T) {
						stickyFreshRuntime(t)
						ctx, client := setupTest(t)
						ctx = contexts.WithProjectID(ctx, createTestProject(t, ctx, client).ID)
						ctx = contexts.WithSource(ctx, request.SourceTest)
						var calls atomic.Int32
						srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							calls.Add(1)
							if streaming {
								w.Header().Set("Content-Type", "text/event-stream")
								_, _ = w.Write([]byte("data: {\"id\":\"test\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\ndata: {\"id\":\"test\",\"model\":\"test-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\ndata: [DONE]\n\n"))
								return
							}
							w.Header().Set("Content-Type", "application/json")
							_, _ = w.Write(buildMockOpenAIResponse("audit", "test-model", "hello", 1, 1))
						}))
						t.Cleanup(srv.Close)
						policies := objects.ChannelPolicies{}
						if streaming {
							policies.Stream = objects.CapabilityPolicyRequire
						}
						ch, err := client.Channel.Create().SetType(channel.TypeOpenai).SetName("audit").
							SetBaseURL(srv.URL + "/v1").SetCredentials(objects.ChannelCredentials{APIKeys: []string{"audit-key-1", "audit-key-2"}}).
							SetSupportedModels([]string{"test-model"}).SetDefaultTestModel("test-model").SetPolicies(policies).Save(ctx)
						require.NoError(t, err)
						cs, rs, ss, us := setupTestServices(t, client)
						systemStrategy := strategy
						if strategy == "model-override" {
							systemStrategy = biz.LoadBalancerStrategyAdaptive
						}
						require.NoError(t, ss.SetRetryPolicy(ctx, &biz.RetryPolicy{Enabled: retryEnabled, LoadBalancerStrategy: systemStrategy}))
						protection := biz.NewPromptProtectionRuleService(biz.PromptProtectionRuleServiceParams{Ent: client})
						t.Cleanup(protection.Stop)
						p := NewTestChannelOrchestrator(cs, rs, ss, us, protection, httpclient.NewHttpClient())
						id := objects.GUID{Type: "Channel", ID: ch.ID}
						switch entry {
						case "channel-test":
							result, err := p.TestChannel(ctx, id, nil, nil)
							require.NoError(t, err)
							t.Logf("success=%v error=%v upstreamCalls=%d", result.Success, result.Error, calls.Load())
							if result.Error != nil {
								t.Logf("message=%s", *result.Error)
							}
							require.True(t, result.Success, "admin channel testing must not require a client API key")
						case "single-upstream-key":
							result, err := p.TestSingleAPIKey(ctx, id, "audit-key-1", nil, nil)
							require.NoError(t, err)
							if result.Error != nil {
								t.Logf("message=%s", *result.Error)
							}
							t.Logf("success=%v upstreamCalls=%d", result.Success, calls.Load())
							require.True(t, result.Success, "an upstream credential is not an AxonHub client API key")
						case "batch-upstream-keys":
							result, err := p.TestChannelAPIKeys(ctx, id, nil, nil)
							require.NoError(t, err)
							for _, key := range result.Results {
								if key.Error != nil {
									t.Logf("message=%s", *key.Error)
								}
							}
							t.Logf("successCount=%d failedCount=%d upstreamCalls=%d", result.SuccessCount, result.FailedCount, calls.Load())
							require.Equal(t, 2, result.SuccessCount, "admin batch tests must reach the selected channel")
						default:
							ctx = contexts.WithSource(ctx, request.SourcePlayground)
							var selector CandidateSelector
							if entry == "playground-specified" {
								selector = NewSpecifiedChannelSelector(cs, id)
							} else {
								channel, err := cs.GetChannel(ctx, ch.ID)
								require.NoError(t, err)
								var modelPolicy *ModelRoutingPolicy
								if strategy == "model-override" {
									modelPolicy = &ModelRoutingPolicy{LoadBalancerStrategy: biz.LoadBalancerStrategyModelSticky}
								}
								selector = &mockSelector{candidates: []*ChannelModelsCandidate{{Channel: channel, ModelRoutingPolicy: modelPolicy, APIFormat: string(llm.APIFormatOpenAIChatCompletion), Models: []biz.ChannelModelEntry{{RequestModel: "test-model", ActualModel: "test-model"}}}}}
							}
							// Same production Process entry as PlaygroundHandlers; auto mode uses a
							// resolved candidate fixture instead of testing the model association DB.
							process := newTestOrchestrator(t, selector, client, httpclient.NewHttpClient())
							process.SystemService = ss
							process.PromptProtecter = protection
							result, err := process.Process(ctx, buildTestRequest("test-model", "hello", streaming))
							t.Logf("error=%v upstreamCalls=%d", err, calls.Load())
							require.NoError(t, err, "JWT-authenticated playground must not require a client API key")
							if streaming {
								require.NotNil(t, result.ChatCompletionStream)
								for result.ChatCompletionStream.Next() {
								}
								require.NoError(t, result.ChatCompletionStream.Err())
								require.NoError(t, result.ChatCompletionStream.Close())
							} else {
								require.NotNil(t, result.ChatCompletion)
							}
						}
						require.Greater(t, calls.Load(), int32(0))
						require.Empty(t, biz.ModelStickyRuntime.Snapshots(), "admin tests/playground without client keys must not create sticky rows")
					})
				}
			}
		}

	}
}

// Failed explicit-channel requests must not escape to another healthy channel,
// including admin tests of disabled channels and an existing sticky route.
func TestModelStickyExplicitFailureNeverSwitchesChannel(t *testing.T) {
	for _, strategy := range []string{biz.LoadBalancerStrategyAdaptive, biz.LoadBalancerStrategyModelSticky} {
		for _, disabled := range []bool{false, true} {
			for _, entry := range []string{"channel-test", "playground-specified"} {
				t.Run(fmt.Sprintf("%s/disabled=%t/%s", strategy, disabled, entry), func(t *testing.T) {
					stickyFreshRuntime(t)
					ctx, client := setupTest(t)
					ctx = contexts.WithProjectID(ctx, createTestProject(t, ctx, client).ID)
					var failedCalls, healthyCalls atomic.Int32
					failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						failedCalls.Add(1)
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(500)
						_, _ = w.Write([]byte(`{"error":{"message":"original upstream fixture failure","type":"server_error"}}`))
					}))
					t.Cleanup(failed.Close)
					healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						healthyCalls.Add(1)
						w.Header().Set("Content-Type", "application/json")
						_, _ = w.Write(buildMockOpenAIResponse("healthy", "test-model", "hello", 1, 1))
					}))
					t.Cleanup(healthy.Close)
					status := channel.StatusEnabled
					if disabled {
						status = channel.StatusDisabled
					}
					selected, err := client.Channel.Create().SetType(channel.TypeOpenai).SetName("selected").SetBaseURL(failed.URL + "/v1").SetCredentials(objects.ChannelCredentials{APIKey: "fixture-key"}).SetSupportedModels([]string{"test-model"}).SetDefaultTestModel("test-model").SetStatus(status).Save(ctx)
					require.NoError(t, err)
					spare, err := client.Channel.Create().SetType(channel.TypeOpenai).SetName("spare").SetBaseURL(healthy.URL + "/v1").SetCredentials(objects.ChannelCredentials{APIKey: "fixture-spare"}).SetSupportedModels([]string{"test-model"}).SetDefaultTestModel("test-model").SetStatus(channel.StatusEnabled).Save(ctx)
					require.NoError(t, err)
					cs, rs, ss, us := setupTestServices(t, client)
					require.NoError(t, ss.SetRetryPolicy(ctx, &biz.RetryPolicy{Enabled: true, MaxChannelRetries: 3, MaxSingleChannelRetries: 1, LoadBalancerStrategy: strategy}))
					lease, err := biz.ModelStickyRuntime.Acquire(ctx, biz.ModelStickyKey{APIKeyID: 77, Model: "test-model"}, "existing", []biz.ModelStickyCandidate{{ID: spare.ID}}, nil, true)
					require.NoError(t, err)
					lease.Success()
					before := biz.ModelStickyRuntime.Snapshots()
					protection := biz.NewPromptProtectionRuleService(biz.PromptProtectionRuleServiceParams{Ent: client})
					t.Cleanup(protection.Stop)
					id := objects.GUID{Type: "Channel", ID: selected.ID}
					if entry == "channel-test" {
						ctx = contexts.WithSource(ctx, request.SourceTest)
						tester := NewTestChannelOrchestrator(cs, rs, ss, us, protection, httpclient.NewHttpClient())
						result, err := tester.TestChannel(ctx, id, nil, nil)
						require.NoError(t, err)
						require.False(t, result.Success)
						require.NotNil(t, result.Error)
						require.Contains(t, *result.Error, "original upstream fixture failure")
						require.NotContains(t, *result.Error, biz.ModelStickyMessagePrefix)
					} else {
						ctx = contexts.WithSource(ctx, request.SourcePlayground)
						process := newTestOrchestrator(t, NewSpecifiedChannelSelector(cs, id), client, httpclient.NewHttpClient())
						process.SystemService = ss
						process.PromptProtecter = protection
						_, err := process.Process(ctx, buildTestRequest("test-model", "hello", false))
						require.Error(t, err)
						require.Contains(t, err.Error(), "original upstream fixture failure")
						require.NotContains(t, err.Error(), biz.ModelStickyMessagePrefix)
					}
					require.Greater(t, failedCalls.Load(), int32(0), "disabled explicit channels remain testable")
					require.Zero(t, healthyCalls.Load(), "must not silently test another channel")
					require.Equal(t, before, biz.ModelStickyRuntime.Snapshots(), "no pin, cooldown, round or probe mutation")
				})
			}
		}
	}
}
