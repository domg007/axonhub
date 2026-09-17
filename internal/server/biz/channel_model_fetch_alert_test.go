//nolint:exhaustruct_v5 // Test fixtures intentionally set only fields relevant to each scenario.
package biz

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/ent/enttest"
	"github.com/looplj/axonhub/internal/objects"
	"github.com/looplj/axonhub/llm/httpclient"
)

// webhookRecorder 是一个假的 webhook 接收端，记录收到的每个请求体。
type webhookRecorder struct {
	server *httptest.Server
	mu     sync.Mutex
	bodies []string
}

func newWebhookRecorder(t *testing.T) *webhookRecorder {
	t.Helper()

	rec := &webhookRecorder{}
	rec.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		rec.mu.Lock()
		rec.bodies = append(rec.bodies, string(body))
		rec.mu.Unlock()

		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(rec.server.Close)

	return rec
}

func (r *webhookRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.bodies)
}

func (r *webhookRecorder) body(i int) string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.bodies[i]
}

// 与管理台里给 Discord 用的模板同款：content 字段 + 用 {{.Event}} 区分事件。
const discordLikeTemplate = `{"content": "{{if eq .Event "channel.model_fetch_recovered"}}recovered{{else}}failed{{end}} | {{.Channel.Name}} | {{.Trigger.Type}} | {{.Trigger.Reason}} | {{.OccurredAt}}"}`

// newModelFetchAlertFixture 装配一个 ChannelService，其 webhook 配置只订阅了 channel.auto_disabled，
// 用来验证「回退到 auto_disabled 订阅者」这条规则。
func newModelFetchAlertFixture(t *testing.T, rec *webhookRecorder, subscribedEvent string) (*ChannelService, *ent.Client, context.Context) {
	t.Helper()

	resetModelFetchAlertState()
	t.Cleanup(resetModelFetchAlertState)

	client := enttest.NewEntClient(t, "sqlite3", "file:ent?mode=memory&_fk=0")
	t.Cleanup(func() { client.Close() })

	cfg := WebhookNotifierConfig{
		Targets: []WebhookTarget{
			{
				Name:      "discord",
				Enabled:   true,
				URL:       rec.server.URL,
				TimeoutMs: 1000,
				Headers:   []objects.HeaderEntry{{Key: "Content-Type", Value: "application/json"}},
				Body:      discordLikeTemplate,
			},
		},
		Subscriptions: []WebhookSubscription{
			{Event: subscribedEvent, TargetNames: []string{"discord"}},
		},
	}

	systemService := newTestSystemServiceWithWebhookConfig(t, client, cfg)

	svc := newTestChannelService(client)
	svc.SystemService = systemService
	svc.WebhookNotifier = NewWebhookNotifier(systemService, httpclient.NewHttpClient())

	// 三条自动同步路径都是 System 身份，这里照样起一个。
	ctx := authz.WithSystemBypass(ent.NewContext(context.Background(), client), "test-model-fetch-alert")

	return svc, client, ctx
}

func testChannel(id int, typ channel.Type) *ent.Channel {
	return &ent.Channel{
		ID:      id,
		Name:    "upstream-" + typ.String(),
		Type:    typ,
		BaseURL: "http://example.invalid/v1",
		Status:  channel.StatusEnabled,
	}
}

func okResult(n int) *FetchModelsResult {
	models := make([]ModelIdentify, 0, n)
	for i := range n {
		models = append(models, ModelIdentify{ID: "m" + string(rune('a'+i))})
	}

	return &FetchModelsResult{Models: models}
}

func waitForWebhooks(t *testing.T, rec *webhookRecorder, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return rec.count() == want }, 3*time.Second, 10*time.Millisecond)
}

// assertNoWebhookSoon 给异步发送留一点时间，确认确实没有请求发出。
func assertNoWebhookSoon(t *testing.T, rec *webhookRecorder) {
	t.Helper()
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, 0, rec.count())
}

func TestEvaluateModelFetchOutcome(t *testing.T) {
	cases := []struct {
		name        string
		result      *FetchModelsResult
		err         error
		wantFailed  bool
		wantTrigger string
	}{
		{name: "go error", err: context.DeadlineExceeded, wantFailed: true, wantTrigger: modelFetchAlertTriggerError},
		{name: "nil result", wantFailed: true, wantTrigger: modelFetchAlertTriggerError},
		{name: "result error", result: &FetchModelsResult{Error: lo.ToPtr("failed to fetch models: 502")}, wantFailed: true, wantTrigger: modelFetchAlertTriggerError},
		{name: "fallback", result: &FetchModelsResult{Models: okResult(1).Models, Fallback: true}, wantFailed: true, wantTrigger: modelFetchAlertTriggerError},
		{name: "empty list", result: &FetchModelsResult{Models: []ModelIdentify{}}, wantFailed: true, wantTrigger: modelFetchAlertTriggerEmpty},
		{name: "ok", result: okResult(3), wantFailed: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := evaluateModelFetchOutcome(tc.result, tc.err)
			require.Equal(t, tc.wantFailed, got.failed)
			require.Equal(t, tc.wantTrigger, got.triggerType)

			if !tc.wantFailed {
				require.Equal(t, 3, got.modelCount)
			}
		})
	}
}

func TestRecordModelFetchOutcome_OnlyTransitionsNotify(t *testing.T) {
	resetModelFetchAlertState()
	t.Cleanup(resetModelFetchAlertState)

	failed := modelFetchOutcome{failed: true, reason: "x"}
	ok := modelFetchOutcome{modelCount: 1}

	require.False(t, recordModelFetchOutcome(1, ok), "healthy from the start: nothing to report")
	require.True(t, recordModelFetchOutcome(1, failed), "first failure notifies")
	require.False(t, recordModelFetchOutcome(1, failed), "still failing: deduplicated")
	require.True(t, recordModelFetchOutcome(1, ok), "recovery notifies once")
	require.False(t, recordModelFetchOutcome(1, ok), "still healthy: silent")
	require.True(t, recordModelFetchOutcome(2, failed), "channels are tracked independently")
}

func TestSanitizeModelFetchAlertReason(t *testing.T) {
	raw := "failed to fetch models: Get \"https://x/v1/models\":\n\tdial tcp:\\ connection   refused"
	got := sanitizeModelFetchAlertReason(raw)

	require.Equal(t, "failed to fetch models: Get 'https://x/v1/models': dial tcp:/ connection refused", got)
	require.NotContains(t, got, `"`)
	require.NotContains(t, got, "\\")
	require.NotContains(t, got, "\n")

	// 嵌进 JSON 字符串后必须仍是合法 JSON。
	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(`{"content":"`+got+`"}`), &payload))

	long := strings.Repeat("é", modelFetchAlertReasonMaxLen+50)
	require.Equal(t, modelFetchAlertReasonMaxLen+1, len([]rune(sanitizeModelFetchAlertReason(long))))
}

func TestModelFetchAlert_FailThenRecover_FallsBackToAutoDisabledSubscribers(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, _, ctx := newModelFetchAlertFixture(t, rec, EventChannelAutoDisabled)
	ch := testChannel(7, channel.TypeOpenai)

	// 第一次失败：发一条。
	svc.observeModelFetchForAlert(ctx, ch, &FetchModelsResult{Error: lo.ToPtr(`failed to fetch models: Get "http://x": dial tcp: connection refused`)}, nil)
	waitForWebhooks(t, rec, 1)

	var payload map[string]string
	require.NoError(t, json.Unmarshal([]byte(rec.body(0)), &payload), "rendered body must be valid JSON: %s", rec.body(0))
	require.Contains(t, payload["content"], "failed | upstream-openai | model_fetch_error | failed to fetch models: Get 'http://x': dial tcp: connection refused")

	// 继续失败（换成空列表）：不再发。
	svc.observeModelFetchForAlert(ctx, ch, &FetchModelsResult{Models: []ModelIdentify{}}, nil)
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, 1, rec.count())

	// 恢复：再发一条 recovered。
	svc.observeModelFetchForAlert(ctx, ch, okResult(2), nil)
	waitForWebhooks(t, rec, 2)
	require.NoError(t, json.Unmarshal([]byte(rec.body(1)), &payload))
	require.Contains(t, payload["content"], "recovered | upstream-openai | model_fetch_recovered | model fetch succeeded again (2 models)")

	// 恢复后保持正常：不发。
	svc.observeModelFetchForAlert(ctx, ch, okResult(2), nil)
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, 2, rec.count())
}

func TestModelFetchAlert_ExplicitSubscriptionWins(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, _, ctx := newModelFetchAlertFixture(t, rec, EventChannelModelFetchFailed)

	svc.observeModelFetchForAlert(ctx, testChannel(1, channel.TypeOpenai), &FetchModelsResult{Models: []ModelIdentify{}}, nil)
	waitForWebhooks(t, rec, 1)
	require.Contains(t, rec.body(0), "model_fetch_empty")
}

func TestModelFetchAlert_SkipsWhenContextExpired(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, _, ctx := newModelFetchAlertFixture(t, rec, EventChannelAutoDisabled)

	expired, cancel := context.WithCancel(ctx)
	cancel()

	svc.observeModelFetchForAlert(expired, testChannel(1, channel.TypeOpenai), &FetchModelsResult{Error: lo.ToPtr("context canceled")}, nil)
	assertNoWebhookSoon(t, rec)

	// 预算耗尽的失败不能把渠道标成失败态，否则之后真正恢复时会发一条无中生有的 recovered。
	svc.observeModelFetchForAlert(ctx, testChannel(1, channel.TypeOpenai), okResult(1), nil)
	assertNoWebhookSoon(t, rec)
}

func TestModelFetchAlert_SkipsNonSystemPrincipal(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, client, _ := newModelFetchAlertFixture(t, rec, EventChannelAutoDisabled)

	// 管理台按钮：登录用户身份。
	userCtx, err := authz.WithPrincipal(ent.NewContext(context.Background(), client), authz.Principal{Type: authz.PrincipalTypeUser, UserID: lo.ToPtr(1)})
	require.NoError(t, err)
	svc.observeModelFetchForAlert(userCtx, testChannel(1, channel.TypeOpenai), &FetchModelsResult{Models: []ModelIdentify{}}, nil)

	// 没有任何身份。
	svc.observeModelFetchForAlert(context.Background(), testChannel(1, channel.TypeOpenai), &FetchModelsResult{Models: []ModelIdentify{}}, nil)

	assertNoWebhookSoon(t, rec)
}

func TestModelFetchAlert_SkipsVolcengine(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, _, ctx := newModelFetchAlertFixture(t, rec, EventChannelAutoDisabled)

	svc.observeModelFetchForAlert(ctx, testChannel(1, channel.TypeVolcengine), &FetchModelsResult{Models: []ModelIdentify{}}, nil)
	assertNoWebhookSoon(t, rec)
}

func TestModelFetchAlert_NoSubscribersIsSilent(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, _, ctx := newModelFetchAlertFixture(t, rec, "some.other.event")

	svc.observeModelFetchForAlert(ctx, testChannel(1, channel.TypeOpenai), &FetchModelsResult{Models: []ModelIdentify{}}, nil)
	assertNoWebhookSoon(t, rec)
}

// 端到端：走真正的 syncChannelModels，上游返回空列表，应该只收到一条通知；
// 再跑一次仍为空，不重复；上游恢复后收到 recovered。
func TestModelFetchAlert_ThroughSyncChannelModels(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, client, ctx := newModelFetchAlertFixture(t, rec, EventChannelAutoDisabled)

	var (
		upstreamMu   sync.Mutex
		upstreamBody = `{"data":[]}`
	)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamMu.Lock()
		defer upstreamMu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(upstreamBody))
	}))
	defer upstream.Close()

	svc.httpClient = httpclient.NewHttpClientWithClient(upstream.Client())

	_, err := client.Channel.Create().
		SetType(channel.TypeOpenai).
		SetName("flaky upstream").
		SetBaseURL(upstream.URL).
		SetCredentials(objects.ChannelCredentials{APIKey: "test-key"}).
		SetSupportedModels([]string{"old-model"}).
		SetDefaultTestModel("old-model").
		SetAutoSyncSupportedModels(true).
		SetStatus(channel.StatusEnabled).
		Save(ctx)
	require.NoError(t, err)

	svc.syncChannelModels(ctx)
	waitForWebhooks(t, rec, 1)
	require.Contains(t, rec.body(0), "model_fetch_empty")

	svc.syncChannelModels(ctx)
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, 1, rec.count())

	upstreamMu.Lock()
	upstreamBody = `{"data":[{"id":"new-model"}]}`
	upstreamMu.Unlock()

	svc.syncChannelModels(ctx)
	waitForWebhooks(t, rec, 2)
	require.Contains(t, rec.body(1), "model_fetch_recovered")
}
