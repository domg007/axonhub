//nolint:exhaustruct_v5 // Test fixtures intentionally set only fields relevant to each scenario.
package biz

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
)

func changeTestChannel(models []string) *ent.Channel {
	ch := testChannel(7, channel.TypeOpenai)
	ch.SupportedModels = models

	return ch
}

// 等待异步通知到达（asyncNotifyChannelModelChange 开的是 goroutine）。
func requireWebhookCount(t *testing.T, rec *webhookRecorder, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return rec.count() == want }, 2*time.Second, 10*time.Millisecond)
}

func TestModelChangeAlert_NotifiesAddedAndRemoved(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, _, ctx := newModelFetchAlertFixture(t, rec, EventChannelModelsChanged)

	before := changeTestChannel([]string{"gpt-5", "claude-4"})
	after := changeTestChannel([]string{"gpt-5", "gpt-6-astra-cc-format"})

	svc.observeModelChangeForAlert(ctx, before, after)
	requireWebhookCount(t, rec, 1)

	body := rec.body(0)
	require.Contains(t, body, modelChangeAlertTrigger)
	require.Contains(t, body, "gpt-6-astra-cc-format")
	require.Contains(t, body, "claude-4")
	require.Contains(t, body, "新增 1 个")
	require.Contains(t, body, "移除 1 个")
}

func TestModelChangeAlert_SilentWhenUnchanged(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, _, ctx := newModelFetchAlertFixture(t, rec, EventChannelModelsChanged)

	// 同一批模型，只是顺序不同：不算变更。
	before := changeTestChannel([]string{"a", "b"})
	after := changeTestChannel([]string{"b", "a"})

	svc.observeModelChangeForAlert(ctx, before, after)
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, rec.count())
}

// 管理台里手动点「同步模型」是登录用户身份，界面上当场就能看到结果，不再发通知。
func TestModelChangeAlert_SkipsNonSystemPrincipal(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, client, _ := newModelFetchAlertFixture(t, rec, EventChannelModelsChanged)

	userCtx := ent.NewContext(context.Background(), client)

	svc.observeModelChangeForAlert(userCtx,
		changeTestChannel([]string{"a"}),
		changeTestChannel([]string{"a", "b"}))
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, rec.count())
}

// 与第十八节的告警不同：没有显式订阅时不回退到 channel.auto_disabled，什么都不发。
func TestModelChangeAlert_DoesNotFallBackToAutoDisabled(t *testing.T) {
	rec := newWebhookRecorder(t)
	svc, _, ctx := newModelFetchAlertFixture(t, rec, EventChannelAutoDisabled)

	svc.observeModelChangeForAlert(ctx,
		changeTestChannel([]string{"a"}),
		changeTestChannel([]string{"a", "b"}))
	time.Sleep(200 * time.Millisecond)
	require.Zero(t, rec.count())
}

func TestFormatModelChangeReason(t *testing.T) {
	// 双引号→单引号，孤立反斜杠→斜杠，否则 Discord 解析 JSON 会失败。
	reason := formatModelChangeReason([]string{`we"ird`, `back\slash`}, nil)
	require.NotContains(t, reason, `"`)
	require.Contains(t, reason, "we'ird")
	require.Contains(t, reason, "back/slash")

	// 换行用的是字面量 \n（两个字符），不是真换行。
	both := formatModelChangeReason([]string{"a"}, []string{"b"})
	require.Contains(t, both, `\n`)
	require.NotContains(t, both, "\n")

	// 超过上限的部分折叠。
	many := make([]string, modelChangeAlertMaxNames+5)
	for i := range many {
		many[i] = "m" + strings.Repeat("x", i)
	}

	folded := formatModelChangeReason(many, nil)
	require.Contains(t, folded, "等共 25 个")
}
