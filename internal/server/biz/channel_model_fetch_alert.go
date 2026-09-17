package biz

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/channel"
	"github.com/looplj/axonhub/internal/log"
)

// 渠道模型拉取告警。
//
// 背景：syncChannelModelsForChannel 拉取上游模型列表失败时只写一条日志；
// 拉到空列表甚至不算失败——渠道会保留上一次同步到的旧列表，下游 /v1/models 里它的模型照常出现，
// 但实际调用全部失败。这个文件在拉取结果刚返回时观察一次，把「报错」和「空列表」两种情况
// 通过已有的 webhook 机制通知出去，并在渠道恢复后再通知一次。
//
// 全部逻辑都在本文件，上游文件 channel_model_sync.go 只加一行调用（observeModelFetchForAlert）。
// 状态放在包级变量而不是 ChannelService 字段上，是为了不改上游的结构体定义。

// 新增的两个 webhook 事件名。管理台的事件列表是前端写死的，不会显示这两个；
// 订阅规则见 selectModelFetchAlertTargets。
const (
	EventChannelModelFetchFailed    = "channel.model_fetch_failed"
	EventChannelModelFetchRecovered = "channel.model_fetch_recovered"
)

// 渲染到模板变量 {{.Trigger.Type}} 的取值，用来在同一个模板里区分是哪种情况。
const (
	modelFetchAlertTriggerError     = "model_fetch_error"
	modelFetchAlertTriggerEmpty     = "model_fetch_empty"
	modelFetchAlertTriggerRecovered = "model_fetch_recovered"
)

// 原因文本的最大长度。Discord 单条消息上限 2000 字符，上游返回的 HTML 错误页可能很长。
const modelFetchAlertReasonMaxLen = 300

// 每个渠道当前是否处于「拉取失败」状态：channelID -> 最近一次失败原因。
// 只在状态翻转（正常→失败、失败→正常）时才发通知；实时同步每 5 分钟就跑一次，
// 不做去重的话渠道挂一晚上会刷上百条消息。
//
// 状态只在内存里，进程重启后清零：一个持续失败的渠道会在重启后再报一次，可以接受。
var (
	modelFetchAlertMu      sync.Mutex
	modelFetchAlertFailing = map[int]string{}
)

// ChannelModelFetchEvent 是一次模型拉取告警（失败或恢复）携带的信息。
type ChannelModelFetchEvent struct {
	ChannelID       int
	ChannelName     string
	ChannelProvider string
	ChannelBaseURL  string
	ChannelStatus   string
	TriggerType     string
	Reason          string
	ModelCount      int
	Recovered       bool
	OccurredAt      time.Time
}

// modelFetchOutcome 是对一次 FetchModels 结果的判定。
type modelFetchOutcome struct {
	failed      bool
	triggerType string
	reason      string
	modelCount  int
}

// evaluateModelFetchOutcome 把 FetchModels 的返回值归为「失败」或「正常」。
//
// 判定顺序与 syncChannelModelsForChannel 自己的错误处理保持一致：
// Go 层错误、上游返回的错误、Cline 回退到内置列表，这三种上游本来就当失败处理；
// 在此之上多加一条：列表为空也算失败——这正是「渠道已经挂了但列表里还有它」的信号。
func evaluateModelFetchOutcome(result *FetchModelsResult, err error) modelFetchOutcome {
	if err != nil {
		return modelFetchOutcome{failed: true, triggerType: modelFetchAlertTriggerError, reason: err.Error()}
	}

	if result == nil {
		return modelFetchOutcome{failed: true, triggerType: modelFetchAlertTriggerError, reason: "model fetch returned no result"}
	}

	if result.Error != nil {
		return modelFetchOutcome{failed: true, triggerType: modelFetchAlertTriggerError, reason: *result.Error}
	}

	if result.Fallback {
		return modelFetchOutcome{failed: true, triggerType: modelFetchAlertTriggerError, reason: "model fetch returned fallback models"}
	}

	if len(result.Models) == 0 {
		return modelFetchOutcome{failed: true, triggerType: modelFetchAlertTriggerEmpty, reason: "upstream returned an empty model list"}
	}

	return modelFetchOutcome{modelCount: len(result.Models)}
}

// observeModelFetchForAlert 是插在 syncChannelModelsForChannel 里的唯一钩子，
// 在 FetchModels 返回之后、上游自己判断错误之前调用，因此能看到全部情况。
//
// 三条前置过滤，每条都对应一个已知的误报来源：
//
//  1. 上下文已经超时/取消：实时同步是串行跑所有渠道、共用一份总预算（AXONHUB_REALTIME_MODELS_TIMEOUT），
//     前面某个渠道把预算吃光后，后面的渠道会瞬间「失败」。这种失败不能算数，直接跳过，状态也不动。
//  2. 只处理系统身份发起的同步：定时任务、/sync-models 手动链接、下游拉 /v1/models 的实时同步，
//     三条路径都是 authz.WithSystemBypass 起的（principal 为 System）。管理台里手动点某个渠道的
//     「同步模型」按钮走的是登录用户的身份（principal 为 User），界面上本来就能看到失败原因，不再另发通知。
//  3. Volcengine 类型按 FetchModels 的设计永远返回空列表，排除掉，否则每次同步都报「空」。
func (svc *ChannelService) observeModelFetchForAlert(ctx context.Context, ch *ent.Channel, result *FetchModelsResult, err error) {
	if ch == nil || ctx.Err() != nil {
		return
	}

	if p, ok := authz.GetPrincipal(ctx); !ok || !p.IsSystem() {
		return
	}

	if ch.Type == channel.TypeVolcengine {
		return
	}

	outcome := evaluateModelFetchOutcome(result, err)
	transition := recordModelFetchOutcome(ch.ID, outcome)

	if outcome.failed {
		log.Warn(ctx, "model fetch alert: channel model fetch failed",
			log.Int("channel_id", ch.ID),
			log.String("channel_name", ch.Name),
			log.String("trigger", outcome.triggerType),
			log.String("reason", outcome.reason),
			log.Bool("notify", transition),
		)
	}

	if !transition {
		return
	}

	event := ChannelModelFetchEvent{
		ChannelID:       ch.ID,
		ChannelName:     ch.Name,
		ChannelProvider: ch.Type.String(),
		ChannelBaseURL:  ch.BaseURL,
		ChannelStatus:   ch.Status.String(),
		TriggerType:     outcome.triggerType,
		Reason:          sanitizeModelFetchAlertReason(outcome.reason),
		ModelCount:      outcome.modelCount,
		Recovered:       !outcome.failed,
		OccurredAt:      time.Now(),
	}

	if event.Recovered {
		event.TriggerType = modelFetchAlertTriggerRecovered
		event.Reason = fmt.Sprintf("model fetch succeeded again (%d models)", outcome.modelCount)
	}

	svc.asyncNotifyChannelModelFetch(ctx, event)
}

// recordModelFetchOutcome 更新渠道的失败状态，返回这次结果是否构成状态翻转（需要发通知）。
func recordModelFetchOutcome(channelID int, outcome modelFetchOutcome) bool {
	modelFetchAlertMu.Lock()
	defer modelFetchAlertMu.Unlock()

	_, wasFailing := modelFetchAlertFailing[channelID]

	if outcome.failed {
		modelFetchAlertFailing[channelID] = outcome.reason
		return !wasFailing
	}

	delete(modelFetchAlertFailing, channelID)

	return wasFailing
}

// resetModelFetchAlertState 清空所有渠道的失败状态，只给测试用。
func resetModelFetchAlertState() {
	modelFetchAlertMu.Lock()
	defer modelFetchAlertMu.Unlock()

	modelFetchAlertFailing = map[int]string{}
}

// sanitizeModelFetchAlertReason 把原因文本整理成可以直接嵌进 JSON 字符串字面量的形式。
//
// webhook 的载荷模板是 text/template 做纯文本替换，不会做 JSON 转义。而 Go 的网络错误
// 格式是 Get "https://…": dial tcp …，自带英文双引号，直接替换进去会让整个 JSON 失效，
// Discord 返回 400，通知静默丢失。这里把双引号换成单引号、反斜杠换成斜杠、
// 控制字符（换行、制表符）换成空格，再截断长度。
func sanitizeModelFetchAlertReason(reason string) string {
	var b strings.Builder
	b.Grow(len(reason))

	lastSpace := false

	for _, r := range reason {
		switch {
		case r == '"':
			r = '\''
		case r == '\\':
			r = '/'
		case unicode.IsControl(r) || unicode.IsSpace(r):
			r = ' '
		}

		if r == ' ' {
			if lastSpace {
				continue
			}

			lastSpace = true
		} else {
			lastSpace = false
		}

		b.WriteRune(r)
	}

	cleaned := strings.TrimSpace(b.String())
	if runes := []rune(cleaned); len(runes) > modelFetchAlertReasonMaxLen {
		cleaned = string(runes[:modelFetchAlertReasonMaxLen]) + "…"
	}

	return cleaned
}

// asyncNotifyChannelModelFetch 在后台发送通知，不阻塞同步流程。写法与 asyncNotifyChannelAutoDisabled 一致。
func (svc *ChannelService) asyncNotifyChannelModelFetch(ctx context.Context, event ChannelModelFetchEvent) {
	notifyCtx := context.WithoutCancel(ctx)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error(notifyCtx, "channel model fetch webhook notification panicked", log.Any("panic", r))
			}
		}()

		svc.WebhookNotifier.NotifyChannelModelFetch(notifyCtx, event)
	}()
}

// NotifyChannelModelFetch 把一次模型拉取告警投递到订阅的 webhook 地址。
//
// 复用 WebhookRenderContext，所以管理台里已有的载荷模板变量全部可用，
// 一份模板同时服务 channel.auto_disabled 和这里的两个事件，用 {{.Event}} 或 {{.Trigger.Type}} 区分。
//
// 故意不调用 webhook_notifier.go 里的 notify：上游已经改过一次它的签名（多了时区参数），
// 直接依赖它会在下一次同步上游时编译失败。这里只依赖它下面几个更稳定的小函数。
func (n *WebhookNotifier) NotifyChannelModelFetch(ctx context.Context, event ChannelModelFetchEvent) {
	log.Info(ctx, "notify channel model fetch", log.Any("event", event))

	eventName := EventChannelModelFetchFailed
	severity := "warning"

	if event.Recovered {
		eventName = EventChannelModelFetchRecovered
		severity = "info"
	}

	renderCtx := WebhookRenderContext{
		Event:    eventName,
		Severity: severity,
	}
	renderCtx.Channel.ID = event.ChannelID
	renderCtx.Channel.Name = event.ChannelName
	renderCtx.Channel.Provider = event.ChannelProvider
	renderCtx.Channel.BaseURL = event.ChannelBaseURL
	renderCtx.Channel.Status = event.ChannelStatus
	renderCtx.Trigger.Type = event.TriggerType
	renderCtx.Trigger.Reason = event.Reason
	renderCtx.Trigger.ActualCount = event.ModelCount

	ctx = authz.WithSystemBypass(context.WithoutCancel(ctx), "webhook-notifier-model-fetch")
	renderCtx.OccurredAt = event.OccurredAt.In(n.SystemService.TimeLocation(ctx)).Format(time.RFC3339)

	cfg := *n.SystemService.WebhookNotifierConfigOrDefault(ctx)
	targets := n.selectModelFetchAlertTargets(cfg, eventName)

	if len(targets) == 0 {
		log.Debug(ctx, "model fetch alert: no webhook targets subscribed", log.String("event", eventName))
		return
	}

	for _, target := range targets {
		body, err := renderWebhookTemplate(target.Body, renderCtx)
		if err != nil {
			log.Warn(ctx, "failed to render webhook body template",
				log.String("event", eventName),
				log.String("target", target.Name),
				log.Cause(err),
			)

			continue
		}

		headers, err := renderWebhookHeaders(target.Headers, renderCtx)
		if err != nil {
			log.Warn(ctx, "failed to render webhook headers",
				log.String("event", eventName),
				log.String("target", target.Name),
				log.Cause(err),
			)

			continue
		}

		if err := n.send(ctx, target, body, headers); err != nil {
			log.Warn(ctx, "failed to send webhook notification",
				log.String("event", eventName),
				log.String("target", target.Name),
				log.Cause(err),
			)
		}
	}
}

// selectModelFetchAlertTargets 决定通知发给哪些地址：
//
// 1. 配置里若有针对该事件名的显式订阅（可通过 GraphQL updateWebhookNotifierConfig 添加），以它为准；
// 2. 否则回退到 channel.auto_disabled 的订阅者——管理台里只能勾这一个事件，
// 「渠道被自动禁用」和「渠道拉不到模型」本来就是同一类需要人看一眼的告警。
func (n *WebhookNotifier) selectModelFetchAlertTargets(cfg WebhookNotifierConfig, eventName string) []WebhookTarget {
	if targets := n.selectTargets(cfg, eventName); len(targets) > 0 {
		return targets
	}

	return n.selectTargets(cfg, EventChannelAutoDisabled)
}
