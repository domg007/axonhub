package biz

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/samber/lo"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/log"
)

// 渠道模型列表发生增减时的通知。
//
// 背景：后台刷新改成异步之后（第十七节 2026-09-28 改版），请求方看不到同步结果了，
// 需要一个「被通知」的渠道。这里在每个渠道同步完成后比一下前后的 supported_models，
// 只要有增减就发一条，并把具体的模型名列出来。
//
// 与第十八节的拉取失败告警的分工：
//   - channel.model_fetch_failed / recovered：渠道「坏了 / 好了」，是故障告警，severity=warning
//   - channel.models_changed：渠道「多了 / 少了模型」，是信息通知，severity=info
//
// 全部逻辑都在本文件，上游文件 channel_model_sync.go 只加一行调用（observeModelChangeForAlert）。

// EventChannelModelsChanged 是新增的 webhook 事件名。
// 管理台的事件列表是前端写死的，不会显示它，订阅规则见 selectModelChangeAlertTargets。
const EventChannelModelsChanged = "channel.models_changed"

// 渲染到模板变量 {{.Trigger.Type}} 的取值。
const modelChangeAlertTrigger = "models_changed"

const (
	// 每个方向（新增 / 移除）最多列出多少个模型名，超出部分折叠成「…等 N 个」。
	modelChangeAlertMaxNames = 20
	// 整段原因文本的上限。Discord 单条消息上限 2000 字符，模板里还有其它字段，留出余量。
	modelChangeAlertReasonMaxLen = 1200
)

// ChannelModelChangeEvent 是一次模型列表变更携带的信息。
type ChannelModelChangeEvent struct {
	ChannelID       int
	ChannelName     string
	ChannelProvider string
	ChannelBaseURL  string
	ChannelStatus   string
	Added           []string
	Removed         []string
	TotalCount      int
	Reason          string
	OccurredAt      time.Time
}

// observeModelChangeForAlert 是插在 syncChannelModelsForChannel 末尾的唯一钩子。
//
// before 是传进同步函数的渠道快照（拉取之前读的），after 是事务里重读/写回的渠道。
// 两者的 SupportedModels 一比就是这次同步的增减。
//
// 两条前置过滤，与第十八节的告警保持一致：
//
//  1. 上下文已超时/取消：同步被掐断时的结果不可信，不发。
//  2. 只处理系统身份发起的同步（定时任务、/sync-models 手动链接、下游拉 /v1/models 触发的后台刷新）。
//     管理台里手动点某个渠道的「同步模型」按钮是登录用户身份，界面上当场就能看到结果，不再另发通知。
//
// 注意：before 是拉取前的快照，如果拉取期间有人在管理台改了手动模型，这次列出的增减会把那部分也算进来。
// 只影响通知文案，不影响落库结果。
func (svc *ChannelService) observeModelChangeForAlert(ctx context.Context, before *ent.Channel, after *ent.Channel) {
	if before == nil || after == nil || ctx.Err() != nil {
		return
	}

	if p, ok := authz.GetPrincipal(ctx); !ok || !p.IsSystem() {
		return
	}

	added := lo.Without(after.SupportedModels, before.SupportedModels...)
	removed := lo.Without(before.SupportedModels, after.SupportedModels...)

	if len(added) == 0 && len(removed) == 0 {
		return
	}

	log.Info(ctx, "model change alert: channel models changed",
		log.Int("channel_id", after.ID),
		log.String("channel_name", after.Name),
		log.Any("added", added),
		log.Any("removed", removed),
	)

	event := ChannelModelChangeEvent{
		ChannelID:       after.ID,
		ChannelName:     after.Name,
		ChannelProvider: after.Type.String(),
		ChannelBaseURL:  after.BaseURL,
		ChannelStatus:   after.Status.String(),
		Added:           added,
		Removed:         removed,
		TotalCount:      len(after.SupportedModels),
		Reason:          formatModelChangeReason(added, removed),
		OccurredAt:      time.Now(),
	}

	svc.asyncNotifyChannelModelChange(ctx, event)
}

// formatModelChangeReason 拼出可直接嵌进 JSON 字符串字面量的变更描述。
//
// \n 是故意保留的两个字符（反斜杠 + n）：模板是纯文本替换，它会原样进入 JSON 源码，
// Discord 解析 JSON 时才变成真正的换行。直接写真换行反而会让 JSON 非法。
func formatModelChangeReason(added, removed []string) string {
	parts := make([]string, 0, 2)

	if len(added) > 0 {
		parts = append(parts, fmt.Sprintf("➕ 新增 %d 个：%s", len(added), joinModelNames(added)))
	}

	if len(removed) > 0 {
		parts = append(parts, fmt.Sprintf("➖ 移除 %d 个：%s", len(removed), joinModelNames(removed)))
	}

	return sanitizeModelChangeText(strings.Join(parts, "\\n"), modelChangeAlertReasonMaxLen)
}

// joinModelNames 把模型名拼成一行，超过上限的部分折叠。
func joinModelNames(names []string) string {
	if len(names) <= modelChangeAlertMaxNames {
		return strings.Join(names, ", ")
	}

	return strings.Join(names[:modelChangeAlertMaxNames], ", ") +
		fmt.Sprintf(", …等共 %d 个", len(names))
}

// sanitizeModelChangeText 与 sanitizeModelFetchAlertReason 同理：模板是纯文本替换，不做 JSON 转义，
// 模型名里万一出现双引号或反斜杠会让整个请求体失效。
//
// 与那个函数的差别：长度上限可传，且不把字面量 \n 当空白压掉（它已经是两个普通字符）。
func sanitizeModelChangeText(s string, maxRunes int) string {
	var b strings.Builder
	b.Grow(len(s))

	lastSpace := false

	for _, r := range s {
		switch {
		case r == '"':
			r = '\''
		case r == '\\':
			// 保留：formatModelChangeReason 里的 \n 靠它。模型名里出现反斜杠的情况极少，
			// 且后面跟的不是 n 时 Discord 会把整个 JSON 判为非法，所以统一只允许 \n 这一种组合。
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
	cleaned = sanitizeStrayBackslashes(cleaned)

	if runes := []rune(cleaned); len(runes) > maxRunes {
		cleaned = string(runes[:maxRunes]) + "…"
	}

	return cleaned
}

// sanitizeStrayBackslashes 只放行 \n，其余反斜杠一律换成斜杠。
// 否则像 "a\b" 这种模型名会让 Discord 解析 JSON 失败，通知静默丢失。
func sanitizeStrayBackslashes(s string) string {
	var b strings.Builder
	b.Grow(len(s))

	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		if runes[i] != '\\' {
			b.WriteRune(runes[i])
			continue
		}

		if i+1 < len(runes) && runes[i+1] == 'n' {
			b.WriteString("\\n")
			i++

			continue
		}

		b.WriteRune('/')
	}

	return b.String()
}

// asyncNotifyChannelModelChange 在后台发送通知，不阻塞同步流程。
func (svc *ChannelService) asyncNotifyChannelModelChange(ctx context.Context, event ChannelModelChangeEvent) {
	notifyCtx := context.WithoutCancel(ctx)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error(notifyCtx, "channel model change webhook notification panicked", log.Any("panic", r))
			}
		}()

		svc.WebhookNotifier.NotifyChannelModelChange(notifyCtx, event)
	}()
}

// NotifyChannelModelChange 把一次模型列表变更投递到订阅的 webhook 地址。
//
// 复用 WebhookRenderContext，所以管理台里已有的载荷模板变量全部可用。
// 不调用 webhook_notifier.go 里的 notify，理由与 NotifyChannelModelFetch 相同：
// 上游改过它的签名，直接依赖会在同步上游时编译失败。
func (n *WebhookNotifier) NotifyChannelModelChange(ctx context.Context, event ChannelModelChangeEvent) {
	renderCtx := WebhookRenderContext{
		Event:    EventChannelModelsChanged,
		Severity: "info",
	}
	renderCtx.Channel.ID = event.ChannelID
	renderCtx.Channel.Name = event.ChannelName
	renderCtx.Channel.Provider = event.ChannelProvider
	renderCtx.Channel.BaseURL = event.ChannelBaseURL
	renderCtx.Channel.Status = event.ChannelStatus
	renderCtx.Trigger.Type = modelChangeAlertTrigger
	renderCtx.Trigger.Reason = event.Reason
	renderCtx.Trigger.ActualCount = event.TotalCount

	ctx = authz.WithSystemBypass(context.WithoutCancel(ctx), "webhook-notifier-model-change")
	renderCtx.OccurredAt = event.OccurredAt.In(n.SystemService.TimeLocation(ctx)).Format(time.RFC3339)

	cfg := *n.SystemService.WebhookNotifierConfigOrDefault(ctx)

	targets := n.selectModelChangeAlertTargets(cfg)
	if len(targets) == 0 {
		log.Debug(ctx, "model change alert: no webhook targets subscribed",
			log.String("event", EventChannelModelsChanged))

		return
	}

	for _, target := range targets {
		body, err := renderWebhookTemplate(target.Body, renderCtx)
		if err != nil {
			log.Warn(ctx, "failed to render webhook body template",
				log.String("event", EventChannelModelsChanged),
				log.String("target", target.Name),
				log.Cause(err),
			)

			continue
		}

		headers, err := renderWebhookHeaders(target.Headers, renderCtx)
		if err != nil {
			log.Warn(ctx, "failed to render webhook headers",
				log.String("event", EventChannelModelsChanged),
				log.String("target", target.Name),
				log.Cause(err),
			)

			continue
		}

		if err := n.send(ctx, target, body, headers); err != nil {
			log.Warn(ctx, "failed to send webhook notification",
				log.String("event", EventChannelModelsChanged),
				log.String("target", target.Name),
				log.Cause(err),
			)
		}
	}
}

// selectModelChangeAlertTargets 只认显式订阅，**故意不回退到 channel.auto_disabled**。
//
// 与第十八节的告警不同：那两个事件回退是为了「不改前端也能用」，
// 而模型变更是高频信息通知，回退进故障告警频道会把真正要紧的消息淹掉。
// 没配显式订阅时什么都不发（日志里会留一条 debug），配法见部署方案第二十节。
func (n *WebhookNotifier) selectModelChangeAlertTargets(cfg WebhookNotifierConfig) []WebhookTarget {
	return n.selectTargets(cfg, EventChannelModelsChanged)
}
