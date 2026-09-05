package biz

import (
	"context"

	"github.com/looplj/axonhub/internal/authz"
)

// SyncAllChannelModelsNow 立即对所有「已启用且开启了自动同步」的渠道执行一次模型列表同步。
//
// 与定时任务 runSyncChannelModelsPeriodically 的差异只有一处：不调用 shouldRunModelSync。
// shouldRunModelSync 会把当前时间按同步频率对齐后与上次执行时间比对，相同则跳过，
// 这是为了防止调度器在同一时间窗内重复触发，对定时任务是必要的。
// 但手动触发的语义就是「现在就跑」，若沿用该判断，点击后会被静默跳过且没有任何反馈。
//
// 需要 WithSystemBypass 的原因：syncChannelModels 会读写所有渠道，
// 而普通请求上下文里没有能覆盖全部渠道的主体身份，权限层会拦截。
// 定时任务走的也是同一个做法（channel_internal.go 中的 channel-run-model-sync）。
//
// 传入的 ctx 必须独立于 HTTP 请求上下文，否则响应返回时同步会被一并取消。
func (svc *ChannelService) SyncAllChannelModelsNow(ctx context.Context) {
	svc.syncChannelModels(authz.WithSystemBypass(ctx, "channel-manual-model-sync"))
}
