package biz

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/log"
)

// 下游拉取模型列表时的「后台刷新」开关与节流参数。
//
// AXONHUB_REALTIME_MODELS_TTL：刷新间隔。不配置（或配置成非法值、<= 0）时整个功能关闭，
// /v1/models 的行为与上游一字不差；配置成例如 5m 后，距上次刷新超过 5 分钟的那次请求会在
// 后台触发一轮全渠道模型同步。
//
// AXONHUB_REALTIME_MODELS_TIMEOUT：单轮后台同步的总时间预算，默认 5m。
// 上游是串行遍历渠道的，渠道多时一轮要跑一两分钟，所以这个预算必须大于「渠道数 × 单渠道耗时」，
// 否则排在后面的渠道会被 context deadline 掐死，永远同步不到。
// 它只是防止某个卡死的上游让这轮同步永远挂着，不再影响 /v1/models 的响应时间。
const (
	realtimeModelSyncTTLEnv     = "AXONHUB_REALTIME_MODELS_TTL"
	realtimeModelSyncTimeoutEnv = "AXONHUB_REALTIME_MODELS_TIMEOUT"

	defaultRealtimeModelSyncTimeout = 5 * time.Minute
)

var (
	// 进程启动时读一次，与 AXONHUB_MODEL_FETCH_UA / AXONHUB_MANUAL_SYNC_SECRET 的做法保持一致。
	realtimeModelSyncTTL     = realtimeModelSyncDurationFromEnv(realtimeModelSyncTTLEnv, 0)
	realtimeModelSyncTimeout = realtimeModelSyncDurationFromEnv(realtimeModelSyncTimeoutEnv, defaultRealtimeModelSyncTimeout)

	// realtimeModelSyncMu 只保护下面这两个状态字段，绝不在同步期间持有 ——
	// 否则 /v1/models 又会被同步卡住，等于白改。
	// 放在包级而不是 ChannelService 字段上，是为了不改上游的 channel.go 结构体定义。
	realtimeModelSyncMu sync.Mutex
	// realtimeModelSyncedAt 记录上一轮后台同步「结束」的时刻，TTL 从这里算起。
	realtimeModelSyncedAt time.Time
	// realtimeModelSyncRunning 是单飞（single-flight）闸门：一轮还没跑完时，
	// 后续请求不会再开第二个 goroutine，避免渠道多时 goroutine 和上游请求堆积。
	realtimeModelSyncRunning bool
)

func realtimeModelSyncDurationFromEnv(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}

	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}

	return d
}

// RealtimeModelSyncEnabled 表示是否配置了后台刷新。未配置时所有相关逻辑都是空操作。
func RealtimeModelSyncEnabled() bool {
	return realtimeModelSyncTTL > 0
}

// SyncAllChannelModelsIfStale 在距上次刷新超过 TTL 时，于后台启动一轮全渠道模型同步，然后立刻返回。
//
// 调用方（/v1/models）不等待：本次响应用的仍是库里的列表，新模型在这一轮后台同步完成之后的请求里出现。
// 这么做的原因：上游 syncChannelModels 是串行遍历渠道的，渠道一多（实测 43 个约 75 秒）根本不可能
// 塞进一次 HTTP 请求里。旧实现给它 20 秒预算并让请求等待，结果是只有前 11 个渠道刷得到，
// 排在后面的渠道每一轮都被 deadline 掐死，永远同步不到。
//
// 同步范围与定时任务完全一致：只覆盖「已启用且开启了自动同步模型」的渠道。
// 未开启自动同步的渠道不会被碰，但它们的模型仍然照常出现在 /v1/models 里 ——
// 列表本身是 ListEnabledModels 按「所有已启用渠道」算出来的，这里只是把该刷新的那部分刷新掉。
//
// 单个渠道拉取失败会被 syncChannelModels 记日志跳过，不影响其余渠道。
func (svc *ChannelService) SyncAllChannelModelsIfStale(ctx context.Context) {
	if !RealtimeModelSyncEnabled() {
		return
	}

	if !beginRealtimeModelSync(time.Now()) {
		return
	}

	go svc.runRealtimeModelSync()
}

// beginRealtimeModelSync 判定「该不该开一轮」并抢占闸门，返回 true 表示调用方负责启动同步。
// TTL 判定和抢占必须在同一把锁里完成，否则两个并发请求会同时判定为过期并各开一轮。
func beginRealtimeModelSync(now time.Time) bool {
	realtimeModelSyncMu.Lock()
	defer realtimeModelSyncMu.Unlock()

	if realtimeModelSyncRunning {
		return false
	}

	if !realtimeModelSyncedAt.IsZero() && now.Sub(realtimeModelSyncedAt) < realtimeModelSyncTTL {
		return false
	}

	realtimeModelSyncRunning = true

	return true
}

// finishRealtimeModelSync 释放闸门并刷新时间戳。
//
// 时间戳记在「结束」而不是「开始」：TTL 的语义是两轮之间至少隔这么久，
// 记开始时间会让一轮跑 2 分钟、TTL 5 分钟的情况变成每 3 分钟就重开一轮。
//
// 无论成功与否都记时间戳：上游整体挂掉时不应该让每个请求都去重试，退化成用库里的旧列表即可。
func finishRealtimeModelSync(now time.Time) {
	realtimeModelSyncMu.Lock()
	defer realtimeModelSyncMu.Unlock()

	realtimeModelSyncedAt = now
	realtimeModelSyncRunning = false
}

// runRealtimeModelSync 是后台那一轮同步的本体，始终在独立 goroutine 里跑。
func (svc *ChannelService) runRealtimeModelSync() {
	startedAt := time.Now()

	// 注册顺序即 LIFO 执行顺序：recover 先跑，兜住 panic 后 finish 才跑。
	// goroutine 里的 panic 没人接就会直接终止整个进程，后台任务必须自己兜住。
	defer func() { finishRealtimeModelSync(time.Now()) }()

	defer func() {
		if r := recover(); r != nil {
			log.Error(context.Background(), "realtime model sync panicked",
				log.String("panic", fmt.Sprint(r)))
		}
	}()

	// 故意不继承请求上下文：请求早就返回了，继承过来会被立刻取消。
	// 与手动同步入口（api/manual_sync.go）的处理方式相同。
	ctx, cancel := context.WithTimeout(
		authz.WithSystemBypass(context.Background(), "channel-realtime-model-sync"),
		realtimeModelSyncTimeout,
	)
	defer cancel()

	svc.syncChannelModels(ctx)

	// 落库后必须重载渠道缓存：ListEnabledModels 读的是 enabledChannelsCache 这份内存快照，
	// 而上游只在变更后发一个异步刷新通知（asyncReloadChannels），到达时机不确定。
	if err := svc.ReloadEnabledChannelsCache(ctx); err != nil {
		log.Warn(ctx, "realtime model sync failed to reload enabled channels cache", log.Cause(err))
	}

	log.Info(ctx, "realtime model sync finished",
		log.String("elapsed", time.Since(startedAt).String()),
		log.String("budget", realtimeModelSyncTimeout.String()),
		log.String("ttl", realtimeModelSyncTTL.String()))
}
