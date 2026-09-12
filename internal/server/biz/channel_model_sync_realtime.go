package biz

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/looplj/axonhub/internal/authz"
	"github.com/looplj/axonhub/internal/log"
)

// 下游拉取模型列表时的「实时同步」开关与节流参数。
//
// AXONHUB_REALTIME_MODELS_TTL：缓存有效期。不配置（或配置成非法值、<= 0）时整个功能关闭，
// /v1/models 的行为与上游一字不差；配置成例如 60s 后，距上次同步超过 60 秒的请求会先去上游拉一次模型列表。
//
// AXONHUB_REALTIME_MODELS_TIMEOUT：单次同步的总时间预算，默认 20s。
// 上游同步是串行遍历渠道的，没有预算的话一个卡住的上游会把 /v1/models 拖到 llm_request_timeout（600s）。
const (
	realtimeModelSyncTTLEnv     = "AXONHUB_REALTIME_MODELS_TTL"
	realtimeModelSyncTimeoutEnv = "AXONHUB_REALTIME_MODELS_TIMEOUT"

	defaultRealtimeModelSyncTimeout = 20 * time.Second
)

var (
	// 进程启动时读一次，与 AXONHUB_MODEL_FETCH_UA / AXONHUB_MANUAL_SYNC_SECRET 的做法保持一致。
	realtimeModelSyncTTL     = realtimeModelSyncDurationFromEnv(realtimeModelSyncTTLEnv, 0)
	realtimeModelSyncTimeout = realtimeModelSyncDurationFromEnv(realtimeModelSyncTimeoutEnv, defaultRealtimeModelSyncTimeout)

	// realtimeModelSyncMu 同时充当 TTL 判定的锁和「单飞」闸门：
	// 并发请求会在这里排队，队首同步完成并刷新时间戳后，后面的请求再判定 TTL 就直接命中缓存返回。
	// 放在包级而不是 ChannelService 字段上，是为了不改上游的 channel.go 结构体定义。
	realtimeModelSyncMu   sync.Mutex
	realtimeModelSyncedAt time.Time
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

// RealtimeModelSyncEnabled 表示是否配置了实时同步。未配置时所有相关逻辑都是空操作。
func RealtimeModelSyncEnabled() bool {
	return realtimeModelSyncTTL > 0
}

// SyncAllChannelModelsIfStale 在距上次同步超过 TTL 时，先同步一次各渠道的模型列表再返回。
//
// 同步范围与定时任务完全一致：只覆盖「已启用且开启了自动同步模型」的渠道。
// 未开启自动同步的渠道不会被碰，但它们的模型仍然照常出现在 /v1/models 里 ——
// 列表本身是 ListEnabledModels 按「所有已启用渠道」算出来的，这里只是把该刷新的那部分刷新掉。
//
// 单个渠道拉取失败会被 syncChannelModels 记日志跳过，不影响其余渠道，也不影响本次响应。
//
// 同步结果会落库（syncChannelModelsForChannel 内部写 supported_models），
// 所以新模型列出来之后立刻就能用，不会在对话请求时掉进「model not found」。
//
// 落库后必须同步重载渠道缓存：ListEnabledModels 读的是 enabledChannelsCache 这份内存快照，
// 而上游只在变更后发一个异步刷新通知，赶不上当前这次响应，会让新模型晚一个请求才出现。
func (svc *ChannelService) SyncAllChannelModelsIfStale(ctx context.Context) {
	if !RealtimeModelSyncEnabled() {
		return
	}

	realtimeModelSyncMu.Lock()
	defer realtimeModelSyncMu.Unlock()

	if !realtimeModelSyncedAt.IsZero() && time.Since(realtimeModelSyncedAt) < realtimeModelSyncTTL {
		return
	}

	// 故意不继承请求上下文：客户端断开或请求超时不应该把已经开始的同步和落库打断到一半。
	// 与手动同步入口（api/manual_sync.go）的处理方式相同。
	syncCtx, cancel := context.WithTimeout(
		authz.WithSystemBypass(context.Background(), "channel-realtime-model-sync"),
		realtimeModelSyncTimeout,
	)
	defer cancel()

	svc.syncChannelModels(syncCtx)

	if err := svc.ReloadEnabledChannelsCache(syncCtx); err != nil {
		log.Warn(ctx, "realtime model sync failed to reload enabled channels cache", log.Cause(err))
	}

	// 无论成功与否都记时间戳：上游挂掉时不应该让每个请求都去重试，退化成用库里的旧列表即可。
	realtimeModelSyncedAt = time.Now()
}
