package api

import (
	"context"
	"crypto/subtle"
	"net/http"
	"os"
	"sync/atomic"

	"github.com/gin-gonic/gin"
	"go.uber.org/fx"

	"github.com/looplj/axonhub/internal/server/biz"
)

// manualSyncSecretEnv 是手动同步端点的访问密钥环境变量名。
const manualSyncSecretEnv = "AXONHUB_MANUAL_SYNC_SECRET"

// manualSyncSecret 在进程启动时读取一次，理由同 biz/model_fetcher_ua.go。
var manualSyncSecret = os.Getenv(manualSyncSecretEnv)

type ManualSyncHandlersParams struct {
	fx.In

	ChannelService *biz.ChannelService
}

type ManualSyncHandlers struct {
	ChannelService *biz.ChannelService

	// running 保证同一时刻只有一次全量同步在跑。
	// 这个端点挂在公开路径上，一次全量同步会向每个渠道的上游各发一次请求，
	// 若被连续访问而不加限制，会成倍放大对上游 API 的请求量，可能触发对方限流。
	running atomic.Bool
}

func NewManualSyncHandlers(params ManualSyncHandlersParams) *ManualSyncHandlers {
	return &ManualSyncHandlers{ChannelService: params.ChannelService}
}

// SyncAllModels 处理 GET /sync-models/:secret，触发一次全渠道模型列表同步。
func (h *ManualSyncHandlers) SyncAllModels(c *gin.Context) {
	// 未配置密钥时端点整个不可用。
	// 这样即使忘记设置环境变量，也不会留下一个无保护的公开触发入口。
	if manualSyncSecret == "" {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}

	// 用 subtle.ConstantTimeCompare 而不是 ==：
	// == 比较字符串会在第一个不相同的字节处立即返回，耗时随匹配前缀的长度变化，
	// 理论上可被用来逐字节猜出密钥。远程利用这一点非常困难（网络抖动远大于该时间差），
	// 这里用它纯粹是因为成本只有一行,且长度不同时该函数同样返回 0。
	if subtle.ConstantTimeCompare([]byte(c.Param("secret")), []byte(manualSyncSecret)) != 1 {
		// 返回 404 而不是 401/403：不向扫描者确认这个路径的存在。
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}

	if !h.running.CompareAndSwap(false, true) {
		c.JSON(http.StatusConflict, gin.H{"status": "already running"})
		return
	}

	// 后台执行，立刻返回。
	//
	// 不同步等待的原因：publicGroup 套了 WithTimeout(RequestTimeout)，
	// 部署配置里该值是 30s，而渠道较多时全量同步会超过这个时限,
	// 届时连接被切断、浏览器显示超时，但同步其实还在跑，状态对不上。
	//
	// 用 context.Background() 而不是 c.Request.Context()：
	// 后者会在 HTTP 响应写回后立即取消，那样同步刚起步就会被中断。
	go func() {
		defer h.running.Store(false)
		h.ChannelService.SyncAllChannelModelsNow(context.Background())
	}()

	// 202 Accepted 的语义是「请求已接受，但处理尚未完成」，正好对应这里的行为。
	// 同步结果请看容器日志，biz 层会记录每个渠道的成功/失败数。
	c.JSON(http.StatusAccepted, gin.H{"status": "sync started"})
}
