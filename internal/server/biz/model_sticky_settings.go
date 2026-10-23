package biz

import (
	"context"
	"encoding/json"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/apikey"
	"github.com/looplj/axonhub/internal/ent/system"
	"github.com/looplj/axonhub/internal/log"
)

func resetModelStickyKeysAfterCommit(ctx context.Context, client *ent.Client, ids ...int) {
	if len(ids) == 0 {
		return
	}
	runAfterCommit(ctx, func(ctx context.Context) {
		keys, err := client.APIKey.Query().Where(apikey.IDIn(ids...)).All(ctx)
		if err != nil {
			log.Warn(ctx, "【新增策略：模型固定渠道】读取已提交的 Key 配置失败，原始错误见详情。", log.Cause(err))
			ModelStickyRuntime.fenceKeys(ids)
			return
		}
		found := make(map[int]bool, len(keys))
		for _, key := range keys {
			found[key.ID] = true
			ModelStickyRuntime.PublishKey(key)
		}
		for _, id := range ids {
			if !found[id] {
				ModelStickyRuntime.ResetKey(id)
			}
		}
	})
}

// Project changes fence requests even when no route has yet been acquired.
// Template changes affect only keys whose active profile actually uses it.
func resetModelStickyProject(ctx context.Context, client *ent.Client, projectID int, templateID int) {
	runAfterCommit(ctx, func(ctx context.Context) {
		if templateID == 0 {
			project, err := client.Project.Get(ctx, projectID)
			if err != nil {
				log.Warn(ctx, "【新增策略：模型固定渠道】读取已提交的项目配置失败，原始错误见详情。", log.Cause(err))
				ModelStickyRuntime.fenceProject(projectID)
				return
			}
			ModelStickyRuntime.PublishProject(project)
			return
		}
		keys, err := client.APIKey.Query().Where(apikey.ProjectIDEQ(projectID)).All(ctx)
		if err != nil {
			log.Warn(ctx, "【新增策略：模型固定渠道】刷新项目固定路由状态失败，原始错误见详情。", log.Cause(err))
			ModelStickyRuntime.fenceProject(projectID)
			return
		}
		for _, key := range keys {
			if profile := key.GetActiveProfile(); profile != nil && profile.TemplateID != nil && *profile.TemplateID == templateID {
				ModelStickyRuntime.PublishKey(key)
			}
		}
	})
}
func resetModelStickySystemAfterCommit(ctx context.Context, service *SystemService) {
	// Context-only services are supported by existing callers; production uses
	// its root client so the callback never queries a committed transaction.
	client := ent.FromContext(ctx)
	if service.AbstractService != nil && service.db != nil {
		client = service.db
	}
	runAfterCommit(ctx, func(ctx context.Context) {
		row, err := client.System.Query().Where(system.KeyEQ(SystemKeyRetryPolicy)).Only(ctx)
		if err != nil {
			log.Warn(ctx, "【新增策略：模型固定渠道】读取已提交的重试配置失败，原始错误见详情。", log.Cause(err))
			ModelStickyRuntime.fenceSystemInherited()
			return
		}
		var policy RetryPolicy
		if err := json.Unmarshal([]byte(row.Value), &policy); err != nil {
			ModelStickyRuntime.fenceSystemInherited()
			return
		}
		service.invalidateSystemValueCache(ctx, SystemKeyRetryPolicy)
		ModelStickyRuntime.publishSystemStrategy(policy.LoadBalancerStrategy, row.UpdatedAt)
	})
}
