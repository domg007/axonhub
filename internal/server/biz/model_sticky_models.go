package biz

import (
	"context"
	"encoding/json"

	"github.com/looplj/axonhub/internal/ent"
	"github.com/looplj/axonhub/internal/ent/model"
	"github.com/looplj/axonhub/internal/ent/schema/schematype"
	"github.com/looplj/axonhub/internal/ent/system"
	"github.com/looplj/axonhub/internal/log"
)

// Builders keep their original errors and transaction behavior. Notification is
// post-save, keyed by entity ID, so no sticky prefetch can intercept a normal
// mutation's not-found/database error. The committed read is batched.
func (svc *ModelService) publishModelStickyModelsAfterCommit(ctx context.Context, ids ...int) {
	if len(ids) == 0 {
		return
	}
	runAfterCommit(ctx, func(ctx context.Context) {
		rows, err := svc.db.Model.Query().Where(model.IDIn(ids...)).All(schematype.SkipSoftDelete(ctx))
		if err != nil {
			log.Warn(ctx, ModelStickyMessagePrefix+"读取已提交的模型配置失败，旧请求已隔离，原始错误见详情。", log.Cause(err))
			ModelStickyRuntime.invalidateModelEntities(ids)
			return
		}
		found := make(map[int]*ent.Model, len(rows))
		for _, row := range rows {
			found[row.ID] = row
		}
		for _, id := range ids {
			ModelStickyRuntime.PublishModel(found[id], id)
		}
	})
}
func (m *ModelStickyManager) invalidateModelEntities(ids []int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.config.entities == nil {
		m.config.entities = make(map[int]modelStickyEntityState)
	}
	for _, id := range ids {
		state := m.config.entities[id]
		state.revision = m.nextConfigRevisionLocked()
		// Do not assert an unread committed fingerprint. Admission still rejects all
		// snapshots from before this change; fresh model resolution can recover.
		state.fingerprint = ""
		m.config.entities[id] = state
		m.invalidateMatchingLocked(func(_ ModelStickyKey, r *modelStickyRoute) bool { return r.modelEntityID == id })
	}
}
func publishModelStickyInheritanceAfterCommit(ctx context.Context, svc *SystemService) {
	client := ent.FromContext(ctx)
	if svc.AbstractService != nil && svc.db != nil {
		client = svc.db
	}
	runAfterCommit(ctx, func(ctx context.Context) {
		row, err := client.System.Query().Where(system.KeyEQ(SystemKeyModelSettings)).Only(ctx)
		if err != nil {
			log.Warn(ctx, ModelStickyMessagePrefix+"读取已提交的模型继承设置失败，原始错误见详情。", log.Cause(err))
			ModelStickyRuntime.fenceInheritance()
			return
		}
		var settings SystemModelSettings
		if err := json.Unmarshal([]byte(row.Value), &settings); err != nil {
			log.Warn(ctx, ModelStickyMessagePrefix+"解析模型继承设置失败，原始错误见详情。", log.Cause(err))
			ModelStickyRuntime.fenceInheritance()
			return
		}
		svc.invalidateSystemValueCache(ctx, SystemKeyModelSettings)
		ModelStickyRuntime.publishInheritance(ModelStickyInheritanceFingerprint(&settings), row.UpdatedAt)
	})
}
func (m *ModelStickyManager) fenceInheritance() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.inheritanceChanged = m.nextConfigRevisionLocked()
	m.config.inheritanceKnown = false
	m.invalidateMatchingLocked(func(ModelStickyKey, *modelStickyRoute) bool { return true })
}

// Used only after enabled-model resolution failed. Include soft-deleted rows so
// deleting a configured model cannot silently revive its channel-model fallback.
func (svc *ModelService) ModelStickyUnavailableModel(ctx context.Context, name string) (*ent.Model, error) {
	return svc.entFromContext(ctx).Model.Query().Where(
		model.ModelID(name), model.Or(model.StatusNEQ(model.StatusEnabled), model.DeletedAtNEQ(0)),
	).Order(ent.Desc(model.FieldID)).First(schematype.SkipSoftDelete(ctx))
}
